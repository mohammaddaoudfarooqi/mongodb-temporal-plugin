package kb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Embedder turns text into vectors (Voyage in ModeVoyage).
type Embedder interface {
	Embed(ctx context.Context, texts []string, inputType string) ([][]float64, error)
}

// Doc is a ticket as stored in the knowledge base.
type Doc struct {
	ID         string    `bson:"_id" json:"id"`
	CustomerID string    `bson:"customer_id,omitempty" json:"customer_id,omitempty"`
	Subject    string    `bson:"subject" json:"subject"`
	Body       string    `bson:"body" json:"body"`
	Resolution string    `bson:"resolution,omitempty" json:"resolution,omitempty"`
	Status     string    `bson:"status" json:"status"`
	Category   string    `bson:"category" json:"category"`
	Priority   int       `bson:"priority,omitempty" json:"priority,omitempty"`
	Text       string    `bson:"text" json:"-"`
	Embedding  []float64 `bson:"embedding,omitempty" json:"-"`
	UpdateTime time.Time `bson:"update_time" json:"-"`
}

// Store is the tickets collection plus its search indexes.
type Store struct {
	client   *mongo.Client
	coll     *mongo.Collection
	mode     Mode
	model    string
	embedder Embedder
	dims     int
}

// Open connects to the knowledge base in database db. embedder is used
// only in ModeVoyage; dims is its vector size.
func Open(ctx context.Context, uri, db string, mode Mode, model string, embedder Embedder, dims int) (*Store, error) {
	if mode == ModeVoyage && embedder == nil {
		return nil, errors.New("kb: voyage mode needs an embedder (set VOYAGE_API_KEY)")
	}
	c, err := mongo.Connect(options.Client().ApplyURI(uri).SetAppName("temporal-mongodb-showcase"))
	if err != nil {
		return nil, fmt.Errorf("kb: connect: %w", err)
	}
	if err := c.Ping(ctx, nil); err != nil {
		_ = c.Disconnect(ctx)
		return nil, fmt.Errorf("kb: ping: %w", err)
	}
	return &Store{client: c, coll: c.Database(db).Collection("tickets"), mode: mode, model: model, embedder: embedder, dims: dims}, nil
}

// Close disconnects.
func (s *Store) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }

// Mode reports how semantic search is served.
func (s *Store) Mode() Mode { return s.mode }

// EnsureIndexes creates the full-text and (unless ModeNone) vector
// indexes if missing, then waits until they are queryable.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	// Search indexes need the collection to exist.
	if err := s.coll.Database().CreateCollection(ctx, s.coll.Name()); err != nil && !isNamespaceExists(err) {
		return fmt.Errorf("kb: create collection: %w", err)
	}
	want := map[string]mongo.SearchIndexModel{
		TextIndexName: {Definition: TextIndexDefinition(), Options: options.SearchIndexes().SetName(TextIndexName).SetType("search")},
	}
	if s.mode != ModeNone {
		want[VectorIndexName] = mongo.SearchIndexModel{
			Definition: VectorIndexDefinition(s.mode, s.model, s.dims),
			Options:    options.SearchIndexes().SetName(VectorIndexName).SetType("vectorSearch"),
		}
	}
	for name, m := range want {
		exists, _, err := s.indexState(ctx, name)
		if err != nil {
			return err
		}
		if !exists {
			if _, err := s.coll.SearchIndexes().CreateOne(ctx, m); err != nil {
				return fmt.Errorf("kb: create search index %s: %w", name, err)
			}
		}
	}
	for name := range want {
		for {
			_, ready, err := s.indexState(ctx, name)
			if err != nil {
				return err
			}
			if ready {
				break
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("kb: waiting for search index %s: %w", name, ctx.Err())
			case <-time.After(2 * time.Second):
			}
		}
	}
	return nil
}

func (s *Store) indexState(ctx context.Context, name string) (exists, queryable bool, err error) {
	cur, err := s.coll.SearchIndexes().List(ctx, options.SearchIndexes().SetName(name))
	if err != nil {
		return false, false, fmt.Errorf("kb: list search indexes: %w", err)
	}
	defer cur.Close(ctx)
	var ixs []struct {
		Name      string `bson:"name"`
		Queryable bool   `bson:"queryable"`
	}
	if err := cur.All(ctx, &ixs); err != nil {
		return false, false, fmt.Errorf("kb: list search indexes: %w", err)
	}
	for _, ix := range ixs {
		if ix.Name == name {
			return true, ix.Queryable, nil
		}
	}
	return false, false, nil
}

func isNamespaceExists(err error) bool {
	var ce mongo.CommandError
	return errors.As(err, &ce) && ce.Code == 48
}

// searchText is what both searches look at: the problem and its fix.
func searchText(d Doc) string {
	return strings.TrimSpace(d.Subject + ". " + d.Body + ". " + d.Resolution)
}

// Upsert stores a ticket, embedding it first in ModeVoyage.
func (s *Store) Upsert(ctx context.Context, d Doc) error {
	d.Text = searchText(d)
	d.UpdateTime = time.Now().UTC()
	if err := s.embed(ctx, &d); err != nil {
		return err
	}
	_, err := s.coll.ReplaceOne(ctx, bson.M{"_id": d.ID}, d, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("kb: store ticket %s: %w", d.ID, err)
	}
	return nil
}

// Seed stores many tickets, embedding them in one batch.
func (s *Store) Seed(ctx context.Context, docs []Doc) error {
	for i := range docs {
		docs[i].Text = searchText(docs[i])
		docs[i].UpdateTime = time.Now().UTC()
	}
	if s.mode == ModeVoyage && len(docs) > 0 {
		texts := make([]string, len(docs))
		for i, d := range docs {
			texts[i] = d.Text
		}
		vecs, err := s.embedder.Embed(ctx, texts, "document")
		if err != nil {
			return fmt.Errorf("kb: embed seed: %w", err)
		}
		for i := range docs {
			docs[i].Embedding = vecs[i]
		}
	}
	for _, d := range docs {
		if _, err := s.coll.ReplaceOne(ctx, bson.M{"_id": d.ID}, d, options.Replace().SetUpsert(true)); err != nil {
			return fmt.Errorf("kb: seed ticket %s: %w", d.ID, err)
		}
	}
	return nil
}

// Resolve records the fix, after which the ticket is an answer for
// similar tickets.
func (s *Store) Resolve(ctx context.Context, id, resolution string) error {
	var d Doc
	if err := s.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&d); err != nil {
		return fmt.Errorf("kb: load ticket %s: %w", id, err)
	}
	d.Resolution, d.Status = resolution, "resolved"
	return s.Upsert(ctx, d)
}

// SetStatus updates a ticket's status.
func (s *Store) SetStatus(ctx context.Context, id, status string, priority int) error {
	_, err := s.coll.UpdateByID(ctx, id, bson.M{"$set": bson.M{"status": status, "priority": priority, "update_time": time.Now().UTC()}})
	if err != nil {
		return fmt.Errorf("kb: update ticket %s: %w", id, err)
	}
	return nil
}

func (s *Store) embed(ctx context.Context, d *Doc) error {
	if s.mode != ModeVoyage {
		return nil
	}
	vecs, err := s.embedder.Embed(ctx, []string{d.Text}, "document")
	if err != nil {
		return fmt.Errorf("kb: embed ticket %s: %w", d.ID, err)
	}
	d.Embedding = vecs[0]
	return nil
}

// Similar finds resolved tickets like the query: full-text and semantic
// search over the same collection, fused with Reciprocal Rank Fusion.
// In ModeNone it is full-text only.
func (s *Store) Similar(ctx context.Context, query string, limit int) ([]Hit, error) {
	r, err := s.Search(ctx, query, limit)
	return r.Hybrid, err
}

// Results are one query's full-text, vector and hybrid (fused) hits.
// Vector is empty in ModeNone.
type Results struct {
	Text   []Hit `json:"text"`
	Vector []Hit `json:"vector"`
	Hybrid []Hit `json:"hybrid"`
}

// Search runs the full-text and vector searches once and returns each
// list and their fusion, up to limit hits each.
func (s *Store) Search(ctx context.Context, query string, limit int) (Results, error) {
	text, err := s.textSearch(ctx, query, limit*3)
	if err != nil {
		return Results{}, err
	}
	var vector []Hit
	if s.mode != ModeNone {
		if vector, err = s.vectorSearch(ctx, query, limit*3); err != nil {
			return Results{}, err
		}
	}
	return Results{Text: top(text, limit), Vector: top(vector, limit), Hybrid: Fuse(text, vector, 60, limit)}, nil
}

func top(h []Hit, n int) []Hit {
	if len(h) > n {
		return h[:n]
	}
	return h
}

func (s *Store) textSearch(ctx context.Context, query string, n int) ([]Hit, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$search", Value: bson.M{
			"index": TextIndexName,
			"compound": bson.M{
				"must":   bson.A{bson.M{"text": bson.M{"query": query, "path": bson.A{"subject", "body", "resolution"}}}},
				"filter": bson.A{bson.M{"equals": bson.M{"path": "status", "value": "resolved"}}},
			},
		}}},
		{{Key: "$limit", Value: n}},
		{{Key: "$project", Value: bson.M{"subject": 1, "resolution": 1, "score": bson.M{"$meta": "searchScore"}}}},
	}
	return s.run(ctx, pipeline, "full-text search")
}

func (s *Store) vectorSearch(ctx context.Context, query string, n int) ([]Hit, error) {
	stage := bson.M{
		"index":         VectorIndexName,
		"numCandidates": n * 10,
		"limit":         n,
		"filter":        bson.M{"status": "resolved"},
	}
	if s.mode == ModeAuto {
		// Atlas embeds the query with the index's Voyage model.
		stage["path"], stage["query"], stage["model"] = "text", query, s.model
	} else {
		vecs, err := s.embedder.Embed(ctx, []string{query}, "query")
		if err != nil {
			return nil, fmt.Errorf("kb: embed query: %w", err)
		}
		stage["path"], stage["queryVector"] = "embedding", vecs[0]
	}
	pipeline := mongo.Pipeline{
		{{Key: "$vectorSearch", Value: stage}},
		{{Key: "$project", Value: bson.M{"subject": 1, "resolution": 1, "score": bson.M{"$meta": "vectorSearchScore"}}}},
	}
	return s.run(ctx, pipeline, "vector search")
}

func (s *Store) run(ctx context.Context, p mongo.Pipeline, what string) ([]Hit, error) {
	cur, err := s.coll.Aggregate(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("kb: %s: %w", what, err)
	}
	defer cur.Close(ctx)
	var rows []struct {
		ID         string  `bson:"_id"`
		Subject    string  `bson:"subject"`
		Resolution string  `bson:"resolution"`
		Score      float64 `bson:"score"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("kb: %s: %w", what, err)
	}
	hits := make([]Hit, len(rows))
	for i, r := range rows {
		hits[i] = Hit{ID: r.ID, Subject: r.Subject, Resolution: r.Resolution, Score: r.Score}
	}
	return hits, nil
}
