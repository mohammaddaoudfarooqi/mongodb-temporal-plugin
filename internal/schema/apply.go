package schema

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MetadataDocID is the _id of the singleton that tracks the applied
// schema version + state.
const MetadataDocID = "singleton"

// SchemaMetadataColl is the collection name holding the _schema_version
// singleton.
const SchemaMetadataColl = "_schema_version"

// State names the allowed values of _schema_version.state.
type State string

const (
	StateReady     State = "ready"
	StateUpgrading State = "upgrading" // suffixed with "-from-<A>-to-<B>"
	StateFailed    State = "failed"    // suffixed with "-from-<A>-to-<B>"
)

// Metadata is the BSON projection of the _schema_version singleton.
// JSON tags mirror BSON field names so `verify` output is uniform.
type Metadata struct {
	ID               string    `bson:"_id" json:"_id"`
	SchemaVersion    string    `bson:"schema_version" json:"schema_version"`
	State            string    `bson:"state" json:"state"`
	NumHistoryShards int32     `bson:"num_history_shards,omitempty" json:"num_history_shards,omitempty"`
	InitTime         time.Time `bson:"init_time,omitempty" json:"init_time,omitempty"`
	UpdateTime       time.Time `bson:"update_time,omitempty" json:"update_time,omitempty"`
}

// ApplyResult captures what applySchema did so the CLI can log a
// human-readable summary (collections created, indexes created, etc.).
type ApplyResult struct {
	CollectionsCreated []string
	IndexesCreated     []string
	IndexesSkipped     []string // already present with matching definition
	IndexesMismatched  []string // already present but definition differs
}

// ApplyCollections creates every collection in the manifest if it does
// not exist. Existing collections are no-ops. MongoDB
// would auto-create on first insert; making it explicit lets operators
// provision storage policies up-front.
func ApplyCollections(ctx context.Context, db *mongo.Database, c *Collections) ([]string, error) {
	if db == nil || c == nil {
		return nil, errors.New("schema: nil db or collections")
	}
	existingNames, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("schema: list collections: %w", err)
	}
	existing := map[string]bool{}
	for _, n := range existingNames {
		existing[n] = true
	}
	var created []string
	for _, coll := range c.Collections {
		if existing[coll.Name] {
			continue
		}
		if err := db.CreateCollection(ctx, coll.Name); err != nil {
			// MongoDB returns a NamespaceExists error on concurrent
			// create; treat as no-op.
			if strings.Contains(err.Error(), "already exists") ||
				strings.Contains(err.Error(), "NamespaceExists") {
				continue
			}
			return nil, fmt.Errorf("schema: create collection %s: %w", coll.Name, err)
		}
		created = append(created, coll.Name)
	}
	return created, nil
}

// ApplyIndexes issues createIndexes per manifest entry. Idempotent per
//
//   - missing index → created
//   - existing with matching definition → no-op
//   - existing with different definition → returned in Mismatched so
//     the operator can run update-schema to reconcile
func ApplyIndexes(ctx context.Context, db *mongo.Database, idx *Indexes) (*ApplyResult, error) {
	if db == nil || idx == nil {
		return nil, errors.New("schema: nil db or indexes")
	}
	res := &ApplyResult{}
	byColl := map[string][]Index{}
	for _, e := range idx.Indexes {
		byColl[e.Collection] = append(byColl[e.Collection], e)
	}
	for collName, want := range byColl {
		have, err := listIndexes(ctx, db.Collection(collName))
		if err != nil {
			return nil, err
		}
		for _, w := range want {
			switch compareIndex(w, have[w.Name]) {
			case indexAbsent:
				im, err := toIndexModel(w)
				if err != nil {
					return nil, err
				}
				_, err = db.Collection(collName).Indexes().CreateOne(ctx, im)
				if err != nil {
					return nil, fmt.Errorf("schema: create index %s.%s: %w", collName, w.Name, err)
				}
				res.IndexesCreated = append(res.IndexesCreated, collName+"."+w.Name)
			case indexMatches:
				res.IndexesSkipped = append(res.IndexesSkipped, collName+"."+w.Name)
			case indexDiffers:
				res.IndexesMismatched = append(res.IndexesMismatched, collName+"."+w.Name)
			}
		}
	}
	if len(res.IndexesMismatched) > 0 {
		return res, fmt.Errorf(
			"schema: %d indexes exist with different definitions than the manifest (%s); run update-schema to reconcile",
			len(res.IndexesMismatched), strings.Join(res.IndexesMismatched, ", "))
	}
	return res, nil
}

// --- helpers ---

// indexCompareResult enumerates the three outcomes of comparing a
// declared index with the currently-stored index (if any).
type indexCompareResult int

const (
	indexAbsent indexCompareResult = iota
	indexMatches
	indexDiffers
)

// storedIndex captures the fields of listIndexes output we compare.
type storedIndex struct {
	Name               string   `bson:"name"`
	Key                bson.D   `bson:"key"`
	Unique             bool     `bson:"unique,omitempty"`
	Sparse             bool     `bson:"sparse,omitempty"`
	ExpireAfterSeconds *int32   `bson:"expireAfterSeconds,omitempty"`
	PartialFilterExpr  bson.Raw `bson:"partialFilterExpression,omitempty"`
}

// listIndexes returns the existing indexes on a collection, keyed by name.
func listIndexes(ctx context.Context, coll *mongo.Collection) (map[string]storedIndex, error) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("schema: list indexes on %s: %w", coll.Name(), err)
	}
	defer cur.Close(ctx)
	out := map[string]storedIndex{}
	for cur.Next(ctx) {
		var s storedIndex
		if err := cur.Decode(&s); err != nil {
			return nil, err
		}
		out[s.Name] = s
	}
	return out, cur.Err()
}

// compareIndex returns matches/differs/absent. The equality check is
// structural — we compare the sorted (field, order) pairs.
func compareIndex(want Index, have storedIndex) indexCompareResult {
	if have.Name == "" {
		return indexAbsent
	}
	if len(want.Keys) != len(have.Key) {
		return indexDiffers
	}
	for i, k := range want.Keys {
		el := have.Key[i]
		if el.Key != k.Field {
			return indexDiffers
		}
		var order int
		switch v := el.Value.(type) {
		case int32:
			order = int(v)
		case int64:
			order = int(v)
		case int:
			order = v
		}
		if order != k.Order {
			return indexDiffers
		}
	}
	if want.Unique != have.Unique {
		return indexDiffers
	}
	if want.Sparse != have.Sparse {
		return indexDiffers
	}
	if (want.TTL == nil) != (have.ExpireAfterSeconds == nil) {
		return indexDiffers
	}
	if want.TTL != nil && have.ExpireAfterSeconds != nil &&
		want.TTL.ExpireAfterSeconds != *have.ExpireAfterSeconds {
		return indexDiffers
	}
	// Partial filters are compared as encoded BSON: a filter that differs
	// only in value type (string vs date) is a different index
	//.
	wantPF, err := want.PartialFilterDoc()
	if err != nil {
		return indexDiffers
	}
	if (len(wantPF) == 0) != (len(have.PartialFilterExpr) == 0) {
		return indexDiffers
	}
	if len(wantPF) > 0 {
		raw, err := bson.Marshal(wantPF)
		if err != nil || !bytes.Equal(raw, have.PartialFilterExpr) {
			return indexDiffers
		}
	}
	return indexMatches
}

// toIndexModel translates the manifest shape into the driver's IndexModel.
func toIndexModel(w Index) (mongo.IndexModel, error) {
	keys := bson.D{}
	for _, k := range w.Keys {
		keys = append(keys, bson.E{Key: k.Field, Value: k.Order})
	}
	opts := options.Index().SetName(w.Name)
	if w.Unique {
		opts = opts.SetUnique(true)
	}
	if w.Sparse {
		opts = opts.SetSparse(true)
	}
	if w.TTL != nil {
		opts = opts.SetExpireAfterSeconds(w.TTL.ExpireAfterSeconds)
	}
	pf, err := w.PartialFilterDoc()
	if err != nil {
		return mongo.IndexModel{}, err
	}
	if len(pf) > 0 {
		opts = opts.SetPartialFilterExpression(pf)
	}
	return mongo.IndexModel{Keys: keys, Options: opts}, nil
}

// ReadMetadata fetches the _schema_version singleton. Returns nil +
// nil when the singleton is absent (fresh cluster).
func ReadMetadata(ctx context.Context, db *mongo.Database) (*Metadata, error) {
	var m Metadata
	err := db.Collection(SchemaMetadataColl).FindOne(ctx, bson.M{"_id": MetadataDocID}).Decode(&m)
	if err == nil {
		return &m, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	return nil, fmt.Errorf("schema: read metadata: %w", err)
}

// WriteInitialMetadata upserts the singleton with the supplied schema
// version + numHistoryShards on first-time setup.
func WriteInitialMetadata(ctx context.Context, db *mongo.Database, schemaVersion string, numHistoryShards int32) error {
	now := time.Now().UTC()
	doc := Metadata{
		ID:               MetadataDocID,
		SchemaVersion:    schemaVersion,
		State:            string(StateReady),
		NumHistoryShards: numHistoryShards,
		InitTime:         now,
		UpdateTime:       now,
	}
	// Upsert-if-absent so concurrent setup-schema runs converge on the
	// same record.
	filter := bson.M{"_id": MetadataDocID}
	update := bson.M{"$setOnInsert": doc}
	opts := options.UpdateOne().SetUpsert(true)
	_, err := db.Collection(SchemaMetadataColl).UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("schema: write initial metadata: %w", err)
	}
	return nil
}

// SetState transitions _schema_version.state. Used by update-schema's
// state machine.
func SetState(ctx context.Context, db *mongo.Database, state string, schemaVersion string) error {
	_, err := db.Collection(SchemaMetadataColl).UpdateOne(ctx,
		bson.M{"_id": MetadataDocID},
		bson.M{"$set": bson.M{
			"state":          state,
			"schema_version": schemaVersion,
			"update_time":    time.Now().UTC(),
		}},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("schema: set state: %w", err)
	}
	return nil
}

// ReconcileIndexes applies the manifest like ApplyIndexes but replaces
// indexes whose stored definition differs (drop + create), which is
// what an upgrade between schema versions needs.
func ReconcileIndexes(ctx context.Context, db *mongo.Database, idx *Indexes) (*ApplyResult, error) {
	if db == nil || idx == nil {
		return nil, errors.New("schema: nil db or indexes")
	}
	res := &ApplyResult{}
	for _, w := range idx.Indexes {
		coll := db.Collection(w.Collection)
		have, err := listIndexes(ctx, coll)
		if err != nil {
			return nil, err
		}
		switch compareIndex(w, have[w.Name]) {
		case indexMatches:
			res.IndexesSkipped = append(res.IndexesSkipped, w.Collection+"."+w.Name)
			continue
		case indexDiffers:
			if err := coll.Indexes().DropOne(ctx, w.Name); err != nil {
				return res, fmt.Errorf("schema: drop index %s.%s: %w", w.Collection, w.Name, err)
			}
			res.IndexesMismatched = append(res.IndexesMismatched, w.Collection+"."+w.Name)
		}
		im, err := toIndexModel(w)
		if err != nil {
			return res, err
		}
		if _, err := coll.Indexes().CreateOne(ctx, im); err != nil {
			return res, fmt.Errorf("schema: create index %s.%s: %w", w.Collection, w.Name, err)
		}
		res.IndexesCreated = append(res.IndexesCreated, w.Collection+"."+w.Name)
	}
	return res, nil
}

// Upgrade moves a ready cluster to the manifest's schema version: it
// refuses downgrades, creates missing collections, reconciles indexes,
// and records the new version. The metadata state passes through
// upgrading-from-A-to-B and ends ready, or failed-from-A-to-B on error.
func Upgrade(ctx context.Context, db *mongo.Database, c *Collections, idx *Indexes) (*ApplyResult, error) {
	meta, err := ReadMetadata(ctx, db)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, errors.New("schema: _schema_version absent; run setup-schema first")
	}
	from, to := meta.SchemaVersion, idx.SchemaVersion
	cmp, err := CompareVersions(from, to)
	if err != nil {
		return nil, err
	}
	switch {
	case cmp > 0:
		return nil, fmt.Errorf("schema: refusing downgrade from %s to %s", from, to)
	case cmp == 0 && meta.State == string(StateReady):
		return &ApplyResult{}, nil
	}
	if err := SetState(ctx, db, fmt.Sprintf("%s-from-%s-to-%s", StateUpgrading, from, to), from); err != nil {
		return nil, err
	}
	fail := func(err error) (*ApplyResult, error) {
		_ = SetState(ctx, db, fmt.Sprintf("%s-from-%s-to-%s", StateFailed, from, to), from)
		return nil, err
	}
	res := &ApplyResult{}
	if c != nil {
		created, err := ApplyCollections(ctx, db, c)
		if err != nil {
			return fail(err)
		}
		res.CollectionsCreated = created
	}
	ir, err := ReconcileIndexes(ctx, db, idx)
	if err != nil {
		return fail(err)
	}
	res.IndexesCreated, res.IndexesSkipped, res.IndexesMismatched = ir.IndexesCreated, ir.IndexesSkipped, ir.IndexesMismatched
	if err := SetState(ctx, db, string(StateReady), to); err != nil {
		return nil, err
	}
	return res, nil
}
