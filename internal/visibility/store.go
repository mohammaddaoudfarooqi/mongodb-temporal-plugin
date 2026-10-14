// Package visibility is the MongoDB-backed implementation of
// store.VisibilityStore.
//
// The store supports two query modes selectable at configuration time
// :
//   - Aggregation mode (Mode=ModeAggregation): predicates compile to
//     BSON filters applied via the MongoDB Aggregation $match stage.
//     Works on every MongoDB deployment from 6.0+.
//   - Atlas Search mode (Mode=ModeAtlasSearch): predicates compile to
//     $search compound clauses.
//
// Collection: visibility_executions, keyed on "<ns>:<run>".
package visibility

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"

	pluginmetrics "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/observe"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility/converter"
)

// Mode selects which query backend the visibility store uses.
type Mode int

const (
	ModeAggregation Mode = iota + 1
	ModeAtlasSearch
)

const (
	executionsColl   = "visibility_executions"
	saMetadataColl   = "visibility_search_attributes"
	defaultIndexName = "visibility_executions_default"
)

// Store is the MongoDB-backed store.VisibilityStore.
type Store struct {
	db             *mongo.Database
	mode           Mode
	atlasSearchIdx string
	indexName      string // search-attribute index key; see GetIndexName
	saProvider     searchattribute.Provider
	saMapper       searchattribute.Mapper
	mapperProvider searchattribute.MapperProvider
	bsonConverter  *converter.BSONConverter
	indexTracker   *IndexTracker
	emitter        *pluginmetrics.Emitter // nil-safe
	closeClient    func()                 // set when the store owns its client
	metricsHandler metrics.Handler        // handed to upstream's query converter (1.32+)
	logger         log.Logger
}

// New constructs a live visibility store. atlasSearchIdx is only
// consulted when mode == ModeAtlasSearch. saProvider and saMapper are
// optional — without them, queries with custom SAs degrade to
// an InvalidArgument so the caller knows to supply registered SAs.
func New(db *mongo.Database, mode Mode, atlasSearchIdx string, opts ...Option) *Store {
	if mode == 0 {
		mode = ModeAggregation
	}
	s := &Store{
		db:             db,
		mode:           mode,
		atlasSearchIdx: atlasSearchIdx,
		indexName:      defaultIndexName,
		bsonConverter:  converter.NewBSONConverter(),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Option configures optional Store dependencies.
type Option func(*Store)

// WithMetrics injects the per-store metrics emitter.
// The store's observeOp helper is nil-safe; this option just pre-
// scopes the emitter with the "visibility" store tag so callers see
// persistence_mongo_op_*{store="visibility",operation="…"} series.
func WithMetrics(em *pluginmetrics.Emitter) Option {
	return func(s *Store) {
		if em != nil {
			s.emitter = em.ForStore("visibility")
		}
	}
}

// observeOp wraps observe.Op with this store's emitter.
func (s *Store) observeOp(op string) func(*error) {
	return observe.Op(s.emitter, op)
}

// WithSearchAttributeProvider injects the upstream SA provider so the
// DSL converter can resolve custom SA types.
func WithSearchAttributeProvider(p searchattribute.Provider) Option {
	return func(s *Store) { s.saProvider = p }
}

// WithSearchAttributeMapper injects a fixed name-alias mapper. Prefer
// WithSearchAttributeMapperProvider, which resolves per namespace.
func WithSearchAttributeMapper(m searchattribute.Mapper) Option {
	return func(s *Store) { s.saMapper = m }
}

// WithSearchAttributeMapperProvider injects the per-namespace
// search-attribute mapper provider the server hands the factory. The
// upstream query converter dereferences the mapper for every custom
// attribute alias.
func WithSearchAttributeMapperProvider(p searchattribute.MapperProvider) Option {
	return func(s *Store) { s.mapperProvider = p }
}

// WithTemporalObservability passes the server's metrics handler and
// logger through to upstream components that take them.
func WithTemporalObservability(h metrics.Handler, l log.Logger) Option {
	return func(s *Store) { s.metricsHandler, s.logger = h, l }
}

// withClientCloser hands ownership of the store's MongoDB client to the
// store so Close releases it.
func withClientCloser(closeFn func()) Option {
	return func(s *Store) { s.closeClient = closeFn }
}

// WithIndexName sets the name under which the server keys this store's
// search attributes. The factory passes CustomDataStoreConfig.IndexName,
// the key the server seeds pre-allocated custom slots under
// .
func WithIndexName(name string) Option {
	return func(s *Store) { s.indexName = name }
}

// WithIndexTracker injects a pre-built IndexTracker. When set, the
// store routes reads through it so queries against a not-yet-active
// Atlas Search index surface Unavailable instead of empty results
// .
func WithIndexTracker(t *IndexTracker) Option {
	return func(s *Store) { s.indexTracker = t }
}

// Close stops the index tracker and, when the store owns its client
// (factory-built stores), disconnects it.
func (s *Store) Close() {
	if s.indexTracker != nil {
		s.indexTracker.Stop()
	}
	if s.closeClient != nil {
		s.closeClient()
	}
}

// GetName is used by the upstream telemetry wrapper.
func (s *Store) GetName() string { return "mongodb-visibility" }

// GetIndexName is the key the server's search attribute manager uses
// for this store; it must equal the datastore's configured indexName
// or custom search attributes cannot be registered.
func (s *Store) GetIndexName() string { return s.indexName }

// ValidateCustomSearchAttributes is a passthrough until a follow-up wires in
// the typed SA metadata store. The upstream manager calls this before
// every write; returning the input unchanged permits writes while
// still recording the attribute set in visibility_search_attributes
// .
func (s *Store) ValidateCustomSearchAttributes(attrs map[string]any) (map[string]any, error) {
	return attrs, nil
}

// RecordWorkflowExecutionStarted upserts an open-execution visibility
// document.
func (s *Store) RecordWorkflowExecutionStarted(
	ctx context.Context,
	req *store.InternalRecordWorkflowExecutionStartedRequest,
) (retErr error) {
	defer s.observeOp("RecordWorkflowExecutionStarted")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("visibility: uninitialised or nil request")
	}
	doc := BaseFromStartedRequest(req)
	s.typeUntypedSearchAttributes(doc, req.SearchAttributes)
	return s.upsert(ctx, doc)
}

// RecordWorkflowExecutionClosed updates terminal fields atomically
// .
func (s *Store) RecordWorkflowExecutionClosed(
	ctx context.Context,
	req *store.InternalRecordWorkflowExecutionClosedRequest,
) (retErr error) {
	defer s.observeOp("RecordWorkflowExecutionClosed")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("visibility: uninitialised or nil request")
	}
	doc := BaseFromClosedRequest(req)
	s.typeUntypedSearchAttributes(doc, req.SearchAttributes)
	return s.upsert(ctx, doc)
}

// UpsertWorkflowExecution handles at-least-once visibility-task
// delivery by tolerating duplicate calls.
func (s *Store) UpsertWorkflowExecution(
	ctx context.Context,
	req *store.InternalUpsertWorkflowExecutionRequest,
) (retErr error) {
	defer s.observeOp("UpsertWorkflowExecution")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("visibility: uninitialised or nil request")
	}
	doc := BaseFromUpsertRequest(req)
	s.typeUntypedSearchAttributes(doc, req.SearchAttributes)
	return s.upsert(ctx, doc)
}

// DeleteWorkflowExecution removes the visibility document; absent
// documents return nil.
func (s *Store) DeleteWorkflowExecution(
	ctx context.Context,
	req *manager.VisibilityDeleteWorkflowExecutionRequest,
) (retErr error) {
	defer s.observeOp("DeleteWorkflowExecution")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("visibility: uninitialised or nil request")
	}
	_, err := s.db.Collection(executionsColl).DeleteOne(ctx,
		bson.M{"_id": DocID(req.NamespaceID.String(), req.RunID)})
	if err != nil {
		return internalmongo.Translate(err, "visibility.DeleteWorkflowExecution")
	}
	return nil
}

// GetWorkflowExecution returns the visibility doc for (ns, run_id) or
// NotFound.
func (s *Store) GetWorkflowExecution(
	ctx context.Context,
	req *manager.GetWorkflowExecutionRequest,
) (_ *store.InternalGetWorkflowExecutionResponse, retErr error) {
	defer s.observeOp("GetWorkflowExecution")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("visibility: uninitialised or nil request")
	}
	var doc Doc
	err := s.db.Collection(executionsColl).FindOne(ctx,
		bson.M{"_id": DocID(req.NamespaceID.String(), req.RunID)}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFound(fmt.Sprintf(
				"visibility record ns=%s run=%s not found", req.NamespaceID, req.RunID))
		}
		return nil, internalmongo.Translate(err, "visibility.GetWorkflowExecution")
	}
	return &store.InternalGetWorkflowExecutionResponse{
		Execution: docToExecutionInfo(&doc),
	}, nil
}

// upsert centralizes the {_id}-filtered $set write used by every
// Record/Upsert method. Single-document atomicity is sufficient
// (no multi-doc txn needed on visibility writes).
func (s *Store) upsert(ctx context.Context, doc *Doc) error {
	if doc == nil {
		return nil
	}
	_, err := s.db.Collection(executionsColl).UpdateOne(ctx,
		bson.M{"_id": doc.ID},
		bson.M{"$set": doc},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return internalmongo.Translate(err, "visibility.upsert")
	}
	return nil
}

// docToExecutionInfo projects a Doc back into InternalExecutionInfo
// for read-side responses.
func docToExecutionInfo(d *Doc) *store.InternalExecutionInfo {
	if d == nil {
		return nil
	}
	info := &store.InternalExecutionInfo{
		WorkflowID:           d.WorkflowID,
		RunID:                d.RunID,
		TypeName:             d.WorkflowTypeName,
		StartTime:            d.StartTime,
		ExecutionTime:        d.ExecutionTime,
		CloseTime:            d.CloseTime,
		ExecutionDuration:    time.Duration(d.ExecutionDuration),
		Status:               enumspb.WorkflowExecutionStatus(d.Status),
		HistoryLength:        d.HistoryLength,
		HistorySizeBytes:     d.HistorySizeBytes,
		StateTransitionCount: d.StateTransitionCount,
		TaskQueue:            d.TaskQueue,
		ParentWorkflowID:     d.ParentWorkflowID,
		ParentRunID:          d.ParentRunID,
		RootWorkflowID:       d.RootWorkflowID,
		RootRunID:            d.RootRunID,
	}
	if d.Memo != nil {
		info.Memo = internalmongo.DataBlobFromBlob(d.Memo)
	}
	info.SearchAttributes = decodeSearchAttributes(d.SearchAttributesEncoded)
	return info
}

// ListWorkflowExecutions / ScanWorkflowExecutions / CountWorkflowExecutions
// share infrastructure with the DSL converter; here we leave
// stubs that return an empty response so the manager layer can wire
// the plugin into place without panicking. The full query path lands
// in subsequent tasks.

// ListWorkflowExecutions translates the request's Query string into the
// mode-specific filter (BSON $match for Aggregation, $search compound
// for Atlas Search) and returns a paginated slice.
func (s *Store) ListWorkflowExecutions(
	ctx context.Context,
	req *manager.ListWorkflowExecutionsRequestV2,
) (_ *store.InternalListExecutionsResponse, retErr error) {
	defer s.observeOp("ListWorkflowExecutions")(&retErr)
	if s.db == nil {
		return nil, errors.New("visibility: uninitialised")
	}
	if s.mode == ModeAtlasSearch {
		return s.listAtlasSearch(ctx, req)
	}
	return s.listAggregation(ctx, req)
}

// listAggregation runs the Aggregation-mode List path: ORDER BY (default
// newest first) with keyset pagination on the sort values plus _id
// .
func (s *Store) listAggregation(
	ctx context.Context,
	req *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListExecutionsResponse, error) {
	cq, err := s.compile(req.Namespace, req.Query, chasm.UnspecifiedArchetypeID, s.bsonConverter)
	if err != nil {
		return nil, err
	}
	return s.findPage(ctx, req.NamespaceID.String(), cq, nil, req.PageSize, req.NextPageToken,
		"visibility.ListWorkflowExecutions")
}

// findPage runs one keyset-paginated Find for an Aggregation-mode query.
func (s *Store) findPage(
	ctx context.Context,
	namespaceID string,
	cq *compiledQuery,
	extra bson.M,
	pageSize int,
	token []byte,
	op string,
) (*store.InternalListExecutionsResponse, error) {
	clauses := []bson.M{{"namespace_id": namespaceID}}
	if len(cq.expr) > 0 {
		clauses = append(clauses, cq.expr)
	}
	if len(extra) > 0 {
		clauses = append(clauses, extra)
	}
	tok, err := decodeToken(token)
	if err != nil {
		return nil, err
	}
	switch {
	case len(tok.Cursor) > 0:
		last, err := decodeCursor(tok.Cursor, len(cq.sort))
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, keysetAfter(cq.sort, last))
	case tok.ID != "":
		// Tokens minted before ORDER BY support resumed on _id.
		clauses = append(clauses, bson.M{"_id": bson.M{"$gt": tok.ID}})
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	cursor, err := s.db.Collection(executionsColl).Find(ctx, bson.M{"$and": clauses},
		options.Find().SetSort(sortDoc(cq.sort)).SetLimit(int64(pageSize)+1))
	if err != nil {
		return nil, internalmongo.Translate(err, op)
	}
	defer cursor.Close(ctx)
	var raws []bson.Raw
	if err := cursor.All(ctx, &raws); err != nil {
		return nil, internalmongo.Translate(err, op+".decode")
	}
	resp := &store.InternalListExecutionsResponse{}
	for i, raw := range raws {
		if i == pageSize {
			c, err := encodeCursor(sortValues(raws[pageSize-1], cq.sort))
			if err != nil {
				return nil, serviceerror.NewInternal(fmt.Sprintf("%s: encode page token: %v", op, err))
			}
			resp.NextPageToken = encodeToken(&pageToken{ID: "-", Cursor: c})
			break
		}
		var d Doc
		if err := bson.Unmarshal(raw, &d); err != nil {
			return nil, internalmongo.Translate(err, op+".decode")
		}
		resp.Executions = append(resp.Executions, docToExecutionInfo(&d))
	}
	return resp, nil
}

// listAtlasSearch runs the Atlas-Search List path, paginating with a
// searchAfter cursor on the _id tiebreaker.
// While the Atlas Search index is not yet active, reads surface
// *serviceerror.Unavailable so callers know to retry; writes still
// land.
func (s *Store) listAtlasSearch(
	ctx context.Context,
	req *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListExecutionsResponse, error) {
	if s.indexTracker != nil && !s.indexTracker.IsReady() {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(
			"visibility: Atlas Search index not yet active (status=%s)",
			s.indexTracker.LastStatus()))
	}
	indexName := s.atlasSearchIdx
	if indexName == "" {
		indexName = "visibility_executions_search"
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	search, err := s.buildSearchStage(indexName, req.NamespaceID.String(), req.Namespace, req.Query)
	if err != nil {
		return nil, err
	}
	tok, err := decodeToken(req.NextPageToken)
	if err != nil {
		return nil, err
	}
	// Pagination strategy (backlog B-4): we use Atlas Search's native
	// `searchAfter` cursor. Each $search stage returns documents with a
	// `searchSequenceToken` in result metadata; we capture that via
	// {$meta: "searchSequenceToken"} and ferry it verbatim in the next
	// pageToken.SearchToken. This is Search-optimised pagination — the
	// index tracks the cursor server-side, so deep pages do not pay
	// linear-scan cost the post-$search $match fallback would.
	//
	// If the incoming token has no SearchToken (first page, or tokens
	// minted before B-4), we fall back to the post-$search $match on
	// _id > tok.ID so upgrades are compatible.
	if tok.SearchToken != "" {
		search["searchAfter"] = tok.SearchToken
	}
	pipeline := mongo.Pipeline{
		{{Key: "$search", Value: search}},
	}
	if tok.SearchToken == "" && tok.ID != "" {
		pipeline = append(pipeline, bson.D{
			{Key: "$match", Value: bson.M{"_id": bson.M{"$gt": tok.ID}}},
		})
	}
	pipeline = append(pipeline,
		bson.D{{Key: "$addFields", Value: bson.M{
			"__pag_token": bson.M{"$meta": "searchSequenceToken"},
		}}},
		bson.D{{Key: "$limit", Value: int64(pageSize) + 1}},
	)
	cursor, err := searchCollection(s.db, executionsColl).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, internalmongo.Translate(err, "visibility.listAtlasSearch")
	}
	defer cursor.Close(ctx)
	type docWithToken struct {
		Doc      `bson:",inline"`
		PagToken string `bson:"__pag_token,omitempty"`
	}
	var rows []docWithToken
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, internalmongo.Translate(err, "visibility.listAtlasSearch.decode")
	}
	resp := &store.InternalListExecutionsResponse{}
	for i, r := range rows {
		if i == pageSize {
			// The cursor is the last *returned* (pageSize-th) document,
			// not the lookahead row. That keeps the next page aligned
			// with the server-side cursor semantics.
			last := rows[pageSize-1]
			resp.NextPageToken = encodeToken(&pageToken{
				ID:          last.ID,
				SearchToken: last.PagToken,
			})
			break
		}
		resp.Executions = append(resp.Executions, docToExecutionInfo(&r.Doc))
	}
	return resp, nil
}

// buildSearchStage assembles the $search stage body: index + compound
// clause scoped by namespace_id + DSL-produced predicate (if any), sorted
// by the query's ORDER BY (default newest first) with an _id tie-breaker
// so searchAfter resumes deterministically.
func (s *Store) buildSearchStage(indexName, namespaceID string, nsName namespace.Name, queryStr string) (bson.M, error) {
	cq, err := s.compile(nsName, queryStr, chasm.UnspecifiedArchetypeID, converter.NewSearchConverter())
	if err != nil {
		return nil, err
	}
	must := []bson.M{{"equals": bson.M{"path": "namespace_id", "value": namespaceID}}}
	if len(cq.expr) > 0 {
		must = append(must, cq.expr)
	}
	return bson.M{
		"index":    indexName,
		"sort":     sortDoc(cq.sort),
		"compound": bson.M{"must": must},
	}, nil
}

// CountWorkflowExecutions: in Aggregation mode it uses
// CountDocuments for exact counts; in Atlas Search mode it runs a
// $searchMeta aggregation with the `count: {type: "total"}` facet.
func (s *Store) CountWorkflowExecutions(
	ctx context.Context,
	req *manager.CountWorkflowExecutionsRequest,
) (_ *store.InternalCountExecutionsResponse, retErr error) {
	defer s.observeOp("CountWorkflowExecutions")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("visibility: uninitialised or nil request")
	}
	if s.mode == ModeAtlasSearch {
		return s.countAtlasSearch(ctx, req)
	}
	cq, err := s.compile(req.Namespace, req.Query, chasm.UnspecifiedArchetypeID, s.bsonConverter)
	if err != nil {
		return nil, err
	}
	clauses := []bson.M{{"namespace_id": req.NamespaceID.String()}}
	if len(cq.expr) > 0 {
		clauses = append(clauses, cq.expr)
	}
	filter := bson.M{"$and": clauses}
	if cq.groupBy != "" {
		return s.countGroups(ctx, s.db.Collection(executionsColl), mongo.Pipeline{{{Key: "$match", Value: filter}}}, cq.groupBy)
	}
	n, err := s.db.Collection(executionsColl).CountDocuments(ctx, filter)
	if err != nil {
		return nil, internalmongo.Translate(err, "visibility.CountWorkflowExecutions")
	}
	return &store.InternalCountExecutionsResponse{Count: n}, nil
}

// countGroups appends a $group on path to pipeline and returns one
// aggregation group per distinct value (GROUP BY).
func (s *Store) countGroups(ctx context.Context, coll *mongo.Collection, pipeline mongo.Pipeline, path string) (*store.InternalCountExecutionsResponse, error) {
	pipeline = append(pipeline,
		bson.D{{Key: "$group", Value: bson.M{"_id": "$" + path, "count": bson.M{"$sum": 1}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "count", Value: -1}}}},
	)
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, internalmongo.Translate(err, "visibility.CountWorkflowExecutions.group")
	}
	defer cursor.Close(ctx)
	var rows []struct {
		Value any   `bson:"_id"`
		Count int64 `bson:"count"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, internalmongo.Translate(err, "visibility.CountWorkflowExecutions.group.decode")
	}
	resp := &store.InternalCountExecutionsResponse{}
	for _, r := range rows {
		value := r.Value
		if path == "status" {
			value = enumspb.WorkflowExecutionStatus(toInt32(r.Value)).String()
		}
		p, err := payload.Encode(value)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("visibility: encode group value: %v", err))
		}
		resp.Groups = append(resp.Groups, store.InternalAggregationGroup{GroupValues: []*commonpb.Payload{p}, Count: r.Count})
		resp.Count += r.Count
	}
	return resp, nil
}

func toInt32(v any) int32 {
	switch n := v.(type) {
	case int32:
		return n
	case int64:
		return int32(n)
	case float64:
		return int32(n)
	}
	return 0
}

// countAtlasSearch runs the $searchMeta count path. Like the
// list path, reads gate on index readiness.
func (s *Store) countAtlasSearch(
	ctx context.Context,
	req *manager.CountWorkflowExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	if s.indexTracker != nil && !s.indexTracker.IsReady() {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(
			"visibility: Atlas Search index not yet active (status=%s)",
			s.indexTracker.LastStatus()))
	}
	indexName := s.atlasSearchIdx
	if indexName == "" {
		indexName = "visibility_executions_search"
	}
	cq, err := s.compile(req.Namespace, req.Query, chasm.UnspecifiedArchetypeID, converter.NewSearchConverter())
	if err != nil {
		return nil, err
	}
	searchStage, err := s.buildSearchStage(indexName, req.NamespaceID.String(), req.Namespace, req.Query)
	if err != nil {
		return nil, err
	}
	if cq.groupBy != "" {
		delete(searchStage, "sort")
		return s.countGroups(ctx, searchCollection(s.db, executionsColl), mongo.Pipeline{{{Key: "$search", Value: searchStage}}}, cq.groupBy)
	}
	// $searchMeta replaces $search + $count with a single meta query
	// that returns {count: {total|lowerBound: <n>}}.
	searchStage["count"] = bson.M{"type": "total"}
	// The sort key is irrelevant for a meta count; drop it to keep the
	// query plan simple.
	delete(searchStage, "sort")

	pipeline := mongo.Pipeline{
		{{Key: "$searchMeta", Value: searchStage}},
	}
	cursor, err := searchCollection(s.db, executionsColl).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, internalmongo.Translate(err, "visibility.countAtlasSearch")
	}
	defer cursor.Close(ctx)
	var results []struct {
		Count struct {
			Total      int64 `bson:"total"`
			LowerBound int64 `bson:"lowerBound"`
		} `bson:"count"`
	}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, internalmongo.Translate(err, "visibility.countAtlasSearch.decode")
	}
	resp := &store.InternalCountExecutionsResponse{}
	if len(results) > 0 {
		if results[0].Count.Total > 0 {
			resp.Count = results[0].Count.Total
		} else {
			resp.Count = results[0].Count.LowerBound
		}
	}
	return resp, nil
}

// ListChasmExecutions / CountChasmExecutions — scoped by ArchetypeID.
// Behavior:
//   - Aggregation mode: translate req.Query to a $match and AND it with
//     {namespace_id, archetype_id} + paginate on _id.
//   - Atlas Search mode: translate req.Query to $search compound and
//     AND a namespace_id + archetype_id equality clause. Gate on index
//     readiness.
//
// The archetype_id comes from the reserved ArchetypeId search attribute
// stamped by CHASM writers (see document.baseFromVisibilityRequestBase).
func (s *Store) ListChasmExecutions(
	ctx context.Context,
	req *visibilityservice.ListChasmExecutionsRequest,
) (_ *store.InternalListExecutionsResponse, retErr error) {
	defer s.observeOp("ListChasmExecutions")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("visibility: uninitialised or nil request")
	}
	return s.listChasmAggregation(ctx, req)
}

func (s *Store) listChasmAggregation(
	ctx context.Context,
	req *visibilityservice.ListChasmExecutionsRequest,
) (*store.InternalListExecutionsResponse, error) {
	cq, err := s.compile(namespace.Name(req.Namespace), req.Query, chasm.ArchetypeID(req.ArchetypeId), s.bsonConverter)
	if err != nil {
		return nil, err
	}
	return s.findPage(ctx, req.NamespaceId, cq, bson.M{"archetype_id": req.ArchetypeId},
		int(req.PageSize), req.NextPageToken, "visibility.ListChasmExecutions")
}

func (s *Store) CountChasmExecutions(
	ctx context.Context,
	req *visibilityservice.CountChasmExecutionsRequest,
) (_ *store.InternalCountExecutionsResponse, retErr error) {
	defer s.observeOp("CountChasmExecutions")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("visibility: uninitialised or nil request")
	}
	cq, err := s.compile(namespace.Name(req.Namespace), req.Query, chasm.ArchetypeID(req.ArchetypeId), s.bsonConverter)
	if err != nil {
		return nil, err
	}
	clauses := []bson.M{{"namespace_id": req.NamespaceId}, {"archetype_id": req.ArchetypeId}}
	if len(cq.expr) > 0 {
		clauses = append(clauses, cq.expr)
	}
	filter := bson.M{"$and": clauses}
	if cq.groupBy != "" {
		return s.countGroups(ctx, s.db.Collection(executionsColl), mongo.Pipeline{{{Key: "$match", Value: filter}}}, cq.groupBy)
	}
	n, err := s.db.Collection(executionsColl).CountDocuments(ctx, filter)
	if err != nil {
		return nil, internalmongo.Translate(err, "visibility.CountChasmExecutions")
	}
	return &store.InternalCountExecutionsResponse{Count: n}, nil
}

// AddSearchAttributes records custom SA name→type entries in the
// metadata collection.
func (s *Store) AddSearchAttributes(
	ctx context.Context,
	req *manager.AddSearchAttributesRequest,
) (retErr error) {
	defer s.observeOp("AddSearchAttributes")(&retErr)
	if s.db == nil || req == nil || len(req.SearchAttributes) == 0 {
		return nil
	}
	docs := make([]any, 0, len(req.SearchAttributes))
	for name, ivt := range req.SearchAttributes {
		docs = append(docs, bson.M{
			"_id":  name,
			"name": name,
			"type": int32(ivt),
		})
	}
	_, err := s.db.Collection(saMetadataColl).InsertMany(ctx, docs,
		options.InsertMany().SetOrdered(false))
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return internalmongo.Translate(err, "visibility.AddSearchAttributes")
	}
	return nil
}

// EnsureIndexes installs the standard visibility indexes. Only a handful of ORDER BY fields are indexed by default; the rest
// require an explicit index creation at setup-schema time.
func EnsureIndexes(ctx context.Context, db *mongo.Database) error {
	_, err := db.Collection(executionsColl).Indexes().CreateMany(ctx, []mongo.IndexModel{
		// Namespace-first compound index for every list query.
		{
			Keys:    bson.D{{Key: "namespace_id", Value: 1}, {Key: "_id", Value: 1}},
			Options: options.Index().SetName("visibility_ns_id"),
		},
		// Namespace + start_time for the most common ORDER BY.
		{
			Keys:    bson.D{{Key: "namespace_id", Value: 1}, {Key: "start_time", Value: -1}},
			Options: options.Index().SetName("visibility_ns_starttime"),
		},
		// Namespace + close_time for closed-workflow scans.
		{
			Keys:    bson.D{{Key: "namespace_id", Value: 1}, {Key: "close_time", Value: -1}},
			Options: options.Index().SetName("visibility_ns_closetime"),
		},
		// Namespace + status for open/closed split queries.
		{
			Keys:    bson.D{{Key: "namespace_id", Value: 1}, {Key: "status", Value: 1}, {Key: "start_time", Value: -1}},
			Options: options.Index().SetName("visibility_ns_status_start"),
		},
	})
	if err != nil {
		return internalmongo.Translate(err, "visibility.EnsureIndexes")
	}
	return nil
}
