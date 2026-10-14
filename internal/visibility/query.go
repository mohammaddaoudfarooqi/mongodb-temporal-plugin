package visibility

import (
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/security"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility/converter"
)

// sortKey is one ORDER BY term on a visibility-document path.
type sortKey struct {
	path string
	desc bool
}

// compiledQuery is a visibility query lowered to MongoDB terms.
type compiledQuery struct {
	expr    bson.M    // predicate (namespace scoping excluded), may be nil
	sort    []sortKey // always ends with the _id tie-breaker
	groupBy string    // document path for GROUP BY, or ""
}

// defaultSort orders newest executions first.
var defaultSort = []sortKey{{path: "start_time", desc: true}}

// compile parses a visibility query with upstream's QueryConverter using
// the per-namespace search-attribute mapper. archetypeID is
// non-zero for CHASM requests. storeQC picks the Aggregation or Atlas
// Search expression builder.
func (s *Store) compile(
	nsName namespace.Name,
	queryStr string,
	archetypeID chasm.ArchetypeID,
	storeQC query.StoreQueryConverter[bson.M],
) (*compiledQuery, error) {
	out := &compiledQuery{}
	if strings.TrimSpace(queryStr) == "" || s.saProvider == nil {
		if strings.TrimSpace(queryStr) != "" {
			return nil, serviceerror.NewInvalidArgument(
				"visibility: DSL query requires a registered search-attribute provider; factory-level wiring missing")
		}
		// No query: apply the same namespace-division default upstream's
		// converter would, so internal executions stay hidden
		//. CHASM requests filter by archetype instead.
		if archetypeID == chasm.UnspecifiedArchetypeID {
			out.expr = divisionAbsent(storeQC)
		}
		out.sort = withTieBreaker(defaultSort)
		return out, nil
	}

	// Search attributes are registered under the store's index name
	//.
	saMap, err := s.saProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return nil, internalmongo.Translate(err, "visibility.compile.saProvider")
	}
	mapper, err := s.mapperFor(nsName)
	if err != nil {
		return nil, err
	}
	qc := storeQC
	if archetypeID != chasm.UnspecifiedArchetypeID {
		qc = &chasmDivision{StoreQueryConverter: storeQC, archetype: strconv.Itoa(int(archetypeID))}
	}
	conv, err := converter.NewQueryConverter(qc, nsName, saMap, mapper, s.metricsHandler, s.logger)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	if archetypeID != chasm.UnspecifiedArchetypeID {
		conv = conv.WithArchetypeID(archetypeID)
	}
	params, err := conv.Convert(queryStr)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("visibility: invalid query: %v", err))
	}
	// Defense-in-depth against JS operators.
	if err := security.RejectJSFilter(params.QueryExpr); err != nil {
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}
	out.expr = params.QueryExpr

	var keys []sortKey
	for _, o := range params.OrderBy {
		col, ok := o.Expr.(*query.SAColumn)
		if !ok {
			return nil, serviceerror.NewInvalidArgument("visibility: unsupported ORDER BY expression")
		}
		keys = append(keys, sortKey{path: converter.FieldPath(col.FieldName), desc: o.Direction == "desc"})
	}
	if len(keys) == 0 {
		keys = defaultSort
	}
	out.sort = withTieBreaker(keys)
	if len(params.GroupBy) > 0 {
		out.groupBy = converter.FieldPath(params.GroupBy[0].FieldName)
	}
	return out, nil
}

// mapperFor returns the search-attribute mapper for a namespace.
func (s *Store) mapperFor(nsName namespace.Name) (searchattribute.Mapper, error) {
	if s.mapperProvider == nil {
		return s.saMapper, nil
	}
	m, err := s.mapperProvider.GetMapper(nsName)
	if err != nil {
		return nil, internalmongo.Translate(err, "visibility.mapper")
	}
	return m, nil
}

// divisionAbsent mirrors upstream's default "TemporalNamespaceDivision
// IS NULL" for the given expression builder.
func divisionAbsent(qc query.StoreQueryConverter[bson.M]) bson.M {
	col := query.NamespaceDivisionSAColumn()
	expr, _ := qc.ConvertIsExpr("is null", col)
	return expr
}

// chasmDivision drops the archetype's namespace-division equality that
// upstream adds for CHASM queries: the store filters CHASM executions by
// the indexed archetype_id instead, which also covers records whose
// archetype came from the ArchetypeId attribute.
type chasmDivision struct {
	query.StoreQueryConverter[bson.M]
	archetype string
}

func (c *chasmDivision) ConvertComparisonExpr(op string, col *query.SAColumn, value any) (bson.M, error) {
	if col.FieldName == sadefs.TemporalNamespaceDivision && op == "=" && value == c.archetype {
		return nil, nil
	}
	return c.StoreQueryConverter.ConvertComparisonExpr(op, col, value)
}

func (c *chasmDivision) ConvertKeywordComparisonExpr(op string, col *query.SAColumn, value any) (bson.M, error) {
	if col.FieldName == sadefs.TemporalNamespaceDivision && op == "=" && value == c.archetype {
		return nil, nil
	}
	return c.StoreQueryConverter.ConvertKeywordComparisonExpr(op, col, value)
}

func withTieBreaker(keys []sortKey) []sortKey {
	out := make([]sortKey, 0, len(keys)+1)
	for _, k := range keys {
		if k.path != "_id" {
			out = append(out, k)
		}
	}
	return append(out, sortKey{path: "_id"})
}

// sortDoc renders the sort keys for Find / $search.
func sortDoc(keys []sortKey) bson.D {
	d := make(bson.D, 0, len(keys))
	for _, k := range keys {
		dir := 1
		if k.desc {
			dir = -1
		}
		d = append(d, bson.E{Key: k.path, Value: dir})
	}
	return d
}

// sortValues extracts the sort-key values of a returned document.
func sortValues(raw bson.Raw, keys []sortKey) bson.A {
	vals := make(bson.A, 0, len(keys))
	for _, k := range keys {
		rv, err := raw.LookupErr(strings.Split(k.path, ".")...)
		if err != nil {
			vals = append(vals, nil)
			continue
		}
		vals = append(vals, rv)
	}
	return vals
}

// keysetAfter returns the predicate selecting documents strictly after
// the row whose sort values are last, honouring each key's direction.
// MongoDB sorts missing/null lowest: first in ascending order, last in
// descending order.
func keysetAfter(keys []sortKey, last bson.A) bson.M {
	var ors []any
	for i, k := range keys {
		var and []any
		for j := 0; j < i; j++ {
			and = append(and, equalTo(keys[j].path, last[j]))
		}
		after := afterValue(k, last[i])
		if after == nil {
			continue
		}
		and = append(and, after)
		ors = append(ors, bson.M{"$and": and})
	}
	if len(ors) == 0 {
		return bson.M{"_id": bson.M{"$exists": false}} // nothing after
	}
	return bson.M{"$or": ors}
}

func equalTo(path string, v any) bson.M {
	if v == nil {
		return bson.M{path: nil}
	}
	return bson.M{path: v}
}

func afterValue(k sortKey, v any) bson.M {
	switch {
	case v == nil && k.desc:
		return nil // nulls are last in descending order
	case v == nil:
		return bson.M{k.path: bson.M{"$ne": nil}}
	case k.desc:
		return bson.M{"$or": bson.A{bson.M{k.path: bson.M{"$lt": v}}, bson.M{k.path: nil}}}
	default:
		return bson.M{k.path: bson.M{"$gt": v}}
	}
}

// listCursor is the Aggregation-mode page token payload: the sort-key
// values of the last row returned.
type listCursor struct {
	Values bson.A `bson:"v"`
}

func encodeCursor(vals bson.A) ([]byte, error) {
	return bson.Marshal(listCursor{Values: vals})
}

func decodeCursor(raw []byte, n int) (bson.A, error) {
	var c listCursor
	if err := bson.Unmarshal(raw, &c); err != nil || len(c.Values) != n {
		return nil, serviceerror.NewInvalidArgument("visibility: malformed page token")
	}
	return c.Values, nil
}
