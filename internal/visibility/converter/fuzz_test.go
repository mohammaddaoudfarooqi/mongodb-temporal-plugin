package converter_test

import (
	"fmt"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/security"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility/converter"
)

// allowedOps are the only query operators the aggregation converter may
// emit. Anything else, and above all a JavaScript-executing operator,
// means user query text reached the database as an operator.
var allowedOps = map[string]bool{
	"$and": true, "$or": true, "$nor": true, "$not": true,
	"$eq": true, "$ne": true, "$gt": true, "$gte": true, "$lt": true, "$lte": true,
	"$in": true, "$nin": true, "$exists": true, "$regex": true, "$options": true,
}

// jsOps must never appear in either converter's output.
var jsOps = map[string]bool{"$where": true, "$function": true, "$accumulator": true, "$expr": true}

// walk visits every key of a BSON tree.
func walk(v any, visit func(key string) error) error {
	switch x := v.(type) {
	case bson.M:
		for k, e := range x {
			if err := visit(k); err != nil {
				return err
			}
			if err := walk(e, visit); err != nil {
				return err
			}
		}
	case map[string]any:
		return walk(bson.M(x), visit)
	case bson.D:
		for _, e := range x {
			if err := visit(e.Key); err != nil {
				return err
			}
			if err := walk(e.Value, visit); err != nil {
				return err
			}
		}
	case bson.A:
		for _, e := range x {
			if err := walk(e, visit); err != nil {
				return err
			}
		}
	case []any:
		return walk(bson.A(x), visit)
	case []bson.M:
		for _, e := range x {
			if err := walk(e, visit); err != nil {
				return err
			}
		}
	}
	return nil
}

func fuzzSeeds(f *testing.F) {
	for _, s := range []string{
		"WorkflowId = 'a'",
		"WorkflowType = 'x' AND ExecutionStatus = 'Running'",
		"AliasForInt01 > 3 OR AliasForKeyword01 IN ('a', 'b')",
		"AliasForText01 = 'refund duplicate'",
		"StartTime BETWEEN '2026-01-01T00:00:00Z' AND '2026-02-01T00:00:00Z'",
		"WorkflowId STARTS_WITH 'order-'",
		"AliasForKeyword01 IS NULL",
		"WorkflowId = '$where'",
		`WorkflowId = "'; return true; //"`,
		"AliasForKeyword01 = '{\"$where\": \"sleep(1000)\"}'",
		"WorkflowId = 'a' ORDER BY StartTime DESC",
		"(((WorkflowId = 'a')))",
		"NOT (AliasForInt01 < 0)",
		"$where = 'x'",
		"AliasForKeywordList01 = 'tag'",
	} {
		f.Add(s)
	}
}

func convert(t *testing.T, c query.StoreQueryConverter[bson.M], q string) (bson.M, bool) {
	t.Helper()
	conv, err := converter.NewQueryConverter(c, namespace.Name("ns"), searchattribute.TestNameTypeMap(),
		&searchattribute.TestMapper{Namespace: "ns"}, metrics.NoopMetricsHandler, log.NewNoopLogger())
	if err != nil {
		t.Fatal(err)
	}
	params, err := conv.Convert(q)
	if err != nil {
		return nil, false // rejected queries are fine; only accepted ones are checked
	}
	return params.QueryExpr, true
}

// FuzzBSONConverter_OnlySafeOperators: whatever the query text, an
// accepted query becomes only allow-listed operators, never JavaScript,
// and the converter never panics.
func FuzzBSONConverter_OnlySafeOperators(f *testing.F) {
	fuzzSeeds(f)
	f.Fuzz(func(t *testing.T, q string) {
		expr, ok := convert(t, converter.NewBSONConverter(), q)
		if !ok {
			return
		}
		if err := security.RejectJSFilter(expr); err != nil {
			t.Fatalf("query %q produced a JavaScript operator: %v", q, err)
		}
		if err := walk(expr, func(k string) error {
			if strings.HasPrefix(k, "$") && !allowedOps[k] {
				return fmt.Errorf("unexpected operator %s", k)
			}
			return nil
		}); err != nil {
			t.Fatalf("query %q: %v\n%v", q, err, expr)
		}
	})
}

// FuzzSearchConverter_NoJavaScript: the Atlas Search converter emits
// $search operators (not $-prefixed), and never a JavaScript operator.
func FuzzSearchConverter_NoJavaScript(f *testing.F) {
	fuzzSeeds(f)
	f.Fuzz(func(t *testing.T, q string) {
		expr, ok := convert(t, converter.NewSearchConverter(), q)
		if !ok {
			return
		}
		if err := walk(expr, func(k string) error {
			if jsOps[k] {
				return fmt.Errorf("JavaScript operator %s", k)
			}
			return nil
		}); err != nil {
			t.Fatalf("query %q: %v\n%v", q, err, expr)
		}
	})
}
