package converter_test

import (
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility/converter"
)

// These tests drive real query strings through upstream's QueryConverter,
// exactly as the store does, so the operator set and value types are the
// ones the server actually produces.

func convertWith(t *testing.T, qc query.StoreQueryConverter[bson.M], q string) bson.M {
	t.Helper()
	typeMap := searchattribute.TestNameTypeMap()
	mapper := &searchattribute.TestMapper{Namespace: "ns"}
	conv, err := converter.NewQueryConverter(qc, namespace.Name("ns"), typeMap, mapper, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	params, err := conv.Convert(q)
	if err != nil {
		t.Fatalf("Convert(%q): %v", q, err)
	}
	if hasNil(params.QueryExpr) {
		t.Fatalf("Convert(%q) produced a nil operand: %v", q, params.QueryExpr)
	}
	return params.QueryExpr
}

// hasNil reports whether any nested document or array element is nil.
func hasNil(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case bson.M:
		if x == nil {
			return true
		}
		for _, e := range x {
			if hasNil(e) {
				return true
			}
		}
	case []bson.M:
		for _, e := range x {
			if hasNil(e) {
				return true
			}
		}
	case []any:
		for _, e := range x {
			if hasNil(e) {
				return true
			}
		}
	case bson.A:
		for _, e := range x {
			if hasNil(e) {
				return true
			}
		}
	}
	return false
}

// findValue returns the first value stored under key anywhere in v.
func findValue(v any, key string) (any, bool) {
	switch x := v.(type) {
	case bson.M:
		for k, e := range x {
			if k == key {
				return e, true
			}
			if got, ok := findValue(e, key); ok {
				return got, true
			}
		}
	case []bson.M:
		for _, e := range x {
			if got, ok := findValue(e, key); ok {
				return got, true
			}
		}
	case []any:
		for _, e := range x {
			if got, ok := findValue(e, key); ok {
				return got, true
			}
		}
	}
	return nil, false
}

func TestQueryConverter_NoNilOperands(t *testing.T) {
	queries := []string{
		"",
		"ORDER BY StartTime DESC",
		"GROUP BY ExecutionStatus",
		"TemporalNamespaceDivision = 'TemporalScheduler'",
		"WorkflowId = 'a' AND (ExecutionStatus = 'Running' OR AliasForInt01 > 3)",
	}
	for name, qc := range map[string]query.StoreQueryConverter[bson.M]{
		"aggregation": converter.NewBSONConverter(), "search": converter.NewSearchConverter(),
	} {
		for _, q := range queries {
			t.Run(fmt.Sprintf("%s/%q", name, q), func(t *testing.T) { convertWith(t, qc, q) })
		}
	}
}

func TestBSONConverter_DatetimeComparesAsDate(t *testing.T) {
	expr := convertWith(t, converter.NewBSONConverter(), "StartTime > '2026-01-01T00:00:00Z'")
	got, ok := findValue(expr, "$gt")
	if _, isTime := got.(time.Time); !ok || !isTime {
		t.Fatalf("StartTime bound = %T %v; want time.Time", got, got)
	}
	expr = convertWith(t, converter.NewBSONConverter(), "AliasForDatetime01 BETWEEN '2026-01-01T00:00:00Z' AND '2026-01-02T00:00:00Z'")
	got, ok = findValue(expr, "$gte")
	if _, isTime := got.(time.Time); !ok || !isTime {
		t.Fatalf("BETWEEN bound = %T %v; want time.Time", got, got)
	}
}

func TestSearchConverter_DatetimeComparesAsDate(t *testing.T) {
	expr := convertWith(t, converter.NewSearchConverter(), "CloseTime < '2026-01-01T00:00:00Z'")
	got, ok := findValue(expr, "lt")
	if _, isTime := got.(time.Time); !ok || !isTime {
		t.Fatalf("CloseTime bound = %T %v; want time.Time", got, got)
	}
}

func TestConverters_ServerOperatorsSupported(t *testing.T) {
	queries := []string{
		"AliasForText01 = 'brave'",
		"AliasForText01 != 'brave'",
		"WorkflowId NOT STARTS_WITH 'wf'",
		"ExecutionStatus IN ('Running', 'Failed')",
		"ExecutionStatus NOT IN ('Running')",
		"AliasForKeywordList01 IN ('a', 'b')",
		"AliasForBool01 = true",
	}
	for name, qc := range map[string]query.StoreQueryConverter[bson.M]{
		"aggregation": converter.NewBSONConverter(), "search": converter.NewSearchConverter(),
	} {
		for _, q := range queries {
			t.Run(fmt.Sprintf("%s/%q", name, q), func(t *testing.T) { convertWith(t, qc, q) })
		}
	}
}
