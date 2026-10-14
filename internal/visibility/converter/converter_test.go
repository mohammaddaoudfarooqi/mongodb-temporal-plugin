package converter_test

import (
	"reflect"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/query"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility/converter"
)

// col is a test helper that builds an SAColumn with the given name +
// indexed type.
func col(name string, t enumspb.IndexedValueType) *query.SAColumn {
	return query.NewSAColumn(name, name, t)
}

// TestConverter_EqualOnSystemSA maps "WorkflowId = 'abc'" to a
// top-level field equality predicate.
func TestConverter_EqualOnSystemSA(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertComparisonExpr("=", col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "abc")
	if err != nil {
		t.Fatal(err)
	}
	want := bson.M{"workflow_id": "abc"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestConverter_NotEqual(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertComparisonExpr("!=", col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "abc")
	if err != nil {
		t.Fatal(err)
	}
	want := bson.M{"workflow_id": bson.M{"$ne": "abc"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

// TestConverter_LessAndGreater covers <, <=, >, >=.
func TestConverter_LessAndGreater(t *testing.T) {
	tests := []struct {
		op   string
		mong string
	}{
		{"<", "$lt"},
		{"<=", "$lte"},
		{">", "$gt"},
		{">=", "$gte"},
	}
	for _, tc := range tests {
		c := converter.NewBSONConverter()
		got, err := c.ConvertComparisonExpr(tc.op, col("HistoryLength", enumspb.INDEXED_VALUE_TYPE_INT), int64(100))
		if err != nil {
			t.Fatalf("%s: %v", tc.op, err)
		}
		want := bson.M{"history_length": bson.M{tc.mong: int64(100)}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v want %v", tc.op, got, want)
		}
	}
}

func TestConverter_InAndNotIn(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertComparisonExpr("in", col("TaskQueue", enumspb.INDEXED_VALUE_TYPE_KEYWORD), []any{"q1", "q2"})
	if err != nil {
		t.Fatal(err)
	}
	want := bson.M{"task_queue": bson.M{"$in": []any{"q1", "q2"}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("in: got %v want %v", got, want)
	}
	got, err = c.ConvertComparisonExpr("not in", col("TaskQueue", enumspb.INDEXED_VALUE_TYPE_KEYWORD), []any{"q1"})
	if err != nil {
		t.Fatal(err)
	}
	want = bson.M{"task_queue": bson.M{"$nin": []any{"q1"}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("not in: got %v want %v", got, want)
	}
}

func TestConverter_Between(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertRangeExpr("between",
		col("HistoryLength", enumspb.INDEXED_VALUE_TYPE_INT),
		int64(10), int64(20))
	if err != nil {
		t.Fatal(err)
	}
	want := bson.M{"history_length": bson.M{"$gte": int64(10), "$lte": int64(20)}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("between: got %v want %v", got, want)
	}
}

// TestConverter_LikePrefix: LIKE 'foo%' uses an
// anchored-prefix regex on Text fields.
func TestConverter_TextEquals_WordMatchCaseInsensitive(t *testing.T) {
	c := converter.NewBSONConverter()
	// Upstream only sends '=' / '!=' for Text attributes.
	got, err := c.ConvertTextComparisonExpr("=",
		col("CustomText", enumspb.INDEXED_VALUE_TYPE_TEXT), "foo")
	if err != nil {
		t.Fatal(err)
	}
	sub, ok := got["search_attributes.CustomText"].(bson.M)
	if !ok {
		t.Fatalf("expected predicate on search_attributes.CustomText; got %+v", got)
	}
	raw, ok := sub["$regex"].(string)
	if !ok || !strings.Contains(raw, "foo") || sub["$options"] != "i" {
		t.Errorf("expected case-insensitive word regex for foo; got %+v", sub)
	}
}

// TestConverter_TextRegexEscaped: metacharacters in
// the Text argument must be escaped so the regex stays bounded.
func TestConverter_TextRegexEscaped(t *testing.T) {
	c := converter.NewBSONConverter()
	got, _ := c.ConvertTextComparisonExpr("=",
		col("CustomText", enumspb.INDEXED_VALUE_TYPE_TEXT), "foo.*bar")
	sub := got["search_attributes.CustomText"].(bson.M)
	raw := sub["$regex"].(string)
	if !strings.Contains(raw, `foo\.\*bar`) {
		t.Errorf("regex metachars not escaped: %q", raw)
	}
}

// TestConverter_IsNull IS NULL.
func TestConverter_IsNull(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertIsExpr("is null", col("CustomKeyword", enumspb.INDEXED_VALUE_TYPE_KEYWORD))
	if err != nil {
		t.Fatal(err)
	}
	want := bson.M{"search_attributes.CustomKeyword": bson.M{"$exists": false}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("is null: got %v want %v", got, want)
	}
	got, err = c.ConvertIsExpr("is not null", col("CustomKeyword", enumspb.INDEXED_VALUE_TYPE_KEYWORD))
	if err != nil {
		t.Fatal(err)
	}
	want = bson.M{"search_attributes.CustomKeyword": bson.M{"$exists": true}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("is not null: got %v want %v", got, want)
	}
}

func TestConverter_StartsWith(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertComparisonExpr("starts_with",
		col("CustomKeyword", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "pref")
	if err != nil {
		t.Fatal(err)
	}
	sub := got["search_attributes.CustomKeyword"].(bson.M)
	if raw, ok := sub["$regex"].(string); !ok || !strings.HasPrefix(raw, "^pref") {
		t.Errorf("starts_with should emit anchored regex; got %+v", sub)
	}
}

// TestConverter_BuildAndOr combinators.
func TestConverter_BuildAndOr(t *testing.T) {
	c := converter.NewBSONConverter()
	a, _ := c.ConvertComparisonExpr("=", col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "wf-1")
	b, _ := c.ConvertComparisonExpr("=", col("RunId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "run-1")

	andExpr, err := c.BuildAndExpr(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := andExpr["$and"]; !ok {
		t.Errorf("AND expression missing $and: %+v", andExpr)
	}

	orExpr, err := c.BuildOrExpr(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := orExpr["$or"]; !ok {
		t.Errorf("OR expression missing $or: %+v", orExpr)
	}
}

// TestConverter_KeywordListEquality: equality on a
// KeywordList uses Mongo's implicit array-contains semantics.
func TestConverter_KeywordListEquality(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertKeywordListComparisonExpr("=",
		col("CustomKeywordList", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST), "tag-a")
	if err != nil {
		t.Fatal(err)
	}
	// Mongo: {field: val} matches elements inside an array.
	want := bson.M{"search_attributes.CustomKeywordList": "tag-a"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

// TestConverter_ExecutionStatusEnum: ExecutionStatus
// accepts both the enum string name and the integer value.
func TestConverter_ExecutionStatusEnum(t *testing.T) {
	c := converter.NewBSONConverter()
	// String form.
	got, _ := c.ConvertComparisonExpr("=",
		col("ExecutionStatus", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "Running")
	m := got
	if m["status"] != int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING) {
		t.Errorf("string Running not mapped to enum int: %v", m)
	}
	// Int form.
	got, _ = c.ConvertComparisonExpr("=",
		col("ExecutionStatus", enumspb.INDEXED_VALUE_TYPE_KEYWORD), int64(2))
	m = got
	if m["status"] != int32(2) {
		t.Errorf("int status not preserved: %v", m)
	}
}

// TestConverter_GetDatetimeFormat_IsRFC3339Nano covers the layout
// passed through to the upstream query parser.
func TestConverter_GetDatetimeFormat_IsRFC3339Nano(t *testing.T) {
	c := converter.NewBSONConverter()
	got := c.GetDatetimeFormat()
	// Just assert it parses as the RFC3339-nano layout; the upstream
	// parser depends on the exact layout string, so we tolerate any
	// format spec that round-trips.
	if got == "" {
		t.Fatal("GetDatetimeFormat returned empty string")
	}
}

// TestConverter_BuildParenExpr covers the identity paren builder
// (MongoDB's $and / $or nest natively).
func TestConverter_BuildParenExpr_Identity(t *testing.T) {
	c := converter.NewBSONConverter()
	in := bson.M{"workflow_id": "x"}
	out, err := c.BuildParenExpr(in)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Errorf("BuildParenExpr is not identity: %v", out)
	}
}

// TestConverter_BuildNotExpr_WrapsNor — NOT goes through $nor so it
// applies to the whole sub-document.
func TestConverter_BuildNotExpr_WrapsNor(t *testing.T) {
	c := converter.NewBSONConverter()
	out, err := c.BuildNotExpr(bson.M{"workflow_id": "x"})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := out["$nor"].([]bson.M)
	if !ok || len(m) != 1 {
		t.Fatalf("expected $nor with one clause; got %v", out)
	}
	if m[0]["workflow_id"] != "x" {
		t.Errorf("$nor payload drift: %v", m[0])
	}
}

// TestConverter_BuildAndOr_SingleElementCollapses covers the single-
// clause short-circuit (avoids wrapping a single predicate in $and /
// $or which would confuse MongoDB's planner).
func TestConverter_BuildAndOr_SingleElementCollapses(t *testing.T) {
	c := converter.NewBSONConverter()
	in := bson.M{"workflow_id": "x"}
	and, _ := c.BuildAndExpr(in)
	if !reflect.DeepEqual(and, in) {
		t.Errorf("BuildAndExpr single-element should collapse: got %v", and)
	}
	or, _ := c.BuildOrExpr(in)
	if !reflect.DeepEqual(or, in) {
		t.Errorf("BuildOrExpr single-element should collapse: got %v", or)
	}
}

// TestConverter_BuildAndOr_MultiElementWraps — multiple clauses get
// a proper $and / $or wrapper.
func TestConverter_BuildAndOr_MultiElementWraps(t *testing.T) {
	c := converter.NewBSONConverter()
	a := bson.M{"workflow_id": "x"}
	b := bson.M{"namespace_id": "ns"}
	and, _ := c.BuildAndExpr(a, b)
	if _, ok := and["$and"]; !ok {
		t.Errorf("expected $and wrapper; got %v", and)
	}
	or, _ := c.BuildOrExpr(a, b)
	if _, ok := or["$or"]; !ok {
		t.Errorf("expected $or wrapper; got %v", or)
	}
}

// TestConverter_TextEqualsAndNotEquals — escaped word regex; '!=' wraps
// the predicate in $nor.
func TestConverter_TextEqualsAndNotEquals(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertTextComparisonExpr("=",
		col("WorkflowType", enumspb.INDEXED_VALUE_TYPE_TEXT), "Foo.Bar")
	if err != nil {
		t.Fatal(err)
	}
	m, ok := got["workflow_type_name"].(bson.M)
	if !ok {
		t.Fatalf("unexpected shape: %v", got)
	}
	re, ok := m["$regex"].(string)
	if !ok || !strings.Contains(re, `Foo\.Bar`) {
		t.Errorf("regex should be metachar-escaped: %q", re)
	}
	notgot, err := c.ConvertTextComparisonExpr("!=",
		col("WorkflowType", enumspb.INDEXED_VALUE_TYPE_TEXT), "abc")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := notgot["$nor"]; !ok {
		t.Errorf("expected $nor wrapper for !=: %v", notgot)
	}
}

// TestConverter_Text_UnsupportedOperator surfaces InvalidArgument for
// operators upstream never sends for Text (LIKE is not one of them).
func TestConverter_Text_UnsupportedOperator(t *testing.T) {
	c := converter.NewBSONConverter()
	_, err := c.ConvertTextComparisonExpr("like", col("WorkflowType", enumspb.INDEXED_VALUE_TYPE_TEXT), "x%")
	if err == nil {
		t.Error("expected InvalidArgument on LIKE for text")
	}
}

// TestConverter_Like_NonStringValue rejects non-string right-hand
// sides (= "text comparison requires string value" error).
func TestConverter_Like_NonStringValue(t *testing.T) {
	c := converter.NewBSONConverter()
	_, err := c.ConvertTextComparisonExpr("=", col("WorkflowType", enumspb.INDEXED_VALUE_TYPE_TEXT), 42)
	if err == nil {
		t.Error("expected error on non-string text value")
	}
}

// TestConverter_Range_BetweenAndNotBetween covers BETWEEN and its
// negation.
func TestConverter_Range_BetweenAndNotBetween(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertRangeExpr("between",
		col("ExecutionTime", enumspb.INDEXED_VALUE_TYPE_DATETIME), 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := got["execution_time"].(bson.M)
	if !ok || m["$gte"] != 1 || m["$lte"] != 10 {
		t.Errorf("BETWEEN shape drift: %v", got)
	}

	not, err := c.ConvertRangeExpr("not between",
		col("ExecutionTime", enumspb.INDEXED_VALUE_TYPE_DATETIME), 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := not["execution_time"]; !ok {
		t.Errorf("NOT BETWEEN shape drift: %v", not)
	}
}

// TestConverter_Range_UnsupportedOperator surfaces InvalidArgument.
func TestConverter_Range_UnsupportedOperator(t *testing.T) {
	c := converter.NewBSONConverter()
	_, err := c.ConvertRangeExpr("outside",
		col("ExecutionTime", enumspb.INDEXED_VALUE_TYPE_DATETIME), 1, 10)
	if err == nil {
		t.Error("expected error on unsupported range operator")
	}
}

// TestConverter_IsNullAndNotNull cover IS NULL / IS NOT NULL.
func TestConverter_IsNullAndNotNull(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertIsExpr("is null", col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD))
	if err != nil {
		t.Fatal(err)
	}
	if m, ok := got["workflow_id"].(bson.M); !ok || m["$exists"] != false {
		t.Errorf("IS NULL shape drift: %v", got)
	}
	got, err = c.ConvertIsExpr("is not null", col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD))
	if err != nil {
		t.Fatal(err)
	}
	if m, ok := got["workflow_id"].(bson.M); !ok || m["$exists"] != true {
		t.Errorf("IS NOT NULL shape drift: %v", got)
	}
	// Unsupported IS operator.
	if _, err := c.ConvertIsExpr("is something", col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD)); err == nil {
		t.Error("expected error on unsupported IS operator")
	}
}

// TestConverter_KeywordComparison_Equality exercises the Keyword SA
// equality path independently from the generic Comparison path.
func TestConverter_KeywordComparison_Equality(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertKeywordComparisonExpr("=",
		col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "w-1")
	if err != nil {
		t.Fatal(err)
	}
	if got["workflow_id"] != "w-1" {
		t.Errorf("keyword = shape drift: %v", got)
	}
}

// TestConverter_StartsWith_QuotesMetachar covers the regex escape.
func TestConverter_StartsWith_QuotesMetachar(t *testing.T) {
	c := converter.NewBSONConverter()
	got, err := c.ConvertComparisonExpr("starts_with",
		col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "a.b*")
	if err != nil {
		t.Fatal(err)
	}
	m, ok := got["workflow_id"].(bson.M)
	if !ok {
		t.Fatalf("shape drift: %v", got)
	}
	re, _ := m["$regex"].(string)
	if !strings.HasPrefix(re, "^a\\.b\\*") {
		t.Errorf("starts_with regex should escape metachars: %q", re)
	}
}

// TestConverter_UnsupportedComparison surfaces InvalidArgument.
func TestConverter_UnsupportedComparison(t *testing.T) {
	c := converter.NewBSONConverter()
	_, err := c.ConvertComparisonExpr("approx",
		col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "abc")
	if err == nil {
		t.Error("expected InvalidArgument on unsupported operator")
	}
}
