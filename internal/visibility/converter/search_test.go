package converter_test

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	enumspb "go.temporal.io/api/enums/v1"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility/converter"
)

// TestSearch_Equality maps "WorkflowId = 'abc'" to a Search equals
// clause on the token field.
func TestSearch_Equality(t *testing.T) {
	c := converter.NewSearchConverter()
	got, err := c.ConvertComparisonExpr("=",
		col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "abc")
	if err != nil {
		t.Fatal(err)
	}
	want := bson.M{"equals": bson.M{"path": "workflow_id", "value": "abc"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

// TestSearch_Range maps <, >, BETWEEN to Search `range`.
func TestSearch_Range(t *testing.T) {
	c := converter.NewSearchConverter()
	got, err := c.ConvertComparisonExpr("<",
		col("HistoryLength", enumspb.INDEXED_VALUE_TYPE_INT), int64(100))
	if err != nil {
		t.Fatal(err)
	}
	want := bson.M{"range": bson.M{"path": "history_length", "lt": int64(100)}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}

	got, err = c.ConvertRangeExpr("between",
		col("HistoryLength", enumspb.INDEXED_VALUE_TYPE_INT), int64(10), int64(20))
	if err != nil {
		t.Fatal(err)
	}
	want = bson.M{"range": bson.M{"path": "history_length", "gte": int64(10), "lte": int64(20)}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("between got %v want %v", got, want)
	}
}

// TestSearch_InIsSynthesized covers IN via should clauses.
func TestSearch_InIsSynthesized(t *testing.T) {
	c := converter.NewSearchConverter()
	got, err := c.ConvertComparisonExpr("in",
		col("TaskQueue", enumspb.INDEXED_VALUE_TYPE_KEYWORD), []any{"q1", "q2"})
	if err != nil {
		t.Fatal(err)
	}
	m := got
	compound, ok := m["compound"].(bson.M)
	if !ok {
		t.Fatalf("expected compound; got %+v", m)
	}
	should, ok := compound["should"].([]bson.M)
	if !ok || len(should) != 2 {
		t.Fatalf("expected 2 should clauses; got %+v", compound["should"])
	}
	mm, ok := compound["minimumShouldMatch"].(int)
	if !ok || mm != 1 {
		t.Errorf("minimumShouldMatch should be 1; got %v", compound["minimumShouldMatch"])
	}
}

// TestSearch_TextEquals maps Text '=' to the Atlas Search text operator
// (upstream only sends '=' / '!=' for Text).
func TestSearch_TextEquals(t *testing.T) {
	c := converter.NewSearchConverter()
	got, err := c.ConvertTextComparisonExpr("=",
		col("CustomText", enumspb.INDEXED_VALUE_TYPE_TEXT), "foo")
	if err != nil {
		t.Fatal(err)
	}
	text, ok := got["text"].(bson.M)
	if !ok {
		t.Fatalf("expected text clause; got %+v", got)
	}
	path, _ := text["path"].(bson.M)
	if path["value"] != "search_attributes.CustomText" || path["multi"] != "text" || text["query"] != "foo" {
		t.Errorf("text clause mismatch: %v", text)
	}
}

// TestSearch_IsNull uses `exists` + `mustNot`.
func TestSearch_IsNull(t *testing.T) {
	c := converter.NewSearchConverter()
	got, err := c.ConvertIsExpr("is null",
		col("CustomKeyword", enumspb.INDEXED_VALUE_TYPE_KEYWORD))
	if err != nil {
		t.Fatal(err)
	}
	// IS NULL → mustNot { exists }
	compound, ok := got["compound"].(bson.M)
	if !ok {
		t.Fatalf("expected compound; got %+v", got)
	}
	mustNot, ok := compound["mustNot"].([]bson.M)
	if !ok || len(mustNot) != 1 {
		t.Fatalf("expected 1 mustNot; got %+v", compound["mustNot"])
	}
	if _, ok := mustNot[0]["exists"]; !ok {
		t.Errorf("mustNot should wrap exists: %+v", mustNot[0])
	}

	// IS NOT NULL → exists alone.
	got, err = c.ConvertIsExpr("is not null",
		col("CustomKeyword", enumspb.INDEXED_VALUE_TYPE_KEYWORD))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["exists"]; !ok {
		t.Errorf("is not null should emit exists; got %+v", got)
	}
}

// TestSearch_BuildAnd composes clauses into compound.must.
func TestSearch_BuildAnd(t *testing.T) {
	c := converter.NewSearchConverter()
	a, _ := c.ConvertComparisonExpr("=",
		col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "wf-1")
	b, _ := c.ConvertComparisonExpr("=",
		col("RunId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "run-1")

	and, err := c.BuildAndExpr(a, b)
	if err != nil {
		t.Fatal(err)
	}
	compound, ok := and["compound"].(bson.M)
	if !ok {
		t.Fatalf("expected compound; got %+v", and)
	}
	must, ok := compound["must"].([]bson.M)
	if !ok || len(must) != 2 {
		t.Errorf("expected 2 must clauses; got %+v", compound["must"])
	}
}

// TestSearch_BuildOr composes clauses into compound.should +
// minimumShouldMatch:1.
func TestSearch_BuildOr(t *testing.T) {
	c := converter.NewSearchConverter()
	a, _ := c.ConvertComparisonExpr("=",
		col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "a")
	b, _ := c.ConvertComparisonExpr("=",
		col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "b")

	or, err := c.BuildOrExpr(a, b)
	if err != nil {
		t.Fatal(err)
	}
	compound := or["compound"].(bson.M)
	should, ok := compound["should"].([]bson.M)
	if !ok || len(should) != 2 {
		t.Errorf("expected 2 should clauses; got %+v", compound["should"])
	}
	if compound["minimumShouldMatch"] != 1 {
		t.Errorf("minimumShouldMatch=1 required; got %v", compound["minimumShouldMatch"])
	}
}

// TestSearch_BuildNot wraps the inner clause in mustNot.
func TestSearch_BuildNot(t *testing.T) {
	c := converter.NewSearchConverter()
	a, _ := c.ConvertComparisonExpr("=",
		col("WorkflowId", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "a")
	not, err := c.BuildNotExpr(a)
	if err != nil {
		t.Fatal(err)
	}
	compound := not["compound"].(bson.M)
	mustNot, ok := compound["mustNot"].([]bson.M)
	if !ok || len(mustNot) != 1 {
		t.Errorf("expected 1 mustNot; got %+v", compound["mustNot"])
	}
}

// TestSearch_KeywordListEquality uses equals on the list field; Atlas
// auto-matches array elements.
func TestSearch_KeywordListEquality(t *testing.T) {
	c := converter.NewSearchConverter()
	got, err := c.ConvertKeywordListComparisonExpr("=",
		col("CustomKeywordList", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST), "tag-a")
	if err != nil {
		t.Fatal(err)
	}
	want := bson.M{"equals": bson.M{"path": "search_attributes.CustomKeywordList", "value": "tag-a"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

// TestSearch_ExecutionStatusEnum handles both string and int forms.
func TestSearch_ExecutionStatusEnum(t *testing.T) {
	c := converter.NewSearchConverter()
	got, err := c.ConvertComparisonExpr("=",
		col("ExecutionStatus", enumspb.INDEXED_VALUE_TYPE_KEYWORD), "Running")
	if err != nil {
		t.Fatal(err)
	}
	eq := got["equals"].(bson.M)
	if eq["path"] != "status" {
		t.Errorf("path: %v", eq["path"])
	}
	if eq["value"] != int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING) {
		t.Errorf("status not mapped: %v", eq["value"])
	}
}
