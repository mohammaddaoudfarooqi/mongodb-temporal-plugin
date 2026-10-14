package converter

import (
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

// SearchConverter implements query.StoreQueryConverter[bson.M] for
// Atlas Search mode.
//
// Emitted expressions are Atlas Search operators (equals, range, text,
// wildcard, exists) wrapped in compound clauses (must / mustNot /
// should). The caller wraps the final expression in a $search stage
// at query time:
//
//	{ $search: { index: "<name>", compound: { … } } }
//
// It is the Atlas Search equivalent of the aggregation-mode converter.
type SearchConverter struct{}

// NewSearchConverter constructs the Atlas Search emitter.
func NewSearchConverter() *SearchConverter { return &SearchConverter{} }

// GetDatetimeFormat — RFC3339 matches the Aggregation converter; Atlas
// Search accepts ISO-8601 dates, the same shape.
func (c *SearchConverter) GetDatetimeFormat() string {
	return time.RFC3339Nano
}

// BuildParenExpr is a no-op; compound clauses nest naturally.
func (c *SearchConverter) BuildParenExpr(expr bson.M) (bson.M, error) {
	return expr, nil
}

// BuildNotExpr wraps the expression in compound.mustNot.
func (c *SearchConverter) BuildNotExpr(expr bson.M) (bson.M, error) {
	if len(expr) == 0 {
		return nil, nil
	}
	return bson.M{"compound": bson.M{"mustNot": []bson.M{expr}}}, nil
}

// BuildAndExpr combines clauses into compound.must.
func (c *SearchConverter) BuildAndExpr(exprs ...bson.M) (bson.M, error) {
	exprs = compact(exprs)
	switch len(exprs) {
	case 0:
		return nil, nil
	case 1:
		return exprs[0], nil
	}
	return bson.M{"compound": bson.M{"must": exprs}}, nil
}

// BuildOrExpr combines clauses into compound.should with
// minimumShouldMatch: 1 (Atlas Search requires at least one match for
// OR semantics).
func (c *SearchConverter) BuildOrExpr(exprs ...bson.M) (bson.M, error) {
	exprs = compact(exprs)
	switch len(exprs) {
	case 0:
		return nil, nil
	case 1:
		return exprs[0], nil
	}
	return bson.M{"compound": bson.M{
		"should":             exprs,
		"minimumShouldMatch": 1,
	}}, nil
}

// ConvertComparisonExpr dispatches on operator.
func (c *SearchConverter) ConvertComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.M, error) {
	return c.convertCmp(operator, col, value)
}

// ConvertKeywordComparisonExpr.
func (c *SearchConverter) ConvertKeywordComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.M, error) {
	return c.convertCmp(operator, col, value)
}

// ConvertKeywordListComparisonExpr: Atlas Search's `equals`
// against an array field matches when the array contains the value.
func (c *SearchConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.M, error) {
	return c.convertCmp(operator, col, value)
}

// ConvertTextComparisonExpr implements full-text '=' / '!=' with the
// Atlas Search `text` operator.
func (c *SearchConverter) ConvertTextComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.M, error) {
	field, _ := resolveField(col.FieldName)
	s, ok := value.(string)
	if !ok {
		return nil, serviceerror.NewInvalidArgument("text comparison requires string value")
	}
	// Custom attributes are indexed through the search_attributes typeSet,
	// whose keyword-analysed string carries a standard-analysed "text"
	// multi for full-text matching (schema/atlas_search/visibility.json).
	var path any = field
	if strings.HasPrefix(field, "search_attributes.") {
		path = bson.M{"value": field, "multi": "text"}
	}
	pred := bson.M{"text": bson.M{"path": path, "query": s}}
	switch operator {
	case "=":
		return pred, nil
	case "!=":
		return bson.M{"compound": bson.M{"mustNot": []bson.M{pred}}}, nil
	}
	return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported text operator: %s", operator))
}

// ConvertRangeExpr — BETWEEN maps to Search `range`. NOT BETWEEN wraps
// in mustNot.
func (c *SearchConverter) ConvertRangeExpr(
	operator string,
	col *query.SAColumn,
	from, to any,
) (bson.M, error) {
	field, _ := resolveField(col.FieldName)
	from, err := coerceValue(col.ValueType, from)
	if err != nil {
		return nil, err
	}
	to, err = coerceValue(col.ValueType, to)
	if err != nil {
		return nil, err
	}
	rng := bson.M{"path": field, "gte": from, "lte": to}
	switch operator {
	case "between":
		return bson.M{"range": rng}, nil
	case "not between":
		return bson.M{"compound": bson.M{"mustNot": []bson.M{{"range": rng}}}}, nil
	}
	return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported range operator: %s", operator))
}

// ConvertIsExpr — IS NOT NULL emits `exists`; IS NULL wraps in mustNot.
func (c *SearchConverter) ConvertIsExpr(
	operator string,
	col *query.SAColumn,
) (bson.M, error) {
	field, _ := resolveField(col.FieldName)
	exists := bson.M{"exists": bson.M{"path": field}}
	switch operator {
	case "is not null":
		return exists, nil
	case "is null":
		return bson.M{"compound": bson.M{"mustNot": []bson.M{exists}}}, nil
	}
	return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported IS operator: %s", operator))
}

// --- shared comparison dispatcher ---

func (c *SearchConverter) convertCmp(op string, col *query.SAColumn, value any) (bson.M, error) {
	field := col.FieldName
	mongoField, _ := resolveField(field)

	// ExecutionStatus — same normalisation as Aggregation mode,
	// applied per element for IN / NOT IN.
	if field == "ExecutionStatus" {
		normalized, err := normalizeStatusValue(value)
		if err != nil {
			return nil, err
		}
		return c.applyCmpToScalar(op, "status", normalized)
	}

	if op == "starts_with" || op == "not starts_with" {
		s, ok := value.(string)
		if !ok {
			return nil, serviceerror.NewInvalidArgument(op + " requires string value")
		}
		pred := bson.M{"wildcard": bson.M{
			"path":  mongoField,
			"query": escapeWildcardMeta(s) + "*",
		}}
		if op == "not starts_with" {
			return bson.M{"compound": bson.M{"mustNot": []bson.M{pred}}}, nil
		}
		return pred, nil
	}

	value, err := coerceValue(col.ValueType, value)
	if err != nil {
		return nil, err
	}
	return c.applyCmpToScalar(op, mongoField, value)
}

// normalizeStatusValue normalises a scalar or an IN-list of statuses.
func normalizeStatusValue(value any) (any, error) {
	list, ok := value.([]any)
	if !ok {
		return normalizeStatus(value)
	}
	out := make([]any, 0, len(list))
	for _, v := range list {
		n, err := normalizeStatus(v)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

// applyCmpToScalar emits the right Search operator for a scalar-valued
// comparison.
func (c *SearchConverter) applyCmpToScalar(op, field string, value any) (bson.M, error) {
	switch op {
	case "=":
		return bson.M{"equals": bson.M{"path": field, "value": value}}, nil
	case "!=":
		return bson.M{"compound": bson.M{"mustNot": []bson.M{
			{"equals": bson.M{"path": field, "value": value}},
		}}}, nil
	case "<":
		return bson.M{"range": bson.M{"path": field, "lt": value}}, nil
	case "<=":
		return bson.M{"range": bson.M{"path": field, "lte": value}}, nil
	case ">":
		return bson.M{"range": bson.M{"path": field, "gt": value}}, nil
	case ">=":
		return bson.M{"range": bson.M{"path": field, "gte": value}}, nil
	case "in":
		arr, ok := value.([]any)
		if !ok {
			return nil, serviceerror.NewInvalidArgument("IN requires array value")
		}
		shoulds := make([]bson.M, 0, len(arr))
		for _, v := range arr {
			shoulds = append(shoulds, bson.M{"equals": bson.M{"path": field, "value": v}})
		}
		return bson.M{"compound": bson.M{
			"should":             shoulds,
			"minimumShouldMatch": 1,
		}}, nil
	case "not in":
		arr, ok := value.([]any)
		if !ok {
			return nil, serviceerror.NewInvalidArgument("NOT IN requires array value")
		}
		mustNots := make([]bson.M, 0, len(arr))
		for _, v := range arr {
			mustNots = append(mustNots, bson.M{"equals": bson.M{"path": field, "value": v}})
		}
		return bson.M{"compound": bson.M{"mustNot": mustNots}}, nil
	}
	return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported comparison operator: %s", op))
}

// escapeWildcardMeta escapes ? and * so a user-supplied prefix string
// cannot introduce unintended wildcards inside the Atlas Search query.
func escapeWildcardMeta(s string) string {
	repl := strings.NewReplacer(`*`, `\*`, `?`, `\?`, `\`, `\\`)
	return repl.Replace(s)
}

// normalizeStatus turns string ("Running") or integer values into the
// int32 enum for the Search index's number-typed status field.
func normalizeStatus(value any) (int32, error) {
	nameMap := map[string]enumspb.WorkflowExecutionStatus{
		"Unspecified":    enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
		"Running":        enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		"Completed":      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		"Failed":         enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		"Canceled":       enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		"Terminated":     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		"ContinuedAsNew": enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		"TimedOut":       enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}
	switch x := value.(type) {
	case string:
		if s, ok := nameMap[x]; ok {
			return int32(s), nil
		}
		return 0, serviceerror.NewInvalidArgument(fmt.Sprintf("unknown ExecutionStatus: %s", x))
	case int:
		return int32(x), nil
	case int32:
		return x, nil
	case int64:
		return int32(x), nil
	case enumspb.WorkflowExecutionStatus:
		return int32(x), nil
	}
	return 0, serviceerror.NewInvalidArgument(fmt.Sprintf("ExecutionStatus value type %T not supported", value))
}
