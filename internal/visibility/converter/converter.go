// Package converter translates the Temporal visibility DSL into
// BSON filter documents for the Aggregation-mode $match stage.
// Implements query.StoreQueryConverter[bson.M].
//
// The upstream QueryConverter[ExprT] parses SQL-like query strings
// into a typed AST (via sqlparser) and calls the corresponding
// Convert* method on the StoreQueryConverter. This package provides
// the bson.M expression emitter.
//
// System search attributes (ExecutionStatus, WorkflowId, RunId,
// TaskQueue, StartTime, etc.) map onto top-level fields of the
// visibility document shape defined in internal/visibility.Doc.
// Custom SAs live under the `search_attributes.<name>` sub-path.
package converter

import (
	"fmt"
	"regexp"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

// BSONConverter implements query.StoreQueryConverter[bson.M].
type BSONConverter struct{}

// NewBSONConverter constructs a converter. Stateless.
func NewBSONConverter() *BSONConverter { return &BSONConverter{} }

// GetDatetimeFormat is the layout the upstream parser uses when
// serialising time values. RFC3339 with nanosecond fraction.
func (c *BSONConverter) GetDatetimeFormat() string {
	return time.RFC3339Nano
}

// BuildParenExpr — BSON $and/$or clauses nest correctly; no grouping
// wrapper needed.
func (c *BSONConverter) BuildParenExpr(expr bson.M) (bson.M, error) {
	return expr, nil
}

// BuildNotExpr wraps the predicate in $nor so it applies to whole
// sub-documents, not just operator expressions.
func (c *BSONConverter) BuildNotExpr(expr bson.M) (bson.M, error) {
	if len(expr) == 0 {
		return nil, nil
	}
	return bson.M{"$nor": []bson.M{expr}}, nil
}

// BuildAndExpr combines multiple BSON predicates, ignoring absent ones.
func (c *BSONConverter) BuildAndExpr(exprs ...bson.M) (bson.M, error) {
	exprs = compact(exprs)
	switch len(exprs) {
	case 0:
		return nil, nil
	case 1:
		return exprs[0], nil
	}
	return bson.M{"$and": toArray(exprs)}, nil
}

// BuildOrExpr combines multiple BSON predicates, ignoring absent ones.
func (c *BSONConverter) BuildOrExpr(exprs ...bson.M) (bson.M, error) {
	exprs = compact(exprs)
	switch len(exprs) {
	case 0:
		return nil, nil
	case 1:
		return exprs[0], nil
	}
	return bson.M{"$or": toArray(exprs)}, nil
}

// ConvertComparisonExpr implements =, !=, <, <=, >, >=, IN, NOT IN,
// and STARTS_WITH.
func (c *BSONConverter) ConvertComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.M, error) {
	return c.convertCmp(operator, col.FieldName, value, col.ValueType)
}

// ConvertKeywordComparisonExpr — Keyword SA allows equality / IN only.
func (c *BSONConverter) ConvertKeywordComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.M, error) {
	return c.convertCmp(operator, col.FieldName, value, col.ValueType)
}

// ConvertKeywordListComparisonExpr — KeywordList uses implicit
// array-contains semantics.
func (c *BSONConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.M, error) {
	return c.convertCmp(operator, col.FieldName, value, col.ValueType)
}

// ConvertTextComparisonExpr implements full-text '=' / '!=' (the only
// Text operators upstream accepts): the value must appear in the field
// as a whole word, case-insensitively. Metacharacters are escaped
// .
func (c *BSONConverter) ConvertTextComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (bson.M, error) {
	field, _ := resolveField(col.FieldName)
	s, ok := value.(string)
	if !ok {
		return nil, serviceerror.NewInvalidArgument("text comparison requires string value")
	}
	pred := bson.M{field: bson.M{"$regex": `(^|\W)` + regexp.QuoteMeta(s) + `($|\W)`, "$options": "i"}}
	switch operator {
	case "=":
		return pred, nil
	case "!=":
		return bson.M{"$nor": []bson.M{pred}}, nil
	}
	return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported text operator: %s", operator))
}

// ConvertRangeExpr handles BETWEEN / NOT BETWEEN.
func (c *BSONConverter) ConvertRangeExpr(
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
	switch operator {
	case "between":
		return bson.M{field: bson.M{"$gte": from, "$lte": to}}, nil
	case "not between":
		return bson.M{field: bson.M{"$not": bson.M{"$gte": from, "$lte": to}}}, nil
	}
	return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported range operator: %s", operator))
}

// ConvertIsExpr handles IS NULL / IS NOT NULL.
func (c *BSONConverter) ConvertIsExpr(
	operator string,
	col *query.SAColumn,
) (bson.M, error) {
	field, _ := resolveField(col.FieldName)
	switch operator {
	case "is null":
		return bson.M{field: bson.M{"$exists": false}}, nil
	case "is not null":
		return bson.M{field: bson.M{"$exists": true}}, nil
	}
	return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported IS operator: %s", operator))
}

// --- shared comparison dispatcher ---

func (c *BSONConverter) convertCmp(
	op, field string,
	value any,
	saType enumspb.IndexedValueType,
) (bson.M, error) {
	mongoField, _ := resolveField(field)

	// ExecutionStatus — accept both string name and int form.
	if field == "ExecutionStatus" {
		return c.executionStatusPredicate(op, value)
	}

	if op == "starts_with" || op == "not starts_with" {
		s, ok := value.(string)
		if !ok {
			return nil, serviceerror.NewInvalidArgument(op + " requires string value")
		}
		prefix := bson.M{"$regex": "^" + regexp.QuoteMeta(s)}
		if op == "not starts_with" {
			return bson.M{mongoField: bson.M{"$not": prefix}}, nil
		}
		return bson.M{mongoField: prefix}, nil
	}

	value, err := coerceValue(saType, value)
	if err != nil {
		return nil, err
	}
	switch op {
	case "=":
		return bson.M{mongoField: value}, nil
	case "!=":
		return bson.M{mongoField: bson.M{"$ne": value}}, nil
	case "<":
		return bson.M{mongoField: bson.M{"$lt": value}}, nil
	case "<=":
		return bson.M{mongoField: bson.M{"$lte": value}}, nil
	case ">":
		return bson.M{mongoField: bson.M{"$gt": value}}, nil
	case ">=":
		return bson.M{mongoField: bson.M{"$gte": value}}, nil
	case "in":
		return bson.M{mongoField: bson.M{"$in": value}}, nil
	case "not in":
		return bson.M{mongoField: bson.M{"$nin": value}}, nil
	}
	return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported comparison operator: %s", op))
}

// --- field + value helpers ---

// systemFieldMap maps the DSL's camelCase system SAs to our doc's
// snake_case field names.
var systemFieldMap = map[string]string{
	"WorkflowId":                "workflow_id",
	"RunId":                     "run_id",
	"WorkflowType":              "workflow_type_name",
	"TaskQueue":                 "task_queue",
	"StartTime":                 "start_time",
	"ExecutionTime":             "execution_time",
	"CloseTime":                 "close_time",
	"ExecutionStatus":           "status",
	"HistoryLength":             "history_length",
	"HistorySizeBytes":          "history_size_bytes",
	"StateTransitionCount":      "state_transition_count",
	"ExecutionDuration":         "execution_duration_ns",
	"ParentWorkflowId":          "parent_workflow_id",
	"ParentRunId":               "parent_run_id",
	"RootWorkflowId":            "root_workflow_id",
	"RootRunId":                 "root_run_id",
	"TemporalNamespaceDivision": "temporal_namespace_division",

	// Note: the Temporal DSL also accepts "WorkflowType" but older
	// clients use "WorkflowTypeName". Alias both.
	"WorkflowTypeName": "workflow_type_name",
}

// resolveField returns the mongo field path and whether it's a system SA.
func resolveField(name string) (string, bool) {
	if f, ok := systemFieldMap[name]; ok {
		return f, true
	}
	return "search_attributes." + name, false
}

// executionStatusPredicate handles both string-name and int-value
// forms of the ExecutionStatus enum.
func (c *BSONConverter) executionStatusPredicate(op string, value any) (bson.M, error) {
	statusName := map[string]enumspb.WorkflowExecutionStatus{
		"Unspecified":    enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
		"Running":        enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		"Completed":      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		"Failed":         enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		"Canceled":       enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		"Terminated":     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		"ContinuedAsNew": enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		"TimedOut":       enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}
	normalize := func(v any) (int32, error) {
		switch x := v.(type) {
		case string:
			if s, ok := statusName[x]; ok {
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
		return 0, serviceerror.NewInvalidArgument(fmt.Sprintf("ExecutionStatus value type %T not supported", v))
	}

	switch op {
	case "=":
		n, err := normalize(value)
		if err != nil {
			return nil, err
		}
		return bson.M{"status": n}, nil
	case "!=":
		n, err := normalize(value)
		if err != nil {
			return nil, err
		}
		return bson.M{"status": bson.M{"$ne": n}}, nil
	case "in":
		arr, ok := value.([]any)
		if !ok {
			return nil, serviceerror.NewInvalidArgument("IN requires array value")
		}
		out := make([]int32, 0, len(arr))
		for _, v := range arr {
			n, err := normalize(v)
			if err != nil {
				return nil, err
			}
			out = append(out, n)
		}
		return bson.M{"status": bson.M{"$in": out}}, nil
	case "not in":
		arr, ok := value.([]any)
		if !ok {
			return nil, serviceerror.NewInvalidArgument("NOT IN requires array value")
		}
		out := make([]int32, 0, len(arr))
		for _, v := range arr {
			n, err := normalize(v)
			if err != nil {
				return nil, err
			}
			out = append(out, n)
		}
		return bson.M{"status": bson.M{"$nin": out}}, nil
	}
	return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported operator for ExecutionStatus: %s", op))
}

// toArray converts []bson.M → []any for $and/$or arrays.
func toArray(exprs []bson.M) []any {
	out := make([]any, 0, len(exprs))
	for _, e := range exprs {
		out = append(out, e)
	}
	return out
}

// FieldPath returns the visibility-document path for a search attribute
// field name (system attributes are top-level, custom ones live under
// search_attributes).
func FieldPath(name string) string {
	f, _ := resolveField(name)
	return f
}
