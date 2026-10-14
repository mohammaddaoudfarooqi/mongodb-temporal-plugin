package converter

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// compact drops absent operands. Upstream's QueryConverter always calls
// BuildAndExpr(namespaceDivision, where) and either side is the zero
// value when absent (no WHERE clause, or an explicit division filter);
// a nil inside $and/$or or a compound clause is rejected by the server
// .
func compact(exprs []bson.M) []bson.M {
	out := make([]bson.M, 0, len(exprs))
	for _, e := range exprs {
		if len(e) > 0 {
			out = append(out, e)
		}
	}
	return out
}

// coerceValue converts the value types upstream's QueryConverter emits
// into the BSON types the visibility document stores. Datetime values
// arrive formatted with GetDatetimeFormat(); they are compared against
// BSON dates, so they must be parsed back.
func coerceValue(saType enumspb.IndexedValueType, value any) (any, error) {
	if saType != enumspb.INDEXED_VALUE_TYPE_DATETIME {
		return value, nil
	}
	switch v := value.(type) {
	case string:
		tm, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid datetime %q: %v", v, err))
		}
		return tm.UTC(), nil
	case []any:
		out := make([]any, 0, len(v))
		for _, e := range v {
			c, err := coerceValue(saType, e)
			if err != nil {
				return nil, err
			}
			out = append(out, c)
		}
		return out, nil
	}
	return value, nil
}
