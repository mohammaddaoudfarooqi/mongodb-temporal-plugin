package converter

import (
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

// NewQueryConverter builds upstream's QueryConverter on every supported
// Temporal minor. The constructor's leading parameters
// are stable; 1.32 appended a metrics.Handler and a log.Logger. Go
// cannot select code by dependency version, so the trailing parameters
// are filled by type from what the compiled release declares.
func NewQueryConverter(
	storeQC query.StoreQueryConverter[bson.M],
	nsName namespace.Name,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	metricsHandler metrics.Handler,
	logger log.Logger,
) (*query.QueryConverter[bson.M], error) {
	if metricsHandler == nil {
		metricsHandler = metrics.NoopMetricsHandler
	}
	if logger == nil {
		logger = log.NewNoopLogger()
	}
	fn := reflect.ValueOf(query.NewQueryConverter[bson.M])
	ft := fn.Type()
	args := []reflect.Value{
		reflect.ValueOf(&storeQC).Elem(),
		reflect.ValueOf(nsName),
		reflect.ValueOf(saTypeMap),
		reflect.ValueOf(&saMapper).Elem(),
	}
	if ft.NumIn() < len(args) {
		return nil, fmt.Errorf("visibility: unsupported query.NewQueryConverter signature %s", ft)
	}
	extras := []any{metricsHandler, logger}
	for i := len(args); i < ft.NumIn(); i++ {
		in := ft.In(i)
		var arg reflect.Value
		for _, e := range extras {
			if v := reflect.ValueOf(e); v.Type().AssignableTo(in) {
				arg = v
				break
			}
		}
		if !arg.IsValid() {
			return nil, fmt.Errorf("visibility: query.NewQueryConverter parameter %d has unsupported type %s", i, in)
		}
		args = append(args, arg)
	}
	out := fn.Call(args)
	qc, ok := out[0].Interface().(*query.QueryConverter[bson.M])
	if !ok {
		return nil, fmt.Errorf("visibility: unexpected query.NewQueryConverter result %s", out[0].Type())
	}
	return qc, nil
}
