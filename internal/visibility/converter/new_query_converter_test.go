package converter_test

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility/converter"
)

// TestNewQueryConverter_AcrossTemporalMinors: the
// adapter builds upstream's QueryConverter whatever trailing parameters
// the compiled Temporal minor declares (1.32 added a metrics handler and
// a logger), and the converter works end to end.
func TestNewQueryConverter_AcrossTemporalMinors(t *testing.T) {
	qc, err := converter.NewQueryConverter(converter.NewBSONConverter(), namespace.Name("ns"),
		searchattribute.TestNameTypeMap(), &searchattribute.TestMapper{Namespace: "ns"},
		metrics.NoopMetricsHandler, log.NewNoopLogger())
	if err != nil {
		t.Fatal(err)
	}
	params, err := qc.Convert("WorkflowId = 'a' AND AliasForInt01 > 3")
	if err != nil {
		t.Fatal(err)
	}
	if len(params.QueryExpr) == 0 {
		t.Fatal("empty expression")
	}
	var _ bson.M = params.QueryExpr
}
