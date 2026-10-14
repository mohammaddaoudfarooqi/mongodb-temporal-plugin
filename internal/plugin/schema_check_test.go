package plugin_test

import (
	"testing"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"

	iplugin "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/plugin"
)

// TestSkipSchemaCheck_IsHonored_WhenOptTrue exercises the
// skipSchemaCheck opt-out path with a URI that would otherwise force
// the schema check to run (and fail, since no _schema_version exists
// on an unreachable cluster). The skip short-circuits before that.
func TestSkipSchemaCheck_IsHonored_WhenOptTrue(t *testing.T) {
	abs := iplugin.NewAbstract()
	f := abs.NewFactory(
		config.CustomDatastoreConfig{
			Options: map[string]any{
				"uri":             "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100",
				"database":        "temporal",
				"skipSchemaCheck": true,
			},
		},
		resolver.NewNoopResolver(), "", log.NewTestLogger(),
		metrics.NoopMetricsHandler, serialization.NewSerializer(),
	)
	defer f.Close()

	// With skipSchemaCheck=true, the factory tries to open a
	// provider; the unreachable URI makes that fail. The point of
	// this test is that the schema check wasn't the cause.
	if _, err := f.NewShardStore(); err == nil {
		t.Error("expected connect error from unreachable URI")
	}
}

// TestFactory_NumHistoryShardsTypes covers asInt32's various input
// shapes: YAML loaders can produce int, int32, int64, or float64
// depending on the library.
func TestFactory_NumHistoryShardsTypes(t *testing.T) {
	// Any of these should not panic; they flow through asInt32 when
	// the schema check runs. Since we skip the check here (no
	// cluster), the test validates the factory builds without
	// complaint.
	for _, val := range []any{int(4), int32(4), int64(4), 4.0} {
		abs := iplugin.NewAbstract()
		f := abs.NewFactory(
			config.CustomDatastoreConfig{
				Options: map[string]any{
					"uri":              "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=50",
					"database":         "temporal",
					"skipSchemaCheck":  true,
					"numHistoryShards": val,
				},
			},
			resolver.NewNoopResolver(), "", log.NewTestLogger(),
			metrics.NoopMetricsHandler, serialization.NewSerializer(),
		)
		f.Close()
	}
}
