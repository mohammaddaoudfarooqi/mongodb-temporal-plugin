package plugin_test

import (
	"strings"
	"testing"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"

	iplugin "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/plugin"
)

// TestEnsureProvider_RedactedErrorOnBadURI:
// when the client provider fails to initialise, the error returned to
// the caller must not contain any credential material. A URI with an
// inline password is the canonical source of credential leakage — we
// supply one, force initialisation to fail (unreachable host,
// serverSelectionTimeoutMS=50 to bail fast), and assert the sentinel
// password never appears in the error text.
func TestEnsureProvider_RedactedErrorOnBadURI(t *testing.T) {
	const sentinel = "s3cretP@ss"
	uri := "mongodb://user:" + sentinel + "@127.0.0.1:1/?serverSelectionTimeoutMS=50"

	abs := iplugin.NewAbstract()
	f := abs.NewFactory(
		config.CustomDatastoreConfig{
			Options: map[string]any{
				"uri":             uri,
				"database":        "temporal",
				"skipSchemaCheck": true,
			},
		},
		resolver.NewNoopResolver(), "",
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		serialization.NewSerializer(),
	)
	defer f.Close()

	_, err := f.NewShardStore()
	if err == nil {
		t.Fatal("expected init error on unreachable URI; got nil")
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Errorf("credential leaked into error message: %q", err.Error())
	}
}
