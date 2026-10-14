//go:build integration

package visibility_test

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

func newFactoryStoreErr(uri, mode string) (store.VisibilityStore, error) {
	return visibility.NewFactory().NewVisibilityStore(
		config.CustomDatastoreConfig{Options: map[string]any{
			"uri": uri, "database": "temporal_vis_test", "visibilityMode": mode,
		}},
		searchattribute.NewTestProvider(),
		searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{Namespace: testNS}),
		nil, nil, nil, log.NewNoopLogger(), metrics.NoopMetricsHandler,
	)
}

func startDBClient(t *testing.T, uri string) *mongo.Database {
	t.Helper()
	p, err := internalmongo.NewClientProvider(context.Background(), internalmongo.ClientConfig{URI: uri, Database: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close(context.Background()) })
	return p.Database()
}
