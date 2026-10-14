//go:build integration

package visibility_test

import (
	"testing"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/searchattribute"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

// TestFactory_IndexName_MatchesDatastoreConfig: the
// server seeds pre-allocated custom search attribute slots under
// CustomDataStoreConfig.IndexName, so the store must report that name.
func TestFactory_IndexName_MatchesDatastoreConfig(t *testing.T) {
	_, uri := startDBWithURI(t)
	for _, indexName := range []string{"", "temporal_visibility"} {
		s, err := visibility.NewFactory().NewVisibilityStore(
			config.CustomDatastoreConfig{IndexName: indexName, Options: map[string]any{
				"uri": uri, "database": "temporal_vis_test", "visibilityMode": "aggregation",
			}},
			searchattribute.NewTestProvider(),
			searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{Namespace: testNS}),
			nil, nil, nil, log.NewNoopLogger(), metrics.NoopMetricsHandler,
		)
		if err != nil {
			t.Fatal(err)
		}
		if got := s.GetIndexName(); got != indexName {
			t.Errorf("GetIndexName() = %q, want %q (the datastore's indexName)", got, indexName)
		}
		s.Close()
	}
}
