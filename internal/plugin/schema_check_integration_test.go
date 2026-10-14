//go:build integration

// Integration tests for the startup schema-version check.
// A live Mongo replica set is required; the check runs against a real
// _schema_version singleton rather than a mock.
package plugin_test

import (
	"context"
	"strings"
	"testing"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	iplugin "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/plugin"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/schema"
)

// buildFactory wraps NewFactory with a caller-supplied options map;
// uri + database are threaded automatically from the testcontainer.
func buildSchemaCheckFactory(t *testing.T, uri string, extra map[string]any) *iplugin.DataStoreFactory {
	t.Helper()
	opts := map[string]any{
		"uri":      uri,
		"database": "temporal",
	}
	for k, v := range extra {
		opts[k] = v
	}
	f := iplugin.NewAbstract().NewFactory(
		config.CustomDatastoreConfig{
			Name:      "mongodb",
			IndexName: "temporal",
			Options:   opts,
		},
		resolver.NewNoopResolver(),
		"test-cluster",
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		serialization.NewSerializer(),
	)
	return f.(*iplugin.DataStoreFactory)
}

// TestSchemaCheck_AbsentSingleton_Refuses: without
// _schema_version the plugin refuses to vend stores and surfaces a
// message pointing at setup-schema.
func TestSchemaCheck_AbsentSingleton_Refuses(t *testing.T) {
	uri := startReplicaSet(t)
	fac := buildSchemaCheckFactory(t, uri, nil)
	defer fac.Close()
	_, err := fac.NewShardStore()
	if err == nil {
		t.Fatal("expected refusal when _schema_version absent")
	}
	if !strings.Contains(err.Error(), "setup-schema") {
		t.Errorf("error should point at setup-schema; got: %v", err)
	}
}

// TestSchemaCheck_StateNotReady_Refuses: upgrading
// or failed states halt startup.
func TestSchemaCheck_StateNotReady_Refuses(t *testing.T) {
	// Spin up a mongo and pre-populate _schema_version in a bad state.
	uri := startReplicaSet(t)
	ctx := context.Background()
	provider, err := internalmongo.NewClientProvider(ctx, internalmongo.ClientConfig{
		URI: uri, Database: "temporal", AppName: "schema-check-seed",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close(ctx)

	// First initialise it normally.
	if err := schema.WriteInitialMetadata(ctx, provider.Database(), "1.0.0", 4); err != nil {
		t.Fatal(err)
	}
	// Then flip the state to failed.
	if err := schema.SetState(ctx, provider.Database(), "failed-from-1.0.0-to-1.1.0", "1.0.0"); err != nil {
		t.Fatal(err)
	}
	_ = provider.Close(ctx)

	// Build the factory against the same URI — without skipSchemaCheck.
	fac := buildSchemaCheckFactory(t, uri, nil)
	defer fac.Close()
	_, err = fac.NewShardStore()
	if err == nil {
		t.Fatal("expected refusal on failed-state schema metadata")
	}
	if !strings.Contains(err.Error(), "failed-from-1.0.0-to-1.1.0") {
		t.Errorf("error should call out the bad state: %v", err)
	}
}

func TestSchemaCheck_NumHistoryShardsMismatch_Refuses(t *testing.T) {
	uri := startReplicaSet(t)
	ctx := context.Background()
	provider, _ := internalmongo.NewClientProvider(ctx, internalmongo.ClientConfig{
		URI: uri, Database: "temporal", AppName: "schema-check-seed",
	})
	defer provider.Close(ctx)
	if err := schema.WriteInitialMetadata(ctx, provider.Database(), "1.0.0", 4); err != nil {
		t.Fatal(err)
	}
	_ = provider.Close(ctx)

	fac := buildSchemaCheckFactory(t, uri, map[string]any{
		"numHistoryShards": 16, // mismatch: recorded is 4
	})
	defer fac.Close()
	_, err := fac.NewShardStore()
	if err == nil {
		t.Fatal("expected refusal on numHistoryShards mismatch")
	}
	if !strings.Contains(err.Error(), "numHistoryShards") {
		t.Errorf("error should mention numHistoryShards: %v", err)
	}
}

// TestSchemaCheck_Ready_Succeeds is the happy path.
func TestSchemaCheck_Ready_Succeeds(t *testing.T) {
	uri := startReplicaSet(t)
	ctx := context.Background()
	provider, _ := internalmongo.NewClientProvider(ctx, internalmongo.ClientConfig{
		URI: uri, Database: "temporal", AppName: "schema-check-seed",
	})
	if err := schema.WriteInitialMetadata(ctx, provider.Database(), "1.0.0", 4); err != nil {
		t.Fatal(err)
	}
	_ = provider.Close(ctx)

	fac := buildSchemaCheckFactory(t, uri, map[string]any{
		"numHistoryShards": 4, // matches recorded
	})
	defer fac.Close()
	if _, err := fac.NewShardStore(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
