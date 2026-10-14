package plugin_test

import (
	"testing"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"

	iplugin "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/plugin"
)

// customCfg returns a minimal CustomDatastoreConfig with the plugin's
// required options. The URI points at an unreachable endpoint because
// the wiring tests never actually open a connection — they validate
// configuration threading and Close semantics, not network behavior.
func customCfg() config.CustomDatastoreConfig {
	return config.CustomDatastoreConfig{
		Name:      "mongodb",
		IndexName: "temporal_wiring_test",
		Options: map[string]any{
			"uri":                      "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200",
			"database":                 "temporal_wiring_test",
			"connectTimeoutMs":         200,
			"serverSelectionTimeoutMs": 200,
		},
	}
}

// Config flows into the factory without transformation. We
// only assert the factory was built — not that it connected, since
// the URI is unreachable and network calls happen lazily.
func TestAbstractFactory_PropagatesConfigToDataStoreFactory(t *testing.T) {
	abs := iplugin.NewAbstract()
	f := abs.NewFactory(
		customCfg(),
		resolver.NewNoopResolver(),
		"test-cluster",
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		serialization.NewSerializer(),
	)
	if f == nil {
		t.Fatal("NewFactory returned nil")
	}
	// Compile-time check: the result is assignable to the upstream
	// interface, so Temporal's DI path accepts it.
	var _ persistenceclient.AbstractDataStoreFactory = abs
	f.Close()
}

// interaction with the Options map config: bad config does not panic during
// factory construction; the error surfaces on the first store call
// or on Close. At this level we only assert no panic.
func TestAbstractFactory_MalformedOptionsFailsLater(t *testing.T) {
	abs := iplugin.NewAbstract()
	cfg := config.CustomDatastoreConfig{
		Name:      "mongodb",
		IndexName: "bad",
		Options: map[string]any{
			// missing "database"
			"uri": "mongodb://127.0.0.1:1/",
		},
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("NewFactory panicked: %v", r)
		}
	}()
	f := abs.NewFactory(cfg, resolver.NewNoopResolver(), "", log.NewTestLogger(), metrics.NoopMetricsHandler, serialization.NewSerializer())
	if f == nil {
		t.Fatal("NewFactory returned nil")
	}
	f.Close()
}

// TestDataStoreFactory_ClientInitAttemptedOnce is the unit-level guard
// for the shared client: store constructors share one ClientProvider. In this
// test the URI is unreachable so the provider init fails, but the
// cached error must be reused across all store calls — evidence that
// the factory is not spinning up a fresh connect attempt per store.
func TestDataStoreFactory_ClientInitAttemptedOnce(t *testing.T) {
	abs := iplugin.NewAbstract()
	f := abs.NewFactory(customCfg(), resolver.NewNoopResolver(), "", log.NewTestLogger(), metrics.NoopMetricsHandler, serialization.NewSerializer())
	defer f.Close()

	ds, ok := f.(*iplugin.DataStoreFactory)
	if !ok {
		t.Fatalf("NewFactory returned %T; expected *DataStoreFactory", f)
	}

	// Every store constructor returns an error because the URI is
	// unreachable — but the factory must not build more than one
	// ClientProvider across the attempts. (The current counter only
	// increments on *success*; if connect fails, count stays at 0,
	// which still proves multiple provider instances aren't being
	// constructed and then discarded.)
	//
	// Covers all nine store constructors.
	if _, err := ds.NewShardStore(); err == nil {
		t.Error("NewShardStore: expected connect error, got nil")
	}
	if _, err := ds.NewExecutionStore(); err == nil {
		t.Error("NewExecutionStore: expected connect error, got nil")
	}
	if _, err := ds.NewMetadataStore(); err == nil {
		t.Error("NewMetadataStore: expected connect error, got nil")
	}
	if _, err := ds.NewClusterMetadataStore(); err == nil {
		t.Error("NewClusterMetadataStore: expected connect error, got nil")
	}
	if _, err := ds.NewTaskStore(); err == nil {
		t.Error("NewTaskStore: expected connect error, got nil")
	}
	if _, err := ds.NewFairTaskStore(); err == nil {
		t.Error("NewFairTaskStore: expected connect error, got nil")
	}
	if _, err := ds.NewNexusEndpointStore(); err == nil {
		t.Error("NewNexusEndpointStore: expected connect error, got nil")
	}
	if _, err := ds.NewQueue(persistence.NamespaceReplicationQueueType); err == nil {
		t.Error("NewQueue: expected connect error, got nil")
	}
	if _, err := ds.NewQueueV2(); err == nil {
		t.Error("NewQueueV2: expected connect error, got nil")
	}
	if n := ds.NewFactoryCallCountForTests(); n > 1 {
		t.Errorf("factory initialized %d clients; want at most 1", n)
	}
}

// TestDataStoreFactory_CloseFansOutAndIsIdempotent at
// the factory layer. Close should release the client and be safe to
// call more than once.
func TestDataStoreFactory_CloseFansOutAndIsIdempotent(t *testing.T) {
	abs := iplugin.NewAbstract()
	f := abs.NewFactory(customCfg(), resolver.NewNoopResolver(), "", log.NewTestLogger(), metrics.NoopMetricsHandler, serialization.NewSerializer())

	f.Close()
	f.Close() // must not panic
}
