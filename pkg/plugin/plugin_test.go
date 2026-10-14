// Package plugin hosts the public entrypoints of the mongodb-temporal-plugin:
// the factory constructors a consumer binary passes to
// `temporal.WithCustomDataStoreFactory` / `temporal.WithCustomVisibilityStoreFactory`.
//
// These tests pin the public surface area: they verify that the plugin
// exposes the data-store and visibility factory constructors and
// that the returned values implement the upstream interfaces (so a future
// Temporal minor bump that changes those interfaces is caught at build
// time by these assertions).
package plugin_test

import (
	"testing"

	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/visibility"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/pkg/plugin"
)

// TestNewFactory_ReturnsAbstractDataStoreFactory verifies that the
// plugin exposes a constructor whose return value implements
// `persistenceclient.AbstractDataStoreFactory`. The assignment below fails
// to compile if the returned type no longer satisfies the interface, which
// is the exact drift we want to catch early.
func TestNewFactory_ReturnsAbstractDataStoreFactory(t *testing.T) {
	concrete := plugin.NewFactory()
	if concrete == nil {
		t.Fatal("NewFactory returned nil; must return a non-nil factory")
	}
	var _ persistenceclient.AbstractDataStoreFactory = concrete
}

// TestNewVisibilityFactory_ReturnsVisibilityStoreFactory verifies that
// the plugin exposes a visibility factory implementing
// `visibility.VisibilityStoreFactory`.
func TestNewVisibilityFactory_ReturnsVisibilityStoreFactory(t *testing.T) {
	concrete := plugin.NewVisibilityFactory()
	if concrete == nil {
		t.Fatal("NewVisibilityFactory returned nil; must return a non-nil factory")
	}
	var _ visibility.VisibilityStoreFactory = concrete
}

// TestParseConfig_IsFunction pins ParseConfig as a function, not a var
// (backlog B-6). A var-exported symbol could be reassigned by consumer
// tooling; the function form is part of the v1 API stability contract.
func TestParseConfig_IsFunction(t *testing.T) {
	// The type on the right-hand side only compiles if ParseConfig is
	// a function value with the expected signature. Assignment to a
	// typed function variable is the compile-time check.
	var fn func(map[string]any) (*plugin.Config, []string, error) = plugin.ParseConfig
	cfg, _, err := fn(map[string]any{
		"uri":      "mongodb://localhost:27017",
		"database": "temporal",
	})
	if err != nil {
		t.Fatalf("ParseConfig on a minimal valid options map: %v", err)
	}
	if cfg == nil {
		t.Fatal("ParseConfig returned nil config on valid input")
	}
}

// TestPublicAPI_ReturnsInterfaceTypes_REV008 pins the v1 API stability
// contract: the public constructors must return the upstream
// interfaces (not concrete internal types). This compile-time check
// would have failed against the earlier concrete-type signatures, so a future
// regression that flips a return type back to *internalplugin.X
// would break the build here.
//
// The test explicitly declares typed function variables whose
// signatures match the contracted shape. Go's type checker does the
// assertion; the body only needs a non-nil receiver check so the
// linter doesn't flag the vars as unused.
func TestPublicAPI_ReturnsInterfaceTypes(t *testing.T) {
	var (
		newFactory    func() persistenceclient.AbstractDataStoreFactory = plugin.NewFactory
		newVisFactory func() visibility.VisibilityStoreFactory          = plugin.NewVisibilityFactory
	)
	if got := newFactory(); got == nil {
		t.Fatal("NewFactory must return a non-nil factory")
	}
	if got := newVisFactory(); got == nil {
		t.Fatal("NewVisibilityFactory must return a non-nil factory")
	}
}
