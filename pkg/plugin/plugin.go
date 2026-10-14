// Package plugin is the public entrypoint of mongodb-temporal-plugin.
// Consumer binaries import only this package; internal/* is out of reach.
//
// A Temporal server binary wires the plugin in with:
//
//	server, err := temporal.NewServer(
//	    temporal.WithCustomDataStoreFactory(plugin.NewFactory()),
//	    temporal.WithCustomVisibilityStoreFactory(plugin.NewVisibilityFactory()),
//	    temporal.WithConfigLoader(...),
//	)
//
// Both factories are cheap to construct and pre-network: they perform no
// MongoDB I/O until Temporal calls `NewFactory`/`NewVisibilityStore`, at
// which point connections are established.
package plugin

import (
	persistenceclient "go.temporal.io/server/common/persistence/client"
	upstreamvis "go.temporal.io/server/common/persistence/visibility"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/config"
	internalplugin "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/plugin"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

// Config is the parsed, validated plugin configuration. Re-exported
// from the internal/config package so consumer tooling (e.g. the
// temporal-mongodb-tool CLI) can reuse the same parser.
type Config = config.Config

// ParseConfig parses Temporal's CustomDatastoreConfig.Options map into a
// validated Config. Returns warnings for unknown options and
// non-majority write concerns. docs/operator/config-reference.md lists
// every option.
//
// Implemented as a function (not a var) so the public API is resilient
// to reassignment by consumer code or test shims — the signature is
// part of the v1 stability contract.
func ParseConfig(raw map[string]any) (*Config, []string, error) {
	return config.ParseConfig(raw)
}

// NewFactory returns the plugin's persistence factory. Pass it to
// temporal.WithCustomDataStoreFactory when building a Temporal server.
//
// The return type is the upstream
// persistenceclient.AbstractDataStoreFactory interface — not a
// concrete *internalplugin.AbstractFactory pointer — so internal types
// do not leak through the public API. This is the exact interface
// temporal.WithCustomDataStoreFactory expects, so consumer code
// written against the concrete type already used the interface
// implicitly and keeps compiling.
func NewFactory() persistenceclient.AbstractDataStoreFactory {
	return internalplugin.NewAbstract()
}

// NewVisibilityFactory returns the plugin's visibility factory. Pass it to
// temporal.WithCustomVisibilityStoreFactory.
//
// Like NewFactory, the return type is the upstream
// visibility.VisibilityStoreFactory interface. Internal concrete
// types stay internal.
func NewVisibilityFactory() upstreamvis.VisibilityStoreFactory {
	return visibility.NewFactory()
}
