// Package testutil hosts test doubles and harness builders shared across
// store packages. The conformance fixture in this file is the adapter
// that lets the upstream Temporal persistence conformance suite drive
// the plugin under test.
//
// Shape:
//   - NewConformanceCluster builds a `persistence-tests.PersistenceTestCluster`
//     backed by a testcontainers-managed MongoDB replica set.
//   - The cluster wires `plugin.NewFactory()` into the upstream
//     `client.DataStoreFactoryProvider` via
//     `config.CustomDatastoreConfig`.
//   - At milestone M0 the plugin's stores return Unimplemented, so
//     conformance tests fail expected — the baseline is a regression
//     guardrail, not a green-light.
//
// The heavy machinery lives under build tag `conformance`:
//   - conformance_fixture_conformance.go — depends on testcontainers and
//     the upstream persistence-tests package which pulls in a large
//     transitive surface.
//
// This file exposes only the zero-cost types that are safe to reference
// from unit tests without the build tag.
package testutil

// ConformanceBackend is the MongoDB topology the fixture connects to.
// It is intentionally a string alias so environment-based selection
// (TEMPORAL_TEST_DB=mongo, etc.) stays readable in logs.
type ConformanceBackend string

const (
	BackendMongoReplicaSet ConformanceBackend = "mongo-replica-set"
)
