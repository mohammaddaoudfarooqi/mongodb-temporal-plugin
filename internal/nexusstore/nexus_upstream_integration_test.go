//go:build integration

package nexusstore_test

import (
	"sync/atomic"
	"testing"

	upstream "go.temporal.io/server/common/persistence/tests"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/nexusstore"
)

// TestUpstream_NexusEndpointSuite with the upstream
// behavioural suite (table-version CAS, version mismatch, not-found).
func TestUpstream_NexusEndpointSuite(t *testing.T) {
	var tableVersion atomic.Int64
	upstream.RunNexusEndpointTestSuite(t, nexusstore.New(startDB(t)), &tableVersion)
}
