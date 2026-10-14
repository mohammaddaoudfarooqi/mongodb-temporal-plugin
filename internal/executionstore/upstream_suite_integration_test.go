//go:build integration

// Upstream behavioural suites from go.temporal.io/server/common/persistence/tests
// run against the shard and execution stores through upstream's own
// ShardManager / ExecutionManager.
package executionstore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	upstream "go.temporal.io/server/common/persistence/tests"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil/conformance"
)

func upstreamStores(t *testing.T) (*shardstore.Stub, *executionstore.Stub) {
	t.Helper()
	db := startDB(t)
	if err := conformance.EnsureAllIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return shardstore.New(db, "cluster0"), executionstore.New(db, "cluster0")
}

func TestUpstream_ShardSuite(t *testing.T) {
	shards, _ := upstreamStores(t)
	suite.Run(t, upstream.NewShardSuite(t, shards, serialization.NewSerializer(), log.NewTestLogger()))
}

func TestUpstream_ExecutionMutableStateSuite(t *testing.T) {
	shards, exec := upstreamStores(t)
	suite.Run(t, upstream.NewExecutionMutableStateSuite(t, shards, exec, serialization.NewSerializer(), log.NewTestLogger()))
}

func TestUpstream_ExecutionMutableStateTaskSuite(t *testing.T) {
	shards, exec := upstreamStores(t)
	suite.Run(t, upstream.NewExecutionMutableStateTaskSuite(t, shards, exec, serialization.NewSerializer(), log.NewTestLogger()))
}

func TestUpstream_HistoryEventsSuite(t *testing.T) {
	_, exec := upstreamStores(t)
	suite.Run(t, upstream.NewHistoryEventsSuite(t, exec, log.NewTestLogger()))
}
