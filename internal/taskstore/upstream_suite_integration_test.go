//go:build integration

// Upstream behavioural suites from go.temporal.io/server/common/persistence/tests
// run against this store. They are the authoritative
// contract tests the SQL and Cassandra backends also pass.
package taskstore_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	upstream "go.temporal.io/server/common/persistence/tests"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/taskstore"
)

func TestUpstream_TaskQueueSuite(t *testing.T) {
	suite.Run(t, upstream.NewTaskQueueSuite(t, taskstore.New(startDB(t)), log.NewTestLogger()))
}

func TestUpstream_TaskQueueTaskSuite(t *testing.T) {
	suite.Run(t, upstream.NewTaskQueueTaskSuite(t, taskstore.New(startDB(t)), log.NewTestLogger()))
}

func TestUpstream_TaskQueueUserDataSuite(t *testing.T) {
	suite.Run(t, upstream.NewTaskQueueUserDataSuite(t, taskstore.New(startDB(t)), log.NewTestLogger()))
}

func TestUpstream_TaskQueueFairTaskSuite(t *testing.T) {
	suite.Run(t, upstream.NewTaskQueueFairTaskSuite(t, taskstore.NewFair(startDB(t)), log.NewTestLogger()))
}
