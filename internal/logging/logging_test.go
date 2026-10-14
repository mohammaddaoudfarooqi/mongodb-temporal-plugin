package logging_test

import (
	"testing"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/logging"
)

// TestWithStoreOp_NilLogger_Noop ensures a nil input Logger is tolerated.
func TestWithStoreOp_NilLogger_Noop(t *testing.T) {
	lg := logging.WithStoreOp(nil, "shardstore", "UpdateShard")
	if lg == nil {
		t.Fatal("WithStoreOp should never return nil")
	}
	// Exercise the returned logger — must not panic.
	lg.Info("ok", logging.NamespaceTag("ns-1"),
		logging.WorkflowTag("wf-1"),
		logging.RunTag("run-1"),
		logging.ShardTag(42))
}

// TestTagBuilders_KeyShape guards the snake_case convention.
func TestTagBuilders_KeyShape(t *testing.T) {
	if logging.NamespaceTag("x").Key() != "namespace_id" {
		t.Errorf("NamespaceTag key: %q", logging.NamespaceTag("x").Key())
	}
	if logging.WorkflowTag("x").Key() != "workflow_id" {
		t.Errorf("WorkflowTag key: %q", logging.WorkflowTag("x").Key())
	}
	if logging.RunTag("x").Key() != "run_id" {
		t.Errorf("RunTag key: %q", logging.RunTag("x").Key())
	}
	if logging.ShardTag(1).Key() != "shard_id" {
		t.Errorf("ShardTag key: %q", logging.ShardTag(1).Key())
	}
}
