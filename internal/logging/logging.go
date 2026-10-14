// Package logging provides the plugin's uniform structured-log tags.
// Every store call that logs should include at least {store, operation};
// identifier tags (namespace_id, shard_id, workflow_id, run_id) are
// added per-call where applicable.
//
// Unlike metrics, namespace_id IS a valid log tag — logs are
// low-cardinality post-hoc analysis, not time-series aggregation.
// Credentials MUST NEVER appear in log lines.
package logging

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// WithStoreOp returns a logger pre-tagged with store + operation.
// A nil input logger becomes a Noop logger.
func WithStoreOp(lg log.Logger, store, op string) log.Logger {
	if lg == nil {
		lg = log.NewNoopLogger()
	}
	return log.With(lg,
		tag.NewStringTag("store", store),
		tag.NewStringTag("operation", op),
	)
}

// NamespaceTag builds a namespace_id tag for log lines.
func NamespaceTag(namespaceID string) tag.Tag {
	return tag.NewStringTag("namespace_id", namespaceID)
}

// WorkflowTag / RunTag / ShardTag use snake_case keys to match the
// plugin's BSON schema.
func WorkflowTag(id string) tag.Tag { return tag.NewStringTag("workflow_id", id) }
func RunTag(id string) tag.Tag      { return tag.NewStringTag("run_id", id) }
func ShardTag(id int32) tag.Tag     { return tag.NewInt32("shard_id", id) }
