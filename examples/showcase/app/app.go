// Package app is the support desk on Temporal: one durable workflow per
// ticket, running on Temporal's MongoDB persistence, with the tickets
// themselves and their hybrid search in the same MongoDB cluster.
package app

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is where the showcase worker polls.
const TaskQueue = "support-desk"

// Workflow message names.
const (
	SignalAssign  = "assign"
	SignalResolve = "resolve"
	UpdateComment = "comment"
	QueryTicket   = "ticket"
)

// Search attributes the ticket workflow maintains. Operators query them
// through Temporal visibility, which the plugin serves from MongoDB
// (Atlas Search in atlas-search mode); Summary is a Text attribute, so
// "Summary = 'refund'" is a full-text match.
const (
	SACustomerID = "CustomerId"
	SACategory   = "Category"
	SAPriority   = "Priority"
	SAStatus     = "TicketStatus"
	SASummary    = "Summary"
)

var (
	CustomerIDKey = temporal.NewSearchAttributeKeyKeyword(SACustomerID)
	CategoryKey   = temporal.NewSearchAttributeKeyKeyword(SACategory)
	PriorityKey   = temporal.NewSearchAttributeKeyInt64(SAPriority)
	StatusKey     = temporal.NewSearchAttributeKeyKeyword(SAStatus)
	SummaryKey    = temporal.NewSearchAttributeKeyString(SASummary)
)

// activityOptions: short activities, retried with backoff.
func activityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, MaximumAttempts: 5},
	}
}
