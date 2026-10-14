package app

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/kb"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/support"
)

// Activities touch the outside world: the knowledge base in MongoDB and
// Temporal visibility.
type Activities struct {
	Store     *kb.Store
	Temporal  client.Client
	Namespace string
	Events    EventSink
}

// EventSink records escalation notices (MongoDB in the worker).
type EventSink interface {
	InsertOne(ctx context.Context, doc any) error
}

// TriageResult is the triage decision.
type TriageResult struct {
	Category string `json:"category"`
	Priority int    `json:"priority"`
}

// Notice is an escalation.
type Notice struct {
	TicketID string `json:"ticket_id"`
	Priority int    `json:"priority"`
	Reason   string `json:"reason"`
}

// StatusInput / ResolveInput carry ticket changes.
type StatusInput struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Priority int    `json:"priority"`
}
type ResolveInput struct {
	ID         string `json:"id"`
	Resolution string `json:"resolution"`
}

// Report is the scheduled SLA report.
type Report struct {
	At        time.Time        `json:"at" bson:"at"`
	ByStatus  map[string]int64 `json:"by_status" bson:"by_status"`
	Escalated []string         `json:"escalated" bson:"escalated"`
}

// Triage classifies the ticket.
func (a *Activities) Triage(ctx context.Context, t support.Ticket) (TriageResult, error) {
	c, p := support.Triage(t.Subject, t.Body)
	return TriageResult{Category: c, Priority: p}, nil
}

// StoreTicket writes the ticket into the knowledge base.
func (a *Activities) StoreTicket(ctx context.Context, d kb.Doc) error { return a.Store.Upsert(ctx, d) }

// FindSimilar suggests resolved tickets like this one (hybrid search).
func (a *Activities) FindSimilar(ctx context.Context, text string) ([]kb.Hit, error) {
	return a.Store.Similar(ctx, text, 3)
}

// SetStatus mirrors the workflow's status into the ticket document.
func (a *Activities) SetStatus(ctx context.Context, in StatusInput) error {
	return a.Store.SetStatus(ctx, in.ID, in.Status, in.Priority)
}

// Resolve records the resolution; the ticket becomes an answer.
func (a *Activities) Resolve(ctx context.Context, in ResolveInput) error {
	return a.Store.Resolve(ctx, in.ID, in.Resolution)
}

// Notify records an escalation.
func (a *Activities) Notify(ctx context.Context, n Notice) error {
	activity.GetLogger(ctx).Info("ticket escalated", "ticket", n.TicketID, "priority", n.Priority)
	if a.Events == nil {
		return nil
	}
	return a.Events.InsertOne(ctx, bson.M{"type": "escalation", "ticket_id": n.TicketID, "priority": n.Priority,
		"reason": n.Reason, "at": time.Now().UTC()})
}

// BuildReport counts tickets by status and lists the running escalated
// ones, all through Temporal visibility queries the plugin answers.
func (a *Activities) BuildReport(ctx context.Context) (Report, error) {
	r := Report{At: time.Now().UTC(), ByStatus: map[string]int64{}}
	// Temporal visibility groups counts only by ExecutionStatus, so each
	// ticket status is its own count query.
	for _, status := range []string{support.StatusOpen, support.StatusAssigned, support.StatusEscalated, support.StatusResolved} {
		cnt, err := a.Temporal.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: a.Namespace, Query: fmt.Sprintf("WorkflowType = 'TicketWorkflow' AND %s = '%s'", SAStatus, status),
		})
		if err != nil {
			return r, fmt.Errorf("count %s tickets: %w", status, err)
		}
		r.ByStatus[status] = cnt.GetCount()
	}
	list, err := a.Temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: a.Namespace, Query: SAStatus + " = 'escalated' AND ExecutionStatus = 'Running'",
	})
	if err != nil {
		return r, fmt.Errorf("list escalations: %w", err)
	}
	for _, e := range list.GetExecutions() {
		r.Escalated = append(r.Escalated, e.GetExecution().GetWorkflowId())
	}
	if a.Events != nil {
		if err := a.Events.InsertOne(ctx, bson.M{"type": "sla_report", "report": r}); err != nil {
			return r, err
		}
	}
	return r, nil
}
