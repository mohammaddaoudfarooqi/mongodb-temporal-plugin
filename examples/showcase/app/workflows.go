package app

import (
	"errors"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/kb"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/support"
)

// TicketInput starts a ticket. SLABase is the P1 time to first
// response (lower priorities get doubling multiples).
type TicketInput struct {
	Ticket  support.Ticket `json:"ticket"`
	SLABase time.Duration  `json:"sla_base"`
}

// TicketState is the ticket's live state, served by the "ticket" query
// and returned when the ticket is resolved.
type TicketState struct {
	Ticket      support.Ticket `json:"ticket"`
	Category    string         `json:"category"`
	Priority    int            `json:"priority"`
	Status      string         `json:"status"`
	Agent       string         `json:"agent,omitempty"`
	Comments    []string       `json:"comments,omitempty"`
	Suggestions []kb.Hit       `json:"suggestions,omitempty"`
	Escalated   bool           `json:"escalated"`
	Resolution  string         `json:"resolution,omitempty"`
}

// TicketWorkflow runs one support ticket from submission to resolution:
// triage, store, suggest past fixes, wait for a person, escalate on an
// SLA breach, and feed the resolution back into the knowledge base.
func TicketWorkflow(ctx workflow.Context, in TicketInput) (TicketState, error) {
	logger := workflow.GetLogger(ctx)
	ctx = workflow.WithActivityOptions(ctx, activityOptions())
	var a *Activities
	st := TicketState{Ticket: in.Ticket, Status: support.StatusOpen}

	if err := workflow.SetQueryHandler(ctx, QueryTicket, func() (TicketState, error) { return st, nil }); err != nil {
		return st, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateComment,
		func(ctx workflow.Context, text string) (int, error) {
			st.Comments = append(st.Comments, strings.TrimSpace(text))
			return len(st.Comments), nil
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, text string) error {
			if strings.TrimSpace(text) == "" {
				return errors.New("comment is empty")
			}
			if st.Status == support.StatusResolved {
				return errors.New("ticket is resolved")
			}
			return nil
		}}); err != nil {
		return st, err
	}

	var tr TriageResult
	if err := workflow.ExecuteActivity(ctx, a.Triage, in.Ticket).Get(ctx, &tr); err != nil {
		return st, err
	}
	st.Category, st.Priority = tr.Category, tr.Priority
	if err := upsert(ctx, &st); err != nil {
		return st, err
	}
	doc := kb.Doc{ID: in.Ticket.ID, CustomerID: in.Ticket.CustomerID, Subject: in.Ticket.Subject, Body: in.Ticket.Body,
		Status: st.Status, Category: st.Category, Priority: st.Priority}
	if err := workflow.ExecuteActivity(ctx, a.StoreTicket, doc).Get(ctx, nil); err != nil {
		return st, err
	}
	// Suggestions are a help, not a requirement: a search outage must not
	// hold up the ticket.
	sctx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 3}})
	if err := workflow.ExecuteActivity(sctx, a.FindSimilar, in.Ticket.Subject+". "+in.Ticket.Body).Get(ctx, &st.Suggestions); err != nil {
		logger.Warn("similar-ticket search failed; continuing without suggestions", "error", err)
		st.Suggestions = nil
	}

	assignCh := workflow.GetSignalChannel(ctx, SignalAssign)
	resolveCh := workflow.GetSignalChannel(ctx, SignalResolve)
	slaCtx, cancelSLA := workflow.WithCancel(ctx)
	slaTimer := workflow.NewTimer(slaCtx, support.SLA(st.Priority, in.SLABase))
	slaRunning := true

	for st.Status != support.StatusResolved {
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(assignCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &st.Agent)
			st.Status = support.StatusAssigned
			if slaRunning {
				cancelSLA() // first response given in time
				slaRunning = false
			}
		})
		sel.AddReceive(resolveCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &st.Resolution)
			st.Status = support.StatusResolved
		})
		if slaRunning {
			sel.AddFuture(slaTimer, func(f workflow.Future) {
				slaRunning = false
				if f.Get(ctx, nil) != nil { // cancelled
					return
				}
				st.Escalated, st.Status = true, support.StatusEscalated
				st.Priority = support.Escalate(st.Priority)
				notice := Notice{TicketID: in.Ticket.ID, Priority: st.Priority, Reason: "no response within SLA"}
				if err := workflow.ExecuteActivity(ctx, a.Notify, notice).Get(ctx, nil); err != nil {
					logger.Warn("escalation notice failed", "error", err)
				}
			})
		}
		sel.Select(ctx)
		if st.Status != support.StatusResolved {
			if err := upsert(ctx, &st); err != nil {
				return st, err
			}
			if err := workflow.ExecuteActivity(ctx, a.SetStatus, StatusInput{ID: in.Ticket.ID, Status: st.Status, Priority: st.Priority}).Get(ctx, nil); err != nil {
				return st, err
			}
		}
	}
	cancelSLA()
	if err := workflow.ExecuteActivity(ctx, a.Resolve, ResolveInput{ID: in.Ticket.ID, Resolution: st.Resolution}).Get(ctx, nil); err != nil {
		return st, err
	}
	return st, upsert(ctx, &st)
}

func upsert(ctx workflow.Context, st *TicketState) error {
	return workflow.UpsertTypedSearchAttributes(ctx,
		CustomerIDKey.ValueSet(st.Ticket.CustomerID),
		CategoryKey.ValueSet(st.Category),
		PriorityKey.ValueSet(int64(st.Priority)),
		StatusKey.ValueSet(st.Status),
		SummaryKey.ValueSet(st.Ticket.Subject+" "+st.Ticket.Body),
	)
}

// SLAReportWorkflow runs on a schedule: it counts tickets by status and
// lists escalated ones, using visibility queries the plugin serves from
// MongoDB, and stores the report.
func SLAReportWorkflow(ctx workflow.Context) (Report, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOptions())
	var a *Activities
	var r Report
	err := workflow.ExecuteActivity(ctx, a.BuildReport).Get(ctx, &r)
	return r, err
}

// Workflows lists what the worker registers.
func Workflows() []any { return []any{TicketWorkflow, SLAReportWorkflow} }
