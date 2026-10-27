// Package desk is the support desk's client side: opening tickets,
// acting on them and finding them. The CLI and the web UI share it.
package desk

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/app"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/support"
)

// Client is the part of the Temporal client the desk uses;
// client.Client satisfies it.
type Client interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error)
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error
	UpdateWorkflow(ctx context.Context, options client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error)
	QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...any) (converter.EncodedValue, error)
	ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error)
}

// Input limits. The customer ID becomes part of the workflow ID.
const (
	MaxSubject = 200
	MaxBody    = 5000
	MaxText    = 2000
)

var customerID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)

// ErrInvalid matches the errors desk returns for invalid input, as
// opposed to errors from Temporal: errors.Is(err, ErrInvalid).
var ErrInvalid = errors.New("invalid input")

type invalid string

func (e invalid) Error() string        { return string(e) }
func (e invalid) Is(target error) bool { return target == ErrInvalid }

func invalidf(format string, args ...any) error { return invalid(fmt.Sprintf(format, args...)) }

// NewTicket is a ticket to open.
type NewTicket struct {
	Customer string `json:"customer"`
	Subject  string `json:"subject"`
	Body     string `json:"body"`
}

// Open starts a ticket workflow and returns its workflow ID.
func Open(ctx context.Context, c Client, t NewTicket, slaBase time.Duration) (string, error) {
	t.Subject, t.Body = strings.TrimSpace(t.Subject), strings.TrimSpace(t.Body)
	switch {
	case !customerID.MatchString(t.Customer):
		return "", invalidf("customer must be 1-40 lowercase letters, digits or dashes")
	case t.Subject == "":
		return "", invalidf("subject is required")
	case len(t.Subject) > MaxSubject:
		return "", invalidf("subject is longer than %d characters", MaxSubject)
	case len(t.Body) > MaxBody:
		return "", invalidf("body is longer than %d characters", MaxBody)
	}
	id := fmt.Sprintf("ticket-%s-%d", t.Customer, time.Now().UnixNano()%1e9)
	_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: app.TaskQueue}, app.TicketWorkflow,
		app.TicketInput{Ticket: support.Ticket{ID: id, CustomerID: t.Customer, Subject: t.Subject, Body: t.Body}, SLABase: slaBase})
	return id, err
}

func text(s, what string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", invalidf("%s is required", what)
	}
	if len(s) > MaxText {
		return "", invalidf("%s is longer than %d characters", what, MaxText)
	}
	return s, nil
}

// Assign signals the ticket that an agent has taken it.
func Assign(ctx context.Context, c Client, id, agent string) error {
	agent, err := text(agent, "agent")
	if err != nil {
		return err
	}
	return c.SignalWorkflow(ctx, id, "", app.SignalAssign, agent)
}

// Resolve signals the ticket's resolution.
func Resolve(ctx context.Context, c Client, id, resolution string) error {
	resolution, err := text(resolution, "resolution")
	if err != nil {
		return err
	}
	return c.SignalWorkflow(ctx, id, "", app.SignalResolve, resolution)
}

// Comment adds a comment through the workflow's validated update and
// returns the ticket's comment count. The workflow's validator decides
// what is accepted, so the text is passed as given.
func Comment(ctx context.Context, c Client, id, comment string) (int, error) {
	if len(comment) > MaxText {
		return 0, invalidf("comment is longer than %d characters", MaxText)
	}
	h, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: id, UpdateName: app.UpdateComment,
		Args: []any{comment}, WaitForStage: client.WorkflowUpdateStageCompleted})
	if err != nil {
		return 0, err
	}
	var n int
	err = h.Get(ctx, &n)
	return n, err
}

// State returns the ticket's live state through the workflow's query.
func State(ctx context.Context, c Client, id string) (app.TicketState, error) {
	var st app.TicketState
	v, err := c.QueryWorkflow(ctx, id, "", app.QueryTicket)
	if err != nil {
		return st, err
	}
	return st, v.Get(&st)
}

// Summary is one workflow as the visibility store lists it.
type Summary struct {
	ID           string     `json:"id"`
	Workflow     string     `json:"workflow"`
	Status       string     `json:"status"`
	Started      time.Time  `json:"started"`
	Closed       *time.Time `json:"closed,omitempty"`
	CustomerID   string     `json:"customer_id,omitempty"`
	Category     string     `json:"category,omitempty"`
	Priority     int64      `json:"priority,omitempty"`
	TicketStatus string     `json:"ticket_status,omitempty"`
	Summary      string     `json:"summary,omitempty"`
}

// List runs a visibility query (Temporal's list filter syntax) and
// returns up to limit workflows.
func List(ctx context.Context, c Client, namespace, query string, limit int) ([]Summary, error) {
	resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: namespace, Query: query, PageSize: int32(limit)})
	if err != nil {
		return nil, err
	}
	out := make([]Summary, 0, len(resp.GetExecutions()))
	for _, e := range resp.GetExecutions() {
		s := Summary{
			ID:       e.GetExecution().GetWorkflowId(),
			Workflow: e.GetType().GetName(),
			Status:   e.GetStatus().String(),
			Started:  e.GetStartTime().AsTime(),
		}
		if e.GetCloseTime() != nil {
			t := e.GetCloseTime().AsTime()
			s.Closed = &t
		}
		f := e.GetSearchAttributes().GetIndexedFields()
		decode(f[app.SACustomerID], &s.CustomerID)
		decode(f[app.SACategory], &s.Category)
		decode(f[app.SAPriority], &s.Priority)
		decode(f[app.SAStatus], &s.TicketStatus)
		decode(f[app.SASummary], &s.Summary)
		out = append(out, s)
	}
	return out, nil
}

// decode leaves v unchanged when the attribute is absent or unreadable.
func decode(p *commonpb.Payload, v any) {
	if p != nil {
		_ = converter.GetDefaultDataConverter().FromPayload(p, v)
	}
}

// Find returns the IDs of the workflows a visibility query matches.
func Find(ctx context.Context, c Client, namespace, query string) ([]string, error) {
	list, err := List(ctx, c, namespace, query, 100)
	ids := make([]string, len(list))
	for i, s := range list {
		ids[i] = s.ID
	}
	return ids, err
}

// Report runs the SLA report workflow once and returns its result.
func Report(ctx context.Context, c Client) (app.Report, error) {
	var r app.Report
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: fmt.Sprintf("sla-report-now-%d", time.Now().UnixNano()), TaskQueue: app.TaskQueue}, app.SLAReportWorkflow)
	if err != nil {
		return r, err
	}
	return r, run.Get(ctx, &r)
}
