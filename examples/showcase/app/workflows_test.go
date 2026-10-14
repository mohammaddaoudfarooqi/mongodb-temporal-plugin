package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/kb"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/support"
)

func newEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	a := &Activities{}
	env.RegisterActivity(a)
	env.OnActivity(a.Triage, mock.Anything, mock.Anything).Return(TriageResult{Category: support.CategoryBilling, Priority: 2}, nil)
	env.OnActivity(a.StoreTicket, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.FindSimilar, mock.Anything, mock.Anything).Return([]kb.Hit{{ID: "kb-1", Subject: "Double charge", Resolution: "Refunded"}}, nil)
	env.OnActivity(a.SetStatus, mock.Anything, mock.Anything).Return(nil)
	return env
}

var ticket = TicketInput{Ticket: support.Ticket{ID: "t-1", CustomerID: "acme", Subject: "Charged twice", Body: "refund please"}, SLABase: time.Minute}

func TestTicket_AssignCommentResolve_ResolvesAndFeedsKnowledgeBase(t *testing.T) {
	env := newEnv(t)
	a := &Activities{}
	var resolvedWith string
	env.OnActivity(a.Resolve, mock.Anything, mock.Anything).Return(func(_ context.Context, r ResolveInput) error {
		resolvedWith = r.Resolution
		return nil
	})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalAssign, "ana") }, 30*time.Second)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateComment, "u1", &testsuite.TestUpdateCallback{
			OnAccept: func() {}, OnReject: func(err error) { t.Errorf("comment rejected: %v", err) },
			OnComplete: func(n any, err error) {
				if err != nil || n.(int) != 1 {
					t.Errorf("comment count %v, %v", n, err)
				}
			},
		}, "looking into it")
	}, 40*time.Second)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalResolve, "Refunded the duplicate charge") }, 50*time.Second)
	env.ExecuteWorkflow(TicketWorkflow, ticket)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var st TicketState
	if err := env.GetWorkflowResult(&st); err != nil {
		t.Fatal(err)
	}
	if st.Status != support.StatusResolved || st.Agent != "ana" || len(st.Comments) != 1 || st.Escalated {
		t.Fatalf("state %+v", st)
	}
	if resolvedWith != "Refunded the duplicate charge" {
		t.Fatalf("resolution not written to the knowledge base: %q", resolvedWith)
	}
	if len(st.Suggestions) != 1 || st.Suggestions[0].ID != "kb-1" {
		t.Fatalf("suggestions %+v", st.Suggestions)
	}
}

func TestTicket_NoResponseWithinSLA_EscalatesOnce(t *testing.T) {
	env := newEnv(t)
	a := &Activities{}
	escalations := 0
	env.OnActivity(a.Notify, mock.Anything, mock.Anything).Return(func(_ context.Context, _ Notice) error { escalations++; return nil })
	env.OnActivity(a.Resolve, mock.Anything, mock.Anything).Return(nil)
	// P2 with a 1m base: the SLA is 2m. Resolve long after it.
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalResolve, "fixed") }, 10*time.Minute)
	env.ExecuteWorkflow(TicketWorkflow, ticket)
	var st TicketState
	if err := env.GetWorkflowResult(&st); err != nil {
		t.Fatal(err)
	}
	if !st.Escalated || st.Priority != 1 || escalations != 1 {
		t.Fatalf("escalated=%v priority=%d notices=%d", st.Escalated, st.Priority, escalations)
	}
}

func TestTicket_CommentValidator_RejectsEmptyAndResolved(t *testing.T) {
	env := newEnv(t)
	a := &Activities{}
	env.OnActivity(a.Resolve, mock.Anything, mock.Anything).Return(nil)
	rejected := 0
	cb := func(id string) *testsuite.TestUpdateCallback {
		return &testsuite.TestUpdateCallback{OnAccept: func() { t.Errorf("update %s accepted", id) },
			OnReject: func(error) { rejected++ }, OnComplete: func(any, error) {}}
	}
	env.RegisterDelayedCallback(func() { env.UpdateWorkflow(UpdateComment, "empty", cb("empty"), "  ") }, 10*time.Second)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalResolve, "done") }, 20*time.Second)
	env.ExecuteWorkflow(TicketWorkflow, ticket)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if rejected != 1 {
		t.Fatalf("rejected %d updates, want 1", rejected)
	}
}

func TestTicket_SimilarSearchFailure_DoesNotBlockTheTicket(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	a := &Activities{}
	env.RegisterActivity(a)
	env.OnActivity(a.Triage, mock.Anything, mock.Anything).Return(TriageResult{Category: "general", Priority: 3}, nil)
	env.OnActivity(a.StoreTicket, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.FindSimilar, mock.Anything, mock.Anything).Return(nil, errors.New("search index not ready"))
	env.OnActivity(a.SetStatus, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.Resolve, mock.Anything, mock.Anything).Return(nil)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalResolve, "done") }, time.Minute)
	env.ExecuteWorkflow(TicketWorkflow, ticket)
	var st TicketState
	if err := env.GetWorkflowResult(&st); err != nil {
		t.Fatalf("a search outage must not fail the ticket: %v", err)
	}
	if st.Status != support.StatusResolved || len(st.Suggestions) != 0 {
		t.Fatalf("state %+v", st)
	}
}
