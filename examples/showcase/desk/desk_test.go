package desk_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/app"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/desk"
)

func TestOpen_StartsTicketWorkflowWithInput(t *testing.T) {
	c := &mocks.Client{}
	run := &mocks.WorkflowRun{}
	var got app.TicketInput
	c.On("ExecuteWorkflow", mock.Anything, mock.MatchedBy(func(o client.StartWorkflowOptions) bool {
		return strings.HasPrefix(o.ID, "ticket-acme-") && o.TaskQueue == app.TaskQueue
	}), mock.Anything, mock.Anything).Run(func(a mock.Arguments) { got = a.Get(3).(app.TicketInput) }).Return(run, nil)
	id, err := desk.Open(context.Background(), c, desk.NewTicket{Customer: "acme", Subject: "Charged twice", Body: "refund"}, 2*time.Minute)
	if err != nil || !strings.HasPrefix(id, "ticket-acme-") {
		t.Fatalf("id=%q err=%v", id, err)
	}
	if got.Ticket.ID != id || got.Ticket.Subject != "Charged twice" || got.SLABase != 2*time.Minute {
		t.Errorf("input = %+v", got)
	}
}

func TestOpen_RejectsInvalidInput(t *testing.T) {
	for _, nt := range []desk.NewTicket{
		{Customer: "acme"},                                    // no subject
		{Customer: "Not Valid!", Subject: "x"},                // customer becomes part of the workflow ID
		{Customer: "acme", Subject: strings.Repeat("s", 201)}, // too long
		{Customer: "acme", Subject: "x", Body: strings.Repeat("b", 5001)},
	} {
		if _, err := desk.Open(context.Background(), &mocks.Client{}, nt, time.Minute); !errors.Is(err, desk.ErrInvalid) {
			t.Errorf("%+v: err = %v, want ErrInvalid", nt, err)
		}
	}
}

func payload(t *testing.T, v any) *commonpb.Payload {
	t.Helper()
	p, err := converter.GetDefaultDataConverter().ToPayload(v)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestList_DecodesSearchAttributes(t *testing.T) {
	c := &mocks.Client{}
	started := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	c.On("ListWorkflow", mock.Anything, mock.MatchedBy(func(r *workflowservice.ListWorkflowExecutionsRequest) bool {
		return r.Namespace == "support" && r.Query == "Category = 'billing'" && r.PageSize == 25
	})).Return(&workflowservice.ListWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "ticket-acme-1"},
		Type:      &commonpb.WorkflowType{Name: "TicketWorkflow"},
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		StartTime: timestamppb.New(started),
		SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			app.SACustomerID: payload(t, "acme"), app.SACategory: payload(t, "billing"),
			app.SAPriority: payload(t, 2), app.SAStatus: payload(t, "triaged"), app.SASummary: payload(t, "Charged twice"),
		}},
	}}}, nil)
	got, err := desk.List(context.Background(), c, "support", "Category = 'billing'", 25)
	if err != nil || len(got) != 1 {
		t.Fatalf("got %v, %v", got, err)
	}
	s := got[0]
	if s.ID != "ticket-acme-1" || s.Workflow != "TicketWorkflow" || s.Status != "Running" || !s.Started.Equal(started) ||
		s.CustomerID != "acme" || s.Category != "billing" || s.Priority != 2 || s.TicketStatus != "triaged" || s.Summary != "Charged twice" {
		t.Errorf("summary = %+v", s)
	}
}

func TestComment_ReturnsCommentCount(t *testing.T) {
	c := &mocks.Client{}
	h := &mocks.WorkflowUpdateHandle{}
	h.On("Get", mock.Anything, mock.Anything).Run(func(a mock.Arguments) { *(a.Get(1).(*int)) = 3 }).Return(nil)
	c.On("UpdateWorkflow", mock.Anything, mock.MatchedBy(func(o client.UpdateWorkflowOptions) bool {
		return o.WorkflowID == "t1" && o.UpdateName == app.UpdateComment && o.WaitForStage == client.WorkflowUpdateStageCompleted
	})).Return(h, nil)
	n, err := desk.Comment(context.Background(), c, "t1", "looking")
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestAssignAndResolve_SignalTheWorkflow(t *testing.T) {
	c := &mocks.Client{}
	c.On("SignalWorkflow", mock.Anything, "t1", "", app.SignalAssign, "dana").Return(nil).Once()
	c.On("SignalWorkflow", mock.Anything, "t1", "", app.SignalResolve, "refunded").Return(nil).Once()
	if err := desk.Assign(context.Background(), c, "t1", "dana"); err != nil {
		t.Fatal(err)
	}
	if err := desk.Resolve(context.Background(), c, "t1", "refunded"); err != nil {
		t.Fatal(err)
	}
	if err := desk.Resolve(context.Background(), c, "t1", " "); err == nil {
		t.Error("an empty resolution was sent")
	}
	c.AssertExpectations(t)
}
