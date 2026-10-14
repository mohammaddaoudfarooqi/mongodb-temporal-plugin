package app

import (
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func newEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{})
	for _, wf := range Workflows() {
		env.RegisterWorkflow(wf)
	}
	return env
}

func smallOrder() Order {
	return Order{ID: "o1", CustomerID: "c1", Items: []Item{{SKU: "a", Quantity: 2, Price: 10}}, Priority: 1}
}

func TestOrderWorkflow_HappyPath_Shipped(t *testing.T) {
	env := newEnv(t)
	o := smallOrder()
	o.FlakyPayment = true // retried twice by the activity retry policy
	env.ExecuteWorkflow(OrderWorkflow, o)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var res OrderResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusShipped || res.TrackingNumber != "trk-o1" || res.Total != 20 {
		t.Fatalf("result = %+v", res)
	}
}

func TestOrderWorkflow_LargeOrderWaitsForApproval(t *testing.T) {
	env := newEnv(t)
	o := smallOrder()
	o.ApprovalThreshold, o.ApprovalTimeout = 5, time.Hour
	env.RegisterDelayedCallback(func() {
		var status string
		v, err := env.QueryWorkflow(QueryStatus)
		if err != nil || v.Get(&status) != nil || status != StatusReserved {
			t.Errorf("status before approval = %q, %v", status, err)
		}
		env.SignalWorkflow(SignalApprove, "manager")
	}, time.Minute)
	env.ExecuteWorkflow(OrderWorkflow, o)
	var res OrderResult
	if err := env.GetWorkflowResult(&res); err != nil || res.Status != StatusShipped {
		t.Fatalf("result = %+v, %v", res, err)
	}
}

func TestOrderWorkflow_ApprovalTimeout_Compensates(t *testing.T) {
	env := newEnv(t)
	o := smallOrder()
	o.ApprovalThreshold, o.ApprovalTimeout = 5, time.Minute
	env.ExecuteWorkflow(OrderWorkflow, o)
	var res OrderResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCancelled || !res.Compensated {
		t.Fatalf("result = %+v", res)
	}
}

func TestOrderWorkflow_DeclinedPayment_Compensates(t *testing.T) {
	env := newEnv(t)
	o := smallOrder()
	o.DeclinePayment = true
	env.ExecuteWorkflow(OrderWorkflow, o)
	var res OrderResult
	if err := env.GetWorkflowResult(&res); err != nil || res.Status != StatusCancelled || !res.Compensated {
		t.Fatalf("result = %+v, %v", res, err)
	}
}

func TestOrderWorkflow_AddItemUpdate_ValidatesAndAdds(t *testing.T) {
	env := newEnv(t)
	o := smallOrder()
	o.ApprovalThreshold, o.ApprovalTimeout = 5, time.Hour
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddItem, "u-bad", &testsuite.TestUpdateCallback{
			OnReject:   func(error) {},
			OnAccept:   func() { t.Error("invalid item accepted") },
			OnComplete: func(any, error) {},
		}, Item{SKU: "bad", Quantity: 0, Price: 1})
		env.UpdateWorkflow(UpdateAddItem, "u-good", &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { t.Errorf("valid item rejected: %v", err) },
			OnAccept:   func() {},
			OnComplete: func(any, error) {},
		}, Item{SKU: "b", Quantity: 1, Price: 5})
		env.SignalWorkflow(SignalApprove, "manager")
	}, time.Minute)
	env.ExecuteWorkflow(OrderWorkflow, o)
	var res OrderResult
	if err := env.GetWorkflowResult(&res); err != nil || res.Total != 25 {
		t.Fatalf("result = %+v, %v", res, err)
	}
}

func TestFanOutWorkflow_SumsChildren(t *testing.T) {
	env := newEnv(t)
	env.ExecuteWorkflow(FanOutWorkflow, 5)
	var sum int
	if err := env.GetWorkflowResult(&sum); err != nil || sum != 2*(1+2+3+4+5) {
		t.Fatalf("sum = %d, %v", sum, err)
	}
}

func TestCounterWorkflow_ContinuesAsNew(t *testing.T) {
	env := newEnv(t)
	env.ExecuteWorkflow(CounterWorkflow, CounterState{Target: 10, Batch: 4})
	var can *workflow.ContinueAsNewError
	if err := env.GetWorkflowError(); !errors.As(err, &can) {
		t.Fatalf("want continue-as-new, got %v", err)
	}
}

func TestBigHistoryWorkflow_RunsAllActivities(t *testing.T) {
	env := newEnv(t)
	env.ExecuteWorkflow(BigHistoryWorkflow, 50)
	var n int
	if err := env.GetWorkflowResult(&n); err != nil || n != 50 {
		t.Fatalf("n = %d, %v", n, err)
	}
}

func TestLongTimerWorkflow_Cancelled_RunsCleanup(t *testing.T) {
	env := newEnv(t)
	var cleaned bool
	env.SetOnActivityCompletedListener(func(info *activity.Info, _ converter.EncodedValue, err error) {
		if info.ActivityType.Name == "Cleanup" && err == nil {
			cleaned = true
		}
	})
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Minute)
	env.ExecuteWorkflow(LongTimerWorkflow, time.Hour)
	var canceled *temporal.CanceledError
	if err := env.GetWorkflowError(); !errors.As(err, &canceled) {
		t.Fatalf("want CanceledError, got %v", err)
	}
	if !cleaned {
		t.Fatal("cleanup activity did not run after cancellation")
	}
}

func TestFailingWorkflow_Fails(t *testing.T) {
	env := newEnv(t)
	env.ExecuteWorkflow(FailingWorkflow, "x")
	var appErr *temporal.ApplicationError
	if err := env.GetWorkflowError(); !errors.As(err, &appErr) || appErr.Type() != "Boom" {
		t.Fatalf("want Boom application error, got %v", err)
	}
}
