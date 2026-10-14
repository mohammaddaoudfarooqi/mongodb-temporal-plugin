package scenarios

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/e2e/app"
)

func order(id, customer string, items int) app.Order {
	o := app.Order{ID: id, CustomerID: customer, Priority: 2}
	for i := 0; i < items; i++ {
		o.Items = append(o.Items, app.Item{SKU: fmt.Sprintf("sku-%d", i), Quantity: 1, Price: 10})
	}
	return o
}

func orderSaga(ctx context.Context, e *Env) error {
	id := e.id("order-saga")
	o := order(id, e.Prefix+"-saga", 3)
	o.FlakyPayment = true
	o.ApprovalThreshold, o.ApprovalTimeout = 5, 5*time.Minute
	run, err := e.start(ctx, id, app.OrderWorkflow, o)
	if err != nil {
		return err
	}
	// Query until the order is reserved and waiting for approval.
	if err := eventually(ctx, 30*time.Second, func() error {
		v, err := e.Client.QueryWorkflow(ctx, id, "", app.QueryStatus)
		if err != nil {
			return err
		}
		var s string
		if err := v.Get(&s); err != nil {
			return err
		}
		return expect(s == app.StatusReserved, "status %q, want RESERVED", s)
	}); err != nil {
		return err
	}
	// Update (validated), then the approve signal.
	h, err := e.Client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: id, UpdateName: app.UpdateAddItem, Args: []any{app.Item{SKU: "extra", Quantity: 2, Price: 5}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	var total float64
	if err := h.Get(ctx, &total); err != nil || total != 40 {
		return fmt.Errorf("update result %v, %v; want 40", total, err)
	}
	// A validator rejection surfaces as the update's outcome, on Get.
	bad, err := e.Client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: id, UpdateName: app.UpdateAddItem, Args: []any{app.Item{SKU: "bad", Quantity: 0}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err == nil {
		err = bad.Get(ctx, nil)
	}
	if err == nil {
		return errors.New("invalid update was accepted")
	}
	if err := e.Client.SignalWorkflow(ctx, id, "", app.SignalApprove, "e2e"); err != nil {
		return err
	}
	var res app.OrderResult
	if err := run.Get(ctx, &res); err != nil {
		return err
	}
	if err := expect(res.Status == app.StatusShipped && res.Total == 40, "result %+v", res); err != nil {
		return err
	}
	// The final search attributes are persisted with the execution.
	desc, err := e.Client.DescribeWorkflowExecution(ctx, id, "")
	if err != nil {
		return err
	}
	if _, ok := desc.WorkflowExecutionInfo.GetSearchAttributes().GetIndexedFields()[app.SAOrderStatus]; !ok {
		return errors.New("OrderStatus search attribute missing from describe")
	}
	return nil
}

func orderCompensation(ctx context.Context, e *Env) error {
	id := e.id("order-declined")
	o := order(id, e.Prefix+"-declined", 1)
	o.DeclinePayment = true
	run, err := e.start(ctx, id, app.OrderWorkflow, o)
	if err != nil {
		return err
	}
	var res app.OrderResult
	if err := run.Get(ctx, &res); err != nil {
		return err
	}
	return expect(res.Status == app.StatusCancelled && res.Compensated, "result %+v", res)
}

func signalWithStart(ctx context.Context, e *Env) error {
	id := e.id("signal-with-start")
	o := order(id, e.Prefix+"-sws", 2)
	o.ApprovalThreshold, o.ApprovalTimeout = 5, 5*time.Minute
	run, err := e.Client.SignalWithStartWorkflow(ctx, id, app.SignalApprove, "pre-approved",
		client.StartWorkflowOptions{ID: id, TaskQueue: app.TaskQueue}, app.OrderWorkflow, o)
	if err != nil {
		return err
	}
	var res app.OrderResult
	if err := run.Get(ctx, &res); err != nil {
		return err
	}
	return expect(res.Status == app.StatusShipped, "result %+v", res)
}

func fanOut(ctx context.Context, e *Env) error {
	run, err := e.start(ctx, e.id("fan-out"), app.FanOutWorkflow, 10)
	if err != nil {
		return err
	}
	var sum int
	if err := run.Get(ctx, &sum); err != nil {
		return err
	}
	return expect(sum == 110, "sum %d, want 110", sum)
}

func continueAsNew(ctx context.Context, e *Env) error {
	id := e.id("continue-as-new")
	first, err := e.start(ctx, id, app.CounterWorkflow, app.CounterState{Target: 25, Batch: 5})
	if err != nil {
		return err
	}
	// Following the chain from the first run reaches the final result.
	var st app.CounterState
	if err := e.Client.GetWorkflow(ctx, id, first.GetRunID()).Get(ctx, &st); err != nil {
		return err
	}
	if err := expect(st.Count == 25 && st.Runs == 5, "state %+v, want count 25 over 5 runs", st); err != nil {
		return err
	}
	// The current-run pointer must name the last run, which completed.
	desc, err := e.Client.DescribeWorkflowExecution(ctx, id, "")
	if err != nil {
		return err
	}
	info := desc.WorkflowExecutionInfo
	if info.GetExecution().GetRunId() == first.GetRunID() {
		return errors.New("current run is still the first run after continue-as-new")
	}
	return expect(info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		"current run status %s, want COMPLETED", info.GetStatus())
}

func cancellation(ctx context.Context, e *Env) error {
	id := e.id("cancel")
	run, err := e.start(ctx, id, app.LongTimerWorkflow, time.Hour)
	if err != nil {
		return err
	}
	if err := eventually(ctx, 30*time.Second, func() error {
		d, err := e.Client.DescribeWorkflowExecution(ctx, id, "")
		if err != nil {
			return err
		}
		return expect(len(d.GetPendingActivities()) == 0 && d.WorkflowExecutionInfo.GetHistoryLength() >= 5,
			"timer not started yet")
	}); err != nil {
		return err
	}
	if err := e.Client.CancelWorkflow(ctx, id, ""); err != nil {
		return err
	}
	err = run.Get(ctx, nil)
	var canceled *temporal.CanceledError
	if !errors.As(err, &canceled) {
		return fmt.Errorf("want cancellation, got %v", err)
	}
	cleaned := false
	iter := e.Client.GetWorkflowHistory(ctx, id, run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			return err
		}
		if ev.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			cleaned = true
		}
	}
	return expect(cleaned, "cleanup activity did not complete after cancellation")
}

func durableTimer(ctx context.Context, e *Env) error {
	run, err := e.start(ctx, e.id("timer"), app.LongTimerWorkflow, 3*time.Second)
	if err != nil {
		return err
	}
	start := time.Now()
	var out string
	if err := run.Get(ctx, &out); err != nil {
		return err
	}
	return expect(out == "slept" && time.Since(start) >= 2*time.Second, "timer returned %q after %s", out, time.Since(start))
}

func bigHistory(ctx context.Context, e *Env) error {
	const n = 300
	id := e.id("big-history")
	run, err := e.start(ctx, id, app.BigHistoryWorkflow, n)
	if err != nil {
		return err
	}
	var done int
	if err := run.Get(ctx, &done); err != nil {
		return err
	}
	if err := expect(done == n, "ran %d activities, want %d", done, n); err != nil {
		return err
	}
	// Read the whole history back (many pages) and replay it.
	hist := &historypb.History{}
	completed := 0
	iter := e.Client.GetWorkflowHistory(ctx, id, run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			return err
		}
		hist.Events = append(hist.Events, ev)
		if ev.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			completed++
		}
	}
	if err := expect(completed == n, "history has %d completed activities, want %d (events read: %d)", completed, n, len(hist.Events)); err != nil {
		return err
	}
	if last := hist.Events[len(hist.Events)-1].GetEventType(); last != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
		return fmt.Errorf("history ends with %s", last)
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(app.BigHistoryWorkflow)
	if err := replayer.ReplayWorkflowHistory(log.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), hist); err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	return nil
}

func failure(ctx context.Context, e *Env) error {
	run, err := e.start(ctx, e.id("failure"), app.FailingWorkflow, "e2e")
	if err != nil {
		return err
	}
	err = run.Get(ctx, nil)
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != "Boom" {
		return fmt.Errorf("want Boom application error, got %v", err)
	}
	return nil
}
