package app

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Workflows lists every workflow the worker registers.
func Workflows() []any {
	return []any{
		OrderWorkflow, FanOutWorkflow, ProcessItemWorkflow, CounterWorkflow,
		BigHistoryWorkflow, LongTimerWorkflow, FailingWorkflow, ScheduledWorkflow,
	}
}

var act *Activities // method values for ExecuteActivity

func activityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		HeartbeatTimeout:    10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			BackoffCoefficient: 2,
			MaximumAttempts:    5,
		},
	}
}

// OrderWorkflow is a saga: reserve → (approve) → charge → ship, with
// compensation on decline or approval timeout. It upserts custom search
// attributes, answers a status query, accepts an add-item update and an
// approve signal.
func OrderWorkflow(ctx workflow.Context, order Order) (OrderResult, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOptions())
	status := ""
	setStatus := func(s string) error {
		status = s
		return workflow.UpsertTypedSearchAttributes(ctx, OrderStatusKey.ValueSet(s))
	}
	if err := workflow.SetQueryHandler(ctx, QueryStatus, func() (string, error) { return status, nil }); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateAddItem,
		func(ctx workflow.Context, it Item) (float64, error) {
			order.Items = append(order.Items, it)
			return order.Total(), workflow.UpsertTypedSearchAttributes(ctx, OrderTotalKey.ValueSet(order.Total()))
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, it Item) error {
			if it.Quantity <= 0 || it.Price < 0 {
				return fmt.Errorf("invalid item %+v", it)
			}
			return nil
		}}); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.UpsertTypedSearchAttributes(ctx,
		CustomerIDKey.ValueSet(order.CustomerID),
		OrderTotalKey.ValueSet(order.Total()),
		PriorityKey.ValueSet(int64(order.Priority)),
	); err != nil {
		return OrderResult{}, err
	}

	var reservation string
	if err := workflow.ExecuteActivity(ctx, act.ReserveInventory, order).Get(ctx, &reservation); err != nil {
		return OrderResult{}, err
	}
	if err := setStatus(StatusReserved); err != nil {
		return OrderResult{}, err
	}

	compensate := func() (OrderResult, error) {
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		if err := workflow.ExecuteActivity(dctx, act.ReleaseInventory, reservation).Get(dctx, nil); err != nil {
			return OrderResult{}, err
		}
		if err := setStatus(StatusCancelled); err != nil {
			return OrderResult{}, err
		}
		return OrderResult{Status: StatusCancelled, Total: order.Total(), Compensated: true}, nil
	}

	if order.ApprovalThreshold > 0 && order.Total() > order.ApprovalThreshold {
		approved := false
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(workflow.GetSignalChannel(ctx, SignalApprove), func(c workflow.ReceiveChannel, _ bool) {
			var by string
			c.Receive(ctx, &by)
			approved = true
		})
		timer := workflow.NewTimer(ctx, order.ApprovalTimeout)
		sel.AddFuture(timer, func(workflow.Future) {})
		sel.Select(ctx)
		if !approved {
			return compensate()
		}
		if err := setStatus(StatusApproved); err != nil {
			return OrderResult{}, err
		}
	}

	var payment string
	if err := workflow.ExecuteActivity(ctx, act.ChargePayment, order).Get(ctx, &payment); err != nil {
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) && appErr.Type() == "PaymentDeclined" {
			return compensate()
		}
		return OrderResult{}, err
	}
	if err := setStatus(StatusCharged); err != nil {
		return OrderResult{}, err
	}

	var tracking string
	if err := workflow.ExecuteActivity(ctx, act.ShipOrder, order).Get(ctx, &tracking); err != nil {
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		_ = workflow.ExecuteActivity(dctx, act.RefundPayment, payment).Get(dctx, nil)
		return compensate()
	}
	if err := setStatus(StatusShipped); err != nil {
		return OrderResult{}, err
	}
	return OrderResult{Status: StatusShipped, Total: order.Total(), TrackingNumber: tracking}, nil
}

// FanOutWorkflow starts n child workflows in parallel and sums their
// results.
func FanOutWorkflow(ctx workflow.Context, n int) (int, error) {
	futures := make([]workflow.ChildWorkflowFuture, 0, n)
	for i := 1; i <= n; i++ {
		cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: fmt.Sprintf("%s-child-%d", workflow.GetInfo(ctx).WorkflowExecution.ID, i),
		})
		futures = append(futures, workflow.ExecuteChildWorkflow(cctx, ProcessItemWorkflow, i))
	}
	sum := 0
	for _, f := range futures {
		var v int
		if err := f.Get(ctx, &v); err != nil {
			return 0, err
		}
		sum += v
	}
	return sum, nil
}

// ProcessItemWorkflow is the child of FanOutWorkflow.
func ProcessItemWorkflow(ctx workflow.Context, n int) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOptions())
	var v int
	err := workflow.ExecuteActivity(ctx, act.ProcessItem, n).Get(ctx, &v)
	return v, err
}

// CounterState is carried across continue-as-new runs.
type CounterState struct {
	Count  int
	Target int
	Batch  int
	Runs   int
}

// CounterWorkflow processes Batch items per run and continues as new
// until Count reaches Target; the final run returns the state.
func CounterWorkflow(ctx workflow.Context, st CounterState) (CounterState, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOptions())
	st.Runs++
	for i := 0; i < st.Batch && st.Count < st.Target; i++ {
		if err := workflow.ExecuteActivity(ctx, act.ProcessItem, st.Count).Get(ctx, nil); err != nil {
			return st, err
		}
		st.Count++
	}
	if st.Count < st.Target {
		return st, workflow.NewContinueAsNewError(ctx, CounterWorkflow, st)
	}
	return st, nil
}

// BigHistoryWorkflow runs n sequential activities so its history spans
// several pages.
func BigHistoryWorkflow(ctx workflow.Context, n int) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOptions())
	done := 0
	for i := 0; i < n; i++ {
		if err := workflow.ExecuteActivity(ctx, act.ProcessItem, i).Get(ctx, nil); err != nil {
			return done, err
		}
		done++
	}
	return done, nil
}

// LongTimerWorkflow sleeps for d; when cancelled it runs a cleanup
// activity on a disconnected context and reports the cancellation.
func LongTimerWorkflow(ctx workflow.Context, d time.Duration) (string, error) {
	err := workflow.Sleep(ctx, d)
	if temporal.IsCanceledError(err) {
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		dctx = workflow.WithActivityOptions(dctx, activityOptions())
		var out string
		if cerr := workflow.ExecuteActivity(dctx, act.Cleanup, "cancelled").Get(dctx, &out); cerr != nil {
			return "", cerr
		}
		return out, err
	}
	return "slept", err
}

// FailingWorkflow fails through a non-retryable activity error.
func FailingWorkflow(ctx workflow.Context, msg string) error {
	ctx = workflow.WithActivityOptions(ctx, activityOptions())
	return workflow.ExecuteActivity(ctx, act.AlwaysFails, msg).Get(ctx, nil)
}

// ScheduledWorkflow is started by a Temporal schedule.
func ScheduledWorkflow(ctx workflow.Context) (string, error) {
	return "tick at " + workflow.Now(ctx).UTC().Format(time.RFC3339), nil
}
