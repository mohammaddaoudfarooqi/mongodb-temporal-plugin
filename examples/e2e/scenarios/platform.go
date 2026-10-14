package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/e2e/app"
)

func reset(ctx context.Context, e *Env) error {
	id := e.id("reset")
	run, err := e.start(ctx, id, app.BigHistoryWorkflow, 5)
	if err != nil {
		return err
	}
	if err := run.Get(ctx, nil); err != nil {
		return err
	}
	// Reset to the first completed workflow task: the server rebuilds
	// mutable state and starts a new run (conflict resolution path).
	var firstWFTCompleted int64
	iter := e.Client.GetWorkflowHistory(ctx, id, run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() && firstWFTCompleted == 0 {
		ev, err := iter.Next()
		if err != nil {
			return err
		}
		if ev.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			firstWFTCompleted = ev.GetEventId()
		}
	}
	resp, err := e.Client.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 e.Namespace,
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: id, RunId: run.GetRunID()},
		Reason:                    "e2e reset",
		WorkflowTaskFinishEventId: firstWFTCompleted,
		RequestId:                 fmt.Sprintf("%s-reset", id),
	})
	if err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	if resp.GetRunId() == run.GetRunID() {
		return errors.New("reset did not create a new run")
	}
	var done int
	if err := e.Client.GetWorkflow(ctx, id, resp.GetRunId()).Get(ctx, &done); err != nil {
		return fmt.Errorf("reset run: %w", err)
	}
	if err := expect(done == 5, "reset run returned %d, want 5", done); err != nil {
		return err
	}
	desc, err := e.Client.DescribeWorkflowExecution(ctx, id, "")
	if err != nil {
		return err
	}
	return expect(desc.WorkflowExecutionInfo.GetExecution().GetRunId() == resp.GetRunId(),
		"current run %s, want reset run %s", desc.WorkflowExecutionInfo.GetExecution().GetRunId(), resp.GetRunId())
}

func visibility(ctx context.Context, e *Env) error {
	customer := e.Prefix + "-vis"
	var runs []string
	for i := 0; i < 3; i++ {
		id := e.id(fmt.Sprintf("vis-%d", i))
		o := order(id, customer, i+1)
		o.Priority = i + 1
		run, err := e.start(ctx, id, app.OrderWorkflow, o)
		if err != nil {
			return err
		}
		if err := run.Get(ctx, nil); err != nil {
			return err
		}
		runs = append(runs, id)
		time.Sleep(20 * time.Millisecond) // distinct start times for ORDER BY
	}
	list := func(q string) ([]string, error) {
		resp, err := e.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Namespace: e.Namespace, Query: q})
		if err != nil {
			return nil, err
		}
		var ids []string
		for _, ex := range resp.GetExecutions() {
			ids = append(ids, ex.GetExecution().GetWorkflowId())
		}
		return ids, nil
	}
	q := fmt.Sprintf("CustomerId = '%s' AND OrderStatus = 'SHIPPED'", customer)
	if err := eventually(ctx, 60*time.Second, func() error {
		ids, err := list(q)
		if err != nil {
			return err
		}
		return expect(len(ids) == 3, "%q returned %d executions, want 3", q, len(ids))
	}); err != nil {
		return err
	}
	ordered, err := list(fmt.Sprintf("CustomerId = '%s' ORDER BY StartTime DESC", customer))
	if err != nil {
		return err
	}
	if fmt.Sprint(ordered) != fmt.Sprint([]string{runs[2], runs[1], runs[0]}) {
		return fmt.Errorf("ORDER BY StartTime DESC returned %v", ordered)
	}
	hi, err := list(fmt.Sprintf("CustomerId = '%s' AND Priority >= 2 AND OrderTotal > 15", customer))
	if err != nil {
		return err
	}
	if len(hi) != 2 {
		return fmt.Errorf("numeric search-attribute filter returned %v, want 2 executions", hi)
	}
	count, err := e.Client.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: e.Namespace, Query: fmt.Sprintf("CustomerId = '%s' GROUP BY ExecutionStatus", customer),
	})
	if err != nil {
		return fmt.Errorf("count group by: %w", err)
	}
	if count.GetCount() != 3 || len(count.GetGroups()) != 1 {
		return fmt.Errorf("GROUP BY ExecutionStatus: count %d, groups %d; want 3 in 1 group", count.GetCount(), len(count.GetGroups()))
	}
	return nil
}

func schedule(ctx context.Context, e *Env) error {
	schedID := e.id("schedule")
	h, err := e.Client.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:      schedID,
		Spec:    client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: 2 * time.Second}}},
		Action:  &client.ScheduleWorkflowAction{ID: schedID + "-wf", Workflow: app.ScheduledWorkflow, TaskQueue: app.TaskQueue},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	})
	if err != nil {
		return fmt.Errorf("create schedule: %w", err)
	}
	defer func() { _ = h.Delete(context.Background()) }()
	if err := eventually(ctx, 90*time.Second, func() error {
		d, err := h.Describe(ctx)
		if err != nil {
			return err
		}
		return expect(d.Info.NumActions >= 2, "schedule ran %d actions, want >= 2", d.Info.NumActions)
	}); err != nil {
		return err
	}
	// Scheduled runs are listed by the reserved scheduled-by attribute;
	// the scheduler's own execution stays out of the default list.
	return eventually(ctx, 60*time.Second, func() error {
		resp, err := e.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: e.Namespace, Query: fmt.Sprintf("TemporalScheduledById = '%s'", schedID),
		})
		if err != nil {
			return err
		}
		return expect(len(resp.GetExecutions()) >= 2, "found %d scheduled runs, want >= 2", len(resp.GetExecutions()))
	})
}

// Load starts n order workflows with at most concurrency in flight and
// measures start-to-completion latency end to end. With d > 0 it
// instead keeps concurrency workflows in flight until d elapses
// .
func Load(ctx context.Context, e *Env, n, concurrency int, d time.Duration) (LoadReport, error) {
	r := Drive(ctx, concurrency, n, d, func(ctx context.Context, i int) error {
		id := e.id(fmt.Sprintf("load-c%d-%d", concurrency, i))
		run, err := e.start(ctx, id, app.OrderWorkflow, order(id, e.Prefix+"-load", 2))
		if err != nil {
			return err
		}
		return run.Get(ctx, nil)
	})
	if r.Failed > 0 {
		return r, fmt.Errorf("%d of %d workflows failed %v", r.Failed, r.Workflows, r.ErrorsByClass)
	}
	return r, nil
}

var _ = errNotYet
