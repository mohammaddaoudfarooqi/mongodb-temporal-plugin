// Package scenarios holds the end-to-end checks the runner executes
// against a live Temporal server backed by the MongoDB plugin. Each
// scenario asserts outcomes the plugin is responsible for (history,
// current-run pointers, visibility, timers, tasks), not just that a
// workflow finished.
package scenarios

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/e2e/app"
)

// Env is what every scenario gets.
type Env struct {
	Client    client.Client
	Namespace string
	// Prefix makes workflow IDs unique per run of the runner.
	Prefix string
}

// Scenario is one end-to-end check.
type Scenario struct {
	Name   string
	Covers string // plugin persistence paths exercised
	Run    func(ctx context.Context, e *Env) error
}

// All returns every scenario in execution order.
func All() []Scenario {
	return []Scenario{
		{"order_saga", "activities+retries, heartbeats, signal, query, update, timer, typed search attributes", orderSaga},
		{"order_compensation", "non-retryable activity failure, compensation activities", orderCompensation},
		{"signal_with_start", "signal-with-start, buffered signal before first task", signalWithStart},
		{"fan_out_children", "child workflow executions, parent close", fanOut},
		{"continue_as_new", "continue-as-new chain, current-run pointer", continueAsNew},
		{"cancellation", "request-cancel, timer cancellation, cleanup on disconnected context", cancellation},
		{"durable_timer", "timer tasks fire after a real delay", durableTimer},
		{"big_history", "multi-page history reads (300 activities), deterministic replay", bigHistory},
		{"failure", "workflow failure propagation", failure},
		{"reset", "ResetWorkflowExecution (conflict resolution), new run", reset},
		{"visibility", "custom search attribute queries, count, GROUP BY, ORDER BY", visibility},
		{"schedule", "schedules (scheduler namespace division), scheduled runs", schedule},
	}
}

func (e *Env) id(name string) string { return fmt.Sprintf("%s-%s", e.Prefix, name) }

func (e *Env) start(ctx context.Context, id string, wf any, args ...any) (client.WorkflowRun, error) {
	return e.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: id, TaskQueue: app.TaskQueue, WorkflowExecutionTimeout: 10 * time.Minute,
	}, wf, args...)
}

// eventually polls cond until it returns nil or the deadline passes.
// Visibility is eventually consistent (and Atlas Search indexes
// asynchronously), so read-after-write checks go through it.
func eventually(ctx context.Context, d time.Duration, cond func() error) error {
	deadline := time.Now().Add(d)
	for {
		err := cond()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not satisfied after %s: %w", d, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func expect(cond bool, format string, args ...any) error {
	if cond {
		return nil
	}
	return fmt.Errorf(format, args...)
}

var errNotYet = errors.New("not yet")

func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*q)]
}

func sortDurations(d []time.Duration) { sort.Slice(d, func(i, j int) bool { return d[i] < d[j] }) }
