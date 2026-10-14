// Command e2e drives the example workflows against a live Temporal
// server backed by the MongoDB plugin and reports PASS/FAIL per
// scenario. It registers the namespace and custom search attributes it
// needs, runs an in-process worker, and exits non-zero on any failure.
//
//	go run ./cmd/e2e -address localhost:7233 -namespace mongodb-e2e
//	go run ./cmd/e2e -scenarios order_saga,visibility
//	go run ./cmd/e2e -load 500 -concurrency 50
//	go run ./cmd/e2e -scenarios none -concurrency 100 -duration 2h -json soak.json
//	go run ./cmd/e2e -scenarios none -ladder 50,100,200 -duration 3m -max-p99 10s
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/e2e/app"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/e2e/report"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/e2e/scenarios"
)

func main() {
	address := flag.String("address", "localhost:7233", "Temporal frontend address")
	namespace := flag.String("namespace", "mongodb-e2e", "namespace to use (registered if missing)")
	only := flag.String("scenarios", "all", "comma-separated scenario names, or all")
	load := flag.Int("load", 0, "if > 0, also run a load test with this many order workflows")
	concurrency := flag.Int("concurrency", 50, "load test: workflows in flight")
	duration := flag.Duration("duration", 0, "load test: keep -concurrency workflows in flight for this long instead of a fixed count")
	ladder := flag.String("ladder", "", "comma-separated concurrencies to step through; each step runs -load workflows or for -duration")
	maxP99 := flag.Duration("max-p99", 10*time.Second, "ladder: a step whose p99 latency exceeds this is not sustainable")
	maxErrRate := flag.Float64("max-error-rate", 0.01, "ladder: a step whose error rate exceeds this is not sustainable")
	jsonOut := flag.String("json", "", "also write machine-readable results to this file")
	phase := flag.String("phase", "", "phase name recorded in the -json file")
	timeout := flag.Duration("timeout", 15*time.Minute, "overall deadline")
	flag.Parse()

	results := report.Results{Phase: *phase, StartedAt: time.Now().Unix()}
	writeJSON := func() {
		if *jsonOut == "" {
			return
		}
		results.FinishedAt = time.Now().Unix()
		if err := report.Write(*jsonOut, results); err != nil {
			fmt.Fprintf(os.Stderr, "e2e: %v\n", err)
		}
	}
	fail := func(format string, args ...any) {
		results.SetupError = fmt.Sprintf(format, args...)
		writeJSON()
		fmt.Fprintf(os.Stderr, "e2e: %s\n", results.SetupError)
		os.Exit(2)
	}
	steps, err := parseLadder(*ladder)
	if err != nil {
		fail("%v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	logger := sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))

	if err := ensureNamespace(ctx, *address, *namespace, logger); err != nil {
		fail("namespace: %v", err)
	}
	c, err := client.Dial(client.Options{HostPort: *address, Namespace: *namespace, Logger: logger})
	if err != nil {
		fail("dial: %v", err)
	}
	defer c.Close()
	if err := ensureSearchAttributes(ctx, c, *namespace); err != nil {
		fail("search attributes: %v", err)
	}

	w := worker.New(c, app.TaskQueue, worker.Options{})
	for _, wf := range app.Workflows() {
		w.RegisterWorkflow(wf)
	}
	w.RegisterActivity(&app.Activities{})
	if err := w.Start(); err != nil {
		fail("worker: %v", err)
	}
	defer w.Stop()

	env := &scenarios.Env{Client: c, Namespace: *namespace, Prefix: fmt.Sprintf("e2e-%d", time.Now().Unix())}
	selected := map[string]bool{}
	for _, s := range strings.Split(*only, ",") {
		selected[strings.TrimSpace(s)] = true
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "SCENARIO\tRESULT\tTIME\tCOVERS / ERROR")
	failed := 0
	for _, s := range scenarios.All() {
		if !selected["all"] && !selected[s.Name] {
			continue
		}
		sctx, scancel := context.WithTimeout(ctx, 5*time.Minute)
		t0 := time.Now()
		err := s.Run(sctx, env)
		scancel()
		took := time.Since(t0)
		result, detail := "PASS", s.Covers
		sr := report.Scenario{Name: s.Name, Pass: err == nil, DurationMS: took.Milliseconds()}
		if err != nil {
			result, detail = "FAIL", err.Error()
			sr.Error = detail
			failed++
		}
		results.Scenarios = append(results.Scenarios, sr)
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", s.Name, result, took.Round(time.Millisecond), detail)
		tw.Flush()
	}

	switch {
	case len(steps) > 0:
		lr := scenarios.Ladder(steps, scenarios.Thresholds{MaxP99: *maxP99, MaxErrorRate: *maxErrRate},
			func(c int) scenarios.LoadReport {
				r, _ := scenarios.Load(ctx, env, *load, c, *duration)
				printLoad("ladder step", nil, r)
				return r
			})
		results.Ladder = &lr
		result := fmt.Sprintf("max sustainable concurrency %d (p99 <= %s, error rate <= %.2g)", lr.MaxSustainable, *maxP99, *maxErrRate)
		if lr.MaxSustainable == 0 {
			result = "FAIL: no step met the thresholds"
			failed++
		}
		fmt.Printf("\nladder: %s\n", result)
	case *load > 0 || *duration > 0:
		r, err := scenarios.Load(ctx, env, *load, *concurrency, *duration)
		if err != nil {
			failed++
		}
		results.Load = &r
		printLoad("load", err, r)
	}
	writeJSON()
	if failed > 0 {
		fmt.Printf("\n%d check(s) failed\n", failed)
		os.Exit(1)
	}
	fmt.Println("\nall checks passed")
}

func printLoad(label string, err error, r scenarios.LoadReport) {
	result := "PASS"
	if err != nil {
		result = "FAIL: " + err.Error()
	}
	fmt.Printf("\n%s: %s — %d order workflows, %d in flight, %.1f workflows/s, latency p50 %s p95 %s p99 %s max %s\n",
		label, result, r.Workflows, r.Concurrency, r.PerSecond,
		r.P50.Round(time.Millisecond), r.P95.Round(time.Millisecond), r.P99.Round(time.Millisecond), r.Max.Round(time.Millisecond))
}

// parseLadder parses "50,100,200" into concurrencies.
func parseLadder(s string) ([]int, error) {
	if s == "" {
		return nil, nil
	}
	var out []int
	for _, f := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("-ladder: %q is not a positive integer", f)
		}
		out = append(out, n)
	}
	return out, nil
}

// ensureNamespace registers the namespace if needed and waits until the
// frontend's namespace cache serves it.
func ensureNamespace(ctx context.Context, address, ns string, logger sdklog.Logger) error {
	nc, err := client.NewNamespaceClient(client.Options{HostPort: address, Logger: logger})
	if err != nil {
		return err
	}
	defer nc.Close()
	if _, err := nc.Describe(ctx, ns); err == nil {
		return nil
	}
	err = nc.Register(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: ns, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	var exists *serviceerror.NamespaceAlreadyExists
	if err != nil && !errors.As(err, &exists) {
		return err
	}
	for {
		if _, err := nc.Describe(ctx, ns); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// ensureSearchAttributes registers the custom attributes the order
// workflow upserts and waits until they are visible.
func ensureSearchAttributes(ctx context.Context, c client.Client, ns string) error {
	want := map[string]enumspb.IndexedValueType{
		app.SACustomerID:  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		app.SAOrderTotal:  enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		app.SAPriority:    enumspb.INDEXED_VALUE_TYPE_INT,
		app.SAOrderStatus: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}
	ops := c.OperatorService()
	// A just-registered namespace reaches the frontend's namespace cache
	// a few seconds after DescribeNamespace sees it; retry NotFound.
	var have *operatorservice.ListSearchAttributesResponse
	for {
		var err error
		have, err = ops.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: ns})
		var nf *serviceerror.NamespaceNotFound
		if err == nil {
			break
		}
		if !errors.As(err, &nf) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	missing := map[string]enumspb.IndexedValueType{}
	for name, t := range want {
		if _, ok := have.GetCustomAttributes()[name]; !ok {
			missing[name] = t
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if _, err := ops.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: ns, SearchAttributes: missing,
	}); err != nil {
		var exists *serviceerror.AlreadyExists
		if !errors.As(err, &exists) {
			return err
		}
	}
	for {
		have, err := ops.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: ns})
		if err == nil {
			ok := true
			for name := range want {
				if _, found := have.GetCustomAttributes()[name]; !found {
					ok = false
				}
			}
			if ok {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
