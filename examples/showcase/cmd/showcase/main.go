// Command showcase is a support desk built on Temporal with the MongoDB
// persistence plugin. Tickets are durable workflows persisted in
// MongoDB; the same cluster stores the tickets and serves their hybrid
// full-text + vector search; operators find workflows through Temporal
// visibility, which the plugin answers with Atlas Search.
//
//	showcase setup                 namespace, search attributes, indexes, seed, schedule
//	showcase worker                run the worker
//	showcase tour                  narrated end-to-end demo of every feature
//	showcase open -customer acme -subject "..." -body "..."
//	showcase assign|resolve|comment <ticket-id> <text>
//	showcase show <ticket-id>      live state via the ticket query
//	showcase find "<visibility query>"
//	showcase search "<text>"       hybrid knowledge-base search
//	showcase report                run the SLA report now
//
// Environment: TEMPORAL_ADDRESS (localhost:7233), TEMPORAL_NAMESPACE
// (support), MONGODB_URI (mongodb://localhost:27017/?directConnection=true),
// SHOWCASE_DB (support), SHOWCASE_EMBEDDING (auto | voyage | none;
// default voyage when VOYAGE_API_KEY is set, else none), VOYAGE_API_KEY,
// VOYAGE_MODEL (voyage-4), VOYAGE_DIMS (1024), SLA_BASE (2m).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/app"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/data"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/kb"
)

type config struct {
	address, namespace, mongoURI, db, voyageKey, model string
	mode                                               kb.Mode
	dims                                               int
	slaBase                                            time.Duration
}

func env(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func loadConfig() (config, error) {
	c := config{
		address: env("TEMPORAL_ADDRESS", "localhost:7233"), namespace: env("TEMPORAL_NAMESPACE", "support"),
		mongoURI: env("MONGODB_URI", "mongodb://localhost:27017/?directConnection=true"), db: env("SHOWCASE_DB", "support"),
		voyageKey: os.Getenv("VOYAGE_API_KEY"), model: env("VOYAGE_MODEL", "voyage-4"),
	}
	def := "none"
	if c.voyageKey != "" {
		def = "voyage"
	}
	var err error
	if c.mode, err = kb.ParseMode(env("SHOWCASE_EMBEDDING", def)); err != nil {
		return c, err
	}
	if c.mode == kb.ModeVoyage && c.voyageKey == "" {
		return c, errors.New("SHOWCASE_EMBEDDING=voyage needs VOYAGE_API_KEY")
	}
	if c.dims, err = strconv.Atoi(env("VOYAGE_DIMS", "1024")); err != nil {
		return c, fmt.Errorf("VOYAGE_DIMS: %w", err)
	}
	if c.slaBase, err = time.ParseDuration(env("SLA_BASE", "2m")); err != nil {
		return c, fmt.Errorf("SLA_BASE: %w", err)
	}
	return c, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: showcase setup|worker|tour|open|assign|resolve|comment|show|find|search|report")
		os.Exit(2)
	}
	cfg, err := loadConfig()
	if err != nil {
		fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cmd, args := os.Args[1], os.Args[2:]
	if err := run(ctx, cfg, cmd, args); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "showcase:", err)
	os.Exit(1)
}

func logger() sdklog.Logger {
	return sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
}

func dial(cfg config) (client.Client, error) {
	return client.Dial(client.Options{HostPort: cfg.address, Namespace: cfg.namespace, Logger: logger()})
}

func openStore(ctx context.Context, cfg config) (*kb.Store, error) {
	var emb kb.Embedder
	if cfg.mode == kb.ModeVoyage {
		emb = &kb.Voyage{Key: cfg.voyageKey, Model: cfg.model}
	}
	return kb.Open(ctx, cfg.mongoURI, cfg.db, cfg.mode, cfg.model, emb, cfg.dims)
}

func run(ctx context.Context, cfg config, cmd string, args []string) error {
	switch cmd {
	case "setup":
		return setup(ctx, cfg)
	case "worker":
		return runWorker(ctx, cfg)
	case "tour":
		return tour(ctx, cfg)
	case "open":
		fs := flag.NewFlagSet("open", flag.ExitOnError)
		customer := fs.String("customer", "acme", "customer id")
		subject := fs.String("subject", "", "ticket subject")
		body := fs.String("body", "", "ticket body")
		_ = fs.Parse(args)
		c, err := dial(cfg)
		if err != nil {
			return err
		}
		defer c.Close()
		id, err := openTicket(ctx, c, cfg, *customer, *subject, *body)
		if err != nil {
			return err
		}
		fmt.Println(id)
		return nil
	case "assign", "resolve":
		if len(args) < 2 {
			return fmt.Errorf("usage: showcase %s <ticket-id> <text>", cmd)
		}
		c, err := dial(cfg)
		if err != nil {
			return err
		}
		defer c.Close()
		return c.SignalWorkflow(ctx, args[0], "", cmd, strings.Join(args[1:], " "))
	case "comment":
		if len(args) < 2 {
			return errors.New("usage: showcase comment <ticket-id> <text>")
		}
		c, err := dial(cfg)
		if err != nil {
			return err
		}
		defer c.Close()
		n, err := comment(ctx, c, args[0], strings.Join(args[1:], " "))
		if err != nil {
			return err
		}
		fmt.Printf("%d comment(s)\n", n)
		return nil
	case "show":
		if len(args) < 1 {
			return errors.New("usage: showcase show <ticket-id>")
		}
		c, err := dial(cfg)
		if err != nil {
			return err
		}
		defer c.Close()
		st, err := ticketState(ctx, c, args[0])
		if err != nil {
			return err
		}
		printState(st)
		return nil
	case "find":
		if len(args) < 1 {
			return errors.New(`usage: showcase find "<visibility query>"`)
		}
		c, err := dial(cfg)
		if err != nil {
			return err
		}
		defer c.Close()
		ids, err := find(ctx, c, cfg, strings.Join(args, " "))
		for _, id := range ids {
			fmt.Println(id)
		}
		return err
	case "search":
		if len(args) < 1 {
			return errors.New(`usage: showcase search "<text>"`)
		}
		s, err := openStore(ctx, cfg)
		if err != nil {
			return err
		}
		defer s.Close(context.Background())
		hits, err := s.Similar(ctx, strings.Join(args, " "), 5)
		printHits(hits)
		return err
	case "report":
		c, err := dial(cfg)
		if err != nil {
			return err
		}
		defer c.Close()
		r, err := report(ctx, c)
		fmt.Printf("by status: %v\nescalated: %v\n", r.ByStatus, r.Escalated)
		return err
	}
	return fmt.Errorf("unknown command %q", cmd)
}

// ---------- setup and worker ----------

func setup(ctx context.Context, cfg config) error {
	if err := ensureNamespace(ctx, cfg); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}
	c, err := dial(cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := ensureSearchAttributes(ctx, c, cfg.namespace); err != nil {
		return fmt.Errorf("search attributes: %w", err)
	}
	fmt.Printf("✓ namespace %q with search attributes %s, %s, %s, %s, %s (Text)\n", cfg.namespace,
		app.SACustomerID, app.SACategory, app.SAPriority, app.SAStatus, app.SASummary)
	s, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer s.Close(context.Background())
	ictx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := s.EnsureIndexes(ictx); err != nil {
		return err
	}
	if cfg.mode == kb.ModeNone {
		fmt.Printf("✓ knowledge-base index ready: %s (full-text; set VOYAGE_API_KEY or SHOWCASE_EMBEDDING=auto for vector search)\n", kb.TextIndexName)
	} else {
		fmt.Printf("✓ knowledge-base indexes ready: %s (full-text) and %s (vector, %s embeddings)\n", kb.TextIndexName, kb.VectorIndexName, cfg.mode)
		fmt.Println("! vector search sends ticket text to Voyage AI (demo data only; SHOWCASE_EMBEDDING=none sends nothing)")
	}
	docs, err := data.Seed()
	if err != nil {
		return err
	}
	if err := s.Seed(ctx, docs); err != nil {
		return err
	}
	fmt.Printf("✓ seeded %d resolved tickets\n", len(docs))
	if err := ensureSchedule(ctx, c); err != nil {
		return fmt.Errorf("schedule: %w", err)
	}
	fmt.Println("✓ schedule sla-report runs SLAReportWorkflow every 5 minutes")
	return nil
}

func runWorker(ctx context.Context, cfg config) error {
	c, err := dial(cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	s, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer s.Close(context.Background())
	mc, err := mongo.Connect(options.Client().ApplyURI(cfg.mongoURI))
	if err != nil {
		return err
	}
	defer mc.Disconnect(context.Background())
	w := worker.New(c, app.TaskQueue, worker.Options{})
	for _, wf := range app.Workflows() {
		w.RegisterWorkflow(wf)
	}
	w.RegisterActivity(&app.Activities{Store: s, Temporal: c, Namespace: cfg.namespace, Events: events{mc.Database(cfg.db).Collection("events")}})
	fmt.Printf("worker polling %q in namespace %q (embedding mode: %s)\n", app.TaskQueue, cfg.namespace, cfg.mode)
	return w.Run(stopCh(ctx))
}

type events struct{ c *mongo.Collection }

func (e events) InsertOne(ctx context.Context, doc any) error {
	_, err := e.c.InsertOne(ctx, doc)
	return err
}

func stopCh(ctx context.Context) <-chan any {
	ch := make(chan any)
	go func() { <-ctx.Done(); close(ch) }()
	return ch
}

func ensureNamespace(ctx context.Context, cfg config) error {
	nc, err := client.NewNamespaceClient(client.Options{HostPort: cfg.address, Logger: logger()})
	if err != nil {
		return err
	}
	defer nc.Close()
	if _, err := nc.Describe(ctx, cfg.namespace); err == nil {
		return nil
	}
	err = nc.Register(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: cfg.namespace, WorkflowExecutionRetentionPeriod: durationpb.New(72 * time.Hour),
		Description: "MongoDB Temporal plugin showcase: support desk",
	})
	var exists *serviceerror.NamespaceAlreadyExists
	if err != nil && !errors.As(err, &exists) {
		return err
	}
	return poll(ctx, time.Minute, func() error { _, err := nc.Describe(ctx, cfg.namespace); return err })
}

func ensureSearchAttributes(ctx context.Context, c client.Client, ns string) error {
	want := map[string]enumspb.IndexedValueType{
		app.SACustomerID: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		app.SACategory:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		app.SAStatus:     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		app.SAPriority:   enumspb.INDEXED_VALUE_TYPE_INT,
		app.SASummary:    enumspb.INDEXED_VALUE_TYPE_TEXT,
	}
	ops := c.OperatorService()
	var have *operatorservice.ListSearchAttributesResponse
	if err := poll(ctx, time.Minute, func() (err error) {
		have, err = ops.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: ns})
		return err
	}); err != nil {
		return err
	}
	missing := map[string]enumspb.IndexedValueType{}
	for n, t := range want {
		if _, ok := have.GetCustomAttributes()[n]; !ok {
			missing[n] = t
		}
	}
	if len(missing) > 0 {
		if _, err := ops.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{Namespace: ns, SearchAttributes: missing}); err != nil {
			var exists *serviceerror.AlreadyExists
			if !errors.As(err, &exists) {
				return err
			}
		}
	}
	return poll(ctx, time.Minute, func() error {
		have, err := ops.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: ns})
		if err != nil {
			return err
		}
		for n := range want {
			if _, ok := have.GetCustomAttributes()[n]; !ok {
				return fmt.Errorf("search attribute %s not visible yet", n)
			}
		}
		return nil
	})
}

func ensureSchedule(ctx context.Context, c client.Client) error {
	_, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:     "sla-report",
		Spec:   client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: 5 * time.Minute}}},
		Action: &client.ScheduleWorkflowAction{ID: "sla-report", Workflow: app.SLAReportWorkflow, TaskQueue: app.TaskQueue},
	})
	var exists *serviceerror.AlreadyExists
	if err != nil && !errors.As(err, &exists) && !strings.Contains(err.Error(), "already") {
		return err
	}
	return nil
}

// ---------- operations ----------

func openTicket(ctx context.Context, c client.Client, cfg config, customer, subject, body string) (string, error) {
	if subject == "" {
		return "", errors.New("-subject is required")
	}
	id := fmt.Sprintf("ticket-%s-%d", customer, time.Now().UnixNano()%1e9)
	_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: app.TaskQueue}, app.TicketWorkflow,
		app.TicketInput{Ticket: supportTicket(id, customer, subject, body), SLABase: cfg.slaBase})
	return id, err
}

func comment(ctx context.Context, c client.Client, id, text string) (int, error) {
	h, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: id, UpdateName: app.UpdateComment,
		Args: []any{text}, WaitForStage: client.WorkflowUpdateStageCompleted})
	if err != nil {
		return 0, err
	}
	var n int
	err = h.Get(ctx, &n)
	return n, err
}

func ticketState(ctx context.Context, c client.Client, id string) (app.TicketState, error) {
	var st app.TicketState
	v, err := c.QueryWorkflow(ctx, id, "", app.QueryTicket)
	if err != nil {
		return st, err
	}
	return st, v.Get(&st)
}

func find(ctx context.Context, c client.Client, cfg config, q string) ([]string, error) {
	resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Namespace: cfg.namespace, Query: q})
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range resp.GetExecutions() {
		ids = append(ids, e.GetExecution().GetWorkflowId())
	}
	return ids, nil
}

func report(ctx context.Context, c client.Client) (app.Report, error) {
	var r app.Report
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: fmt.Sprintf("sla-report-now-%d", time.Now().Unix()), TaskQueue: app.TaskQueue}, app.SLAReportWorkflow)
	if err != nil {
		return r, err
	}
	return r, run.Get(ctx, &r)
}

func poll(ctx context.Context, d time.Duration, f func() error) error {
	deadline := time.Now().Add(d)
	for {
		err := f()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func printState(st app.TicketState) {
	fmt.Printf("%s  [%s, P%d, %s]  %s\n", st.Ticket.ID, st.Category, st.Priority, st.Status, st.Ticket.Subject)
	if st.Agent != "" {
		fmt.Printf("  agent: %s\n", st.Agent)
	}
	for _, c := range st.Comments {
		fmt.Printf("  comment: %s\n", c)
	}
	if len(st.Suggestions) > 0 {
		fmt.Println("  suggested from past tickets:")
		printHits(st.Suggestions)
	}
}

func printHits(hits []kb.Hit) {
	for _, h := range hits {
		how := []string{}
		if h.TextRank > 0 {
			how = append(how, fmt.Sprintf("full-text #%d", h.TextRank))
		}
		if h.VectorRank > 0 {
			how = append(how, fmt.Sprintf("vector #%d", h.VectorRank))
		}
		fmt.Printf("    • %s — %s\n      fix: %s\n", h.Subject, strings.Join(how, ", "), h.Resolution)
	}
}
