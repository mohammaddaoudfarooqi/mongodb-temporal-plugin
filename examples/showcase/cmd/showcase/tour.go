package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/api/workflowservice/v1"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/app"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/kb"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/support"
)

// tour walks through every feature the showcase demonstrates and checks
// each outcome; it exits non-zero if any check fails, so it doubles as
// the showcase's end-to-end test. A worker must be running.
func tour(ctx context.Context, cfg config) error {
	cfg.slaBase = 15 * time.Second // short SLAs so the escalation happens during the tour
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
	customer := fmt.Sprintf("tour%d", time.Now().Unix()%1e6)
	t := &tally{}

	step("1. Hybrid search on the same MongoDB cluster that runs Temporal",
		"Full-text ($search) and semantic ($vectorSearch, mode "+cfg.mode.String()+") search over past tickets, fused with Reciprocal Rank Fusion.")
	hits, err := s.Similar(ctx, "I was charged twice this month and want my money back", 3)
	printHits(hits)
	t.check("hybrid search returns the double-charge fix", err == nil && len(hits) > 0 && strings.Contains(strings.ToLower(hits[0].Subject+hits[0].Resolution), "charge"), err)
	if cfg.mode != kb.ModeNone {
		both := len(hits) > 0 && hits[0].TextRank > 0 && hits[0].VectorRank > 0
		t.check("top hit found by both full-text and vector search", both, nil)
	}

	step("2. Tickets are durable workflows persisted in MongoDB",
		"Each ticket runs TicketWorkflow: triage, store, suggest past fixes, wait for a person, escalate on SLA breach.")
	billing, err1 := openTicket(ctx, c, cfg, customer, "Refund for duplicate charge", "My card was charged twice for the invoice, please refund")
	outage, err2 := openTicket(ctx, c, cfg, customer, "Production API outage", "All requests failing with 503, service unavailable")
	general, err3 := openTicket(ctx, c, cfg, customer, "How do I paginate list results?", "The list call only returns the first page")
	t.check("three tickets started", err1 == nil && err2 == nil && err3 == nil, firstErr(err1, err2, err3))
	var st app.TicketState
	err = poll(ctx, 30*time.Second, func() error {
		var e error
		if st, e = ticketState(ctx, c, billing); e != nil {
			return e
		}
		if len(st.Suggestions) == 0 {
			return fmt.Errorf("no suggestions yet")
		}
		return nil
	})
	printState(st)
	t.check("billing ticket triaged as billing/P2 with suggested fixes (query)", err == nil && st.Category == support.CategoryBilling && st.Priority == 2, err)

	step("3. Signals and validated updates",
		"assign is a signal; comment is an update whose validator rejects empty text before it reaches history.")
	err = c.SignalWorkflow(ctx, billing, "", app.SignalAssign, "ana")
	t.check("assign signal delivered", err == nil, err)
	n, err := comment(ctx, c, billing, "Looking into the duplicate charge now")
	t.check("comment update accepted and returned the comment count", err == nil && n == 1, err)
	_, err = comment(ctx, c, billing, "   ")
	t.check("empty comment rejected by the update validator", err != nil, nil)

	step("4. Durable timers: SLA escalation",
		"Nobody answers the outage ticket; its P1 SLA timer (15s in the tour) fires and the workflow escalates it.")
	err = poll(ctx, 90*time.Second, func() error {
		ids, e := find(ctx, c, cfg, fmt.Sprintf("%s = 'escalated' AND %s = '%s'", app.SAStatus, app.SACustomerID, customer))
		if e != nil {
			return e
		}
		if !contains(ids, outage) {
			return fmt.Errorf("not escalated yet")
		}
		return nil
	})
	t.check("outage ticket escalated by its SLA timer and found by TicketStatus = 'escalated'", err == nil, err)

	step("5. Operator search through Temporal visibility",
		"The plugin answers visibility queries from MongoDB (Atlas Search in atlas-search mode): keyword, integer range, full-text on a Text attribute, ordering, counts.")
	queries := []struct {
		label, q string
		want     []string
	}{
		{"keyword: Category = 'billing'", fmt.Sprintf("%s = 'billing' AND %s = '%s'", app.SACategory, app.SACustomerID, customer), []string{billing}},
		{"integer range: Priority >= 3", fmt.Sprintf("%s >= 3 AND %s = '%s'", app.SAPriority, app.SACustomerID, customer), []string{general}},
		{"full-text on Text attribute: Summary = 'unavailable'", fmt.Sprintf("%s = 'unavailable' AND %s = '%s'", app.SASummary, app.SACustomerID, customer), []string{outage}},
		{"ordering: ORDER BY StartTime DESC", fmt.Sprintf("%s = '%s' ORDER BY StartTime DESC", app.SACustomerID, customer), []string{general, outage, billing}},
	}
	for _, q := range queries {
		var ids []string
		err := poll(ctx, 30*time.Second, func() error {
			var e error
			if ids, e = find(ctx, c, cfg, q.q); e != nil {
				return e
			}
			if !sameSet(ids, q.want) {
				return fmt.Errorf("got %v", ids)
			}
			return nil
		})
		ok := err == nil && (!strings.Contains(q.q, "ORDER BY") || equal(ids, q.want))
		fmt.Printf("    %s → %v\n", q.label, ids)
		t.check(q.label, ok, err)
	}
	cnt, err := c.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: cfg.namespace,
		Query: fmt.Sprintf("%s = '%s' GROUP BY ExecutionStatus", app.SACustomerID, customer)})
	groups := map[string]int64{}
	for _, g := range cnt.GetGroups() {
		if vs := g.GetGroupValues(); len(vs) > 0 {
			groups[strings.Trim(string(vs[0].GetData()), `"`)] = g.GetCount()
		}
	}
	fmt.Printf("    COUNT … GROUP BY ExecutionStatus → %v\n", groups)
	t.check("count of the tour's tickets grouped by ExecutionStatus", err == nil && cnt.GetCount() == 3 && groups["Running"] == 3, err)

	step("6. Resolution feeds the knowledge base",
		"Resolving the outage writes the fix back; a new, similar ticket gets it as a suggestion.")
	fix := "Rolled back the faulty gateway release and drained the stuck connection pool"
	err = c.SignalWorkflow(ctx, outage, "", app.SignalResolve, fix)
	t.check("resolve signal delivered", err == nil, err)
	var out app.TicketState
	err = c.GetWorkflow(ctx, outage, "").Get(ctx, &out)
	t.check("outage workflow completed as resolved and escalated", err == nil && out.Status == support.StatusResolved && out.Escalated, err)
	err = poll(ctx, 90*time.Second, func() error {
		h, e := s.Similar(ctx, "gateway release rollback connection pool", 3)
		if e != nil {
			return e
		}
		for _, x := range h {
			if x.ID == outage {
				return nil
			}
		}
		return fmt.Errorf("not searchable yet")
	})
	t.check("the new resolution is found by hybrid search", err == nil, err)

	step("7. Schedules and visibility-driven reporting",
		"The sla-report schedule runs SLAReportWorkflow every 5 minutes; here it runs once now.")
	r, err := report(ctx, c)
	fmt.Printf("    by status: %v, escalated & running: %d\n", r.ByStatus, len(r.Escalated))
	t.check("SLA report built from visibility counts", err == nil && len(r.ByStatus) > 0, err)
	_, err = c.ScheduleClient().GetHandle(ctx, "sla-report").Describe(ctx)
	t.check("sla-report schedule exists", err == nil, err)

	// Leave nothing running. Resolutions feed the knowledge base, so they
	// must be real fixes, not placeholders.
	_ = c.SignalWorkflow(ctx, billing, "", app.SignalResolve, "Refunded the duplicate invoice charge and confirmed with the customer")
	_ = c.SignalWorkflow(ctx, general, "", app.SignalResolve, "Explained pagination: pass next_page_token from each response until it is empty")
	fmt.Printf("\n%d checks passed, %d failed. See the workflows in the Temporal UI (namespace %q, customer %s).\n", t.pass, t.fail, cfg.namespace, customer)
	if t.fail > 0 {
		return fmt.Errorf("%d tour check(s) failed", t.fail)
	}
	return nil
}

type tally struct{ pass, fail int }

func (t *tally) check(what string, ok bool, err error) {
	if ok {
		t.pass++
		fmt.Printf("  ✓ %s\n", what)
		return
	}
	t.fail++
	if err != nil {
		fmt.Printf("  ✗ %s: %v\n", what, err)
	} else {
		fmt.Printf("  ✗ %s\n", what)
	}
}

func step(title, what string) { fmt.Printf("\n%s\n  %s\n", title, what) }

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range b {
		if !contains(a, x) {
			return false
		}
	}
	return true
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
