package conformance

import (
	"context"
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/searchattribute"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

// runVisibilityAggregation exercises the visibility store on the default
// (aggregation) mode: record started/closed, list + count with empty
// query, pagination, add search attributes.
func runVisibilityAggregation(t *testing.T, f Fixture) {
	newStore := func(t *testing.T) (TestContext, *visibility.Store) {
		t.Helper()
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", visibility.EnsureIndexes(tc.Ctx, tc.DB))
		return tc, visibility.New(tc.DB, visibility.ModeAggregation, "")
	}

	record := func(t *testing.T, tc TestContext, s *visibility.Store, ns, wf, run string, status enumspb.WorkflowExecutionStatus) {
		t.Helper()
		req := &store.InternalRecordWorkflowExecutionStartedRequest{
			InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
				NamespaceID:      ns,
				WorkflowID:       wf,
				RunID:            run,
				WorkflowTypeName: "MyWF",
				StartTime:        time.Now().UTC(),
				ExecutionTime:    time.Now().UTC(),
				Status:           status,
				TaskQueue:        "tq",
				TaskID:           1,
			},
		}
		if err := s.RecordWorkflowExecutionStarted(tc.Ctx, req); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	t.Run("List_EmptyQuery_Paginates", func(t *testing.T) {
		tc, s := newStore(t)
		ns := uniqStr("ns")
		for i := 0; i < 5; i++ {
			record(t, tc, s, ns, "wf", fmt.Sprintf("run-%d", i), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
		}
		p1, err := s.ListWorkflowExecutions(tc.Ctx, &manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: namespace.ID(ns), PageSize: 3,
		})
		fatalOnErr(t, "page 1", err)
		if len(p1.Executions) != 3 {
			t.Errorf("page 1: got %d, want 3", len(p1.Executions))
		}
		if len(p1.NextPageToken) == 0 {
			t.Error("expected NextPageToken after page 1")
		}
		p2, err := s.ListWorkflowExecutions(tc.Ctx, &manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: namespace.ID(ns), PageSize: 3, NextPageToken: p1.NextPageToken,
		})
		fatalOnErr(t, "page 2", err)
		if len(p2.Executions) != 2 {
			t.Errorf("page 2: got %d, want 2", len(p2.Executions))
		}
	})

	t.Run("Count_EmptyQuery", func(t *testing.T) {
		tc, s := newStore(t)
		ns := uniqStr("ns")
		for i := 0; i < 4; i++ {
			record(t, tc, s, ns, "wf", fmt.Sprintf("run-%d", i), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
		}
		resp, err := s.CountWorkflowExecutions(tc.Ctx, &manager.CountWorkflowExecutionsRequest{
			NamespaceID: namespace.ID(ns),
		})
		fatalOnErr(t, "count", err)
		if resp.Count != 4 {
			t.Errorf("count got %d; want 4", resp.Count)
		}
	})

	t.Run("List_InvalidToken_Rejected", func(t *testing.T) {
		tc, s := newStore(t)
		_, err := s.ListWorkflowExecutions(tc.Ctx, &manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: namespace.ID(uniqStr("ns")), PageSize: 10,
			NextPageToken: []byte("not-base64!@#"),
		})
		if err == nil {
			t.Error("expected InvalidArgument on malformed token")
		}
	})

	// The visibility suite previously only exercised
	// started / list / count. The scenarios below fill in the
	// remaining public-surface methods.

	t.Run("GetWorkflowExecution_ReadsBack", func(t *testing.T) {
		tc, s := newStore(t)
		ns := uniqStr("ns")
		record(t, tc, s, ns, "wf-get", "run-get", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)

		resp, err := s.GetWorkflowExecution(tc.Ctx, &manager.GetWorkflowExecutionRequest{
			NamespaceID: namespace.ID(ns),
			WorkflowID:  "wf-get",
			RunID:       "run-get",
		})
		fatalOnErr(t, "GetWorkflowExecution", err)
		if resp.Execution == nil {
			t.Fatal("expected non-nil execution info")
		}
		if resp.Execution.WorkflowID != "wf-get" {
			t.Errorf("WorkflowID drift: %q", resp.Execution.WorkflowID)
		}
		if resp.Execution.RunID != "run-get" {
			t.Errorf("RunID drift: %q", resp.Execution.RunID)
		}
	})

	t.Run("RecordClosed_FlipsStatusAndAddsCloseTime", func(t *testing.T) {
		tc, s := newStore(t)
		ns := uniqStr("ns")
		record(t, tc, s, ns, "wf-close", "run-close", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)

		closed := &store.InternalRecordWorkflowExecutionClosedRequest{
			InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
				NamespaceID:      ns,
				WorkflowID:       "wf-close",
				RunID:            "run-close",
				WorkflowTypeName: "MyWF",
				StartTime:        time.Now().UTC().Add(-time.Minute),
				ExecutionTime:    time.Now().UTC().Add(-time.Minute),
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				TaskQueue:        "tq",
				TaskID:           2,
			},
			CloseTime:        time.Now().UTC(),
			HistoryLength:    42,
			HistorySizeBytes: 1024,
		}
		fatalOnErr(t, "RecordClosed", s.RecordWorkflowExecutionClosed(tc.Ctx, closed))

		resp, err := s.GetWorkflowExecution(tc.Ctx, &manager.GetWorkflowExecutionRequest{
			NamespaceID: namespace.ID(ns), WorkflowID: "wf-close", RunID: "run-close",
		})
		fatalOnErr(t, "Get after close", err)
		if resp.Execution.Status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			t.Errorf("status not flipped to COMPLETED: %v", resp.Execution.Status)
		}
		if resp.Execution.CloseTime.IsZero() {
			t.Errorf("close_time should be set after RecordClosed")
		}
		if resp.Execution.HistoryLength != 42 {
			t.Errorf("history_length drift: %d", resp.Execution.HistoryLength)
		}
	})

	t.Run("Upsert_IsIdempotent", func(t *testing.T) {
		tc, s := newStore(t)
		ns := uniqStr("ns")
		base := &store.InternalVisibilityRequestBase{
			NamespaceID:      ns,
			WorkflowID:       "wf-up",
			RunID:            "run-up",
			WorkflowTypeName: "MyWF",
			StartTime:        time.Now().UTC(),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			TaskQueue:        "tq",
		}
		req := &store.InternalUpsertWorkflowExecutionRequest{InternalVisibilityRequestBase: base}
		fatalOnErr(t, "first upsert", s.UpsertWorkflowExecution(tc.Ctx, req))
		// Second upsert with the same key replaces in place — must not error.
		fatalOnErr(t, "repeat upsert", s.UpsertWorkflowExecution(tc.Ctx, req))

		resp, err := s.CountWorkflowExecutions(tc.Ctx, &manager.CountWorkflowExecutionsRequest{
			NamespaceID: namespace.ID(ns),
		})
		fatalOnErr(t, "count", err)
		if resp.Count != 1 {
			t.Errorf("upsert should replace in place; got count=%d", resp.Count)
		}
	})

	t.Run("DeleteWorkflowExecution_RemovesRow", func(t *testing.T) {
		tc, s := newStore(t)
		ns := uniqStr("ns")
		record(t, tc, s, ns, "wf-del", "run-del", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)

		fatalOnErr(t, "delete", s.DeleteWorkflowExecution(tc.Ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{
			NamespaceID: namespace.ID(ns),
			WorkflowID:  "wf-del",
			RunID:       "run-del",
		}))

		// Idempotent: repeat delete is a no-op.
		fatalOnErr(t, "idempotent delete", s.DeleteWorkflowExecution(tc.Ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{
			NamespaceID: namespace.ID(ns),
			WorkflowID:  "wf-del",
			RunID:       "run-del",
		}))

		resp, err := s.CountWorkflowExecutions(tc.Ctx, &manager.CountWorkflowExecutionsRequest{
			NamespaceID: namespace.ID(ns),
		})
		fatalOnErr(t, "count after delete", err)
		if resp.Count != 0 {
			t.Errorf("delete should remove the row; got count=%d", resp.Count)
		}
	})

	t.Run("AddSearchAttributes_IsIdempotent", func(t *testing.T) {
		tc, s := newStore(t)
		attrs := map[string]enumspb.IndexedValueType{
			uniqStr("CustomKeyword"): enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			uniqStr("CustomInt"):     enumspb.INDEXED_VALUE_TYPE_INT,
		}
		req := &manager.AddSearchAttributesRequest{SearchAttributes: attrs}
		fatalOnErr(t, "first add", s.AddSearchAttributes(tc.Ctx, req))
		// Re-adding the same SA keys must not fail; duplicate-key is
		// treated as idempotent per the upstream contract.
		fatalOnErr(t, "repeat add", s.AddSearchAttributes(tc.Ctx, req))
	})
}

// runVisibilityAtlasSearch covers the atlas-search path. Driver is
// responsible for creating the search index and gating the tracker;
// this helper assumes the driver's AtlasSearch capability flag means
// both are in place.
func runVisibilityAtlasSearch(t *testing.T, f Fixture) {
	t.Run("AtlasSearch_TypedQueries", func(t *testing.T) {
		// Typed queries in Atlas Search mode: the
		// shipped mapping (token + keyword string + text multi via the
		// search_attributes typeSet) must serve every operator the
		// converter emits.
		runAtlasTypedQueries(t, f)
	})
	t.Run("AtlasSearch_List_ThenCount", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", visibility.EnsureIndexes(tc.Ctx, tc.DB))
		f.EnsureAtlasSearchIndex(t, tc.DB)

		tr := visibility.NewIndexTracker(tc.DB, f.AtlasSearchIndexName(), 5*time.Second)
		tr.Start(tc.Ctx)
		t.Cleanup(tr.Stop)
		// WaitFor replaces the prior fixed-sleep loop. The
		// tracker flips IsReady asynchronously when its poller observes
		// READY; we back off exponentially up to 5 s per probe.
		if err := WaitFor(tc.Ctx, WaitForOptions{Total: 2 * time.Minute}, func(context.Context) (bool, error) {
			return tr.IsReady(), nil
		}); err != nil {
			t.Fatalf("atlas search index never became READY; last status=%s: %v", tr.LastStatus(), err)
		}

		s := visibility.New(tc.DB, visibility.ModeAtlasSearch, f.AtlasSearchIndexName(),
			visibility.WithIndexTracker(tr))
		t.Cleanup(s.Close)

		ns := uniqStr("ns-atlas")
		for i := 0; i < 4; i++ {
			req := &store.InternalRecordWorkflowExecutionStartedRequest{
				InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
					NamespaceID: ns, WorkflowID: "wf",
					RunID: fmt.Sprintf("run-%d", i), StartTime: time.Now().UTC(),
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, TaskQueue: "tq",
				},
			}
			if err := s.RecordWorkflowExecutionStarted(tc.Ctx, req); err != nil {
				t.Fatalf("Record: %v", err)
			}
		}

		// Atlas indexes writes asynchronously. Instead of a
		// fixed sleep, poll until $searchMeta count matches our seed;
		// bounded at 90 s so a slow-indexing cluster surfaces as a
		// clear timeout rather than a subtle wrong-count assertion.
		fatalOnErr(t, "wait for atlas indexing", WaitFor(tc.Ctx,
			WaitForOptions{Total: 90 * time.Second},
			func(ctx context.Context) (bool, error) {
				resp, err := s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
					NamespaceID: namespace.ID(ns),
				})
				if err != nil {
					return false, err
				}
				return resp.Count >= 4, nil
			}))

		p1, err := s.ListWorkflowExecutions(tc.Ctx, &manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: namespace.ID(ns), PageSize: 3,
		})
		fatalOnErr(t, "list page 1", err)
		if len(p1.Executions) != 3 {
			t.Errorf("page 1: got %d want 3", len(p1.Executions))
		}
		if len(p1.NextPageToken) == 0 {
			t.Error("expected NextPageToken")
		}

		p2, err := s.ListWorkflowExecutions(tc.Ctx, &manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: namespace.ID(ns), PageSize: 3, NextPageToken: p1.NextPageToken,
		})
		fatalOnErr(t, "list page 2", err)
		if len(p2.Executions) != 1 {
			t.Errorf("page 2: got %d want 1", len(p2.Executions))
		}

		// Count via $searchMeta.
		countResp, err := s.CountWorkflowExecutions(tc.Ctx, &manager.CountWorkflowExecutionsRequest{
			NamespaceID: namespace.ID(ns),
		})
		fatalOnErr(t, "count", err)
		if countResp.Count != 4 {
			t.Errorf("count got %d want 4", countResp.Count)
		}
	})
}

func runAtlasTypedQueries(t *testing.T, f Fixture) {
	tc := newTestContext(t, f)
	fatalOnErr(t, "ensure indexes", visibility.EnsureIndexes(tc.Ctx, tc.DB))
	f.EnsureAtlasSearchIndex(t, tc.DB)
	tr := visibility.NewIndexTracker(tc.DB, f.AtlasSearchIndexName(), 5*time.Second)
	tr.Start(tc.Ctx)
	t.Cleanup(tr.Stop)
	fatalOnErr(t, "index ready", WaitFor(tc.Ctx, WaitForOptions{Total: 3 * time.Minute},
		func(context.Context) (bool, error) { return tr.IsReady(), nil }))

	ns := uniqStr("ns-typed")
	s := visibility.New(tc.DB, visibility.ModeAtlasSearch, f.AtlasSearchIndexName(),
		visibility.WithIndexTracker(tr),
		visibility.WithSearchAttributeProvider(searchattribute.NewTestProvider()),
		visibility.WithSearchAttributeMapperProvider(searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{Namespace: ns})))
	t.Cleanup(s.Close)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	typeMap := searchattribute.TestNameTypeMap()
	record := func(run string, start time.Time, status enumspb.WorkflowExecutionStatus, attrs map[string]any) {
		t.Helper()
		var sa *commonpb.SearchAttributes
		if len(attrs) > 0 {
			var err error
			sa, err = searchattribute.Encode(attrs, &typeMap)
			fatalOnErr(t, "encode", err)
		}
		fatalOnErr(t, "record "+run, s.RecordWorkflowExecutionStarted(tc.Ctx, &store.InternalRecordWorkflowExecutionStartedRequest{
			InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
				NamespaceID: ns, WorkflowID: "wf-" + run, RunID: run, WorkflowTypeName: "type",
				StartTime: start, ExecutionTime: start, Status: status, TaskQueue: "tq",
				SearchAttributes: sa,
			},
		}))
	}
	record("a", base, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, map[string]any{
		"Int01": int64(10), "Double01": 2.5, "Bool01": true, "Datetime01": base.Add(time.Hour),
		"KeywordList01": []string{"red", "blue"}, "Text01": "hello brave world", "Keyword01": "gold", "Keyword02": "alpha-1",
	})
	record("b", base.Add(time.Second), enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, map[string]any{
		"Int01": int64(1), "Double01": 0.5, "Bool01": false, "Datetime01": base,
		"KeywordList01": []string{"green"}, "Text01": "goodbye", "Keyword01": "silver", "Keyword02": "beta-1",
	})
	record("sched", base.Add(2*time.Second), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		map[string]any{"TemporalNamespaceDivision": "TemporalScheduler"})

	list := func(q string) ([]string, error) {
		resp, err := s.ListWorkflowExecutions(tc.Ctx, &manager.ListWorkflowExecutionsRequestV2{
			NamespaceID: namespace.ID(ns), Namespace: namespace.Name(ns), Query: q, PageSize: 10,
		})
		if err != nil {
			return nil, err
		}
		var runs []string
		for _, e := range resp.Executions {
			runs = append(runs, e.RunID)
		}
		return runs, nil
	}
	// Atlas indexes asynchronously: wait until the default list shows
	// both user executions.
	fatalOnErr(t, "wait for indexing", WaitFor(tc.Ctx, WaitForOptions{Total: 2 * time.Minute},
		func(context.Context) (bool, error) {
			runs, err := list("")
			return err == nil && len(runs) == 2, err
		}))

	rfc := func(d time.Duration) string { return base.Add(d).Format(time.RFC3339) }
	cases := []struct {
		q    string
		want []string
	}{
		{"", []string{"b", "a"}}, // newest first; scheduler hidden
		{"AliasForKeyword01 = 'gold'", []string{"a"}},
		{"AliasForKeyword01 IN ('silver', 'bronze')", []string{"b"}},
		{"AliasForKeyword02 STARTS_WITH 'alpha'", []string{"a"}},
		{"AliasForKeyword02 NOT STARTS_WITH 'alpha'", []string{"b"}},
		{"AliasForText01 = 'brave'", []string{"a"}},
		{"AliasForInt01 > 5", []string{"a"}},
		{"AliasForDouble01 >= 1.5", []string{"a"}},
		{"AliasForBool01 = true", []string{"a"}},
		{"AliasForDatetime01 > '" + rfc(time.Minute) + "'", []string{"a"}},
		{"AliasForDatetime01 BETWEEN '" + rfc(-time.Minute) + "' AND '" + rfc(time.Minute) + "'", []string{"b"}},
		{"AliasForKeywordList01 = 'blue'", []string{"a"}},
		{"ExecutionStatus IN ('Running', 'Failed')", []string{"a"}},
		{"WorkflowId = 'wf-b'", []string{"b"}},
		{"ORDER BY StartTime ASC", []string{"a", "b"}},
		{"TemporalNamespaceDivision = 'TemporalScheduler'", []string{"sched"}},
	}
	for _, c := range cases {
		got, err := list(c.q)
		if err != nil {
			t.Errorf("%q: %v", c.q, err)
			continue
		}
		if fmt.Sprint(got) != fmt.Sprint(c.want) {
			t.Errorf("%q: got %v want %v", c.q, got, c.want)
		}
	}

	resp, err := s.CountWorkflowExecutions(tc.Ctx, &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID(ns), Namespace: namespace.Name(ns), Query: "GROUP BY ExecutionStatus",
	})
	fatalOnErr(t, "count group by", err)
	if resp.Count != 2 || len(resp.Groups) != 2 {
		t.Errorf("GROUP BY ExecutionStatus: count=%d groups=%d; want 2 and 2", resp.Count, len(resp.Groups))
	}
}
