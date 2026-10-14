//go:build integration

// Visibility query regression tests. Records
// are written with search attributes encoded by upstream's
// searchattribute.Encode (as the visibility manager does) and queried
// with real query strings through upstream's QueryConverter, using the
// SQL-style test provider (pre-allocated Keyword01.. fields) and the test
// mapper's per-namespace aliases ("AliasForKeyword01").
package visibility_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/proto"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

const testNS = "ns"

var base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func saStore(t *testing.T) *visibility.Store {
	t.Helper()
	return visibility.New(startDB(t), visibility.ModeAggregation, "",
		visibility.WithSearchAttributeProvider(searchattribute.NewTestProvider()),
		visibility.WithSearchAttributeMapperProvider(
			searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{Namespace: testNS})))
}

func encodeSA(t *testing.T, attrs map[string]any) *commonpb.SearchAttributes {
	t.Helper()
	if len(attrs) == 0 {
		return nil
	}
	typeMap := searchattribute.TestNameTypeMap()
	sa, err := searchattribute.Encode(attrs, &typeMap)
	if err != nil {
		t.Fatalf("encode search attributes: %v", err)
	}
	return sa
}

type rec struct {
	run    string
	wf     string
	start  time.Time
	status enumspb.WorkflowExecutionStatus
	attrs  map[string]any
}

func record(t *testing.T, s *visibility.Store, r rec) {
	t.Helper()
	if r.wf == "" {
		r.wf = "wf-" + r.run
	}
	if r.status == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
		r.status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}
	err := s.RecordWorkflowExecutionStarted(context.Background(), &store.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			NamespaceID: testNS, WorkflowID: r.wf, RunID: r.run, WorkflowTypeName: "type",
			StartTime: r.start, ExecutionTime: r.start, Status: r.status, TaskQueue: "tq",
			SearchAttributes: encodeSA(t, r.attrs),
		},
	})
	if err != nil {
		t.Fatalf("record %s: %v", r.run, err)
	}
}

func list(t *testing.T, s *visibility.Store, query string, pageSize int, token []byte) *store.InternalListExecutionsResponse {
	t.Helper()
	resp, err := s.ListWorkflowExecutions(context.Background(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID(testNS), Namespace: namespace.Name(testNS),
		Query: query, PageSize: pageSize, NextPageToken: token,
	})
	if err != nil {
		t.Fatalf("List(%q): %v", query, err)
	}
	return resp
}

func runIDs(resp *store.InternalListExecutionsResponse) []string {
	var out []string
	for _, e := range resp.Executions {
		out = append(out, e.RunID)
	}
	return out
}

func listAll(t *testing.T, s *visibility.Store, query string, pageSize int) []string {
	t.Helper()
	var out []string
	var token []byte
	for i := 0; i < 50; i++ {
		resp := list(t, s, query, pageSize, token)
		out = append(out, runIDs(resp)...)
		if len(resp.NextPageToken) == 0 {
			return out
		}
		token = resp.NextPageToken
	}
	t.Fatal("pagination did not terminate")
	return nil
}

func requireRuns(t *testing.T, query string, got []string, want ...string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%s: got %v; want %v", query, got, want)
	}
}

// TestVisibility_CustomKeywordEquality_Matches/071.
func TestVisibility_CustomKeywordEquality_Matches(t *testing.T) {
	s := saStore(t)
	record(t, s, rec{run: "gold", start: base, attrs: map[string]any{"Keyword01": "gold"}})
	record(t, s, rec{run: "silver", start: base.Add(time.Second), attrs: map[string]any{"Keyword01": "silver"}})
	q := "AliasForKeyword01 = 'gold'"
	requireRuns(t, q, runIDs(list(t, s, q, 10, nil)), "gold")
}

// TestVisibility_CustomTypedAttributes_Match/073/074.
func TestVisibility_CustomTypedAttributes_Match(t *testing.T) {
	s := saStore(t)
	record(t, s, rec{run: "a", start: base, attrs: map[string]any{
		"Int01": int64(10), "Double01": 2.5, "Bool01": true,
		"Datetime01": base.Add(time.Hour), "KeywordList01": []string{"red", "blue"},
		"Text01": "hello brave world", "Keyword02": "alpha-1",
	}})
	record(t, s, rec{run: "b", start: base.Add(time.Second), attrs: map[string]any{
		"Int01": int64(1), "Double01": 0.5, "Bool01": false,
		"Datetime01": base, "KeywordList01": []string{"green"},
		"Text01": "goodbye", "Keyword02": "beta-1",
	}})
	cases := map[string][]string{
		"AliasForInt01 > 5":       {"a"},
		"AliasForDouble01 >= 1.5": {"a"},
		"AliasForBool01 = true":   {"a"},
		"AliasForDatetime01 > '" + base.Add(time.Minute).Format(time.RFC3339) + "'":                                                                 {"a"},
		"AliasForDatetime01 BETWEEN '" + base.Add(-time.Minute).Format(time.RFC3339) + "' AND '" + base.Add(time.Minute).Format(time.RFC3339) + "'": {"b"},
		"AliasForKeywordList01 = 'blue'":                                                {"a"},
		"AliasForKeywordList01 IN ('green', 'yellow')":                                  {"b"},
		"AliasForText01 = 'brave'":                                                      {"a"},
		"AliasForText01 != 'brave'":                                                     {"b"},
		"AliasForKeyword02 STARTS_WITH 'alpha'":                                         {"a"},
		"AliasForKeyword02 NOT STARTS_WITH 'alpha'":                                     {"b"},
		"StartTime > '" + base.Add(500*time.Millisecond).Format(time.RFC3339Nano) + "'": {"b"},
		"ExecutionStatus IN ('Running', 'Completed')":                                   {"b", "a"},
	}
	for q, want := range cases {
		t.Run(q, func(t *testing.T) {
			requireRuns(t, q, runIDs(list(t, s, q, 10, nil)), want...)
		})
	}
}

func TestList_OrderByOnly_Succeeds(t *testing.T) {
	s := saStore(t)
	for i := 0; i < 3; i++ {
		record(t, s, rec{run: fmt.Sprintf("r%d", i), start: base.Add(time.Duration(i) * time.Second)})
	}
	q := "ORDER BY StartTime ASC"
	requireRuns(t, q, listAll(t, s, q, 2), "r0", "r1", "r2")
}

// TestList_OrderByStartTimeDesc_Ordered with keyset
// pagination across pages.
func TestList_OrderByStartTimeDesc_Ordered(t *testing.T) {
	s := saStore(t)
	// Run IDs deliberately sort opposite to start time.
	for i := 0; i < 5; i++ {
		record(t, s, rec{run: fmt.Sprintf("z%d", 4-i), start: base.Add(time.Duration(i) * time.Second)})
	}
	q := "ExecutionStatus = 'Running' ORDER BY StartTime DESC"
	requireRuns(t, q, listAll(t, s, q, 2), "z0", "z1", "z2", "z3", "z4")
}

// TestList_DefaultOrder_NewestFirst: without ORDER BY
// the newest executions come first, not _id order.
func TestList_DefaultOrder_NewestFirst(t *testing.T) {
	s := saStore(t)
	record(t, s, rec{run: "a-old", start: base})
	record(t, s, rec{run: "b-mid", start: base.Add(time.Second)})
	record(t, s, rec{run: "c-new", start: base.Add(2 * time.Second)})
	requireRuns(t, "", listAll(t, s, "", 2), "c-new", "b-mid", "a-old")
}

func TestCount_GroupByStatus_Groups(t *testing.T) {
	s := saStore(t)
	record(t, s, rec{run: "r1", start: base})
	record(t, s, rec{run: "r2", start: base})
	record(t, s, rec{run: "c1", start: base, status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED})
	resp, err := s.CountWorkflowExecutions(context.Background(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID(testNS), Namespace: namespace.Name(testNS),
		Query: "GROUP BY ExecutionStatus",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, g := range resp.Groups {
		var status string
		if err := payload.Decode(g.GroupValues[0], &status); err != nil {
			t.Fatal(err)
		}
		got[status] = g.Count
	}
	if resp.Count != 3 || got["Running"] != 2 || got["Completed"] != 1 {
		t.Fatalf("count=%d groups=%v; want 3 {Running:2 Completed:1}", resp.Count, got)
	}
}

func TestList_SchedulerWorkflows_HiddenByDefault(t *testing.T) {
	s := saStore(t)
	record(t, s, rec{run: "user", start: base})
	record(t, s, rec{run: "sched", start: base.Add(time.Second), attrs: map[string]any{"TemporalNamespaceDivision": "TemporalScheduler"}})
	requireRuns(t, "default", listAll(t, s, "", 10), "user")
	requireRuns(t, "ExecutionStatus = 'Running'", listAll(t, s, "ExecutionStatus = 'Running'", 10), "user")
	q := "TemporalNamespaceDivision = 'TemporalScheduler'"
	requireRuns(t, q, listAll(t, s, q, 10), "sched")
}

func TestVisibility_GetWorkflowExecution_ReturnsSearchAttributes(t *testing.T) {
	s := saStore(t)
	attrs := map[string]any{"Keyword01": "gold", "Int01": int64(7)}
	record(t, s, rec{run: "r", start: base, attrs: attrs})
	want := encodeSA(t, attrs)
	resp, err := s.GetWorkflowExecution(context.Background(), &manager.GetWorkflowExecutionRequest{
		NamespaceID: namespace.ID(testNS), Namespace: namespace.Name(testNS), RunID: "r",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(resp.Execution.SearchAttributes, want) {
		t.Fatalf("search attributes not read back: %v", resp.Execution.SearchAttributes)
	}
	listed := list(t, s, "", 10, nil)
	if len(listed.Executions) != 1 || !proto.Equal(listed.Executions[0].SearchAttributes, want) {
		t.Fatal("list does not return search attributes")
	}
}

// indexScopedProvider only knows search attributes for one index name,
// like the cluster-metadata provider the frontend writes to.
type indexScopedProvider struct {
	index string
}

func (p indexScopedProvider) GetSearchAttributes(index string, force bool) (searchattribute.NameTypeMap, error) {
	if index != p.index {
		return searchattribute.NameTypeMap{}, nil
	}
	return searchattribute.NewTestProvider().GetSearchAttributes(index, force)
}

func TestVisibility_IndexNameMatchesSALookup(t *testing.T) {
	db := startDB(t)
	probe := visibility.New(db, visibility.ModeAggregation, "")
	s := visibility.New(db, visibility.ModeAggregation, "",
		visibility.WithSearchAttributeProvider(indexScopedProvider{index: probe.GetIndexName()}),
		visibility.WithSearchAttributeMapperProvider(
			searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{Namespace: testNS})))
	record(t, s, rec{run: "gold", start: base, attrs: map[string]any{"Keyword01": "gold"}})
	_, err := s.ListWorkflowExecutions(context.Background(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID(testNS), Namespace: namespace.Name(testNS),
		Query: "AliasForKeyword01 = 'gold'", PageSize: 10,
	})
	var ia *serviceerror.InvalidArgument
	if errors.As(err, &ia) {
		t.Fatalf("search attributes looked up under the wrong index name: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
}

// TestVisibilityFactory_CustomAttributeQuery_NoPanic:
// the factory must hand the mapper provider to the store.
func TestVisibilityFactory_CustomAttributeQuery_NoPanic(t *testing.T) {
	_, uri := startDBWithURI(t)
	st := newFactoryStore(t, uri, "aggregation")
	defer st.Close()
	s := st.(*visibility.Store)
	record(t, s, rec{run: "gold", start: base, attrs: map[string]any{"Keyword01": "gold"}})
	q := "AliasForKeyword01 = 'gold'"
	requireRuns(t, q, runIDs(list(t, s, q, 10, nil)), "gold")
}

func newFactoryStore(t *testing.T, uri, mode string) store.VisibilityStore {
	t.Helper()
	st, err := newFactoryStoreErr(uri, mode)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// countConnections returns the server's current connection count.
func countConnections(t *testing.T, db *mongo.Database) int32 {
	t.Helper()
	var out struct {
		Connections struct {
			Current int32 `bson:"current"`
		} `bson:"connections"`
	}
	if err := db.RunCommand(context.Background(), bson.D{{Key: "serverStatus", Value: 1}}).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.Connections.Current
}

func TestVisibilityFactory_StartupFailure_ClosesClient(t *testing.T) {
	_, uri := startDBWithURI(t)
	probe := startDBClient(t, uri)
	before := countConnections(t, probe)
	for i := 0; i < 10; i++ {
		if _, err := newFactoryStoreErr(uri, "no-such-mode"); err == nil {
			t.Fatal("unknown mode accepted")
		}
	}
	waitForConnections(t, probe, before+5)
}

func TestVisibilityStore_Close_DisconnectsClient(t *testing.T) {
	_, uri := startDBWithURI(t)
	probe := startDBClient(t, uri)
	before := countConnections(t, probe)
	for i := 0; i < 10; i++ {
		newFactoryStore(t, uri, "aggregation").Close()
	}
	waitForConnections(t, probe, before+5)
}

func waitForConnections(t *testing.T, db *mongo.Database, max int32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		n := countConnections(t, db)
		if n <= max {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("connections leaked: %d open, want <= %d", n, max)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
