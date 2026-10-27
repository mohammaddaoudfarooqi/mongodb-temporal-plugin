package web_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/app"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/kb"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/web"
)

type fakeSearch struct {
	got string
	err error
}

func (f *fakeSearch) Search(_ context.Context, q string, _ int) (kb.Results, error) {
	f.got = q
	return kb.Results{Text: []kb.Hit{{ID: "kb-1", Subject: "Duplicate charge"}}, Hybrid: []kb.Hit{{ID: "kb-1"}}}, f.err
}

func newServer(c *mocks.Client, s *fakeSearch, static fstest.MapFS) http.Handler {
	if static == nil {
		static = fstest.MapFS{}
	}
	return (&web.Server{Client: c, Search: s, Namespace: "support", SLABase: time.Minute,
		TemporalUI: "http://localhost:8080", Embedding: "none", Static: static,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler()
}

func do(t *testing.T, h http.Handler, method, path, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Host = "localhost:8090"
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec, out
}

func TestConfig(t *testing.T) {
	rec, out := do(t, newServer(&mocks.Client{}, &fakeSearch{}, nil), "GET", "/api/config", "")
	if rec.Code != 200 || out["namespace"] != "support" || out["embedding"] != "none" || out["temporal_ui"] != "http://localhost:8080" {
		t.Fatalf("%d %v", rec.Code, out)
	}
}

func TestOpenTicket_Created(t *testing.T) {
	c := &mocks.Client{}
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mocks.WorkflowRun{}, nil)
	rec, out := do(t, newServer(c, &fakeSearch{}, nil), "POST", "/api/tickets", `{"customer":"acme","subject":"Charged twice","body":"refund"}`)
	if rec.Code != http.StatusCreated || !strings.HasPrefix(out["id"].(string), "ticket-acme-") {
		t.Fatalf("%d %v", rec.Code, out)
	}
}

func TestOpenTicket_InvalidInput_400WithMessage(t *testing.T) {
	rec, out := do(t, newServer(&mocks.Client{}, &fakeSearch{}, nil), "POST", "/api/tickets", `{"customer":"acme","subject":""}`)
	if rec.Code != 400 || !strings.Contains(out["error"].(string), "subject") {
		t.Fatalf("%d %v", rec.Code, out)
	}
	rec, _ = do(t, newServer(&mocks.Client{}, &fakeSearch{}, nil), "POST", "/api/tickets", `{"customer":"acme","subject":"x","extra":1}`)
	if rec.Code != 400 {
		t.Errorf("unknown field accepted: %d", rec.Code)
	}
}

func TestTicketState(t *testing.T) {
	c := &mocks.Client{}
	v := &mocks.Value{}
	v.On("Get", mock.Anything).Run(func(a mock.Arguments) {
		*(a.Get(0).(*app.TicketState)) = app.TicketState{Category: "billing", Priority: 2, Status: "triaged"}
	}).Return(nil)
	c.On("QueryWorkflow", mock.Anything, "ticket-acme-1", "", app.QueryTicket).Return(v, nil)
	rec, out := do(t, newServer(c, &fakeSearch{}, nil), "GET", "/api/tickets/ticket-acme-1", "")
	if rec.Code != 200 || out["category"] != "billing" || out["status"] != "triaged" {
		t.Fatalf("%d %v", rec.Code, out)
	}
	rec, _ = do(t, newServer(c, &fakeSearch{}, nil), "GET", "/api/tickets/bad%20id", "")
	if rec.Code != 400 {
		t.Errorf("invalid id: %d", rec.Code)
	}
}

func TestActions_SignalAndUpdate(t *testing.T) {
	c := &mocks.Client{}
	c.On("SignalWorkflow", mock.Anything, "t-1", "", app.SignalAssign, "dana").Return(nil)
	c.On("SignalWorkflow", mock.Anything, "t-1", "", app.SignalResolve, "refunded").Return(nil)
	h := &mocks.WorkflowUpdateHandle{}
	h.On("Get", mock.Anything, mock.Anything).Run(func(a mock.Arguments) { *(a.Get(1).(*int)) = 2 }).Return(nil)
	c.On("UpdateWorkflow", mock.Anything, mock.MatchedBy(func(o client.UpdateWorkflowOptions) bool { return o.WorkflowID == "t-1" })).Return(h, nil)
	s := newServer(c, &fakeSearch{}, nil)
	if rec, _ := do(t, s, "POST", "/api/tickets/t-1/assign", `{"agent":"dana"}`); rec.Code != 204 {
		t.Errorf("assign: %d", rec.Code)
	}
	if rec, _ := do(t, s, "POST", "/api/tickets/t-1/resolve", `{"resolution":"refunded"}`); rec.Code != 204 {
		t.Errorf("resolve: %d", rec.Code)
	}
	if rec, out := do(t, s, "POST", "/api/tickets/t-1/comment", `{"text":"looking"}`); rec.Code != 200 || out["comments"] != float64(2) {
		t.Errorf("comment: %d %v", rec.Code, out)
	}
}

func TestComment_RejectedByWorkflowValidator_400WithItsMessage(t *testing.T) {
	c := &mocks.Client{}
	h := &mocks.WorkflowUpdateHandle{}
	h.On("Get", mock.Anything, mock.Anything).Return(temporal.NewApplicationError("comment must not be empty", app.ErrCommentRejected))
	c.On("UpdateWorkflow", mock.Anything, mock.Anything).Return(h, nil)
	rec, out := do(t, newServer(c, &fakeSearch{}, nil), "POST", "/api/tickets/t-1/comment", `{"text":"  "}`)
	if rec.Code != 400 || out["error"] != "comment must not be empty" {
		t.Fatalf("%d %v", rec.Code, out)
	}
}

func TestBackendError_502WithoutDetails(t *testing.T) {
	c := &mocks.Client{}
	c.On("ListWorkflow", mock.Anything, mock.Anything).Return(nil, errors.New("dial tcp 10.0.0.5:7233: secret detail"))
	rec, out := do(t, newServer(c, &fakeSearch{}, nil), "GET", "/api/tickets", "")
	if rec.Code != http.StatusBadGateway || strings.Contains(rec.Body.String(), "secret") || out["error"] == nil {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

func TestTickets_ListsTicketWorkflowsNewestFirst(t *testing.T) {
	c := &mocks.Client{}
	c.On("ListWorkflow", mock.Anything, mock.MatchedBy(func(r *workflowservice.ListWorkflowExecutionsRequest) bool {
		return r.Namespace == "support" && r.Query == "WorkflowType = 'TicketWorkflow' ORDER BY StartTime DESC"
	})).Return(&workflowservice.ListWorkflowExecutionsResponse{}, nil)
	if rec, _ := do(t, newServer(c, &fakeSearch{}, nil), "GET", "/api/tickets", ""); rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

func TestWorkflows_RunsTheOperatorsQuery(t *testing.T) {
	c := &mocks.Client{}
	c.On("ListWorkflow", mock.Anything, mock.MatchedBy(func(r *workflowservice.ListWorkflowExecutionsRequest) bool {
		return r.Query == "Summary = 'refund'"
	})).Return(&workflowservice.ListWorkflowExecutionsResponse{}, nil)
	s := newServer(c, &fakeSearch{}, nil)
	if rec, _ := do(t, s, "GET", "/api/workflows?query=Summary+%3D+%27refund%27", ""); rec.Code != 200 {
		t.Fatalf("%d", rec.Code)
	}
	if rec, _ := do(t, s, "GET", "/api/workflows?query="+strings.Repeat("x", 1001), ""); rec.Code != 400 {
		t.Errorf("an overlong query was accepted: %d", rec.Code)
	}
}

func TestSearch_ReturnsAllThreeLists(t *testing.T) {
	f := &fakeSearch{}
	rec, out := do(t, newServer(&mocks.Client{}, f, nil), "GET", "/api/search?q=duplicate+charge", "")
	if rec.Code != 200 || f.got != "duplicate charge" || out["mode"] != "none" || out["text"] == nil || out["hybrid"] == nil {
		t.Fatalf("%d %v", rec.Code, out)
	}
	if rec, _ := do(t, newServer(&mocks.Client{}, f, nil), "GET", "/api/search?q=", ""); rec.Code != 400 {
		t.Errorf("empty query: %d", rec.Code)
	}
}

func TestStatic_ServesTheAppOrAPlaceholder(t *testing.T) {
	rec, _ := do(t, newServer(&mocks.Client{}, &fakeSearch{}, nil), "GET", "/", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "npm run build") {
		t.Errorf("placeholder: %d %s", rec.Code, rec.Body.String())
	}
	built := fstest.MapFS{"index.html": {Data: []byte("<div id=root>")}, "assets/app.js": {Data: []byte("x")}}
	s := newServer(&mocks.Client{}, &fakeSearch{}, built)
	if rec, _ := do(t, s, "GET", "/", ""); !strings.Contains(rec.Body.String(), "id=root") {
		t.Errorf("index: %s", rec.Body.String())
	}
	if rec, _ := do(t, s, "GET", "/assets/app.js", ""); rec.Code != 200 {
		t.Errorf("asset: %d", rec.Code)
	}
	if rec, _ := do(t, s, "GET", "/tickets/abc", ""); !strings.Contains(rec.Body.String(), "id=root") {
		t.Errorf("client-side route should serve the app: %s", rec.Body.String())
	}
}

func TestSecurityHeaders(t *testing.T) {
	rec, _ := do(t, newServer(&mocks.Client{}, &fakeSearch{}, nil), "GET", "/api/config", "")
	for k, want := range map[string]string{"X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY"} {
		if rec.Header().Get(k) != want {
			t.Errorf("%s = %q", k, rec.Header().Get(k))
		}
	}
	if !strings.Contains(rec.Header().Get("Content-Security-Policy"), "default-src 'self'") {
		t.Errorf("CSP = %q", rec.Header().Get("Content-Security-Policy"))
	}
}

func raw(t *testing.T, h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestPost_RequiresJSON: a cross-site form or text/plain POST, which a
// browser sends without asking first, is refused.
func TestPost_RequiresJSON(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/tickets/t-1/resolve", strings.NewReader(`{"resolution":"x"}`))
	req.Host = "localhost:8090"
	req.Header.Set("Content-Type", "text/plain")
	if rec := raw(t, newServer(&mocks.Client{}, &fakeSearch{}, nil), req); rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("text/plain POST: %d", rec.Code)
	}
}

func TestCrossSiteRequests_Forbidden(t *testing.T) {
	s := newServer(&mocks.Client{}, &fakeSearch{}, nil)
	for name, set := range map[string]func(*http.Request){
		"Sec-Fetch-Site": func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") },
		"Origin":         func(r *http.Request) { r.Header.Set("Origin", "http://evil.example") },
	} {
		req := httptest.NewRequest("POST", "/api/tickets/t-1/resolve", strings.NewReader(`{"resolution":"x"}`))
		req.Host = "localhost:8090"
		req.Header.Set("Content-Type", "application/json")
		set(req)
		if rec := raw(t, s, req); rec.Code != http.StatusForbidden {
			t.Errorf("%s: %d", name, rec.Code)
		}
	}
	req := httptest.NewRequest("GET", "/api/config", nil)
	req.Host = "localhost:8090"
	req.Header.Set("Origin", "http://localhost:8090")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	if rec := raw(t, s, req); rec.Code != 200 {
		t.Errorf("same-origin request refused: %d", rec.Code)
	}
}

// TestHost_NotLocal_Forbidden: DNS rebinding points another site's name
// at 127.0.0.1; a Host header that is not ours is refused.
func TestHost_NotLocal_Forbidden(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/tickets", nil)
	req.Host = "attacker.example:8090"
	if rec := raw(t, newServer(&mocks.Client{}, &fakeSearch{}, nil), req); rec.Code != http.StatusForbidden {
		t.Fatalf("foreign Host: %d", rec.Code)
	}
	for _, h := range []string{"127.0.0.1:8090", "[::1]:8090", "localhost"} {
		req := httptest.NewRequest("GET", "/api/config", nil)
		req.Host = h
		if rec := raw(t, newServer(&mocks.Client{}, &fakeSearch{}, nil), req); rec.Code != 200 {
			t.Errorf("%s refused: %d", h, rec.Code)
		}
	}
	allowed := (&web.Server{Client: &mocks.Client{}, Search: &fakeSearch{}, Static: fstest.MapFS{}, AllowedHosts: []string{"desk.internal"},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler()
	req = httptest.NewRequest("GET", "/api/config", nil)
	req.Host = "desk.internal:8090"
	if rec := raw(t, allowed, req); rec.Code != 200 {
		t.Errorf("an allowed host was refused: %d", rec.Code)
	}
}

// TestReport_ActivityFailure_502WithoutDetails: an activity's error
// reaches the client wrapped as an ApplicationError; it must not be
// shown.
func TestReport_ActivityFailure_502WithoutDetails(t *testing.T) {
	c := &mocks.Client{}
	run := &mocks.WorkflowRun{}
	run.On("Get", mock.Anything, mock.Anything).Return(fmt.Errorf("workflow failed: %w",
		temporal.NewApplicationError("mongo: connection to 10.1.2.3 secret refused", "")))
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(run, nil)
	rec, _ := do(t, newServer(c, &fakeSearch{}, nil), "POST", "/api/report", "")
	if rec.Code != http.StatusBadGateway || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

func TestTicket_NotFound_404(t *testing.T) {
	c := &mocks.Client{}
	c.On("QueryWorkflow", mock.Anything, "ticket-x", "", app.QueryTicket).Return(nil, serviceerror.NewNotFound("workflow not found"))
	if rec, _ := do(t, newServer(c, &fakeSearch{}, nil), "GET", "/api/tickets/ticket-x", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("%d", rec.Code)
	}
}

func TestBody_TrailingData_400(t *testing.T) {
	rec, _ := do(t, newServer(&mocks.Client{}, &fakeSearch{}, nil), "POST", "/api/tickets", `{"customer":"acme","subject":"x"} {"more":1}`)
	if rec.Code != 400 {
		t.Fatalf("%d", rec.Code)
	}
}
