// Package web serves the support desk's browser UI: a JSON API over the
// desk and the knowledge base, and the built React app.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/app"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/desk"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/kb"
)

//go:embed all:dist
var dist embed.FS

// Dist is the built React app (ui/, `npm run build`). Without a build it
// holds only a placeholder, and the server says how to build it.
func Dist() fs.FS {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	return sub
}

// Searcher is the knowledge base's search; *kb.Store satisfies it.
type Searcher interface {
	Search(ctx context.Context, query string, limit int) (kb.Results, error)
}

// Server is the UI's HTTP server.
type Server struct {
	Client     desk.Client
	Search     Searcher
	Namespace  string
	SLABase    time.Duration
	TemporalUI string // base URL of the Temporal UI, for links to workflows
	Embedding  string // knowledge-base embedding mode: auto, voyage or none
	Static     fs.FS
	Log        *slog.Logger
	// AllowedHosts are host names, besides localhost and loopback
	// addresses, that the UI may be reached by (Host header). Anything
	// else is refused, which stops DNS rebinding.
	AllowedHosts []string
}

const (
	maxBody     = 64 << 10
	maxQuery    = 1000
	maxSearch   = 500
	listLimit   = 50
	searchLimit = 5
)

var workflowID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$`)

const ticketsQuery = "WorkflowType = 'TicketWorkflow' ORDER BY StartTime DESC"

// Handler returns the API and the app.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/config", s.config)
	mux.HandleFunc("GET /api/tickets", s.listTickets)
	mux.HandleFunc("POST /api/tickets", s.openTicket)
	mux.HandleFunc("GET /api/tickets/{id}", s.ticket)
	mux.HandleFunc("POST /api/tickets/{id}/assign", s.assign)
	mux.HandleFunc("POST /api/tickets/{id}/resolve", s.resolve)
	mux.HandleFunc("POST /api/tickets/{id}/comment", s.comment)
	mux.HandleFunc("GET /api/workflows", s.workflows)
	mux.HandleFunc("GET /api/search", s.search)
	mux.HandleFunc("POST /api/report", s.report)
	mux.Handle("/api/", http.NotFoundHandler())
	mux.HandleFunc("/", s.static)
	return headers(s.guard(mux))
}

// guard refuses requests from other sites. The UI has no
// authentication and is meant for localhost, so:
//   - the Host header must be ours, which stops DNS rebinding;
//   - API requests must come from this origin;
//   - POSTs must be JSON, which a browser never sends cross-site without
//     a preflight check that this server does not answer.
func (s *Server) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.hostAllowed(r.Host) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "cross-site request refused"})
				return
			}
			if o := r.Header.Get("Origin"); o != "" && o != "http://"+r.Host && o != "https://"+r.Host {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "cross-origin request refused"})
				return
			}
			if r.Method == http.MethodPost && r.ContentLength != 0 {
				if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
					writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "send application/json"})
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) hostAllowed(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(strings.ToLower(host), "[]")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return true
	}
	for _, a := range s.AllowedHosts {
		if strings.EqualFold(strings.TrimSpace(a), host) {
			return true
		}
	}
	return false
}

// headers sets the security headers on every response. The app loads
// nothing from other origins.
func headers(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func badRequest(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": msg})
}

// fail answers a backend error. Only the comment validator's
// rejections, which are written for the user, are shown; a missing
// workflow is a 404; anything else, including activity failures (which
// the SDK also reports as application errors), is logged and answered
// generically, so no internal detail reaches the browser.
func (s *Server) fail(w http.ResponseWriter, what string, err error) {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.Type() == app.ErrCommentRejected {
		badRequest(w, appErr.Message())
		return
	}
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such ticket"})
		return
	}
	s.Log.Error("ui request failed", "request", what, "error", err)
	writeJSON(w, http.StatusBadGateway, map[string]string{"error": what + " failed; see the showcase log"})
}

// readJSON decodes a small JSON body, rejecting unknown fields.
func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil || dec.More() {
		badRequest(w, "invalid request body")
		return false
	}
	return true
}

func pathID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !workflowID.MatchString(id) {
		badRequest(w, "invalid ticket id")
		return "", false
	}
	return id, true
}

func (s *Server) config(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"namespace": s.Namespace, "embedding": s.Embedding, "temporal_ui": s.TemporalUI})
}

func (s *Server) list(w http.ResponseWriter, r *http.Request, query string, limit int) {
	list, err := desk.List(r.Context(), s.Client, s.Namespace, query, limit)
	if err != nil {
		s.fail(w, "listing workflows", err)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) listTickets(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, ticketsQuery, listLimit)
}

// workflows runs an operator's visibility query, in Temporal's list
// filter syntax, against the namespace.
func (s *Server) workflows(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("query"))
	if len(q) > maxQuery {
		badRequest(w, fmt.Sprintf("query is longer than %d characters", maxQuery))
		return
	}
	s.list(w, r, q, 2*listLimit)
}

func (s *Server) openTicket(w http.ResponseWriter, r *http.Request) {
	var nt desk.NewTicket
	if !readJSON(w, r, &nt) {
		return
	}
	id, err := desk.Open(r.Context(), s.Client, nt, s.SLABase)
	if err != nil {
		if !isBackend(err) {
			badRequest(w, err.Error())
			return
		}
		s.fail(w, "opening the ticket", err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

// isBackend tells a Temporal error from desk's own input validation,
// which never reaches the client.
func isBackend(err error) bool {
	return !errors.Is(err, desk.ErrInvalid)
}

func (s *Server) ticket(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	st, err := desk.State(r.Context(), s.Client, id)
	if err != nil {
		s.fail(w, "reading the ticket", err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) signal(w http.ResponseWriter, r *http.Request, field string, send func(ctx context.Context, c desk.Client, id, v string) error) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	body := map[string]string{}
	if !readJSON(w, r, &body) {
		return
	}
	for k := range body {
		if k != field {
			badRequest(w, "invalid request body")
			return
		}
	}
	if err := send(r.Context(), s.Client, id, body[field]); err != nil {
		if !isBackend(err) {
			badRequest(w, err.Error())
			return
		}
		s.fail(w, "updating the ticket", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) assign(w http.ResponseWriter, r *http.Request) { s.signal(w, r, "agent", desk.Assign) }

func (s *Server) resolve(w http.ResponseWriter, r *http.Request) {
	s.signal(w, r, "resolution", desk.Resolve)
}

func (s *Server) comment(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		Text string `json:"text"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	n, err := desk.Comment(r.Context(), s.Client, id, body.Text)
	if err != nil {
		if !isBackend(err) {
			badRequest(w, err.Error())
			return
		}
		s.fail(w, "adding the comment", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"comments": n})
}

func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" || len(q) > maxSearch {
		badRequest(w, fmt.Sprintf("q must be 1-%d characters", maxSearch))
		return
	}
	res, err := s.Search.Search(r.Context(), q, searchLimit)
	if err != nil {
		s.fail(w, "searching the knowledge base", err)
		return
	}
	if res.Text == nil {
		res.Text = []kb.Hit{}
	}
	if res.Vector == nil {
		res.Vector = []kb.Hit{}
	}
	if res.Hybrid == nil {
		res.Hybrid = []kb.Hit{}
	}
	writeJSON(w, http.StatusOK, struct {
		Mode string `json:"mode"`
		kb.Results
	}{s.Embedding, res})
}

func (s *Server) report(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	rep, err := desk.Report(ctx, s.Client)
	if err != nil {
		s.fail(w, "building the report", err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

const placeholder = `<!doctype html><meta charset="utf-8"><title>Support desk</title>
<p>The UI is not built. Run <code>npm ci &amp;&amp; npm run build</code> in
<code>examples/showcase/ui</code>, then rebuild the showcase. The API is up at <code>/api/</code>.</p>`

// static serves the app's files, and index.html for any other path so
// the app's own routes work on reload.
func (s *Server) static(w http.ResponseWriter, r *http.Request) {
	index, err := fs.ReadFile(s.Static, "index.html")
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, placeholder)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/")
	if name != "" && name != "index.html" {
		if st, err := fs.Stat(s.Static, name); err == nil && !st.IsDir() {
			http.ServeFileFS(w, r, s.Static, name)
			return
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(index)
}
