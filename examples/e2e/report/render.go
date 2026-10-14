package report

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Meta describes one qualification run; the orchestrator writes it.
type Meta struct {
	Profile    string   `json:"profile"`
	RunID      string   `json:"run_id"`
	Ref        string   `json:"ref"`
	Commit     string   `json:"commit"`
	AtlasTier  string   `json:"atlas_tier"`
	MongoDB    string   `json:"mongodb_version"`
	Servers    int      `json:"server_count"`
	Shards     int      `json:"history_shards"`
	Required   []string `json:"required_phases"`
	StartedAt  string   `json:"started_at"`
	FinishedAt string   `json:"finished_at"`
}

// Rendered is the report and its verdict.
type Rendered struct {
	Verdict  string
	Markdown string
}

// hostBoundCPU is the CPU peak above which a host, not the database,
// may have limited the result.
const hostBoundCPU = 80.0

// Render reads <dir>/meta.json, <dir>/phases/*.json and
// <dir>/hosts/*/samples.csv and renders the run report.
func Render(dir string) (Rendered, error) {
	var meta Meta
	b, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return Rendered{Verdict: "NO-GO"}, fmt.Errorf("report: %w", err)
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		return Rendered{Verdict: "NO-GO"}, fmt.Errorf("report: meta.json: %w", err)
	}
	phases := map[string]Results{}
	files, _ := filepath.Glob(filepath.Join(dir, "phases", "*.json"))
	sort.Strings(files)
	var order []string
	for _, f := range files {
		r, err := Read(f)
		if err != nil {
			return Rendered{Verdict: "NO-GO"}, err
		}
		phases[r.Phase] = r
		order = append(order, r.Phase)
	}

	var md strings.Builder
	verdict := "GO"
	var problems []string
	for _, name := range meta.Required {
		r, ok := phases[name]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("required phase `%s` is missing", name))
		case !r.Pass():
			problems = append(problems, fmt.Sprintf("phase `%s` failed", name))
		}
	}
	if len(problems) > 0 {
		verdict = "NO-GO"
	}

	fmt.Fprintf(&md, "# Release qualification report: %s (%s)\n\n", meta.Profile, meta.RunID)
	fmt.Fprintf(&md, "**Verdict: %s**\n\n", verdict)
	for _, p := range problems {
		fmt.Fprintf(&md, "- %s\n", p)
	}
	if len(problems) > 0 {
		md.WriteString("\n")
	}
	md.WriteString("| Setting | Value |\n|---|---|\n")
	for _, kv := range [][2]string{
		{"ref", meta.Ref}, {"commit", meta.Commit}, {"Atlas tier", meta.AtlasTier},
		{"MongoDB", meta.MongoDB}, {"server hosts", strconv.Itoa(meta.Servers)},
		{"history shards", strconv.Itoa(meta.Shards)}, {"started", meta.StartedAt}, {"finished", meta.FinishedAt},
	} {
		if kv[1] != "" && kv[1] != "0" {
			fmt.Fprintf(&md, "| %s | %s |\n", kv[0], kv[1])
		}
	}

	md.WriteString("\n## Phases\n\n| Phase | Result | Detail |\n|---|---|---|\n")
	for _, name := range order {
		r := phases[name]
		res := "PASS"
		if !r.Pass() {
			res = "FAIL"
		}
		fmt.Fprintf(&md, "| %s | %s | %s |\n", name, res, phaseDetail(r))
	}
	for _, name := range order {
		writePhase(&md, phases[name])
	}
	var windows [][2]int64
	for _, name := range order {
		if r := phases[name]; (r.Load != nil || r.Ladder != nil) && r.StartedAt > 0 {
			windows = append(windows, [2]int64{r.StartedAt, r.FinishedAt})
		}
	}
	if err := writeHosts(&md, dir, windows); err != nil {
		return Rendered{Verdict: verdict, Markdown: md.String()}, err
	}
	return Rendered{Verdict: verdict, Markdown: md.String()}, nil
}

func phaseDetail(r Results) string {
	switch {
	case r.FaultError != "":
		return "fault not injected: " + cell(r.FaultError)
	case r.SetupError != "":
		return "setup: " + cell(r.SetupError)
	case r.Ladder != nil:
		return fmt.Sprintf("max sustainable concurrency %d", r.Ladder.MaxSustainable)
	case r.Load != nil:
		return loadLine(*r.Load)
	}
	pass := 0
	for _, s := range r.Scenarios {
		if s.Pass {
			pass++
		}
	}
	return fmt.Sprintf("%d/%d checks passed", pass, len(r.Scenarios))
}

func loadLine(l Load) string {
	return fmt.Sprintf("%d/%d completed, %.1f wf/s, p50 %s p99 %s max %s",
		l.Completed, l.Workflows, l.PerSecond, ms(l.P50), ms(l.P99), ms(l.Max))
}

func writePhase(md *strings.Builder, r Results) {
	fmt.Fprintf(md, "\n### %s\n\n", r.Phase)
	if r.SetupError != "" {
		fmt.Fprintf(md, "Setup failed: %s\n", r.SetupError)
	}
	if r.Fault != "" {
		fmt.Fprintf(md, "Fault: %s.\n\n", r.Fault)
	}
	if r.FaultError != "" {
		fmt.Fprintf(md, "Fault not injected: %s. The load result below does not show that the fault was survived.\n\n", r.FaultError)
	}
	if len(r.Scenarios) > 0 {
		md.WriteString("| Check | Result | Time | Error |\n|---|---|---|---|\n")
		for _, s := range r.Scenarios {
			res := "PASS"
			if !s.Pass {
				res = "FAIL"
			}
			fmt.Fprintf(md, "| %s | %s | %s | %s |\n", s.Name, res, ms(time.Duration(s.DurationMS)*time.Millisecond), cell(s.Error))
		}
	}
	if r.Load != nil {
		writeLoadTable(md, []Load{*r.Load})
	}
	if r.Ladder != nil {
		fmt.Fprintf(md, "Thresholds: p99 <= %s, error rate <= %.2g. Max sustainable concurrency: **%d**.\n\n",
			ms(r.Ladder.MaxP99), r.Ladder.MaxErrorRate, r.Ladder.MaxSustainable)
		writeLoadTable(md, r.Ladder.Steps)
	}
}

func writeLoadTable(md *strings.Builder, ls []Load) {
	md.WriteString("| Concurrency | Workflows | Completed | Failed | wf/s | p50 | p95 | p99 | max | Errors |\n|---|---|---|---|---|---|---|---|---|---|\n")
	for _, l := range ls {
		var errs []string
		for k, v := range l.ErrorsByClass {
			errs = append(errs, fmt.Sprintf("%s: %d", k, v))
		}
		sort.Strings(errs)
		fmt.Fprintf(md, "| %d | %d | %d | %d | %.1f | %s | %s | %s | %s | %s |\n",
			l.Concurrency, l.Workflows, l.Completed, l.Failed, l.PerSecond,
			ms(l.P50), ms(l.P95), ms(l.P99), ms(l.Max), strings.Join(errs, ", "))
	}
	md.WriteString("\n")
}

// writeHosts renders per-host CPU, memory and server RSS peaks over the
// load phases' windows (all samples when no window is known), so work
// outside a measurement, such as compiling the Atlas gates, can't make
// a run look host-bound.
func writeHosts(md *strings.Builder, dir string, windows [][2]int64) error {
	files, _ := filepath.Glob(filepath.Join(dir, "hosts", "*", "samples.csv"))
	if len(files) == 0 {
		return nil
	}
	sort.Strings(files)
	scope := "during the load phases"
	if len(windows) == 0 {
		scope = "over the whole run"
	}
	fmt.Fprintf(md, "\n## Hosts\n\nPeaks %s.\n\n", scope)
	md.WriteString("| Host | Peak CPU % | Peak memory % | Peak server RSS (MiB) | Note |\n|---|---|---|---|---|\n")
	for _, f := range files {
		host := filepath.Base(filepath.Dir(f))
		cpu, mem, rss, err := peaks(f, windows)
		if err != nil {
			return err
		}
		note := ""
		if cpu > hostBoundCPU {
			note = "host-bound: results may be limited by this host, not the database"
		}
		fmt.Fprintf(md, "| %s | %.1f | %.1f | %.0f | %s |\n", host, cpu, mem, rss/1024, note)
	}
	return nil
}

func peaks(path string, windows [][2]int64) (cpu, mem, rssKB float64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("report: %w", err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("report: %s: %w", path, err)
	}
	for i, row := range rows {
		if i == 0 || len(row) < 4 {
			continue
		}
		if ts, _ := strconv.ParseInt(row[0], 10, 64); !inWindows(ts, windows) {
			continue
		}
		c, _ := strconv.ParseFloat(row[1], 64)
		m, _ := strconv.ParseFloat(row[2], 64)
		r, _ := strconv.ParseFloat(row[3], 64)
		cpu, mem, rssKB = max(cpu, c), max(mem, m), max(rssKB, r)
	}
	return cpu, mem, rssKB, nil
}

func inWindows(ts int64, windows [][2]int64) bool {
	if len(windows) == 0 {
		return true
	}
	for _, w := range windows {
		if ts >= w[0] && (w[1] == 0 || ts <= w[1]) {
			return true
		}
	}
	return false
}

func ms(d time.Duration) string { return d.Round(time.Millisecond).String() }

func cell(s string) string { return strings.ReplaceAll(strings.ReplaceAll(s, "|", "\\|"), "\n", " ") }

func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
