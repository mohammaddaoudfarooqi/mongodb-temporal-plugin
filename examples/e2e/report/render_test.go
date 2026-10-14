package report

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeRun(t *testing.T, meta Meta, phases ...Results) string {
	t.Helper()
	dir := t.TempDir()
	if err := writeJSONFile(filepath.Join(dir, "meta.json"), meta); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "phases"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range phases {
		if err := Write(filepath.Join(dir, "phases", p.Phase+".json"), p); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

var okSmoke = Results{Phase: "smoke", Scenarios: []Scenario{{Name: "order_saga", Pass: true, DurationMS: 10}}}

// GO only when every required phase is present and passed.
func TestRender_AllRequiredPhasesPass_Go(t *testing.T) {
	dir := writeRun(t, Meta{Profile: "functional", RunID: "r1", Required: []string{"smoke", "load"}},
		okSmoke, Results{Phase: "load", Load: &Load{Workflows: 5, Completed: 5, PerSecond: 2, P99: time.Second}})
	rep, err := Render(dir)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Verdict != "GO" {
		t.Fatalf("verdict %s, want GO\n%s", rep.Verdict, rep.Markdown)
	}
	if !strings.Contains(rep.Markdown, "**Verdict: GO**") {
		t.Errorf("markdown lacks verdict line:\n%s", rep.Markdown)
	}
}

// A missing required phase is a NO-GO and is named.
func TestRender_MissingRequiredPhase_NoGo(t *testing.T) {
	dir := writeRun(t, Meta{Profile: "functional", RunID: "r1", Required: []string{"smoke", "failover"}}, okSmoke)
	rep, _ := Render(dir)
	if rep.Verdict != "NO-GO" || !strings.Contains(rep.Markdown, "failover") || !strings.Contains(rep.Markdown, "missing") {
		t.Fatalf("verdict %s; markdown:\n%s", rep.Verdict, rep.Markdown)
	}
}

// A failed scenario makes the phase and the run NO-GO, and
// its error appears in the report.
func TestRender_FailedScenario_NoGoWithError(t *testing.T) {
	bad := Results{Phase: "smoke", Scenarios: []Scenario{{Name: "schedule", Pass: false, Error: "found 0 runs"}}}
	dir := writeRun(t, Meta{Profile: "functional", RunID: "r1", Required: []string{"smoke"}}, bad)
	rep, _ := Render(dir)
	if rep.Verdict != "NO-GO" || !strings.Contains(rep.Markdown, "found 0 runs") {
		t.Fatalf("verdict %s; markdown:\n%s", rep.Verdict, rep.Markdown)
	}
}

// Host CPU and memory peaks are reported per host.
func TestRender_HostSamples_PeaksReported(t *testing.T) {
	dir := writeRun(t, Meta{Profile: "load", RunID: "r1", Required: []string{"smoke"}}, okSmoke)
	h := filepath.Join(dir, "hosts", "server-1")
	if err := os.MkdirAll(h, 0o755); err != nil {
		t.Fatal(err)
	}
	csv := "ts,cpu_pct,mem_used_pct,server_rss_kb\n1,12.5,40.0,100000\n2,91.0,55.5,250000\n3,30.0,50.0,200000\n"
	if err := os.WriteFile(filepath.Join(h, "samples.csv"), []byte(csv), 0o644); err != nil {
		t.Fatal(err)
	}
	rep, err := Render(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"server-1", "91.0", "55.5", "244"} { // 250000 KiB ≈ 244 MiB
		if !strings.Contains(rep.Markdown, want) {
			t.Errorf("markdown lacks %q:\n%s", want, rep.Markdown)
		}
	}
	if !strings.Contains(rep.Markdown, "host-bound") {
		t.Errorf("a host above 80%% CPU must be flagged as host-bound:\n%s", rep.Markdown)
	}
}

// A phase whose fault injection failed is a NO-GO even if
// every workflow completed.
func TestRender_FaultError_NoGo(t *testing.T) {
	ph := Results{Phase: "failover", Load: &Load{Workflows: 5, Completed: 5}, FaultError: "restartPrimaries returned HTTP 500"}
	dir := writeRun(t, Meta{Profile: "functional", RunID: "r1", Required: []string{"failover"}}, ph)
	rep, _ := Render(dir)
	if rep.Verdict != "NO-GO" || !strings.Contains(rep.Markdown, "HTTP 500") {
		t.Fatalf("verdict %s; markdown:\n%s", rep.Verdict, rep.Markdown)
	}
}

// Host peaks count only samples inside measured load
// phases, so a compile spike during the Atlas gates can't flag the run
// as host-bound.
func TestRender_HostPeaks_OnlyDuringLoadPhases(t *testing.T) {
	ph := Results{Phase: "sanity-load", StartedAt: 100, FinishedAt: 200,
		Load: &Load{Workflows: 5, Completed: 5}, Fault: "Atlas failover accepted; cluster healthy after 312s"}
	dir := writeRun(t, Meta{Profile: "functional", RunID: "r1", Required: []string{"sanity-load"}}, ph)
	h := filepath.Join(dir, "hosts", "server-1")
	if err := os.MkdirAll(h, 0o755); err != nil {
		t.Fatal(err)
	}
	csv := "ts,cpu_pct,mem_used_pct,server_rss_kb\n50,99.8,60.0,900000\n150,30.0,40.0,200000\n250,95.0,70.0,800000\n"
	if err := os.WriteFile(filepath.Join(h, "samples.csv"), []byte(csv), 0o644); err != nil {
		t.Fatal(err)
	}
	rep, err := Render(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rep.Markdown, "| server-1 | 30.0 | 40.0 | 195 |") {
		t.Errorf("peaks must come from the load window only:\n%s", rep.Markdown)
	}
	if strings.Contains(rep.Markdown, "host-bound") {
		t.Errorf("a spike outside load phases must not flag host-bound:\n%s", rep.Markdown)
	}
	if !strings.Contains(rep.Markdown, "cluster healthy after 312s") {
		t.Errorf("the fault note must be reported:\n%s", rep.Markdown)
	}
}
