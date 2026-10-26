package tools

import (
	"regexp"
	"strings"
	"testing"
)

// jobMatrix returns the matrix line of a CI job.
func jobMatrix(t *testing.T, ci, job, key string) string {
	t.Helper()
	i := strings.Index(ci, "\n  "+job+":")
	if i < 0 {
		t.Fatalf("job %s missing", job)
	}
	m := regexp.MustCompile(`(?m)^\s+` + key + `:\s*(\[.*\])`).FindStringSubmatch(ci[i:])
	if m == nil {
		t.Fatalf("job %s has no %s matrix", job, key)
	}
	return m[1]
}

// TestCI_MongoMatrixIncludes90: MongoDB 9.0 is
// exercised on every PR (integration) and nightly (conformance), and by
// the Docker functional e2e.
func TestCI_MongoMatrixIncludes90(t *testing.T) {
	ci := readFile(t, ".github", "workflows", "ci.yml")
	for _, job := range []string{"integration-matrix", "conformance-matrix"} {
		if m := jobMatrix(t, ci, job, "mongo"); !strings.Contains(m, `"9.0"`) {
			t.Errorf("%s mongo matrix %s lacks 9.0", job, m)
		}
	}
	if !regexp.MustCompile(`for v in [0-9. ]*9\.0`).MatchString(readFile(t, "ci", "scripts", "docker-functional-e2e.sh")) {
		t.Error("docker-functional-e2e.sh does not cover MongoDB 9.0")
	}
}

// TestCI_TemporalMatrixCoversLatestMinors: the
// latest patch of the current and previous Temporal minors is tested,
// including the integration tier (upstream persistence suites).
func TestCI_TemporalMatrixCoversLatestMinors(t *testing.T) {
	ci := readFile(t, ".github", "workflows", "ci.yml")
	m := jobMatrix(t, ci, "temporal-matrix", "temporal")
	for _, v := range []string{`"v1.31.3"`, `"v1.32.1"`} {
		if !strings.Contains(m, v) {
			t.Errorf("temporal matrix %s lacks %s", m, v)
		}
	}
	script := readFile(t, "ci", "scripts", "temporal-matrix.sh")
	if !strings.Contains(script, "-tags=integration") {
		t.Error("temporal-matrix.sh does not run the integration tier")
	}
}

// jobBlock returns the YAML of one CI job, up to the next job.
func jobBlock(t *testing.T, ci, job string) string {
	t.Helper()
	i := strings.Index(ci, "\n  "+job+":")
	if i < 0 {
		t.Fatalf("job %s missing", job)
	}
	rest := ci[i+1:]
	if j := regexp.MustCompile(`\n  [a-z0-9-]+:\n`).FindStringIndex(rest[1:]); j != nil {
		return rest[:j[0]+1]
	}
	return rest
}

// TestCI_MongoMatrixCoversCurrentReleases: every supported MongoDB
// release line, including the rapid releases, runs through the
// integration and conformance tiers and the Docker functional e2e, and
// one line failing does not cancel the others.
func TestCI_MongoMatrixCoversCurrentReleases(t *testing.T) {
	ci := readFile(t, ".github", "workflows", "ci.yml")
	lines := []string{"7.0", "8.0", "8.2", "8.3", "9.0"}
	for _, job := range []string{"integration-matrix", "conformance-matrix"} {
		m := jobMatrix(t, ci, job, "mongo")
		for _, v := range lines {
			if !strings.Contains(m, `"`+v+`"`) {
				t.Errorf("%s mongo matrix %s lacks %s", job, m, v)
			}
		}
		if !strings.Contains(jobBlock(t, ci, job), "fail-fast: false") {
			t.Errorf("%s does not set fail-fast: false", job)
		}
	}
	loop := regexp.MustCompile(`(?m)^for v in ([0-9. ]+); do`).FindStringSubmatch(readFile(t, "ci", "scripts", "docker-functional-e2e.sh"))
	if loop == nil {
		t.Fatal("docker-functional-e2e.sh has no version loop")
	}
	for _, v := range lines {
		if !strings.Contains(" "+loop[1]+" ", " "+v+" ") {
			t.Errorf("docker-functional-e2e.sh does not cover MongoDB %s", v)
		}
	}
}

// TestCI_RunnersPinned: runner images are pinned, so a GitHub image
// upgrade (a new kernel, a new Docker) can't silently break every job.
// New platforms are tracked by the platform-canary job instead.
func TestCI_RunnersPinned(t *testing.T) {
	ci := readFile(t, ".github", "workflows", "ci.yml")
	if strings.Contains(ci, "-latest") {
		t.Error("ci.yml uses a floating runner image (*-latest)")
	}
	canary := jobBlock(t, ci, "platform-canary")
	if !strings.Contains(canary, "runs-on: ubuntu-26.04") {
		t.Error("platform-canary does not run on the newest runner image")
	}
	if !strings.Contains(canary, "continue-on-error: true") {
		t.Error("platform-canary must not fail the run: it reports platform drift")
	}
}
