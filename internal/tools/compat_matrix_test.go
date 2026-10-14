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
