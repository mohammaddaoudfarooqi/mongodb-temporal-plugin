// Package tools hosts tests for project-level guardrails that do not belong
// to any specific store — e.g., the DCO check, CI pinning and NOTICE drift
// that the Makefile and CI expose. The tests drive the same Makefile targets CI runs, so a
// regression in tooling fails locally before it reaches a PR.
package tools

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// repoRoot walks up from the test file to the module root (where go.mod
// lives). Tests run `make` targets against this root.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(thisFile)
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("could not locate module root")
	return ""
}

// TestDCO_HeadCommitSignedOff_Passes checks that `make dco` succeeds when
// HEAD carries a Signed-off-by line. The project's own commit history is
// DCO-clean, so running against the repo HEAD is a valid positive test.
func TestDCO_HeadCommitSignedOff_Passes(t *testing.T) {
	root := repoRoot(t)

	// If HEAD is missing Signed-off-by (e.g., developer bypassed the hook),
	// the test is inconclusive — skip rather than fail, since this test
	// validates the tool, not the developer's local history.
	msg, err := exec.Command("git", "-C", root, "log", "-1", "--format=%B").CombinedOutput()
	if err != nil {
		t.Skipf("git log failed, skipping: %v", err)
	}
	if !strings.Contains(string(msg), "Signed-off-by:") {
		t.Skipf("HEAD commit is missing Signed-off-by; test is inconclusive")
	}

	cmd := exec.Command("make", "dco")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make dco failed on signed-off HEAD: %v\n%s", err, out)
	}
}
