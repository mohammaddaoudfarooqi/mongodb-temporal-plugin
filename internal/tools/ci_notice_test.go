package tools

import (
	"regexp"
	"strings"
	"testing"
)

// TestCI_ActionsPinnedBySHA: third-party actions are
// referenced by immutable commit SHA, not a movable tag.
func TestCI_ActionsPinnedBySHA(t *testing.T) {
	ci := readFile(t, ".github", "workflows", "ci.yml")
	uses := regexp.MustCompile(`uses:\s*(\S+)`).FindAllStringSubmatch(ci, -1)
	if len(uses) == 0 {
		t.Fatal("no actions found")
	}
	sha := regexp.MustCompile(`@[0-9a-f]{40}$`)
	for _, u := range uses {
		if !sha.MatchString(u[1]) {
			t.Errorf("action not pinned by SHA: %s", u[1])
		}
	}
	if strings.Contains(ci, "staticcheck@latest") {
		t.Error("staticcheck is installed @latest; pin a version")
	}
}

// TestCI_BuildsAndLintsImage: CI builds the binaries
// and the container image and runs the image lint.
func TestCI_BuildsAndLintsImage(t *testing.T) {
	ci := readFile(t, ".github", "workflows", "ci.yml")
	for _, want := range []string{"make build", "docker build", "ci/scripts/docker-image-lint.sh"} {
		if !strings.Contains(ci, want) {
			t.Errorf("ci.yml does not run %q", want)
		}
	}
}

// TestNotice_ListsDirectDependencies: every direct
// dependency in go.mod is attributed in NOTICE (a module path or its
// parent family, e.g. go.opentelemetry.io/otel covers otel/sdk).
func TestNotice_ListsDirectDependencies(t *testing.T) {
	gomod := readFile(t, "go.mod")
	notice := readFile(t, "NOTICE")
	block := regexp.MustCompile(`(?s)require \((.*?)\)`).FindAllStringSubmatch(gomod, -1)
	var direct []string
	for _, b := range block {
		for _, line := range strings.Split(b[1], "\n") {
			f := strings.Fields(line)
			if len(f) >= 2 && !strings.Contains(line, "// indirect") {
				direct = append(direct, f[0])
			}
		}
	}
	if len(direct) == 0 {
		t.Fatal("no direct dependencies parsed from go.mod")
	}
	attributed := regexp.MustCompile(`\(([a-z0-9.\-]+\.[a-z]+/[^)\s]+)\)`).FindAllStringSubmatch(notice, -1)
	covered := func(mod string) bool {
		for _, a := range attributed {
			if mod == a[1] || strings.HasPrefix(mod, a[1]+"/") {
				return true
			}
		}
		return false
	}
	for _, mod := range direct {
		if !covered(mod) {
			t.Errorf("direct dependency %s is not attributed in NOTICE", mod)
		}
	}
}
