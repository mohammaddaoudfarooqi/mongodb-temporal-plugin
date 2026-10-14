package tools

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestImages_PinnedByDigest: every image pulled from a registry (Dockerfile
// FROM lines, compose image: lines, the quickstart host's UI image) is
// pinned by digest, so a re-pointed tag can't change what users run.
// Images this repository builds itself are exempt.
func TestImages_PinnedByDigest(t *testing.T) {
	root := repoRoot(t)
	refs := map[string][]string{}
	from := regexp.MustCompile(`(?m)^FROM\s+(\S+)`)
	image := regexp.MustCompile(`(?m)^\s*image:\s*(\S+)`)
	uiDefault := regexp.MustCompile(`UI_IMAGE=\$\{QS_UI_IMAGE:-([^}]+)\}`)
	scan := func(rel string, re *regexp.Regexp) {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			refs[rel] = append(refs[rel], m[1])
		}
	}
	scan("Dockerfile", from)
	scan("examples/showcase/Dockerfile", from)
	scan("deploy/docker-compose.yml", image)
	scan("quickstart/docker/docker-compose.yml", image)
	scan("deploy/aws-qualify/remote/quickstart.sh", uiDefault)
	n := 0
	for file, list := range refs {
		for _, ref := range list {
			if strings.Contains(ref, "PLUGIN_IMAGE") || strings.Contains(ref, "SHOWCASE_IMAGE") {
				continue // built from this repository
			}
			n++
			if strings.Contains(ref, ":latest") {
				t.Errorf("%s: %s uses the floating latest tag", file, ref)
			}
			if !strings.Contains(ref, "@sha256:") {
				t.Errorf("%s: %s is not pinned by digest", file, ref)
			}
		}
	}
	if n < 6 {
		t.Fatalf("found only %d registry image references; the scan is broken", n)
	}
}
