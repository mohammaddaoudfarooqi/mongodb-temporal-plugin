package tools

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestDeployRenderConfig_SpecialCharsInURI: rendering
// the Temporal config must insert the URI verbatim. The documented Atlas
// URI contains '&' (which sed treats as "the matched text"), and
// passwords may contain '|' or '\'.
func TestDeployRenderConfig_SpecialCharsInURI(t *testing.T) {
	root := repoRoot(t)
	uri := `mongodb+srv://user:p|a\ss&w0rd@cluster0.example.net/?retryWrites=true&w=majority&appName=x`
	dst := filepath.Join(t.TempDir(), "development.yaml")
	cmd := exec.Command("sh", filepath.Join(root, "deploy", "config", "render-config.sh"),
		filepath.Join(root, "deploy", "config", "development.yaml"), dst)
	cmd.Env = append(os.Environ(), "TEMPORAL_MONGO_URI="+uri, "TEMPORAL_VISIBILITY_MODE=aggregation")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("render-config.sh: %v\n%s", err, out)
	}
	rendered, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(rendered), uri) != 2 {
		t.Fatalf("URI not rendered verbatim (want 2 occurrences):\n%s", rendered)
	}
	if strings.Contains(string(rendered), "${") {
		t.Fatalf("unrendered placeholder left:\n%s", rendered)
	}
}

// TestCompose_UsesRenderScript keeps the compose file on the safe renderer.
func TestCompose_UsesRenderScript(t *testing.T) {
	compose := readFile(t, "deploy", "docker-compose.yml")
	if !strings.Contains(compose, "render-config.sh") {
		t.Fatal("docker-compose.yml does not render config with render-config.sh")
	}
	if regexp.MustCompile(`sed[^\n]*\$\$\{TEMPORAL_MONGO_URI\}\|`).MatchString(compose) {
		t.Fatal("docker-compose.yml still substitutes the URI with an unescaped sed replacement")
	}
}

// TestCompose_MongoBindsLocalhost: the bundled
// unauthenticated mongod must not be published on every interface.
func TestCompose_MongoBindsLocalhost(t *testing.T) {
	compose := readFile(t, "deploy", "docker-compose.yml")
	if !strings.Contains(compose, `"127.0.0.1:${MONGO_HOST_PORT:-27017}:27017"`) {
		t.Fatal("mongo port is not bound to 127.0.0.1")
	}
}

// TestDockerfile_HealthcheckProbesServer: the health
// check must fail when the server is not serving. Running the server
// binary without arguments prints help and exits 0.
func TestDockerfile_HealthcheckProbesServer(t *testing.T) {
	df := readFile(t, "Dockerfile")
	i := strings.Index(df, "HEALTHCHECK")
	if i < 0 {
		t.Fatal("HEALTHCHECK missing")
	}
	block := df[i:]
	if j := strings.Index(block, "\nENTRYPOINT"); j > 0 {
		block = block[:j]
	}
	if !strings.Contains(block, "nc -z 127.0.0.1 7233") {
		t.Fatalf("HEALTHCHECK does not probe the frontend port:\n%s", block)
	}
	if regexp.MustCompile(`temporal-mongodb-server\s*>\s*/dev/null`).MatchString(block) {
		t.Fatalf("HEALTHCHECK still runs the server binary as a liveness probe:\n%s", block)
	}
}

func readFile(t *testing.T, parts ...string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(append([]string{repoRoot(t)}, parts...)...))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
