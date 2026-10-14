package config_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/config"
)

// Helper for concise option-map construction in the test table.
type opts = map[string]any

// TestConfig_ParseValidURI: a config with a
// full `uri` parses cleanly, and the URI flows through into the resolved
// config without modification.
func TestConfig_ParseValidURI(t *testing.T) {
	cfg, warnings, err := config.ParseConfig(opts{
		"uri":      "mongodb://host1:27017,host2:27017/?replicaSet=rs0",
		"database": "temporal",
	})
	if err != nil {
		t.Fatalf("ParseConfig returned error: %v (warnings: %v)", err, warnings)
	}
	if cfg.URI != "mongodb://host1:27017,host2:27017/?replicaSet=rs0" {
		t.Errorf("URI not preserved: %q", cfg.URI)
	}
	if cfg.Database != "temporal" {
		t.Errorf("Database wrong: %q", cfg.Database)
	}
}

// TestConfig_ParseValidComponents second branch: without
// a URI, component fields (hosts, auth, TLS) are accepted and a URI is
// synthesized.
func TestConfig_ParseValidComponents(t *testing.T) {
	cfg, _, err := config.ParseConfig(opts{
		"hosts":      []any{"a:27017", "b:27017"},
		"replicaSet": "rs0",
		"database":   "temporal",
		"username":   "temporal",
		"password":   "s3cret",
		"authSource": "admin",
	})
	if err != nil {
		t.Fatalf("ParseConfig returned error: %v", err)
	}
	if cfg.URI == "" {
		t.Error("expected synthesized URI when components supplied")
	}
	if !strings.Contains(cfg.URI, "replicaSet=rs0") {
		t.Errorf("URI missing replicaSet: %q", cfg.URI)
	}
	if !strings.Contains(cfg.URI, "authSource=admin") {
		t.Errorf("URI missing authSource: %q", cfg.URI)
	}
}

// TestConfig_MutualExclusionURIAndComponents: providing
// both a URI and component fields is a startup error.
func TestConfig_MutualExclusionURIAndComponents(t *testing.T) {
	_, _, err := config.ParseConfig(opts{
		"uri":      "mongodb://a:27017/",
		"hosts":    []any{"a:27017"},
		"database": "temporal",
	})
	if err == nil {
		t.Fatal("expected error when both uri and component fields set")
	}
	if !strings.Contains(err.Error(), "mutual") && !strings.Contains(err.Error(), "both") {
		t.Errorf("error text should explain mutual exclusion: %v", err)
	}
}

// TestConfig_UnknownOptionWarns: unknown keys produce a
// warning, not an error — so forward-compatible server configs don't
// break older plugin builds.
func TestConfig_UnknownOptionWarns(t *testing.T) {
	_, warnings, err := config.ParseConfig(opts{
		"uri":          "mongodb://a:27017/",
		"database":     "temporal",
		"futureOption": "x",
	})
	if err != nil {
		t.Fatalf("unknown option should not error: %v", err)
	}
	if len(warnings) == 0 {
		t.Fatal("expected a warning for unknown option")
	}
	joined := strings.Join(warnings, " ")
	if !strings.Contains(joined, "futureOption") {
		t.Errorf("warning text should mention the unknown key: %v", warnings)
	}
}

// TestConfig_MissingDatabaseFails: an empty database is
// rejected at parse time so the plugin fails fast at startup rather than
// at first query.
func TestConfig_MissingDatabaseFails(t *testing.T) {
	_, _, err := config.ParseConfig(opts{
		"uri": "mongodb://a:27017/",
	})
	if err == nil {
		t.Fatal("expected error when database is missing")
	}
}

// TestConfig_InvalidDatabaseNameFails also: names with
// MongoDB-reserved characters are rejected.
func TestConfig_InvalidDatabaseNameFails(t *testing.T) {
	_, _, err := config.ParseConfig(opts{
		"uri":      "mongodb://a:27017/",
		"database": "bad name/with?specials",
	})
	if err == nil {
		t.Fatal("expected error for invalid database name")
	}
}

// TestConfig_CredentialEscaping: special characters in
// credentials are URL-escaped when synthesizing a URI.
func TestConfig_CredentialEscaping(t *testing.T) {
	cfg, _, err := config.ParseConfig(opts{
		"hosts":    []any{"a:27017"},
		"database": "temporal",
		"username": "user@admin",
		"password": "p@ss w0rd",
	})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if strings.Contains(cfg.URI, "user@admin:p@ss w0rd") {
		t.Errorf("URI contains unescaped credentials: %q", cfg.URI)
	}
	if !strings.Contains(cfg.URI, "user%40admin") {
		t.Errorf("username not percent-escaped: %q", cfg.URI)
	}
	if !strings.Contains(cfg.URI, "p%40ss%20w0rd") {
		t.Errorf("password not percent-escaped: %q", cfg.URI)
	}
}

// TestConfig_WriteConcernNonMajorityWarns: any write
// concern other than "majority" emits a startup warning because Temporal
// correctness depends on durable writes.
func TestConfig_WriteConcernNonMajorityWarns(t *testing.T) {
	_, warnings, err := config.ParseConfig(opts{
		"uri":          "mongodb://a:27017/",
		"database":     "temporal",
		"writeConcern": "1",
	})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	joined := strings.Join(warnings, " ")
	if !strings.Contains(joined, "majority") {
		t.Errorf("expected majority-write-concern warning; got: %v", warnings)
	}
}

// TestConfig_PasswordEnvResolution branch 2: password
// may be supplied via an env-var reference.
func TestConfig_PasswordEnvResolution(t *testing.T) {
	t.Setenv("MONGO_TEMPORAL_PW", "secret-from-env")
	cfg, _, err := config.ParseConfig(opts{
		"hosts":       []any{"a:27017"},
		"database":    "temporal",
		"username":    "temporal",
		"passwordEnv": "MONGO_TEMPORAL_PW",
	})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Password != "secret-from-env" {
		t.Errorf("password not resolved from env: got %q", cfg.Password)
	}
	if strings.Contains(cfg.Redacted(), "secret-from-env") {
		t.Error("Redacted() leaked the password")
	}
}

// TestConfig_PasswordEnvMissingFails: an env-var reference that does not
// resolve is a startup error.
func TestConfig_PasswordEnvMissingFails(t *testing.T) {
	_ = os.Unsetenv("DEFINITELY_UNSET_TEMPORAL_PW_VAR")
	_, _, err := config.ParseConfig(opts{
		"hosts":       []any{"a:27017"},
		"database":    "temporal",
		"username":    "temporal",
		"passwordEnv": "DEFINITELY_UNSET_TEMPORAL_PW_VAR",
	})
	if err == nil {
		t.Fatal("expected error when passwordEnv is unset")
	}
}

// TestConfig_PasswordCommandResolution branch 3: a
// shell command whose stdout becomes the password.
func TestConfig_PasswordCommandResolution(t *testing.T) {
	cfg, _, err := config.ParseConfig(opts{
		"hosts":           []any{"a:27017"},
		"database":        "temporal",
		"username":        "temporal",
		"passwordCommand": "printf cmd-secret",
	})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Password != "cmd-secret" {
		t.Errorf("passwordCommand did not resolve: got %q", cfg.Password)
	}
}

// TestConfig_PasswordCommandTimeout covers the timeout branch of
// passwordCommand: a slow command is aborted and reported as an error,
// so plugin startup never blocks indefinitely on a user-supplied
// resolver.
func TestConfig_PasswordCommandTimeout(t *testing.T) {
	// Use sleep to force a timeout. ParseConfig's budget is small enough
	// that any sleep > 1s exceeds it.
	start := time.Now()
	_, _, err := config.ParseConfig(opts{
		"hosts":                  []any{"a:27017"},
		"database":               "temporal",
		"username":               "temporal",
		"passwordCommand":        "sleep 10",
		"passwordCommandTimeout": 200, // milliseconds
	})
	if err == nil {
		t.Fatal("expected error when passwordCommand exceeds timeout")
	}
	if time.Since(start) > 3*time.Second {
		t.Errorf("timeout did not fire in reasonable time: took %v", time.Since(start))
	}
}

// TestConfig_RedactedNeverLeaksCredentials is a belt-and-suspenders
// regression for credential redaction: whatever path credentials take
// into the config, Redacted() must not surface them.
func TestConfig_RedactedNeverLeaksCredentials(t *testing.T) {
	cfg, _, err := config.ParseConfig(opts{
		"uri":      "mongodb://temporal:s3cret@a:27017/?authSource=admin",
		"database": "temporal",
	})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	red := cfg.Redacted()
	if strings.Contains(red, "s3cret") {
		t.Errorf("Redacted leaked password: %q", red)
	}
}

// TestConfig_DefaultsApplied: parse applies the required
// defaults (majority concerns, primary read pref, retry on).
func TestConfig_DefaultsApplied(t *testing.T) {
	cfg, _, err := config.ParseConfig(opts{
		"uri":      "mongodb://a:27017/",
		"database": "temporal",
	})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.WriteConcern != "majority" {
		t.Errorf("expected default writeConcern=majority, got %q", cfg.WriteConcern)
	}
	if cfg.ReadConcern != "majority" {
		t.Errorf("expected default readConcern=majority, got %q", cfg.ReadConcern)
	}
	if cfg.ReadPreference != "primary" {
		t.Errorf("expected default readPreference=primary, got %q", cfg.ReadPreference)
	}
	if !cfg.RetryWrites {
		t.Error("expected default retryWrites=true")
	}
	if !cfg.RetryReads {
		t.Error("expected default retryReads=true")
	}
}

// TestParseConfig_URIUserinfoAndUsername_Rejected:
// credentials in two places are ambiguous and must fail fast.
func TestParseConfig_URIUserinfoAndUsername_Rejected(t *testing.T) {
	_, _, err := config.ParseConfig(map[string]any{
		"uri": "mongodb://a:b@h1/?replicaSet=rs0", "database": "temporal", "username": "c",
	})
	if err == nil || !strings.Contains(err.Error(), "username") {
		t.Fatalf("want username/uri conflict error; got %v", err)
	}
}

// TestParseConfig_StringNumbers_Accepted: values that
// arrive as strings from env-templated YAML are honoured.
func TestParseConfig_StringNumbers_Accepted(t *testing.T) {
	cfg, _, err := config.ParseConfig(map[string]any{
		"uri": "mongodb://h1/?replicaSet=rs0", "database": "temporal",
		"maxPoolSize": "200", "minPoolSize": "5", "retryWrites": "false", "tlsInsecureSkipVerify": "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxPoolSize != 200 || cfg.MinPoolSize != 5 || cfg.RetryWrites || !cfg.TLSInsecureSkipVerify {
		t.Fatalf("string values ignored: %+v", cfg)
	}
}

func TestParseConfig_InvalidRanges_Rejected(t *testing.T) {
	cases := []map[string]any{
		{"maxPoolSize": -1},
		{"minPoolSize": 10, "maxPoolSize": 5},
		{"maxPoolSize": "lots"},
		{"retryWrites": "maybe"},
		{"connectTimeoutMs": -5},
		{"maxPoolSize": 1.5},
	}
	for _, extra := range cases {
		raw := map[string]any{"uri": "mongodb://h1/?replicaSet=rs0", "database": "temporal"}
		for k, v := range extra {
			raw[k] = v
		}
		if _, _, err := config.ParseConfig(raw); err == nil {
			t.Errorf("config.ParseConfig(%v) accepted invalid input", extra)
		}
	}
}
