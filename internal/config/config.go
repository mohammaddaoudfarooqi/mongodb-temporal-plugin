// Package config parses and validates the plugin's runtime configuration
// from the map[string]any Temporal hands via
// config.CustomDatastoreConfig.Options. Lives under internal/ so both
// the public package (pkg/plugin) and the internal factory
// (internal/plugin) can import it without an import cycle.
package config

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config is the parsed, validated configuration for the plugin. It is
// derived from the raw `map[string]any` Temporal hands the plugin via
// `config.CustomDatastoreConfig.Options`. Consumers should not mutate it
// after ParseConfig returns; the plugin treats it as immutable for the
// lifetime of the server.
//
// docs/operator/config-reference.md documents every option.
type Config struct {
	// Connection — exactly one of URI or Hosts+ReplicaSet is required.
	URI        string
	Hosts      []string
	ReplicaSet string

	// Database: the Mongo database name Temporal will use.
	Database string

	// Authentication. Password may be supplied inline, via an env var
	// (PasswordEnv), or via a shell command (PasswordCommand).
	Username   string
	Password   string
	AuthSource string

	// TLS.
	TLSCAFile             string
	TLSCertFile           string
	TLSKeyFile            string
	TLSInsecureSkipVerify bool

	// Driver-level options. Defaults.
	ReadPreference           string
	WriteConcern             string
	ReadConcern              string
	MaxPoolSize              int
	MinPoolSize              int
	MaxConnecting            int
	MaxIdleTimeMs            int
	ConnectTimeoutMs         int
	ServerSelectionTimeoutMs int
	SocketTimeoutMs          int
	HeartbeatIntervalMs      int
	RetryWrites              bool
	RetryReads               bool
	AppName                  string

	// Visibility mode selector.
	VisibilityMode       string
	AtlasSearchIndexName string
}

// Redacted returns a string representation safe for logs. Credentials and
// any URI query parameters carrying secrets are scrubbed.
func (c *Config) Redacted() string {
	b := strings.Builder{}
	b.WriteString("Config{")
	b.WriteString("URI=" + redactURI(c.URI))
	b.WriteString(",Database=" + c.Database)
	b.WriteString(",Username=" + c.Username)
	b.WriteString(",Password=[REDACTED]")
	b.WriteString(",WriteConcern=" + c.WriteConcern)
	b.WriteString(",ReadConcern=" + c.ReadConcern)
	b.WriteString(",ReadPreference=" + c.ReadPreference)
	b.WriteString(fmt.Sprintf(",RetryWrites=%v,RetryReads=%v", c.RetryWrites, c.RetryReads))
	b.WriteString(",VisibilityMode=" + c.VisibilityMode)
	b.WriteString("}")
	return b.String()
}

// redactURI strips the userinfo and any password-bearing query parameters
// from a MongoDB URI. Unparseable URIs become the literal "[UNPARSEABLE]"
// to avoid leaking partial credentials via substring.
func redactURI(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "[UNPARSEABLE]"
	}
	if u.User != nil {
		u.User = url.User(u.User.Username())
	}
	return u.String()
}

// knownOptions lists every key ParseConfig recognizes. Lookups are
// case-sensitive because Temporal's YAML loader preserves case.
var knownOptions = map[string]struct{}{
	"uri":                      {},
	"hosts":                    {},
	"replicaSet":               {},
	"database":                 {},
	"username":                 {},
	"password":                 {},
	"passwordEnv":              {},
	"passwordCommand":          {},
	"passwordCommandTimeout":   {},
	"authSource":               {},
	"appName":                  {},
	"tlsCAFile":                {},
	"tlsCertFile":              {},
	"tlsKeyFile":               {},
	"tlsInsecureSkipVerify":    {},
	"readPreference":           {},
	"writeConcern":             {},
	"readConcern":              {},
	"maxPoolSize":              {},
	"minPoolSize":              {},
	"maxConnecting":            {},
	"maxIdleTimeMs":            {},
	"connectTimeoutMs":         {},
	"serverSelectionTimeoutMs": {},
	"socketTimeoutMs":          {},
	"heartbeatIntervalMs":      {},
	"retryWrites":              {},
	"retryReads":               {},
	"visibilityMode":           {},
	"atlasSearchIndexName":     {},
	"skipSchemaCheck":          {},
	"numHistoryShards":         {},
}

// dbNamePattern enforces the conservative subset of MongoDB's database
// naming rules that work across all supported deployments.
// MongoDB disallows /, \\, ., \" , $, <space>, NUL, and also the name
// "admin"-adjacent reserved words in specific contexts; we reject the
// reliably-illegal characters and accept the rest.
var dbNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// ParseConfig reads the raw option map and returns a validated Config.
// Warnings (unknown options, non-majority write concern) are returned
// alongside the Config so the caller can surface them via the Temporal
// logger. A nil warnings slice is valid.
//
// ParseConfig is intentionally non-mutating of the input map.
func ParseConfig(raw map[string]any) (*Config, []string, error) {
	var warnings []string
	cfg := &Config{
		// Majority write/read concern and primary read preference by default.
		WriteConcern:   "majority",
		ReadConcern:    "majority",
		ReadPreference: "primary",
		RetryWrites:    true,
		RetryReads:     true,
	}

	// Track unknown options for a single aggregated warning.
	var unknown []string
	for k := range raw {
		if _, ok := knownOptions[k]; !ok {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		warnings = append(warnings,
			fmt.Sprintf("plugin: ignoring unknown option(s): %s", strings.Join(unknown, ", ")))
	}

	// Read into the struct.
	if v, ok := raw["uri"].(string); ok {
		cfg.URI = v
	}
	if v, ok := raw["hosts"]; ok {
		cfg.Hosts = toStringSlice(v)
	}
	if v, ok := raw["replicaSet"].(string); ok {
		cfg.ReplicaSet = v
	}
	if v, ok := raw["database"].(string); ok {
		cfg.Database = v
	}
	if v, ok := raw["username"].(string); ok {
		cfg.Username = v
	}
	if v, ok := raw["password"].(string); ok {
		cfg.Password = v
	}
	if v, ok := raw["authSource"].(string); ok {
		cfg.AuthSource = v
	}
	if v, ok := raw["appName"].(string); ok {
		cfg.AppName = v
	}
	if v, ok := raw["writeConcern"].(string); ok {
		cfg.WriteConcern = v
	}
	if v, ok := raw["readConcern"].(string); ok {
		cfg.ReadConcern = v
	}
	if v, ok := raw["readPreference"].(string); ok {
		cfg.ReadPreference = v
	}
	if v, ok := raw["visibilityMode"].(string); ok {
		cfg.VisibilityMode = v
	}
	if v, ok := raw["atlasSearchIndexName"].(string); ok {
		cfg.AtlasSearchIndexName = v
	}
	var errs []error
	boolOpt := func(key string, dst *bool) {
		v, ok, err := asBool(raw[key])
		if err != nil {
			errs = append(errs, fmt.Errorf("plugin: %q: %w", key, err))
		} else if ok {
			*dst = v
		}
	}
	intOpt := func(key string, dst *int) {
		v, err := asInt(raw[key])
		if err != nil {
			errs = append(errs, fmt.Errorf("plugin: %q: %w", key, err))
			return
		}
		if v < 0 {
			errs = append(errs, fmt.Errorf("plugin: %q must not be negative (got %d)", key, v))
			return
		}
		*dst = v
	}
	boolOpt("retryWrites", &cfg.RetryWrites)
	boolOpt("retryReads", &cfg.RetryReads)
	cfg.TLSCAFile, _ = raw["tlsCAFile"].(string)
	cfg.TLSCertFile, _ = raw["tlsCertFile"].(string)
	cfg.TLSKeyFile, _ = raw["tlsKeyFile"].(string)
	boolOpt("tlsInsecureSkipVerify", &cfg.TLSInsecureSkipVerify)
	intOpt("maxPoolSize", &cfg.MaxPoolSize)
	intOpt("minPoolSize", &cfg.MinPoolSize)
	intOpt("maxConnecting", &cfg.MaxConnecting)
	intOpt("maxIdleTimeMs", &cfg.MaxIdleTimeMs)
	intOpt("connectTimeoutMs", &cfg.ConnectTimeoutMs)
	intOpt("serverSelectionTimeoutMs", &cfg.ServerSelectionTimeoutMs)
	intOpt("socketTimeoutMs", &cfg.SocketTimeoutMs)
	intOpt("heartbeatIntervalMs", &cfg.HeartbeatIntervalMs)
	var pwCmdTimeoutMs int
	intOpt("passwordCommandTimeout", &pwCmdTimeoutMs)
	if cfg.MaxPoolSize > 0 && cfg.MinPoolSize > cfg.MaxPoolSize {
		errs = append(errs, fmt.Errorf("plugin: minPoolSize (%d) exceeds maxPoolSize (%d)", cfg.MinPoolSize, cfg.MaxPoolSize))
	}
	if len(errs) > 0 {
		return nil, warnings, errors.Join(errs...)
	}

	// Resolve password from env or command if inline password absent
	//. Inline password wins if supplied.
	if cfg.Password == "" {
		if pwEnv, _ := raw["passwordEnv"].(string); pwEnv != "" {
			v, ok := os.LookupEnv(pwEnv)
			if !ok {
				return nil, warnings, fmt.Errorf("passwordEnv %q is not set in environment", pwEnv)
			}
			cfg.Password = v
		} else if pwCmd, _ := raw["passwordCommand"].(string); pwCmd != "" {
			timeoutMs := pwCmdTimeoutMs
			if timeoutMs <= 0 {
				timeoutMs = 5000 // 5s default — small enough to never block startup
			}
			pw, err := runPasswordCommand(pwCmd, time.Duration(timeoutMs)*time.Millisecond)
			if err != nil {
				return nil, warnings, fmt.Errorf("passwordCommand failed: %w", err)
			}
			cfg.Password = pw
		}
	}

	// Mutual exclusion.
	componentFieldSet := len(cfg.Hosts) > 0 || cfg.ReplicaSet != ""
	if cfg.URI != "" && componentFieldSet {
		return nil, warnings, errors.New("plugin: both 'uri' and component fields (hosts/replicaSet) set; supply one or the other")
	}

	// Credentials in both the URI userinfo and the username option are
	// ambiguous; refuse rather than guess which one wins.
	if cfg.URI != "" && cfg.Username != "" {
		if u, err := url.Parse(cfg.URI); err == nil && u.User != nil {
			return nil, warnings, errors.New("plugin: 'username' is set but 'uri' already carries credentials; supply one or the other")
		}
	}

	// Required database name.
	if cfg.Database == "" {
		return nil, warnings, errors.New("plugin: 'database' is required")
	}
	if !dbNamePattern.MatchString(cfg.Database) {
		return nil, warnings, fmt.Errorf("plugin: 'database' %q contains invalid characters (allowed: A-Z a-z 0-9 _ -)", cfg.Database)
	}

	// Synthesize a URI from components if URI not provided
	//.
	if cfg.URI == "" {
		if len(cfg.Hosts) == 0 {
			return nil, warnings, errors.New("plugin: either 'uri' or 'hosts' must be supplied")
		}
		cfg.URI = buildURIFromComponents(cfg)
	}

	// Non-majority write concern warning.
	if cfg.WriteConcern != "majority" {
		warnings = append(warnings,
			fmt.Sprintf("plugin: writeConcern=%q — Temporal correctness requires 'majority'", cfg.WriteConcern))
	}

	return cfg, warnings, nil
}

// runPasswordCommand runs the shell command and returns its trimmed
// stdout, aborting after `budget`. The call is intentionally shell-based
// (`/bin/sh -c`) to match the Temporal SQL `passwordCommand` precedent.
//
// We use cmd.WaitDelay so that Go's exec package reaps the process
// aggressively when the context deadline fires; without it, a command
// that holds stdout open (a common case with `sleep`) can keep Wait()
// blocked well past the user-visible deadline.
func runPasswordCommand(cmdStr string, budget time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", cmdStr)
	// 100ms grace after cancel before SIGKILL; guarantees Wait() unblocks
	// promptly whatever the child is holding open.
	cmd.WaitDelay = 100 * time.Millisecond
	out, err := cmd.Output()
	if ctx.Err() == context.DeadlineExceeded {
		return "", fmt.Errorf("timed out after %s", budget)
	}
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(out), "\n\r"), nil
}

// buildURIFromComponents assembles a mongodb:// URI from the individual
// option fields and percent-escapes all credential characters
// . MongoDB URIs follow RFC 3986 userinfo encoding, which
// percent-encodes space as %20 (not '+'), so we post-process
// QueryEscape's output to strip the form-encoded '+'.
func buildURIFromComponents(c *Config) string {
	var sb strings.Builder
	sb.WriteString("mongodb://")
	if c.Username != "" || c.Password != "" {
		sb.WriteString(uriEscape(c.Username))
		sb.WriteString(":")
		sb.WriteString(uriEscape(c.Password))
		sb.WriteString("@")
	}
	sb.WriteString(strings.Join(c.Hosts, ","))
	sb.WriteString("/")

	params := make([]string, 0, 4)
	if c.ReplicaSet != "" {
		params = append(params, "replicaSet="+uriEscape(c.ReplicaSet))
	}
	if c.AuthSource != "" {
		params = append(params, "authSource="+uriEscape(c.AuthSource))
	}
	if c.AppName != "" {
		params = append(params, "appName="+uriEscape(c.AppName))
	}
	if len(params) > 0 {
		sb.WriteString("?")
		sb.WriteString(strings.Join(params, "&"))
	}
	return sb.String()
}

// uriEscape percent-encodes a string for use inside a MongoDB URI
// userinfo or query-parameter slot. It differs from url.QueryEscape in
// one detail: space is encoded as %20, not '+'. MongoDB's connection
// parser accepts both, but %20 is the canonical form and matches
// RFC 3986 §3.2.1 userinfo encoding.
func uriEscape(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

// ------ small converters. Kept local so callers don't have to. ------

// asInt accepts integer kinds, whole floats (YAML/JSON numbers) and
// decimal strings (env-templated YAML). Absent keys yield 0.
func asInt(v any) (int, error) {
	switch t := v.(type) {
	case nil:
		return 0, nil
	case int:
		return t, nil
	case int32:
		return int(t), nil
	case int64:
		return int(t), nil
	case float64:
		if t != float64(int(t)) {
			return 0, fmt.Errorf("expected an integer, got %v", t)
		}
		return int(t), nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(t))
		if err != nil {
			return 0, fmt.Errorf("expected an integer, got %q", t)
		}
		return n, nil
	}
	return 0, fmt.Errorf("expected an integer, got %T", v)
}

// asBool accepts booleans and "true"/"false" strings. The second result
// reports whether the key was present.
func asBool(v any) (bool, bool, error) {
	switch t := v.(type) {
	case nil:
		return false, false, nil
	case bool:
		return t, true, nil
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(t))
		if err != nil {
			return false, false, fmt.Errorf("expected a boolean, got %q", t)
		}
		return b, true, nil
	}
	return false, false, fmt.Errorf("expected a boolean, got %T", v)
}

func toStringSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
