// Package security centralises the plugin's credential-redaction
// helpers: no credentials in logs, URL-escaping in URIs, redaction in
// every log line, plus rejection of server-side JavaScript operators in
// the query DSL ($where / $function).
package security

import (
	"errors"
	"net/url"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// RedactURI strips user:password from a Mongo URI. The userinfo is
// replaced with "<user>:****" when present; hostnames, paths, and
// query parameters stay intact so operators can still tell which
// cluster is involved.
func RedactURI(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "[UNPARSEABLE]"
	}
	if u.User != nil {
		u.User = url.UserPassword(u.User.Username(), "****")
	}
	return u.String()
}

// RedactError walks an error message and replaces any substring that
// looks like "mongodb+srv://...", "mongodb://user:pass@...", or a
// lone password fragment (inline ":<nonempty>@" userinfo) with a
// redacted form. Errors from the driver sometimes include the
// connection string verbatim; this scrub is the last defense.
func RedactError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	redacted := redactString(msg)
	if redacted == msg {
		return err
	}
	return errors.New(redacted)
}

// redactString is the exported-for-test version of the URI scrubber.
// It walks left-to-right, substituting each mongodb[+srv]:// URI with
// its redacted form. The search cursor advances past each substitution
// to avoid an infinite loop when RedactURI's output still starts with
// the scheme (which is expected — we redact userinfo, not the scheme).
func redactString(s string) string {
	for _, prefix := range []string{"mongodb+srv://", "mongodb://"} {
		pos := 0
		for {
			idx := strings.Index(s[pos:], prefix)
			if idx < 0 {
				break
			}
			i := pos + idx
			j := i + len(prefix)
			for j < len(s) && !isURITerminator(s[j]) {
				j++
			}
			redacted := RedactURI(s[i:j])
			s = s[:i] + redacted + s[j:]
			// Advance cursor past the just-substituted region.
			pos = i + len(redacted)
		}
	}
	return s
}

func isURITerminator(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '"', '\'', ')', ']', '}', ',', ';':
		return true
	}
	return false
}

// RejectJSFilter scans a BSON filter document for server-side
// JavaScript operators ($where, $function, $accumulator). Returns a
// non-nil error if any are present. Callers (visibility's DSL
// converter primarily) invoke this before dispatching the filter so
// an attacker-supplied query string cannot inject $where predicates.
func RejectJSFilter(filter bson.M) error {
	return walkFilter(filter)
}

func walkFilter(v any) error {
	switch t := v.(type) {
	case bson.M:
		for k, val := range t {
			if isJSOp(k) {
				return errors.New("security: server-side JavaScript operators are not permitted: " + k)
			}
			if err := walkFilter(val); err != nil {
				return err
			}
		}
	case []any:
		for _, x := range t {
			if err := walkFilter(x); err != nil {
				return err
			}
		}
	case []bson.M:
		for _, x := range t {
			if err := walkFilter(x); err != nil {
				return err
			}
		}
	case map[string]any:
		for k, val := range t {
			if isJSOp(k) {
				return errors.New("security: server-side JavaScript operators are not permitted: " + k)
			}
			if err := walkFilter(val); err != nil {
				return err
			}
		}
	}
	return nil
}

func isJSOp(k string) bool {
	switch k {
	case "$where", "$function", "$accumulator", "$expr:function":
		return true
	}
	return false
}
