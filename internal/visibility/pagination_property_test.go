package visibility

import (
	"encoding/json"
	"math/rand"
	"testing"
	"unicode/utf8"
)

// Property-based tests for the opaque page-token codec.
// The table-driven tests in pagination_test.go cover specific
// fixtures; these tests assert two universal invariants:
//
//   P1: For any token t with a non-empty ID, decode(encode(t)) == t.
//       Token round-trips must be a fixed point; anything else loses
//       caller state across pages.
//   P2: Re-encoding a decoded token is byte-identical to the first
//       encoding. "Re-encode stability" matters because upstream
//       code may log or compare tokens.
//
// Test runs generate 200 random token shapes per invariant so
// corner cases (empty SortKey, binary-unsafe SearchToken bytes,
// Unicode runes in IDs) surface as failures rather than slip
// through bespoke fixtures.

// genToken builds a plausible pageToken. ID is always non-empty so
// encode doesn't shortcut to nil — that case has its own unit test.
func genToken(rng *rand.Rand) *pageToken {
	t := &pageToken{
		ID: randString(rng, 1, 64),
	}
	if rng.Intn(2) == 0 {
		// SortKey is JSON-encoded; supply either a JSON number or a
		// quoted string, matching the real emitter.
		if rng.Intn(2) == 0 {
			t.SortKey = json.RawMessage(jsonNumber(rng))
		} else {
			raw, _ := json.Marshal(randString(rng, 0, 32))
			t.SortKey = json.RawMessage(raw)
		}
		t.SortField = randString(rng, 1, 16)
		t.Descending = rng.Intn(2) == 0
	}
	if rng.Intn(2) == 0 {
		// SearchToken is ferried verbatim — may contain characters
		// that would trip a naive encoder.
		t.SearchToken = randString(rng, 0, 128)
	}
	return t
}

func randString(rng *rand.Rand, minLen, maxLen int) string {
	n := minLen
	if maxLen > minLen {
		n += rng.Intn(maxLen - minLen)
	}
	out := make([]rune, 0, n)
	for len(out) < n {
		// Bias toward ASCII but mix in the full BMP so we catch
		// non-ASCII round-trip issues. Skip surrogates.
		r := rune(rng.Intn(0xFFFF))
		if r >= 0xD800 && r <= 0xDFFF {
			continue
		}
		if !utf8.ValidRune(r) {
			continue
		}
		out = append(out, r)
	}
	return string(out)
}

func jsonNumber(rng *rand.Rand) string {
	switch rng.Intn(3) {
	case 0:
		return "0"
	case 1:
		return "-12345"
	default:
		return "1700000000000000000" // unix-nanos shape
	}
}

// TestPageToken_Property_RoundTrip is P1.
func TestPageToken_Property_RoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 200; i++ {
		orig := genToken(rng)
		encoded := encodeToken(orig)
		if len(encoded) == 0 {
			t.Fatalf("iter %d: encodeToken returned empty bytes for ID=%q", i, orig.ID)
		}
		decoded, err := decodeToken(encoded)
		if err != nil {
			t.Fatalf("iter %d: decodeToken: %v (orig=%+v)", i, err, orig)
		}
		if decoded.ID != orig.ID ||
			decoded.SortField != orig.SortField ||
			decoded.Descending != orig.Descending ||
			decoded.SearchToken != orig.SearchToken {
			t.Errorf("iter %d: scalar drift\n orig:    %+v\n decoded: %+v", i, orig, decoded)
		}
		if string(decoded.SortKey) != string(orig.SortKey) {
			t.Errorf("iter %d: SortKey drift\n orig:    %q\n decoded: %q", i, orig.SortKey, decoded.SortKey)
		}
	}
}

// TestPageToken_Property_ReEncodeStable is P2.
func TestPageToken_Property_ReEncodeStable(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 200; i++ {
		orig := genToken(rng)
		first := encodeToken(orig)
		decoded, err := decodeToken(first)
		if err != nil {
			t.Fatalf("iter %d: decode: %v", i, err)
		}
		second := encodeToken(decoded)
		if string(first) != string(second) {
			t.Errorf("iter %d: re-encode drift\n first:  %q\n second: %q", i, first, second)
		}
	}
}
