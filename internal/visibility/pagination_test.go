package visibility

import (
	"encoding/json"
	"errors"
	"testing"

	"go.temporal.io/api/serviceerror"
)

// TestPageToken_FixedPointRoundTrip: encode → decode →
// re-encode is a fixed point, so resumption is deterministic.
func TestPageToken_FixedPointRoundTrip(t *testing.T) {
	sk, _ := json.Marshal(int64(42))
	original := &pageToken{
		ID:         "ns:run-1",
		SortKey:    sk,
		SortField:  "start_time",
		Descending: true,
	}
	encoded := encodeToken(original)
	if len(encoded) == 0 {
		t.Fatal("encode produced empty token")
	}
	decoded, err := decodeToken(encoded)
	if err != nil {
		t.Fatalf("decodeToken: %v", err)
	}
	if decoded.ID != original.ID {
		t.Errorf("ID drift: %q vs %q", decoded.ID, original.ID)
	}
	if decoded.SortField != original.SortField {
		t.Errorf("SortField drift: %q vs %q", decoded.SortField, original.SortField)
	}
	if decoded.Descending != original.Descending {
		t.Errorf("Descending drift: %v vs %v", decoded.Descending, original.Descending)
	}
	if string(decoded.SortKey) != string(original.SortKey) {
		t.Errorf("SortKey drift: %s vs %s", decoded.SortKey, original.SortKey)
	}

	// Re-encode and compare with the first encoding — must be identical.
	re := encodeToken(decoded)
	if string(re) != string(encoded) {
		t.Errorf("token not a fixed point; first=%q second=%q", encoded, re)
	}
}

func TestPageToken_InvalidBase64(t *testing.T) {
	_, err := decodeToken([]byte("not base64!@#"))
	if err == nil {
		t.Fatal("expected InvalidArgument for malformed base64")
	}
	var iae *serviceerror.InvalidArgument
	if !errors.As(err, &iae) {
		t.Errorf("expected *InvalidArgument; got %T: %v", err, err)
	}
}

func TestPageToken_InvalidJSON(t *testing.T) {
	_, err := decodeToken([]byte("aGVsbG8=")) // "hello", valid base64 but not JSON
	if err == nil {
		t.Fatal("expected InvalidArgument for malformed JSON")
	}
	var iae *serviceerror.InvalidArgument
	if !errors.As(err, &iae) {
		t.Errorf("expected *InvalidArgument; got %T: %v", err, err)
	}
}

// TestPageToken_EmptyTokenDecodes covers the first-page case: an empty
// NextPageToken decodes to a zero pageToken.
func TestPageToken_EmptyTokenDecodes(t *testing.T) {
	tok, err := decodeToken(nil)
	if err != nil {
		t.Fatal(err)
	}
	if tok.ID != "" {
		t.Errorf("empty token should decode to empty ID; got %q", tok.ID)
	}
}

// TestPageToken_EmptyIDEncodesNil covers the "end of results" case:
// nil tokens encode to empty bytes.
func TestPageToken_EmptyIDEncodesNil(t *testing.T) {
	if b := encodeToken(nil); b != nil {
		t.Errorf("nil token should encode nil; got %q", b)
	}
	if b := encodeToken(&pageToken{}); b != nil {
		t.Errorf("empty-ID token should encode nil; got %q", b)
	}
}
