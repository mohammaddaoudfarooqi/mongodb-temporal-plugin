// Tests for the BSON blob codec. These stay pure Go — no MongoDB driver
// required — because BSON marshal/unmarshal is the round-trip target.
package mongo_test

import (
	"crypto/rand"
	"encoding/hex"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

// TestBlob_RoundTrip_Proto3: a DataBlob with proto3
// encoding round-trips through BSON marshal/unmarshal with byte-identical
// payload and preserved encoding tag.
func TestBlob_RoundTrip_Proto3(t *testing.T) {
	payload := []byte{0x0a, 0x04, 't', 'e', 's', 't'} // arbitrary valid proto3 prefix
	in := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         payload,
	}

	b := internalmongo.BlobFromDataBlob(in)
	if b == nil {
		t.Fatal("BlobFromDataBlob returned nil for a non-nil blob")
	}

	// Round-trip through BSON to catch any encoding issues the in-memory
	// path would miss.
	raw, err := bson.Marshal(b)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	var decoded internalmongo.Blob
	if err := bson.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("bson.Unmarshal: %v", err)
	}

	out := internalmongo.DataBlobFromBlob(&decoded)
	if out.EncodingType != enumspb.ENCODING_TYPE_PROTO3 {
		t.Errorf("encoding lost: got %v", out.EncodingType)
	}
	if !bytesEqual(out.Data, payload) {
		t.Errorf("payload bytes differ: in=%s out=%s", hex.EncodeToString(payload), hex.EncodeToString(out.Data))
	}
}

// TestBlob_RoundTrip_JSON for the second encoding tag.
func TestBlob_RoundTrip_JSON(t *testing.T) {
	payload := []byte(`{"hello":"world"}`)
	in := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_JSON,
		Data:         payload,
	}
	b := internalmongo.BlobFromDataBlob(in)
	raw, _ := bson.Marshal(b)
	var decoded internalmongo.Blob
	_ = bson.Unmarshal(raw, &decoded)
	out := internalmongo.DataBlobFromBlob(&decoded)
	if out.EncodingType != enumspb.ENCODING_TYPE_JSON {
		t.Errorf("encoding lost: got %v", out.EncodingType)
	}
	if !bytesEqual(out.Data, payload) {
		t.Error("JSON payload round-trip mismatched")
	}
}

// TestBlob_NilInput covers the robustness path: nil DataBlob → nil Blob,
// and nil Blob → nil DataBlob.
func TestBlob_NilInput(t *testing.T) {
	if got := internalmongo.BlobFromDataBlob(nil); got != nil {
		t.Errorf("expected nil Blob for nil input; got %+v", got)
	}
	if got := internalmongo.DataBlobFromBlob(nil); got != nil {
		t.Errorf("expected nil DataBlob for nil input; got %+v", got)
	}
}

// TestBlob_EmptyPayload: a zero-byte DataBlob is a valid input — some
// Temporal records carry empty payloads — and must round-trip.
func TestBlob_EmptyPayload(t *testing.T) {
	in := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte{},
	}
	b := internalmongo.BlobFromDataBlob(in)
	raw, _ := bson.Marshal(b)
	var decoded internalmongo.Blob
	_ = bson.Unmarshal(raw, &decoded)
	out := internalmongo.DataBlobFromBlob(&decoded)
	if len(out.Data) != 0 {
		t.Errorf("empty payload grew: %d bytes", len(out.Data))
	}
}

// TestBlob_RandomFuzz is a property-based check: generate 1000 random blobs, round-trip each, and require byte
// equality. under adversarial inputs (random byte
// strings that exercise every BSON escape corner).
func TestBlob_RandomFuzz(t *testing.T) {
	for i := 0; i < 1000; i++ {
		// Sizes 0..4096 bytes. The upstream contract does not bound blob
		// size (the 12 MB execution-doc cap is enforced elsewhere), but
		// fuzzing small-to-medium sizes adequately stresses the codec.
		sz := i % 4096
		payload := make([]byte, sz)
		if _, err := rand.Read(payload); err != nil {
			t.Fatalf("rand: %v", err)
		}
		enc := enumspb.ENCODING_TYPE_PROTO3
		if i%2 == 1 {
			enc = enumspb.ENCODING_TYPE_JSON
		}
		in := &commonpb.DataBlob{EncodingType: enc, Data: payload}
		raw, err := bson.Marshal(internalmongo.BlobFromDataBlob(in))
		if err != nil {
			t.Fatalf("marshal iter %d: %v", i, err)
		}
		var decoded internalmongo.Blob
		if err := bson.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal iter %d: %v", i, err)
		}
		out := internalmongo.DataBlobFromBlob(&decoded)
		if out.EncodingType != enc {
			t.Fatalf("iter %d: encoding drifted %v→%v", i, enc, out.EncodingType)
		}
		if !bytesEqual(out.Data, payload) {
			t.Fatalf("iter %d: bytes drifted (len=%d)", i, len(payload))
		}
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
