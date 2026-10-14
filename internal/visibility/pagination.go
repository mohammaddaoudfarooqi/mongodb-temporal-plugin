package visibility

import (
	"encoding/base64"
	"encoding/json"

	"go.temporal.io/api/serviceerror"
)

// pageToken is the resume cursor visibility List returns. Opaque
// Base64-JSON; callers treat it as a bytestring. The
// fields inside capture enough state to resume a sorted scan even
// when the sort key is non-unique (the _id tiebreaker rules out
// ambiguous resumes).
type pageToken struct {
	// ID is the _id of the last returned document. Required.
	ID string `json:"id"`
	// SortKey is the primary sort value; present when ORDER BY is
	// something other than _id. JSON-encoded so both strings and
	// numbers round-trip without losing type.
	SortKey json.RawMessage `json:"sk,omitempty"`
	// SortField names the primary sort field.
	SortField string `json:"sf,omitempty"`
	// Descending reflects the ORDER BY direction.
	Descending bool `json:"desc,omitempty"`
	// SearchToken is Atlas Search's opaque searchSequenceToken for
	// the last returned document, used by Atlas-Search-mode pagination
	// (backlog B-4). Empty in Aggregation mode. Atlas requires this
	// value be a string, so we ferry it verbatim rather than decoding.
	SearchToken string `json:"st,omitempty"`
	// Cursor holds the BSON-encoded sort values of the last row returned
	// by an Aggregation-mode page (keyset pagination).
	Cursor []byte `json:"c,omitempty"`
}

// encodeToken base64-encodes the JSON form. Empty ID → empty token.
func encodeToken(t *pageToken) []byte {
	if t == nil || t.ID == "" {
		return nil
	}
	raw, err := json.Marshal(t)
	if err != nil {
		return nil
	}
	return []byte(base64.StdEncoding.EncodeToString(raw))
}

// decodeToken parses the token or returns *serviceerror.InvalidArgument
// on malformed input.
func decodeToken(raw []byte) (*pageToken, error) {
	if len(raw) == 0 {
		return &pageToken{}, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(string(raw))
	if err != nil {
		return nil, serviceerror.NewInvalidArgument("visibility: malformed page token")
	}
	var t pageToken
	if err := json.Unmarshal(decoded, &t); err != nil {
		return nil, serviceerror.NewInvalidArgument("visibility: malformed page token JSON")
	}
	return &t, nil
}
