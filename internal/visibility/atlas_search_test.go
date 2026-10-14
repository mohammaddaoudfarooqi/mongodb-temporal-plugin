package visibility

import (
	"errors"
	"strings"
	"testing"
)

// TestClassifyAtlasProbeError_UnrecognizedStage's
// happy error message: plain-Mongo's "Unrecognized pipeline stage"
// must surface guidance steering the operator to either an Atlas
// cluster or visibilityMode=aggregation.
func TestClassifyAtlasProbeError_UnrecognizedStage(t *testing.T) {
	raw := errors.New("(Unknown) Unrecognized pipeline stage name: '$listSearchIndexes'")
	got := classifyAtlasProbeError(raw)
	if got == nil {
		t.Fatal("expected an error; got nil")
	}
	msg := got.Error()
	if !strings.Contains(msg, "Atlas Search unavailable") {
		t.Errorf("message should explain Atlas Search unavailability: %q", msg)
	}
	if !strings.Contains(msg, "visibilityMode=aggregation") {
		t.Errorf("message should point at aggregation fallback: %q", msg)
	}
}

// TestClassifyAtlasProbeError_AtlasTier covers the tier-too-low case.
func TestClassifyAtlasProbeError_AtlasTier(t *testing.T) {
	raw := errors.New("the current Atlas cluster tier M0 does not support Search")
	got := classifyAtlasProbeError(raw)
	msg := got.Error()
	if !strings.Contains(msg, "tier does not support Search") {
		t.Errorf("message should call out tier: %q", msg)
	}
	if !strings.Contains(msg, "M0/M2/M5/Flex/Serverless are excluded") {
		t.Errorf("message should list the excluded tiers: %q", msg)
	}
}

// TestClassifyAtlasProbeError_OtherError wraps unknown failures.
func TestClassifyAtlasProbeError_OtherError(t *testing.T) {
	raw := errors.New("network timeout")
	got := classifyAtlasProbeError(raw)
	if !strings.Contains(got.Error(), "Atlas Search probe failed") {
		t.Errorf("unknown errors should wrap with probe context: %q", got)
	}
	// And errors.Is / Unwrap should still surface the original.
	if !errors.Is(got, raw) {
		t.Errorf("wrapped error should expose underlying via errors.Is")
	}
}
