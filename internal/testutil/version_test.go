package testutil

import (
	"testing"
)

// TestMongoImage_Default_MongoSix: no env → mongo:6.
func TestMongoImage_Default_MongoSix(t *testing.T) {
	t.Setenv("MONGO_VERSION", "")
	if got := MongoImage(); got != "mongo:6" {
		t.Fatalf("expected mongo:6 default; got %q", got)
	}
}

// TestMongoImage_BareVersion_Prefixed: "7.0" → "mongo:7.0".
// The CI matrix passes bare versions so the workflow YAML stays readable.
func TestMongoImage_BareVersion_Prefixed(t *testing.T) {
	t.Setenv("MONGO_VERSION", "7.0")
	if got := MongoImage(); got != "mongo:7.0" {
		t.Fatalf("expected mongo:7.0; got %q", got)
	}
}

// TestMongoImage_QualifiedTag_Passthrough: a fully qualified tag (e.g.
// a custom registry or pinned digest alias prefixed with `mongo:`) is
// returned unchanged. Lets ops override for an air-gapped registry.
func TestMongoImage_QualifiedTag_Passthrough(t *testing.T) {
	t.Setenv("MONGO_VERSION", "mongo:8.0.4")
	if got := MongoImage(); got != "mongo:8.0.4" {
		t.Fatalf("expected passthrough; got %q", got)
	}
}
