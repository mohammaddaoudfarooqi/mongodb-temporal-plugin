// Package testutil provides shared helpers for integration tests.
//
// This file lives outside build tags so that any test file (tagged
// `integration` or `conformance`) can call it without duplicating the
// env-var parsing logic.
package testutil

import "os"

// MongoImage returns the docker image to run integration tests against.
// Defaults to `mongo:6` for local runs; CI sets MONGO_VERSION to drive
// the 6.0 / 7.0 / 8.0 matrix.
//
// Accepts bare versions (e.g. "7.0") or fully-qualified tags
// (e.g. "mongo:7.0"). This lets ops override the registry if needed.
func MongoImage() string {
	v := os.Getenv("MONGO_VERSION")
	if v == "" {
		return "mongo:6"
	}
	if len(v) > len("mongo:") && v[:len("mongo:")] == "mongo:" {
		return v
	}
	return "mongo:" + v
}
