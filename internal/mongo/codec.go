// Package mongo wraps the MongoDB Go driver v2 for the plugin. It owns
// the shared client, a transaction wrapper that defers to
// the driver's WithTransaction retry, an error
// translator, and the BSON blob codec documented here
// .
//
// The blob codec is the plugin's contract with the manager layer:
// Temporal hands the plugin opaque DataBlob values (protobuf or JSON
// payloads) and the plugin stores them verbatim. The plugin never
// inspects blob contents and must return byte-identical
// bytes on read.
package mongo

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

// Blob is the BSON-serialisable shape of a Temporal DataBlob. The schema
// is intentionally minimal: a byte array plus a short string tag, both
// BSON-native types, so older drivers can decode documents without any
// custom registry.
//
// bson field names use snake_case per docs/developer/conventions.md.
type Blob struct {
	Data     []byte `bson:"data"`
	Encoding string `bson:"encoding"`
}

// Encoding names. The values are ASCII strings chosen to match the
// project's conventions and to survive transparently through mongosh
// inspection. Unrecognised tags decode as ENCODING_TYPE_UNSPECIFIED to
// keep the store forward-compatible with future Temporal encoding
// additions.
const (
	encodingProto3      = "proto3"
	encodingJSON        = "json"
	encodingUnspecified = ""
)

// BlobFromDataBlob converts a Temporal DataBlob into the BSON shape.
// Returns nil for nil input so callers can do unconditional assignments
// without a guard.
func BlobFromDataBlob(in *commonpb.DataBlob) *Blob {
	if in == nil {
		return nil
	}
	return &Blob{
		Data:     append([]byte(nil), in.Data...), // defensive copy; callers own the slice
		Encoding: encodingOf(in.EncodingType),
	}
}

// DataBlobFromBlob converts the BSON shape back into a Temporal DataBlob.
// Returns nil for nil input.
func DataBlobFromBlob(in *Blob) *commonpb.DataBlob {
	if in == nil {
		return nil
	}
	return &commonpb.DataBlob{
		EncodingType: decodingOf(in.Encoding),
		Data:         append([]byte(nil), in.Data...),
	}
}

func encodingOf(e enumspb.EncodingType) string {
	switch e {
	case enumspb.ENCODING_TYPE_PROTO3:
		return encodingProto3
	case enumspb.ENCODING_TYPE_JSON:
		return encodingJSON
	default:
		return encodingUnspecified
	}
}

func decodingOf(s string) enumspb.EncodingType {
	switch s {
	case encodingProto3:
		return enumspb.ENCODING_TYPE_PROTO3
	case encodingJSON:
		return enumspb.ENCODING_TYPE_JSON
	default:
		return enumspb.ENCODING_TYPE_UNSPECIFIED
	}
}
