package executionstore_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/executionstore"
)

func appendReq(eventBytes int) *persistence.InternalAppendHistoryNodesRequest {
	return &persistence.InternalAppendHistoryNodesRequest{
		BranchInfo: &persistencespb.HistoryBranch{TreeId: "t", BranchId: "b"},
		Node: persistence.InternalHistoryNode{
			NodeID: 1, TransactionID: 1,
			Events: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: bytes.Repeat([]byte("e"), eventBytes)},
		},
	}
}

// TestAppendHistoryNodes_OverBudget_TransactionSizeLimit: a history batch
// that could not fit in one MongoDB document is refused before any write,
// with the error Temporal answers by terminating the workflow, rather
// than a server error it would retry for ever.
func TestAppendHistoryNodes_OverBudget_TransactionSizeLimit(t *testing.T) {
	s := executionstore.New(nil, "cluster0")
	err := s.AppendHistoryNodes(context.Background(), appendReq(13<<20))
	var tooBig *persistence.TransactionSizeLimitError
	if !errors.As(err, &tooBig) {
		t.Fatalf("err = %T %v; want TransactionSizeLimitError", err, err)
	}
}

func TestAppendHistoryNodes_WithinBudget_NotRefusedForSize(t *testing.T) {
	s := executionstore.New(nil, "cluster0")
	err := s.AppendHistoryNodes(context.Background(), appendReq(4<<20))
	var tooBig *persistence.TransactionSizeLimitError
	if errors.As(err, &tooBig) {
		t.Fatalf("a 4 MB batch was refused for size: %v", err)
	}
}
