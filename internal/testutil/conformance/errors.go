package conformance

import (
	"errors"
	"testing"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metadatastore"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
)

// runErrorMatrix exercises the error-classification contract end-
// to-end: the same conditions reliably surface the same serviceerror
// type whichever backend the fixture fronts. Each scenario names the
// expected type so a misclassification surfaces as a fail-not-match
// rather than a silent pass.
func runErrorMatrix(t *testing.T, f Fixture) {
	t.Run("ShardStore_StaleCAS_ShardOwnershipLost", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", shardstore.EnsureIndexes(tc.Ctx, tc.DB))
		s := shardstore.New(tc.DB, "cluster0")

		// UpdateShard on a non-existent shard with a wrong prev range:
		// the filter never matches → ShardOwnershipLostError.
		err := s.UpdateShard(tc.Ctx, &persistence.InternalUpdateShardRequest{
			ShardID: 9001, RangeID: 1, PreviousRangeID: 0,
			ShardInfo: dataBlob("ignored"),
		})
		if err == nil {
			t.Fatal("expected error; got nil")
		}
		var sol *persistence.ShardOwnershipLostError
		if !errors.As(err, &sol) {
			t.Errorf("want *ShardOwnershipLostError; got %T: %v", err, err)
		}
	})

	t.Run("MetadataStore_Missing_NamespaceNotFoundOrNotFound", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", metadatastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := metadatastore.New(tc.DB)
		_, err := s.GetNamespace(tc.Ctx, &persistence.GetNamespaceRequest{ID: uniqStr("nope")})
		if err == nil {
			t.Fatal("expected error; got nil")
		}
		// Either the specific NamespaceNotFound or the generic NotFound
		// satisfies the "not found" classification contract.
		var nsnf *serviceerror.NamespaceNotFound
		var nf *serviceerror.NotFound
		if !errors.As(err, &nsnf) && !errors.As(err, &nf) {
			t.Errorf("want *NamespaceNotFound or *NotFound; got %T: %v", err, err)
		}
	})

	t.Run("MetadataStore_DuplicateName_AlreadyExistsOrCondition", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", metadatastore.EnsureIndexes(tc.Ctx, tc.DB))
		s := metadatastore.New(tc.DB)
		name := uniqStr("dup")
		_, err := s.CreateNamespace(tc.Ctx, &persistence.InternalCreateNamespaceRequest{
			ID: uniqStr("id"), Name: name, Namespace: dataBlob("v1"),
		})
		fatalOnErr(t, "seed", err)
		_, err = s.CreateNamespace(tc.Ctx, &persistence.InternalCreateNamespaceRequest{
			ID: uniqStr("id"), Name: name, Namespace: dataBlob("v2"),
		})
		if err == nil {
			t.Fatal("expected duplicate to error; got nil")
		}
		// Upstream has two equivalent signals for "this namespace
		// already exists": the specific NamespaceAlreadyExists
		// serviceerror and the persistence-layer ConditionFailedError.
		// Both satisfy the caller's "stop, don't overwrite" contract.
		var na *serviceerror.NamespaceAlreadyExists
		var cfe *persistence.ConditionFailedError
		if !errors.As(err, &na) && !errors.As(err, &cfe) {
			t.Errorf("want *NamespaceAlreadyExists or *ConditionFailedError; got %T: %v", err, err)
		}
	})
}
