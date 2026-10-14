package conformance

import (
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/shardstore"
)

// runShardStore exercises the shard store + the single-doc
// CAS invariant. Scenarios:
//   - GetOrCreate idempotent.
//   - UpdateShard happy-path CAS (PrevRangeID matches).
//   - UpdateShard stale CAS → ShardOwnershipLostError.
//   - AssertShardOwnership matching and mismatching range.
func runShardStore(t *testing.T, f Fixture) {
	t.Run("GetOrCreate_Idempotent", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", shardstore.EnsureIndexes(tc.Ctx, tc.DB))
		s := shardstore.New(tc.DB, "cluster0")

		first, err := s.GetOrCreateShard(tc.Ctx, &persistence.InternalGetOrCreateShardRequest{
			ShardID: 1,
			CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
				return 1, dataBlob("initial"), nil
			},
		})
		fatalOnErr(t, "first GetOrCreate", err)

		second, err := s.GetOrCreateShard(tc.Ctx, &persistence.InternalGetOrCreateShardRequest{
			ShardID: 1,
			CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
				return 999, dataBlob("ignored"), nil
			},
		})
		fatalOnErr(t, "idempotent GetOrCreate", err)

		// Idempotency: the second call must return the originally
		// seeded blob, not the CreateShardInfo closure we supplied
		// for the would-be-new-shard path.
		if first.ShardInfo == nil || second.ShardInfo == nil {
			t.Fatalf("expected non-nil ShardInfo on both calls; got first=%v second=%v", first.ShardInfo, second.ShardInfo)
		}
		if string(first.ShardInfo.Data) != string(second.ShardInfo.Data) {
			t.Errorf("idempotent GetOrCreate drift: first=%q second=%q",
				first.ShardInfo.Data, second.ShardInfo.Data)
		}
	})

	t.Run("UpdateShard_MatchingRange_Succeeds", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", shardstore.EnsureIndexes(tc.Ctx, tc.DB))
		s := shardstore.New(tc.DB, "cluster0")

		_, err := s.GetOrCreateShard(tc.Ctx, &persistence.InternalGetOrCreateShardRequest{
			ShardID: 2,
			CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
				return 1, dataBlob("init"), nil
			},
		})
		fatalOnErr(t, "seed shard", err)

		err = s.UpdateShard(tc.Ctx, &persistence.InternalUpdateShardRequest{
			ShardID:         2,
			RangeID:         2,
			PreviousRangeID: 1,
			ShardInfo:       dataBlob("updated"),
		})
		fatalOnErr(t, "CAS update", err)
	})

	t.Run("UpdateShard_StaleRange_ReturnsShardOwnershipLost", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", shardstore.EnsureIndexes(tc.Ctx, tc.DB))
		s := shardstore.New(tc.DB, "cluster0")

		_, err := s.GetOrCreateShard(tc.Ctx, &persistence.InternalGetOrCreateShardRequest{
			ShardID: 3,
			CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
				return 1, dataBlob("init"), nil
			},
		})
		fatalOnErr(t, "seed shard", err)

		err = s.UpdateShard(tc.Ctx, &persistence.InternalUpdateShardRequest{
			ShardID:         3,
			RangeID:         99,
			PreviousRangeID: 42, // wrong
			ShardInfo:       dataBlob("ignored"),
		})
		if err == nil {
			t.Fatal("expected ShardOwnershipLostError; got nil")
		}
		var sol *persistence.ShardOwnershipLostError
		if !errors.As(err, &sol) {
			t.Errorf("expected *ShardOwnershipLostError; got %T: %v", err, err)
		}
	})

	t.Run("AssertShardOwnership_Matching_NoError", func(t *testing.T) {
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", shardstore.EnsureIndexes(tc.Ctx, tc.DB))
		s := shardstore.New(tc.DB, "cluster0")

		_, err := s.GetOrCreateShard(tc.Ctx, &persistence.InternalGetOrCreateShardRequest{
			ShardID: 4,
			CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
				return 5, dataBlob("init"), nil
			},
		})
		fatalOnErr(t, "seed shard", err)

		if err := s.AssertShardOwnership(tc.Ctx, &persistence.AssertShardOwnershipRequest{
			ShardID: 4, RangeID: 5,
		}); err != nil {
			t.Errorf("matching ownership should succeed: %v", err)
		}
	})

	t.Run("AssertShardOwnership_Mismatch_NoError_Per_REQ_F_024", func(t *testing.T) {
		// AssertShardOwnership is a no-op on this backend.
		// The range-ID check is already covered by UpdateShard's
		// single-doc CAS; this call is informational from upstream's
		// perspective. A mismatch is NOT an error — the caller's next
		// write-path call will detect stale ownership via the normal
		// CAS flow.
		tc := newTestContext(t, f)
		fatalOnErr(t, "ensure indexes", shardstore.EnsureIndexes(tc.Ctx, tc.DB))
		s := shardstore.New(tc.DB, "cluster0")

		_, err := s.GetOrCreateShard(tc.Ctx, &persistence.InternalGetOrCreateShardRequest{
			ShardID: 5,
			CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
				return 5, dataBlob("init"), nil
			},
		})
		fatalOnErr(t, "seed shard", err)

		if err := s.AssertShardOwnership(tc.Ctx, &persistence.AssertShardOwnershipRequest{
			ShardID: 5, RangeID: 999,
		}); err != nil {
			t.Errorf("AssertShardOwnership is a no-op; got unexpected error: %v", err)
		}
	})

	// Unused imports kept via blank-use for forward compatibility when
	// additional shard scenarios (e.g., multi-range RSM) land here.
	_ = enumspb.ENCODING_TYPE_PROTO3
	_ = &persistencespb.ShardInfo{}
}
