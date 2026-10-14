// Package shardstore will own the MongoDB-backed implementation of
// persistence.ShardStore. At scaffold stage it exposes
// a stub that returns serviceerror.Unimplemented so the plugin satisfies
// the factory contract without yet doing any MongoDB I/O.
package shardstore

import (
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
)

const storeName = "shardstore"

// Stub is the scaffold implementation of persistence.ShardStore. The real
// implementation fills in the methods incrementally.
type Stub struct {
	db          *mongo.Database
	clusterName string
	// emitter is the per-store metrics emitter. Nil when the store
	// was constructed via the plain New (tests); observeOp no-ops.
	emitter *metrics.Emitter
}

// NewStub returns a scaffold ShardStore. All methods return Unimplemented.
func NewStub() *Stub { return &Stub{} }

// New returns a scaffold store that retains the shared *mongo.Database
// and cluster name. Used by the factory's shared-client wiring.
func New(db *mongo.Database, clusterName string) *Stub {
	return &Stub{db: db, clusterName: clusterName}
}

// NewWithMetrics is New + a pre-scoped metrics emitter.
func NewWithMetrics(db *mongo.Database, clusterName string, em *metrics.Emitter) *Stub {
	s := New(db, clusterName)
	if em != nil {
		s.emitter = em.ForStore(storeName)
	}
	return s
}

func (s *Stub) Close()                 {}
func (s *Stub) GetName() string        { return storeName }
func (s *Stub) GetClusterName() string { return s.clusterName }

// Static interface guard: catches upstream interface drift at build time.
// Real store methods live in store.go. The Stub type remains the
// concrete receiver for backward compatibility with NewStub().
var _ persistence.ShardStore = (*Stub)(nil)
