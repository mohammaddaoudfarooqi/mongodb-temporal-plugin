// Package executionstore hosts the MongoDB-backed implementation of
// persistence.ExecutionStore. Real methods live in
// store.go, document.go, history.go, tasks.go, and dlq.go. This file
// keeps the constructor and the HistoryBranchUtil delegation.
package executionstore

import (
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
)

const storeName = "executionstore"

// Stub is the executionstore state. Named "Stub" for historical reasons
// (M0 scaffold); rename pending when all methods are real.
type Stub struct {
	db          *mongo.Database
	clusterName string
	branchUtil  persistence.HistoryBranchUtil
	emitter     *metrics.Emitter // nil-safe: every emitter method no-ops when nil
}

// NewStub is retained for compatibility with scaffold wiring. Returns a
// stateless instance; methods that require the database error out.
func NewStub() *Stub {
	return &Stub{
		branchUtil: persistence.NewHistoryBranchUtil(serialization.NewSerializer()),
	}
}

// New returns a fully-wired execution store bound to the shared
// database. Callers that want persistence_mongo_* metrics should use
// NewWithMetrics instead — the zero-arg New keeps test callers
// source-compatible.
func New(db *mongo.Database, clusterName string) *Stub {
	return &Stub{
		db:          db,
		clusterName: clusterName,
		branchUtil:  persistence.NewHistoryBranchUtil(serialization.NewSerializer()),
	}
}

// NewWithMetrics is New + a pre-scoped metrics emitter. It
// threads the factory's emitter into the hot path so store methods
// can emit persistence_mongo_op_count / latency / errors.
func NewWithMetrics(db *mongo.Database, clusterName string, em *metrics.Emitter) *Stub {
	s := New(db, clusterName)
	if em != nil {
		s.emitter = em.ForStore(storeName)
	}
	return s
}

func (s *Stub) Close()          {}
func (s *Stub) GetName() string { return storeName }

// GetHistoryBranchUtil delegates to the upstream implementation.
func (s *Stub) GetHistoryBranchUtil() persistence.HistoryBranchUtil {
	return s.branchUtil
}

var _ persistence.ExecutionStore = (*Stub)(nil)
