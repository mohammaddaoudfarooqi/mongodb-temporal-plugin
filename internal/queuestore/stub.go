// Package queuestore hosts the MongoDB-backed implementations of
// persistence.Queue (legacy) and persistence.QueueV2.
// Real methods live in store.go.
package queuestore

import (
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
)

const (
	storeNameLegacy = "queuestore"
	storeNameV2     = "queuestore_v2"
)

// Stub is the Queue implementation (retained name for scaffold wiring).
type Stub struct {
	db        *mongo.Database
	queueType persistence.QueueType
	emitter   *metrics.Emitter // nil-safe
}

// NewStub retains the scaffold constructor.
func NewStub(qt persistence.QueueType) *Stub {
	return &Stub{queueType: qt}
}

// New returns a Queue bound to the shared database.
func New(db *mongo.Database, qt persistence.QueueType) *Stub {
	return &Stub{db: db, queueType: qt}
}

// NewWithMetrics is New + a pre-scoped metrics emitter.
func NewWithMetrics(db *mongo.Database, qt persistence.QueueType, em *metrics.Emitter) *Stub {
	s := New(db, qt)
	if em != nil {
		s.emitter = em.ForStore(storeNameLegacy)
	}
	return s
}

func (s *Stub) Close() {}

var _ persistence.Queue = (*Stub)(nil)

// V2Stub is the QueueV2 implementation.
type V2Stub struct {
	db      *mongo.Database
	emitter *metrics.Emitter // nil-safe
}

// NewV2Stub retains the scaffold constructor.
func NewV2Stub() *V2Stub { return &V2Stub{} }

// NewV2 returns a QueueV2 bound to the shared database.
func NewV2(db *mongo.Database) *V2Stub { return &V2Stub{db: db} }

// NewV2WithMetrics is NewV2 + a pre-scoped metrics emitter.
func NewV2WithMetrics(db *mongo.Database, em *metrics.Emitter) *V2Stub {
	v := NewV2(db)
	if em != nil {
		v.emitter = em.ForStore(storeNameV2)
	}
	return v
}

var _ persistence.QueueV2 = (*V2Stub)(nil)
