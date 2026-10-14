// Package taskstore hosts the MongoDB-backed implementation of
// persistence.TaskStore. Real methods live in store.go;
// this file keeps only the constructor and GetName shim.
package taskstore

import (
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
)

const storeName = "taskstore"

type Stub struct {
	db      *mongo.Database
	emitter *metrics.Emitter // nil-safe
	// Backlog collections. The fair store keeps its own pair because
	// matching runs both backlogs of a queue at once, each holding its
	// own RangeID lease. User data stays shared.
	queuesColl string
	tasksColl  string
}

// NewStub retains the scaffold constructor for factory wiring. Methods
// that need the database error out without one.
func NewStub() *Stub { return &Stub{} }

// New returns a taskstore bound to the shared database.
func New(db *mongo.Database) *Stub {
	return &Stub{db: db, queuesColl: taskQueuesColl, tasksColl: tasksColl}
}

// NewFair returns the fair-backlog taskstore, backed by its own
// task-queue and task collections.
func NewFair(db *mongo.Database) *Stub {
	return &Stub{db: db, queuesColl: fairTaskQueuesColl, tasksColl: fairTasksColl}
}

// NewWithMetrics is New + a pre-scoped metrics emitter.
func NewWithMetrics(db *mongo.Database, em *metrics.Emitter) *Stub {
	return withMetrics(New(db), em)
}

// NewFairWithMetrics is NewFair + a pre-scoped metrics emitter.
func NewFairWithMetrics(db *mongo.Database, em *metrics.Emitter) *Stub {
	return withMetrics(NewFair(db), em)
}

func withMetrics(s *Stub, em *metrics.Emitter) *Stub {
	if em != nil {
		s.emitter = em.ForStore(storeName)
	}
	return s
}

func (s *Stub) Close()          {}
func (s *Stub) GetName() string { return storeName }

var _ persistence.TaskStore = (*Stub)(nil)
