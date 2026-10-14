// Package nexusstore hosts the MongoDB-backed implementation of
// persistence.NexusEndpointStore. Real methods live
// in store.go.
package nexusstore

import (
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
)

const storeName = "nexusstore"

type Stub struct {
	db      *mongo.Database
	emitter *metrics.Emitter // nil-safe
}

// NewStub retains scaffold compatibility.
func NewStub() *Stub { return &Stub{} }

// New returns a nexus store bound to the shared database.
func New(db *mongo.Database) *Stub { return &Stub{db: db} }

// NewWithMetrics is New + a pre-scoped metrics emitter.
func NewWithMetrics(db *mongo.Database, em *metrics.Emitter) *Stub {
	s := New(db)
	if em != nil {
		s.emitter = em.ForStore(storeName)
	}
	return s
}

func (s *Stub) Close()          {}
func (s *Stub) GetName() string { return storeName }

var _ persistence.NexusEndpointStore = (*Stub)(nil)
