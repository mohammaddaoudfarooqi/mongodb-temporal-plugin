// Package metadatastore hosts the MongoDB-backed implementation of
// persistence.MetadataStore. Real behaviour lives in
// store.go; this file keeps only the constructor and the no-I/O
// bookkeeping methods.
package metadatastore

import (
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
)

const storeName = "metadatastore"

type Stub struct {
	db      *mongo.Database
	emitter *metrics.Emitter // nil-safe
}

// NewStub returns a disconnected metadata store; all methods error out
// without a *mongo.Database. Retained so the factory's early wiring
// paths compile before a provider exists.
func NewStub() *Stub { return &Stub{} }

// New returns a metadata store bound to the shared database.
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

var _ persistence.MetadataStore = (*Stub)(nil)
