// Package clustermetastore owns persistence.ClusterMetadataStore
// . Real behaviour in store.go; this file keeps the
// constructor and GetName shim.
package clustermetastore

import (
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/metrics"
)

const storeName = "clustermetastore"

type Stub struct {
	db      *mongo.Database
	emitter *metrics.Emitter // nil-safe
}

func NewStub() *Stub { return &Stub{} }

// New returns a cluster-metadata store bound to the shared database.
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

var _ persistence.ClusterMetadataStore = (*Stub)(nil)
