package visibility

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// IndexStatus names the states an Atlas Search index transitions
// through. The subset we track matches Atlas's published lifecycle.
type IndexStatus string

const (
	IndexStatusUnknown  IndexStatus = ""
	IndexStatusPending  IndexStatus = "PENDING"
	IndexStatusBuilding IndexStatus = "BUILDING"
	IndexStatusReady    IndexStatus = "READY"
	IndexStatusFailed   IndexStatus = "FAILED"
	IndexStatusDeleting IndexStatus = "DELETING"
)

// IndexTracker polls the Atlas Search index status on a bounded
// interval. While the index is not READY, visibility reads
// return *serviceerror.Unavailable; writes still land so no records are
// lost (Atlas indexes them on activation). Once READY, reads succeed
// without a plugin restart.
type IndexTracker struct {
	db         *mongo.Database
	collection string
	indexName  string
	poll       time.Duration

	// ready is atomic so Store.IsIndexReady can read it without locks.
	ready atomic.Bool

	// lastStatus is the most recently observed status, cached for
	// metrics + log emission.
	lastStatus atomic.Pointer[IndexStatus]

	cancel context.CancelFunc
}

// NewIndexTracker constructs a tracker and starts its background
// polling goroutine. The poll interval is capped at 30 seconds to
// keep Atlas API load bounded on large fleets. Close via Stop().
//
// Accepts a nil db for unit tests — Start becomes a no-op.
func NewIndexTracker(db *mongo.Database, indexName string, poll time.Duration) *IndexTracker {
	if poll < time.Second {
		poll = 5 * time.Second
	}
	if poll > 30*time.Second {
		poll = 30 * time.Second
	}
	return &IndexTracker{
		db:         db,
		collection: executionsColl,
		indexName:  indexName,
		poll:       poll,
	}
}

// Start begins background polling. Safe to call more than once; only
// the first call starts the goroutine. The goroutine is
// owned by the tracker and stops when Stop() is called (the factory
// calls Stop during Close, so goroutines do not outlive the factory).
func (t *IndexTracker) Start(ctx context.Context) {
	if t.db == nil || t.cancel != nil {
		return
	}
	child, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	go t.pollLoop(child)
}

// Stop cancels the polling goroutine.
func (t *IndexTracker) Stop() {
	if t.cancel != nil {
		t.cancel()
		t.cancel = nil
	}
}

// IsReady reports the most recently observed state. Atomic, safe to
// call from any goroutine.
func (t *IndexTracker) IsReady() bool { return t.ready.Load() }

// LastStatus returns the last observed status. Useful for metrics.
func (t *IndexTracker) LastStatus() IndexStatus {
	p := t.lastStatus.Load()
	if p == nil {
		return IndexStatusUnknown
	}
	return *p
}

// pollLoop runs until ctx is cancelled, refreshing the status on each tick.
func (t *IndexTracker) pollLoop(ctx context.Context) {
	// Initial check — don't wait a full poll interval to discover the
	// index state at startup.
	_ = t.checkOnce(ctx)
	tk := time.NewTicker(t.poll)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			_ = t.checkOnce(ctx)
		}
	}
}

// checkOnce queries the current index status and updates ready/lastStatus.
func (t *IndexTracker) checkOnce(ctx context.Context) error {
	status, err := QueryIndexStatus(ctx, t.db, t.collection, t.indexName)
	if err != nil {
		// A probe failure is not fatal; we just keep the previous status
		// and let the next tick retry.
		return err
	}
	s := status
	t.lastStatus.Store(&s)
	t.ready.Store(status == IndexStatusReady)
	return nil
}

// QueryIndexStatus runs $listSearchIndexes and returns the status of
// the named index. Exported so tests and diagnostics (metrics, verify
// CLI) can reuse it. Returns IndexStatusUnknown when the named index
// is not present; errors surface MongoDB driver failures.
func QueryIndexStatus(ctx context.Context, db *mongo.Database, collection, indexName string) (IndexStatus, error) {
	if db == nil {
		return IndexStatusUnknown, errors.New("visibility: nil database")
	}
	pipeline := mongo.Pipeline{
		{{Key: "$listSearchIndexes", Value: bson.M{"name": indexName}}},
	}
	cursor, err := searchCollection(db, collection).Aggregate(ctx, pipeline)
	if err != nil {
		return IndexStatusUnknown, err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var raw struct {
			Name   string `bson:"name"`
			Status string `bson:"status"`
		}
		if err := cursor.Decode(&raw); err != nil {
			return IndexStatusUnknown, err
		}
		if raw.Name != indexName {
			continue
		}
		return IndexStatus(strings.ToUpper(raw.Status)), nil
	}
	return IndexStatusUnknown, nil
}
