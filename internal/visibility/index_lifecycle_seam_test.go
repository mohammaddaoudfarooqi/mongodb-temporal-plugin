package visibility

// setStatusForTest is a test-only seam that simulates an Atlas Search
// status poll without needing a live cluster. Production code only
// transitions lastStatus/ready via checkOnce (which calls
// QueryIndexStatus). Lives in a _test.go file so it never ships in a
// production binary and cannot be called by non-test code.
//
// Prior location in index_lifecycle.go was a test surface
// leaked into the production type. Moving it here keeps the type's
// public surface honest without losing unit-test coverage of the
// status-transition machine (index_lifecycle_test.go).
func (t *IndexTracker) setStatusForTest(s IndexStatus) {
	cp := s
	t.lastStatus.Store(&cp)
	t.ready.Store(s == IndexStatusReady)
}
