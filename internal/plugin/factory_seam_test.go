package plugin

// NewFactoryCallCountForTests returns the number of distinct
// mongo.Client instances this factory has created. Test-only seam
// that lets the "single client across all stores" guarantee be
// asserted without exposing the provider itself.
//
// Lives in a _test.go file so it never ships in a
// production binary and can't be called by non-test code.
func (f *DataStoreFactory) NewFactoryCallCountForTests() int {
	return int(f.clientBuilds.Load())
}
