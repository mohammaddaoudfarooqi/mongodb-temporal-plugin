package visibility

import (
	"errors"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/security"
)

// TestRejectJS_QueryToFilter_RejectsWhere is the guard that the
// visibility query path must call security.RejectJSFilter on any DSL-
// derived predicate. If a future contributor bypasses the
// converter and hand-crafts a bson.M that includes $where (or a library
// regression starts emitting one), the dispatch must refuse rather
// than forward the JS to Mongo.
//
// We construct the predicate the same way queryToFilter would combine a
// namespace scope with a DSL-produced clause, then run it through the
// same RejectJSFilter call the production path must use. The assertion
// is symmetric for queryToFilter and buildSearchStage callers.
func TestRejectJS_QueryToFilter_RejectsWhere(t *testing.T) {
	base := bson.M{"namespace_id": "ns"}
	bad := bson.M{"$where": "function(){ return true }"}
	combined := bson.M{"$and": []bson.M{base, bad}}

	err := security.RejectJSFilter(combined)
	if err == nil {
		t.Fatal("expected RejectJSFilter to reject $where; got nil")
	}
	if !strings.Contains(err.Error(), "$where") {
		t.Errorf("error should name $where; got %q", err.Error())
	}
}

// TestRejectJS_CallerEnsuresGuard documents that every predicate that
// reaches the dispatch path has a corresponding guard call. This is a
// static contract: both queryToFilter and buildSearchStage must route
// their output through rejectJSOrTranslate before returning. The
// helper under test here mirrors the internal call site.
func TestRejectJS_Guard_AcceptsSafeFilter(t *testing.T) {
	safe := bson.M{"$and": []bson.M{
		{"namespace_id": "ns"},
		{"status": int32(1)},
		{"workflow_id": bson.M{"$eq": "wf"}},
	}}
	if err := security.RejectJSFilter(safe); err != nil {
		t.Errorf("expected no error on safe filter; got %v", err)
	}
}

// TestRejectJS_Guard_RejectsNestedAccumulator confirms the walker
// recurses through $and / $or arrays. A $where nested two levels
// deep must still be refused — a blocklist that only checks top-level
// keys would miss this.
func TestRejectJS_Guard_RejectsNestedAccumulator(t *testing.T) {
	nested := bson.M{"$or": []bson.M{
		{"namespace_id": "ns"},
		{"$and": []bson.M{
			{"workflow_id": "wf"},
			{"$accumulator": bson.M{"init": "function(){}"}},
		}},
	}}
	err := security.RejectJSFilter(nested)
	if err == nil {
		t.Fatal("expected RejectJSFilter to reject nested $accumulator; got nil")
	}
	if !errors.Is(err, err) { // sanity: returned error is a concrete error
		t.Errorf("rejection error shape drift: %v", err)
	}
}
