package security_test

import (
	"errors"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/security"
)

func TestRedactURI_ReplacesPassword(t *testing.T) {
	raw := "mongodb+srv://alice:s3cret@cluster-0.example.net/?retryWrites=true"
	out := security.RedactURI(raw)
	if strings.Contains(out, "s3cret") {
		t.Errorf("password leaked in redacted URI: %q", out)
	}
	if !strings.Contains(out, "alice") {
		t.Errorf("username should remain: %q", out)
	}
	if !strings.Contains(out, "cluster-0.example.net") {
		t.Errorf("host should remain: %q", out)
	}
}

// TestRedactError_StripsURIFromMessages ensures driver errors that
// embed the full URI get scrubbed before surfacing to the caller.
func TestRedactError_StripsURIFromMessages(t *testing.T) {
	in := errors.New(`connect failed: mongodb+srv://u:p@c.example.net/db — timeout`)
	out := security.RedactError(in)
	if strings.Contains(out.Error(), ":p@") {
		t.Errorf("password leaked: %q", out)
	}
	if !strings.Contains(out.Error(), "c.example.net") {
		t.Errorf("host lost: %q", out)
	}
}

// TestRedactError_NoURI_Unchanged ensures the helper is a no-op on
// messages without a URI.
func TestRedactError_NoURI_Unchanged(t *testing.T) {
	in := errors.New("some other error without a URI")
	out := security.RedactError(in)
	if out.Error() != in.Error() {
		t.Errorf("no-URI error should pass through unchanged; got %q", out.Error())
	}
}

func TestRejectJSFilter_CatchesDollarWhere(t *testing.T) {
	bad := bson.M{
		"namespace_id": "x",
		"$where":       "function(){return true}",
	}
	if err := security.RejectJSFilter(bad); err == nil {
		t.Error("expected rejection of $where")
	}
}

// TestRejectJSFilter_CatchesNested covers nested $function under $and.
func TestRejectJSFilter_CatchesNested(t *testing.T) {
	bad := bson.M{
		"$and": []bson.M{
			{"status": 1},
			{"$function": bson.M{"body": "..."}},
		},
	}
	if err := security.RejectJSFilter(bad); err == nil {
		t.Error("expected rejection of nested $function")
	}
}

func TestRejectJSFilter_AllowsRegularFilters(t *testing.T) {
	ok := bson.M{
		"namespace_id": "ns",
		"status":       bson.M{"$in": []int32{1, 2}},
		"$and":         []bson.M{{"start_time": bson.M{"$gte": 0}}},
	}
	if err := security.RejectJSFilter(ok); err != nil {
		t.Errorf("safe filter rejected: %v", err)
	}
}
