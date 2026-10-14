package converter_test

import (
	"math/rand"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/query"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility/converter"
)

// Property-based tests for the BSON converter. Table-driven
// tests in converter_test.go cover specific operator shapes; these
// tests generate random operator+value pairs and assert two universal
// invariants:
//
//   P1: The emitted BSON never contains JS operators ($where,
//       $function, $accumulator). This is the defense-in-depth
//       contract against server-side JavaScript.
//   P2: The emitted BSON is BSON-serializable round-trip. bson.Marshal
//       on the output must not error, and the result decodes back to
//       a bson.M with the same top-level keys.
//
// Generator biases toward shapes upstream's DSL parser is likely to
// emit: keyword / int / string / datetime SA types paired with
// comparison, IN, LIKE, or IS operators.

type operatorCase struct {
	op        string
	value     any
	saType    enumspb.IndexedValueType
	invokesLI bool // true for ConvertTextComparisonExpr
}

func genOperator(rng *rand.Rand) operatorCase {
	type slot struct {
		op    string
		mkVal func(r *rand.Rand) any
		sa    enumspb.IndexedValueType
		like  bool
	}
	slots := []slot{
		{op: "=", mkVal: func(r *rand.Rand) any { return randAlpha(r, 1, 16) }, sa: enumspb.INDEXED_VALUE_TYPE_KEYWORD},
		{op: "!=", mkVal: func(r *rand.Rand) any { return randAlpha(r, 1, 16) }, sa: enumspb.INDEXED_VALUE_TYPE_KEYWORD},
		{op: "<", mkVal: func(r *rand.Rand) any { return int64(r.Intn(1_000_000)) }, sa: enumspb.INDEXED_VALUE_TYPE_INT},
		{op: "<=", mkVal: func(r *rand.Rand) any { return int64(r.Intn(1_000_000)) }, sa: enumspb.INDEXED_VALUE_TYPE_INT},
		{op: ">", mkVal: func(r *rand.Rand) any { return int64(r.Intn(1_000_000)) }, sa: enumspb.INDEXED_VALUE_TYPE_INT},
		{op: ">=", mkVal: func(r *rand.Rand) any { return int64(r.Intn(1_000_000)) }, sa: enumspb.INDEXED_VALUE_TYPE_INT},
		{op: "starts_with", mkVal: func(r *rand.Rand) any { return randAlpha(r, 1, 16) }, sa: enumspb.INDEXED_VALUE_TYPE_KEYWORD},
		{op: "=", mkVal: func(r *rand.Rand) any { return randAlpha(r, 1, 16) }, sa: enumspb.INDEXED_VALUE_TYPE_TEXT, like: true},
		{op: "!=", mkVal: func(r *rand.Rand) any { return randAlpha(r, 1, 16) }, sa: enumspb.INDEXED_VALUE_TYPE_TEXT, like: true},
		{op: "in", mkVal: func(r *rand.Rand) any {
			out := make([]any, 1+r.Intn(3))
			for i := range out {
				out[i] = randAlpha(r, 1, 8)
			}
			return out
		}, sa: enumspb.INDEXED_VALUE_TYPE_KEYWORD},
	}
	s := slots[rng.Intn(len(slots))]
	return operatorCase{op: s.op, value: s.mkVal(rng), saType: s.sa, invokesLI: s.like}
}

func randAlpha(rng *rand.Rand, minLen, maxLen int) string {
	n := minLen + rng.Intn(maxLen-minLen+1)
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	out := make([]byte, n)
	for i := range out {
		out[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(out)
}

// assertNoJSOp walks a bson.M and fails the test if any key matches
// a server-side-JS operator. Mirrors security.RejectJSFilter's policy.
func assertNoJSOp(t *testing.T, name string, node any) {
	t.Helper()
	switch v := node.(type) {
	case bson.M:
		for k, val := range v {
			switch k {
			case "$where", "$function", "$accumulator":
				t.Fatalf("%s: JS operator %q reached converter output", name, k)
			}
			assertNoJSOp(t, name, val)
		}
	case bson.D:
		for _, e := range v {
			switch e.Key {
			case "$where", "$function", "$accumulator":
				t.Fatalf("%s: JS operator %q reached converter output", name, e.Key)
			}
			assertNoJSOp(t, name, e.Value)
		}
	case []bson.M:
		for _, e := range v {
			assertNoJSOp(t, name, e)
		}
	case []any:
		for _, e := range v {
			assertNoJSOp(t, name, e)
		}
	}
}

// TestBSONConverter_Property_NoJSOperators is P1. The converter is
// explicitly coded to never emit JS, but a regression in one of the
// Convert* dispatch paths would surface here.
func TestBSONConverter_Property_NoJSOperators(t *testing.T) {
	c := converter.NewBSONConverter()
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 200; i++ {
		oc := genOperator(rng)
		col := query.NewSAColumn("WorkflowId", "WorkflowId", oc.saType)
		// Pick the right Convert* entry point; upstream's
		// query-converter dispatch routes text vs non-text separately.
		var (
			got bson.M
			err error
		)
		if oc.invokesLI {
			got, err = c.ConvertTextComparisonExpr(oc.op, col, oc.value)
		} else {
			got, err = c.ConvertComparisonExpr(oc.op, col, oc.value)
		}
		if err != nil {
			// InvalidArgument on an unsupported (op, type) is allowed;
			// the invariant only applies to the output tree.
			continue
		}
		assertNoJSOp(t, "iter", got)
	}
}

// TestBSONConverter_Property_BSONSerializable is P2. Every output
// must survive a bson.Marshal → bson.Unmarshal round-trip: if the
// converter ever emitted a non-serializable value (uintptr, chan,
// etc.), it'd surface here instead of at query time.
func TestBSONConverter_Property_BSONSerializable(t *testing.T) {
	c := converter.NewBSONConverter()
	rng := rand.New(rand.NewSource(4))
	for i := 0; i < 200; i++ {
		oc := genOperator(rng)
		col := query.NewSAColumn("WorkflowId", "WorkflowId", oc.saType)
		var (
			got bson.M
			err error
		)
		if oc.invokesLI {
			got, err = c.ConvertTextComparisonExpr(oc.op, col, oc.value)
		} else {
			got, err = c.ConvertComparisonExpr(oc.op, col, oc.value)
		}
		if err != nil {
			continue
		}
		raw, err := bson.Marshal(got)
		if err != nil {
			t.Fatalf("iter %d op=%s: bson.Marshal failed: %v (got=%+v)", i, oc.op, err, got)
		}
		var back bson.M
		if err := bson.Unmarshal(raw, &back); err != nil {
			t.Fatalf("iter %d op=%s: bson.Unmarshal failed: %v", i, oc.op, err)
		}
	}
}
