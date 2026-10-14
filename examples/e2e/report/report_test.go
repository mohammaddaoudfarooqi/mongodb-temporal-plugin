package report

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestResults_WriteRead_RoundTrip(t *testing.T) {
	in := Results{
		Phase: "smoke-aggregation",
		Scenarios: []Scenario{
			{Name: "order_saga", Pass: true, DurationMS: 2800},
			{Name: "schedule", Pass: false, DurationMS: 60000, Error: "found 0 scheduled runs"},
		},
	}
	p := filepath.Join(t.TempDir(), "r.json")
	if err := Write(p, in); err != nil {
		t.Fatal(err)
	}
	out, err := Read(p)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in %+v\nout %+v", in, out)
	}
	if out.Pass() {
		t.Error("results with a failed scenario reported as passing")
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatal(err)
	}
}
