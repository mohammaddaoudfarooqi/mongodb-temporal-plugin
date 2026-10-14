package kb

import (
	"reflect"
	"testing"
)

// Reciprocal Rank Fusion: a document ranked high by both searches wins
// over one ranked first by only one of them.
func TestFuse_RRFPrefersAgreementBetweenSearches(t *testing.T) {
	text := []Hit{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	vector := []Hit{{ID: "b"}, {ID: "d"}, {ID: "a"}}
	got := Fuse(text, vector, 60, 3)
	ids := []string{}
	for _, h := range got {
		ids = append(ids, h.ID)
	}
	if !reflect.DeepEqual(ids, []string{"b", "a", "d"}) && !reflect.DeepEqual(ids, []string{"b", "a", "c"}) {
		t.Fatalf("fused order %v; b and a appear in both lists and must lead", ids)
	}
	if got[0].TextRank != 2 || got[0].VectorRank != 1 {
		t.Fatalf("ranks not recorded: %+v", got[0])
	}
}

func TestFuse_OneListOnly(t *testing.T) {
	got := Fuse([]Hit{{ID: "x"}, {ID: "y"}}, nil, 60, 5)
	if len(got) != 2 || got[0].ID != "x" || got[0].VectorRank != 0 {
		t.Fatalf("full-text-only fusion must keep the text order: %+v", got)
	}
}
