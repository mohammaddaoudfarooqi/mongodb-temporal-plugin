package kb

import (
	"encoding/json"
	"strings"
	"testing"
)

func defJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestVectorIndex_AutoModeUsesAtlasAutoEmbed(t *testing.T) {
	d := defJSON(t, VectorIndexDefinition(ModeAuto, "voyage-4", 0))
	for _, want := range []string{`"type":"autoEmbed"`, `"modality":"text"`, `"model":"voyage-4"`, `"path":"text"`, `"type":"filter"`} {
		if !strings.Contains(d, want) {
			t.Errorf("auto-embed definition lacks %s: %s", want, d)
		}
	}
}

func TestVectorIndex_VoyageModeIndexesStoredVectors(t *testing.T) {
	d := defJSON(t, VectorIndexDefinition(ModeVoyage, "voyage-4", 1024))
	for _, want := range []string{`"type":"vector"`, `"path":"embedding"`, `"numDimensions":1024`, `"similarity":"cosine"`} {
		if !strings.Contains(d, want) {
			t.Errorf("client-side vector definition lacks %s: %s", want, d)
		}
	}
}

func TestTextIndex_AnalysesTicketFields(t *testing.T) {
	d := defJSON(t, TextIndexDefinition())
	for _, want := range []string{`"subject"`, `"body"`, `"resolution"`, `"lucene.english"`, `"status"`} {
		if !strings.Contains(d, want) {
			t.Errorf("text index lacks %s: %s", want, d)
		}
	}
}

func TestParseMode(t *testing.T) {
	for in, want := range map[string]Mode{"auto": ModeAuto, "voyage": ModeVoyage, "none": ModeNone, "": ModeNone} {
		if got, err := ParseMode(in); err != nil || got != want {
			t.Errorf("ParseMode(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := ParseMode("magic"); err == nil {
		t.Error("an unknown mode must be rejected")
	}
}
