package kb

import "fmt"

// Mode is how vectors for semantic search are produced.
type Mode int

const (
	// ModeNone: no embeddings; hybrid search falls back to full-text.
	ModeNone Mode = iota
	// ModeAuto: Atlas Vector Search automated embedding. Atlas embeds the
	// text field with Voyage AI; queries pass plain text (Atlas, preview).
	ModeAuto
	// ModeVoyage: the app calls the Voyage AI API and stores vectors
	// (any MongoDB 8 deployment with search, e.g. local Docker).
	ModeVoyage
)

func (m Mode) String() string {
	switch m {
	case ModeAuto:
		return "auto"
	case ModeVoyage:
		return "voyage"
	}
	return "none"
}

// ParseMode reads SHOWCASE_EMBEDDING.
func ParseMode(s string) (Mode, error) {
	switch s {
	case "auto":
		return ModeAuto, nil
	case "voyage":
		return ModeVoyage, nil
	case "", "none":
		return ModeNone, nil
	}
	return ModeNone, fmt.Errorf("SHOWCASE_EMBEDDING must be auto, voyage or none, not %q", s)
}

// Index names on the tickets collection.
const (
	TextIndexName   = "tickets_text"
	VectorIndexName = "tickets_vector"
)

// TextIndexDefinition is the Atlas Search index behind full-text ticket
// search: English analysis on the prose fields, and tokens for filtering.
func TextIndexDefinition() map[string]any {
	english := map[string]any{"type": "string", "analyzer": "lucene.english"}
	return map[string]any{
		"mappings": map[string]any{
			"dynamic": false,
			"fields": map[string]any{
				"subject":     english,
				"body":        english,
				"resolution":  english,
				"status":      map[string]any{"type": "token"},
				"category":    map[string]any{"type": "token"},
				"customer_id": map[string]any{"type": "token"},
			},
		},
	}
}

// VectorIndexDefinition is the vector index for the mode. In auto mode
// Atlas embeds the "text" field itself; in voyage mode the app stores
// vectors in "embedding". Both can pre-filter on status and category.
func VectorIndexDefinition(mode Mode, model string, dims int) map[string]any {
	filters := []any{
		map[string]any{"type": "filter", "path": "status"},
		map[string]any{"type": "filter", "path": "category"},
	}
	var field map[string]any
	if mode == ModeAuto {
		field = map[string]any{"type": "autoEmbed", "modality": "text", "path": "text", "model": model}
	} else {
		field = map[string]any{"type": "vector", "path": "embedding", "numDimensions": dims, "similarity": "cosine"}
	}
	return map[string]any{"fields": append([]any{field}, filters...)}
}
