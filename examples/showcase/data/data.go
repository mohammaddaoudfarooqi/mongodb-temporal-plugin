// Package data embeds the knowledge-base seed: past resolved tickets, so
// suggestions work from the first run.
package data

import (
	_ "embed"
	"encoding/json"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/kb"
)

//go:embed kb.json
var raw []byte

// Seed returns the resolved tickets to load.
func Seed() ([]kb.Doc, error) {
	var docs []kb.Doc
	if err := json.Unmarshal(raw, &docs); err != nil {
		return nil, err
	}
	for i := range docs {
		docs[i].Status = "resolved"
	}
	return docs, nil
}
