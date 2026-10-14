// Package kb is the support knowledge base: tickets stored in MongoDB,
// found again with hybrid full-text ($search) and semantic
// ($vectorSearch) search on the same cluster that runs Temporal.
package kb

import "sort"

// Hit is one search result. TextRank and VectorRank are 1-based
// positions in each search's results (0: not returned by that search).
type Hit struct {
	ID         string  `json:"id"`
	Subject    string  `json:"subject"`
	Resolution string  `json:"resolution,omitempty"`
	Score      float64 `json:"score"`
	TextRank   int     `json:"text_rank,omitempty"`
	VectorRank int     `json:"vector_rank,omitempty"`
}

// Fuse merges two ranked lists with Reciprocal Rank Fusion: each
// document scores the sum of 1/(k+rank) over the lists it appears in,
// so agreement between the searches outranks a single strong match.
func Fuse(text, vector []Hit, k, limit int) []Hit {
	byID := map[string]*Hit{}
	var order []string
	add := func(list []Hit, isText bool) {
		for i, h := range list {
			cur, ok := byID[h.ID]
			if !ok {
				c := h
				c.Score, c.TextRank, c.VectorRank = 0, 0, 0
				cur = &c
				byID[h.ID] = cur
				order = append(order, h.ID)
			}
			if cur.Resolution == "" {
				cur.Resolution = h.Resolution
			}
			cur.Score += 1 / float64(k+i+1)
			if isText {
				cur.TextRank = i + 1
			} else {
				cur.VectorRank = i + 1
			}
		}
	}
	add(text, true)
	add(vector, false)
	out := make([]Hit, 0, len(order))
	for _, id := range order {
		out = append(out, *byID[id])
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}
