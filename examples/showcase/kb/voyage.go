package kb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"
)

// Voyage calls the Voyage AI embeddings API. Used in ModeVoyage, where
// the deployment cannot embed for us (e.g. local Docker).
type Voyage struct {
	Key   string
	Model string
	URL   string // default https://api.voyageai.com/v1/embeddings
	HTTP  *http.Client
}

// Embed returns one vector per input, in input order. inputType is
// "document" when storing and "query" when searching.
func (v *Voyage) Embed(ctx context.Context, texts []string, inputType string) ([][]float64, error) {
	url := v.URL
	if url == "" {
		url = "https://api.voyageai.com/v1/embeddings"
	}
	hc := v.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	body, _ := json.Marshal(map[string]any{"input": texts, "model": v.Model, "input_type": inputType})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+v.Key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("voyage: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("voyage: HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(raw[:min(len(raw), 300)]))
	}
	var out struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("voyage: decoding response: %w", err)
	}
	if len(out.Data) != len(texts) {
		return nil, fmt.Errorf("voyage: %d embeddings for %d inputs", len(out.Data), len(texts))
	}
	sort.Slice(out.Data, func(i, j int) bool { return out.Data[i].Index < out.Data[j].Index })
	vecs := make([][]float64, len(out.Data))
	for i, d := range out.Data {
		vecs[i] = d.Embedding
	}
	return vecs, nil
}
