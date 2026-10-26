package kb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Voyage calls the Embedding API for Voyage AI models served by MongoDB
// (ai.mongodb.com). Used in ModeVoyage, where the deployment cannot
// embed for us (e.g. local Docker).
type Voyage struct {
	Key   string
	Model string
	URL   string // default EndpointFor(Key)
	HTTP  *http.Client
}

// voyageAttempts bounds retries of a rate-limited (HTTP 429) request.
const voyageAttempts = 5

// EndpointFor returns the embeddings endpoint for a model API key
// created in Atlas. A key scoped to a geography carries it in its prefix
// (al-eu-…) and works only against that geography's endpoint.
func EndpointFor(key string) string {
	for _, geo := range []string{"eu", "us"} {
		if strings.HasPrefix(key, "al-"+geo+"-") {
			return "https://" + geo + ".ai.mongodb.com/v1/embeddings"
		}
	}
	return "https://ai.mongodb.com/v1/embeddings"
}

// Embed returns one vector per input, in input order. inputType is
// "document" when storing and "query" when searching.
func (v *Voyage) Embed(ctx context.Context, texts []string, inputType string) ([][]float64, error) {
	url := v.URL
	if url == "" {
		url = EndpointFor(v.Key)
	}
	hc := v.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	body, _ := json.Marshal(map[string]any{"input": texts, "model": v.Model, "input_type": inputType})
	var raw []byte
	for attempt := 1; ; attempt++ {
		status, wait, b, err := post(ctx, hc, url, v.Key, body)
		if err != nil {
			return nil, err
		}
		raw = b
		if status == http.StatusOK {
			break
		}
		if status != http.StatusTooManyRequests || attempt == voyageAttempts {
			return nil, fmt.Errorf("voyage: HTTP %d: %s", status, bytes.TrimSpace(raw[:min(len(raw), 300)]))
		}
		if wait < 0 {
			wait = min(time.Duration(1<<attempt)*time.Second, 20*time.Second)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
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

// post sends one request. wait is the server's Retry-After, or -1.
func post(ctx context.Context, hc *http.Client, url, key string, body []byte) (status int, wait time.Duration, raw []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("voyage: %w", err)
	}
	defer resp.Body.Close()
	raw, _ = io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	wait = -1
	if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s >= 0 {
		wait = time.Duration(min(s, 60)) * time.Second
	}
	return resp.StatusCode, wait, raw, nil
}
