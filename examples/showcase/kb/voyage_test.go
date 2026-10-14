package kb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVoyage_EmbedSendsInputTypeAndParsesVectorsInOrder(t *testing.T) {
	var got struct {
		Input     []string `json:"input"`
		Model     string   `json:"model"`
		InputType string   `json:"input_type"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k" {
			t.Errorf("missing bearer token")
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		// Out of order on purpose: the client must sort by index.
		_, _ = w.Write([]byte(`{"data":[{"index":1,"embedding":[0.3,0.4]},{"index":0,"embedding":[0.1,0.2]}]}`))
	}))
	defer srv.Close()
	v := &Voyage{Key: "k", Model: "voyage-4", URL: srv.URL}
	vecs, err := v.Embed(context.Background(), []string{"a", "b"}, "query")
	if err != nil {
		t.Fatal(err)
	}
	if got.Model != "voyage-4" || got.InputType != "query" || len(got.Input) != 2 {
		t.Fatalf("request = %+v", got)
	}
	if vecs[0][0] != 0.1 || vecs[1][0] != 0.3 {
		t.Fatalf("vectors out of order: %v", vecs)
	}
}

func TestVoyage_ErrorStatusIsReportedWithoutTheKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"detail":"bad key"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()
	_, err := (&Voyage{Key: "secret-key", Model: "voyage-4", URL: srv.URL}).Embed(context.Background(), []string{"a"}, "document")
	if err == nil || !strings.Contains(err.Error(), "401") || strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("err = %v", err)
	}
}
