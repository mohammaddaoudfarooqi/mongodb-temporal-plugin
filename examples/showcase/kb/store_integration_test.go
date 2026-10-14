//go:build integration

package kb

import (
	"context"
	"hash/fnv"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// bagEmbedder is a deterministic stand-in for Voyage: a hashed bag of
// words, so texts sharing words are close. TestVoyage_Live checks the
// real API against the same Embedder contract.
type bagEmbedder struct{ dims int }

func (b bagEmbedder) Embed(_ context.Context, texts []string, _ string) ([][]float64, error) {
	out := make([][]float64, len(texts))
	for i, t := range texts {
		v := make([]float64, b.dims)
		for _, w := range strings.Fields(strings.ToLower(t)) {
			h := fnv.New32a()
			_, _ = h.Write([]byte(strings.Trim(w, ".,!?")))
			v[int(h.Sum32())%b.dims]++
		}
		var n float64
		for _, x := range v {
			n += x * x
		}
		for j := range v {
			v[j] /= math.Sqrt(n) + 1e-9
		}
		out[i] = v
	}
	return out, nil
}

func startAtlasLocal(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mongodb/mongodb-atlas-local:8.2",
			ExposedPorts: []string{"27017/tcp"},
			WaitingFor:   wait.ForHealthCheck().WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start atlas-local: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "27017")
	return "mongodb://" + host + ":" + port.Port() + "/?directConnection=true"
}

var kbSeed = []Doc{
	{ID: "kb-1", Subject: "Double charge on invoice", Body: "Customer was charged twice for the same month", Resolution: "Refunded the duplicate charge from the billing console", Status: "resolved", Category: "billing"},
	{ID: "kb-2", Subject: "Password reset email missing", Body: "Reset link never arrives", Resolution: "Email was caught by a spam filter; whitelisted our sender domain", Status: "resolved", Category: "account"},
	{ID: "kb-3", Subject: "Cluster outage in us-east-1", Body: "All requests failing with timeouts", Resolution: "Failed over to the secondary region", Status: "resolved", Category: "outage"},
	{ID: "open-1", Subject: "Charged twice", Body: "duplicate charge again", Status: "open", Category: "billing"},
}

// The outcome a support agent needs: a new ticket finds the past
// resolution for the same problem, through both full-text and vector
// search, and open tickets are never offered as answers.
func TestStore_HybridSearchFindsResolvedTicketByTextAndMeaning(t *testing.T) {
	uri := startAtlasLocal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s, err := Open(ctx, uri, "support_test", ModeVoyage, "test", bagEmbedder{dims: 32}, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	for _, d := range kbSeed {
		if err := s.Upsert(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	var hits []Hit
	deadline := time.Now().Add(2 * time.Minute) // search indexes sync asynchronously
	for time.Now().Before(deadline) {
		hits, err = s.Similar(ctx, "I was charged twice, need a refund", 3)
		if err == nil && len(hits) > 0 && hits[0].TextRank > 0 && hits[0].VectorRank > 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 || hits[0].ID != "kb-1" {
		t.Fatalf("top hit %+v, want kb-1 (the billing resolution)", hits)
	}
	if hits[0].TextRank == 0 || hits[0].VectorRank == 0 {
		t.Fatalf("kb-1 must be found by both searches: %+v", hits[0])
	}
	if !strings.Contains(hits[0].Resolution, "Refunded") {
		t.Fatalf("the hit must carry the past resolution: %+v", hits[0])
	}
	for _, h := range hits {
		if h.ID == "open-1" {
			t.Fatal("an unresolved ticket was suggested as an answer")
		}
	}
}

// Resolving a ticket makes it an answer for later tickets.
func TestStore_ResolvedTicketBecomesSearchable(t *testing.T) {
	uri := startAtlasLocal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s, err := Open(ctx, uri, "support_test", ModeVoyage, "test", bagEmbedder{dims: 32}, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(ctx, Doc{ID: "t-9", Subject: "Webhook retries flood our endpoint", Body: "thousands of retries", Status: "open", Category: "general"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Resolve(ctx, "t-9", "Enabled exponential backoff for webhook retries"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		hits, _ := s.Similar(ctx, "webhook retries backoff", 3)
		if len(hits) > 0 && hits[0].ID == "t-9" {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("the resolved ticket never became searchable")
}

// Parity of the Embedder contract against the real Voyage API.
func TestVoyage_Live(t *testing.T) {
	key := os.Getenv("VOYAGE_API_KEY")
	if key == "" {
		t.Skip("VOYAGE_API_KEY not set")
	}
	v := &Voyage{Key: key, Model: "voyage-4"}
	vecs, err := v.Embed(context.Background(), []string{"refund a double charge", "password reset"}, "document")
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 2 || len(vecs[0]) != len(vecs[1]) || len(vecs[0]) == 0 {
		t.Fatalf("got %d vectors of %d dims", len(vecs), len(vecs[0]))
	}
}
