package mongo_test

import (
	"context"
	"errors"
	"testing"
	"time"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

// TestClientProvider_Unreachable_ReturnsErrorNotPanic:
// a configuration that cannot establish a connection surfaces an error
// from the constructor rather than panicking. Uses a TCP address no
// process is listening on and a short timeout so the test is fast.
func TestClientProvider_Unreachable_ReturnsErrorNotPanic(t *testing.T) {
	cfg := internalmongo.ClientConfig{
		URI:                      "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200",
		Database:                 "temporal",
		AppName:                  "temporal-mongodb-plugin/test",
		ConnectTimeoutMs:         200,
		ServerSelectionTimeoutMs: 200,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var p *internalmongo.ClientProvider
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("constructor panicked: %v", r)
			}
		}()
		var err error
		p, err = internalmongo.NewClientProvider(ctx, cfg)
		if err == nil {
			t.Error("expected connection error; got nil")
		}
	}()
	if p != nil {
		// If the provider was somehow created, clean up.
		_ = p.Close(context.Background())
	}
}

// TestClientProvider_CloseOnNilClient is a defensive check: an
// improperly-constructed provider (e.g., from a future test path) must
// tolerate Close() without panicking.
func TestClientProvider_CloseOnNilClient(t *testing.T) {
	p := &internalmongo.ClientProvider{}
	err := p.Close(context.Background())
	// Close on empty provider is a no-op (no client to drain); never
	// panics and never times out.
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("unexpected error from Close on zero provider: %v", err)
	}
}
