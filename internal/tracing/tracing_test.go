package tracing_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/tracing"
)

// TestTracer_Start_SpanNameConvention + P12: span
// names start with "mongo.driver." to avoid collision with upstream's
// persistence.* spans.
func TestTracer_Start_SpanNameConvention(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := trace.NewTracerProvider(trace.WithSyncer(exporter))
	tr := tracing.New(provider)

	_, span := tr.Start(context.Background(), "shardstore", "UpdateShard")
	span.End()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("want 1 span; got %d", len(spans))
	}
	want := "mongo.driver.shardstore.UpdateShard"
	if spans[0].Name != want {
		t.Errorf("span name: got %q want %q", spans[0].Name, want)
	}
}

// TestTracer_NilProvider_Noop ensures a nil TracerProvider does not
// panic. A zero-value Tracer must be callable.
func TestTracer_NilProvider_Noop(t *testing.T) {
	tr := tracing.New(nil)
	_, span := tr.Start(context.Background(), "store", "op")
	span.End() // must not panic
}
