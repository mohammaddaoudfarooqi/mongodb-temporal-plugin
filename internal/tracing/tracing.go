// Package tracing wraps the OpenTelemetry tracer so the plugin can
// emit per-operation spans without each store re-importing the
// OTel SDK. Span names follow the pattern
// mongo.driver.<store>.<op> (P12) so they never collide with the
// upstream telemetry wrapper's `persistence.<op>` spans.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Tracer wraps a go-otel trace.Tracer. A zero-value Tracer is a no-op;
// stores can call Start without checking for nil.
type Tracer struct {
	t trace.Tracer
}

// New constructs a Tracer bound to a TracerProvider. Passing nil
// returns a no-op Tracer.
func New(provider trace.TracerProvider) *Tracer {
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	return &Tracer{t: provider.Tracer("github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin")}
}

// Start opens a span named mongo.driver.<store>.<op>. The returned
// context carries the span; callers must defer End() on the
// returned trace.Span.
func (t *Tracer) Start(ctx context.Context, store, op string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if t == nil || t.t == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	name := "mongo.driver." + store + "." + op
	return t.t.Start(ctx, name, trace.WithAttributes(attrs...))
}
