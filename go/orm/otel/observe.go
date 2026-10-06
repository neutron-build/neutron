// Package otel adapts redacted ORM query events to the caller's OpenTelemetry
// span. It installs no global tracer provider or exporter and owns no workers.
package otel

import (
	"context"

	"github.com/neutron-build/neutron/go/orm"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Observe adds a bounded query-completion event to the span in this operation's
// context. It does not create/end a span, mark a request failed or record raw
// native errors. Caller request instrumentation owns those decisions/lifetimes.
// Pass Observe to orm.ObserveExecutor, which isolates observer panics.
func Observe(ctx context.Context, event orm.QueryEvent) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "postgresql"),
		attribute.String("neutron.orm.operation", event.Operation),
		attribute.Int64("neutron.orm.duration_ns", int64(event.Duration)),
		attribute.Int64("neutron.orm.rows", event.Rows),
	}
	if event.ErrorClass != "" {
		attrs = append(attrs, attribute.String("error.type", event.ErrorClass))
	}
	if event.SQLState != "" {
		attrs = append(attrs, attribute.String("db.response.status_code", event.SQLState))
	}
	span.AddEvent("neutron.orm.query", trace.WithAttributes(attrs...))
}
