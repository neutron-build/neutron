package neutron

import (
	"context"
)

type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyTraceID
)

// RequestIDFromContext returns the request ID stored in the context.
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyRequestID).(string); ok {
		return v
	}
	return ""
}

// withRequestID stores a request ID in the context.
func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, id)
}

// TraceIDFromContext returns the trace ID stored in the context.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyTraceID).(string); ok {
		return v
	}
	return ""
}

// withTraceID stores a trace ID in the context.
func withTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyTraceID, id)
}
