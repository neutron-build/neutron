package otel

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/go/orm"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type recordingSpan struct {
	trace.Span
	events []trace.EventConfig
	names  []string
}

func (s *recordingSpan) IsRecording() bool { return true }
func (s *recordingSpan) AddEvent(name string, options ...trace.EventOption) {
	s.names = append(s.names, name)
	s.events = append(s.events, trace.NewEventConfig(options...))
}
func TestOperationContextOTelEventsStayRequestOwned(t *testing.T) {
	first := &recordingSpan{Span: noop.Span{}}
	second := &recordingSpan{Span: noop.Span{}}
	firstContext := trace.ContextWithSpan(context.Background(), first)
	secondContext := trace.ContextWithSpan(context.Background(), second)
	Observe(firstContext, orm.QueryEvent{Operation: "query", Duration: time.Millisecond, Rows: 2})
	Observe(secondContext, orm.QueryEvent{Operation: "exec", ErrorClass: "postgres", SQLState: "22P02"})
	Observe(context.Background(), orm.QueryEvent{Operation: "query"})
	if len(first.events) != 1 || len(second.events) != 1 || first.names[0] != "neutron.orm.query" {
		t.Fatal("request span leakage")
	}
	for _, span := range []*recordingSpan{first, second} {
		for _, event := range span.events {
			for _, attr := range event.Attributes() {
				key := string(attr.Key)
				if strings.Contains(key, "statement") || strings.Contains(key, "password") || strings.Contains(key, "parameter") {
					t.Fatal("unsafe tracing attribute")
				}
			}
		}
	}
	if first.events[0].Attributes()[1].Value.AsString() != "query" || second.events[0].Attributes()[1].Value.AsString() != "exec" {
		t.Fatal("events assigned to wrong spans")
	}
}
