package nucleus

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPubSubModelExists(t *testing.T) {
	var _ *PubSubModel
}

func TestPubSubPublish(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 3
				return nil
			}}
		},
	}

	ps := &PubSubModel{pool: q, client: nucleusClient()}
	n, err := ps.Publish(context.Background(), "events", `{"type":"update"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 3 {
		t.Errorf("subscriber count = %d, want 3", n)
	}
	if capturedSQL != "SELECT PUBSUB_PUBLISH($1, $2)" {
		t.Errorf("SQL = %q, want SELECT PUBSUB_PUBLISH($1, $2)", capturedSQL)
	}
	if len(capturedArgs) != 2 {
		t.Fatalf("args len = %d, want 2", len(capturedArgs))
	}
	if capturedArgs[0] != "events" {
		t.Errorf("channel = %v, want events", capturedArgs[0])
	}
	if capturedArgs[1] != `{"type":"update"}` {
		t.Errorf("message = %v", capturedArgs[1])
	}
}

func TestPubSubChannelsWithPattern(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = "events,notifications"
				return nil
			}}
		},
	}

	ps := &PubSubModel{pool: q, client: nucleusClient()}
	result, err := ps.Channels(context.Background(), "ev*")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == "" {
		t.Error("expected non-empty result")
	}
	if capturedSQL != "SELECT PUBSUB_CHANNELS($1)" {
		t.Errorf("SQL = %q, want SELECT PUBSUB_CHANNELS($1)", capturedSQL)
	}
	if len(capturedArgs) != 1 || capturedArgs[0] != "ev*" {
		t.Errorf("args = %v, want [ev*]", capturedArgs)
	}
}

func TestPubSubChannelsNoPattern(t *testing.T) {
	var capturedSQL string

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = "ch1,ch2,ch3"
				return nil
			}}
		},
	}

	ps := &PubSubModel{pool: q, client: nucleusClient()}
	_, err := ps.Channels(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedSQL != "SELECT PUBSUB_CHANNELS()" {
		t.Errorf("SQL = %q, want SELECT PUBSUB_CHANNELS()", capturedSQL)
	}
}

func TestPubSubSubscribers(t *testing.T) {
	var capturedSQL string
	var capturedArgs []any

	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			capturedSQL = sql
			capturedArgs = args
			return &mockCDCRow{scanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 7
				return nil
			}}
		},
	}

	ps := &PubSubModel{pool: q, client: nucleusClient()}
	n, err := ps.Subscribers(context.Background(), "events")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 7 {
		t.Errorf("subscribers = %d, want 7", n)
	}
	if capturedSQL != "SELECT PUBSUB_SUBSCRIBERS($1)" {
		t.Errorf("SQL = %q", capturedSQL)
	}
	if len(capturedArgs) != 1 || capturedArgs[0] != "events" {
		t.Errorf("args = %v", capturedArgs)
	}
}

func TestPubSubRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	ps := &PubSubModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Publish", func() error { _, err := ps.Publish(context.Background(), "ch", "msg"); return err }},
		{"Channels", func() error { _, err := ps.Channels(context.Background(), ""); return err }},
		{"Subscribers", func() error { _, err := ps.Subscribers(context.Background(), "ch"); return err }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Fatal("expected error for non-Nucleus database")
			}
		})
	}
}

func TestPubSubPublishDBError(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				return fmt.Errorf("connection lost")
			}}
		},
	}

	ps := &PubSubModel{pool: q, client: nucleusClient()}
	_, err := ps.Publish(context.Background(), "ch", "msg")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPubSubSubscribersDBError(t *testing.T) {
	q := &mockCDCQuerier{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockCDCRow{scanFn: func(dest ...any) error {
				return fmt.Errorf("timeout")
			}}
		},
	}

	ps := &PubSubModel{pool: q, client: nucleusClient()}
	_, err := ps.Subscribers(context.Background(), "ch")
	if err == nil {
		t.Fatal("expected error")
	}
}
