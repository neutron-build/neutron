package nucleus

import (
	"context"
	"encoding/json"
	"fmt"
)

// StreamModel provides Redis Streams-compatible operations over Nucleus SQL functions.
type StreamModel struct {
	pool   querier
	client *Client
}

// StreamEntry represents a single entry in a stream.
type StreamEntry struct {
	ID     string         `json:"id"`
	Fields map[string]any `json:"fields"`
}

// XAdd appends an entry to a stream. Returns the generated entry ID.
func (s *StreamModel) XAdd(ctx context.Context, stream string, fields map[string]any) (string, error) {
	if err := s.client.requireNucleus("Streams.XAdd"); err != nil {
		return "", err
	}
	// Build variadic field/value arguments for STREAM_XADD
	args := []any{stream}
	for k, v := range fields {
		args = append(args, k, v)
	}
	// Build the SQL with placeholders
	placeholders := "$1" // stream name
	for i := 1; i < len(args); i++ {
		placeholders += fmt.Sprintf(", $%d", i+1)
	}
	q := fmt.Sprintf("SELECT STREAM_XADD(%s)", placeholders)
	var id string
	err := s.pool.QueryRow(ctx, q, args...).Scan(&id)
	return id, wrapErr("stream xadd", err)
}

// XLen returns the number of entries in a stream.
func (s *StreamModel) XLen(ctx context.Context, stream string) (int64, error) {
	if err := s.client.requireNucleus("Streams.XLen"); err != nil {
		return 0, err
	}
	var n int64
	err := s.pool.QueryRow(ctx, "SELECT STREAM_XLEN($1)", stream).Scan(&n)
	return n, wrapErr("stream xlen", err)
}

// XRange returns entries in a stream between start and end timestamps (inclusive).
func (s *StreamModel) XRange(ctx context.Context, stream string, startMs, endMs int64, count int64) ([]StreamEntry, error) {
	if err := s.client.requireNucleus("Streams.XRange"); err != nil {
		return nil, err
	}
	var raw string
	err := s.pool.QueryRow(ctx, "SELECT STREAM_XRANGE($1, $2, $3, $4)",
		stream, startMs, endMs, count).Scan(&raw)
	if err != nil {
		return nil, wrapErr("stream xrange", err)
	}
	var entries []StreamEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("nucleus: stream xrange unmarshal: %w", err)
	}
	return entries, nil
}

// XRead reads new entries from a stream after the given ID.
func (s *StreamModel) XRead(ctx context.Context, stream string, lastIDMs int64, count int64) ([]StreamEntry, error) {
	if err := s.client.requireNucleus("Streams.XRead"); err != nil {
		return nil, err
	}
	var raw string
	err := s.pool.QueryRow(ctx, "SELECT STREAM_XREAD($1, $2, $3)",
		stream, lastIDMs, count).Scan(&raw)
	if err != nil {
		return nil, wrapErr("stream xread", err)
	}
	var entries []StreamEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("nucleus: stream xread unmarshal: %w", err)
	}
	return entries, nil
}

// XGroupCreate creates a consumer group on a stream.
func (s *StreamModel) XGroupCreate(ctx context.Context, stream, group string, startID int64) (bool, error) {
	if err := s.client.requireNucleus("Streams.XGroupCreate"); err != nil {
		return false, err
	}
	var ok bool
	err := s.pool.QueryRow(ctx, "SELECT STREAM_XGROUP_CREATE($1, $2, $3)",
		stream, group, startID).Scan(&ok)
	return ok, wrapErr("stream xgroup_create", err)
}

// XReadGroup reads entries from a consumer group.
func (s *StreamModel) XReadGroup(ctx context.Context, stream, group, consumer string, count int64) ([]StreamEntry, error) {
	if err := s.client.requireNucleus("Streams.XReadGroup"); err != nil {
		return nil, err
	}
	var raw string
	err := s.pool.QueryRow(ctx, "SELECT STREAM_XREADGROUP($1, $2, $3, $4)",
		stream, group, consumer, count).Scan(&raw)
	if err != nil {
		return nil, wrapErr("stream xreadgroup", err)
	}
	var entries []StreamEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("nucleus: stream xreadgroup unmarshal: %w", err)
	}
	return entries, nil
}

// XAck acknowledges processing of a stream entry in a consumer group.
func (s *StreamModel) XAck(ctx context.Context, stream, group string, idMs, idSeq int64) (bool, error) {
	if err := s.client.requireNucleus("Streams.XAck"); err != nil {
		return false, err
	}
	var ok bool
	err := s.pool.QueryRow(ctx, "SELECT STREAM_XACK($1, $2, $3, $4)",
		stream, group, idMs, idSeq).Scan(&ok)
	return ok, wrapErr("stream xack", err)
}
