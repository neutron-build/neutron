package nucleus

import (
	"context"
	"encoding/json"
)

// CDCModel provides Change Data Capture operations over Nucleus SQL functions.
type CDCModel struct {
	pool   querier
	client *Client
}

// CDCEvent is a single change-data-capture log entry as emitted by the engine.
//
// Read and TableRead returned the engine's raw JSON string and there was no
// event type at all, so every caller wrote its own unmarshalling — and the
// cross-SDK conformance case asserting a list passed against a non-empty
// string, because a non-empty string is truthy. Python and TypeScript both
// returned parsed events; Go and Rust did not.
type CDCEvent struct {
	// Seq is the monotonic sequence number of the change.
	Seq int64 `json:"seq"`
	// Table is the table the change applies to.
	Table string `json:"table"`
	// Change is the kind of change: INSERT, UPDATE or DELETE.
	Change string `json:"change"`
	// TS is the timestamp of the change, in epoch milliseconds.
	TS int64 `json:"ts"`
}

// defaultCDCLimit is used when a non-positive limit is passed to Read or
// TableRead.
const defaultCDCLimit = 100

// Read reads up to limit CDC events with sequence greater than afterSeq.
// A non-positive limit defaults to 100.
func (c *CDCModel) Read(ctx context.Context, afterSeq, limit int64) ([]CDCEvent, error) {
	if err := c.client.requireNucleus("CDC.Read"); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = defaultCDCLimit
	}
	var raw string
	if err := c.pool.QueryRow(ctx, "SELECT CDC_READ($1, $2)", afterSeq, limit).Scan(&raw); err != nil {
		return nil, wrapErr("cdc read", err)
	}
	return parseCDCEvents("cdc read", raw)
}

// Count returns the total number of CDC events.
func (c *CDCModel) Count(ctx context.Context) (int64, error) {
	if err := c.client.requireNucleus("CDC.Count"); err != nil {
		return 0, err
	}
	var n int64
	err := c.pool.QueryRow(ctx, "SELECT CDC_COUNT()").Scan(&n)
	return n, wrapErr("cdc count", err)
}

// TableRead reads up to limit CDC events for a specific table with sequence
// greater than afterSeq. A non-positive limit defaults to 100.
func (c *CDCModel) TableRead(ctx context.Context, table string, afterSeq, limit int64) ([]CDCEvent, error) {
	if err := c.client.requireNucleus("CDC.TableRead"); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = defaultCDCLimit
	}
	var raw string
	if err := c.pool.QueryRow(ctx, "SELECT CDC_TABLE_READ($1, $2, $3)", table, afterSeq, limit).Scan(&raw); err != nil {
		return nil, wrapErr("cdc table_read", err)
	}
	return parseCDCEvents("cdc table_read", raw)
}

// parseCDCEvents decodes the engine's event array.
//
// An empty result is an empty slice, never an error: "no changes since that
// sequence" is the common case, not a failure. A malformed payload IS an error
// rather than an empty slice, because silently returning "no changes" when the
// engine said something unparseable is the shape of bug this whole model exists
// to detect.
func parseCDCEvents(op, raw string) ([]CDCEvent, error) {
	if raw == "" {
		return []CDCEvent{}, nil
	}
	var events []CDCEvent
	if err := json.Unmarshal([]byte(raw), &events); err != nil {
		return nil, wrapErr(op+" decode", err)
	}
	if events == nil {
		events = []CDCEvent{}
	}
	return events, nil
}
