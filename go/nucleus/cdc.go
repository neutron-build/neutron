package nucleus

import (
	"context"
)

// CDCModel provides Change Data Capture operations over Nucleus SQL functions.
type CDCModel struct {
	pool   querier
	client *Client
}

// defaultCDCLimit is used when a non-positive limit is passed to Read or
// TableRead.
const defaultCDCLimit = 100

// Read reads up to limit CDC events with sequence greater than afterSeq.
// A non-positive limit defaults to 100. Returns raw CDC event data as a
// JSON array of {"seq","table","change","ts"} objects.
func (c *CDCModel) Read(ctx context.Context, afterSeq, limit int64) (string, error) {
	if err := c.client.requireNucleus("CDC.Read"); err != nil {
		return "", err
	}
	if limit <= 0 {
		limit = defaultCDCLimit
	}
	var raw string
	err := c.pool.QueryRow(ctx, "SELECT CDC_READ($1, $2)", afterSeq, limit).Scan(&raw)
	return raw, wrapErr("cdc read", err)
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
// greater than afterSeq. A non-positive limit defaults to 100. Returns raw
// CDC event data as a JSON array of {"seq","table","change","ts"} objects.
func (c *CDCModel) TableRead(ctx context.Context, table string, afterSeq, limit int64) (string, error) {
	if err := c.client.requireNucleus("CDC.TableRead"); err != nil {
		return "", err
	}
	if limit <= 0 {
		limit = defaultCDCLimit
	}
	var raw string
	err := c.pool.QueryRow(ctx, "SELECT CDC_TABLE_READ($1, $2, $3)", table, afterSeq, limit).Scan(&raw)
	return raw, wrapErr("cdc table_read", err)
}
