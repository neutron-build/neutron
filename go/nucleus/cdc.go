package nucleus

import (
	"context"
)

// CDCModel provides Change Data Capture operations over Nucleus SQL functions.
type CDCModel struct {
	pool   querier
	client *Client
}

// Read reads CDC events starting from the given offset.
// Returns raw CDC event data as a JSON string.
func (c *CDCModel) Read(ctx context.Context, offset int64) (string, error) {
	if err := c.client.requireNucleus("CDC.Read"); err != nil {
		return "", err
	}
	var raw string
	err := c.pool.QueryRow(ctx, "SELECT CDC_READ($1)", offset).Scan(&raw)
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

// TableRead reads CDC events for a specific table starting from the given offset.
// Returns raw CDC event data as a JSON string.
func (c *CDCModel) TableRead(ctx context.Context, table string, offset int64) (string, error) {
	if err := c.client.requireNucleus("CDC.TableRead"); err != nil {
		return "", err
	}
	var raw string
	err := c.pool.QueryRow(ctx, "SELECT CDC_TABLE_READ($1, $2)", table, offset).Scan(&raw)
	return raw, wrapErr("cdc table_read", err)
}
