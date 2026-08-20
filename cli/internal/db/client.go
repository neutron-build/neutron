// Package db provides database communication via pgwire.
package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Client wraps a pgx connection pool for database operations.
type Client struct {
	pool *pgxpool.Pool
	url  string
}

// Connect creates a new database client.
func Connect(ctx context.Context, url string) (*Client, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	// Verify connectivity
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &Client{pool: pool, url: url}, nil
}

// Close closes the connection pool.
func (c *Client) Close() {
	c.pool.Close()
}

// Exec executes a SQL statement.
func (c *Client) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := c.pool.Exec(ctx, sql, args...)
	return err
}

// Query executes a SQL query and returns rows.
func (c *Client) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return c.pool.Query(ctx, sql, args...)
}

// QueryRow executes a query returning a single row.
func (c *Client) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return c.pool.QueryRow(ctx, sql, args...)
}

// IsNucleus checks if the connected database is Nucleus.
// Returns isNucleus, version string, error.
// Detection parses SELECT VERSION() for the "Nucleus" marker — Nucleus embeds
// its name and version there (there is no separate NUCLEUS_VERSION() function).
func (c *Client) IsNucleus(ctx context.Context) (bool, string, error) {
	var version string
	err := c.pool.QueryRow(ctx, "SELECT VERSION()").Scan(&version)
	if err != nil {
		return false, "", err
	}

	if strings.Contains(version, "Nucleus") {
		return true, parseNucleusVersion(version), nil
	}

	return false, version, nil
}

// nucleusModels is the fixed set of models every Nucleus build ships. Nucleus
// exposes no runtime feature-flag function, so all models are assumed present
// when connected to Nucleus.
var nucleusModels = []string{
	"sql", "kv", "vector", "timeseries", "document", "graph",
	"fts", "geo", "blob", "streams", "columnar", "datalog", "cdc", "pubsub",
}

// NucleusFeatures returns per-model feature flags for the connected Nucleus.
// Nucleus has no feature-flag function, so this returns all models enabled.
// Returns nil if not connected to Nucleus.
func (c *Client) NucleusFeatures(ctx context.Context) (map[string]bool, error) {
	isNucleus, _, err := c.IsNucleus(ctx)
	if err != nil {
		return nil, err
	}
	if !isNucleus {
		return nil, nil
	}

	features := make(map[string]bool, len(nucleusModels))
	for _, m := range nucleusModels {
		features[m] = true
	}
	return features, nil
}

// Status returns database status information.
func (c *Client) Status(ctx context.Context) (*StatusInfo, error) {
	info := &StatusInfo{URL: c.url}

	var version string
	if err := c.pool.QueryRow(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return nil, err
	}
	info.Version = version
	info.IsNucleus = strings.Contains(version, "Nucleus")

	if info.IsNucleus {
		info.NucleusVersion = parseNucleusVersion(version)
	}

	// Get current time for uptime display
	var serverTime time.Time
	if err := c.pool.QueryRow(ctx, "SELECT now()").Scan(&serverTime); err == nil {
		info.ServerTime = serverTime
	}

	return info, nil
}

// StatusInfo holds database status information.
type StatusInfo struct {
	URL            string
	Version        string
	IsNucleus      bool
	NucleusVersion string
	ServerTime     time.Time
}

// parseNucleusVersion extracts the Nucleus version from the VERSION() string.
// Example: "PostgreSQL 16.0 (Nucleus 0.1.0 — The Definitive Database)" -> "0.1.0"
func parseNucleusVersion(version string) string {
	idx := strings.Index(version, "Nucleus ")
	if idx < 0 {
		return ""
	}
	rest := version[idx+len("Nucleus "):]
	// Find end of version (space or dash or paren)
	end := strings.IndexAny(rest, " —)")
	if end < 0 {
		return rest
	}
	return rest[:end]
}
