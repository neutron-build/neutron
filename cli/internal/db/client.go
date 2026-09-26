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

// ExecTag executes a SQL statement and returns the affected row count.
func (c *Client) ExecTag(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := c.pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// hasExecutableSQL reports whether a plan entry contains anything besides
// SQL comments/whitespace. It reads the single tokenizer's token stream:
// both `--` line comments and (nested) /* */ block comments are comments,
// so a comment-only fragment — however spelled — is never executed.
func hasExecutableSQL(stmt string) bool {
	for _, t := range tokenizeSQL(stmt) {
		if t.kind != 'c' {
			return true
		}
	}
	return false
}

// HasExecutableSQL is the exported form of hasExecutableSQL for the
// command layer's statement filters.
func HasExecutableSQL(stmt string) bool {
	return hasExecutableSQL(stmt)
}

// firstSQLLine returns the first non-comment line of a statement for error
// messages.
func firstSQLLine(stmt string) string {
	for _, line := range strings.Split(stmt, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "--") {
			return trimmed
		}
	}
	return strings.TrimSpace(stmt)
}

// BeginTx starts one transaction on a single pooled connection and hands
// the caller the pgx.Tx: every statement the caller runs through it shares
// that connection and transaction, so multi-statement units (e.g. Studio's
// atomic edit commits) execute all-or-nothing. The caller owns the
// lifecycle: Commit to persist, Rollback (a deferred Rollback is the usual
// pattern) to discard. Do NOT run BEGIN/COMMIT as plain Exec calls — pool
// queries may hop connections between statements.
func (c *Client) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return c.pool.Begin(ctx)
}

// ReadOnlyTx runs fn inside a READ ONLY transaction on one dedicated
// connection and always rolls back: inspection reads run under the
// server's own write refusal (PostgreSQL rejects writes with 25006).
// Engines that do not apply READ ONLY (Nucleus: capability report
// txn.read_only_rejects_writes) need a separate guard.
//
// discardSession: a rolled-back transaction does not undo session-level
// effects (a session advisory lock taken by the statement, a dblink
// connection, session settings). With discardSession the connection never
// returns to the pool: its advisory locks are released explicitly (so the
// release is synchronous, not left to backend exit) and it is closed.
func (c *Client) ReadOnlyTx(ctx context.Context, discardSession bool, fn func(pgx.Tx) error) error {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		if discardSession {
			closeConn(conn.Hijack())
		} else {
			conn.Release()
		}
		return err
	}
	fnErr := fn(tx)
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	rbErr := tx.Rollback(cleanup)
	if !discardSession {
		// The pool destroys a connection left mid-transaction or broken.
		conn.Release()
		if fnErr == nil && rbErr != nil {
			return rbErr
		}
		return fnErr
	}
	raw := conn.Hijack()
	_, _ = raw.Exec(cleanup, "SELECT pg_advisory_unlock_all()", pgx.QueryExecModeSimpleProtocol)
	closeConn(raw)
	if fnErr == nil && rbErr != nil {
		return rbErr
	}
	return fnErr
}

func closeConn(conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn.Close(ctx) //nolint:errcheck
}

// Query executes a SQL query and returns rows.
func (c *Client) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return c.pool.Query(ctx, sql, args...)
}

// Acquire checks out one dedicated pool connection. The caller owns it until
// Release: every statement on it runs on the same server backend, which is
// what request-scoped cancellation needs (the backend PID is known before
// the statement is sent).
func (c *Client) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return c.pool.Acquire(ctx)
}

// CancelBackend asks the server to cancel the statement currently running on
// backend pid, via pg_cancel_backend on a separate short-lived connection.
// The side channel deliberately bypasses the pool: a pool exhausted by long
// statements must still be able to cancel them. Returns the server's answer
// (false: no such backend, or it could not be signalled).
func (c *Client) CancelBackend(ctx context.Context, pid uint32) (bool, error) {
	// The pool's parsed connection config, not c.url: the URL may carry
	// pool-only parameters (pool_max_conns, …) that a plain connection would
	// send to the server as unknown runtime settings.
	conn, err := pgx.ConnectConfig(ctx, c.pool.Config().ConnConfig.Copy())
	if err != nil {
		return false, fmt.Errorf("cancel side connection: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		conn.Close(closeCtx) //nolint
	}()
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT pg_cancel_backend($1)", int32(pid)).Scan(&ok); err != nil {
		return false, err
	}
	return ok, nil
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
