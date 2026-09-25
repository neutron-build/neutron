package studio

// S05 performance diagnosis: a duration log of every statement this Studio
// process executed (the server-side analogue of I02's structured query
// events), an honest slow-query view over it, and per-table/index
// statistics from PostgreSQL's own cumulative statistics system.
//
// Boundaries, stated honestly in every response:
//   - the duration log covers ONLY statements executed through this Studio
//     server process since its launch (in-memory, bounded); it is not a
//     server-wide statement history;
//   - statement TEXT is recorded as submitted (truncated), but bound
//     parameters are never recorded;
//   - pg_stat_statements surfaces server-wide history only when the
//     extension exists AND the role may read it (superuser /
//     pg_read_all_stats) — otherwise the endpoint says exactly why it is
//     unavailable instead of pretending;
//   - table statistics come from pg_stat_user_tables / pg_stat_user_indexes
//     / pg_indexes; counters are cumulative since the last statistics reset.

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// --- statement duration log ---

// loggedStatement is one recorded execution.
type loggedStatement struct {
	At         time.Time `json:"at"`
	Connection string    `json:"connectionId"`
	Surface    string    `json:"surface"` // editor | table-read
	RequestID  string    `json:"requestId,omitempty"`
	SQL        string    `json:"sql"`
	DurationMs float64   `json:"durationMs"`
	RowCount   int       `json:"rowCount,omitempty"`
	State      string    `json:"state"` // ok | error | canceled
	Error      string    `json:"error,omitempty"`

	// epoch is the connection binding the statement ran on: reconnecting a
	// connection id (possibly to another database) starts a fresh log view.
	epoch string
}

const (
	statementLogCapacity = 500
	statementTextCap     = 2000
)

// statementLog is a bounded ring of recent executions.
type statementLog struct {
	mu   sync.Mutex
	logs []loggedStatement
}

func (l *statementLog) record(entry loggedStatement) {
	if len(entry.SQL) > statementTextCap {
		entry.SQL = entry.SQL[:statementTextCap] + " …"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.logs == nil {
		l.logs = make([]loggedStatement, 0, statementLogCapacity)
	}
	l.logs = append(l.logs, entry)
	if len(l.logs) > statementLogCapacity {
		l.logs = l.logs[len(l.logs)-statementLogCapacity:]
	}
}

func (l *statementLog) snapshot(connectionID, epoch string, minMs float64, limit int) []loggedStatement {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]loggedStatement, 0, len(l.logs))
	for _, e := range l.logs {
		if e.Connection != connectionID || e.epoch != epoch {
			continue
		}
		if e.DurationMs < minMs {
			continue
		}
		out = append(out, e)
	}
	// Newest first.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (s *Server) statementLog() *statementLog {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.statements == nil {
		s.statements = &statementLog{}
	}
	return s.statements
}

// recordStatement appends one execution to the duration log (S05). The
// entry carries the statement text as submitted (bound parameters are
// separate and never recorded) and the outcome state.
func (s *Server) recordStatement(entry loggedStatement) {
	entry.epoch = s.connectionEpoch(entry.Connection)
	s.statementLog().record(entry)
}

// errorTextFor renders an execution error for the duration log (empty for
// success), sanitized.
func errorTextFor(err error) string {
	if err == nil {
		return ""
	}
	msg := sanitizeError(err)
	if len(msg) > 200 {
		msg = msg[:200]
	}
	return msg
}

// --- GET /api/diagnostics/queries ---

// handleDiagnosticsQueries returns this process's recorded statement
// executions (slow-query identification) plus a pg_stat_statements probe so
// the view can say what server-wide evidence exists and what does not.
func (s *Server) handleDiagnosticsQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	connID := q.Get("connectionId")
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	minMs := parseFloat(q.Get("minMs"), 0)
	limit := parseInt(q.Get("limit"), 200)
	if limit <= 0 || limit > statementLogCapacity {
		limit = statementLogCapacity
	}

	epoch := s.connectionEpoch(connID)
	entries := s.statementLog().snapshot(connID, epoch, minMs, limit)
	// Percentiles over the unfiltered per-connection history (the log, not
	// the filtered slice) so the summary does not change with the view's
	// threshold.
	all := s.statementLog().snapshot(connID, epoch, 0, 0)
	durations := make([]float64, 0, len(all))
	for _, e := range all {
		if e.State == "ok" || e.State == "error" {
			durations = append(durations, e.DurationMs)
		}
	}
	sort.Float64s(durations)
	pct := func(p float64) float64 {
		if len(durations) == 0 {
			return 0
		}
		idx := p / 100 * float64(len(durations)-1)
		return durations[int(idx)]
	}
	maxOf := func(vals []float64) float64 {
		if len(vals) == 0 {
			return 0
		}
		return vals[len(vals)-1]
	}

	resp := map[string]any{
		"entries": entries,
		"stats": map[string]any{
			"count": len(durations),
			"p50Ms": pct(50),
			"p95Ms": pct(95),
			"maxMs": maxOf(durations),
		},
		"scope": "statements executed through this Studio server process on this connection since it was connected (in-memory, bounded at " + strconv.Itoa(statementLogCapacity) + " entries); durations are wall-clock as measured by Studio, including network and result transfer; statement text as submitted, bound parameters never recorded",
	}

	// pg_stat_statements probe: server-wide history, only with the
	// extension AND the privilege to read it.
	if isNucleus, _, err := client.IsNucleus(r.Context()); err == nil && !isNucleus {
		probe, reason := pgStatStatementsOverview(r.Context(), client)
		if probe != nil {
			resp["pgStatStatements"] = probe
		} else {
			resp["pgStatStatements"] = map[string]any{
				"available": false,
				"reason":    reason,
			}
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// pgStatStatementsOverview returns the top statements by total execution
// time, or nil + an honest reason when the extension or the privilege is
// missing.
func pgStatStatementsOverview(ctx context.Context, client *db.Client) (map[string]any, string) {
	var ext bool
	if err := client.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')`).Scan(&ext); err != nil {
		return nil, "the pg_stat_statements extension could not be probed: " + sanitizeError(err)
	}
	if !ext {
		return nil, "the pg_stat_statements extension is not installed on this server; server-wide statement statistics need it (superuser: CREATE EXTENSION pg_stat_statements, plus shared_preload_libraries and a restart)"
	}
	rows, err := client.Query(ctx, `
		SELECT query, calls, round(total_exec_time::numeric, 3), round(mean_exec_time::numeric, 3)
		FROM pg_stat_statements
		WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
		ORDER BY total_exec_time DESC
		LIMIT 20`)
	if err != nil {
		// Typical causes: the library is not in shared_preload_libraries
		// (the view then errors on read) or the role lacks access to the view.
		return nil, "the pg_stat_statements extension is installed but its statistics could not be read: " + sanitizeError(err)
	}
	defer rows.Close()
	type stmtRow struct {
		Query   string  `json:"query"`
		Calls   int64   `json:"calls"`
		TotalMs float64 `json:"totalMs"`
		MeanMs  float64 `json:"meanMs"`
	}
	list := []stmtRow{}
	for rows.Next() {
		var sr stmtRow
		if err := rows.Scan(&sr.Query, &sr.Calls, &sr.TotalMs, &sr.MeanMs); err != nil {
			return nil, "pg_stat_statements could not be read: " + sanitizeError(err)
		}
		list = append(list, sr)
	}
	if err := rows.Err(); err != nil {
		return nil, "pg_stat_statements could not be read: " + sanitizeError(err)
	}
	return map[string]any{
		"available":  true,
		"statements": list,
		"note":       "server-wide, per-database, since the last statistics reset; reading other users' statements requires superuser or pg_read_all_stats",
	}, ""
}

// --- GET /api/diagnostics/table-stats ---

type indexStatRow struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
	Unique     bool   `json:"unique"`
	IdxScan    int64  `json:"idxScan"`
	IdxTupRead int64  `json:"idxTupRead"`
	SizeBytes  int64  `json:"sizeBytes"`
}

// handleDiagnosticsTableStats returns one table's usage statistics and its
// indexes' definitions and usage (index diagnostics), honest about what the
// connected role can and cannot see.
func (s *Server) handleDiagnosticsTableStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	connID := q.Get("connectionId")
	schemaName := q.Get("schema")
	tableName := q.Get("table")
	if connID == "" || schemaName == "" || tableName == "" {
		writeError(w, http.StatusBadRequest, "connectionId, schema and table are required")
		return
	}
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	if isNucleus, _, err := client.IsNucleus(r.Context()); err == nil && isNucleus {
		writeError(w, http.StatusUnprocessableEntity,
			"table statistics use PostgreSQL's cumulative statistics system (pg_stat_user_tables), which is not verified on Nucleus (X00)")
		return
	}

	stats, err := fetchTableStats(r.Context(), client, schemaName, tableName)
	if err != nil {
		log.Printf("studio: table stats error: %v", err)
		writeError(w, http.StatusUnprocessableEntity, sanitizeError(err))
		return
	}
	if stats == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": "no statistics row for " + schemaName + "." + tableName + " — the relation does not exist, is not a plain table, or is not visible to this role; refresh the schema tree",
		})
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// fetchTableStats reads the statistics rows. A missing pg_stat row (absent
// table or no privilege) yields nil, nil.
func fetchTableStats(ctx context.Context, client *db.Client, schemaName, tableName string) (map[string]any, error) {
	var (
		relOID                                                        uint32
		seqScan, idxScan, idxTupFetch, nLive, nDead, nModSinceAnalyze int64
		lastAnalyze, lastAutoAnalyze, lastVacuum, lastAutoVacuum      *time.Time
	)
	// idx_scan / idx_tup_fetch are NULL for a table without indexes; the
	// other counters are COALESCEd defensively. The relation is resolved by
	// quoted identity (to_regclass over format('%I.%I')), so mixed-case and
	// dotted names address exactly one relation.
	err := client.QueryRow(ctx, `
		SELECT relid::oid, COALESCE(seq_scan, 0), COALESCE(idx_scan, 0), COALESCE(idx_tup_fetch, 0),
		       COALESCE(n_live_tup, 0), COALESCE(n_dead_tup, 0), COALESCE(n_mod_since_analyze, 0),
		       last_analyze, last_autoanalyze, last_vacuum, last_autovacuum
		FROM pg_stat_user_tables
		WHERE relid = to_regclass(format('%I.%I', $1::text, $2::text))`, schemaName, tableName).Scan(
		&relOID, &seqScan, &idxScan, &idxTupFetch, &nLive, &nDead, &nModSinceAnalyze,
		&lastAnalyze, &lastAutoAnalyze, &lastVacuum, &lastAutoVacuum)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	var relSize, totalSize int64
	sizeOK := true
	if err := client.QueryRow(ctx, `
		SELECT pg_relation_size($1::oid), pg_total_relation_size($1::oid)`,
		relOID).Scan(&relSize, &totalSize); err != nil {
		sizeOK = false
	}

	idxRows, err := client.Query(ctx, `
		SELECT ci.relname, pg_get_indexdef(i.indexrelid), i.indisunique,
		       COALESCE(s.idx_scan, 0), COALESCE(s.idx_tup_read, 0),
		       COALESCE(pg_relation_size(i.indexrelid), 0)
		FROM pg_index i
		JOIN pg_class ci ON ci.oid = i.indexrelid
		LEFT JOIN pg_stat_user_indexes s
		       ON s.indexrelid = i.indexrelid AND s.relid = i.indrelid
		WHERE i.indrelid = $1::oid
		ORDER BY ci.relname`, relOID)
	if err != nil {
		return nil, err
	}
	defer idxRows.Close()
	indexes := []indexStatRow{}
	for idxRows.Next() {
		var row indexStatRow
		if err := idxRows.Scan(&row.Name, &row.Definition, &row.Unique, &row.IdxScan, &row.IdxTupRead, &row.SizeBytes); err != nil {
			return nil, err
		}
		indexes = append(indexes, row)
	}
	if err := idxRows.Err(); err != nil {
		return nil, err
	}

	out := map[string]any{
		"schema": schemaName,
		"table":  tableName,
		"stats": map[string]any{
			"seqScan":          seqScan,
			"idxScan":          idxScan,
			"idxTupFetch":      idxTupFetch,
			"nLiveTup":         nLive,
			"nDeadTup":         nDead,
			"nModSinceAnalyze": nModSinceAnalyze,
			"lastAnalyze":      nullableTime(lastAnalyze),
			"lastAutoAnalyze":  nullableTime(lastAutoAnalyze),
			"lastVacuum":       nullableTime(lastVacuum),
			"lastAutoVacuum":   nullableTime(lastAutoVacuum),
		},
		"indexes": indexes,
		"notes": []string{
			"counters are cumulative since the last statistics reset (pg_stat_reset) or server restart",
			"an index with zero scans is a candidate for review only — it may serve write-time uniqueness or rare queries; check the reset time before dropping anything",
		},
	}
	if sizeOK {
		out["relSizeBytes"] = relSize
		out["totalSizeBytes"] = totalSize
	} else {
		out["sizeUnavailableReason"] = "pg_relation_size/pg_total_relation_size was not readable for this relation by this role"
	}
	return out, nil
}

func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.Format(time.RFC3339)
}

func parseFloat(s string, def float64) float64 {
	if s == "" {
		return def
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return def
	}
	return v
}
