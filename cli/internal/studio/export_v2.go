package studio

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Streamed table export (S06), served under /api/table/v2/export.
//
// An export is requested in two steps so that it passes the S01 session and
// exact-origin checks AND streams straight to disk through the browser's
// native download machinery (no accumulation in the SPA):
//
//  1. POST /api/table/v2/export (session token + exact origin, like every
//     mutation-class request) validates the table, format, filters, sorts
//     and match tuple against the live catalog and returns a single-use,
//     short-lived ticket.
//  2. GET /api/table/v2/export/download?ticket=... redeems the ticket once
//     (browser-marked cross-site requests are refused) and streams the rows
//     with Content-Disposition: attachment.
//
// Memory is bounded: one SELECT runs inside a READ ONLY transaction and rows
// are encoded one at a time through a fixed-size buffer that is flushed to
// the client as it fills; nothing accumulates per row. The client going away
// cancels the request context, which cancels the statement and returns the
// connection to the pool. An error after the first byte aborts the HTTP
// response (http.ErrAbortHandler) instead of ending it cleanly, so a
// truncated export is reported by the browser as a failed download — never a
// silently short file that looks complete.
//
// Cell text is PostgreSQL's own ::text output under pinned output settings
// (ISO dates, hex bytea, postgres interval style), so values are exact: int8
// and numeric digits, timestamp microseconds and time-zone offsets are never
// routed through a float or a client-side formatter.
//
// CSV follows PostgreSQL COPY ... CSV conventions so NULL and the empty
// string stay distinct: SQL NULL is an unquoted empty field, the empty
// string is "" (quoted). Fields containing the delimiter, a quote, CR or LF,
// or leading/trailing whitespace are quoted with doubled inner quotes.
//
// JSON (an array) and NDJSON (one object per line) carry SQL NULL as null;
// booleans as true/false; integer, numeric and float columns as JSON number
// literals holding PostgreSQL's exact digits (NaN/Infinity, which JSON cannot
// express as numbers, as strings); json/jsonb as embedded JSON; every other
// type as its text in a JSON string. Readers that parse numbers as IEEE
// doubles (JavaScript's JSON.parse) round integers beyond 2^53 — use a
// lossless reader (the Studio importer is one) for exact round-trips.

const (
	// exportTicketTTL bounds how long a validated export may wait for its
	// download to start.
	exportTicketTTL = 2 * time.Minute
	// maxPendingExports bounds tickets held in memory; the oldest pending
	// ticket is dropped (and must be requested again) beyond this.
	maxPendingExports = 32
	// exportBufferSize is the per-download encode buffer: the only memory
	// that scales with the export, independent of the row count.
	exportBufferSize = 64 << 10
	// exportFlushRows flushes the response at least this often even when
	// the buffer has room, so slow tables still stream visibly.
	exportFlushRows = 500
)

type exportRequestV2 struct {
	ConnectionID string          `json:"connectionId"`
	Schema       string          `json:"schema"`
	Table        string          `json:"table"`
	Format       string          `json:"format"`
	Filters      json.RawMessage `json:"filters,omitempty"`
	Sorts        json.RawMessage `json:"sorts,omitempty"`
	Match        json.RawMessage `json:"match,omitempty"`
}

// exportJob is a validated export waiting for its download.
type exportJob struct {
	connID   string
	format   string
	sql      string
	args     []any
	columns  []tableColumnMeta
	filename string
	created  time.Time
}

// exportTicketStore holds validated exports until their single download.
type exportTicketStore struct {
	mu    sync.Mutex
	ttl   time.Duration
	max   int
	jobs  map[string]*exportJob
	order []string
}

func newExportTicketStore(ttl time.Duration, max int) *exportTicketStore {
	return &exportTicketStore{ttl: ttl, max: max, jobs: map[string]*exportJob{}}
}

func (e *exportTicketStore) pruneLocked(now time.Time) {
	kept := e.order[:0]
	for _, t := range e.order {
		job, ok := e.jobs[t]
		if !ok {
			continue
		}
		if now.Sub(job.created) > e.ttl {
			delete(e.jobs, t)
			continue
		}
		kept = append(kept, t)
	}
	e.order = kept
}

func (e *exportTicketStore) put(ticket string, job *exportJob) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pruneLocked(job.created)
	for len(e.order) >= e.max {
		delete(e.jobs, e.order[0])
		e.order = e.order[1:]
	}
	e.jobs[ticket] = job
	e.order = append(e.order, ticket)
}

// take redeems a ticket exactly once; expired or unknown tickets are nil.
func (e *exportTicketStore) take(ticket string, now time.Time) *exportJob {
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.jobs[ticket]
	if !ok {
		return nil
	}
	delete(e.jobs, ticket)
	for i, t := range e.order {
		if t == ticket {
			e.order = append(e.order[:i], e.order[i+1:]...)
			break
		}
	}
	if now.Sub(job.created) > e.ttl {
		return nil
	}
	return job
}

func (s *Server) exportTickets() *exportTicketStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exports == nil {
		s.exports = newExportTicketStore(exportTicketTTL, maxPendingExports)
	}
	return s.exports
}

var exportFormats = map[string]struct{ ext, mime string }{
	"csv":    {"csv", "text/csv; charset=utf-8"},
	"json":   {"json", "application/json; charset=utf-8"},
	"ndjson": {"ndjson", "application/x-ndjson; charset=utf-8"},
}

var unsafeFilenameChars = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// exportFilename renders an ASCII-safe attachment name for the table.
func exportFilename(schemaName, tableName, ext string) string {
	base := unsafeFilenameChars.ReplaceAllString(schemaName+"."+tableName, "_")
	base = strings.Trim(base, "._")
	if base == "" {
		base = "export"
	}
	return base + "." + ext
}

// exportColumnExpr renders one column's exact text form. bytea is spelled
// out as \x-hex so a session-level bytea_output=escape cannot change it.
func exportColumnExpr(col tableColumnMeta) string {
	if col.TypeOID == oidBytea {
		return fmt.Sprintf(`'\x' || encode(%s, 'hex')`, quoteIdent(col.Name))
	}
	return quoteIdent(col.Name) + "::text"
}

// buildExportQuery renders the streamed SELECT for a validated export: the
// same filter/sort/match grammar (and strictness) as GET /api/table, the
// columns in attnum order as exact text, and a deterministic order — user
// sorts first, then the primary key as tiebreaker (the key alone when no
// sort is given) — so two exports of an unchanged table are identical.
func buildExportQuery(meta *tableMeta, schemaName, tableName string, filters, sorts, match json.RawMessage) (string, []any, error) {
	var conds []string
	var args []any
	if raw := rawParam(filters); raw != "" {
		c, a, err := parseTableFilters(raw, meta, true, len(args))
		if err != nil {
			return "", nil, err
		}
		conds, args = append(conds, c...), append(args, a...)
	}
	if raw := rawParam(match); raw != "" {
		c, a, err := parseMatch(raw, meta, true, len(args))
		if err != nil {
			return "", nil, err
		}
		conds, args = append(conds, c...), append(args, a...)
	}
	var order []string
	sorted := map[string]bool{}
	if raw := rawParam(sorts); raw != "" {
		parts, err := parseTableSorts(raw, tableName, meta, true)
		if err != nil {
			return "", nil, err
		}
		order = append(order, parts...)
		var keys []tableSort
		_ = json.Unmarshal([]byte(raw), &keys) // already validated above
		for _, k := range keys {
			sorted[k.Column] = true
		}
	}
	order = append(order, keyTiebreakers(meta, tableName, sorted)...)

	selects := make([]string, 0, len(meta.Order))
	for _, col := range meta.Order {
		selects = append(selects, exportColumnExpr(col))
	}
	q := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(selects, ", "), quoteIdent(schemaName), quoteIdent(tableName))
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	if len(order) > 0 {
		q += " ORDER BY " + strings.Join(order, ", ")
	}
	return q, args, nil
}

// rawParam turns an optional JSON field into the raw text the shared
// parsers take; JSON null counts as absent.
func rawParam(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return ""
	}
	return s
}

// keyTiebreakers returns ascending primary-key order terms for key columns
// not already sorted on: the unique tail that makes paging and exports
// deterministic. Tables without a primary key get none (no unique order
// exists to append).
func keyTiebreakers(meta *tableMeta, tableName string, already map[string]bool) []string {
	if meta == nil {
		return nil
	}
	var out []string
	for _, pk := range meta.PKCols {
		if already[pk] {
			continue
		}
		out = append(out, fmt.Sprintf("%s.%s ASC", quoteIdent(tableName), quoteIdent(pk)))
	}
	return out
}

func (s *Server) handleTableExportV2(w http.ResponseWriter, r *http.Request) {
	var body exportRequestV2
	if !s.readMutationBody(w, r, &body) {
		return
	}
	if body.ConnectionID == "" || body.Schema == "" || body.Table == "" {
		writeError(w, http.StatusBadRequest, "connectionId, schema and table are required")
		return
	}
	format, ok := exportFormats[body.Format]
	if !ok {
		writeError(w, http.StatusBadRequest, `format must be "csv", "json" or "ndjson"`)
		return
	}
	client, ok := s.clientFor(body.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	meta, err := fetchTableMeta(r.Context(), client, body.Schema, body.Table)
	if err != nil {
		log.Printf("studio: export introspection error: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": sanitizeError(err)})
		return
	}
	if !meta.Exists {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("%s.%s was not found or is not an ordinary table", body.Schema, body.Table))
		return
	}
	sqlText, args, err := buildExportQuery(meta, body.Schema, body.Table, body.Filters, body.Sorts, body.Match)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ticket, err := newSessionToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not mint an export ticket")
		return
	}
	job := &exportJob{
		connID:   body.ConnectionID,
		format:   body.Format,
		sql:      sqlText,
		args:     args,
		columns:  append([]tableColumnMeta(nil), meta.Order...),
		filename: exportFilename(body.Schema, body.Table, format.ext),
		created:  time.Now(),
	}
	s.exportTickets().put(ticket, job)
	writeJSON(w, http.StatusOK, map[string]any{
		"ticket":    ticket,
		"url":       "/api/table/v2/export/download?ticket=" + ticket,
		"filename":  job.filename,
		"format":    body.Format,
		"expiresIn": int(exportTicketTTL.Seconds()),
	})
}

// exportSessionSettings pins the text output of every exported type for
// the export's own transaction (SET LOCAL never leaks to the pool).
const exportSessionSettings = `SET TRANSACTION READ ONLY; ` +
	`SET LOCAL DateStyle = 'ISO, YMD'; SET LOCAL IntervalStyle = 'postgres'; ` +
	`SET LOCAL bytea_output = 'hex'; SET LOCAL extra_float_digits = 1`

func (s *Server) handleTableExportDownloadV2(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.checkOrigin(r) {
		writeAuthError(w, "origin", "origin not allowed")
		return
	}
	job := s.exportTickets().take(r.URL.Query().Get("ticket"), time.Now())
	if job == nil {
		writeError(w, http.StatusNotFound, "unknown, expired or already used export ticket — request the export again")
		return
	}
	client, ok := s.clientFor(job.connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}

	ctx := r.Context()
	tx, err := client.BeginTx(ctx)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": sanitizeError(err)})
		return
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck
	if _, err := tx.Exec(ctx, exportSessionSettings); err != nil {
		// An engine without these settings (or SET TRANSACTION) still
		// exports, in its own default text forms, inside a fresh transaction.
		tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck
		tx, err = client.BeginTx(ctx)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": sanitizeError(err)})
			return
		}
		defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck
	}

	rows, err := tx.Query(ctx, job.sql, job.args...)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": sanitizeError(err)})
		return
	}
	defer rows.Close()
	// Surface a statement that fails before producing any row as a plain
	// error response; nothing has been written yet.
	hasFirst := rows.Next()
	if !hasFirst {
		if err := rows.Err(); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": sanitizeError(err)})
			return
		}
	}

	format := exportFormats[job.format]
	h := w.Header()
	h.Set("Content-Type", format.mime)
	h.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, job.filename))
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	enc := newRowEncoder(w, job.format, job.columns)
	if err := streamExportRows(rows, hasFirst, enc); err != nil {
		if ctx.Err() == nil {
			log.Printf("studio: export aborted: %v", sanitizeError(err))
		}
		// Abort the response: the browser reports a failed download
		// instead of keeping a truncated file that looks complete.
		panic(http.ErrAbortHandler)
	}
}

// streamExportRows encodes every row of an already-started result.
func streamExportRows(rows pgx.Rows, hasFirst bool, enc *rowEncoder) error {
	if err := enc.begin(); err != nil {
		return err
	}
	vals := make([]*string, len(enc.columns))
	dest := make([]any, len(vals))
	for i := range vals {
		dest[i] = &vals[i]
	}
	n := 0
	for ok := hasFirst; ok; ok = rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		if err := enc.row(vals); err != nil {
			return err
		}
		n++
		if n%exportFlushRows == 0 {
			if err := enc.flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return enc.end()
}

// rowEncoder writes one export format through a bounded buffer.
type rowEncoder struct {
	out     *bufio.Writer
	flusher http.Flusher
	format  string
	columns []tableColumnMeta
	keys    [][]byte // pre-encoded JSON member names ("name":)
	rows    int
}

func newRowEncoder(w io.Writer, format string, columns []tableColumnMeta) *rowEncoder {
	enc := &rowEncoder{out: bufio.NewWriterSize(w, exportBufferSize), format: format, columns: columns}
	if f, ok := w.(http.Flusher); ok {
		enc.flusher = f
	}
	if format != "csv" {
		enc.keys = make([][]byte, len(columns))
		for i, col := range columns {
			name, _ := json.Marshal(col.Name)
			enc.keys[i] = append(name, ':')
		}
	}
	return enc
}

func (e *rowEncoder) begin() error {
	switch e.format {
	case "csv":
		for i, col := range e.columns {
			if i > 0 {
				e.out.WriteByte(',')
			}
			writeCSVField(e.out, &col.Name)
		}
		_, err := e.out.WriteString("\n")
		return err
	case "json":
		_, err := e.out.WriteString("[")
		return err
	}
	return nil
}

func (e *rowEncoder) row(vals []*string) error {
	switch e.format {
	case "csv":
		for i, v := range vals {
			if i > 0 {
				e.out.WriteByte(',')
			}
			writeCSVField(e.out, v)
		}
		e.out.WriteByte('\n')
	default:
		if e.format == "json" {
			if e.rows > 0 {
				e.out.WriteByte(',')
			}
			e.out.WriteByte('\n')
		}
		e.out.WriteByte('{')
		for i, v := range vals {
			if i > 0 {
				e.out.WriteByte(',')
			}
			e.out.Write(e.keys[i])
			writeJSONCell(e.out, e.columns[i], v)
		}
		e.out.WriteByte('}')
		if e.format == "ndjson" {
			e.out.WriteByte('\n')
		}
	}
	e.rows++
	// A failed underlying write is sticky in bufio; the periodic flush()
	// surfaces it and aborts the stream.
	return nil
}

func (e *rowEncoder) end() error {
	if e.format == "json" {
		if e.rows > 0 {
			e.out.WriteByte('\n')
		}
		e.out.WriteString("]\n")
	}
	return e.flush()
}

func (e *rowEncoder) flush() error {
	if err := e.out.Flush(); err != nil {
		return err
	}
	if e.flusher != nil {
		e.flusher.Flush()
	}
	return nil
}

// writeCSVField writes one CSV field: nil is SQL NULL (unquoted empty),
// the empty string is "" (quoted), and fields needing protection are
// quoted with doubled inner quotes.
func writeCSVField(w *bufio.Writer, v *string) {
	if v == nil {
		return
	}
	s := *v
	if !csvNeedsQuotes(s) {
		w.WriteString(s)
		return
	}
	w.WriteByte('"')
	w.WriteString(strings.ReplaceAll(s, `"`, `""`))
	w.WriteByte('"')
}

func csvNeedsQuotes(s string) bool {
	if s == "" {
		return true
	}
	if strings.ContainsAny(s, ",\"\r\n") {
		return true
	}
	first, last := s[0], s[len(s)-1]
	return first == ' ' || first == '\t' || last == ' ' || last == '\t'
}

// jsonNumber matches the RFC 8259 number grammar; PostgreSQL's numeric
// text that matches it is emitted verbatim as a number literal.
var jsonNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

const (
	oidBool   = 16
	oidInt2   = 21
	oidInt4   = 23
	oidOID    = 26
	oidFloat4 = 700
	oidFloat8 = 701
)

func writeJSONCell(w *bufio.Writer, col tableColumnMeta, v *string) {
	if v == nil {
		w.WriteString("null")
		return
	}
	s := *v
	switch col.TypeOID {
	case oidBool:
		switch s {
		case "t", "true":
			w.WriteString("true")
			return
		case "f", "false":
			w.WriteString("false")
			return
		}
	case oidInt2, oidInt4, oidInt8, oidOID, oidNumeric, oidFloat4, oidFloat8:
		if jsonNumber.MatchString(s) {
			w.WriteString(s)
			return
		}
	case oidJSON, oidJSONB:
		if json.Valid([]byte(s)) {
			w.WriteString(s)
			return
		}
	}
	b, _ := json.Marshal(s)
	w.Write(b)
}
