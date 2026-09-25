package studio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// SQL editor execution (S04): request-scoped statements, server-side
// cancellation and EXPLAIN with explicit execution semantics.
//
// Every editor statement runs on ONE dedicated pool connection, registered
// under a request ID before the statement is sent. The connection's backend
// PID (from the startup handshake) is therefore known up front, and
// POST /api/query/cancel signals exactly that backend with
// pg_cancel_backend over a separate short-lived connection (the pool may be
// exhausted by the very statements being cancelled). A cancel is only ever
// signalled while the registered statement is in flight: the registration
// lock is held across the signal, and the executing handler must take the
// same lock to mark the statement settled before it touches the connection
// again. After a cancelled statement the same backend is probed before it
// returns to the pool, so the pool only ever receives a usable connection.
//
// EXPLAIN never executes the statement: it runs as EXPLAIN (FORMAT JSON)
// inside a READ ONLY transaction that is always rolled back. EXPLAIN
// ANALYZE executes the statement. By default it also runs READ ONLY and
// rolled back, and statements that are writes by their leading keyword are
// refused before anything is sent; the read-only transaction catches the
// rest (data-modifying CTEs, nextval, write functions). Executing writes
// requires allowWrites, and even then the transaction is rolled back —
// Studio never commits from EXPLAIN.

const (
	// maxQueryBody bounds one editor request (SQL text plus parameters).
	maxQueryBody = 4 << 20
	// maxQueryParams bounds the bound-parameter list.
	maxQueryParams = 10000
	// cancelHardStop is how long a signalled statement may keep running
	// before its connection is closed instead (and discarded by the pool).
	cancelHardStop = 15 * time.Second
	// cancelSignalTimeout bounds one pg_cancel_backend round trip.
	cancelSignalTimeout = 5 * time.Second
)

// cancelResendAfter lists when an in-flight cancel is signalled again: a
// signal that reaches a backend still reading the statement from the socket
// is ignored by PostgreSQL, so a late re-signal closes that race.
var cancelResendAfter = []time.Duration{300 * time.Millisecond, 1500 * time.Millisecond}

// requestIDPattern is the accepted client-supplied request ID shape.
var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{8,128}$`)

// errCanceledBeforeDispatch is the outcome of a cancel that arrived before
// the statement was sent: the statement never reached the server.
var errCanceledBeforeDispatch = errors.New("canceled before the statement was sent; nothing was executed")

// errHardStopped reports a statement whose connection was closed instead of
// cancelled: it ignored the cancel signal for the whole grace period, or
// the engine gave no backend to signal.
var errHardStopped = fmt.Errorf("canceled by closing the statement's connection (the cancel signal was not honored within %s, or the engine cannot be signalled); the connection was discarded", cancelHardStop)

// runningQuery is one registered editor statement.
type runningQuery struct {
	mu     sync.Mutex
	connID string
	client *db.Client
	pid    uint32
	// dispatched: the statement has been (or is being) sent. settled: it has
	// returned. Cancel signals are sent only strictly between the two.
	dispatched      bool
	settled         bool
	cancelRequested bool
	hardStopped     bool
	// hardStop interrupts the statement context (pgx then closes the
	// connection) — the last resort when a cancel signal is ignored.
	hardStop context.CancelFunc
	// abortAcquire stops waiting for a pool slot; nil once acquired.
	abortAcquire context.CancelFunc
}

// queryRegistry maps request IDs to running editor statements.
type queryRegistry struct {
	mu      sync.Mutex
	running map[string]*runningQuery
}

func (s *Server) queryRegistry() *queryRegistry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queries == nil {
		s.queries = &queryRegistry{running: map[string]*runningQuery{}}
	}
	return s.queries
}

// editorExec is one registered execution on a dedicated connection.
type editorExec struct {
	s    *Server
	id   string
	rq   *runningQuery
	conn *pgxpool.Conn
	// ctx is the statement context. It survives the HTTP request context
	// (a disconnect cancels server-side instead of killing the connection)
	// and is interrupted only by the hard stop.
	ctx       context.Context
	stopWatch func() bool
}

// startEditorExec registers requestID, then acquires a dedicated connection.
// The ID is reserved first, so a duplicate ID is refused (409) without
// waiting for a pool slot, and a request still queued for a slot can be
// cancelled (it then never runs).
func (s *Server) startEditorExec(r *http.Request, client *db.Client, connID, requestID string) (*editorExec, int, error) {
	acqCtx, abortAcquire := context.WithTimeout(r.Context(), 30*time.Second)
	defer abortAcquire()
	ctx, hardStop := context.WithCancel(context.WithoutCancel(r.Context()))
	rq := &runningQuery{connID: connID, client: client, hardStop: hardStop, abortAcquire: abortAcquire}
	reg := s.queryRegistry()
	reg.mu.Lock()
	if _, dup := reg.running[requestID]; dup {
		reg.mu.Unlock()
		hardStop()
		return nil, http.StatusConflict, fmt.Errorf("request id %q is already running", requestID)
	}
	reg.running[requestID] = rq
	reg.mu.Unlock()
	unregister := func() {
		reg.mu.Lock()
		if reg.running[requestID] == rq {
			delete(reg.running, requestID)
		}
		reg.mu.Unlock()
		hardStop()
	}

	conn, err := client.Acquire(acqCtx)
	if err != nil {
		rq.mu.Lock()
		canceled := rq.cancelRequested
		rq.settled = true
		rq.mu.Unlock()
		unregister()
		if canceled {
			return nil, 0, errCanceledBeforeDispatch
		}
		return nil, http.StatusBadGateway, fmt.Errorf("acquire connection: %s", sanitizeError(err))
	}
	rq.mu.Lock()
	rq.pid = conn.Conn().PgConn().PID()
	rq.abortAcquire = nil
	rq.mu.Unlock()

	e := &editorExec{s: s, id: requestID, rq: rq, conn: conn, ctx: ctx}
	// A browser that goes away (tab closed, fetch aborted) cancels its
	// statement on the server as well.
	e.stopWatch = context.AfterFunc(r.Context(), func() { s.cancelRunning(connID, requestID) })
	return e, 0, nil
}

// dispatch marks the statement as being sent. It returns false when a
// cancel arrived first; the statement must then not be sent at all.
func (e *editorExec) dispatch() bool {
	e.rq.mu.Lock()
	defer e.rq.mu.Unlock()
	if e.rq.cancelRequested {
		return false
	}
	e.rq.dispatched = true
	return true
}

// settle marks the statement as returned. It blocks while a cancel signal is
// being delivered, so no signal is sent after this point.
func (e *editorExec) settle() {
	e.rq.mu.Lock()
	e.rq.settled = true
	e.rq.mu.Unlock()
}

// finish unregisters the request and returns the connection to the pool.
// After a cancel the same backend is probed first; a connection that fails
// the probe is closed so the pool discards it. reused reports the probe.
func (e *editorExec) finish() (canceled, hardStopped, reused bool) {
	e.stopWatch()
	e.settle()
	e.rq.mu.Lock()
	canceled = e.rq.cancelRequested
	hardStopped = e.rq.hardStopped
	e.rq.mu.Unlock()
	reg := e.s.queryRegistry()
	reg.mu.Lock()
	if reg.running[e.id] == e.rq {
		delete(reg.running, e.id)
	}
	reg.mu.Unlock()
	e.rq.hardStop()

	reused = true
	if canceled {
		reused = probeConnection(e.conn)
	}
	e.conn.Release()
	return canceled, hardStopped, reused
}

// probeConnection verifies a connection after a cancel with a trivial
// statement on the same backend. A late cancel signal can only land on the
// probe itself (PostgreSQL ignores signals while idle), so a 57014 is
// retried; any other failure closes the connection so the pool drops it.
func probeConnection(conn *pgxpool.Conn) bool {
	for attempt := 0; attempt < 3; attempt++ {
		if conn.Conn().IsClosed() {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var one int
		err := conn.QueryRow(ctx, "SELECT 1").Scan(&one)
		cancel()
		if err == nil && one == 1 {
			return true
		}
		if !isQueryCanceled(err) {
			break
		}
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn.Conn().Close(closeCtx) //nolint
	return false
}

// cancelOutcome is the answer to a cancel request.
type cancelOutcome struct {
	State  string // sent | canceled-before-dispatch | not-running | failed | unsupported
	Method string
	Err    error
}

// cancelRunning requests cancellation of a registered statement. The
// registration lock is held while the signal is delivered, so the signal
// can only reach the backend while that statement is in flight.
func (s *Server) cancelRunning(connID, requestID string) cancelOutcome {
	reg := s.queryRegistry()
	reg.mu.Lock()
	rq := reg.running[requestID]
	reg.mu.Unlock()
	if rq == nil || rq.connID != connID {
		return cancelOutcome{State: "not-running"}
	}

	rq.mu.Lock()
	defer rq.mu.Unlock()
	if rq.settled {
		return cancelOutcome{State: "not-running"}
	}
	first := !rq.cancelRequested
	rq.cancelRequested = true
	if !rq.dispatched {
		if rq.abortAcquire != nil {
			rq.abortAcquire()
		}
		return cancelOutcome{State: "canceled-before-dispatch"}
	}
	if rq.pid == 0 {
		// No backend key: nothing to signal. Close the statement's
		// connection instead (what the driver would do on a cancelled
		// context) and say so — the server may still finish the statement.
		rq.hardStopped = true
		rq.hardStop()
		return cancelOutcome{State: "unsupported", Err: errors.New(
			"the engine reported no backend process id for this connection, so server-side cancellation is unavailable; Studio closed the statement's connection instead (the server may still finish the statement)")}
	}
	err := rq.signalLocked()
	if first {
		// Re-signal and, as a last resort, hard-stop — also when this first
		// signal failed (e.g. the side connection could not be opened).
		go rq.followUp()
	}
	if err != nil {
		return cancelOutcome{State: "failed", Method: "pg_cancel_backend", Err: err}
	}
	return cancelOutcome{State: "sent", Method: "pg_cancel_backend"}
}

// signalLocked delivers one pg_cancel_backend. Caller holds rq.mu.
func (rq *runningQuery) signalLocked() error {
	ctx, cancel := context.WithTimeout(context.Background(), cancelSignalTimeout)
	defer cancel()
	ok, err := rq.client.CancelBackend(ctx, rq.pid)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("pg_cancel_backend(%d) returned false: the backend could not be signalled", rq.pid)
	}
	return nil
}

// followUp re-signals a statement that is still in flight (closing the
// signal-before-read race) and, as a last resort, hard-stops it.
func (rq *runningQuery) followUp() {
	start := time.Now()
	for _, after := range cancelResendAfter {
		time.Sleep(time.Until(start.Add(after)))
		rq.mu.Lock()
		if rq.settled {
			rq.mu.Unlock()
			return
		}
		if err := rq.signalLocked(); err != nil {
			log.Printf("studio: cancel re-signal for backend %d: %v", rq.pid, sanitizeError(err))
		}
		rq.mu.Unlock()
	}
	for time.Since(start) < cancelHardStop {
		time.Sleep(100 * time.Millisecond)
		rq.mu.Lock()
		settled := rq.settled
		rq.mu.Unlock()
		if settled {
			return
		}
	}
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if !rq.settled {
		rq.hardStopped = true
		rq.hardStop()
	}
}

// isQueryCanceled reports a PostgreSQL query_canceled (57014) error.
func isQueryCanceled(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "57014"
}

// sqlStateOf returns the SQLSTATE of a server error, or "".
func sqlStateOf(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// resolveRequestID validates a client-supplied request ID, or mints one.
func resolveRequestID(supplied string) (string, error) {
	if supplied == "" {
		tok, err := newSessionToken()
		if err != nil {
			return "", fmt.Errorf("request id: %w", err)
		}
		return "srv-" + tok[:24], nil
	}
	if !requestIDPattern.MatchString(supplied) {
		return "", fmt.Errorf("requestId must be 8-128 characters of [A-Za-z0-9_-]")
	}
	return supplied, nil
}

// decodeQueryParams converts the request's bound parameters ($1..$n) to
// driver values. Strings and numbers are sent as text so PostgreSQL parses
// them into the parameter's inferred type (never a float64 detour); null is
// SQL NULL; tagged wire cells decode exactly. JSON documents are passed as
// strings, arrays as PostgreSQL array literals.
func decodeQueryParams(raw []any) ([]any, error) {
	if len(raw) > maxQueryParams {
		return nil, fmt.Errorf("at most %d parameters are allowed", maxQueryParams)
	}
	out := make([]any, len(raw))
	for i, v := range raw {
		switch val := v.(type) {
		case nil:
			out[i] = nil
		case string:
			out[i] = val
		case bool:
			out[i] = val
		case json.Number:
			out[i] = val.String()
		case map[string]any:
			cell, ok := taggedCellOf(val)
			if !ok {
				return nil, fmt.Errorf("parameter $%d: objects must be {\"t\",\"v\"} wire cells; pass JSON documents as strings", i+1)
			}
			d, err := decodeTagged(cell)
			if err != nil {
				return nil, fmt.Errorf("parameter $%d: %v", i+1, err)
			}
			out[i] = d
		default:
			return nil, fmt.Errorf("parameter $%d: unsupported value; pass arrays as PostgreSQL array literals such as '{1,2}'", i+1)
		}
	}
	return out, nil
}

// --- POST /api/query ---

type queryRequest struct {
	SQL          string `json:"sql"`
	ConnectionID string `json:"connectionId"`
	Params       []any  `json:"params"`
	RequestID    string `json:"requestId"`
}

// handleQuery executes arbitrary editor SQL (including mutations, so it is
// guarded like the row endpoints) as one registered, cancellable request.
// SQL errors keep the endpoint's documented 200 + {"error"} convention, now
// with sqlState/canceled/requestId; request-shape errors are 400.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireMutationAuth(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxQueryBody)
	var body queryRequest
	if err := decodeStrictJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	requestID, err := resolveRequestID(body.RequestID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	args, err := decodeQueryParams(body.Params)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	client, ok := s.clientFor(body.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected — call /api/connections/:id/connect first")
		return
	}
	w.Header().Set("X-Request-Id", requestID)

	exec, status, err := s.startEditorExec(r, client, body.ConnectionID, requestID)
	if errors.Is(err, errCanceledBeforeDispatch) {
		writeJSON(w, http.StatusOK, map[string]any{
			"columns": []string{}, "rows": [][]any{}, "rowCount": 0, "duration": 0,
			"error": err.Error(), "requestId": requestID, "canceled": true,
		})
		return
	}
	if err != nil {
		writeJSON(w, status, map[string]any{"error": err.Error(), "requestId": requestID})
		return
	}

	start := time.Now()
	var result *taggedResult
	var qerr error
	if exec.dispatch() {
		rows, err := exec.conn.Query(exec.ctx, body.SQL, args...)
		if err == nil {
			result, qerr = collectTaggedRowsCapped(rows, maxEditorResultRows)
			rows.Close()
			if qerr == nil && result != nil && result.truncated {
				// Rows past the cap were drained and discarded by Close; a
				// statement that failed after the cap still reports its error.
				qerr = rows.Err()
			}
		} else {
			qerr = err
		}
		exec.settle()
	} else {
		qerr = errCanceledBeforeDispatch
	}
	canceled, hardStopped, reused := exec.finish()
	duration := time.Since(start).Milliseconds()

	// S05: the duration log behind the slow-query diagnosis view. Statement
	// text as submitted; bound parameters are never recorded.
	logState := "ok"
	if errors.Is(qerr, errCanceledBeforeDispatch) || hardStopped || (canceled && isQueryCanceled(qerr)) {
		logState = "canceled"
	} else if qerr != nil {
		logState = "error"
	}
	rowCount := 0
	if result != nil {
		rowCount = len(result.data)
	}
	s.recordStatement(loggedStatement{
		At: time.Now(), Connection: body.ConnectionID, Surface: "editor",
		RequestID: requestID, SQL: body.SQL, DurationMs: float64(time.Since(start).Microseconds()) / 1000,
		RowCount: rowCount, State: logState,
		Error: errorTextFor(qerr),
	})

	if qerr != nil {
		if hardStopped {
			qerr = errHardStopped
		}
		if !errors.Is(qerr, errCanceledBeforeDispatch) && !hardStopped {
			log.Printf("studio: query error: %v", sanitizeError(qerr))
		}
		cols := []string{}
		if result != nil && result.columns != nil {
			cols = result.columns
		}
		resp := map[string]any{
			"columns":   cols,
			"rows":      [][]any{},
			"rowCount":  0,
			"duration":  duration,
			"error":     sanitizeError(qerr),
			"requestId": requestID,
			"canceled":  canceled && (isQueryCanceled(qerr) || hardStopped || errors.Is(qerr, errCanceledBeforeDispatch)),
		}
		if code := sqlStateOf(qerr); code != "" {
			resp["sqlState"] = code
		}
		if canceled {
			resp["connectionReused"] = reused
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}
	data := result.data
	if data == nil {
		data = [][]any{}
	}
	resp := map[string]any{
		"columns":   result.columns,
		"rows":      data,
		"rowCount":  len(data),
		"duration":  duration,
		"requestId": requestID,
	}
	if result.truncated {
		resp["truncated"] = true
		resp["rowLimit"] = maxEditorResultRows
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- POST /api/query/cancel ---

// handleQueryCancel cancels a running editor statement by request ID. The
// request ID only addresses statements on the named connection.
func (s *Server) handleQueryCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireMutationAuth(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	var body struct {
		ConnectionID string `json:"connectionId"`
		RequestID    string `json:"requestId"`
	}
	if err := decodeStrictJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if !requestIDPattern.MatchString(body.RequestID) {
		writeError(w, http.StatusBadRequest, "requestId must be 8-128 characters of [A-Za-z0-9_-]")
		return
	}
	out := s.cancelRunning(body.ConnectionID, body.RequestID)
	resp := map[string]any{"requestId": body.RequestID, "state": out.State}
	if out.Method != "" {
		resp["method"] = out.Method
	}
	switch out.State {
	case "not-running":
		resp["error"] = "no running statement with this request id on this connection (it may already have finished)"
		writeJSON(w, http.StatusNotFound, resp)
	case "failed":
		resp["error"] = fmt.Sprintf("cancel signal failed: %s (Studio re-signals, and closes the statement's connection if it is still running after %s)", sanitizeError(out.Err), cancelHardStop)
		writeJSON(w, http.StatusBadGateway, resp)
	case "unsupported":
		resp["error"] = out.Err.Error()
		writeJSON(w, http.StatusUnprocessableEntity, resp)
	default:
		writeJSON(w, http.StatusOK, resp)
	}
}

// --- POST /api/query/explain ---

type explainRequest struct {
	SQL          string `json:"sql"`
	ConnectionID string `json:"connectionId"`
	Params       []any  `json:"params"`
	RequestID    string `json:"requestId"`
	// Analyze executes the statement (EXPLAIN ANALYZE).
	Analyze bool `json:"analyze"`
	// AllowWrites lets EXPLAIN ANALYZE execute a writing statement inside a
	// read-write transaction that is still rolled back afterwards.
	AllowWrites bool `json:"allowWrites"`
}

// explainableKeywords are the leading keywords of statements PostgreSQL can
// EXPLAIN. "(" is a parenthesized query. CREATE is passed through for
// CREATE TABLE AS / CREATE MATERIALIZED VIEW; the server decides.
var explainableKeywords = map[string]bool{
	"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "MERGE": true,
	"VALUES": true, "TABLE": true, "WITH": true, "EXECUTE": true, "DECLARE": true,
	"CREATE": true, "(": true,
}

// writeKeywords are leading keywords of statements that write by definition.
// EXPLAIN ANALYZE refuses them without allowWrites before sending anything;
// writes hidden deeper (CTEs, functions) hit the read-only transaction.
var writeKeywords = map[string]bool{
	"INSERT": true, "UPDATE": true, "DELETE": true, "MERGE": true, "CREATE": true,
}

// writeBlockedMessage explains a refused EXPLAIN ANALYZE of a write.
const writeBlockedMessage = "EXPLAIN ANALYZE executes the statement, and this statement writes. " +
	"Nothing was executed or written. To measure it anyway, allow writes: the statement then " +
	"runs for real inside a transaction that Studio rolls back (sequence increments and effects " +
	"outside the database are not undone)."

// handleQueryExplain renders a statement's plan. Without analyze the
// statement is planned, never executed. With analyze it is executed with
// the guards described at the top of this file. The plan is PostgreSQL's
// FORMAT JSON document, passed through unmodified.
func (s *Server) handleQueryExplain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.requireMutationAuth(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxQueryBody)
	var body explainRequest
	if err := decodeStrictJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	requestID, err := resolveRequestID(body.RequestID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.AllowWrites && !body.Analyze {
		writeError(w, http.StatusBadRequest, "allowWrites applies only to EXPLAIN ANALYZE (plain EXPLAIN never executes the statement)")
		return
	}
	args, err := decodeQueryParams(body.Params)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	stmt := trimStatement(body.SQL)
	if stmt == "" {
		writeError(w, http.StatusBadRequest, "sql is required")
		return
	}
	kw := leadingKeyword(stmt)
	if kw == "EXPLAIN" {
		writeError(w, http.StatusBadRequest, "the statement is already an EXPLAIN; remove the prefix and choose Explain or Analyze (a hand-written EXPLAIN ANALYZE executes the statement)")
		return
	}
	w.Header().Set("X-Request-Id", requestID)
	unprocessable := func(state, msg string, extra map[string]any) {
		resp := map[string]any{"error": msg, "state": state, "requestId": requestID, "analyze": body.Analyze}
		for k, v := range extra {
			resp[k] = v
		}
		writeJSON(w, http.StatusUnprocessableEntity, resp)
	}
	if !explainableKeywords[kw] {
		unprocessable("unsupported", fmt.Sprintf(
			"PostgreSQL has no query plan for a %s statement; EXPLAIN covers SELECT, INSERT, UPDATE, DELETE, MERGE, VALUES, TABLE, WITH, EXECUTE, DECLARE and CREATE TABLE AS", displayKeyword(kw)), nil)
		return
	}
	if body.Analyze && !body.AllowWrites && writeKeywords[kw] {
		unprocessable("write-blocked", writeBlockedMessage, map[string]any{"executed": false})
		return
	}
	client, ok := s.clientFor(body.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected — call /api/connections/:id/connect first")
		return
	}
	isNucleus, _, err := client.IsNucleus(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "engine detection failed: "+sanitizeError(err))
		return
	}
	if isNucleus {
		// No conformance evidence yet for Nucleus EXPLAIN (plan shape,
		// read-only transactions, non-execution of mutations), so Studio
		// does not offer it rather than guessing its semantics.
		unprocessable("unsupported",
			"EXPLAIN is not available for Nucleus connections in Studio: the engine's plan format and its read-only guarantees are not yet verified by the conformance suite",
			map[string]any{"engine": "nucleus"})
		return
	}

	exec, status, err := s.startEditorExec(r, client, body.ConnectionID, requestID)
	if errors.Is(err, errCanceledBeforeDispatch) {
		unprocessable("canceled", err.Error(), map[string]any{"committed": false})
		return
	}
	if err != nil {
		writeJSON(w, status, map[string]any{"error": err.Error(), "requestId": requestID})
		return
	}

	options := "FORMAT JSON"
	if body.Analyze {
		options = "ANALYZE, BUFFERS, FORMAT JSON"
	}
	explainSQL := "EXPLAIN (" + options + ") " + stmt
	access := pgx.ReadOnly
	if body.AllowWrites {
		access = pgx.ReadWrite
	}

	start := time.Now()
	var plan []byte
	var qerr error
	if exec.dispatch() {
		plan, qerr = runExplain(exec, access, explainSQL, args)
	} else {
		qerr = errCanceledBeforeDispatch
	}
	canceled, hardStopped, reused := exec.finish()
	duration := time.Since(start).Milliseconds()

	if qerr != nil {
		extra := map[string]any{"duration": duration, "committed": false}
		if code := sqlStateOf(qerr); code != "" {
			extra["sqlState"] = code
		}
		if canceled {
			extra["connectionReused"] = reused
			if isQueryCanceled(qerr) || hardStopped || errors.Is(qerr, errCanceledBeforeDispatch) {
				msg := sanitizeError(qerr)
				if hardStopped {
					msg = errHardStopped.Error()
				}
				unprocessable("canceled", msg, extra)
				return
			}
		}
		if body.Analyze && !body.AllowWrites && sqlStateOf(qerr) == "25006" {
			extra["executed"] = false
			unprocessable("write-blocked", writeBlockedMessage+" (PostgreSQL: "+sanitizeError(qerr)+")", extra)
			return
		}
		unprocessable("sql-error", sanitizeError(qerr), extra)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"requestId":     requestID,
		"engine":        "postgresql",
		"format":        "json",
		"plan":          json.RawMessage(plan),
		"analyze":       body.Analyze,
		"executed":      body.Analyze,
		"writesAllowed": body.AllowWrites,
		"readOnly":      !body.AllowWrites,
		"committed":     false,
		"duration":      duration,
	})
}

// runExplain runs the EXPLAIN inside a transaction that is always rolled
// back. The statement is sent through the extended protocol (Parse), which
// refuses multiple commands, so text after the statement can never run
// outside the EXPLAIN.
func runExplain(exec *editorExec, access pgx.TxAccessMode, explainSQL string, args []any) ([]byte, error) {
	tx, err := exec.conn.BeginTx(exec.ctx, pgx.TxOptions{AccessMode: access})
	if err != nil {
		exec.settle()
		return nil, err
	}
	var plan []byte
	queryArgs := append([]any{pgx.QueryExecModeDescribeExec}, args...)
	rows, err := tx.Query(exec.ctx, explainSQL, queryArgs...)
	if err == nil {
		for rows.Next() {
			raw := rows.RawValues()
			if len(raw) > 0 && plan == nil {
				plan = append([]byte(nil), raw[0]...)
			}
		}
		rows.Close()
		err = rows.Err()
	}
	exec.settle()
	rbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A failed rollback makes pgx close the connection; the server then
	// aborts the transaction, so nothing is committed either way.
	_ = tx.Rollback(rbCtx)
	if err != nil {
		return nil, err
	}
	if plan == nil || !json.Valid(plan) {
		return nil, fmt.Errorf("the server returned no JSON plan")
	}
	return plan, nil
}

// trimStatement strips surrounding whitespace and trailing semicolons.
func trimStatement(sql string) string {
	s := strings.TrimSpace(sql)
	for strings.HasSuffix(s, ";") {
		s = strings.TrimSpace(strings.TrimSuffix(s, ";"))
	}
	return s
}

// leadingKeyword returns the first keyword of a statement, upper-cased,
// skipping whitespace and -- / nested /* */ comments. A leading "(" (a
// parenthesized query) is returned as "(".
func leadingKeyword(sql string) string {
	i := 0
	n := len(sql)
	for i < n {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < n && sql[i+1] == '-':
			for i < n && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*':
			depth := 0
			for i < n {
				if i+1 < n && sql[i] == '/' && sql[i+1] == '*' {
					depth++
					i += 2
				} else if i+1 < n && sql[i] == '*' && sql[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
				} else {
					i++
				}
			}
		case c == '(':
			return "("
		default:
			j := i
			for j < n && (sql[j] == '_' || (sql[j] >= 'a' && sql[j] <= 'z') || (sql[j] >= 'A' && sql[j] <= 'Z')) {
				j++
			}
			return strings.ToUpper(sql[i:j])
		}
	}
	return ""
}

// displayKeyword renders a keyword for an error message.
func displayKeyword(kw string) string {
	if kw == "" {
		return "non-keyword"
	}
	return kw
}
