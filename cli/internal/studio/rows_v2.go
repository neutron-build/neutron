package studio

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// Versioned full-key row identities and the typed mutation protocol (S01),
// served under /api/table/v2/.
//
// A row identity is the tuple the table read hands out per row:
//
//	connectionId  the saved connection the row was read through
//	binding       "<connection epoch>:<relation oid>" — the live client
//	              instance (fresh random value per connect) and the exact
//	              pg_class relation. A reconnect, or a table dropped and
//	              recreated under the same name, invalidates every identity
//	              read before it (409 state "binding"), so an identity can
//	              never be replayed against a different database or relation.
//	schema, table the relation's qualified name (must still resolve to the
//	              bound relation oid)
//	key           the COMPLETE primary key tuple [{column, value}], values in
//	              wire form (tagged cells for int8/numeric/bytea/temporal)
//	version       the row's PostgreSQL xmin at read time
//
// xmin is the honest authoritative revision: it changes on every write to
// the row, requires no maintained counters, triggers or audit tables (no
// hidden DDL on databases whose owner never consented), and is readable by
// any role that can read the row. Caveats, accepted and documented: xmin is
// 32-bit and wraps (a false conflict is possible only after ~4 billion
// transactions — the failure direction is a spurious conflict, never a silent
// overwrite), and engines that do not expose xmin cannot use this protocol
// (their tables report versioned=false and stay read-only).
//
// Stale-row detection: the mutation's WHERE clause includes xmin::text =
// $version alongside the full key tuple, inside one guarded statement that
// also enforces exactly-one-row. A version mismatch never writes; the
// response distinguishes conflict (row exists, changed since read, with its
// current version) from missing (stale, deleted, or not visible —
// deliberately not distinguishing RLS-hidden from absent).
//
// Requests are validated strictly and identically whether they come from the
// SPA or a direct client: unknown JSON fields and trailing data are refused,
// key tuples must be exactly the introspected primary key, values must match
// the column's catalog wire shape (decodeColumnValue), and generated/
// identity/key columns are refused before any SQL runs.
//
// Status codes: 400 malformed or domain-invalid request, 403 auth failure
// ("auth") or missing privilege (state "privilege"), 409 row/relation state
// (state "binding" | "conflict" | "missing" | "constraint"), 502 backend
// failure, 200 success. The interim B04 endpoints keep their 200+error
// convention during the transition.

type keyCell struct {
	Column string `json:"column"`
	Value  any    `json:"value"`
}

type rowUpdateRequestV2 struct {
	ConnectionID string    `json:"connectionId"`
	Binding      string    `json:"binding"`
	Schema       string    `json:"schema"`
	Table        string    `json:"table"`
	Key          []keyCell `json:"key"`
	Version      string    `json:"version"`
	Column       string    `json:"column"`
	Value        any       `json:"value"`
	IsNull       bool      `json:"isNull"`
}

type rowDeleteRequestV2 struct {
	ConnectionID string    `json:"connectionId"`
	Binding      string    `json:"binding"`
	Schema       string    `json:"schema"`
	Table        string    `json:"table"`
	Key          []keyCell `json:"key"`
	Version      string    `json:"version"`
}

type rowInsertRequestV2 struct {
	ConnectionID string         `json:"connectionId"`
	Binding      string         `json:"binding"`
	Schema       string         `json:"schema"`
	Table        string         `json:"table"`
	Values       map[string]any `json:"values"`
}

// rowIdentityRef is the identity part shared by update and delete.
type rowIdentityRef struct {
	ConnectionID, Binding, Schema, Table, Version string
	Key                                           []keyCell
}

// mutationDomainError marks a request the catalog itself refuses (forged
// key, read-only column, read-only table, malformed value): reported as 400.
type mutationDomainError struct{ msg string }

func (e mutationDomainError) Error() string { return e.msg }

// rowStateError marks row/relation state failures: 409 with an explicit
// machine-readable state, never a silent overwrite.
type rowStateError struct {
	msg            string
	state          string // "binding" | "conflict" | "missing"
	currentVersion string
}

func (e rowStateError) Error() string { return e.msg }

// bindingFor renders the identity binding for a connection epoch and
// relation oid.
func bindingFor(epoch string, relOID uint32) string {
	return epoch + ":" + strconv.FormatUint(uint64(relOID), 10)
}

// identityKeyOIDs are the key column types whose wire form round-trips
// exactly and whose equality is safe to address a row with. Other key types
// (floats, time/interval, network, domains, composites, arrays, ...) make
// the table read-only until a proven strategy exists.
var identityKeyOIDs = map[uint32]bool{
	16:   true, // bool
	20:   true, // int8 (tagged)
	21:   true, // int2
	23:   true, // int4
	25:   true, // text
	1042: true, // bpchar
	1043: true, // varchar
	1700: true, // numeric (tagged)
	2950: true, // uuid (canonical string)
	17:   true, // bytea (tagged)
	1082: true, // date (tagged)
	1114: true, // timestamp (tagged)
	1184: true, // timestamptz (tagged)
}

func keyTypeSupported(col tableColumnMeta) bool {
	return identityKeyOIDs[col.TypeOID] || col.TypType == "e"
}

// editableReason returns "" when a column may be updated, else why not.
// Authoritative (catalog-backed): generated and identity columns are
// read-only, key columns are read-only because they address the row —
// changing a key is a different operation from editing a value — and a
// column the current role cannot UPDATE is read-only. Columns with a plain
// or serial default remain writable (a default only applies when a value is
// absent on insert).
func editableReason(col tableColumnMeta) string {
	switch {
	case col.Generated != "":
		return "generated column (computed by the database) is read-only"
	case col.Identity != "":
		return "identity column (assigned by the database) is read-only"
	case col.IsPK:
		return "key column is read-only (it addresses the row)"
	case !col.CanUpdate:
		return "the connected role has no UPDATE privilege on this column"
	}
	return ""
}

// insertableReason returns "" when a column may carry an explicit value in
// an insert request, else why not. Plain (non-generated) primary keys are
// insertable — supplying them is how keyed rows are created.
func insertableReason(col tableColumnMeta) string {
	switch {
	case col.Generated != "":
		return "generated column (computed by the database) cannot be inserted"
	case col.Identity != "":
		return "identity column (assigned by the database) cannot be inserted — omit it to use its default"
	case col.autoAssigned():
		return "auto-assigned column (serial default) cannot be inserted — omit it to use its default"
	case !col.CanInsert:
		return "the connected role has no INSERT privilege on this column"
	}
	return ""
}

// readOnlyState derives the table-level editing state from fresh
// introspection. A table is editable only when rows can be identified
// (primary key of supported types exists) and revisioned (xmin visible on
// this connection).
type readOnlyState struct {
	readOnly  bool
	reason    string
	versioned bool
}

func tableReadOnlyState(meta *tableMeta, versioned bool) readOnlyState {
	if !meta.Exists {
		return readOnlyState{readOnly: true, reason: "was not found or is not an ordinary table"}
	}
	if len(meta.PKCols) == 0 {
		return readOnlyState{readOnly: true, reason: "has no primary key — rows cannot be identified, so the table is read-only", versioned: versioned}
	}
	for _, pk := range meta.PKCols {
		col := meta.Columns[pk]
		if !keyTypeSupported(col) {
			return readOnlyState{readOnly: true, versioned: versioned, reason: fmt.Sprintf(
				"has key column %q of type %s, which Studio cannot yet compare exactly — the table is read-only", pk, col.TypeName)}
		}
	}
	if !versioned {
		return readOnlyState{readOnly: true, reason: "does not expose row versions on this connection (no xmin) — editing is disabled to prevent stale overwrites"}
	}
	return readOnlyState{versioned: true}
}

// probeVersioned reports whether xmin is usable on this table/connection.
// The probe selects zero rows, so empty tables are probed correctly.
// Degrading to false (and the table becoming read-only) is the fail-safe
// direction; a permission or engine surface that hides xmin never widens
// into unguarded editing.
func probeVersioned(ctx context.Context, client *db.Client, schemaName, tableName string) bool {
	rows, err := client.Query(ctx, fmt.Sprintf(
		"SELECT xmin::text FROM %s.%s LIMIT 0",
		quoteIdent(schemaName), quoteIdent(tableName),
	))
	if err != nil {
		return false
	}
	rows.Close()
	return rows.Err() == nil
}

// validateKeyTuple checks a request's key against the freshly introspected
// primary key: exactly the full tuple, no extras, no duplicates, no forged
// non-key columns, no NULLs. Values are decoded strictly for each key
// column's catalog type.
func validateKeyTuple(meta *tableMeta, key []keyCell) ([]any, []string, error) {
	if len(key) != len(meta.PKCols) {
		return nil, nil, mutationDomainError{msg: fmt.Sprintf(
			"key must address the full primary key (%s) with exactly %d column(s), got %d",
			strings.Join(meta.PKCols, ", "), len(meta.PKCols), len(key))}
	}
	pkSet := map[string]bool{}
	for _, pk := range meta.PKCols {
		pkSet[pk] = true
	}
	seen := map[string]bool{}
	var args []any
	var cols []string
	for _, cell := range key {
		if !pkSet[cell.Column] {
			return nil, nil, mutationDomainError{msg: fmt.Sprintf(
				"key column %q is not part of the primary key (%s)",
				cell.Column, strings.Join(meta.PKCols, ", "))}
		}
		if seen[cell.Column] {
			return nil, nil, mutationDomainError{msg: fmt.Sprintf("key column %q appears twice", cell.Column)}
		}
		seen[cell.Column] = true
		if cell.Value == nil {
			return nil, nil, mutationDomainError{msg: fmt.Sprintf("key column %q: NULL can never match a primary key", cell.Column)}
		}
		v, err := decodeColumnValue(meta.Columns[cell.Column], cell.Value)
		if err != nil {
			return nil, nil, mutationDomainError{msg: fmt.Sprintf("key column %q: %v", cell.Column, err)}
		}
		args = append(args, v)
		cols = append(cols, cell.Column)
	}
	return args, cols, nil
}

// keyPredicate builds `"a" = $n AND "b" = $n+1 ...` with 1-based
// placeholders starting at start, over the given args in order.
func keyPredicate(cols []string, start int) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s = $%d", quoteIdent(c), start+i)
	}
	return strings.Join(parts, " AND ")
}

// buildGuardedMutationV2 wraps a full-key + version-guarded mutation. The
// count subquery shares the statement snapshot with the write, so the write
// only fires when the key addresses exactly one row; the xmin comparison
// makes a stale revision write zero rows instead of clobbering. Placeholders
// are keys ($1..$k), version ($k+1), then the mutation's own value
// parameter ($k+2) when it has one.
func buildGuardedMutationV2(schemaName, tableName string, mut func(keyWhere string, valueParam int) string, keyArgs []any, keyCols []string, version string, extra ...any) (string, []any) {
	tableRef := fmt.Sprintf("%s.%s", quoteIdent(schemaName), quoteIdent(tableName))
	keyWhere := keyPredicate(keyCols, 1)
	sqlText := fmt.Sprintf(
		"%s WHERE (%s AND %s.xmin::text = $%d) AND (SELECT count(*) FROM %s WHERE %s) = 1",
		mut(keyWhere, len(keyArgs)+2), keyWhere, tableRef, len(keyArgs)+1, tableRef, keyWhere,
	)
	args := append(append([]any{}, keyArgs...), version)
	args = append(args, extra...)
	return sqlText, args
}

// runGuardedMutation executes the guarded statement as one atomic statement
// (implicit transaction) and returns the affected count plus the row's new
// version. Runs identically on the pool or inside an explicit transaction.
func runGuardedMutation(ctx context.Context, q rowQuerier, sqlText string, args ...any) (int64, string, error) {
	var n int64
	var ver string
	wrapped := fmt.Sprintf(
		"WITH mutated AS (%s RETURNING xmin::text AS ver) SELECT count(*) AS n, COALESCE(max(ver),'') AS ver FROM mutated",
		sqlText,
	)
	err := q.QueryRow(ctx, wrapped, args...).Scan(&n, &ver)
	return n, ver, err
}

// explainRowConflictV2 classifies a zero-row mutation. A row found under
// the key with a different version is a conflict (report the current
// version so the UI can refresh its identity); a row not found is missing
// — the wording deliberately does not distinguish RLS-hidden from deleted.
func explainRowConflictV2(ctx context.Context, q rowQuerier, schemaName, tableName string, keyArgs []any, keyCols []string, verb string) error {
	tableRef := fmt.Sprintf("%s.%s", quoteIdent(schemaName), quoteIdent(tableName))
	keyWhere := keyPredicate(keyCols, 1)
	var current string
	err := q.QueryRow(ctx, fmt.Sprintf("SELECT xmin::text FROM %s WHERE %s", tableRef, keyWhere), keyArgs...).Scan(&current)
	if err == nil {
		return rowStateError{
			state:          "conflict",
			currentVersion: current,
			msg: fmt.Sprintf("%s refused: row changed since it was read (current row version %s); reload the row and reapply the edit",
				verb, current),
		}
	}
	return rowStateError{
		state: "missing",
		msg:   fmt.Sprintf("%s matched no row for this key: the row is stale, deleted, or not visible to this connection", verb),
	}
}

// resolvedTarget is a validated connection + relation for a v2 mutation.
type resolvedTarget struct {
	client *db.Client
	meta   *tableMeta
}

// rowQuerier is the single-row query surface *db.Client and pgx.Tx share,
// so the guarded-statement helpers run identically inside and outside a
// transaction.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// errNotConnected marks a connection the server has no client for.
type errNotConnected struct{}

func (errNotConnected) Error() string { return "not connected" }

// errIntrospection marks a catalog-read failure; the wrapped error is for
// the log, the sanitized one for the client.
type errIntrospection struct{ err error }

func (e errIntrospection) Error() string { return sanitizeError(e.err) }

// resolveTargetErr runs the checks every v2 mutation shares after auth and
// body decoding: connection lookup, fresh introspection, the read-only
// gate, then the binding match. The table-level domain checks come BEFORE
// the binding check (S01 review F1): a client probing a no-key or
// unsupported table gets the table's honest read-only reason even when its
// binding is also wrong — safety is identical (refused either way), but
// the reported cause is the more useful one. The binding check still runs
// last so identities from a reconnected client or a replaced relation are
// refused with state "binding" on tables that are otherwise editable.
func (s *Server) resolveTargetErr(ctx context.Context, connID, binding, schemaName, tableName string) (*resolvedTarget, error) {
	client, ok := s.clientFor(connID)
	if !ok {
		return nil, errNotConnected{}
	}
	meta, err := fetchTableMeta(ctx, client, schemaName, tableName)
	if err != nil {
		return nil, errIntrospection{err}
	}
	state := tableReadOnlyState(meta, false)
	if !meta.Exists {
		// Not-found is a read-only-class reason; no probe, no binding to check.
		return nil, mutationDomainError{msg: fmt.Sprintf("%s.%s %s", schemaName, tableName, state.reason)}
	}
	state = tableReadOnlyState(meta, probeVersioned(ctx, client, schemaName, tableName))
	if state.readOnly {
		return nil, mutationDomainError{msg: fmt.Sprintf("%s.%s %s", schemaName, tableName, state.reason)}
	}
	if bindingFor(s.connectionEpoch(connID), meta.RelOID) != binding {
		return nil, rowStateError{state: "binding", msg: fmt.Sprintf(
			"%s.%s is no longer the relation these rows were read from (reconnected, or the table was replaced); reload before editing",
			schemaName, tableName)}
	}
	return &resolvedTarget{client: client, meta: meta}, nil
}

// resolveTarget wraps resolveTargetErr with the HTTP response mapping.
// It writes the response and returns false on refusal.
func (s *Server) resolveTarget(w http.ResponseWriter, r *http.Request, connID, binding, schemaName, tableName string) (*resolvedTarget, bool) {
	target, err := s.resolveTargetErr(r.Context(), connID, binding, schemaName, tableName)
	if err != nil {
		switch e := err.(type) {
		case errNotConnected:
			writeError(w, http.StatusBadRequest, e.Error())
		case errIntrospection:
			log.Printf("studio: key introspection error: %v", e.err)
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": e.Error()})
		case mutationDomainError:
			writeDomainError(w, e)
		case rowStateError:
			writeMutationOutcome(w, e, "resolve target")
		default:
			log.Printf("studio: resolve target error: %v", err)
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": sanitizeError(err)})
		}
		return nil, false
	}
	return target, true
}

// readMutationBody applies the shared preamble: auth, method, size bound and
// strict decoding. It writes the response and returns false on refusal.
func (s *Server) readMutationBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	if !s.requireMutationAuth(w, r) {
		return false
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.mutationBodyLimit())
	if err := decodeStrictJSONBody(r, dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return false
	}
	return true
}

// mutationBodyLimit is the request-size bound for this launch (default
// maxMutationBody, lowered via env at launch).
func (s *Server) mutationBodyLimit() int64 {
	if s.maxMutBody > 0 {
		return s.maxMutBody
	}
	return maxMutationBody
}

// commitOpLimit bounds one commit's operation count (default
// maxCommitOperations, lowered via env at launch).
func (s *Server) commitOpLimit() int {
	if s.maxCommitOps > 0 {
		return s.maxCommitOps
	}
	return maxCommitOperations
}

// outcomeStore lazily hands hand-constructed servers the default retention.
func (s *Server) outcomeRecords() *outcomeStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.outcomes == nil {
		s.outcomes = newOutcomeStore(defaultOutcomeCapacity, defaultOutcomeTTL, defaultStaleReservation)
	}
	return s.outcomes
}

// validateIdentityFields checks the identity's scalar fields before any
// database work.
func validateIdentityFields(id rowIdentityRef) error {
	if id.ConnectionID == "" || id.Binding == "" || id.Schema == "" || id.Table == "" || id.Version == "" {
		return mutationDomainError{msg: "connectionId, binding, schema, table, key and version are required"}
	}
	if len(id.Key) == 0 {
		return mutationDomainError{msg: "key is required: the full primary key tuple reported by the table read"}
	}
	if _, err := strconv.ParseUint(id.Version, 10, 32); err != nil {
		return mutationDomainError{msg: "version must be the row version string reported by the table read"}
	}
	return nil
}

func writeDomainError(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
}

// writeMutationOutcome maps an error to the v2 status convention. It
// returns false when the request is finished (handler must return).
func writeMutationOutcome(w http.ResponseWriter, err error, verb string) bool {
	if err == nil {
		return true
	}
	status, body := classifyMutationOutcome(err, verb)
	if status >= 500 {
		log.Printf("studio: %s error: %v", verb, err)
	}
	writeJSON(w, status, body)
	return false
}

func (s *Server) handleTableRowUpdateV2(w http.ResponseWriter, r *http.Request) {
	var body rowUpdateRequestV2
	if !s.readMutationBody(w, r, &body) {
		return
	}
	id := rowIdentityRef{body.ConnectionID, body.Binding, body.Schema, body.Table, body.Version, body.Key}
	if err := validateIdentityFields(id); err != nil {
		writeDomainError(w, err)
		return
	}
	if body.Column == "" {
		writeDomainError(w, mutationDomainError{msg: "column is required"})
		return
	}
	if body.IsNull && body.Value != nil {
		writeDomainError(w, mutationDomainError{msg: "isNull=true must not carry a value (SQL NULL and a value are mutually exclusive)"})
		return
	}
	if !body.IsNull && body.Value == nil {
		writeDomainError(w, mutationDomainError{msg: "value is required (or set isNull=true for SQL NULL)"})
		return
	}
	target, ok := s.resolveTarget(w, r, body.ConnectionID, body.Binding, body.Schema, body.Table)
	if !ok {
		return
	}
	meta := target.meta
	keyArgs, keyCols, err := validateKeyTuple(meta, body.Key)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	col, exists := meta.Columns[body.Column]
	if !exists {
		writeDomainError(w, mutationDomainError{msg: fmt.Sprintf(
			"column %q does not exist on %s.%s; mutation rejected", body.Column, body.Schema, body.Table)})
		return
	}
	if reason := editableReason(col); reason != "" {
		writeDomainError(w, mutationDomainError{msg: fmt.Sprintf("column %q: %s", body.Column, reason)})
		return
	}
	if body.IsNull && col.NotNull {
		writeDomainError(w, mutationDomainError{msg: fmt.Sprintf("column %q is NOT NULL; SQL NULL rejected", body.Column)})
		return
	}
	var value any
	if !body.IsNull {
		value, err = decodeColumnValue(col, body.Value)
		if err != nil {
			writeDomainError(w, mutationDomainError{msg: fmt.Sprintf("column %q: %v", body.Column, err)})
			return
		}
	}

	tableRef := fmt.Sprintf("%s.%s", quoteIdent(body.Schema), quoteIdent(body.Table))
	mut := func(keyWhere string, valueParam int) string {
		if body.IsNull {
			return fmt.Sprintf("UPDATE %s SET %s = NULL", tableRef, quoteIdent(body.Column))
		}
		return fmt.Sprintf("UPDATE %s SET %s = $%d", tableRef, quoteIdent(body.Column), valueParam)
	}
	var sqlText string
	var args []any
	if body.IsNull {
		sqlText, args = buildGuardedMutationV2(body.Schema, body.Table, mut, keyArgs, keyCols, body.Version)
	} else {
		sqlText, args = buildGuardedMutationV2(body.Schema, body.Table, mut, keyArgs, keyCols, body.Version, value)
	}
	n, newVersion, err := runGuardedMutation(r.Context(), target.client, sqlText, args...)
	if err != nil {
		writeMutationOutcome(w, err, "row update")
		return
	}
	if n != 1 {
		writeMutationOutcome(w, explainRowConflictV2(r.Context(), target.client, body.Schema, body.Table, keyArgs, keyCols, "update"), "row update")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rowsAffected": n, "version": newVersion})
}

func (s *Server) handleTableRowDeleteV2(w http.ResponseWriter, r *http.Request) {
	var body rowDeleteRequestV2
	if !s.readMutationBody(w, r, &body) {
		return
	}
	id := rowIdentityRef{body.ConnectionID, body.Binding, body.Schema, body.Table, body.Version, body.Key}
	if err := validateIdentityFields(id); err != nil {
		writeDomainError(w, err)
		return
	}
	target, ok := s.resolveTarget(w, r, body.ConnectionID, body.Binding, body.Schema, body.Table)
	if !ok {
		return
	}
	keyArgs, keyCols, err := validateKeyTuple(target.meta, body.Key)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	tableRef := fmt.Sprintf("%s.%s", quoteIdent(body.Schema), quoteIdent(body.Table))
	mut := func(keyWhere string, valueParam int) string {
		return fmt.Sprintf("DELETE FROM %s", tableRef)
	}
	sqlText, args := buildGuardedMutationV2(body.Schema, body.Table, mut, keyArgs, keyCols, body.Version)
	n, _, err := runGuardedMutation(r.Context(), target.client, sqlText, args...)
	if err != nil {
		writeMutationOutcome(w, err, "row delete")
		return
	}
	if n != 1 {
		writeMutationOutcome(w, explainRowConflictV2(r.Context(), target.client, body.Schema, body.Table, keyArgs, keyCols, "delete"), "row delete")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rowsAffected": n})
}

func (s *Server) handleTableRowInsertV2(w http.ResponseWriter, r *http.Request) {
	var body rowInsertRequestV2
	if !s.readMutationBody(w, r, &body) {
		return
	}
	if body.ConnectionID == "" || body.Binding == "" || body.Schema == "" || body.Table == "" {
		writeDomainError(w, mutationDomainError{msg: "connectionId, binding, schema, and table are required"})
		return
	}
	if body.Values == nil {
		writeDomainError(w, mutationDomainError{msg: "values is required — an explicit column map; omit a column to request its DEFAULT, send {} for an all-DEFAULT row"})
		return
	}
	target, ok := s.resolveTarget(w, r, body.ConnectionID, body.Binding, body.Schema, body.Table)
	if !ok {
		return
	}
	meta := target.meta

	colNames, args, err := validateInsertValues(meta, body.Values, body.Schema, body.Table)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	sqlText, _ := buildInsertStatementV2(meta, body.Schema, body.Table, colNames)

	scanVals := make([]any, len(meta.PKCols)+1)
	dest := make([]any, len(scanVals))
	for i := range scanVals {
		dest[i] = &scanVals[i]
	}
	if err := target.client.QueryRow(r.Context(), sqlText, args...).Scan(dest...); err != nil {
		writeMutationOutcome(w, err, "row insert")
		return
	}
	keyOut := make([]keyCell, len(meta.PKCols))
	for i, pk := range meta.PKCols {
		keyOut[i] = keyCell{Column: pk, Value: encodeTaggedCell(meta.Columns[pk].TypeOID, scanVals[i])}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"rowsAffected": 1,
		"key":          keyOut,
		"version":      scanVals[len(scanVals)-1],
		"binding":      body.Binding,
	})
}

// validateInsertValues checks an insert values map against the catalog and
// decodes every value BEFORE any SQL. Omitted columns request DEFAULT;
// JSON null requests SQL NULL — the two are never conflated. Column order
// is deterministic (attnum), independent of JSON object order.
func validateInsertValues(meta *tableMeta, values map[string]any, schemaName, tableName string) ([]string, []any, error) {
	for name := range values {
		if _, exists := meta.Columns[name]; !exists {
			return nil, nil, mutationDomainError{msg: fmt.Sprintf(
				"column %q does not exist on %s.%s; insert rejected", name, schemaName, tableName)}
		}
	}
	var colNames []string
	var args []any
	for _, col := range meta.Order {
		raw, provided := values[col.Name]
		if !provided {
			continue
		}
		if reason := insertableReason(col); reason != "" {
			return nil, nil, mutationDomainError{msg: fmt.Sprintf("column %q: %s", col.Name, reason)}
		}
		if raw == nil && col.NotNull {
			return nil, nil, mutationDomainError{msg: fmt.Sprintf("column %q is NOT NULL; SQL NULL rejected (omit it to use its DEFAULT)", col.Name)}
		}
		v, err := decodeColumnValue(col, raw)
		if err != nil {
			return nil, nil, mutationDomainError{msg: fmt.Sprintf("column %q: %v", col.Name, err)}
		}
		colNames = append(colNames, col.Name)
		args = append(args, v)
	}
	return colNames, args, nil
}

// buildInsertStatementV2 renders the single-row INSERT with RETURNING of
// the full key tuple and the new row version. An empty column list is the
// valid all-DEFAULT row (DEFAULT VALUES), never "INSERT () VALUES ()".
func buildInsertStatementV2(meta *tableMeta, schemaName, tableName string, colNames []string) (string, []string) {
	tableRef := fmt.Sprintf("%s.%s", quoteIdent(schemaName), quoteIdent(tableName))
	returning := make([]string, 0, len(meta.PKCols)+1)
	for _, pk := range meta.PKCols {
		returning = append(returning, quoteIdent(pk))
	}
	returning = append(returning, "xmin::text")

	if len(colNames) == 0 {
		return fmt.Sprintf("INSERT INTO %s DEFAULT VALUES RETURNING %s", tableRef, strings.Join(returning, ", ")), returning
	}
	quotedCols := make([]string, len(colNames))
	placeholders := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = quoteIdent(c)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
		tableRef, strings.Join(quotedCols, ", "), strings.Join(placeholders, ", "), strings.Join(returning, ", ")), returning
}

// --- GET /api/table/v2/meta: authoritative editable-column metadata ---

type metaColumnV2 struct {
	Name           string `json:"name"`
	Type           string `json:"type"`
	Tag            string `json:"tag"`
	Nullable       bool   `json:"nullable"`
	IsKey          bool   `json:"isKey"`
	Generated      bool   `json:"generated"`
	Identity       bool   `json:"identity"`
	HasDefault     bool   `json:"hasDefault"`
	AutoAssigned   bool   `json:"autoAssigned"`
	Editable       bool   `json:"editable"`
	ReadOnlyReason string `json:"readOnlyReason,omitempty"`
	Insertable     bool   `json:"insertable"`
}

type tableMetaResponseV2 struct {
	Exists         bool           `json:"exists"`
	Binding        string         `json:"binding,omitempty"`
	KeyColumns     []string       `json:"keyColumns"`
	Versioned      bool           `json:"versioned"`
	ReadOnly       bool           `json:"readOnly"`
	ReadOnlyReason string         `json:"readOnlyReason,omitempty"`
	CanDelete      bool           `json:"canDelete"`
	Columns        []metaColumnV2 `json:"columns"`
}

// buildTableMetaV2 renders the authoritative metadata. When the table is
// read-only every column is non-editable with the table's reason, so a
// client that only looks at columns cannot offer an edit either.
func buildTableMetaV2(meta *tableMeta, binding string, versioned bool, schemaName, tableName string) tableMetaResponseV2 {
	resp := tableMetaResponseV2{
		Exists:     meta.Exists,
		KeyColumns: append([]string{}, meta.PKCols...),
		Columns:    []metaColumnV2{},
	}
	if !meta.Exists {
		resp.ReadOnly = true
		resp.ReadOnlyReason = fmt.Sprintf("%s.%s was not found or is not an ordinary table", schemaName, tableName)
		return resp
	}
	resp.Binding = binding
	resp.Versioned = versioned
	state := tableReadOnlyState(meta, versioned)
	resp.ReadOnly = state.readOnly
	if state.readOnly {
		resp.ReadOnlyReason = fmt.Sprintf("%s.%s %s", schemaName, tableName, state.reason)
	}
	resp.CanDelete = !state.readOnly && meta.CanDelete
	for _, col := range meta.Order {
		reason := editableReason(col)
		if state.readOnly {
			reason = resp.ReadOnlyReason
		}
		resp.Columns = append(resp.Columns, metaColumnV2{
			Name:           col.Name,
			Type:           col.TypeName,
			Tag:            wireTag(col.TypeOID),
			Nullable:       !col.NotNull,
			IsKey:          col.IsPK,
			Generated:      col.Generated != "",
			Identity:       col.Identity != "",
			HasDefault:     col.DefaultExpr != "",
			AutoAssigned:   col.autoAssigned(),
			Editable:       reason == "",
			ReadOnlyReason: reason,
			Insertable:     !state.readOnly && insertableReason(col) == "",
		})
	}
	return resp
}

func (s *Server) handleTableRowMetaV2(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	connID := q.Get("connectionId")
	schemaName := q.Get("schema")
	tableName := q.Get("table")
	if connID == "" || schemaName == "" || tableName == "" {
		writeError(w, http.StatusBadRequest, "connectionId, schema, and table are required")
		return
	}
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	meta, err := fetchTableMeta(r.Context(), client, schemaName, tableName)
	if err != nil {
		log.Printf("studio: meta introspection error: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": sanitizeError(err)})
		return
	}
	versioned := meta.Exists && probeVersioned(r.Context(), client, schemaName, tableName)
	writeJSON(w, http.StatusOK, buildTableMetaV2(meta, bindingFor(s.connectionEpoch(connID), meta.RelOID), versioned, schemaName, tableName))
}
