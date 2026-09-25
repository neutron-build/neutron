package studio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Atomic edit commits and retry outcomes (S02), served under
// /api/table/v2/{commit,preview,outcome,revert}.
//
// A commit stages one bounded list of row operations (the exact S01 row
// protocol: full-key identities, relation binding, xmin version guards,
// tagged wire values) and applies them as ONE transaction on a single
// connection: every guarded statement runs inside it, so a failure of any
// operation — including relation/FK consequences — rolls back everything
// before it. Originals are rechecked per operation by the same xmin guard
// the single-op endpoints use.
//
// Operation IDs are client-generated idempotency keys. Every commit
// reserves its ID BEFORE executing and records the terminal outcome
// together with the transaction's result:
//
//   - same ID + same canonical payload  -> the recorded outcome is
//     replayed; the operations are NOT executed again;
//   - same ID + different payload       -> 409 state "operation_conflict";
//   - concurrent duplicate (reservation open) -> 409 state "in_progress",
//     retryable;
//   - dropped response after a commit   -> retry (or the outcome lookup)
//     with the same ID resolves the recorded outcome without a second
//     execution;
//   - expired/evicted outcome records   -> "unknown". Never guessed, never
//     safe-to-repeat: a retried commit on an expired ID is refused with
//     409 state "unknown" and the client must verify database state and
//     use a new operation ID.
//
// Outcomes live in the server process (bounded capacity + TTL, see
// defaultOutcomeCapacity). A Studio restart therefore makes prior IDs
// unknown — the honest direction; durable cross-restart outcomes would
// need a provisioned table in the target database and are deliberately
// not attempted here (no hidden DDL on databases whose owner never
// consented).
//
// preview runs the identical validation and execution inside a transaction
// that is ALWAYS rolled back, and reports the data diff (before/after per
// operation). revert undoes a committed operation list through the inverse
// recorded at commit time — update back to the old value, delete the
// inserted row, re-insert the deleted row — and refuses honestly where the
// inverse cannot be exact (identity/serial keys, generated columns that
// cannot be re-supplied, values that cannot round-trip the wire, or FK
// cascade side effects the inverse does not capture).
//
// Connections or models lacking the required semantics are rejected: row
// commits demand versioned, editable tables (non-xmin engines are refused
// by the read-only gate), and the operation shape carries no SQL, no
// multi-model verbs — unknown fields are refused by the strict decoder.

const (
	// maxCommitOperations bounds one commit/preview request. Configurable
	// downward at launch via NEUTRON_STUDIO_MAX_COMMIT_OPERATIONS.
	maxCommitOperations = 100
	// maxOperationIDLen bounds the client-generated operation ID.
	maxOperationIDLen = 128
	// defaultOutcomeCapacity bounds retained outcome records; the oldest
	// record is evicted (tombstoned) when the store is full.
	defaultOutcomeCapacity = 256
	// defaultOutcomeTTL is how long a terminal outcome stays resolvable.
	defaultOutcomeTTL = 15 * time.Minute
	// defaultStaleReservation: an in-progress reservation older than this
	// is treated as abandoned (its handler died) and may be replaced.
	defaultStaleReservation = 2 * time.Minute
)

// --- request shapes ---

// commitOp is one staged row operation. Fields are pointers where presence
// matters so per-kind strictness can reject smuggled intent (an insert
// carrying a version, an update carrying values, ...).
type commitOp struct {
	Op      string         `json:"op"`
	Schema  string         `json:"schema"`
	Table   string         `json:"table"`
	Binding string         `json:"binding"`
	Values  map[string]any `json:"values,omitempty"` // insert: column -> wire value
	Key     []keyCell      `json:"key,omitempty"`    // update/delete: full key tuple
	Version *string        `json:"version,omitempty"`
	Column  *string        `json:"column,omitempty"` // update
	Value   any            `json:"value,omitempty"`  // update: wire value
	IsNull  *bool          `json:"isNull,omitempty"` // update: SQL NULL
}

type commitRequestV2 struct {
	ConnectionID string     `json:"connectionId"`
	OperationID  string     `json:"operationId"`
	Operations   []commitOp `json:"operations"`
}

type previewRequestV2 struct {
	ConnectionID string     `json:"connectionId"`
	Operations   []commitOp `json:"operations"`
}

type outcomeRequestV2 struct {
	ConnectionID string `json:"connectionId"`
	OperationID  string `json:"operationId"`
}

type revertRequestV2 struct {
	ConnectionID      string `json:"connectionId"`
	OperationID       string `json:"operationId"`
	RevertOperationID string `json:"revertOperationId"`
}

// envInt / envInt64 read an optional launch configuration value.
func envInt(name string) int {
	v, err := strconv.Atoi(os.Getenv(name))
	if err != nil {
		return 0
	}
	return v
}

func envInt64(name string) int64 {
	v, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// clampDownwardInt/64 restrict configuration to at most the default:
// limits are configurable downward only, never raised past the coded bound.
func clampDownwardInt(v, def int) int {
	if v <= 0 || v > def {
		return def
	}
	return v
}

func clampDownwardInt64(v, def int64) int64 {
	if v <= 0 || v > def {
		return def
	}
	return v
}

// validateOperationID checks the client-generated idempotency key shape.
func validateOperationID(id string) error {
	if id == "" {
		return mutationDomainError{msg: "operationId is required — a client-generated idempotency key (e.g. a UUID) that identifies this exact commit"}
	}
	if len(id) > maxOperationIDLen {
		return mutationDomainError{msg: fmt.Sprintf("operationId must be at most %d characters", maxOperationIDLen)}
	}
	for _, r := range id {
		if r < 0x20 || r == 0x7f {
			return mutationDomainError{msg: "operationId must not contain control characters"}
		}
	}
	return nil
}

// validateCommitOpShape enforces the per-kind field contract before any
// database work: operations carry exactly the fields their kind uses, so a
// client cannot smuggle extra intent the executor would silently ignore.
func validateCommitOpShape(i int, op commitOp) error {
	where := fmt.Sprintf("operations[%d]", i)
	base := func() error {
		if op.Schema == "" || op.Table == "" || op.Binding == "" {
			return mutationDomainError{msg: where + ": schema, table and binding are required (the binding from the table read)"}
		}
		return nil
	}
	hasUpdateExtras := op.Column != nil || op.Value != nil || op.IsNull != nil || len(op.Key) > 0 || op.Version != nil
	switch op.Op {
	case "insert":
		if err := base(); err != nil {
			return err
		}
		if op.Values == nil {
			return mutationDomainError{msg: where + ": insert requires values — an explicit column map; omit a column for DEFAULT, {} for an all-DEFAULT row"}
		}
		if hasUpdateExtras {
			return mutationDomainError{msg: where + ": insert carries update/delete-only fields (key/version/column/value/isNull); use only values"}
		}
	case "update":
		if err := base(); err != nil {
			return err
		}
		if op.Values != nil {
			return mutationDomainError{msg: where + ": update carries insert-only fields (values); use key/version/column/value"}
		}
		if len(op.Key) == 0 || op.Version == nil || op.Column == nil {
			return mutationDomainError{msg: where + ": update requires key, version and column"}
		}
		isNull := op.IsNull != nil && *op.IsNull
		if isNull && op.Value != nil {
			return mutationDomainError{msg: where + ": isNull=true must not carry a value"}
		}
		if !isNull && op.Value == nil {
			return mutationDomainError{msg: where + ": value is required (or isNull=true for SQL NULL)"}
		}
	case "delete":
		if err := base(); err != nil {
			return err
		}
		if op.Values != nil || op.Column != nil || op.Value != nil || op.IsNull != nil {
			return mutationDomainError{msg: where + ": delete carries update/insert-only fields; use key and version only"}
		}
		if len(op.Key) == 0 || op.Version == nil {
			return mutationDomainError{msg: where + ": delete requires key and version"}
		}
	default:
		return mutationDomainError{msg: fmt.Sprintf("%s: op must be \"insert\", \"update\" or \"delete\", got %q", where, op.Op)}
	}
	return nil
}

// validateCommitOps checks the batch limits and every operation's shape.
func validateCommitOps(ops []commitOp, limit int) error {
	if len(ops) == 0 {
		return mutationDomainError{msg: "operations is required — stage at least one operation"}
	}
	if len(ops) > limit {
		return mutationDomainError{msg: fmt.Sprintf(
			"commit is limited to %d operations per request, got %d; split the batch", limit, len(ops))}
	}
	for i := range ops {
		if err := validateCommitOpShape(i, ops[i]); err != nil {
			return err
		}
		if ops[i].Op != "insert" {
			if _, err := strconv.ParseUint(*ops[i].Version, 10, 32); err != nil {
				return mutationDomainError{msg: fmt.Sprintf(
					"operations[%d]: version must be the row version string reported by the table read", i)}
			}
		}
	}
	return nil
}

// canonicalCommitPayload is the hash basis: the decoded request body
// re-marshaled deterministically (Go sorts map keys; struct field order is
// fixed; json.Number keeps literal digits), so two bodies that decode to
// the same operations hash identically regardless of key order or number
// spelling.
type canonicalCommitPayload struct {
	ConnectionID string     `json:"connectionId"`
	Operations   []commitOp `json:"operations"`
}

func canonicalCommitHash(connID string, ops []commitOp) string {
	b, err := json.Marshal(canonicalCommitPayload{ConnectionID: connID, Operations: ops})
	if err != nil {
		// The payload came from the JSON decoder; re-marshaling cannot fail.
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func outcomeKey(connID, operationID string) string {
	return connID + "\x00" + operationID
}

// --- outcome store ---

// outcome states: "in_progress" (reservation open), "committed",
// "failed" (rolled back, nothing applied), "unknown" (ambiguous commit).
const (
	outcomeInProgress = "in_progress"
	outcomeCommitted  = "committed"
	outcomeFailed     = "failed"
	outcomeUnknown    = "unknown"
)

type outcomeRecord struct {
	OperationID  string
	ConnectionID string
	PayloadHash  string
	State        string
	Status       int            // replay HTTP status for terminal states
	Response     map[string]any // exact replay body
	Reversible   bool
	Refusal      string
	Inverse      []commitOp
	CreatedAt    time.Time
}

// reservation is what reserve returns for a commit attempt.
type reservation struct {
	kind string // "claimed" | "replay" | "conflict" | "in_progress" | "unknown"
	rec  *outcomeRecord
}

// outcomeStore keeps operation outcomes in-process with bounded retention:
// capacity FIFO eviction plus a TTL, both of which turn further lookups
// into honest "unknown" answers via tombstones. Tombstones themselves are
// bounded (2x capacity); beyond that the ID is fully forgotten and behaves
// as never-seen. All methods are goroutine-safe.
type outcomeStore struct {
	mu          sync.Mutex
	capacity    int
	ttl         time.Duration
	staleRes    time.Duration
	records     map[string]*outcomeRecord
	order       []string // insertion order of live records, oldest first
	tombstones  []string // ring of evicted/expired keys
	tombstoneAt map[string]bool
}

func newOutcomeStore(capacity int, ttl, staleReservation time.Duration) *outcomeStore {
	if capacity <= 0 {
		capacity = defaultOutcomeCapacity
	}
	return &outcomeStore{
		capacity:    capacity,
		ttl:         ttl,
		staleRes:    staleReservation,
		records:     map[string]*outcomeRecord{},
		tombstoneAt: map[string]bool{},
	}
}

func (o *outcomeStore) tombstone(key string) {
	if o.tombstoneAt[key] {
		return
	}
	o.tombstoneAt[key] = true
	o.tombstones = append(o.tombstones, key)
	if len(o.tombstones) > 2*o.capacity {
		drop := o.tombstones[0]
		o.tombstones = o.tombstones[1:]
		delete(o.tombstoneAt, drop)
	}
}

// evictLocked drops the oldest evictable record when the store is at
// capacity. Live in-progress reservations are skipped (their handler will
// finish them); with only live reservations the store may briefly exceed
// capacity rather than evict a record whose outcome is still being written.
func (o *outcomeStore) evictLocked() {
	if len(o.records) < o.capacity {
		return
	}
	for idx, key := range o.order {
		rec, ok := o.records[key]
		if ok && rec.State == outcomeInProgress && time.Since(rec.CreatedAt) < o.staleRes {
			continue
		}
		o.order = append(o.order[:idx], o.order[idx+1:]...)
		delete(o.records, key)
		o.tombstone(key)
		return
	}
}

// reserve claims an operation ID for a commit attempt. The caller MUST
// finish the reservation (store.finish) with a terminal outcome.
func (o *outcomeStore) reserve(key, connID, hash string) reservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := time.Now()
	if rec, ok := o.records[key]; ok {
		if rec.State != outcomeInProgress && o.ttl > 0 && now.Sub(rec.CreatedAt) > o.ttl {
			delete(o.records, key)
			o.tombstone(key)
			return reservation{kind: "unknown"}
		}
		switch {
		case rec.State == outcomeInProgress:
			if o.staleRes > 0 && now.Sub(rec.CreatedAt) > o.staleRes {
				// Abandoned reservation (its handler died): take it over.
				rec.PayloadHash = hash
				rec.CreatedAt = now
				return reservation{kind: "claimed", rec: rec}
			}
			return reservation{kind: "in_progress", rec: rec}
		case rec.PayloadHash != hash:
			return reservation{kind: "conflict", rec: rec}
		default:
			return reservation{kind: "replay", rec: rec}
		}
	}
	if o.tombstoneAt[key] {
		return reservation{kind: "unknown"}
	}
	o.evictLocked()
	rec := &outcomeRecord{
		OperationID:  splitOutcomeKey(key),
		ConnectionID: connID,
		PayloadHash:  hash,
		State:        outcomeInProgress,
		CreatedAt:    now,
	}
	o.records[key] = rec
	o.order = append(o.order, key)
	return reservation{kind: "claimed", rec: rec}
}

// finish records the terminal outcome of a claimed reservation.
func (o *outcomeStore) finish(key string, mutate func(*outcomeRecord)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rec, ok := o.records[key]
	if !ok {
		return
	}
	if mutate != nil {
		mutate(rec)
	}
	rec.CreatedAt = time.Now()
}

// lookup resolves an operation ID for status checks and reverts.
// state is one of the outcome* constants or "absent".
func (o *outcomeStore) lookup(key string) (*outcomeRecord, string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rec, ok := o.records[key]
	if !ok {
		if o.tombstoneAt[key] {
			return nil, outcomeUnknown
		}
		return nil, "absent"
	}
	if rec.State != outcomeInProgress && o.ttl > 0 && time.Since(rec.CreatedAt) > o.ttl {
		delete(o.records, key)
		o.tombstone(key)
		return nil, outcomeUnknown
	}
	return rec, rec.State
}

func splitOutcomeKey(key string) string {
	if i := strings.IndexByte(key, 0); i >= 0 {
		return key[i+1:]
	}
	return key
}

// --- outcome classification ---

// classifyMutationOutcome maps an execution error to the v2 status
// convention (the same mapping writeMutationOutcome applies), returning
// the status and body so batch handlers can both record and replay it.
func classifyMutationOutcome(err error, verb string) (int, map[string]any) {
	var pgErr *pgconn.PgError
	switch e := err.(type) {
	case nil:
		return http.StatusOK, nil
	case mutationDomainError:
		return http.StatusBadRequest, map[string]any{"error": e.Error()}
	case rowStateError:
		body := map[string]any{"error": e.msg, "state": e.state}
		if e.state == "conflict" {
			body["currentVersion"] = e.currentVersion
		}
		return http.StatusConflict, body
	case errNotConnected:
		return http.StatusBadRequest, map[string]any{"error": e.Error()}
	case errIntrospection:
		return http.StatusBadGateway, map[string]any{"error": e.Error()}
	default:
		switch {
		case errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22"):
			return http.StatusBadRequest, map[string]any{"error": sanitizeError(err)}
		case errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "23"):
			return http.StatusConflict, map[string]any{"error": sanitizeError(err), "state": "constraint"}
		case errors.As(err, &pgErr) && pgErr.Code == "42501":
			return http.StatusForbidden, map[string]any{"error": sanitizeError(err), "state": "privilege"}
		default:
			return http.StatusBadGateway, map[string]any{"error": sanitizeError(err)}
		}
	}
}

// --- batch preparation ---

// batchResolver caches per-table introspection within one request so a
// 100-operation batch over one table introspects once. Every operation
// still passes the full resolveTargetErr checks (existence, read-only
// gate, binding) against the cached-but-fresh metadata.
type batchResolver struct {
	s       *Server
	connID  string
	targets map[string]*resolvedTarget
}

func (br *batchResolver) resolve(ctx context.Context, binding, schemaName, tableName string) (*resolvedTarget, error) {
	cacheKey := schemaName + "\x00" + tableName
	if t, ok := br.targets[cacheKey]; ok {
		if bindingFor(br.s.connectionEpoch(br.connID), t.meta.RelOID) != binding {
			return nil, rowStateError{state: "binding", msg: fmt.Sprintf(
				"%s.%s is no longer the relation these rows were read from (reconnected, or the table was replaced); reload before editing",
				schemaName, tableName)}
		}
		return t, nil
	}
	t, err := br.s.resolveTargetErr(ctx, br.connID, binding, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	br.targets[cacheKey] = t
	return t, nil
}

// preparedOp is a fully validated, decoded operation ready to execute.
type preparedOp struct {
	index   int
	kind    string // insert | update | delete
	schema  string
	table   string
	binding string
	meta    *tableMeta

	// insert
	insCols []string
	insArgs []any
	insRaw  map[string]any // original wire values (preview "after")

	// update/delete
	keyArgs  []any
	keyCols  []string
	keyCells []keyCell
	version  string

	// update
	column  string
	colMeta tableColumnMeta
	value   any // decoded driver value; nil means SQL NULL
	isNull  bool
	rawVal  any // original wire value (preview "after")
}

// prepareCommitOps validates and decodes every operation against fresh
// catalog metadata BEFORE any transaction starts: unknown columns, forged
// keys, read-only tables/columns, malformed tagged values and NOT NULL
// violations are refused up front, exactly like the single-op endpoints.
func (s *Server) prepareCommitOps(ctx context.Context, connID string, ops []commitOp) ([]*preparedOp, error) {
	if _, ok := s.clientFor(connID); !ok {
		return nil, errNotConnected{}
	}
	br := &batchResolver{s: s, connID: connID, targets: map[string]*resolvedTarget{}}
	prepared := make([]*preparedOp, 0, len(ops))
	for i := range ops {
		op := ops[i]
		target, err := br.resolve(ctx, op.Binding, op.Schema, op.Table)
		if err != nil {
			return nil, fmtOpError(i, err)
		}
		p := &preparedOp{
			index:   i,
			kind:    op.Op,
			schema:  op.Schema,
			table:   op.Table,
			binding: op.Binding,
			meta:    target.meta,
		}
		switch op.Op {
		case "insert":
			cols, args, err := validateInsertValues(target.meta, op.Values, op.Schema, op.Table)
			if err != nil {
				return nil, fmtOpError(i, err)
			}
			p.insCols, p.insArgs, p.insRaw = cols, args, op.Values
		case "update":
			keyArgs, keyCols, err := validateKeyTuple(target.meta, op.Key)
			if err != nil {
				return nil, fmtOpError(i, err)
			}
			col, exists := target.meta.Columns[*op.Column]
			if !exists {
				return nil, fmtOpError(i, mutationDomainError{msg: fmt.Sprintf(
					"column %q does not exist on %s.%s; mutation rejected", *op.Column, op.Schema, op.Table)})
			}
			if reason := editableReason(col); reason != "" {
				return nil, fmtOpError(i, mutationDomainError{msg: fmt.Sprintf("column %q: %s", *op.Column, reason)})
			}
			if op.IsNull != nil && *op.IsNull && col.NotNull {
				return nil, fmtOpError(i, mutationDomainError{msg: fmt.Sprintf(
					"column %q is NOT NULL; SQL NULL rejected", *op.Column)})
			}
			var value any
			if op.IsNull == nil || !*op.IsNull {
				value, err = decodeColumnValue(col, op.Value)
				if err != nil {
					return nil, fmtOpError(i, mutationDomainError{msg: fmt.Sprintf("column %q: %v", *op.Column, err)})
				}
			}
			p.keyArgs, p.keyCols, p.keyCells = keyArgs, keyCols, op.Key
			p.version = *op.Version
			p.column, p.colMeta = *op.Column, col
			p.value = value
			p.isNull = op.IsNull != nil && *op.IsNull
			p.rawVal = op.Value
		case "delete":
			keyArgs, keyCols, err := validateKeyTuple(target.meta, op.Key)
			if err != nil {
				return nil, fmtOpError(i, err)
			}
			p.keyArgs, p.keyCols, p.keyCells = keyArgs, keyCols, op.Key
			p.version = *op.Version
		}
		prepared = append(prepared, p)
	}
	return prepared, nil
}

// fmtOpError prefixes an operation error with its position in the batch.
func fmtOpError(i int, err error) error {
	if de, ok := err.(mutationDomainError); ok {
		return mutationDomainError{msg: fmt.Sprintf("operations[%d]: %s", i, de.msg)}
	}
	if se, ok := err.(rowStateError); ok {
		return rowStateError{state: se.state, currentVersion: se.currentVersion,
			msg: fmt.Sprintf("operations[%d]: %s", i, se.msg)}
	}
	return fmt.Errorf("operations[%d]: %w", i, err)
}

// --- execution ---

// opExecution is one operation's result plus the data captured for the
// preview diff and the recorded inverse.
type opExecution struct {
	result    opResult
	before    any            // update: old cell, wire form
	beforeRow map[string]any // delete: full old row, wire form
	fkSide    bool           // changing this row affects child rows via FK rules
	fkErr     error          // the probe could not decide (fail-safe refusal)
}

type opResult struct {
	Index        int       `json:"index"`
	Op           string    `json:"op"`
	RowsAffected int64     `json:"rowsAffected"`
	Key          []keyCell `json:"key,omitempty"`
	Version      string    `json:"version,omitempty"`
}

// wireValueOf renders a captured driver value in request wire form: tagged
// cells for tagged types (as the {"t","v"} map the strict decoder accepts),
// canonical UUID strings, JSON as text, native numeric scalars as
// json.Number (exact literal digits), other scalars as themselves. The
// result round-trips decodeColumnValue.
func wireValueOf(col tableColumnMeta, v any) any {
	if v == nil {
		return nil
	}
	switch col.TypeOID {
	case oidJSON, oidJSONB:
		if b, ok := v.([]byte); ok {
			return string(b)
		}
		return v
	}
	enc := encodeTaggedCell(col.TypeOID, v)
	switch tc := enc.(type) {
	case taggedCell:
		return map[string]any{"t": tc.T, "v": tc.V}
	case int16:
		return json.Number(strconv.FormatInt(int64(tc), 10))
	case int32:
		return json.Number(strconv.FormatInt(int64(tc), 10))
	case int64:
		return json.Number(strconv.FormatInt(tc, 10))
	case float32:
		return json.Number(strconv.FormatFloat(float64(tc), 'g', -1, 32))
	case float64:
		return json.Number(strconv.FormatFloat(tc, 'g', -1, 64))
	}
	return enc
}

// wireValueRoundTrips reports whether a captured wire value would be
// accepted back by the strict decoder for its column.
func wireValueRoundTrips(col tableColumnMeta, v any) bool {
	if v == nil {
		return true
	}
	_, err := decodeColumnValue(col, v)
	return err == nil
}

// fkSideEffectRules are the pg_constraint columns whose values mean "this
// rule silently changes child rows": cascade, set null, set default.
const (
	ruleFKDelete = "confdeltype" // ON DELETE ...
	ruleFKUpdate = "confupdtype" // ON UPDATE ...
)

var fkSideEffectRules = map[string]bool{ruleFKDelete: true, ruleFKUpdate: true}

// incomingFK is one foreign key pointing at a table whose referential
// rules can silently change other rows when the parent row changes.
type incomingFK struct {
	childSchema, childTable string
	childCols, parentCols   []string // paired tuples in constraint order
}

// sideEffectFKs lists incoming FKs on the table whose rule column
// ("confdeltype" or "confupdtype") is a silent-change rule.
func sideEffectFKs(ctx context.Context, tx pgx.Tx, schemaName, tableName, rule string) ([]incomingFK, error) {
	if !fkSideEffectRules[rule] {
		return nil, fmt.Errorf("unknown fk rule column %q", rule)
	}
	rows, err := tx.Query(ctx, `
		SELECT cns.nspname, ch.relname,
		       array_agg(ca.attname ORDER BY u.ord) AS child_cols,
		       array_agg(pa.attname ORDER BY u.ord) AS parent_cols
		FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class pt ON pt.oid = con.confrelid
		JOIN pg_catalog.pg_namespace pns ON pns.oid = pt.relnamespace
		JOIN pg_catalog.pg_class ch ON ch.oid = con.conrelid
		JOIN pg_catalog.pg_namespace cns ON cns.oid = ch.relnamespace
		CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS u(cattr, pattr, ord)
		JOIN pg_catalog.pg_attribute ca ON ca.attrelid = con.conrelid AND ca.attnum = u.cattr
		JOIN pg_catalog.pg_attribute pa ON pa.attrelid = con.confrelid AND pa.attnum = u.pattr
		WHERE con.contype = 'f' AND pns.nspname = $1 AND pt.relname = $2
		  AND con.`+rule+` IN ('c','n','d')
		GROUP BY cns.nspname, ch.relname, con.oid
		ORDER BY con.oid`, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var fks []incomingFK
	for rows.Next() {
		var fk incomingFK
		if err := rows.Scan(&fk.childSchema, &fk.childTable, &fk.childCols, &fk.parentCols); err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}

// fkSideEffectsAffectRow reports whether changing the addressed row would
// change CHILD rows through FK rules (ON DELETE ... for "confdeltype",
// ON UPDATE ... for "confupptype"). The probe runs inside the transaction
// and resolves the addressed row's values on each referenced tuple first,
// so the verdict is exact for THIS row: a delete on a table with cascade
// children elsewhere is reversible when nothing references this row.
// onlyColumn narrows update-rule checks to FKs referencing that column.
// An error means reversibility could not be verified (fail-safe refusal).
func fkSideEffectsAffectRow(ctx context.Context, tx pgx.Tx, schemaName, tableName string, keyCols []string, keyArgs []any, rule, onlyColumn string) (bool, error) {
	fks, err := sideEffectFKs(ctx, tx, schemaName, tableName, rule)
	if err != nil {
		return true, err
	}
	parentRef := fmt.Sprintf("%s.%s", quoteIdent(schemaName), quoteIdent(tableName))
	keyWhere := keyPredicate(keyCols, 1)
	for _, fk := range fks {
		if onlyColumn != "" {
			relevant := false
			for _, pc := range fk.parentCols {
				if pc == onlyColumn {
					relevant = true
					break
				}
			}
			if !relevant {
				continue
			}
		}
		selects := make([]string, len(fk.parentCols))
		for i, pc := range fk.parentCols {
			selects[i] = quoteIdent(pc)
		}
		vals := make([]any, len(fk.parentCols))
		dest := make([]any, len(vals))
		for i := range vals {
			dest[i] = &vals[i]
		}
		err := tx.QueryRow(ctx, fmt.Sprintf(
			"SELECT %s FROM %s WHERE %s", strings.Join(selects, ", "), parentRef, keyWhere),
			keyArgs...).Scan(dest...)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// The mutation itself will report the missing row; no
				// child can reference a row that is not there.
				continue
			}
			return true, err
		}
		childRef := fmt.Sprintf("%s.%s", quoteIdent(fk.childSchema), quoteIdent(fk.childTable))
		conds := make([]string, len(fk.childCols))
		for i, cc := range fk.childCols {
			conds[i] = fmt.Sprintf("%s = $%d", quoteIdent(cc), i+1)
		}
		var affects bool
		if err := tx.QueryRow(ctx, fmt.Sprintf(
			"SELECT EXISTS (SELECT 1 FROM %s WHERE %s)", childRef, strings.Join(conds, " AND ")),
			vals...).Scan(&affects); err != nil {
			return true, err
		}
		if affects {
			return true, nil
		}
	}
	return false, nil
}

// runCommitOps executes the prepared operations in order inside the given
// transaction, capturing old values for the preview diff and the recorded
// inverse. Every guarded statement rechecks the operation's originals
// (full key + xmin version, exactly-one-row); a zero-row mutation is
// classified conflict/missing and aborts the whole batch. EVERY error is
// prefixed with its operations[N] position (S05 entry condition S03-F1):
// execution-time failures are as attributable as prepare-time ones, so a
// multi-op batch never pins an innocent first row.
func runCommitOps(ctx context.Context, tx pgx.Tx, prepared []*preparedOp) ([]opExecution, error) {
	var out []opExecution
	for _, p := range prepared {
		switch p.kind {
		case "insert":
			sqlText, _ := buildInsertStatementV2(p.meta, p.schema, p.table, p.insCols)
			scanVals := make([]any, len(p.meta.PKCols)+1)
			dest := make([]any, len(scanVals))
			for i := range scanVals {
				dest[i] = &scanVals[i]
			}
			if err := tx.QueryRow(ctx, sqlText, p.insArgs...).Scan(dest...); err != nil {
				return nil, fmtOpError(p.index, err)
			}
			keyOut := make([]keyCell, len(p.meta.PKCols))
			for i, pk := range p.meta.PKCols {
				// wireValueOf normalizes to decoder-acceptable wire form
				// (json.Number / tagged cells): the returned key is reused
				// verbatim as the inverse delete's key tuple.
				keyOut[i] = keyCell{Column: pk, Value: wireValueOf(p.meta.Columns[pk], scanVals[i])}
			}
			res := opResult{Index: p.index, Op: "insert", RowsAffected: 1,
				Key: keyOut, Version: scanVals[len(scanVals)-1].(string)}
			out = append(out, opExecution{result: res})

		case "update":
			// Capture the old value for the inverse/preview BEFORE the
			// guarded statement: if the guard then applies, the captured
			// value is exactly what changed (READ COMMITTED: the SELECT and
			// the guarded UPDATE see the same committed row; a row changed
			// in between fails the xmin guard instead).
			tableRef := fmt.Sprintf("%s.%s", quoteIdent(p.schema), quoteIdent(p.table))
			keyWhere := keyPredicate(p.keyCols, 1)
			var oldValue any
			err := tx.QueryRow(ctx, fmt.Sprintf(
				"SELECT %s FROM %s WHERE %s", quoteIdent(p.column), tableRef, keyWhere),
				p.keyArgs...).Scan(&oldValue)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil, fmtOpError(p.index, explainRowConflictV2(ctx, tx, p.schema, p.table, p.keyArgs, p.keyCols, "update"))
				}
				return nil, fmtOpError(p.index, err)
			}
			before := wireValueOf(p.colMeta, oldValue)
			ex := opExecution{}
			ex.fkSide, ex.fkErr = fkSideEffectsAffectRow(ctx, tx, p.schema, p.table, p.keyCols, p.keyArgs, ruleFKUpdate, p.column)

			mut := func(keyWhere string, valueParam int) string {
				if p.isNull {
					return fmt.Sprintf("UPDATE %s SET %s = NULL", tableRef, quoteIdent(p.column))
				}
				return fmt.Sprintf("UPDATE %s SET %s = $%d", tableRef, quoteIdent(p.column), valueParam)
			}
			var sqlText string
			var args []any
			if p.isNull {
				sqlText, args = buildGuardedMutationV2(p.schema, p.table, mut, p.keyArgs, p.keyCols, p.version)
			} else {
				sqlText, args = buildGuardedMutationV2(p.schema, p.table, mut, p.keyArgs, p.keyCols, p.version, p.value)
			}
			n, newVersion, err := runGuardedMutation(ctx, tx, sqlText, args...)
			if err != nil {
				return nil, fmtOpError(p.index, err)
			}
			if n != 1 {
				return nil, fmtOpError(p.index, explainRowConflictV2(ctx, tx, p.schema, p.table, p.keyArgs, p.keyCols, "update"))
			}
			ex.result = opResult{Index: p.index, Op: "update", RowsAffected: n, Version: newVersion}
			ex.before = before
			out = append(out, ex)

		case "delete":
			tableRef := fmt.Sprintf("%s.%s", quoteIdent(p.schema), quoteIdent(p.table))
			keyWhere := keyPredicate(p.keyCols, 1)
			// Capture the complete old row: the inverse re-insert needs
			// every column's exact value.
			selects := make([]string, 0, len(p.meta.Order))
			for _, col := range p.meta.Order {
				selects = append(selects, quoteIdent(col.Name))
			}
			scanVals := make([]any, len(p.meta.Order))
			dest := make([]any, len(scanVals))
			for i := range scanVals {
				dest[i] = &scanVals[i]
			}
			err := tx.QueryRow(ctx, fmt.Sprintf(
				"SELECT %s FROM %s WHERE %s", strings.Join(selects, ", "), tableRef, keyWhere),
				p.keyArgs...).Scan(dest...)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil, fmtOpError(p.index, explainRowConflictV2(ctx, tx, p.schema, p.table, p.keyArgs, p.keyCols, "delete"))
				}
				return nil, fmtOpError(p.index, err)
			}
			beforeRow := make(map[string]any, len(p.meta.Order))
			for i, col := range p.meta.Order {
				beforeRow[col.Name] = wireValueOf(col, scanVals[i])
			}
			ex := opExecution{beforeRow: beforeRow}
			ex.fkSide, ex.fkErr = fkSideEffectsAffectRow(ctx, tx, p.schema, p.table, p.keyCols, p.keyArgs, ruleFKDelete, "")

			mut := func(keyWhere string, valueParam int) string {
				return fmt.Sprintf("DELETE FROM %s", tableRef)
			}
			sqlText, args := buildGuardedMutationV2(p.schema, p.table, mut, p.keyArgs, p.keyCols, p.version)
			n, _, err := runGuardedMutation(ctx, tx, sqlText, args...)
			if err != nil {
				return nil, fmtOpError(p.index, err)
			}
			if n != 1 {
				return nil, fmtOpError(p.index, explainRowConflictV2(ctx, tx, p.schema, p.table, p.keyArgs, p.keyCols, "delete"))
			}
			ex.result = opResult{Index: p.index, Op: "delete", RowsAffected: n}
			out = append(out, ex)
		}
	}
	return out, nil
}

// buildInverse derives the recorded inverse of a committed batch from the
// prepared operations and their captured old values, plus an honest
// reversibility verdict with the first refusing reason.
func buildInverse(ctx context.Context, tx pgx.Tx, prepared []*preparedOp, execs []opExecution) ([]commitOp, bool, string) {
	inverse := make([]commitOp, 0, len(prepared))
	reversible := true
	var refusal string
	setRefusal := func(format string, args ...any) {
		if reversible {
			reversible = false
			refusal = fmt.Sprintf(format, args...)
		}
	}
	for _, p := range prepared {
		ex := execs[p.index]
		switch p.kind {
		case "insert":
			// The inserted row is deleted by its returned identity.
			v := ex.result.Version
			inverse = append(inverse, commitOp{
				Op: "delete", Schema: p.schema, Table: p.table, Binding: p.binding,
				Key: ex.result.Key, Version: &v,
			})
		case "update":
			if !wireValueRoundTrips(p.colMeta, ex.before) {
				setRefusal("operations[%d]: the old value of column %q cannot round-trip the wire protocol exactly", p.index, p.column)
			}
			if ex.fkErr != nil {
				setRefusal("operations[%d]: reversibility of column %q could not be verified (%v)", p.index, p.column, sanitizeError(ex.fkErr))
			} else if ex.fkSide {
				setRefusal("operations[%d]: column %q is referenced by rows honoring an ON UPDATE cascade/set-null/set-default rule the inverse cannot capture", p.index, p.column)
			}
			inv := commitOp{
				Op: "update", Schema: p.schema, Table: p.table, Binding: p.binding,
				Key: p.keyCells, Version: &ex.result.Version, Column: &p.column,
			}
			if ex.before == nil {
				t := true
				inv.IsNull = &t
			} else {
				inv.Value = ex.before
			}
			inverse = append(inverse, inv)
		case "delete":
			// A delete is reversible only when the exact row can be
			// re-created: plainly insertable keys (no identity/serial/
			// generated keys), every value round-trippable, generated
			// columns recomputed by omission, and no FK rules that
			// changed OTHER rows when this row disappeared.
			if ex.fkErr != nil {
				setRefusal("operations[%d]: reversibility could not be verified (%v)", p.index, sanitizeError(ex.fkErr))
			} else if ex.fkSide {
				setRefusal("operations[%d]: deleting this row changed other rows through an ON DELETE cascade/set-null/set-default rule; those side effects cannot be captured for revert", p.index)
			}
			values := make(map[string]any, len(p.meta.Order))
			for _, col := range p.meta.Order {
				v, captured := ex.beforeRow[col.Name]
				if !captured {
					continue
				}
				if reason := insertableReason(col); reason != "" {
					if col.Generated != "" && !col.IsPK {
						// Stored generated columns are recomputed from the
						// re-inserted base columns; omit and let the engine
						// derive them.
						continue
					}
					setRefusal("operations[%d]: column %q cannot be re-inserted on revert (%s)", p.index, col.Name, reason)
					continue
				}
				if !wireValueRoundTrips(col, v) {
					setRefusal("operations[%d]: the old value of column %q cannot round-trip the wire protocol exactly", p.index, col.Name)
					continue
				}
				values[col.Name] = v
			}
			for _, pk := range p.meta.PKCols {
				if _, ok := values[pk]; !ok {
					setRefusal("operations[%d]: key column %q cannot be re-supplied on revert", p.index, pk)
				}
			}
			inverse = append(inverse, commitOp{
				Op: "insert", Schema: p.schema, Table: p.table, Binding: p.binding, Values: values,
			})
		}
	}
	if !reversible {
		// Keep the inverse for diagnostics but mark the batch irreversible.
		return inverse, false, refusal
	}
	return inverse, true, ""
}

// --- handlers ---

func (s *Server) handleTableCommitV2(w http.ResponseWriter, r *http.Request) {
	var body commitRequestV2
	if !s.readMutationBody(w, r, &body) {
		return
	}
	if err := validateOperationID(body.OperationID); err != nil {
		writeDomainError(w, err)
		return
	}
	if err := validateCommitOps(body.Operations, s.commitOpLimit()); err != nil {
		writeDomainError(w, err)
		return
	}

	hash := canonicalCommitHash(body.ConnectionID, body.Operations)
	key := outcomeKey(body.ConnectionID, body.OperationID)
	store := s.outcomeRecords()
	resv := store.reserve(key, body.ConnectionID, hash)
	switch resv.kind {
	case "replay":
		out := copyResponseBody(resv.rec)
		out["replayed"] = true
		writeJSON(w, resv.rec.Status, out)
		return
	case "conflict":
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "operation_conflict",
			"error": fmt.Sprintf(
				"operation ID %q was already used with a different payload on this connection; operation IDs are single-use idempotency keys — send a new ID for different content",
				body.OperationID)})
		return
	case "in_progress":
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "in_progress",
			"error": "this operation ID is currently committing (a concurrent duplicate); retry the same payload to receive its outcome"})
		return
	case "unknown":
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "unknown",
			"error": "no retained outcome for this operation ID (expired, evicted, or the server restarted); its previous attempt cannot be replayed or safely repeated — verify the table state and use a new operation ID",
		})
		return
	}

	// Everything is validated before the transaction opens; a failure here
	// is a failed (rolled-back, never-applied) outcome for this ID.
	prepared, err := s.prepareCommitOps(r.Context(), body.ConnectionID, body.Operations)
	if err != nil {
		finishFailed(store, key, err, "commit prepare")
		writeMutationOutcome(w, err, "commit prepare")
		return
	}

	client, _ := s.clientFor(body.ConnectionID)
	tx, err := client.BeginTx(r.Context())
	if err != nil {
		log.Printf("studio: commit begin error: %v", err)
		body := map[string]any{"state": "unknown",
			"error": "the transaction could not be started; nothing was applied — " + sanitizeError(err)}
		store.finish(key, func(rec *outcomeRecord) {
			rec.State, rec.Status, rec.Response = outcomeFailed, http.StatusBadGateway, body
		})
		writeJSON(w, http.StatusBadGateway, body)
		return
	}
	defer tx.Rollback(r.Context())

	execs, err := runCommitOps(r.Context(), tx, prepared)
	if err != nil {
		status, out := classifyMutationOutcome(err, "commit")
		if out == nil {
			out = map[string]any{}
		}
		out["operationId"] = body.OperationID
		store.finish(key, func(rec *outcomeRecord) {
			rec.State, rec.Status, rec.Response = outcomeFailed, status, out
		})
		writeJSON(w, status, out)
		return
	}
	inverse, reversible, refusal := buildInverse(r.Context(), tx, prepared, execs)

	if err := tx.Commit(r.Context()); err != nil {
		// Ambiguous: the commit may or may not have persisted. The honest
		// outcome is unknown — never a failure receipt that invites a
		// blind retry, never a success that was not observed.
		log.Printf("studio: commit error: %v", err)
		out := map[string]any{
			"state": "unknown",
			"error": "the commit failed after the operations were applied; the outcome cannot be determined — inspect the table state before retrying with a NEW operation ID",
		}
		store.finish(key, func(rec *outcomeRecord) {
			rec.State, rec.Status, rec.Response = outcomeUnknown, http.StatusBadGateway, out
		})
		writeJSON(w, http.StatusBadGateway, out)
		return
	}

	out := commitResponseBody(body.OperationID, execs, reversible, refusal)
	store.finish(key, func(rec *outcomeRecord) {
		rec.State, rec.Status, rec.Response = outcomeCommitted, http.StatusOK, out
		rec.Reversible, rec.Refusal, rec.Inverse = reversible, refusal, inverse
	})
	writeJSON(w, http.StatusOK, out)
}

// finishFailed records a preparation-stage failure (nothing executed).
func finishFailed(store *outcomeStore, key string, err error, verb string) {
	status, out := classifyMutationOutcome(err, verb)
	if out == nil {
		out = map[string]any{}
	}
	store.finish(key, func(rec *outcomeRecord) {
		rec.State, rec.Status, rec.Response = outcomeFailed, status, out
	})
}

func commitResponseBody(operationID string, execs []opExecution, reversible bool, refusal string) map[string]any {
	results := make([]opResult, 0, len(execs))
	var total int64
	for _, ex := range execs {
		results = append(results, ex.result)
		total += ex.result.RowsAffected
	}
	out := map[string]any{
		"operationId":  operationID,
		"rowsAffected": total,
		"operations":   results,
		"reversible":   reversible,
	}
	if !reversible && refusal != "" {
		out["reversibleReason"] = refusal
	}
	return out
}

func copyResponseBody(rec *outcomeRecord) map[string]any {
	out := make(map[string]any, len(rec.Response)+1)
	for k, v := range rec.Response {
		out[k] = v
	}
	return out
}

func (s *Server) handleTablePreviewV2(w http.ResponseWriter, r *http.Request) {
	var body previewRequestV2
	if !s.readMutationBody(w, r, &body) {
		return
	}
	if err := validateCommitOps(body.Operations, s.commitOpLimit()); err != nil {
		writeDomainError(w, err)
		return
	}
	prepared, err := s.prepareCommitOps(r.Context(), body.ConnectionID, body.Operations)
	if err != nil {
		writeMutationOutcome(w, err, "preview prepare")
		return
	}
	client, ok := s.clientFor(body.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	tx, err := client.BeginTx(r.Context())
	if err != nil {
		log.Printf("studio: preview begin error: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": sanitizeError(err)})
		return
	}
	defer tx.Rollback(r.Context())
	execs, err := runCommitOps(r.Context(), tx, prepared)
	if err != nil {
		status, out := classifyMutationOutcome(err, "preview")
		writeJSON(w, status, out)
		return
	}
	// The transaction always rolls back (the deferred Rollback above);
	// preview reports only what WOULD change.
	ops := make([]map[string]any, 0, len(execs))
	counts := map[string]int{"insert": 0, "update": 0, "delete": 0}
	for _, p := range prepared {
		ex := execs[p.index]
		entry := map[string]any{
			"index": p.index, "op": p.kind, "schema": p.schema, "table": p.table,
		}
		switch p.kind {
		case "insert":
			entry["after"] = p.insRaw
			entry["key"] = ex.result.Key
			entry["version"] = ex.result.Version
		case "update":
			entry["key"] = p.keyCells
			entry["column"] = p.column
			entry["before"] = ex.before
			if p.isNull {
				entry["after"] = nil
			} else {
				entry["after"] = p.rawVal
			}
		case "delete":
			entry["key"] = p.keyCells
			entry["before"] = ex.beforeRow
		}
		counts[p.kind]++
		ops = append(ops, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "counts": counts, "operations": ops})
}

func (s *Server) handleTableOutcomeV2(w http.ResponseWriter, r *http.Request) {
	var body outcomeRequestV2
	if !s.readMutationBody(w, r, &body) {
		return
	}
	if err := validateOperationID(body.OperationID); err != nil {
		writeDomainError(w, err)
		return
	}
	rec, state := s.outcomeRecords().lookup(outcomeKey(body.ConnectionID, body.OperationID))
	switch state {
	case "absent", outcomeUnknown:
		writeJSON(w, http.StatusOK, map[string]any{
			"operationId": body.OperationID,
			"state":       outcomeUnknown,
			"error":       "no outcome is recorded for this operation ID on this connection (never seen, expired, evicted, or the server restarted)",
		})
		return
	case outcomeInProgress:
		writeJSON(w, http.StatusOK, map[string]any{
			"operationId": body.OperationID,
			"state":       outcomeInProgress,
			"error":       "this operation is currently committing",
		})
		return
	}
	out := map[string]any{
		"operationId": body.OperationID,
		"state":       rec.State,
		"status":      rec.Status,
		"response":    copyResponseBody(rec),
		"reversible":  rec.Reversible,
	}
	if !rec.Reversible && rec.Refusal != "" {
		out["reversibleReason"] = rec.Refusal
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleTableRevertV2(w http.ResponseWriter, r *http.Request) {
	var body revertRequestV2
	if !s.readMutationBody(w, r, &body) {
		return
	}
	if err := validateOperationID(body.OperationID); err != nil {
		writeDomainError(w, err)
		return
	}
	if err := validateOperationID(body.RevertOperationID); err != nil {
		writeDomainError(w, mutationDomainError{msg: "revertOperationId is required — a fresh client-generated idempotency key for the revert itself (reverts are commits and are deduplicated like any other)"})
		return
	}

	rec, state := s.outcomeRecords().lookup(outcomeKey(body.ConnectionID, body.OperationID))
	switch state {
	case "absent", outcomeUnknown:
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": outcomeUnknown,
			"error": "no retained outcome for this operation ID (expired, evicted, never committed here, or the server restarted); the recorded inverse is gone — the database state must be repaired manually",
		})
		return
	case outcomeInProgress:
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": outcomeInProgress,
			"error": "this operation is still committing; wait for its outcome before reverting",
		})
		return
	case outcomeFailed:
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "irreversible",
			"error": "this operation failed and was rolled back — nothing was applied, so there is nothing to revert",
		})
		return
	}
	if rec.State != outcomeCommitted {
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "irreversible",
			"error": fmt.Sprintf("only committed operations can be reverted (outcome state %q)", rec.State),
		})
		return
	}
	if !rec.Reversible {
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "irreversible",
			"error": "this commit is not reversible: " + rec.Refusal,
		})
		return
	}

	// The revert is itself a deduplicated commit under its own key.
	revertKey := outcomeKey(body.ConnectionID, body.RevertOperationID)
	hash := canonicalCommitHash(body.ConnectionID, rec.Inverse)
	resv := s.outcomeRecords().reserve(revertKey, body.ConnectionID, hash)
	switch resv.kind {
	case "replay":
		out := copyResponseBody(resv.rec)
		out["replayed"] = true
		writeJSON(w, resv.rec.Status, out)
		return
	case "conflict":
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "operation_conflict",
			"error": "revertOperationId was already used with a different payload; use a fresh ID"})
		return
	case "in_progress":
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "in_progress",
			"error": "this revert is currently committing; retry to receive its outcome"})
		return
	case "unknown":
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "unknown",
			"error": "no retained outcome for revertOperationId; verify the table state and use a new operation ID",
		})
		return
	}

	store := s.outcomeRecords()
	prepared, err := s.prepareCommitOps(r.Context(), body.ConnectionID, rec.Inverse)
	if err != nil {
		finishFailed(store, revertKey, err, "revert prepare")
		if se, ok := err.(rowStateError); ok && se.state == "binding" {
			se.msg = "the recorded inverse no longer matches the live relation (reconnected or table replaced); reload and repair manually: " + se.msg
			writeMutationOutcome(w, se, "revert prepare")
			return
		}
		writeMutationOutcome(w, err, "revert prepare")
		return
	}
	client, _ := s.clientFor(body.ConnectionID)
	tx, err := client.BeginTx(r.Context())
	if err != nil {
		log.Printf("studio: revert begin error: %v", err)
		out := map[string]any{"error": sanitizeError(err)}
		store.finish(revertKey, func(rec *outcomeRecord) {
			rec.State, rec.Status, rec.Response = outcomeFailed, http.StatusBadGateway, out
		})
		writeJSON(w, http.StatusBadGateway, out)
		return
	}
	defer tx.Rollback(r.Context())
	execs, err := runCommitOps(r.Context(), tx, prepared)
	if err != nil {
		status, out := classifyMutationOutcome(err, "revert")
		if out == nil {
			out = map[string]any{}
		}
		store.finish(revertKey, func(rec *outcomeRecord) {
			rec.State, rec.Status, rec.Response = outcomeFailed, status, out
		})
		writeJSON(w, status, out)
		return
	}
	// runCommitOps re-probed FK side effects for every inverse update/delete
	// INSIDE this transaction (execs[i].fkSide/fkErr). Between the commit and
	// this revert a child row may have started honoring a cascade/set-null/
	// set-default rule against the committed value; applying the inverse
	// would change that child silently. Abort before Commit: the deferred
	// Rollback discards everything, and the refusal explains what to repair
	// (S02 review F1).
	for i := range execs {
		if execs[i].fkErr != nil {
			out := map[string]any{
				"state": "irreversible",
				"error": fmt.Sprintf(
					"operations[%d]: the revert could not be verified to stay row-precise (%v); nothing was applied — repair the rows that changed after the commit",
					i, sanitizeError(execs[i].fkErr)),
			}
			store.finish(revertKey, func(rec *outcomeRecord) {
				rec.State, rec.Status, rec.Response = outcomeFailed, http.StatusConflict, out
			})
			writeJSON(w, http.StatusConflict, out)
			return
		}
		if execs[i].fkSide {
			out := map[string]any{
				"state": "irreversible",
				"error": fmt.Sprintf(
					"operations[%d]: rows now reference the committed value through an ON DELETE/UPDATE cascade/set-null/set-default rule (they appeared after the commit); reverting would change them silently — nothing was applied",
					i),
			}
			store.finish(revertKey, func(rec *outcomeRecord) {
				rec.State, rec.Status, rec.Response = outcomeFailed, http.StatusConflict, out
			})
			writeJSON(w, http.StatusConflict, out)
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		log.Printf("studio: revert commit error: %v", err)
		out := map[string]any{
			"state": "unknown",
			"error": "the revert failed after the inverse operations were applied; the outcome cannot be determined — inspect the table state before retrying with a NEW operation ID",
		}
		store.finish(revertKey, func(rec *outcomeRecord) {
			rec.State, rec.Status, rec.Response = outcomeUnknown, http.StatusBadGateway, out
		})
		writeJSON(w, http.StatusBadGateway, out)
		return
	}

	out := commitResponseBody(body.RevertOperationID, execs, false,
		"a revert is itself a commit; re-commit the original operations if you need the change back")
	out["reverted"] = body.OperationID
	store.finish(revertKey, func(rec *outcomeRecord) {
		rec.State, rec.Status, rec.Response = outcomeCommitted, http.StatusOK, out
		rec.Reversible, rec.Refusal = false, "revert outcomes are terminal"
	})
	writeJSON(w, http.StatusOK, out)
}
