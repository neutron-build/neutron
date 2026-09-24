package db

// Journaled operational migrations (M06).
//
// M05 ships the minimal honest nontransactional path: statement-by-statement
// concurrent-index DDL, structure-only, DML refused, recovery via explicit
// resolve with structural postconditions. M06 formalizes the operational
// shapes on top of that architecture — nothing M05 guards is re-implemented
// here. A migration file OPTS INTO the journal with a `-- neutron:journaled`
// line comment before its first executable statement; unmarked files keep
// the exact M05 behavior (the M05 battery pins it).
//
// The journal model: every step (one executable statement) carries a
// verification — either a built-in structural postcondition from M05's
// StatementPostconditionOf (CREATE/DROP of tables, indexes, types, schemas —
// the concurrent variants included) or a declared one:
//
//	-- neutron:step verify="SELECT count(*) FROM t WHERE x IS NULL" expect="0"
//	UPDATE t SET x = 0 WHERE x IS NULL AND id <= 500;
//
// The declared verify query runs inside a READ ONLY transaction; its single
// scalar result is compared as text against expect. Verification is what
// makes the operational shapes safe to interrupt: retry and status evaluate
// each step's effect and skip what is provably present (idempotent re-runs),
// exactly like M05's resolve already did for structural postconditions.
//
// Per-step knobs: lock_timeout (default 10s) and statement_timeout (default
// 0s = unbounded). Transaction-capable steps run in their own transaction
// with the knobs applied through SET LOCAL — the one SET form the migration
// allowlist permits, used for exactly this purpose. Concurrent-index steps
// cannot run in a transaction, so the runner applies the knobs at session
// level on its own pinned connection and RESETs them afterwards; migration
// files never need (and cannot use) session-level SET.
//
// Concurrent-index lifecycle: a failed CREATE INDEX CONCURRENTLY leaves an
// INVALID index. For journaled index steps the debris recovery is built in:
// DROP INDEX CONCURRENTLY the invalid index, then re-run the CREATE — judged
// against PostgreSQL 17, where REINDEX CONCURRENTLY is the alternative:
// REINDEX is outside the migration allowlist as an opaque maintenance kind
// (M05 pass-3), and its own failure mode leaves a second debris class (the
// *_ccnew index). DROP+recreate is expressible entirely in allowlisted,
// guarded kinds and drops only the named invalid index of this step. M05's
// by-hand debris recovery for UNMARKED files is unchanged.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Journal directive prefixes (line comments only; block comments are ignored
// by the grammar on purpose — a directive hidden in /* */ would be silently
// inert, so the grammar refuses to guess).
const (
	journalMarkerDirective  = "neutron:journaled"
	journalStepDirective    = "neutron:step"
	journalDirectivePrefix  = "neutron:"
	journalAnnotationQuoted = '"'
)

// Journaled step defaults. lock_timeout is bounded by default so an
// operational step queues behind a lock for at most 10s before failing
// honestly (resumable); statement_timeout defaults to unbounded because
// legitimate backfill batches and index builds can be slow — both are
// per-step overridable and documented in the runbook.
const (
	JournaledDefaultLockTimeout      = 10 * time.Second
	JournaledDefaultStatementTimeout = 0 * time.Second
)

// StepAnnotation is the parsed `-- neutron:step key=value ...` directive.
type StepAnnotation struct {
	// Verify is the declared verification query (a single SELECT). Empty
	// when the step relies on a built-in postcondition.
	Verify string
	// Expect is the text form the verify query's scalar result must have.
	Expect string
	// LockTimeout/StatementTimeout are canonical durations ("" = default).
	LockTimeout      string
	StatementTimeout string
	// OnFailure is the declared on-failure action; only "stop" exists.
	OnFailure string
}

// JournaledStep is one executable statement of a journaled migration with
// its verification and transaction configuration.
type JournaledStep struct {
	Index      int    // 1-based step number
	Statement  string // verbatim executable SQL (comments stripped by classifiers, kept in text)
	FirstLine  string
	Kind       string // StatementKind of the statement
	Annotation *StepAnnotation
	// ConfigSQL holds SET LOCAL statements preceding this step; the runner
	// executes them inside the step's transaction.
	ConfigSQL []string
	// BuiltIn is the structural postcondition, when one exists.
	BuiltIn    StatementPostcondition
	HasBuiltIn bool
}

// Verify returns the step's declared verification query ("" when the step
// verifies through its built-in postcondition).
func (s *JournaledStep) Verify() string {
	if s.Annotation == nil {
		return ""
	}
	return s.Annotation.Verify
}

// Concurrent reports whether the step's statement cannot run inside a
// transaction (concurrent index operations).
func (s *JournaledStep) Concurrent() bool {
	return IsNontransactionalStatement(s.Statement)
}

// pgInterval renders a duration in the one spelling both the runner and
// PostgreSQL accept for the timeout GUCs: bare milliseconds ("60000ms").
// Go's own canonical rendering rewrites 60s as "1m0s", which PostgreSQL
// interval parsing rejects (SQLSTATE 22023) — never send d.String().
func pgInterval(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Milliseconds())
}

// EffectiveLockTimeout / EffectiveStatementTimeout resolve the step's knobs
// against the documented defaults, rendered for the server.
func (s *JournaledStep) EffectiveLockTimeout() string {
	if s.Annotation != nil && s.Annotation.LockTimeout != "" {
		return s.Annotation.LockTimeout
	}
	return pgInterval(JournaledDefaultLockTimeout)
}

func (s *JournaledStep) EffectiveStatementTimeout() string {
	if s.Annotation != nil && s.Annotation.StatementTimeout != "" {
		return s.Annotation.StatementTimeout
	}
	return pgInterval(JournaledDefaultStatementTimeout)
}

// JournaledFile is the parsed journal of one migration file.
type JournaledFile struct {
	Steps []JournaledStep
}

// HasJournaledMarker reports whether a SQL document carries the journal
// marker (`-- neutron:journaled`). Down-file validation refuses markers;
// up-file parsing dispatches on it.
func HasJournaledMarker(sql string) bool {
	for _, frag := range SplitSQLStatements(sql) {
		if hasExecutableSQL(frag) {
			return false // marker must precede any executable statement
		}
		if fragmentHasMarker(frag) {
			return true
		}
	}
	return false
}

// fragmentHasMarker scans a comments-only fragment for the marker line.
func fragmentHasMarker(frag string) bool {
	_, isMarker, _ := parseJournalDirectives(frag)
	return isMarker
}

// RefuseJournaledDown refuses journal markers in down files: downs run in
// one transaction under the existing reversibility limits, and a journal
// marker there would imply per-step semantics this runner does not provide
// on the down path.
func RefuseJournaledDown(downSQL string) error {
	if HasJournaledMarker(downSQL) {
		return fmt.Errorf("down file carries a `-- neutron:journaled` marker — down migrations run in one transaction with the reversibility limits; remove the marker (concurrent drops never belong in down files)")
	}
	return nil
}

// ParseJournaledFile parses a migration file's journal. It returns
// (nil, nil) when the file carries no marker (the caller keeps the M05
// paths), and a validated JournaledFile otherwise. All validation is
// pre-execution: a malformed journal fails the whole batch before any
// statement runs.
func ParseJournaledFile(sql string) (*JournaledFile, error) {
	frags := SplitSQLStatements(sql)

	var steps []JournaledStep
	var pending *StepAnnotation
	var config []string
	markerSeen := false
	execSeen := false

	stepErr := func(i int, stmt, format string, args ...any) error {
		return fmt.Errorf("journaled step %d (%s): %s", i, firstSQLLine(stmt), fmt.Sprintf(format, args...))
	}

	for _, frag := range frags {
		if !hasExecutableSQL(frag) {
			directives, isMarker, derr := parseJournalDirectives(frag)
			if derr != nil {
				return nil, derr
			}
			if isMarker {
				if markerSeen {
					return nil, errors.New("duplicate `-- neutron:journaled` marker")
				}
				if execSeen {
					return nil, errors.New("`-- neutron:journaled` marker must precede every executable statement")
				}
				markerSeen = true
			}
			for _, d := range directives {
				if pending != nil {
					return nil, errors.New("two -- neutron:step annotations for one statement")
				}
				if !markerSeen {
					return nil, fmt.Errorf("-- neutron:step annotation before any `-- neutron:journaled` marker — add the marker header first")
				}
				pending = d
			}
			continue
		}

		execSeen = true
		kind := StatementKind(frag)
		if kind == "set local" {
			config = append(config, frag)
			continue
		}
		step := JournaledStep{
			Index:      len(steps) + 1,
			Statement:  frag,
			FirstLine:  firstSQLLine(frag),
			Kind:       kind,
			Annotation: pending,
			ConfigSQL:  config,
		}
		if post, ok := StatementPostconditionOf(frag); ok {
			step.BuiltIn, step.HasBuiltIn = post, true
		}
		pending, config = nil, nil
		steps = append(steps, step)
	}

	if !markerSeen {
		return nil, nil
	}
	if pending != nil {
		return nil, errors.New("dangling -- neutron:step annotation with no following statement")
	}
	if len(config) > 0 {
		return nil, errors.New("SET LOCAL statement with no following step to attach to")
	}

	for i := range steps {
		step := &steps[i]
		if step.Kind == "" {
			return nil, fmt.Errorf("journaled step %d (%s): statement kind is outside the migration allowlist — refused with the batch", step.Index, step.FirstLine)
		}
		if step.Annotation != nil {
			a := step.Annotation
			if a.Verify == "" {
				return nil, stepErr(step.Index, step.Statement, "annotation declares no verify — drop the annotation (built-in postconditions need none) or add verify=\"SELECT ...\"")
			}
			if a.Expect == "" {
				return nil, stepErr(step.Index, step.Statement, "verify declared without expect — the expected scalar (compared as text) is required")
			}
			if kind := StatementKind(a.Verify); kind != "select" {
				return nil, stepErr(step.Index, step.Statement, "verify query must be a single SELECT (got kind %q) — verification is read-only by contract", kind)
			}
			if a.OnFailure != "" && a.OnFailure != "stop" {
				return nil, stepErr(step.Index, step.Statement, "on-failure=%q is not supported (only \"stop\"; recovery stays explicit via resolve)", a.OnFailure)
			}
			for knob, val := range map[string]string{"lock_timeout": a.LockTimeout, "statement_timeout": a.StatementTimeout} {
				if val == "" {
					continue
				}
				if err := validateTimeoutKnob(knob, val); err != nil {
					return nil, stepErr(step.Index, step.Statement, "%v", err)
				}
			}
		}
		if !step.HasBuiltIn && step.Annotation == nil {
			return nil, stepErr(step.Index, step.Statement, "no verifiable effect — this statement kind leaves no built-in postcondition; declare one: -- neutron:step verify=\"SELECT ...\" expect=\"...\"")
		}
		if step.Concurrent() && len(step.ConfigSQL) > 0 {
			return nil, stepErr(step.Index, step.Statement, "SET LOCAL cannot attach to a concurrent-index step (it cannot run in a transaction) — use the step's lock_timeout/statement_timeout knobs")
		}
	}
	return &JournaledFile{Steps: steps}, nil
}

// validateTimeoutKnob accepts Go duration spellings of at least 1ms and
// stores the canonical re-render (what the runner sends to the server).
func validateTimeoutKnob(name, val string) error {
	d, err := time.ParseDuration(val)
	if err != nil {
		return fmt.Errorf("%s=%q is not a duration (examples: 2s, 500ms, 1m30s, 0)", name, val)
	}
	if d < time.Millisecond {
		return fmt.Errorf("%s=%q is below 1ms", name, val)
	}
	return nil
}

// parseJournalDirectives scans one comments-only fragment for journal
// directives. It returns the step annotations found (usually zero or one),
// whether the file marker is present, and a parse error for malformed or
// unknown directives — a typo'd directive must never silently degrade the
// journal contract.
func parseJournalDirectives(frag string) (directives []*StepAnnotation, isMarker bool, err error) {
	for _, line := range strings.Split(frag, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "--") {
			continue
		}
		inner := strings.TrimSpace(trimmed[2:])
		if !strings.HasPrefix(inner, journalDirectivePrefix) {
			continue
		}
		rest := strings.TrimPrefix(inner, journalDirectivePrefix)
		word, args, _ := strings.Cut(rest, " ")
		switch word {
		case "journaled":
			isMarker = true
		case "step":
			a, err := parseStepAnnotation(args)
			if err != nil {
				return nil, false, fmt.Errorf("-- neutron:step %s: %w", args, err)
			}
			directives = append(directives, a)
		default:
			return nil, false, fmt.Errorf("unknown journal directive %q (known: neutron:journaled, neutron:step)", "neutron:"+word)
		}
	}
	return directives, isMarker, nil
}

// parseStepAnnotation parses the key=value list of a -- neutron:step line.
func parseStepAnnotation(args string) (*StepAnnotation, error) {
	a := &StepAnnotation{}
	fields, err := splitAnnotationFields(args)
	if err != nil {
		return nil, err
	}
	for _, f := range fields {
		key, val, ok := strings.Cut(f, "=")
		if !ok {
			return nil, fmt.Errorf("annotation field %q is not key=value", f)
		}
		switch key {
		case "verify":
			a.Verify = val
		case "expect":
			a.Expect = val
		case "lock_timeout", "statement_timeout":
			if err := validateTimeoutKnob(key, val); err != nil {
				return nil, err
			}
			d, _ := time.ParseDuration(val)
			if key == "lock_timeout" {
				a.LockTimeout = pgInterval(d)
			} else {
				a.StatementTimeout = pgInterval(d)
			}
		case "on-failure":
			a.OnFailure = val
		default:
			return nil, fmt.Errorf("unknown annotation key %q", key)
		}
	}
	return a, nil
}

// splitAnnotationFields splits the annotation argument list on whitespace,
// honoring double-quoted values with \" and \\ escapes.
func splitAnnotationFields(s string) ([]string, error) {
	var fields []string
	var cur strings.Builder
	inQuote := false
	escaped := false
	flush := func() {
		if cur.Len() > 0 {
			fields = append(fields, cur.String())
			cur.Reset()
		}
	}
	for _, r := range s {
		switch {
		case escaped:
			cur.WriteRune(r)
			escaped = false
		case inQuote && r == '\\':
			escaped = true
		case r == journalAnnotationQuoted:
			inQuote = !inQuote
			// Quotes delimit; the value keeps its inner text.
		case !inQuote && (r == ' ' || r == '\t'):
			flush()
		default:
			cur.WriteRune(r)
		}
	}
	if inQuote {
		return nil, fmt.Errorf("unterminated quoted annotation value")
	}
	if escaped {
		return nil, fmt.Errorf("dangling escape in annotation value")
	}
	flush()
	return fields, nil
}

// ---------------------------------------------------------------------------
// Step evaluation (verification)
// ---------------------------------------------------------------------------

// Journaled step states: the same vocabulary as M05's postcondition states.
const (
	JournalStateSatisfied   = "satisfied"
	JournalStateUnsatisfied = "unsatisfied"
	JournalStateInvalid     = "invalid"
)

// JournaledVerifyError reports a step whose verification did not hold after
// the statement ran (or could not be evaluated): the honest failure of the
// journal contract. Never retried automatically.
type JournaledVerifyError struct {
	StepIndex int
	FirstLine string
	Verify    string
	Expect    string
	Got       string
	Err       error
}

func (e *JournaledVerifyError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("journaled step %d (%s): verification could not be evaluated: %v (verify: %s)", e.StepIndex, e.FirstLine, e.Err, e.Verify)
	}
	return fmt.Sprintf("journaled step %d (%s): verification mismatch — verify %s returned %q, want %q", e.StepIndex, e.FirstLine, e.Verify, e.Got, e.Expect)
}

func (e *JournaledVerifyError) Unwrap() error { return e.Err }

// JournaledIdentityError reports a step REFUSED BEFORE EXECUTION because
// its effect identity could not be pinned, is ambiguous, or provably
// collides with another relation's object: the runner never skips, drops
// or verifies an index step against a bare name — identity is
// schema+table+name (M06 rework, review-1 MAJOR-1/2, MINOR-1). The
// statement did not run; earlier steps' effects (if any) remain.
type JournaledIdentityError struct {
	StepIndex int
	FirstLine string
	Detail    string
}

func (e *JournaledIdentityError) Error() string {
	return fmt.Sprintf("journaled step %d (%s): %s", e.StepIndex, e.FirstLine, e.Detail)
}

// EvaluateJournaledStep checks one step's verification against the live
// database. Built-in postconditions use M05's EvaluatePostcondition —
// except index-create postconditions, which evaluate by full identity
// (schema+table+name; see evaluateIndexCreateIdentity). Declared verify
// queries run inside a READ ONLY transaction (the read-only contract is
// enforced by the server, not just by parsing) and their single scalar
// result is compared as text against expect. SQL NULL never matches an
// expectation.
func (c *Client) EvaluateJournaledStep(ctx context.Context, step *JournaledStep) (state, detail string, err error) {
	if step.HasBuiltIn {
		if step.BuiltIn.Kind == PostconditionIndexCreate {
			return c.evaluateIndexCreateIdentity(ctx, step)
		}
		state, err := c.EvaluatePostcondition(ctx, step.BuiltIn)
		return state, "", err
	}
	got, err := c.EvaluateVerifyQuery(ctx, step.Annotation.Verify)
	if err != nil {
		return "", "", err
	}
	if got == step.Annotation.Expect {
		return JournalStateSatisfied, "", nil
	}
	return JournalStateUnsatisfied, fmt.Sprintf("verify returned %q, want %q", got, step.Annotation.Expect), nil
}

// EvaluateVerifyQuery runs a declared verification query in a READ ONLY
// transaction and returns its single scalar result as text.
func (c *Client) EvaluateVerifyQuery(ctx context.Context, verify string) (string, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return "", fmt.Errorf("acquire verification connection: %w", err)
	}
	defer conn.Release()
	tx, err := conn.Conn().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return "", fmt.Errorf("begin read-only verification tx: %w", err)
	}
	defer tx.Rollback(ctx)
	var got *string
	if err := tx.QueryRow(ctx, "SELECT ("+verify+")::text").Scan(&got); err != nil {
		return "", fmt.Errorf("verify query failed: %w", err)
	}
	if got == nil {
		return "<NULL>", nil
	}
	return *got, nil
}

// evaluateIndexCreateIdentity evaluates an index-create step by full
// identity — schema + table + name (M06 rework, review-1 MAJOR-1/MAJOR-2).
// The former name-only probe let a same-name index in ANY schema or table
// satisfy the step (silent skip + history + index never built) and raised
// SQLSTATE 21000 on name twins after the CREATE had already succeeded;
// identity makes every verdict name-twin-proof by construction.
func (c *Client) evaluateIndexCreateIdentity(ctx context.Context, step *JournaledStep) (string, string, error) {
	p := step.BuiltIn
	schema, tableOid, err := c.pinIndexTargetSchema(ctx, p)
	if err != nil {
		var ident *JournaledIdentityError
		if errors.As(err, &ident) && ident.StepIndex == 0 {
			ident.StepIndex, ident.FirstLine = step.Index, step.FirstLine
		}
		return "", "", err
	}
	if tableOid == 0 {
		return JournalStateUnsatisfied, fmt.Sprintf("target table %s is absent — no index can exist on it", regclassRef(p.Schema, p.Table)), nil
	}
	exists, valid, err := c.indexStatusByIdentity(ctx, tableOid, schema, p.Name)
	if err != nil {
		return "", "", err
	}
	switch {
	case exists && valid:
		return JournalStateSatisfied, "", nil
	case exists:
		return JournalStateInvalid, "", nil
	default:
		return JournalStateUnsatisfied, "", nil
	}
}

// pinIndexTargetSchema resolves the schema an index-create
// postcondition's target table lives in — the identity half the name-only
// probe lacked. Qualified references resolve directly; unqualified
// references resolve exactly the way the server resolves the statement's
// own table reference and the runner's other catalog checks do
// (RelationExists: to_regclass through the session search path), so
// evaluation and execution can never disagree. tableOid == 0 with a nil
// error is returned only for a QUALIFIED reference whose table is absent:
// the identity is pinned, so "no index can exist on it" is provable. An
// unqualified reference whose table cannot be resolved is an identity
// REFUSAL — without a schema the step's effect has no pinnable identity,
// and no skip, drop or verdict may be derived from a bare name.
func (c *Client) pinIndexTargetSchema(ctx context.Context, p StatementPostcondition) (string, uint32, error) {
	var oid *uint32
	if err := c.pool.QueryRow(ctx, "SELECT to_regclass($1)::oid", regclassRef(p.Schema, p.Table)).Scan(&oid); err != nil {
		return "", 0, err
	}
	if oid == nil {
		if p.Schema != "" {
			return p.Schema, 0, nil
		}
		return "", 0, &JournaledIdentityError{Detail: fmt.Sprintf(
			"the unqualified table %q cannot be resolved through the search path, so this index step's effect has no pinnable identity (schema+table+name) — qualify the table (schema.table) in the statement, or make sure the table exists in a searchable schema; a bare index name must never decide skip, drop or verdict", p.Table)}
	}
	var schema string
	if err := c.pool.QueryRow(ctx, `
		SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.oid = $1`, *oid).Scan(&schema); err != nil {
		return "", 0, err
	}
	return schema, *oid, nil
}

// indexStatusByIdentity reports whether the index named name attached to
// the pinned table relation exists and is valid. Identity-complete by
// construction: the table oid plus the index's schema+name admit at most
// one catalog row (index names are unique per schema), so same-name
// indexes on other tables or in other schemas can never satisfy, miss or
// blow up the probe — the 21000 the name-only scalar subquery raised on
// name twins is unreachable here.
func (c *Client) indexStatusByIdentity(ctx context.Context, tableOid uint32, schema, name string) (exists, valid bool, err error) {
	err = c.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_index i
			JOIN pg_class ic ON ic.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = ic.relnamespace
			WHERE i.indrelid = $1 AND ic.relname = $3 AND n.nspname = $2
		),
		COALESCE((
			SELECT i.indisvalid FROM pg_index i
			JOIN pg_class ic ON ic.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = ic.relnamespace
			WHERE i.indrelid = $1 AND ic.relname = $3 AND n.nspname = $2
		), false)`, tableOid, schema, name).Scan(&exists, &valid)
	return exists, valid, err
}

// IndexDebrisIdentity checks the INVALID index this step's debris path
// intends to drop — already pinned to the step's own resolved table by
// the identity-aware evaluation — for extension membership. The drop is
// runner-internal (it never passes the file-level statement guard), so
// the protected-object rule is enforced here directly: an extension
// member is never dropped, whatever flags are passed. exists=false when
// the invalid index vanished between evaluation and the drop (someone
// else cleaned it; the CREATE then proceeds and succeeds or fails
// honestly on its own).
func (c *Client) IndexDebrisIdentity(ctx context.Context, tableOid uint32, schema, name string) (exists, extensionMember bool, err error) {
	err = c.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_index i
			JOIN pg_class ic ON ic.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = ic.relnamespace
			WHERE i.indrelid = $1 AND ic.relname = $3 AND n.nspname = $2
			  AND NOT i.indisvalid
		),
		EXISTS (
			SELECT 1 FROM pg_depend d
			JOIN pg_index i ON i.indexrelid = d.objid
			JOIN pg_class ic ON ic.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = ic.relnamespace
			WHERE i.indrelid = $1 AND ic.relname = $3 AND n.nspname = $2
			  AND NOT i.indisvalid
			  AND d.classid = 'pg_class'::regclass AND d.deptype = 'e'
		)`, tableOid, schema, name).Scan(&exists, &extensionMember)
	return exists, extensionMember, err
}

// relationNameOwnerInSchema reports the relation currently holding name
// in schema ("" when the name is free). pg_class names are unique per
// namespace, so at most one holder exists; a table or sequence carrying
// the step's index name collides with the CREATE exactly like an index
// would.
func (c *Client) relationNameOwnerInSchema(ctx context.Context, schema, name string) (string, bool, error) {
	var owner string
	err := c.pool.QueryRow(ctx, `
		SELECT CASE ic.relkind
			WHEN 'i' THEN 'index on table ' || ct.relname
			WHEN 'I' THEN 'partitioned index'
			ELSE 'relation ' || ic.relname || ' of kind ' || ic.relkind::text
		END
		FROM pg_class ic
		JOIN pg_namespace n ON n.oid = ic.relnamespace
		LEFT JOIN pg_index i ON i.indexrelid = ic.oid
		LEFT JOIN pg_class ct ON ct.oid = i.indrelid
		WHERE ic.relname = $2 AND n.nspname = $1`, schema, name).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return owner, true, nil
}

// ---------------------------------------------------------------------------
// Concurrent-index progress (pg_stat_progress_create_index)
// ---------------------------------------------------------------------------

// IndexBuildProgress is one observation of pg_stat_progress_create_index.
type IndexBuildProgress struct {
	Table        string
	Phase        string
	BlocksDone   int64
	BlocksTotal  int64
	TuplesDone   int64
	TuplesTotal  int64
	LockersDone  int64
	LockersTotal int64
}

// WatchCreateIndexProgress polls pg_stat_progress_create_index for builds on
// the named table until ctx ends, calling onProgress on each change. Best
// effort by contract: a server without the view (or a query error) ends the
// watcher silently — progress reporting must never fail a migration.
func (c *Client) WatchCreateIndexProgress(ctx context.Context, table string, onProgress func(IndexBuildProgress)) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last string
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		rows, err := c.pool.Query(ctx, `
			SELECT p.relid::regclass::text, p.phase,
			       p.blocks_done, p.blocks_total, p.tuples_done, p.tuples_total,
			       p.lockers_done, p.lockers_total
			FROM pg_stat_progress_create_index p
			WHERE p.command = 'CREATE INDEX' AND ($1 = '' OR p.relid = $1::regclass)`, table)
		if err != nil {
			return
		}
		for rows.Next() {
			var pr IndexBuildProgress
			if err := rows.Scan(&pr.Table, &pr.Phase, &pr.BlocksDone, &pr.BlocksTotal, &pr.TuplesDone, &pr.TuplesTotal, &pr.LockersDone, &pr.LockersTotal); err != nil {
				rows.Close()
				return
			}
			key := fmt.Sprintf("%s|%s|%d/%d", pr.Table, pr.Phase, pr.BlocksDone, pr.BlocksTotal)
			if key != last {
				last = key
				onProgress(pr)
			}
		}
		rows.Close()
		if rows.Err() != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// The journaled executor
// ---------------------------------------------------------------------------

// JournaledRunEvent reports one step's execution decision (CLI progress and
// tests consume it).
type JournaledRunEvent struct {
	StepIndex int
	Action    string // "skip" | "run" | "debris-drop" | "progress"
	Detail    string
	Rows      int64
}

// ApplyJournaledMigration executes a journaled migration step-by-step on the
// pinned advisory-lock session and records the history row once every step
// is verified. Per step: evaluate (verified effects are skipped — the
// idempotency contract), execute with the step's timeout knobs (per-step
// transaction with SET LOCAL, or session-level around concurrent-index
// statements, which cannot run in a transaction), re-evaluate, and fail
// honestly on a mismatch. INVALID debris of a concurrent-index step is
// dropped (DROP INDEX CONCURRENTLY) and the create re-run — the documented
// PostgreSQL 17 lifecycle judgment. client is used only for best-effort
// progress observation (pg_stat_progress_create_index); all effects run on
// the locked session.
func (s *MigrationSession) ApplyJournaledMigration(ctx context.Context, client *Client, mf MigrationFile, jf *JournaledFile, onEvent func(JournaledRunEvent)) error {
	emit := func(e JournaledRunEvent) {
		if onEvent != nil {
			onEvent(e)
		}
	}
	for i := range jf.Steps {
		step := &jf.Steps[i]

		state, _, err := client.EvaluateJournaledStep(ctx, step)
		if err != nil {
			var ident *JournaledIdentityError
			if errors.As(err, &ident) {
				// Refused BEFORE execution: the step's effect identity
				// could not be pinned (or collides). The statement did
				// not run; earlier steps' effects remain.
				return ident
			}
			// Other evaluation errors (e.g. a declared verify whose
			// subject does not exist yet) fall through to execution —
			// the statement may create what the verify reads; the
			// post-run check decides honestly.
		}
		if err == nil && state == JournalStateSatisfied {
			emit(JournaledRunEvent{StepIndex: step.Index, Action: "skip", Detail: "verified effect present"})
			continue
		}

		// Index-create steps act on their full identity (schema+table+
		// name): evaluation proved it pinnable whenever it returned a
		// state, so pin it once more for the arms below.
		if err == nil && step.HasBuiltIn && step.BuiltIn.Kind == PostconditionIndexCreate {
			schema, tableOid, pinErr := client.pinIndexTargetSchema(ctx, step.BuiltIn)
			if pinErr != nil {
				var ident *JournaledIdentityError
				if errors.As(pinErr, &ident) && ident.StepIndex == 0 {
					ident.StepIndex, ident.FirstLine = step.Index, step.FirstLine
				}
				return pinErr
			}

			if state == JournalStateInvalid {
				// An INVALID index with this step's exact identity is
				// attached to this step's own table (the debris of a
				// failed build). Only creation-side index postconditions
				// can report invalid.
				if !step.Concurrent() {
					return &JournaledIdentityError{StepIndex: step.Index, FirstLine: step.FirstLine,
						Detail: fmt.Sprintf("an INVALID index %s already occupies this step's own target %s — a plain CREATE INDEX cannot recover debris (the name is taken); drop the invalid index by hand, or declare the step CREATE INDEX CONCURRENTLY so the runner can drop-and-rebuild it",
							qualifiedIndexName(schema, step.BuiltIn.Name), qualifiedIndexName(schema, step.BuiltIn.Table))}
				}
				// The drop is runner-internal and bypasses the file-level
				// statement guard, so the protected-object rule is
				// enforced directly on the pinned debris: an extension
				// member is never dropped (_neutron_/extension protection
				// stays absolute through the journal surface).
				exists, extMember, idErr := client.IndexDebrisIdentity(ctx, tableOid, schema, step.BuiltIn.Name)
				if idErr != nil {
					return &NontransactionalPartialError{Applied: i, Total: len(jf.Steps), Err: fmt.Errorf("debris identity check failed: %w", idErr)}
				}
				if extMember {
					return &JournaledIdentityError{StepIndex: step.Index, FirstLine: step.FirstLine,
						Detail: fmt.Sprintf("the INVALID index %s is an extension member (protected) — never dropped by the runner; resolve it by hand if that is really intended",
							qualifiedIndexName(schema, step.BuiltIn.Name))}
				}
				if exists {
					emit(JournaledRunEvent{StepIndex: step.Index, Action: "debris-drop", Detail: "INVALID index from a failed build — dropping concurrently and rebuilding"})
					drop := "DROP INDEX CONCURRENTLY IF EXISTS " + qualifiedIndexName(schema, step.BuiltIn.Name)
					if _, err := s.execConcurrent(ctx, client, step, drop, emit); err != nil {
						return &NontransactionalPartialError{Applied: i, Total: len(jf.Steps), Err: fmt.Errorf("debris drop failed: %w", err)}
					}
				}
				// exists == false: the debris vanished between evaluation
				// and the drop — the CREATE decides honestly on its own.
			} else if state == JournalStateUnsatisfied && tableOid != 0 {
				// Pre-execution collision honesty: the identity is pinned
				// and free, but the NAME within the resolved schema may
				// be held by another relation — the CREATE would fail
				// 42P07. Refuse before execution, naming the holder.
				// Foreign-schema name twins are irrelevant here (index
				// names are unique per schema): the CREATE proceeds and
				// builds — never a name-only skip.
				owner, taken, oErr := client.relationNameOwnerInSchema(ctx, schema, step.BuiltIn.Name)
				if oErr != nil {
					return &NontransactionalPartialError{Applied: i, Total: len(jf.Steps), Err: fmt.Errorf("index name collision check failed: %w", oErr)}
				}
				if taken {
					return &JournaledIdentityError{StepIndex: step.Index, FirstLine: step.FirstLine,
						Detail: fmt.Sprintf("the name %q is already taken in schema %q (%s) — CREATE INDEX would collide (42P07), and the runner never drops another relation's object: drop or rename the existing object by hand, or give this step's index a free name",
							step.BuiltIn.Name, schema, owner)}
				}
			}
		}

		var rows int64
		var err2 error
		if step.Concurrent() {
			rows, err2 = s.execConcurrent(ctx, client, step, step.Statement, emit)
		} else {
			rows, err2 = s.execTransactionalStep(ctx, step)
		}
		if err2 != nil {
			return &NontransactionalPartialError{Applied: i, Total: len(jf.Steps), Err: err2}
		}
		emit(JournaledRunEvent{StepIndex: step.Index, Action: "run", Rows: rows})

		// Post-verification: the effect must now be provable.
		postState, postDetail, postErr := client.EvaluateJournaledStep(ctx, step)
		switch {
		case postErr != nil:
			return &JournaledVerifyError{StepIndex: step.Index, FirstLine: step.FirstLine, Verify: step.VerifyLabel(), Err: postErr}
		case postState == JournalStateSatisfied:
			continue
		default:
			return &JournaledVerifyError{StepIndex: step.Index, FirstLine: step.FirstLine, Verify: step.VerifyLabel(), Expect: "satisfied", Got: postState + " " + postDetail}
		}
	}
	return s.RecordAppliedVersion(ctx, mf)
}

func (s *JournaledStep) VerifyLabel() string {
	if s.HasBuiltIn {
		return "built-in postcondition " + s.BuiltIn.Kind
	}
	return s.Annotation.Verify
}

// qualifiedIndexName renders a schema-qualified, quoted index reference for
// the debris drop.
func qualifiedIndexName(schema, name string) string {
	if schema != "" {
		return quoteIdentPart(schema) + "." + quoteIdentPart(name)
	}
	return quoteIdentPart(name)
}

// execConcurrent runs one statement that cannot run in a transaction
// (concurrent index operations) on the pinned session with the step's
// timeout knobs applied at session level, resetting them afterwards. The
// knobs live on the runner's own pinned connection only — migration SQL
// never carries session-level SET.
func (s *MigrationSession) execConcurrent(ctx context.Context, client *Client, step *JournaledStep, sql string, emit func(JournaledRunEvent)) (int64, error) {
	if err := s.setSessionTimeouts(ctx, step.EffectiveLockTimeout(), step.EffectiveStatementTimeout()); err != nil {
		return 0, err
	}
	defer func() {
		resetCtx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 5*time.Second)
		defer cancel()
		_, _ = s.conn.Exec(resetCtx, "RESET lock_timeout")
		_, _ = s.conn.Exec(resetCtx, "RESET statement_timeout")
	}()

	// Best-effort progress observation from a second connection; the step
	// itself blocks the pinned session.
	var watchTable string
	if step.HasBuiltIn {
		watchTable = step.BuiltIn.Table
		if step.BuiltIn.Schema != "" {
			watchTable = step.BuiltIn.Schema + "." + step.BuiltIn.Table
		}
	}
	if client != nil {
		wctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			client.WatchCreateIndexProgress(wctx, watchTable, func(p IndexBuildProgress) {
				emit(JournaledRunEvent{StepIndex: step.Index, Action: "progress",
					Detail: fmt.Sprintf("%s: %s (%d/%d blocks)", p.Table, p.Phase, p.BlocksDone, p.BlocksTotal)})
			})
		}()
		defer func() {
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
			}
		}()
	}

	tag, err := s.conn.Exec(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("execute %q: %w", firstSQLLine(sql), err)
	}
	return tag.RowsAffected(), nil
}

// execTransactionalStep runs one step inside its own transaction: the
// timeout knobs through SET LOCAL (the allowlisted mechanism, runner-issued
// from the step's declared knobs), the step's attached SET LOCAL statements,
// then the statement itself.
func (s *MigrationSession) execTransactionalStep(ctx context.Context, step *JournaledStep) (int64, error) {
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin step tx: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+step.EffectiveLockTimeout()+"'"); err != nil {
		return 0, fmt.Errorf("set step lock_timeout: %w", err)
	}
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '"+step.EffectiveStatementTimeout()+"'"); err != nil {
		return 0, fmt.Errorf("set step statement_timeout: %w", err)
	}
	for _, cfg := range step.ConfigSQL {
		if _, err := tx.Exec(ctx, cfg); err != nil {
			return 0, fmt.Errorf("step SET LOCAL %q: %w", firstSQLLine(cfg), err)
		}
	}
	tag, err := tx.Exec(ctx, step.Statement)
	if err != nil {
		return 0, fmt.Errorf("execute %q: %w", step.FirstLine, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit step: %w", err)
	}
	return tag.RowsAffected(), nil
}

// setSessionTimeouts applies the concurrent-step knobs on the pinned
// session. Values are canonical re-renders of validated durations — never
// raw migration text.
func (s *MigrationSession) setSessionTimeouts(ctx context.Context, lockTimeout, statementTimeout string) error {
	if _, err := s.conn.Exec(ctx, "SET lock_timeout = '"+lockTimeout+"'"); err != nil {
		return fmt.Errorf("set lock_timeout: %w", err)
	}
	if _, err := s.conn.Exec(ctx, "SET statement_timeout = '"+statementTimeout+"'"); err != nil {
		return fmt.Errorf("set statement_timeout: %w", err)
	}
	return nil
}
