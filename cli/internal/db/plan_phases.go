package db

// Transaction boundaries and server-version floors of a planned change (Q09).
//
// Enum value additions: PostgreSQL cannot use an enum value inside the
// transaction that adds it (SQLSTATE 55P04: a view, check, default or cast
// naming the value fails), and every product apply path runs a plan unit
// in one transaction. Whether a later statement "uses" a value is not
// decidable from SQL text without type resolution (a literal coerced by a
// comparison, a USING cast over existing data, an enum function evaluated
// while a check is validated), so the rule is structural: when a plan adds
// enum values AND changes anything else, the additions form their own
// earlier unit. `migrate generate` writes that unit as its own migration;
// `db push` applies it as its own, reported transaction. Nothing is split
// silently: the boundary is in the artifacts or in the push output.
//
// Server-version floors: the planner emits one statement form outside the
// supported matrix's common floor (PostgreSQL 16): ALTER COLUMN ... SET
// EXPRESSION, PostgreSQL 17+. Live planning refuses it on older servers;
// snapshot plans record the floor and migrate checks it before any
// statement runs.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/pgconn"
)

// SetExpressionMinServerMajor is the first PostgreSQL major version with
// ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION.
const SetExpressionMinServerMajor = 17

// StatementMinServerMajor returns the lowest PostgreSQL major version that
// can run a statement form this package's planner emits, with the feature
// name, or (0, "") when the statement has no floor above the supported
// matrix. It matches bare keyword tokens of the single tokenizer, so
// comments cannot hide the form and string literals, dollar quotes and
// quoted identifiers spelling it cannot fake it; it is not a SQL parser.
func StatementMinServerMajor(sql string) (int, string) {
	toks := significantTokens(sql)
	word := func(i int, w string) bool { return toks[i].kind == 'w' && toks[i].text == w }
	if len(toks) < 2 || !word(0, "alter") || !word(1, "table") {
		return 0, ""
	}
	for i := 2; i+2 < len(toks); i++ {
		if word(i, "set") && word(i+1, "expression") && word(i+2, "as") {
			return SetExpressionMinServerMajor, "ALTER COLUMN ... SET EXPRESSION"
		}
	}
	return 0, ""
}

// ServerMajorVersion returns the connected server's PostgreSQL major
// version from the server_version parameter every PostgreSQL server reports
// at connection startup. 0 means the server reported nothing parseable.
func (c *Client) ServerMajorVersion(ctx context.Context) (int, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()
	return parseServerMajor(conn.Conn().PgConn().ParameterStatus("server_version")), nil
}

// parseServerMajor reads the leading integer of a server_version string
// ("17.11 (Homebrew)", "16.15 (Debian 16.15-1.pgdg120+1)", "18beta1").
func parseServerMajor(v string) int {
	end := 0
	for end < len(v) && v[end] >= '0' && v[end] <= '9' {
		end++
	}
	n, err := strconv.Atoi(v[:end])
	if err != nil {
		return 0
	}
	return n
}

// PlanPhase is one transaction of a planned change.
type PlanPhase struct {
	// EnumAdditions marks the phase that holds only enum value additions.
	EnumAdditions bool
	Up            []string
	Down          []string // index-paired with Up
	Warnings      []string
}

// PlanPhases partitions a plan into its transactions: one, or two when the
// plan adds enum values and also changes anything else (enum additions
// first, then the rest in planned order). The enum phase gets the
// additions' own warnings; every other warning stays with the rest.
func PlanPhases(res DiffResult) ([]PlanPhase, error) {
	single := []PlanPhase{{Up: res.Up, Down: res.Down, Warnings: res.Warnings}}
	if len(res.EnumAdditions) == 0 {
		return single, nil
	}
	adds := make(map[string]bool, len(res.EnumAdditions))
	for _, a := range res.EnumAdditions {
		adds[a.Statement] = true
	}
	other := false
	for _, stmt := range res.Up {
		if !adds[stmt] && hasExecutableSQL(stmt) {
			other = true
			break
		}
	}
	if !other {
		return single, nil
	}
	if len(res.Up) != len(res.Down) {
		return nil, fmt.Errorf("diff produced %d up statements but %d down statements — refusing to split the enum additions from the rest of the plan; this is a planner defect", len(res.Up), len(res.Down))
	}
	enum := PlanPhase{EnumAdditions: true}
	rest := PlanPhase{}
	for i, stmt := range res.Up {
		if adds[stmt] {
			enum.Up = append(enum.Up, stmt)
			enum.Down = append(enum.Down, res.Down[i])
		} else {
			rest.Up = append(rest.Up, stmt)
			rest.Down = append(rest.Down, res.Down[i])
		}
	}
	pending := map[string]int{}
	for _, a := range res.EnumAdditions {
		enum.Warnings = append(enum.Warnings, a.Warning)
		pending[a.Warning]++
	}
	for _, w := range res.Warnings {
		if pending[w] > 0 {
			pending[w]--
			continue
		}
		rest.Warnings = append(rest.Warnings, w)
	}
	return []PlanPhase{enum, rest}, nil
}

// EnumPhaseReason is the user-facing reason for a separate enum phase.
const EnumPhaseReason = "PostgreSQL cannot use an enum value inside the transaction that adds it (SQLSTATE 55P04)"

// PhaseApplyError reports the failed phase of ApplyPhases. Phases before it
// are committed; the failed phase rolled back as a whole.
type PhaseApplyError struct {
	Phase int // zero-based index of the failed phase
	Total int
	Err   error
}

func (e *PhaseApplyError) Error() string {
	return fmt.Sprintf("phase %d of %d: %v", e.Phase+1, e.Total, e.Err)
}

func (e *PhaseApplyError) Unwrap() error { return e.Err }

// ApplyPhases runs each phase as its own transaction on the pinned session,
// in order, and stops at the first failure. It is the apply primitive of
// db push; nothing else decides where a plan's transactions begin and end.
func (s *MigrationSession) ApplyPhases(ctx context.Context, phases []PlanPhase, onApplied func(phase int, stmt string)) error {
	for i, ph := range phases {
		err := s.ApplyStatementsTx(ctx, ph.Up, func(stmt string) {
			if onApplied != nil {
				onApplied(i, stmt)
			}
		})
		if err != nil {
			return &PhaseApplyError{Phase: i, Total: len(phases), Err: err}
		}
	}
	return nil
}

// EnumAdditionsTarget returns the planning base with every managed enum's
// values replaced by the desired values: the state after an enum phase
// only. `migrate generate --mode snapshot` records it as the snapshot of
// the enum-additions migration, so the next migration plans from it. The
// base document is edited as its canonical JSON tree, so nothing else in
// it changes.
func EnumAdditionsTarget(base, desired *V2Document) (*V2Document, error) {
	dm, err := ModelFromRoot(desired.Root)
	if err != nil {
		return nil, err
	}
	want := map[V2Identity][]string{}
	for _, e := range dm.Enums {
		if e.Managed {
			want[e.Identity] = e.Values
		}
	}
	var root map[string]any
	if err := json.Unmarshal(base.Canonical, &root); err != nil {
		return nil, fmt.Errorf("re-read planning base: %w", err)
	}
	enums, _ := root["enums"].([]any)
	for _, raw := range enums {
		e, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		ident, _ := e["identity"].(map[string]any)
		schema, _ := ident["schema"].(string)
		name, _ := ident["name"].(string)
		values, ok := want[V2Identity{Schema: schema, Name: name}]
		if !ok {
			continue
		}
		list := make([]any, len(values))
		for i, v := range values {
			list[i] = v
		}
		e["values"] = list
	}
	out, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	return ParseV2Document(out)
}

// EnumValueUseError is a migration that uses an enum value it also adds:
// PostgreSQL rejected it (55P04) and the migration's transaction rolled
// back. The message names the fix.
type EnumValueUseError struct {
	Version string
	Err     error
}

func (e *EnumValueUseError) Error() string {
	return fmt.Sprintf(
		"execute migration %s: %v — this migration uses an enum value that it also adds: %s, and each migration runs in one transaction, so nothing from it was applied. Move its `alter type ... add value` statements into their own earlier migration file, or delete this unapplied migration and regenerate it with `neutron migrate generate`, which writes enum value additions as their own earlier migration",
		e.Version, e.Err, EnumPhaseReason)
}

func (e *EnumValueUseError) Unwrap() error { return e.Err }

// isUnsafeNewEnumValue reports PostgreSQL's "unsafe use of new value" error.
func isUnsafeNewEnumValue(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "55P04"
}
