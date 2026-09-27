package db

// Catalog-aware expression equivalence for the v2 diff (M02).
//
// PostgreSQL stores expressions deparsed (pg_get_expr/pg_get_viewdef), so a
// desired document's hand-written spelling ("pos > 0") can textually differ
// from the live catalog's ("(pos > 0)") while being the same expression.
// String-trimming guesses are forbidden (README 4.2); instead the twin
// normalizer asks the catalog itself: it materializes the DESIRED table (or
// view) as a session-local TEMPORARY twin on one pinned connection,
// introspects the twin with the same introspection code used for live
// tables, and returns the catalog-deparsed spelling of the desired
// expressions. The diff then compares deparse-to-deparse — equal iff
// PostgreSQL considers them equal.
//
// Temporary objects live in pg_temp and vanish with the session; no user
// object is read-modified-written. Normalization is best-effort: when the
// twin cannot be created (e.g. an element uses an enum value this plan
// adds, or a column is typed by an enum this plan creates first), the
// table is normalized element by element on twins without the columns
// whose type does not exist yet. An element that still cannot be
// normalized (a view whose base table does not exist yet, an element of
// such a column) keeps the desired document text, and the diff marks any
// resulting textual difference as unverified instead of silently guessing
// equivalence.

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// V2Normalizer resolves expression equivalence for the v2 diff. Both
// methods return the catalog-canonical form of the desired object's
// expression fields, or an error when the catalog could not be consulted.
type V2Normalizer interface {
	// NormalizeTable returns the desired table with literal/expression
	// defaults, check expressions, index key expressions and predicates
	// replaced by their catalog-deparsed spelling. Structural fields are
	// untouched.
	NormalizeTable(ctx context.Context, table V2Table) (V2Table, error)
	// NormalizeView returns the desired view with its definition replaced
	// by the catalog's deparse of that definition.
	NormalizeView(ctx context.Context, view V2View) (V2View, error)
	Close()
}

// TwinNormalizer is the live-Postgres V2Normalizer.
type TwinNormalizer struct {
	conn *pgxpool.Conn
	seq  int
}

// NewTwinNormalizer acquires one pinned connection from the pool: temporary
// objects are session-local, so every twin statement must run on the same
// connection. Close releases it.
func (c *Client) NewTwinNormalizer(ctx context.Context) (*TwinNormalizer, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire normalizer connection: %w", err)
	}
	return &TwinNormalizer{conn: conn}, nil
}

func (n *TwinNormalizer) Close() {
	if n.conn != nil {
		n.conn.Release()
		n.conn = nil
	}
}

func (n *TwinNormalizer) nextName() string {
	n.seq++
	return fmt.Sprintf("neutron_norm_%d", n.seq)
}

// PartialNormalizationError reports a table whose whole twin could not be
// created, and some of whose expression elements could not be normalized
// one at a time either: Table carries every element the catalog could
// deparse, and the original text for the rest (Failed). The typical cause
// is an element that uses an enum value this plan adds (the live type does
// not have it yet), which must not leave the table's unrelated expressions
// compared as raw text. Comparisons on such a table stay flagged
// unverified.
type PartialNormalizationError struct {
	Table  V2Table
	Failed []string
	Err    error
}

func (e *PartialNormalizationError) Error() string {
	return fmt.Sprintf("%v (normalized element by element; not normalized: %s)", e.Err, strings.Join(e.Failed, ", "))
}

func (e *PartialNormalizationError) Unwrap() error { return e.Err }

func (n *TwinNormalizer) NormalizeTable(ctx context.Context, table V2Table) (V2Table, error) {
	if n.conn == nil {
		return table, fmt.Errorf("normalizer closed")
	}
	out, err := n.normalizeTableTwin(ctx, table)
	if err == nil {
		return out, nil
	}
	return n.normalizeTableElements(ctx, table, err)
}

// normalizeTableElements is the fallback after the whole twin failed. The
// twin carries only the column types that can exist yet: a column typed by
// something this plan creates first (a new enum, say) is left out, found by
// creating each column alone. Every expression element is then normalized
// on its own twin; an element that references a left-out column fails
// there and keeps its original text. When every element normalized, the
// table is fully normalized: the left-out columns carry no expression the
// diff compares against the catalog.
func (n *TwinNormalizer) normalizeTableElements(ctx context.Context, table V2Table, cause error) (V2Table, error) {
	typesOnly := make([]V2Column, len(table.Columns))
	for i, c := range table.Columns {
		if d := c.Default; d != nil && d.Kind != "identity" {
			c.Default = nil
		}
		c.Generated = nil
		typesOnly[i] = c
	}
	bare := table
	bare.Columns = typesOnly
	bare.Constraints = nil
	bare.Indexes = nil
	if _, err := n.normalizeTableTwin(ctx, bare); err != nil {
		bare.Columns = nil
		for _, c := range typesOnly {
			one := bare
			one.Columns = []V2Column{c}
			if _, err := n.normalizeTableTwin(ctx, one); err == nil {
				bare.Columns = append(bare.Columns, c)
			}
		}
		if _, err := n.normalizeTableTwin(ctx, bare); err != nil {
			return table, cause
		}
	}
	bareIdx := map[string]int{}
	for j, c := range bare.Columns {
		bareIdx[c.Name] = j
	}

	out := table
	out.Columns = append([]V2Column(nil), table.Columns...)
	out.Constraints = append([]V2Constraint(nil), table.Constraints...)
	out.Indexes = append([]V2Index(nil), table.Indexes...)
	var failed []string
	for i, c := range table.Columns {
		j, ok := bareIdx[c.Name]
		if c.Default != nil && (c.Default.Kind == "literal" || c.Default.Kind == "expression") {
			normalized := false
			if ok {
				one := bare
				one.Columns = append([]V2Column(nil), bare.Columns...)
				one.Columns[j].Default = c.Default
				if got, err := n.normalizeTableTwin(ctx, one); err == nil {
					out.Columns[i].Default = got.Columns[j].Default
					normalized = true
				}
			}
			if !normalized {
				failed = append(failed, "column "+c.Name+" default")
			}
		}
		if c.Generated != nil {
			normalized := false
			if ok {
				one := bare
				one.Columns = append([]V2Column(nil), bare.Columns...)
				one.Columns[j].Generated = c.Generated
				if got, err := n.normalizeTableTwin(ctx, one); err == nil {
					out.Columns[i].Generated = got.Columns[j].Generated
					normalized = true
				}
			}
			if !normalized {
				failed = append(failed, "column "+c.Name+" generation expression")
			}
		}
	}
	for i, con := range table.Constraints {
		if con.Type != "check" || con.Expression == nil {
			continue
		}
		one := bare
		one.Constraints = []V2Constraint{con}
		if got, err := n.normalizeTableTwin(ctx, one); err == nil {
			out.Constraints[i].Expression = got.Constraints[0].Expression
		} else {
			failed = append(failed, "check "+con.Name)
		}
	}
	for i, idx := range table.Indexes {
		one := bare
		one.Indexes = []V2Index{idx}
		if got, err := n.normalizeTableTwin(ctx, one); err == nil {
			out.Indexes[i].Key = got.Indexes[0].Key
			out.Indexes[i].Where = got.Indexes[0].Where
		} else {
			failed = append(failed, "index "+idx.Identity.Name)
		}
	}
	if len(failed) == 0 {
		return out, nil
	}
	return out, &PartialNormalizationError{Table: out, Failed: failed, Err: cause}
}

func (n *TwinNormalizer) normalizeTableTwin(ctx context.Context, table V2Table) (V2Table, error) {
	tmp := n.nextName()

	// Columns with defaults and check constraints; foreign keys and PK/
	// unique constraints are compared structurally and add nothing here.
	lines := make([]string, 0, len(table.Columns))
	for _, c := range table.Columns {
		ddl, err := v2ColumnDDL(c)
		if err != nil {
			return table, err
		}
		lines = append(lines, ddl)
	}
	for _, con := range table.Constraints {
		if con.Type != "check" || con.Expression == nil {
			continue
		}
		lines = append(lines, fmt.Sprintf("constraint %s check (%s)", quoteIdent(con.Name), *con.Expression))
	}
	create := fmt.Sprintf("create temporary table %s (%s)", quoteIdent(tmp), strings.Join(lines, ", "))
	if _, err := n.conn.Exec(ctx, create); err != nil {
		return table, fmt.Errorf("twin table for %s: %w", table.Identity, err)
	}
	defer n.conn.Exec(context.WithoutCancel(ctx), fmt.Sprintf("drop table if exists %s", quoteIdent(tmp)))

	for _, idx := range table.Indexes {
		// Twin indexes carry the desired index name (unqualified: they are
		// created in pg_temp with the temp table) so introspection maps
		// them back by name instead of relying on catalog name generation.
		ddl, err := indexBodySQL(idx)
		if err != nil {
			return table, err
		}
		stmt := "create "
		if idx.Unique {
			stmt += "unique "
		}
		stmt += fmt.Sprintf("index %s on %s.%s %s", quoteIdent(idx.Identity.Name), quoteIdent("pg_temp"), quoteIdent(tmp), ddl)
		if _, err := n.conn.Exec(ctx, stmt); err != nil {
			return table, fmt.Errorf("twin index for %s.%s: %w", table.Identity, idx.Identity.Name, err)
		}
	}

	var oid uint32
	if err := n.conn.QueryRow(ctx, `SELECT $1::regclass::oid`, "pg_temp."+tmp).Scan(&oid); err != nil {
		return table, err
	}
	rel := v2RelationInfo{oid: oid, schema: "pg_temp", name: tmp, kind: "r"}
	twin, reasons, _, err := introspectV2TableOn(ctx, n.conn, rel, new(bool))
	if err != nil {
		return table, err
	}
	if len(reasons) > 0 {
		return table, fmt.Errorf("twin table %s could not be introspected: %s", table.Identity, strings.Join(reasons, "; "))
	}

	out := table
	byName := map[string]V2Column{}
	for _, tc := range twin.Columns {
		byName[tc.Name] = tc
	}
	for i := range out.Columns {
		tc, ok := byName[out.Columns[i].Name]
		if !ok {
			return table, fmt.Errorf("twin table %s lacks column %q", table.Identity, out.Columns[i].Name)
		}
		if out.Columns[i].Default != nil && (out.Columns[i].Default.Kind == "literal" || out.Columns[i].Default.Kind == "expression") {
			if tc.Default != nil && tc.Default.Kind == out.Columns[i].Default.Kind {
				out.Columns[i].Default = tc.Default
			}
		}
		if out.Columns[i].Generated != nil && tc.Generated != nil {
			out.Columns[i].Generated = tc.Generated
		}
	}
	twinChecks := map[string]string{}
	for _, con := range twin.Constraints {
		if con.Type == "check" && con.Expression != nil {
			twinChecks[con.Name] = *con.Expression
		}
	}
	for i := range out.Constraints {
		if out.Constraints[i].Type != "check" || out.Constraints[i].Expression == nil {
			continue
		}
		if expr, ok := twinChecks[out.Constraints[i].Name]; ok {
			out.Constraints[i].Expression = strPtrV2(expr)
		}
	}
	twinIdx := map[string]V2Index{}
	for _, idx := range twin.Indexes {
		twinIdx[idx.Identity.Name] = idx
	}
	for i := range out.Indexes {
		ti, ok := twinIdx[out.Indexes[i].Identity.Name]
		if !ok {
			return table, fmt.Errorf("twin table %s lacks index %q", table.Identity, out.Indexes[i].Identity.Name)
		}
		out.Indexes[i].Key = ti.Key
		out.Indexes[i].Where = ti.Where
	}
	return out, nil
}

func (n *TwinNormalizer) NormalizeView(ctx context.Context, view V2View) (V2View, error) {
	if n.conn == nil {
		return view, fmt.Errorf("normalizer closed")
	}
	tmp := n.nextName()
	create := createV2ViewSQL(V2View{
		Identity:        V2Identity{Schema: "pg_temp", Name: tmp},
		Definition:      view.Definition,
		CheckOption:     view.CheckOption,
		SecurityInvoker: view.SecurityInvoker,
	})
	if _, err := n.conn.Exec(ctx, create); err != nil {
		return view, fmt.Errorf("twin view for %s: %w", view.Identity, err)
	}
	defer n.conn.Exec(context.WithoutCancel(ctx), fmt.Sprintf("drop view if exists %s", quoteIdent(tmp)))

	var def string
	// pg_temp resolves the session's real temp schema (pg_temp_N); the
	// catalog namespace name itself is not literally "pg_temp".
	if err := n.conn.QueryRow(ctx,
		`SELECT pg_get_viewdef($1::regclass)`, "pg_temp."+tmp,
	).Scan(&def); err != nil {
		return view, err
	}
	out := view
	out.Definition = def
	return out, nil
}
