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
//
// A table with planned column renames is compared against its live
// expressions as they will read after the rename: RenameTable twins the
// live table, lets PostgreSQL rename the twin's columns, and deparses.

import (
	"context"
	"fmt"
	"sort"
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
	// RenameTable returns a live table with its expression fields (the
	// same fields NormalizeTable replaces) as the catalog deparses them
	// after renaming its columns (renames: live name -> new name). Index
	// key and INCLUDE columns come back renamed as well, as the index
	// reads after the rename; column names and the other structural
	// fields are untouched.
	RenameTable(ctx context.Context, table V2Table, renames map[string]string) (V2Table, error)
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

// exec runs one twin statement over the extended protocol, which refuses
// more than one command: twin DDL embeds the document's expression text,
// and planning must not run anything a crafted expression appends.
func (n *TwinNormalizer) exec(ctx context.Context, sql string) error {
	return n.conn.Conn().PgConn().ExecParams(ctx, sql, nil, nil, nil, nil).Read().Err
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
// compared as raw text. Comparisons of the Failed elements stay flagged
// unverified; the table's other elements are catalog-normalized.
type PartialNormalizationError struct {
	Table  V2Table
	Failed []string // element keys (v2DefaultElement and friends)
	Err    error
}

// Element keys name one expression element of a table, in
// PartialNormalizationError.Failed and in the diff's per-element
// verification.
func v2DefaultElement(column string) string   { return "column " + column + " default" }
func v2GeneratedElement(column string) string { return "column " + column + " generation expression" }
func v2CheckElement(constraint string) string { return "check " + constraint }
func v2IndexElement(index string) string      { return "index " + index }

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

// RenameTable materializes the live table as a twin under its live column
// names, renames the twin's columns and reads the expressions back:
// PostgreSQL rewrites generation expressions, checks and index definitions
// on RENAME COLUMN itself, so the twin holds exactly what the live catalog
// will hold after the planned rename. No expression text is rewritten here.
func (n *TwinNormalizer) RenameTable(ctx context.Context, table V2Table, renames map[string]string) (V2Table, error) {
	if n.conn == nil {
		return table, fmt.Errorf("normalizer closed")
	}
	return n.twin(ctx, table, renames)
}

// normalizeTableElements is the fallback after the whole twin failed. The
// twin carries only the column types that can exist yet: a column typed by
// something this plan creates first (a new enum, say) is left out, found by
// creating each column alone. The elements that do not name a left-out
// column are then normalized together on one twin; only when that fails,
// or for an element that names one, is an element normalized on its own
// twin. An element that references a left-out column fails there and
// keeps its original text. When every element normalized, the table is
// fully normalized: the left-out columns carry no expression the diff
// compares against the catalog.
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
	var left map[string]bool
	if _, err := n.normalizeTableTwin(ctx, bare); err != nil {
		bare.Columns = nil
		left = map[string]bool{}
		for _, c := range typesOnly {
			one := bare
			one.Columns = []V2Column{c}
			if _, err := n.normalizeTableTwin(ctx, one); err == nil {
				bare.Columns = append(bare.Columns, c)
			} else {
				left[strings.ToLower(c.Name)] = true
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
	done := map[string]bool{}
	if len(left) > 0 {
		done = n.normalizeTogether(ctx, table, bare, bareIdx, left, &out)
	}
	var failed []string
	for i, c := range table.Columns {
		j, ok := bareIdx[c.Name]
		if c.Default != nil && (c.Default.Kind == "literal" || c.Default.Kind == "expression") && !done[v2DefaultElement(c.Name)] {
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
				failed = append(failed, v2DefaultElement(c.Name))
			}
		}
		if c.Generated != nil && !done[v2GeneratedElement(c.Name)] {
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
				failed = append(failed, v2GeneratedElement(c.Name))
			}
		}
	}
	for i, con := range table.Constraints {
		if con.Type != "check" || con.Expression == nil || done[v2CheckElement(con.Name)] {
			continue
		}
		one := bare
		one.Constraints = []V2Constraint{con}
		if got, err := n.normalizeTableTwin(ctx, one); err == nil {
			out.Constraints[i].Expression = got.Constraints[0].Expression
		} else {
			failed = append(failed, v2CheckElement(con.Name))
		}
	}
	for i, idx := range table.Indexes {
		if done[v2IndexElement(idx.Identity.Name)] {
			continue
		}
		one := bare
		one.Indexes = []V2Index{idx}
		if got, err := n.normalizeTableTwin(ctx, one); err == nil {
			out.Indexes[i].Key = got.Indexes[0].Key
			out.Indexes[i].Where = got.Indexes[0].Where
		} else {
			failed = append(failed, v2IndexElement(idx.Identity.Name))
		}
	}
	if len(failed) == 0 {
		return out, nil
	}
	return out, &PartialNormalizationError{Table: out, Failed: failed, Err: cause}
}

// normalizeTogether normalizes, on one twin of the reduced columns, every
// element that does not name a left-out column, writes their deparsed
// spelling into out and returns their element keys. When that twin fails
// (an element fails for another reason, such as an enum value the plan
// adds) it returns none and the caller goes element by element. The name
// scan only picks the candidates: an element that references a left-out
// column cannot be created on the reduced twin, so a missed reference
// fails the twin instead of passing as normalized.
func (n *TwinNormalizer) normalizeTogether(ctx context.Context, table V2Table, bare V2Table, bareIdx map[string]int, left map[string]bool, out *V2Table) map[string]bool {
	all := bare
	all.Columns = append([]V2Column(nil), bare.Columns...)
	var keys []string
	var defaults, generated, checks, indexes []int
	for i, c := range table.Columns {
		j, ok := bareIdx[c.Name]
		if !ok {
			continue
		}
		if c.Default != nil && (c.Default.Kind == "literal" || c.Default.Kind == "expression") && !namesColumn(c.Default.SQL, left) {
			all.Columns[j].Default = c.Default
			defaults = append(defaults, i)
			keys = append(keys, v2DefaultElement(c.Name))
		}
		if c.Generated != nil && !namesColumn(&c.Generated.Expression, left) {
			all.Columns[j].Generated = c.Generated
			generated = append(generated, i)
			keys = append(keys, v2GeneratedElement(c.Name))
		}
	}
	for i, con := range table.Constraints {
		if con.Type == "check" && con.Expression != nil && !namesColumn(con.Expression, left) {
			all.Constraints = append(all.Constraints, con)
			checks = append(checks, i)
			keys = append(keys, v2CheckElement(con.Name))
		}
	}
	for i, idx := range table.Indexes {
		if !indexNamesColumn(idx, left) {
			all.Indexes = append(all.Indexes, idx)
			indexes = append(indexes, i)
			keys = append(keys, v2IndexElement(idx.Identity.Name))
		}
	}
	if len(keys) == 0 {
		return nil
	}
	got, err := n.normalizeTableTwin(ctx, all)
	if err != nil {
		return nil
	}
	for _, i := range defaults {
		out.Columns[i].Default = got.Columns[bareIdx[table.Columns[i].Name]].Default
	}
	for _, i := range generated {
		out.Columns[i].Generated = got.Columns[bareIdx[table.Columns[i].Name]].Generated
	}
	for k, i := range checks {
		out.Constraints[i].Expression = got.Constraints[k].Expression
	}
	for k, i := range indexes {
		out.Indexes[i].Key = got.Indexes[k].Key
		out.Indexes[i].Where = got.Indexes[k].Where
	}
	done := make(map[string]bool, len(keys))
	for _, k := range keys {
		done[k] = true
	}
	return done
}

// namesColumn reports whether SQL text may name one of the columns (keys
// lowercased): a bare word or a quoted identifier equal to a name without
// regard to case. It over-matches rather than under-matches (a mention
// inside a string literal is not a reference); see normalizeTogether.
func namesColumn(sql *string, columns map[string]bool) bool {
	if sql == nil {
		return false
	}
	for _, t := range significantTokens(*sql) {
		if (t.kind == 'w' || t.kind == 'q') && columns[strings.ToLower(t.text)] {
			return true
		}
	}
	return false
}

func indexNamesColumn(idx V2Index, columns map[string]bool) bool {
	for _, k := range idx.Key {
		if k.Column != nil && columns[strings.ToLower(*k.Column)] {
			return true
		}
		if namesColumn(k.Expression, columns) {
			return true
		}
	}
	for _, c := range idx.Include {
		if columns[strings.ToLower(c)] {
			return true
		}
	}
	return namesColumn(idx.Where, columns)
}

func (n *TwinNormalizer) normalizeTableTwin(ctx context.Context, table V2Table) (V2Table, error) {
	return n.twin(ctx, table, nil)
}

// twin creates the table's twin, renames its columns (live name -> new
// name) and returns the table with the twin's deparsed expressions.
func (n *TwinNormalizer) twin(ctx context.Context, table V2Table, renames map[string]string) (V2Table, error) {
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
	if err := n.exec(ctx, create); err != nil {
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
		if err := n.exec(ctx, stmt); err != nil {
			return table, fmt.Errorf("twin index for %s.%s: %w", table.Identity, idx.Identity.Name, err)
		}
	}
	olds := make([]string, 0, len(renames))
	for old := range renames {
		olds = append(olds, old)
	}
	sort.Strings(olds)
	for _, old := range olds {
		stmt := fmt.Sprintf("alter table %s.%s rename column %s to %s", quoteIdent("pg_temp"), quoteIdent(tmp), quoteIdent(old), quoteIdent(renames[old]))
		if err := n.exec(ctx, stmt); err != nil {
			return table, fmt.Errorf("twin rename for %s.%s: %w", table.Identity, old, err)
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

	// Expression text can declare columns of its own (a check that closes
	// its parenthesis early, or a comment that swallows a declaration); a
	// twin whose columns are not exactly the document's would resolve
	// references against them.
	if len(twin.Columns) != len(table.Columns) {
		return table, fmt.Errorf("twin table %s has %d columns, the document declares %d", table.Identity, len(twin.Columns), len(table.Columns))
	}
	out := table
	out.Columns = append([]V2Column(nil), table.Columns...)
	out.Constraints = append([]V2Constraint(nil), table.Constraints...)
	out.Indexes = append([]V2Index(nil), table.Indexes...)
	byName := map[string]V2Column{}
	for _, tc := range twin.Columns {
		byName[tc.Name] = tc
	}
	for i := range out.Columns {
		name := out.Columns[i].Name
		if to, ok := renames[name]; ok {
			name = to
		}
		tc, ok := byName[name]
		if !ok {
			return table, fmt.Errorf("twin table %s lacks column %q", table.Identity, out.Columns[i].Name)
		}
		if !tc.Type.SameAs(out.Columns[i].Type) {
			return table, fmt.Errorf("twin table %s declares column %q with another type", table.Identity, out.Columns[i].Name)
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
		if len(renames) > 0 {
			out.Indexes[i].Include = ti.Include
		}
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
	if err := n.exec(ctx, create); err != nil {
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
