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
// twin cannot be created (e.g. the desired table uses an enum this plan
// would create first, or a base table does not exist yet), the desired
// document text is used unchanged and the diff marks any resulting textual
// difference as unverified instead of silently guessing equivalence.

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

func (n *TwinNormalizer) NormalizeTable(ctx context.Context, table V2Table) (V2Table, error) {
	if n.conn == nil {
		return table, fmt.Errorf("normalizer closed")
	}
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
