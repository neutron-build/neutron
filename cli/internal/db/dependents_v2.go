package db

// Catalog dependents of columns and views (Q12 review-1 F3/F4).
//
// PostgreSQL refuses to change the type of, or drop, a column that another
// object depends on: a view or materialized view (through its rewrite
// rule), a function whose body is parsed at creation (BEGIN ATOMIC), a
// policy, a trigger with a column list, or a rule. It also refuses a type
// change while a column of another relation stores the table's row type.
// The planner owns none of these unless they are declared views, so a live
// plan asks the catalog which of them exist and refuses instead of writing
// a plan that fails at apply.

import (
	"context"
	"fmt"
)

// V2Dependent is one catalog object that depends on a column, a view or a
// table's row type.
type V2Dependent struct {
	Column   string     // the column depended on ("" for a view or row type)
	Kind     string     // view | materialized view | rule | function | policy | trigger | row-type column
	Identity V2Identity // the view or materialized view, for kinds view and materialized view
	Display  string     // how messages name it
}

// V2DependencyInspector is implemented by live normalizers: planning then
// reads dependencies from the catalog. Offline planning has none.
type V2DependencyInspector interface {
	// ColumnDependents returns the objects that depend on the named live
	// columns of a table, and, with rowType, the columns of other
	// relations that store the table's row type.
	ColumnDependents(ctx context.Context, table V2Identity, columns []string, rowType bool) ([]V2Dependent, error)
	// RelationDependents returns the objects that depend on a view.
	RelationDependents(ctx context.Context, view V2Identity) ([]V2Dependent, error)
}

// dependentsSQL lists the dependents of relation $1 (a regclass text): of
// the columns named in $2 when $3 is false, of any column or the whole
// relation otherwise. Internal triggers (foreign keys) are left out; a view's
// own rewrite rule depends on it internally ('i') and is not listed.
const dependentsSQL = `
WITH rel AS (SELECT to_regclass($1) AS oid),
deps AS (
	SELECT a.attname::text AS col, d.classid, d.objid
	FROM pg_depend d
	JOIN rel ON d.refclassid = 'pg_class'::regclass AND d.refobjid = rel.oid
	LEFT JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = d.refobjsubid AND d.refobjsubid > 0
	WHERE d.deptype = 'n' AND ($3 OR a.attname = ANY($2::text[]))
)
SELECT DISTINCT deps.col, x.kind, x.nsp, x.name, x.display
FROM deps
CROSS JOIN LATERAL (
	SELECT CASE c.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' ELSE 'rule' END AS kind,
		n.nspname::text AS nsp, c.relname::text AS name,
		CASE WHEN c.relkind IN ('v', 'm') THEN format('%s %I.%I', CASE c.relkind WHEN 'v' THEN 'view' ELSE 'materialized view' END, n.nspname, c.relname)
			ELSE format('rule %I on %I.%I', r.rulename, n.nspname, c.relname) END AS display
	FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE deps.classid = 'pg_rewrite'::regclass AND r.oid = deps.objid
	UNION ALL
	SELECT 'function', '', '', format('function %s', deps.objid::regprocedure)
	WHERE deps.classid = 'pg_proc'::regclass
	UNION ALL
	SELECT 'policy', '', '', format('policy %I on %s', p.polname, p.polrelid::regclass)
	FROM pg_policy p WHERE deps.classid = 'pg_policy'::regclass AND p.oid = deps.objid
	UNION ALL
	SELECT 'trigger', '', '', format('trigger %I on %s', t.tgname, t.tgrelid::regclass)
	FROM pg_trigger t WHERE deps.classid = 'pg_trigger'::regclass AND t.oid = deps.objid AND NOT t.tgisinternal
) x
ORDER BY 5, 1`

// rowTypeSQL lists the columns of other relations that store the row type
// of relation $1, directly or as an array.
const rowTypeSQL = `
SELECT format('column %s.%I', a.attrelid::regclass, a.attname)
FROM pg_class t
JOIN pg_type rt ON rt.oid = t.reltype
JOIN pg_attribute a ON a.atttypid IN (rt.oid, rt.typarray) AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_class c ON c.oid = a.attrelid AND c.relkind IN ('r', 'p', 'm', 'f')
WHERE t.oid = to_regclass($1)
ORDER BY 1`

func (n *TwinNormalizer) dependents(ctx context.Context, rel V2Identity, columns []string, all bool) ([]V2Dependent, error) {
	if columns == nil {
		columns = []string{}
	}
	rows, err := n.conn.Query(ctx, dependentsSQL, qualifiedNameSQL(rel), columns, all)
	if err != nil {
		return nil, fmt.Errorf("read the dependents of %s: %w", rel, err)
	}
	defer rows.Close()
	var out []V2Dependent
	for rows.Next() {
		var col *string
		var d V2Dependent
		var nsp, name string
		if err := rows.Scan(&col, &d.Kind, &nsp, &name, &d.Display); err != nil {
			return nil, fmt.Errorf("read the dependents of %s: %w", rel, err)
		}
		if col != nil {
			d.Column = *col
		}
		if d.Kind == "view" || d.Kind == "materialized view" {
			d.Identity = V2Identity{Schema: nsp, Name: name}
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ColumnDependents implements V2DependencyInspector.
func (n *TwinNormalizer) ColumnDependents(ctx context.Context, table V2Identity, columns []string, rowType bool) ([]V2Dependent, error) {
	out, err := n.dependents(ctx, table, columns, false)
	if err != nil || !rowType {
		return out, err
	}
	rows, err := n.conn.Query(ctx, rowTypeSQL, qualifiedNameSQL(table))
	if err != nil {
		return nil, fmt.Errorf("read the row-type users of %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var display string
		if err := rows.Scan(&display); err != nil {
			return nil, err
		}
		out = append(out, V2Dependent{Kind: "row-type column", Display: display + " (it stores the row type of " + table.String() + ")"})
	}
	return out, rows.Err()
}

// RelationDependents implements V2DependencyInspector.
func (n *TwinNormalizer) RelationDependents(ctx context.Context, view V2Identity) ([]V2Dependent, error) {
	return n.dependents(ctx, view, nil, true)
}
