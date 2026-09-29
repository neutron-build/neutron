package db

// Catalog dependents of columns and views (Q12 review-1 F3/F4, review-2).
//
// PostgreSQL refuses to change the type of, or drop, a column that another
// object depends on: a view or materialized view (through its rewrite
// rule), a function whose body is parsed at creation (BEGIN ATOMIC), a
// policy, a trigger with a column list, a rule, or a publication's row
// filter or column list. It refuses a type change while a column of a
// table, partitioned table or materialized view stores the table's row
// type, directly or through arrays, domains and composite types. And it
// refuses to drop a view that another view, a function (SETOF the view's
// row type) or a stored column of that row type depends on. The planner
// owns none of these unless they are declared views, so a live plan asks
// the catalog which of them exist and refuses instead of writing a plan
// that fails at apply.

import (
	"context"
	"fmt"
)

// V2Dependent is one catalog object that depends on a column, a view or a
// table's row type.
type V2Dependent struct {
	Column   string     // the column depended on ("" for a view or row type)
	Kind     string     // view | materialized view | rule | function | policy | trigger | publication | row-type column | row-type user
	Identity V2Identity // the view or materialized view, for kinds view and materialized view
	Display  string     // how messages name it
}

// V2DependencyInspector is implemented by live normalizers: planning then
// reads dependencies from the catalog. Offline planning has none.
type V2DependencyInspector interface {
	// ColumnDependents returns the objects that depend on the named live
	// columns of a table, and, with rowType, the columns that store the
	// table's row type.
	ColumnDependents(ctx context.Context, table V2Identity, columns []string, rowType bool) ([]V2Dependent, error)
	// ViewDependents returns, in one query, the objects that depend on
	// each of the views (through the view or its row type).
	ViewDependents(ctx context.Context, views []V2Identity) (map[V2Identity][]V2Dependent, error)
}

// classifySQL names a pg_depend row (deps.classid, deps.objid): the kinds
// the planner does not manage. Rows of other classes (constraints,
// indexes, defaults of the table itself) are handled by the plan.
const classifySQL = `
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
	UNION ALL
	SELECT 'publication', '', '', format('publication %I (its row filter or column list for %s)', pb.pubname, pr.prrelid::regclass)
	FROM pg_publication_rel pr JOIN pg_publication pb ON pb.oid = pr.prpubid
	WHERE deps.classid = 'pg_publication_rel'::regclass AND pr.oid = deps.objid`

// columnDependentsSQL lists the dependents of the columns $2 of relation $1.
const columnDependentsSQL = `
WITH rel AS (SELECT to_regclass($1) AS oid),
deps AS (
	SELECT a.attname::text AS col, d.classid, d.objid
	FROM pg_depend d
	JOIN rel ON d.refclassid = 'pg_class'::regclass AND d.refobjid = rel.oid
	JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = d.refobjsubid
	WHERE d.deptype = 'n' AND d.refobjsubid > 0 AND a.attname = ANY($2::text[])
)
SELECT DISTINCT deps.col, x.kind, x.nsp, x.name, x.display
FROM deps CROSS JOIN LATERAL (` + classifySQL + `) x
ORDER BY 5, 1`

// rowTypeSQL lists the columns of tables, partitioned tables and
// materialized views that store the row type of relation $1, directly or
// through arrays, domains and composite types (PostgreSQL's
// find_composite_type_dependencies; foreign tables are not checked).
const rowTypeSQL = `
WITH RECURSIVE types(oid, via) AS (
	SELECT c.reltype, ''::text FROM pg_class c WHERE c.oid = to_regclass($1)
	UNION
	SELECT t.oid, CASE WHEN types.via = '' THEN format_type(t.oid, NULL) ELSE types.via END
	FROM types
	JOIN pg_type t ON true
	LEFT JOIN pg_class cc ON cc.oid = t.typrelid AND cc.relkind = 'c'
	LEFT JOIN pg_attribute ca ON ca.attrelid = cc.oid AND ca.attnum > 0 AND NOT ca.attisdropped AND ca.atttypid = types.oid
	WHERE (t.typelem = types.oid AND t.typcategory = 'A') OR (t.typtype = 'd' AND t.typbasetype = types.oid) OR ca.attrelid IS NOT NULL
)
SELECT DISTINCT format('column %s.%I', a.attrelid::regclass, a.attname), types.via
FROM types
JOIN pg_attribute a ON a.atttypid = types.oid AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_class c ON c.oid = a.attrelid AND c.relkind IN ('r', 'p', 'm')
ORDER BY 1`

// viewDependentsSQL lists, for each view in $1 (qualified names), what
// depends on it or on its row type.
const viewDependentsSQL = `
WITH v AS (SELECT x AS vname, to_regclass(x) AS oid FROM unnest($1::text[]) x),
deps AS (
	SELECT v.vname, d.classid, d.objid, d.objsubid, false AS rowtype
	FROM v JOIN pg_depend d ON d.refclassid = 'pg_class'::regclass AND d.refobjid = v.oid AND d.deptype = 'n'
	UNION ALL
	SELECT v.vname, d.classid, d.objid, d.objsubid, true
	FROM v JOIN pg_class c ON c.oid = v.oid
	JOIN pg_depend d ON d.refclassid = 'pg_type'::regclass AND d.refobjid = c.reltype AND d.deptype = 'n'
)
SELECT DISTINCT deps.vname, x.kind, x.nsp, x.name, x.display
FROM deps CROSS JOIN LATERAL (
	SELECT * FROM (` + classifySQL + `) k WHERE NOT deps.rowtype
	UNION ALL
	SELECT 'row-type user', '', '', pg_describe_object(deps.classid, deps.objid, deps.objsubid)
	WHERE deps.rowtype AND deps.classid <> 'pg_proc'::regclass
	UNION ALL
	SELECT 'function', '', '', format('function %s', deps.objid::regprocedure)
	WHERE deps.rowtype AND deps.classid = 'pg_proc'::regclass
) x
ORDER BY 1, 5`

func scanDependents(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}, key func(first string) string) (map[string][]V2Dependent, error) {
	out := map[string][]V2Dependent{}
	for rows.Next() {
		var first *string
		var d V2Dependent
		var nsp, name string
		if err := rows.Scan(&first, &d.Kind, &nsp, &name, &d.Display); err != nil {
			return nil, err
		}
		k := ""
		if first != nil {
			k = key(*first)
		}
		if d.Kind == "view" || d.Kind == "materialized view" {
			d.Identity = V2Identity{Schema: nsp, Name: name}
		}
		out[k] = append(out[k], d)
	}
	return out, rows.Err()
}

// ColumnDependents implements V2DependencyInspector.
func (n *TwinNormalizer) ColumnDependents(ctx context.Context, table V2Identity, columns []string, rowType bool) ([]V2Dependent, error) {
	if columns == nil {
		columns = []string{}
	}
	rows, err := n.conn.Query(ctx, columnDependentsSQL, qualifiedNameSQL(table), columns)
	if err != nil {
		return nil, fmt.Errorf("read the dependents of %s: %w", table, err)
	}
	byCol, err := scanDependents(rows, func(s string) string { return s })
	rows.Close()
	if err != nil {
		return nil, fmt.Errorf("read the dependents of %s: %w", table, err)
	}
	var out []V2Dependent
	for _, c := range columns {
		for _, d := range byCol[c] {
			d.Column = c
			out = append(out, d)
		}
	}
	if !rowType {
		return out, nil
	}
	trows, err := n.conn.Query(ctx, rowTypeSQL, qualifiedNameSQL(table))
	if err != nil {
		return nil, fmt.Errorf("read the row-type users of %s: %w", table, err)
	}
	defer trows.Close()
	for trows.Next() {
		var display, via string
		if err := trows.Scan(&display, &via); err != nil {
			return nil, err
		}
		how := "it stores the row type of " + table.String()
		if via != "" {
			how += " through type " + via
		}
		out = append(out, V2Dependent{Kind: "row-type column", Display: display + " (" + how + ")"})
	}
	return out, trows.Err()
}

// ViewDependents implements V2DependencyInspector.
func (n *TwinNormalizer) ViewDependents(ctx context.Context, views []V2Identity) (map[V2Identity][]V2Dependent, error) {
	out := map[V2Identity][]V2Dependent{}
	if len(views) == 0 {
		return out, nil
	}
	names := make([]string, len(views))
	byName := map[string]V2Identity{}
	for i, v := range views {
		names[i] = qualifiedNameSQL(v)
		byName[names[i]] = v
	}
	rows, err := n.conn.Query(ctx, viewDependentsSQL, names)
	if err != nil {
		return nil, fmt.Errorf("read the dependents of views: %w", err)
	}
	defer rows.Close()
	got, err := scanDependents(rows, func(s string) string { return s })
	if err != nil {
		return nil, fmt.Errorf("read the dependents of views: %w", err)
	}
	for k, deps := range got {
		out[byName[k]] = deps
	}
	return out, nil
}

// V2OpclassResolver names the default operator class of a column type for
// an index method, so an explicitly spelled default compares equal to the
// absent class introspection records for it.
type V2OpclassResolver interface {
	DefaultOpclass(ctx context.Context, method, typeSQL string) (string, error)
}

// defaultOpclassSQL resolves the default class of method $1 for type $2
// as PostgreSQL's GetDefaultOpClass does: domains resolve to their base
// type; a default class for the type itself wins; otherwise a class for a
// binary-coercible type (or the polymorphic class of an array, enum,
// range, multirange or composite type), preferring the preferred type of
// the type's category (varchar resolves to text_ops, not bpchar_ops); an
// ambiguous result is NULL.
const defaultOpclassSQL = `
WITH RECURSIVE base(oid, typtype, typbasetype) AS (
	SELECT t.oid, t.typtype, t.typbasetype FROM pg_type t WHERE t.oid = to_regtype($2)
	UNION ALL
	SELECT t.oid, t.typtype, t.typbasetype FROM pg_type t JOIN base ON t.oid = base.typbasetype WHERE base.typtype = 'd'
),
bt AS (
	SELECT ty.oid, ty.typcategory, ty.typtype FROM base JOIN pg_type ty ON ty.oid = base.oid WHERE base.typtype <> 'd'
),
cand AS (
	SELECT c.opcname::text AS name, c.opcintype = bt.oid AS exact,
		(ot.typcategory = bt.typcategory AND ot.typispreferred) AS preferred
	FROM pg_opclass c
	JOIN pg_am a ON a.oid = c.opcmethod
	JOIN pg_type ot ON ot.oid = c.opcintype
	CROSS JOIN bt
	WHERE a.amname = $1 AND c.opcdefault AND (
		c.opcintype = bt.oid
		OR EXISTS (SELECT 1 FROM pg_cast k WHERE k.castsource = bt.oid AND k.casttarget = c.opcintype AND k.castmethod = 'b')
		OR (c.opcintype = 'anyarray'::regtype AND bt.typcategory = 'A')
		OR (c.opcintype = 'anyenum'::regtype AND bt.typtype = 'e')
		OR (c.opcintype = 'anyrange'::regtype AND bt.typtype = 'r')
		OR (c.opcintype = 'anymultirange'::regtype AND bt.typtype = 'm')
		OR (c.opcintype = 'record'::regtype AND bt.typtype = 'c'))
)
SELECT CASE
	WHEN (SELECT count(*) FROM cand WHERE exact) >= 1 THEN
		CASE WHEN (SELECT count(*) FROM cand WHERE exact) = 1 THEN (SELECT name FROM cand WHERE exact) END
	WHEN (SELECT count(*) FROM cand WHERE preferred) = 1 THEN (SELECT name FROM cand WHERE preferred)
	WHEN (SELECT count(*) FROM cand WHERE preferred) = 0 AND (SELECT count(*) FROM cand) = 1 THEN (SELECT name FROM cand)
END`

// DefaultOpclass implements V2OpclassResolver.
func (n *TwinNormalizer) DefaultOpclass(ctx context.Context, method, typeSQL string) (string, error) {
	var name *string
	if err := n.conn.QueryRow(ctx, defaultOpclassSQL, method, typeSQL).Scan(&name); err != nil {
		return "", fmt.Errorf("default operator class of %s for %s: %w", typeSQL, method, err)
	}
	if name == nil {
		return "", fmt.Errorf("no single default operator class of %s for %s", typeSQL, method)
	}
	return *name, nil
}
