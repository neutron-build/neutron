package db

// Live-catalog introspection into schema document v2 (M02).
//
// IntrospectV2 reads user schemas (everything that is not pg_* or
// information_schema) and produces a VALIDATED v2 document: representable
// objects become managed tables/enums/views; everything the contract cannot
// represent faithfully becomes a read-only opaque inventory entry
// (extension-owned objects and unsupported structures). The output is run
// through ParseV2Document before being returned, so an introspection bug that
// produces an unrepresentable document fails loudly instead of silently
// feeding the diff engine a malformed document.
//
// Expression-bearing fields (defaults, check expressions, index predicates
// and key expressions, view definitions) are emitted in the catalog's
// deparsed spelling (pg_get_expr / pg_get_viewdef / pg_get_indexdef) with
// type-name casts compacted to the contract vocabulary ("character varying"
// -> "varchar"). The diff compares desired text against these spellings
// through the twin normalizer, never by trimming guesses.

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// pgQueryer is the subset of the pgx pool/connection API the introspection
// queries need; both *pgxpool.Pool and *pgxpool.Conn satisfy it.
type pgQueryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const introspectV2SchemasSQL = `
SELECT nspname
FROM pg_namespace
WHERE nspname <> 'information_schema' AND nspname !~ '^pg_'
ORDER BY nspname
`

// introspectV2EnumsSQL lists enum types with their values (semantic order)
// and extension ownership (an extension-owned enum is inventory, never
// managed).
const introspectV2EnumsSQL = `
SELECT n.nspname,
       t.typname,
       EXISTS (
         SELECT 1 FROM pg_depend d
         WHERE d.classid = 'pg_type'::regclass AND d.objid = t.oid AND d.deptype = 'e'
       ) AS extension_owned,
       COALESCE((
         SELECT x.extname FROM pg_depend d
         JOIN pg_extension x ON x.oid = d.refobjid
         WHERE d.classid = 'pg_type'::regclass AND d.objid = t.oid AND d.deptype = 'e'
         LIMIT 1
       ), '') AS extension_owner,
       e.enumlabel
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
LEFT JOIN pg_enum e ON e.enumtypid = t.oid
WHERE t.typtype = 'e' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
ORDER BY n.nspname, t.typname, e.enumsortorder
`

// introspectV2RelationsSQL lists every user-schema relation this tool must
// classify: tables (r), partitioned tables (p), views (v), materialized
// views (m), foreign tables (f) and sequences (S), with the flags that make
// a table unrepresentable and extension ownership. sequence_attached marks
// sequences owned by a table column (auto/identity dependencies); those are
// managed through their column's default, not as inventory.
const introspectV2RelationsSQL = `
SELECT c.oid,
       n.nspname,
       c.relname,
       c.relkind::text,
       c.relpersistence::text,
       c.relrowsecurity,
       c.relforcerowsecurity,
       c.relispartition,
       COALESCE(c.reloptions, '{}'::name[])::text[],
       EXISTS (
         SELECT 1 FROM pg_depend d
         WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e'
       ) AS extension_owned,
       COALESCE((
         SELECT x.extname FROM pg_depend d
         JOIN pg_extension x ON x.oid = d.refobjid
         WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e'
         LIMIT 1
       ), '') AS extension_owner,
       (SELECT count(*) FROM pg_policy p WHERE p.polrelid = c.oid) AS npolicies,
       (SELECT count(*) FROM pg_trigger g WHERE g.tgrelid = c.oid AND NOT g.tgisinternal) AS ntriggers,
       (SELECT count(*) FROM pg_inherits i WHERE i.inhrelid = c.oid) AS nparents,
       (SELECT count(*) FROM pg_inherits i2 WHERE i2.inhparent = c.oid) AS nchildren,
       EXISTS (
         SELECT 1 FROM pg_depend d
         WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
           AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')
       ) AS sequence_attached
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
  AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
ORDER BY n.nspname, c.relname
`

const introspectV2ColumnsSQL = `
SELECT a.attnum,
       a.attname,
       tn.nspname AS type_schema,
       t.typname, t.typtype::text AS typtype, t.typcategory::text AS typcategory,
       et.typname AS elem_name, et.typtype::text AS elem_typtype, et.typcategory::text AS elem_category,
       ens.nspname AS elem_schema,
       a.atttypmod,
       a.attnotnull,
       a.attidentity::text,
       a.attgenerated::text,
       a.attcollation <> t.typcollation AS nondefault_collation,
       a.attcollation::oid,
       pg_get_expr(ad.adbin, a.attrelid) AS default_expr
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
JOIN pg_namespace tn ON tn.oid = t.typnamespace
LEFT JOIN pg_type et ON et.oid = t.typelem
LEFT JOIN pg_namespace ens ON ens.oid = et.typnamespace
LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
`

const introspectV2ConstraintsSQL = `
SELECT rc.conname,
       rc.contype::text,
       rc.conkey::int2[],
       rn.nspname AS ref_schema,
       rt.relname AS ref_table,
       (SELECT array_agg(ta.attname ORDER BY k.ord)
          FROM unnest(rc.confkey) WITH ORDINALITY AS k(attnum, ord)
          JOIN pg_attribute ta ON ta.attrelid = rc.confrelid AND ta.attnum = k.attnum) AS ref_cols,
       rc.confdeltype::text,
       rc.confupdtype::text,
       rc.confmatchtype::text,
       rc.condeferrable,
       rc.condeferred,
       pg_get_constraintdef(rc.oid) AS condef,
       pg_get_expr(rc.conbin, rc.conrelid) AS check_expr
FROM pg_constraint rc
LEFT JOIN pg_class rt ON rt.oid = rc.confrelid
LEFT JOIN pg_namespace rn ON rn.oid = rt.relnamespace
WHERE rc.conrelid = $1 AND rc.contype IN ('p', 'u', 'c', 'f', 'x')
ORDER BY rc.conname
`

const introspectV2IndexesSQL = `
SELECT ic.oid,
       ic.relname,
       i.indisunique,
       am.amname,
       i.indnkeyatts,
       i.indnatts,
       i.indkey::int2[],
       i.indoption::int2[],
       i.indcollation::oid[],
       pg_get_indexdef(i.indexrelid) AS indexdef,
       pg_get_expr(i.indpred, i.indrelid) AS predicate,
       EXISTS (
         SELECT 1 FROM pg_depend d
         WHERE d.classid = 'pg_class'::regclass AND d.objid = ic.oid AND d.deptype = 'e'
       ) AS extension_owned,
       (
         SELECT array_agg(oc.opcdefault ORDER BY k.ord)
           FROM unnest(i.indclass) WITH ORDINALITY AS k(opclass, ord)
           JOIN pg_opclass oc ON oc.oid = k.opclass
       ) AS opclass_defaults
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_am am ON am.oid = ic.relam
WHERE i.indrelid = $1
  AND i.indisvalid AND i.indisready
  AND NOT EXISTS (SELECT 1 FROM pg_constraint pc WHERE pc.conindid = i.indexrelid)
ORDER BY ic.relname
`

// introspectV2InvalidIndexSQL finds indexes on a table that are not valid or
// not ready (e.g. from a failed CREATE INDEX CONCURRENTLY). They cannot be
// represented in the document (the contract only describes live indexes) and
// dropping them implicitly would destroy state, so they make the table
// unrepresentable.
const introspectV2InvalidIndexSQL = `
SELECT ic.relname FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_namespace n ON n.oid = ic.relnamespace AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
WHERE i.indrelid = $1 AND (NOT i.indisvalid OR NOT i.indisready)
`

// introspectBaseTypes maps pg_type.typname onto the contract type vocabulary.
// Types absent from this map are unrepresentable.
var introspectBaseTypes = map[string]bool{
	"int2": true, "int4": true, "int8": true,
	"float4": true, "float8": true, "numeric": true,
	"bool": true, "text": true, "varchar": true,
	"timestamp": true, "timestamptz": true, "date": true,
	"bytea": true, "uuid": true, "json": true, "jsonb": true,
	"vector": true,
}

// pgCastCompactions rewrites deparsed type-name casts into the contract's
// compact type spelling so literal defaults validate against the contract's
// literal pattern ("character varying" contains a space and cannot appear in
// a contract literal). Applied to every deparsed default expression; the
// mapping is deterministic and applies equally to the twin-normalized
// desired side, so comparisons stay symmetric.
var pgCastCompactions = []struct {
	re   *regexp.Regexp
	repl string
}{
	{regexp.MustCompile(`::timestamp without time zone`), "::timestamp"},
	{regexp.MustCompile(`::timestamp with time zone`), "::timestamptz"},
	{regexp.MustCompile(`::character varying`), "::varchar"},
	{regexp.MustCompile(`::double precision`), "::float8"},
	{regexp.MustCompile(`::real`), "::float4"},
	{regexp.MustCompile(`::smallint`), "::int2"},
	{regexp.MustCompile(`::integer`), "::int4"},
	{regexp.MustCompile(`::bigint`), "::int8"},
	{regexp.MustCompile(`::boolean`), "::bool"},
}

func compactPGCasts(expr string) string {
	out := expr
	for _, c := range pgCastCompactions {
		out = c.re.ReplaceAllString(out, c.repl)
	}
	return out
}

var nextvalDefaultRe = regexp.MustCompile(`^nextval\('(.*)'::regclass\)$`)

// parseQualifiedRelName splits a deparsed, possibly schema-qualified and
// quoted relation reference ("public.t", "\"odd name\"", "s.\"t.x\"") into
// its schema and name.
func parseQualifiedRelName(ref string) (schema, name string, ok bool) {
	var parts []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(ref); i++ {
		ch := ref[i]
		switch {
		case ch == '"':
			if inQuote && i+1 < len(ref) && ref[i+1] == '"' {
				cur.WriteByte('"')
				i++
			} else {
				inQuote = !inQuote
			}
		case ch == '.' && !inQuote:
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(ch)
		}
	}
	parts = append(parts, cur.String())
	if len(parts) == 1 {
		return "", parts[0], true
	}
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func strPtrV2(s string) *string { return &s }

// referentialActionFromPG maps pg_constraint confdeltype/confupdtype codes
// onto contract actions. "no action" (code a) is the catalog default for an
// omitted clause and is emitted as absent: the catalog cannot distinguish an
// explicit NO ACTION from omission, so the canonical minimal spelling is the
// only deterministic choice.
func referentialActionFromPG(code string) *string {
	switch code {
	case "r":
		return strPtrV2("restrict")
	case "c":
		return strPtrV2("cascade")
	case "n":
		return strPtrV2("set null")
	case "d":
		return strPtrV2("set default")
	}
	return nil
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// v2RelationInfo carries one row of introspectV2RelationsSQL.
type v2RelationInfo struct {
	oid              uint32
	schema, name     string
	kind             string
	persistence      string
	rowSecurity      bool
	forceRowSecurity bool
	isPartition      bool
	reloptions       []string
	extensionOwned   bool
	extensionOwner   string
	policies         int
	triggers         int
	parents          int
	children         int
	sequenceAttached bool
}

// IntrospectV2 reads the connected database into a validated schema
// document v2.
func (c *Client) IntrospectV2(ctx context.Context) (*V2Document, error) {
	schemaNames, err := c.introspectV2Schemas(ctx)
	if err != nil {
		return nil, err
	}

	enums, opaque, err := c.introspectV2Enums(ctx)
	if err != nil {
		return nil, err
	}

	relations, err := c.introspectV2Relations(ctx)
	if err != nil {
		return nil, err
	}

	model := V2DocumentModel{
		Version:      SchemaDocumentVersionV2,
		Dialect:      "postgresql",
		Capabilities: []string{},
		Schemas:      make([]V2SchemaDecl, 0, len(schemaNames)),
		Enums:        enums,
		Views:        []V2View{},
		Opaque:       opaque,
	}
	declared := make(map[string]bool, len(schemaNames))
	for _, s := range schemaNames {
		model.Schemas = append(model.Schemas, V2SchemaDecl{Name: s})
		declared[s] = true
	}

	// Candidate tables: relkind r, not extension-owned. Everything else in
	// the relation list becomes inventory (or a view declaration).
	type tableResult struct {
		info    v2RelationInfo
		table   V2Table
		reasons []string
		fkRefs  []V2Identity
	}
	var candidates []*tableResult
	unrepresentable := make(map[V2Identity]string) // identity -> reason (grows with cascades)

	for i := range relations {
		rel := relations[i]
		id := V2Identity{Schema: rel.schema, Name: rel.name}
		switch {
		case rel.extensionOwned:
			kind := "extension-object"
			if rel.kind == "r" || rel.kind == "p" {
				kind = "extension-table"
			}
			model.Opaque = append(model.Opaque, V2Opaque{
				Kind: kind, Identity: id, Owner: rel.extensionOwner,
				Reason: fmt.Sprintf("owned by extension %s", rel.extensionOwner),
			})
			unrepresentable[id] = "extension-owned object (never managed)"
		case rel.kind == "S":
			if rel.sequenceAttached {
				continue // managed through its column's sequence/identity default
			}
			model.Opaque = append(model.Opaque, V2Opaque{
				Kind: "unsupported-object", Identity: id,
				Reason: "standalone sequence (sequences are managed only as column defaults)",
			})
			unrepresentable[id] = "standalone sequence"
		case rel.kind == "m":
			model.Opaque = append(model.Opaque, V2Opaque{
				Kind: "unsupported-object", Identity: id,
				Reason: "materialized view (not representable in schema document v2)",
			})
			unrepresentable[id] = "materialized view"
		case rel.kind == "f":
			model.Opaque = append(model.Opaque, V2Opaque{
				Kind: "unsupported-table", Identity: id,
				Reason: "foreign table (not representable in schema document v2)",
			})
			unrepresentable[id] = "foreign table"
		case rel.kind == "p":
			model.Opaque = append(model.Opaque, V2Opaque{
				Kind: "unsupported-table", Identity: id,
				Reason: "partitioned table (partitioning is not representable in schema document v2)",
			})
			unrepresentable[id] = "partitioned table"
		case rel.kind == "v":
			view, reasons, err := c.introspectV2View(ctx, rel)
			if err != nil {
				return nil, err
			}
			if len(reasons) > 0 {
				// The contract models a plain SELECT view only; a view
				// carrying unrepresented structure must never be
				// flattened into that shape (README 4.1): inventory it
				// as opaque so it is reported, never planned, and
				// blocks a desired view with the same identity.
				model.Opaque = append(model.Opaque, V2Opaque{
					Kind:     "unsupported-object",
					Identity: id,
					Reason:   strings.Join(reasons, "; "),
				})
				continue
			}
			model.Views = append(model.Views, view)
		case rel.kind == "r":
			res := &tableResult{info: rel}
			var reasons []string
			if rel.isPartition {
				reasons = append(reasons, "member of a partitioned table")
			}
			if rel.parents > 0 || rel.children > 0 {
				reasons = append(reasons, "table inheritance parent or child")
			}
			if rel.persistence == "u" {
				reasons = append(reasons, "unlogged table")
			}
			if rel.rowSecurity || rel.forceRowSecurity {
				reasons = append(reasons, "row-level security enabled")
			}
			if rel.policies > 0 {
				reasons = append(reasons, fmt.Sprintf("%d row-level security polic%s", rel.policies, pluralY(rel.policies)))
			}
			if rel.triggers > 0 {
				reasons = append(reasons, fmt.Sprintf("%d user trigger%s", rel.triggers, pluralS(rel.triggers)))
			}
			res.reasons = reasons
			candidates = append(candidates, res)
		}
	}

	// Detail pass: columns, constraints, indexes per candidate.
	hasVector := false
	for _, res := range candidates {
		table, reasons, fkRefs, err := c.introspectV2Table(ctx, res.info, &hasVector)
		if err != nil {
			return nil, err
		}
		res.table = table
		res.reasons = append(res.reasons, reasons...)
		res.fkRefs = fkRefs
	}

	// FK cascade: a table whose foreign key targets an unrepresentable
	// table cannot be managed either (its declared FK could not be rendered
	// faithfully). Iterate to a fixpoint.
	byIdentity := make(map[V2Identity]*tableResult, len(candidates))
	for _, res := range candidates {
		byIdentity[res.table.Identity] = res
	}
	changed := true
	for changed {
		changed = false
		for _, res := range candidates {
			if len(res.reasons) > 0 {
				continue
			}
			for _, ref := range res.fkRefs {
				if reason, bad := unrepresentable[ref]; bad {
					res.reasons = append(res.reasons, fmt.Sprintf(
						"foreign key references table %s (%s)", ref, reason))
					changed = true
					break
				}
				if target := byIdentity[ref]; target != nil && len(target.reasons) > 0 {
					res.reasons = append(res.reasons, fmt.Sprintf(
						"foreign key references table %s, which carries unrepresentable structure", ref))
					changed = true
					break
				}
				if !declared[ref.Schema] {
					res.reasons = append(res.reasons, fmt.Sprintf(
						"foreign key references table %s outside the user schemas this tool manages", ref))
					changed = true
					break
				}
			}
		}
	}

	for _, res := range candidates {
		if len(res.reasons) > 0 {
			unrepresentable[res.table.Identity] = strings.Join(res.reasons, "; ")
			model.Opaque = append(model.Opaque, V2Opaque{
				Kind:     "unsupported-table",
				Identity: res.table.Identity,
				Reason:   strings.Join(res.reasons, "; "),
			})
			continue
		}
		model.Tables = append(model.Tables, res.table)
	}

	if hasVector {
		model.Capabilities = append(model.Capabilities, "pgvector")
	}
	// The contract requires every collection present; nil slices would
	// marshal to null and invalidate the document.
	if model.Schemas == nil {
		model.Schemas = []V2SchemaDecl{}
	}
	if model.Tables == nil {
		model.Tables = []V2Table{}
	}
	if model.Enums == nil {
		model.Enums = []V2EnumDecl{}
	}
	if model.Views == nil {
		model.Views = []V2View{}
	}
	if model.Opaque == nil {
		model.Opaque = []V2Opaque{}
	}
	if model.Capabilities == nil {
		model.Capabilities = []string{}
	}
	for i := range model.Tables {
		if model.Tables[i].Constraints == nil {
			model.Tables[i].Constraints = []V2Constraint{}
		}
		if model.Tables[i].Indexes == nil {
			model.Tables[i].Indexes = []V2Index{}
		}
	}
	sortV2Model(&model)

	root, err := RootFromModel(model)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("encode introspected document: %w", err)
	}
	doc, err := ParseV2Document(raw)
	if err != nil {
		return nil, fmt.Errorf("introspected catalog does not form a valid schema document v2 (introspection bug): %w", err)
	}
	return doc, nil
}

func pluralS(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func pluralY(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

func sortV2Model(m *V2DocumentModel) {
	sort.Slice(m.Opaque, func(i, j int) bool {
		a, b := m.Opaque[i], m.Opaque[j]
		if a.Identity.String() != b.Identity.String() {
			return a.Identity.String() < b.Identity.String()
		}
		return a.Kind < b.Kind
	})
	sort.Slice(m.Tables, func(i, j int) bool { return m.Tables[i].Identity.String() < m.Tables[j].Identity.String() })
	sort.Slice(m.Enums, func(i, j int) bool { return m.Enums[i].Identity.String() < m.Enums[j].Identity.String() })
	sort.Slice(m.Views, func(i, j int) bool { return m.Views[i].Identity.String() < m.Views[j].Identity.String() })
	sort.Strings(m.Capabilities)
}

func (c *Client) introspectV2Schemas(ctx context.Context) ([]string, error) {
	rows, err := c.pool.Query(ctx, introspectV2SchemasSQL)
	if err != nil {
		return nil, fmt.Errorf("introspect v2 schemas: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func (c *Client) introspectV2Enums(ctx context.Context) (enums []V2EnumDecl, opaque []V2Opaque, err error) {
	rows, err := c.pool.Query(ctx, introspectV2EnumsSQL)
	if err != nil {
		return nil, nil, fmt.Errorf("introspect v2 enums: %w", err)
	}
	defer rows.Close()

	type enumState struct {
		decl     V2EnumDecl
		extOwned bool
		extOwner string
	}
	byKey := map[string]*enumState{}
	var order []string
	for rows.Next() {
		var schema, typname, owner string
		var extOwned bool
		var label *string
		if err := rows.Scan(&schema, &typname, &extOwned, &owner, &label); err != nil {
			return nil, nil, err
		}
		key := schema + "." + typname
		st, ok := byKey[key]
		if !ok {
			st = &enumState{
				decl: V2EnumDecl{
					Identity: V2Identity{Schema: schema, Name: typname},
					Managed:  true,
					Values:   []string{},
				},
				extOwned: extOwned,
				extOwner: owner,
			}
			byKey[key] = st
			order = append(order, key)
		}
		if label != nil {
			st.decl.Values = append(st.decl.Values, *label)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	for _, key := range order {
		st := byKey[key]
		if st.extOwned {
			opaque = append(opaque, V2Opaque{
				Kind:     "extension-object",
				Identity: st.decl.Identity,
				Owner:    st.extOwner,
				Reason:   fmt.Sprintf("enum type owned by extension %s", st.extOwner),
			})
			continue
		}
		if len(st.decl.Values) == 0 {
			return nil, nil, fmt.Errorf("introspection bug: enum %s has no values", key)
		}
		enums = append(enums, st.decl)
	}
	return enums, opaque, nil
}

func (c *Client) introspectV2Relations(ctx context.Context) ([]v2RelationInfo, error) {
	rows, err := c.pool.Query(ctx, introspectV2RelationsSQL)
	if err != nil {
		return nil, fmt.Errorf("introspect v2 relations: %w", err)
	}
	defer rows.Close()
	var out []v2RelationInfo
	for rows.Next() {
		var rel v2RelationInfo
		if err := rows.Scan(
			&rel.oid, &rel.schema, &rel.name, &rel.kind, &rel.persistence,
			&rel.rowSecurity, &rel.forceRowSecurity, &rel.isPartition, &rel.reloptions,
			&rel.extensionOwned, &rel.extensionOwner,
			&rel.policies, &rel.triggers, &rel.parents, &rel.children, &rel.sequenceAttached,
		); err != nil {
			return nil, err
		}
		out = append(out, rel)
	}
	return out, rows.Err()
}

// introspectV2View reads one view. Returned reasons make the view
// unrepresentable (opaque inventory): the contract models a plain SELECT
// view with check_option/security_invoker only, so user triggers (INSTEAD
// OF or statement-level), security barriers and any other unmodeled
// reloption must never be flattened into the plain shape.
func (c *Client) introspectV2View(ctx context.Context, rel v2RelationInfo) (V2View, []string, error) {
	view := V2View{
		Identity: V2Identity{Schema: rel.schema, Name: rel.name},
		Managed:  true,
	}
	var reasons []string
	if rel.triggers > 0 {
		reasons = append(reasons, fmt.Sprintf("%d user trigger%s (not representable in schema document v2)", rel.triggers, pluralS(rel.triggers)))
	}
	if rel.rowSecurity || rel.forceRowSecurity {
		reasons = append(reasons, "row-level security enabled (not representable in schema document v2)")
	}
	for _, opt := range rel.reloptions {
		switch {
		case opt == "check_option=local":
			view.CheckOption = strPtrV2("local")
		case opt == "check_option=cascaded":
			view.CheckOption = strPtrV2("cascaded")
		case opt == "security_invoker=true":
			view.SecurityInvoker = boolPtr(true)
		case opt == "security_invoker=false":
			// Canonical equivalence (Q07d): absent and explicit false are
			// the same PostgreSQL state (the default), and the contract's
			// canonical form writes securityInvoker only when true —
			// recording an explicit false would make an unchanged view
			// differ from every desired document forever.
		default:
			reasons = append(reasons, fmt.Sprintf("view option %q is not representable in schema document v2", opt))
		}
	}
	if len(reasons) > 0 {
		return view, reasons, nil
	}
	var def string
	err := c.pool.QueryRow(ctx,
		`SELECT pg_get_viewdef(c.oid) FROM pg_class c WHERE c.oid = $1`, rel.oid).Scan(&def)
	if err != nil {
		return view, nil, fmt.Errorf("introspect view definition %s: %w", view.Identity, err)
	}
	view.Definition = def
	return view, nil, nil
}

// introspectV2Table reads one candidate table. Returned reasons make the
// table unrepresentable (opaque inventory); fkRefs are the qualified FK
// target identities for the cascade pass.
func (c *Client) introspectV2Table(ctx context.Context, rel v2RelationInfo, hasVector *bool) (V2Table, []string, []V2Identity, error) {
	return introspectV2TableOn(ctx, c.pool, rel, hasVector)
}

// introspectV2TableOn runs the per-table introspection against a specific
// query target (pool for live introspection, a pinned connection for the
// twin normalizer's temporary objects).
func introspectV2TableOn(ctx context.Context, q pgQueryer, rel v2RelationInfo, hasVector *bool) (V2Table, []string, []V2Identity, error) {
	table := V2Table{
		Identity:    V2Identity{Schema: rel.schema, Name: rel.name},
		Managed:     true,
		Constraints: []V2Constraint{},
		Indexes:     []V2Index{},
	}
	var reasons []string
	var fkRefs []V2Identity

	// Invalid/not-ready indexes are unrepresentable state on this table.
	invRows, err := q.Query(ctx, introspectV2InvalidIndexSQL, rel.oid)
	if err != nil {
		return table, nil, nil, fmt.Errorf("introspect invalid indexes of %s: %w", table.Identity, err)
	}
	defer invRows.Close()
	for invRows.Next() {
		var name string
		if err := invRows.Scan(&name); err != nil {
			return table, nil, nil, err
		}
		reasons = append(reasons, fmt.Sprintf("index %q is invalid or not ready", name))
	}
	if err := invRows.Err(); err != nil {
		return table, nil, nil, err
	}

	// Columns in attnum order (the contract's ordered tuple).
	attnumNames := make(map[int16]string)
	attnumCollations := make(map[int16]uint32)
	columns := []V2Column{}
	colRows, err := q.Query(ctx, introspectV2ColumnsSQL, rel.oid)
	if err != nil {
		return table, nil, nil, fmt.Errorf("introspect v2 columns of %s: %w", table.Identity, err)
	}
	defer colRows.Close()
	for colRows.Next() {
		var (
			attnum                             int16
			name, typeSchema, typname, typtype string
			typcategory                        string
			elemName, elemSchema               *string
			elemTypetype, elemCategory         *string
			typmod                             int
			notNull                            bool
			identity, generated                string
			nonDefaultCollation                bool
			collation                          uint32
			defaultExpr                        *string
		)
		if err := colRows.Scan(
			&attnum, &name, &typeSchema, &typname, &typtype, &typcategory,
			&elemName, &elemTypetype, &elemCategory, &elemSchema,
			&typmod, &notNull, &identity, &generated, &nonDefaultCollation, &collation, &defaultExpr,
		); err != nil {
			return table, nil, nil, err
		}
		attnumNames[attnum] = name
		attnumCollations[attnum] = collation

		if generated == "v" {
			reasons = append(reasons, fmt.Sprintf("column %q is a virtual generated column (not representable in schema document v2)", name))
			continue
		}
		if generated != "" && generated != "s" {
			reasons = append(reasons, fmt.Sprintf("column %q has unknown attgenerated kind %q", name, generated))
			continue
		}
		if nonDefaultCollation {
			reasons = append(reasons, fmt.Sprintf("column %q has a non-default collation", name))
			continue
		}

		colType, typeReason := introspectV2ColumnType(name, typeSchema, typname, typtype, typcategory,
			elemName, elemTypetype, elemCategory, elemSchema, typmod)
		if typeReason != "" {
			reasons = append(reasons, typeReason)
			continue
		}
		if colType.Name == "vector" {
			*hasVector = true
		}

		col := V2Column{Name: name, Type: colType, NotNull: notNull}
		if generated == "s" {
			// Stored generated column: the pg_attrdef row carries the
			// generation expression, which is NOT a default.
			if defaultExpr == nil {
				reasons = append(reasons, fmt.Sprintf("generated column %q has no stored expression in the catalog", name))
				continue
			}
			col.Generated = &V2Generated{Expression: *defaultExpr}
		} else if identity == "a" || identity == "d" {
			gen := "always"
			if identity == "d" {
				gen = "by default"
			}
			col.Default = &V2ColumnDefault{Kind: "identity", Generated: strPtrV2(gen)}
		} else if defaultExpr != nil {
			def, defReason := introspectV2Default(*defaultExpr, rel.schema, colType)
			if defReason != "" {
				reasons = append(reasons, defReason)
				continue
			}
			col.Default = def
		}
		columns = append(columns, col)
	}
	if err := colRows.Err(); err != nil {
		return table, nil, nil, err
	}
	if len(columns) == 0 && len(reasons) == 0 {
		reasons = append(reasons, "table has no introspectable columns")
	}

	// Constraints (composite PK/unique/check/FK; order within key tuples is
	// semantic and preserved).
	conRows, err := q.Query(ctx, introspectV2ConstraintsSQL, rel.oid)
	if err != nil {
		return table, nil, nil, fmt.Errorf("introspect v2 constraints of %s: %w", table.Identity, err)
	}
	defer conRows.Close()
	constraintNames := map[string]bool{}
	for conRows.Next() {
		var (
			name, contype        string
			conkey               []int16
			refSchema, refTable  *string
			refCols              []string
			confDel, confUpd     *string
			confMatch            *string
			deferrable, deferred bool
			condef               string
			checkExpr            *string
		)
		if err := conRows.Scan(&name, &contype, &conkey, &refSchema, &refTable, &refCols,
			&confDel, &confUpd, &confMatch, &deferrable, &deferred, &condef, &checkExpr); err != nil {
			return table, nil, nil, err
		}
		if constraintNames[name] {
			reasons = append(reasons, fmt.Sprintf("duplicate constraint name %q (catalog anomaly)", name))
			continue
		}
		constraintNames[name] = true
		// NULLS NOT DISTINCT is detected from the constraint definition:
		// the pg_constraint.connullsnotdistinct catalog column is a
		// PostgreSQL 15+ addition that wire-compatible engines may not
		// carry, while the deparse spelling is authoritative wherever the
		// syntax exists at all.
		if contype == "u" && strings.Contains(condef, "NULLS NOT DISTINCT") {
			reasons = append(reasons, fmt.Sprintf("unique constraint %q is NULLS NOT DISTINCT (not representable in schema document v2)", name))
			continue
		}
		if contype == "x" {
			reasons = append(reasons, fmt.Sprintf("exclusion constraint %q (not representable)", name))
			continue
		}

		con := V2Constraint{Name: name}
		bad := false
		// The contract carries columns only on key constraints; check
		// constraints are expression-only.
		if contype != "c" {
			con.Columns = []string{}
			for _, attnum := range conkey {
				cn, ok := attnumNames[attnum]
				if !ok {
					reasons = append(reasons, fmt.Sprintf("constraint %q references an introspect-rejected column", name))
					bad = true
					break
				}
				con.Columns = append(con.Columns, cn)
			}
		}
		if bad {
			continue
		}

		switch contype {
		case "p":
			con.Type = "primary-key"
		case "u":
			con.Type = "unique"
		case "c":
			con.Type = "check"
			if checkExpr == nil {
				reasons = append(reasons, fmt.Sprintf("check constraint %q has no readable expression", name))
				continue
			}
			con.Expression = strPtrV2(compactPGCasts(*checkExpr))
		case "f":
			con.Type = "foreign-key"
			if refSchema == nil || refTable == nil || refCols == nil {
				reasons = append(reasons, fmt.Sprintf("foreign key %q has an unreadable target", name))
				continue
			}
			ref := V2FKReference{
				Table:    V2Identity{Schema: *refSchema, Name: *refTable},
				Columns:  refCols,
				OnDelete: referentialActionFromPG(deref(confDel)),
				OnUpdate: referentialActionFromPG(deref(confUpd)),
			}
			if m := confMatch; m != nil && *m != "s" {
				switch *m {
				case "f":
					ref.Match = strPtrV2("full")
				case "p":
					ref.Match = strPtrV2("partial")
				}
			}
			con.References = &ref
			fkRefs = append(fkRefs, ref.Table)
		}
		if deferrable {
			con.Deferrable = boolPtr(true)
			if deferred {
				con.InitiallyDeferred = boolPtr(true)
			}
		}
		table.Constraints = append(table.Constraints, con)
	}
	if err := conRows.Err(); err != nil {
		return table, nil, nil, err
	}

	// Indexes (non-constraint-backed): method, ordered key parts (column or
	// expression), predicate and INCLUDE columns. Rows are drained before
	// the per-part pg_get_indexdef queries so a single pinned connection
	// (the twin normalizer) never interleaves an open iterator with a new
	// query.
	type rawIndex struct {
		oid             uint32
		name            string
		method          string
		unique          bool
		nkey, natt      int
		indkey          []int16
		indoption       []int16
		indcoll         []uint32
		indexdef        string
		predicate       *string
		extOwned        bool
		opclassDefaults []bool
	}
	var rawIndexes []rawIndex
	idxRows, err := q.Query(ctx, introspectV2IndexesSQL, rel.oid)
	if err != nil {
		return table, nil, nil, fmt.Errorf("introspect v2 indexes of %s: %w", table.Identity, err)
	}
	for idxRows.Next() {
		var r rawIndex
		if err := idxRows.Scan(&r.oid, &r.name, &r.unique, &r.method, &r.nkey, &r.natt, &r.indkey, &r.indoption, &r.indcoll, &r.indexdef, &r.predicate, &r.extOwned, &r.opclassDefaults); err != nil {
			idxRows.Close()
			return table, nil, nil, err
		}
		rawIndexes = append(rawIndexes, r)
	}
	if err := idxRows.Err(); err != nil {
		idxRows.Close()
		return table, nil, nil, err
	}
	idxRows.Close()

	for _, r := range rawIndexes {
		if r.extOwned {
			reasons = append(reasons, fmt.Sprintf("index %q is owned by an extension", r.name))
			continue
		}
		// NULLS NOT DISTINCT unique indexes: detected from the index
		// definition (pg_index.indnullsnotdistinct is a PostgreSQL 15+
		// catalog column wire-compatible engines may not carry; the deparse
		// spelling is authoritative wherever the syntax exists).
		if r.unique && strings.Contains(r.indexdef, "NULLS NOT DISTINCT") {
			reasons = append(reasons, fmt.Sprintf("unique index %q is NULLS NOT DISTINCT (not representable in schema document v2)", r.name))
			continue
		}
		idx := V2Index{
			Identity: V2Identity{Schema: rel.schema, Name: r.name},
			Unique:   r.unique,
			Method:   r.method,
			Key:      []V2IndexKeyPart{},
		}
		bad := false
		for i := 0; i < r.nkey; i++ {
			// Per-part ordering, in the contract's minimal form (Q07):
			// indoption bit 0x0001 = DESC, bit 0x0002 = NULLS FIRST. ASC
			// defaults to NULLS LAST and DESC to NULLS FIRST, so nulls is
			// written only when it is not the direction default.
			var order, nulls *string
			if i < len(r.indoption) && r.indoption[i] != 0 {
				desc := r.indoption[i]&0x0001 != 0
				nullsFirst := r.indoption[i]&0x0002 != 0
				if desc {
					order = strPtrV2("desc")
					if !nullsFirst {
						nulls = strPtrV2("last")
					}
				} else if nullsFirst {
					nulls = strPtrV2("first")
				}
			}
			// A non-default opclass is not representable (the contract key
			// shape has no opclass slot). pg_get_indexdef(part) omits
			// opclass/collation entirely, so this is checked against the
			// catalog, never the deparse.
			if i < len(r.opclassDefaults) && !r.opclassDefaults[i] {
				reasons = append(reasons, fmt.Sprintf("index %q key part %d uses a non-default operator class (not representable)", r.name, i+1))
				bad = true
				break
			}
			attnum := r.indkey[i]
			if attnum == 0 {
				if order != nil || nulls != nil {
					reasons = append(reasons, fmt.Sprintf("index %q orders expression key part %d (ordered expression keys are not representable)", r.name, i+1))
					bad = true
					break
				}
				expr, err := indexPartExpressionOn(ctx, q, r.oid, i+1)
				if err != nil {
					return table, nil, nil, err
				}
				idx.Key = append(idx.Key, V2IndexKeyPart{Expression: strPtrV2(expr)})
				continue
			}
			cn, ok := attnumNames[attnum]
			if !ok {
				reasons = append(reasons, fmt.Sprintf("index %q references an introspect-rejected column", r.name))
				bad = true
				break
			}
			// An explicit COLLATE on the key part (indcollation differing
			// from the column's own collation) is not representable; the
			// deparse omits it, so this is checked against the catalog.
			partColl := uint32(0)
			if i < len(r.indcoll) {
				partColl = r.indcoll[i]
			}
			if partColl != attnumCollations[attnum] {
				reasons = append(reasons, fmt.Sprintf("index %q key part %d carries an explicit COLLATE (not representable)", r.name, i+1))
				bad = true
				break
			}
			// pg_get_indexdef(idx, part, false) returns the bare column
			// name; anything else means a non-default decoration the
			// catalog checks above did not classify.
			partDef, err := indexPartExpressionOn(ctx, q, r.oid, i+1)
			if err != nil {
				return table, nil, nil, err
			}
			if partDef != cn && partDef != quoteIdent(cn) {
				reasons = append(reasons, fmt.Sprintf("index %q key part %d carries a non-default decoration (%q)", r.name, i+1, partDef))
				bad = true
				break
			}
			idx.Key = append(idx.Key, V2IndexKeyPart{Column: strPtrV2(cn), Order: order, Nulls: nulls})
		}
		if bad {
			continue
		}
		for i := r.nkey; i < r.natt; i++ {
			attnum := r.indkey[i]
			if attnum == 0 {
				reasons = append(reasons, fmt.Sprintf("index %q INCLUDE lists an expression (not representable)", r.name))
				bad = true
				break
			}
			cn, ok := attnumNames[attnum]
			if !ok {
				reasons = append(reasons, fmt.Sprintf("index %q includes an introspect-rejected column", r.name))
				bad = true
				break
			}
			idx.Include = append(idx.Include, cn)
		}
		if bad {
			continue
		}
		if r.predicate != nil {
			idx.Where = strPtrV2(compactPGCasts(*r.predicate))
		}
		table.Indexes = append(table.Indexes, idx)
	}

	table.Columns = columns
	return table, reasons, fkRefs, nil
}

func (c *Client) indexPartExpression(ctx context.Context, indexOID uint32, part int) (string, error) {
	return indexPartExpressionOn(ctx, c.pool, indexOID, part)
}

func indexPartExpressionOn(ctx context.Context, q pgQueryer, indexOID uint32, part int) (string, error) {
	var def string
	if err := q.QueryRow(ctx, `SELECT pg_get_indexdef($1, $2, false)`, indexOID, part).Scan(&def); err != nil {
		return "", fmt.Errorf("read index key part %d of index %d: %w", part, indexOID, err)
	}
	return def, nil
}

// introspectV2ColumnType maps catalog type information onto the contract
// type shape. A non-empty return reason makes the table unrepresentable.
func introspectV2ColumnType(colName, typeSchema, typname, typtype, typcategory string,
	elemName, elemTypetype, elemCategory, elemSchema *string, typmod int) (V2ColumnType, string) {

	build := func(name string, params map[string]int64, enum *V2Identity, array bool) V2ColumnType {
		codec := v2TypeCodecs[name]
		if array {
			codec = "array"
		}
		return V2ColumnType{Name: name, Params: params, Array: array, Codec: codec, Enum: enum}
	}

	var base func(typeSchema, typname, typtype, typcategory string, elemName, elemTypetype, elemCategory, elemSchema *string) (V2ColumnType, string)
	base = func(typeSchema, typname, typtype, typcategory string, elemName, elemTypetype, elemCategory, elemSchema *string) (V2ColumnType, string) {
		if typtype == "e" {
			// Enum types are resolved by identity; the enum itself is
			// declared (or inventoried) by the enums pass.
			id := V2Identity{Schema: typeSchema, Name: typname}
			return build("enum", nil, &id, false), ""
		}
		if typcategory == "A" {
			if elemName == nil || elemTypetype == nil || elemCategory == nil {
				return V2ColumnType{}, fmt.Sprintf("column %q is an array with an unreadable element type", colName)
			}
			elem, reason := base(*elemSchema, *elemName, *elemTypetype, *elemCategory, nil, nil, nil, nil)
			if reason != "" {
				return V2ColumnType{}, reason
			}
			elem.Array = true
			elem.Codec = "array"
			return elem, ""
		}
		if !introspectBaseTypes[typname] {
			return V2ColumnType{}, fmt.Sprintf("column %q has type %q, which is outside the contract type vocabulary", colName, typname)
		}
		var params map[string]int64
		switch typname {
		case "varchar":
			if typmod != -1 {
				params = map[string]int64{"length": int64(typmod - 4)}
			}
		case "numeric":
			if typmod != -1 {
				t := typmod - 4
				params = map[string]int64{"precision": int64(t >> 16), "scale": int64(t & 0xFFFF)}
			}
		case "timestamp", "timestamptz":
			if typmod != -1 {
				params = map[string]int64{"precision": int64(typmod)}
			}
		case "vector":
			if typmod > 0 {
				params = map[string]int64{"dimensions": int64(typmod)}
			}
		}
		return build(typname, params, nil, false), ""
	}

	return base(typeSchema, typname, typtype, typcategory, elemName, elemTypetype, elemCategory, elemSchema)
}

// introspectV2Default classifies a deparsed column default into the tagged
// contract shape: identity columns, nextval sequence references, single
// literal tokens (after cast compaction) and everything else as expression.
func introspectV2Default(deparsed, tableSchema string, colType V2ColumnType) (*V2ColumnDefault, string) {
	if m := nextvalDefaultRe.FindStringSubmatch(deparsed); m != nil && v2IntegerTypes[colType.Name] && !colType.Array {
		schema, name, ok := parseQualifiedRelName(m[1])
		if !ok {
			return nil, fmt.Sprintf("default %q references an unparseable sequence name", deparsed)
		}
		if schema == "" {
			schema = tableSchema
		}
		return &V2ColumnDefault{Kind: "sequence", Sequence: &V2Identity{Schema: schema, Name: name}}, ""
	}
	compacted := compactPGCasts(deparsed)
	if v2LiteralRegexp.MatchString(compacted) {
		return &V2ColumnDefault{Kind: "literal", SQL: strPtrV2(compacted)}, ""
	}
	if err := v2CheckSQLText(compacted, "default", true); err != nil {
		return nil, fmt.Sprintf("column default %q is not representable (%v)", deparsed, err)
	}
	return &V2ColumnDefault{Kind: "expression", SQL: strPtrV2(compacted)}, ""
}
