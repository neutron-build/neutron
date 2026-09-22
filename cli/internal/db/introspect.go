package db

import (
	"context"
	"fmt"
	"strings"
)

// IntrospectSchema reads the current schema of the connected database
// (default schema = current_schema()) into the same Schema model the JSON
// exporter produces, so DiffSchema can compare them.

const introspectTablesSQL = `
SELECT c.relname,
       EXISTS (
         SELECT 1 FROM pg_depend d
         WHERE d.classid = 'pg_class'::regclass
           AND d.objid = c.oid
           AND d.deptype = 'e'
       ) AS extension_owned,
       COALESCE((
         SELECT e.extname
         FROM pg_depend d
         JOIN pg_extension e ON e.oid = d.refobjid
         WHERE d.classid = 'pg_class'::regclass
           AND d.objid = c.oid
           AND d.deptype = 'e'
         LIMIT 1
       ), '') AS extension_owner
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = current_schema()
WHERE c.relkind IN ('r', 'p')
ORDER BY c.relname
`

const introspectColumnsSQL = `
SELECT c.column_name,
       c.data_type,
       c.character_maximum_length,
       c.is_nullable = 'YES',
       c.column_default,
       EXISTS (
         SELECT 1 FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage k
           ON k.constraint_name = tc.constraint_name AND k.table_schema = tc.table_schema
         WHERE tc.table_schema = c.table_schema
           AND tc.table_name = c.table_name
           AND tc.constraint_type = 'PRIMARY KEY'
           AND k.column_name = c.column_name
       ) AS is_pk
FROM information_schema.columns c
WHERE c.table_schema = current_schema() AND c.table_name = $1
ORDER BY c.ordinal_position
`

const introspectIndexesSQL = `
SELECT ic.relname AS index_name,
       i.indisunique,
       string_agg(a.attname, ',' ORDER BY x.ord) AS cols
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_class tc ON tc.oid = i.indrelid
JOIN pg_namespace n ON n.oid = tc.relnamespace AND n.nspname = current_schema()
CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord)
JOIN pg_attribute a ON a.attrelid = tc.oid AND a.attnum = x.attnum
WHERE tc.relname = $1 AND NOT i.indisprimary
  -- constraint-backed indexes are managed as column constraints, not indexes
  AND NOT EXISTS (SELECT 1 FROM pg_constraint pc WHERE pc.conindid = i.indexrelid)
GROUP BY ic.relname, i.indisunique
ORDER BY ic.relname
`

const introspectFKsSQL = `
SELECT fa.attname AS concol,
       rt.relname AS reftable,
       ta.attname AS refcol,
       rc.confdeltype::text AS deltype
FROM pg_constraint rc
JOIN pg_class cl ON cl.oid = rc.conrelid
JOIN pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = current_schema()
JOIN pg_class rt ON rt.oid = rc.confrelid
CROSS JOIN LATERAL unnest(rc.conkey, rc.confkey) AS k(cnum, rnum)
JOIN pg_attribute fa ON fa.attrelid = cl.oid AND fa.attnum = k.cnum
JOIN pg_attribute ta ON ta.attrelid = rt.oid AND ta.attnum = k.rnum
WHERE rc.contype = 'f' AND cl.relname = $1
ORDER BY fa.attname
`

const introspectUniquesSQL = `
SELECT rc.conname, fa.attname
FROM pg_constraint rc
JOIN pg_class cl ON cl.oid = rc.conrelid
JOIN pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = current_schema()
CROSS JOIN LATERAL unnest(rc.conkey) AS k(cnum)
JOIN pg_attribute fa ON fa.attrelid = cl.oid AND fa.attnum = k.cnum
WHERE rc.contype = 'u' AND cl.relname = $1
ORDER BY rc.conname, fa.attname
`

// introspectUnsupportedSQL finds catalog structures on a table that the
// current schema model cannot represent faithfully: composite (multi-column)
// unique constraints, composite foreign keys, and indexes that are not plain
// btree column indexes (partial, expression, non-btree methods). Planning
// against such tables is rejected instead of claiming synchronization.
const introspectUnsupportedSQL = `
SELECT reason FROM (
  SELECT format('multi-column unique constraint %I (spanning %s columns)',
                rc.conname, cardinality(rc.conkey)) AS reason
  FROM pg_constraint rc
  JOIN pg_class cl ON cl.oid = rc.conrelid
  JOIN pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = current_schema()
  WHERE rc.contype = 'u' AND cl.relname = $1 AND cardinality(rc.conkey) > 1
  UNION ALL
  SELECT format('composite foreign key %I (spanning %s columns)',
                rc.conname, cardinality(rc.conkey))
  FROM pg_constraint rc
  JOIN pg_class cl ON cl.oid = rc.conrelid
  JOIN pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = current_schema()
  WHERE rc.contype = 'f' AND cl.relname = $1 AND cardinality(rc.conkey) > 1
  UNION ALL
  SELECT CASE
           WHEN am.amname <> 'btree'
             THEN format('index %I uses unsupported method %s', ic.relname, am.amname)
           WHEN i.indpred IS NOT NULL
             THEN format('partial index %I (with a WHERE predicate)', ic.relname)
           ELSE format('expression index %I', ic.relname)
         END
  FROM pg_index i
  JOIN pg_class ic ON ic.oid = i.indexrelid
  JOIN pg_class tc ON tc.oid = i.indrelid
  JOIN pg_namespace n ON n.oid = tc.relnamespace AND n.nspname = current_schema()
  JOIN pg_am am ON am.oid = ic.relam
  WHERE tc.relname = $1
    AND (am.amname <> 'btree' OR i.indpred IS NOT NULL OR 0 = ANY(i.indkey))
) u
ORDER BY reason
`

// normalizePGType maps information_schema data_type strings onto the compact
// type vocabulary used by schema definitions ("timestamptz", "double", ...).
func normalizePGType(dataType string) string {
	switch dataType {
	case "character varying":
		return "varchar"
	case "timestamp without time zone":
		return "timestamp"
	case "timestamp with time zone":
		return "timestamptz"
	case "double precision":
		return "double"
	default:
		return dataType
	}
}

func onDeleteFromChar(code string) string {
	switch code {
	case "c":
		return "cascade"
	case "r":
		return "restrict"
	case "n":
		return "set null"
	default:
		return ""
	}
}

func (c *Client) IntrospectSchema(ctx context.Context) (Schema, error) {
	var schema Schema
	schema.Version = 1

	tableRows, err := c.pool.Query(ctx, introspectTablesSQL)
	if err != nil {
		return schema, fmt.Errorf("introspect tables: %w", err)
	}
	defer tableRows.Close()

	var tableNames []string
	extensionOwned := make(map[string]bool)
	extensionOwner := make(map[string]string)
	for tableRows.Next() {
		var name, owner string
		var ext bool
		if err := tableRows.Scan(&name, &ext, &owner); err != nil {
			return schema, fmt.Errorf("scan table name: %w", err)
		}
		tableNames = append(tableNames, name)
		extensionOwned[name] = ext
		extensionOwner[name] = owner
	}
	if err := tableRows.Err(); err != nil {
		return schema, err
	}

	for _, name := range tableNames {
		td, err := c.introspectTable(ctx, name)
		if err != nil {
			return schema, err
		}
		td.ExtensionOwned = extensionOwned[name]
		td.ExtensionOwner = extensionOwner[name]
		schema.Tables = append(schema.Tables, td)
	}
	return schema, nil
}

func (c *Client) introspectTable(ctx context.Context, name string) (TableDef, error) {
	td := TableDef{Name: name}

	colRows, err := c.pool.Query(ctx, introspectColumnsSQL, name)
	if err != nil {
		return td, fmt.Errorf("introspect columns of %s: %w", name, err)
	}
	defer colRows.Close()

	for colRows.Next() {
		var colName, dataType string
		var varcharLen *int
		var nullable bool
		var columnDefault *string
		var isPK bool
		if err := colRows.Scan(&colName, &dataType, &varcharLen, &nullable, &columnDefault, &isPK); err != nil {
			return td, fmt.Errorf("scan column of %s: %w", name, err)
		}
		cd := ColumnDef{
			Name:       colName,
			Type:       normalizePGType(dataType),
			NotNull:    !nullable,
			PrimaryKey: isPK,
		}
		if varcharLen != nil {
			cd.VarcharLength = *varcharLen
		}
		if columnDefault != nil {
			def := *columnDefault
			if strings.HasPrefix(def, "nextval(") && (cd.Type == "integer" || cd.Type == "bigint") {
				// serial columns surface as integer + nextval default
				cd.Type = "serial"
				cd.HasDefault = false
			} else if def == "now()" {
				cd.HasDefault = true
				cd.DefaultNow = true
			} else {
				cd.HasDefault = true
				trimmed := def
				if strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'::") {
					trimmed = strings.TrimSuffix(strings.TrimPrefix(trimmed, "'"), "'::")
				} else if strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'") {
					trimmed = strings.Trim(trimmed, "'")
				}
				cd.Default = &trimmed
			}
		}
		td.Columns = append(td.Columns, cd)
	}
	if err := colRows.Err(); err != nil {
		return td, err
	}

	idxRows, err := c.pool.Query(ctx, introspectIndexesSQL, name)
	if err != nil {
		return td, fmt.Errorf("introspect indexes of %s: %w", name, err)
	}
	defer idxRows.Close()
	for idxRows.Next() {
		var idxName, cols string
		var unique bool
		if err := idxRows.Scan(&idxName, &unique, &cols); err != nil {
			return td, fmt.Errorf("scan index of %s: %w", name, err)
		}
		td.Indexes = append(td.Indexes, IndexDef{Name: idxName, Unique: unique, Columns: strings.Split(cols, ",")})
	}
	if err := idxRows.Err(); err != nil {
		return td, err
	}

	uniqueRows, err := c.pool.Query(ctx, introspectUniquesSQL, name)
	if err != nil {
		return td, fmt.Errorf("introspect unique constraints of %s: %w", name, err)
	}
	defer uniqueRows.Close()
	for uniqueRows.Next() {
		var conName, colName string
		if err := uniqueRows.Scan(&conName, &colName); err != nil {
			return td, fmt.Errorf("scan unique constraint of %s: %w", name, err)
		}
		if col := td.Column(colName); col != nil {
			col.Unique = true
			col.UniqueName = conName
		}
	}
	if err := uniqueRows.Err(); err != nil {
		return td, err
	}

	fkRows, err := c.pool.Query(ctx, introspectFKsSQL, name)
	if err != nil {
		return td, fmt.Errorf("introspect foreign keys of %s: %w", name, err)
	}
	defer fkRows.Close()
	for fkRows.Next() {
		var conCol, refTable, refCol, delCode string
		if err := fkRows.Scan(&conCol, &refTable, &refCol, &delCode); err != nil {
			return td, fmt.Errorf("scan foreign key of %s: %w", name, err)
		}
		col := td.Column(conCol)
		if col == nil {
			continue
		}
		col.ForeignKey = &ForeignKeyDef{Table: refTable, Column: refCol, OnDelete: onDeleteFromChar(delCode)}
	}
	if err := fkRows.Err(); err != nil {
		return td, err
	}

	unsupRows, err := c.pool.Query(ctx, introspectUnsupportedSQL, name)
	if err != nil {
		return td, fmt.Errorf("introspect unsupported structures of %s: %w", name, err)
	}
	defer unsupRows.Close()
	for unsupRows.Next() {
		var reason string
		if err := unsupRows.Scan(&reason); err != nil {
			return td, fmt.Errorf("scan unsupported structure of %s: %w", name, err)
		}
		td.UnsupportedCatalog = append(td.UnsupportedCatalog, reason)
	}
	if err := unsupRows.Err(); err != nil {
		return td, err
	}

	return td, nil
}
