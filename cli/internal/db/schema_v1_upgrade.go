package db

// v1 upgrade reader — converts a validated version 1 schema document (the
// @neutron-build/sql exportSchema shape) into a schema document v2.
//
// The reader upgrades only what the v1 shape determines unambiguously and
// reports every ambiguity as an explicit error naming the field; it never
// guesses whether a default string was a literal or an SQL expression
// (program README section 4.1). Derived names (primary-key, unique and
// foreign-key constraint names, sequence names) are exactly the names the
// version 1 DDL produces in PostgreSQL, so the upgrade is faithful rather
// than inventive. The derivation rules are normative in
// contracts/data/CANONICAL.md section 6.

import (
	"fmt"
)

// v1TypeNames maps the v1 compact type vocabulary onto the v2 pg_catalog
// type names.
var v1TypeNames = map[string]string{
	"serial": "int4", "integer": "int4", "smallint": "int2", "bigint": "int8",
	"double": "float8", "real": "float4", "numeric": "numeric",
	"text": "text", "varchar": "varchar", "boolean": "bool",
	"timestamp": "timestamp", "timestamptz": "timestamptz", "date": "date",
	"json": "json", "jsonb": "jsonb", "uuid": "uuid", "bytea": "bytea",
	"vector": "vector",
}

// UpgradeV1Schema upgrades a validated v1 Schema to a v2 document tree. The
// tree is the same generic shape ParseV2Document produces; marshal it with
// encoding/json and re-parse to obtain a canonicalized V2Document, or use it
// directly with v2CanonicalBytes. The input must already have passed
// ValidateSchema; this reader performs no v1 validation of its own.
func UpgradeV1Schema(s *Schema) (map[string]any, error) {
	hasVector := false
	tables := make([]any, 0, len(s.Tables))
	for i := range s.Tables {
		upgraded, err := upgradeV1Table(&s.Tables[i])
		if err != nil {
			return nil, err
		}
		tables = append(tables, upgraded)
		if !hasVector && v1HasVectorColumn(&s.Tables[i]) {
			hasVector = true
		}
	}

	capabilities := []any{}
	if hasVector {
		capabilities = []any{"nucleus"}
	}

	return map[string]any{
		"version":      float64(SchemaDocumentVersionV2),
		"dialect":      "postgresql",
		"capabilities": capabilities,
		"schemas":      []any{map[string]any{"name": "public"}},
		"tables":       tables,
		"enums":        []any{},
		"views":        []any{},
		"opaque":       []any{},
	}, nil
}

func v1HasVectorColumn(t *TableDef) bool {
	for _, c := range t.Columns {
		if c.Type == "vector" {
			return true
		}
	}
	return false
}

func upgradeV1Table(t *TableDef) (map[string]any, error) {
	tableKey := v2TableKey("public", t.Name)
	columns := make([]any, 0, len(t.Columns))
	constraints := []any{}
	indexes := []any{}
	var pkCols []string

	for _, c := range t.Columns {
		// hasDefault without a spelled value: the default exists but v1
		// cannot represent it. Dropping it silently would plan an
		// ALTER COLUMN ... DROP DEFAULT against a column that has one, so
		// the reader reports the ambiguity instead (program README 4.1:
		// upgrade only representable data).
		if c.HasDefault && c.Default == nil && !c.DefaultNow {
			return nil, contractErr("ambiguous-default", fmt.Sprintf("tables[%s].columns[%s].default", tableKey, c.Name),
				"column sets hasDefault: true but spells no default value; the default exists and is not representable in v1 — re-export as schema document v2")
		}

		typeName := v1TypeNames[c.Type]
		colType := map[string]any{"name": typeName, "codec": v2TypeCodecs[typeName]}
		switch {
		case c.Type == "vector":
			colType["params"] = map[string]any{"dimensions": float64(c.VectorDims)}
		case c.Type == "varchar" && c.VarcharLength > 0:
			colType["params"] = map[string]any{"length": float64(c.VarcharLength)}
		}

		col := map[string]any{
			"name":    c.Name,
			"type":    colType,
			"notNull": c.NotNull || c.PrimaryKey, // PK implies NOT NULL (v1 normalization rule)
		}

		switch {
		case c.Type == "serial":
			// The v1 DDL `serial` creates exactly this named sequence.
			if c.Default != nil {
				return nil, contractErr("ambiguous-default", fmt.Sprintf("tables[%s].columns[%s].default", tableKey, c.Name),
					"serial column carries an explicit default %q; v1 cannot tell whether it supplements or replaces the implicit sequence default — re-export as schema document v2", *c.Default)
			}
			if c.DefaultNow {
				return nil, contractErr("ambiguous-default", fmt.Sprintf("tables[%s].columns[%s].default", tableKey, c.Name),
					"serial column also sets defaultNow; v1 cannot tell which default applies — re-export as schema document v2")
			}
			col["default"] = map[string]any{
				"kind":     "sequence",
				"sequence": map[string]any{"schema": "public", "name": fmt.Sprintf("%s_%s_seq", t.Name, c.Name)},
			}
		case c.DefaultNow:
			col["default"] = map[string]any{"kind": "expression", "sql": "now()"}
		case c.Default != nil:
			tagged, err := upgradeV1Default(&c, tableKey)
			if err != nil {
				return nil, err
			}
			col["default"] = tagged
		}
		columns = append(columns, col)

		if c.PrimaryKey {
			pkCols = append(pkCols, c.Name) // declaration order is the v1 PK column order
		}
		if c.Unique {
			// v1 introspection may carry uniqueName; the TS exporter never
			// emitted one, and the v1 inline `unique` DDL produces
			// PostgreSQL's default <table>_<column>_key.
			name := c.UniqueName
			if name == "" {
				name = fmt.Sprintf("%s_%s_key", t.Name, c.Name)
			}
			constraints = append(constraints, map[string]any{
				"name":    name,
				"type":    "unique",
				"columns": []any{c.Name},
			})
		}
		if c.ForeignKey != nil {
			ref := map[string]any{
				"table":   map[string]any{"schema": "public", "name": c.ForeignKey.Table},
				"columns": []any{c.ForeignKey.Column},
			}
			if c.ForeignKey.OnDelete != "" {
				ref["onDelete"] = c.ForeignKey.OnDelete
			}
			constraints = append(constraints, map[string]any{
				"name":       fmt.Sprintf("%s_%s_fkey", t.Name, c.Name), // PostgreSQL default name for inline REFERENCES
				"type":       "foreign-key",
				"columns":    []any{c.Name},
				"references": ref,
			})
		}
	}

	if len(pkCols) > 0 {
		constraints = append(constraints, map[string]any{
			"name":    fmt.Sprintf("%s_pkey", t.Name), // PostgreSQL default name for inline PRIMARY KEY
			"type":    "primary-key",
			"columns": toAnySlice(pkCols),
		})
	}

	for _, idx := range t.Indexes {
		key := make([]any, 0, len(idx.Columns))
		for _, col := range idx.Columns {
			key = append(key, map[string]any{"column": col})
		}
		indexes = append(indexes, map[string]any{
			"identity": map[string]any{"schema": "public", "name": idx.Name},
			"unique":   idx.Unique,
			"method":   "btree", // v1 could only express plain btree column indexes
			"key":      key,
		})
	}

	return map[string]any{
		"identity":    map[string]any{"schema": "public", "name": t.Name},
		"managed":     true,
		"columns":     columns,
		"constraints": constraints,
		"indexes":     indexes,
	}, nil
}

// upgradeV1Default tags a v1 default string. Only the two shapes the v1
// validator itself could verify are upgradable: a numeric literal on a
// numeric column and true/false on a boolean column. Everything else is
// ambiguous — the v1 export cannot distinguish the literal string "now()"
// from the expression now() — and reported, never guessed.
func upgradeV1Default(c *ColumnDef, tableKey string) (map[string]any, error) {
	d := *c.Default
	switch c.Type {
	case "integer", "smallint", "bigint", "double", "real", "numeric":
		if numericDefaultPattern.MatchString(d) {
			return map[string]any{"kind": "literal", "sql": d}, nil
		}
	case "boolean":
		if d == "true" || d == "false" {
			return map[string]any{"kind": "literal", "sql": d}, nil
		}
	}
	return nil, contractErr("ambiguous-default", fmt.Sprintf("tables[%s].columns[%s].default", tableKey, c.Name),
		"v1 default %q on column %s.%s could be a string literal or an SQL expression; the v1 format cannot distinguish them — re-export the schema as document v2 where defaults are tagged",
		d, tableKey, c.Name)
}

func toAnySlice(in []string) []any {
	out := make([]any, 0, len(in))
	for _, s := range in {
		out = append(out, s)
	}
	return out
}
