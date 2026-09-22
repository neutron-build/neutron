package db

// Schema model shared by the JSON exporter (neutron-sql exportSchema) and the
// live-database introspection. The migrate generate / db push commands diff a
// desired Schema against an introspected Schema to emit SQL.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

type ForeignKeyDef struct {
	Table    string `json:"table"`
	Column   string `json:"column"`
	OnDelete string `json:"onDelete,omitempty"`
}

type ColumnDef struct {
	Name          string         `json:"name"`
	Type          string         `json:"type"`
	NotNull       bool           `json:"notNull"`
	PrimaryKey    bool           `json:"primaryKey"`
	Unique        bool           `json:"unique"`
	UniqueName    string         `json:"uniqueName,omitempty"`
	HasDefault    bool           `json:"hasDefault"`
	Default       *string        `json:"default,omitempty"`
	DefaultNow    bool           `json:"defaultNow,omitempty"`
	VarcharLength int            `json:"varcharLength,omitempty"`
	VectorDims    int            `json:"vectorDimensions,omitempty"`
	NucleusOnly   bool           `json:"nucleusOnly,omitempty"`
	ForeignKey    *ForeignKeyDef `json:"foreignKey,omitempty"`
}

type IndexDef struct {
	Name    string   `json:"name"`
	Unique  bool     `json:"unique"`
	Columns []string `json:"columns"`
}

type TableDef struct {
	Name    string      `json:"name"`
	Columns []ColumnDef `json:"columns"`
	Indexes []IndexDef  `json:"indexes"`

	// ExtensionOwned is set by introspection when the table is owned by a
	// database extension (pg_depend deptype 'e'). Such tables are never
	// dropped or modified by generated plans; declaring one in a desired
	// schema is rejected.
	ExtensionOwned bool `json:"-"`
	// ExtensionOwner names the owning extension when ExtensionOwned is set
	// (pg_extension.extname; empty when unknown).
	ExtensionOwner string `json:"-"`
	// UnsupportedCatalog names catalog structures introspection found on
	// this table that the current diff engine cannot represent faithfully
	// (composite constraints, non-btree/partial/expression indexes, ...).
	// Planning that touches these tables is rejected instead of claiming
	// synchronization.
	UnsupportedCatalog []string `json:"-"`
}

type Schema struct {
	Version int        `json:"version"`
	Tables  []TableDef `json:"tables"`
}

// UnmarshalJSON rejects an explicit `"tables": null`: hand-written input
// must be unambiguous (null must not silently read as an empty schema; use
// "tables": [] instead). A missing "tables" key remains a valid explicitly
// empty schema, matching the in-struct zero value.
func (s *Schema) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if v, present := raw["tables"]; present && string(bytes.TrimSpace(v)) == "null" {
		return fmt.Errorf(`"tables" must not be null — write "tables": [] for an explicitly empty schema`)
	}
	type schemaAlias Schema
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var aux schemaAlias
	if err := dec.Decode(&aux); err != nil {
		return err
	}
	*s = Schema(aux)
	return nil
}

func (s *Schema) Table(name string) *TableDef {
	for i := range s.Tables {
		if s.Tables[i].Name == name {
			return &s.Tables[i]
		}
	}
	return nil
}

func (t *TableDef) Column(name string) *ColumnDef {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i]
		}
	}
	return nil
}

func (t *TableDef) Index(name string) *IndexDef {
	for i := range t.Indexes {
		if t.Indexes[i].Name == name {
			return &t.Indexes[i]
		}
	}
	return nil
}

func (t *TableDef) PrimaryKeys() []string {
	var pks []string
	for _, c := range t.Columns {
		if c.PrimaryKey {
			pks = append(pks, c.Name)
		}
	}
	return pks
}

// SchemaVersion is the only schema JSON format version this CLI understands.
const SchemaVersion = 1

// isProtectedTableName reports whether a table belongs to neutron's own
// metadata namespace. Protected tables (_neutron_migrations,
// _neutron_migration_lock, future schema-owner/mutation-outcome metadata, ...)
// are never dropped or modified by generated plans, regardless of the desired
// schema or command flags.
func isProtectedTableName(name string) bool {
	return strings.HasPrefix(name, "_neutron_")
}

// supportedColumnTypes is the type vocabulary shared by the neutron-sql
// exporter and this CLI. Anything else in a desired schema is rejected rather
// than emitted as unverified DDL.
var supportedColumnTypes = map[string]bool{
	"serial": true, "integer": true, "smallint": true, "bigint": true,
	"double": true, "real": true, "numeric": true,
	"text": true, "varchar": true,
	"boolean": true,
	"timestamp": true, "timestamptz": true, "date": true,
	"json": true, "jsonb": true,
	"uuid": true, "bytea": true,
	"vector": true,
}

// supportedOnDelete lists the referential actions that may appear in desired
// schemas; anything else would be spliced verbatim into DDL.
var supportedOnDelete = map[string]bool{
	"": true, "cascade": true, "restrict": true, "no action": true,
	"set null": true, "set default": true,
}

// numericDefaultPattern matches the literal default values accepted for
// numeric/boolean columns (the raw string is emitted into DDL unquoted).
var numericDefaultPattern = regexp.MustCompile(`^-?\d+(\.\d+)?([eE][+-]?\d+)?$`)

// ValidateSchema checks a desired schema document for the version, duplicate
// identities, ambiguous constructs and unsupported types the migration
// commands refuse to plan around. A version-1 schema with zero tables is
// valid: an explicitly empty managed schema is distinct from malformed input.
// It validates the version 1 shape only; version detection across document
// versions (including the v2 contract) is ValidateSchemaDocument.
func ValidateSchema(s *Schema) error {
	if s.Version != SchemaVersion {
		return fmt.Errorf(
			"schema JSON declares version %d, but this CLI supports version %d only — "+
				"an empty object ({}) or a missing \"version\" field reads as 0; "+
				"regenerate the file with a matching @neutron-build/sql exportSchema",
			s.Version, SchemaVersion)
	}

	// PostgreSQL marks primary-key columns NOT NULL implicitly; the TS
	// exporter normalizes primaryKey => notNull at export time
	// (neutron-sql/src/export.ts). Apply the same normalization here so
	// hand-written JSON with primaryKey but no notNull never plans a doomed
	// `drop not null` on a PK column.
	for i := range s.Tables {
		for j := range s.Tables[i].Columns {
			if s.Tables[i].Columns[j].PrimaryKey {
				s.Tables[i].Columns[j].NotNull = true
			}
		}
	}

	tableNames := make(map[string]bool, len(s.Tables))
	indexOwners := make(map[string]string) // index name -> owning table (index names are schema-global in Postgres)
	type fkRef struct{ table, column string }
	var fkRefs []struct {
		col  ColumnDef
		ref  fkRef
		tbl  string
	}

	for _, t := range s.Tables {
		if strings.TrimSpace(t.Name) == "" {
			return fmt.Errorf("schema contains a table with an empty name")
		}
		if isProtectedTableName(t.Name) {
			return fmt.Errorf(
				"table %q is neutron-internal metadata and is managed automatically — "+
					"remove it from the schema JSON; internal tables are never part of a diff or plan",
				t.Name)
		}
		if tableNames[t.Name] {
			return fmt.Errorf("duplicate table name %q in schema", t.Name)
		}
		tableNames[t.Name] = true

		if len(t.Columns) == 0 {
			return fmt.Errorf(
				"table %q declares no columns — zero-column tables cannot be created by generated DDL; declare at least one column",
				t.Name)
		}

		columnNames := make(map[string]bool, len(t.Columns))
		for _, c := range t.Columns {
			if strings.TrimSpace(c.Name) == "" {
				return fmt.Errorf("table %q contains a column with an empty name", t.Name)
			}
			if columnNames[c.Name] {
				return fmt.Errorf("duplicate column name %q on table %q", c.Name, t.Name)
			}
			columnNames[c.Name] = true

			if !supportedColumnTypes[c.Type] {
				return fmt.Errorf(
					"column %s.%s has unsupported type %q — the supported set is: serial, integer, smallint, bigint, double, real, numeric, text, varchar, boolean, timestamp, timestamptz, date, json, jsonb, uuid, bytea, vector",
					t.Name, c.Name, c.Type)
			}
			if c.Type == "vector" {
				if !c.NucleusOnly {
					return fmt.Errorf(
						"column %s.%s: vector columns must be nucleusOnly on vanilla Postgres (until pgvector planning is supported) — mark the column nucleusOnly or remove it",
						t.Name, c.Name)
				}
				if c.VectorDims <= 0 {
					return fmt.Errorf("column %s.%s: vector type requires vectorDimensions > 0", t.Name, c.Name)
				}
			} else {
				if c.NucleusOnly {
					return fmt.Errorf("column %s.%s: nucleusOnly is only valid for vector columns", t.Name, c.Name)
				}
				if c.VectorDims != 0 {
					return fmt.Errorf("column %s.%s: vectorDimensions is only valid for vector columns", t.Name, c.Name)
				}
			}
			if c.VarcharLength < 0 {
				return fmt.Errorf("column %s.%s: varcharLength must not be negative", t.Name, c.Name)
			}
			if c.VarcharLength > 0 && c.Type != "varchar" {
				return fmt.Errorf("column %s.%s: varcharLength is only valid for varchar columns", t.Name, c.Name)
			}

			if !c.HasDefault && (c.Default != nil || c.DefaultNow) {
				return fmt.Errorf(
					"column %s.%s carries a default value but hasDefault is false — ambiguous input; set hasDefault or remove the default",
					t.Name, c.Name)
			}
			if c.DefaultNow && c.Default != nil {
				return fmt.Errorf(
					"column %s.%s sets both defaultNow and a literal default — ambiguous input; keep exactly one",
					t.Name, c.Name)
			}
			if c.DefaultNow && c.Type != "timestamp" && c.Type != "timestamptz" {
				return fmt.Errorf("column %s.%s: defaultNow is only valid for timestamp/timestamptz columns", t.Name, c.Name)
			}
			if c.HasDefault && c.Default != nil && !c.DefaultNow {
				switch c.Type {
				case "integer", "smallint", "bigint", "serial", "double", "real", "numeric":
					if !numericDefaultPattern.MatchString(*c.Default) {
						return fmt.Errorf(
							"column %s.%s: default %q is not a plain numeric literal — expression defaults are not supported in schema JSON v1",
							t.Name, c.Name, *c.Default)
					}
				case "boolean":
					if *c.Default != "true" && *c.Default != "false" {
						return fmt.Errorf(
							"column %s.%s: boolean default must be \"true\" or \"false\", got %q",
							t.Name, c.Name, *c.Default)
					}
				}
			}

			if c.ForeignKey != nil {
				if !supportedOnDelete[c.ForeignKey.OnDelete] {
					return fmt.Errorf(
						"column %s.%s: foreign key onDelete %q is not one of: cascade, restrict, no action, set null, set default",
						t.Name, c.Name, c.ForeignKey.OnDelete)
				}
				fkRefs = append(fkRefs, struct {
					col  ColumnDef
					ref  fkRef
					tbl  string
				}{col: c, ref: fkRef{table: c.ForeignKey.Table, column: c.ForeignKey.Column}, tbl: t.Name})
			}
		}

		for _, idx := range t.Indexes {
			if strings.TrimSpace(idx.Name) == "" {
				return fmt.Errorf("table %q contains an index with an empty name", t.Name)
			}
			if owner, dup := indexOwners[idx.Name]; dup {
				return fmt.Errorf("duplicate index name %q (used on tables %q and %q) — index names are schema-global in Postgres",
					idx.Name, owner, t.Name)
			}
			indexOwners[idx.Name] = t.Name
			if len(idx.Columns) == 0 {
				return fmt.Errorf("index %q on table %q lists no columns", idx.Name, t.Name)
			}
			for _, col := range idx.Columns {
				if !columnNames[col] {
					return fmt.Errorf("index %q on table %q references unknown column %q", idx.Name, t.Name, col)
				}
			}
		}
	}

	// Foreign-key targets must resolve inside the desired schema (typos would
	// otherwise surface as runtime SQL failures, or silently dangle).
	for _, r := range fkRefs {
		target := s.Table(r.ref.table)
		if target == nil {
			if isProtectedTableName(r.ref.table) {
				return fmt.Errorf("column %s.%s references neutron-internal table %q — internal tables cannot be foreign-key targets in a schema",
					r.tbl, r.col.Name, r.ref.table)
			}
			return fmt.Errorf("column %s.%s has a foreign key referencing table %q, which is not declared in the schema",
				r.tbl, r.col.Name, r.ref.table)
		}
		if target.Column(r.ref.column) == nil {
			return fmt.Errorf("column %s.%s references column %s.%s, which does not exist in the schema",
				r.tbl, r.col.Name, r.ref.table, r.ref.column)
		}
	}

	return nil
}
