package db

import (
	"fmt"
	"sort"
	"strings"
)

// DiffResult carries forward (up) and reverse (down) SQL plus human-facing
// warnings for anything destructive or lossy.
type DiffResult struct {
	Up       []string
	Down     []string
	Warnings []string
}

func (d *DiffResult) warn(format string, args ...any) {
	d.Warnings = append(d.Warnings, fmt.Sprintf(format, args...))
}

// DiffOptions parameterizes DiffSchema. AllowDestructive is the explicit
// acknowledgement ("I accept data loss for objects I own") required before any
// DROP of an existing table, column, or index is planned. Neutron-internal
// tables and extension-owned objects are protected regardless of this flag.
type DiffOptions struct {
	Renames          map[string]string
	AllowDestructive bool
}

// pgType renders the PostgreSQL DDL type for a ColumnDef.
func pgType(c ColumnDef) string {
	switch c.Type {
	case "double":
		return "double precision"
	case "varchar":
		if c.VarcharLength > 0 {
			return fmt.Sprintf("varchar(%d)", c.VarcharLength)
		}
		return "varchar"
	default:
		return c.Type
	}
}

func defaultLiteral(c ColumnDef) string {
	if !c.HasDefault {
		return ""
	}
	if c.DefaultNow {
		return "now()"
	}
	if c.Default == nil {
		return "null"
	}
	switch c.Type {
	case "integer", "smallint", "bigint", "serial", "double", "real", "numeric", "boolean":
		return *c.Default
	default:
		return "'" + strings.ReplaceAll(*c.Default, "'", "''") + "'"
	}
}

func columnDDL(c ColumnDef, includeFK bool) string {
	parts := []string{quoteIdent(c.Name), pgType(c)}
	if c.PrimaryKey {
		parts = append(parts, "primary key")
	}
	if c.NotNull {
		parts = append(parts, "not null")
	}
	if c.Unique {
		parts = append(parts, "unique")
	}
	if c.HasDefault {
		parts = append(parts, "default "+defaultLiteral(c))
	}
	if includeFK && c.ForeignKey != nil {
		ref := fmt.Sprintf("references %s (%s)", quoteIdent(c.ForeignKey.Table), quoteIdent(c.ForeignKey.Column))
		if c.ForeignKey.OnDelete != "" {
			ref += " on delete " + c.ForeignKey.OnDelete
		}
		parts = append(parts, ref)
	}
	return strings.Join(parts, " ")
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func pgColumns(t TableDef) []ColumnDef {
	out := make([]ColumnDef, 0, len(t.Columns))
	for _, c := range t.Columns {
		if !c.NucleusOnly {
			out = append(out, c)
		}
	}
	return out
}

func createTable(t TableDef) string {
	lines := make([]string, 0, len(t.Columns))
	for _, c := range pgColumns(t) {
		lines = append(lines, "  "+columnDDL(c, true))
	}
	return fmt.Sprintf("create table %s (\n%s\n)", quoteIdent(t.Name), strings.Join(lines, ",\n"))
}

func indexDDL(t TableDef, idx IndexDef) string {
	cols := make([]string, 0, len(idx.Columns))
	for _, c := range idx.Columns {
		cols = append(cols, quoteIdent(c))
	}
	unique := ""
	if idx.Unique {
		unique = "unique "
	}
	return fmt.Sprintf("create %sindex %s on %s (%s)", unique, quoteIdent(idx.Name), quoteIdent(t.Name), strings.Join(cols, ", "))
}

// DiffSchema produces up/down SQL that moves `actual` to `desired`.
// opts.Renames maps "table.oldcol" -> "table.newcol" for explicit rename
// decisions. The returned error rejects invalid input, forged renames, shared
// tables whose catalog structure the diff engine cannot represent faithfully,
// and any other situation where emitting SQL would falsely claim
// synchronization. Destructive operations (drops of existing objects) are
// only planned when opts.AllowDestructive is set; without it they are skipped
// with a warning. Neutron-internal tables and extension-owned objects are
// never dropped or modified, whatever the desired schema says — declaring an
// extension-owned table in the desired schema is rejected outright.
func DiffSchema(desired, actual Schema, opts DiffOptions) (DiffResult, error) {
	var result DiffResult

	if err := ValidateSchema(&desired); err != nil {
		return result, fmt.Errorf("invalid desired schema: %w", err)
	}
	if err := validateRenames(desired, actual, opts.Renames); err != nil {
		return result, err
	}

	desiredNames := map[string]bool{}
	for _, t := range desired.Tables {
		desiredNames[t.Name] = true
	}
	actualNames := map[string]bool{}
	for _, t := range actual.Tables {
		actualNames[t.Name] = true
	}

	// Shared tables: reject catalog structures and changes the diff engine
	// cannot represent faithfully instead of claiming synchronization.
	if err := checkSharedTables(desired, actual); err != nil {
		return result, err
	}

	// Tables to create, topologically ordered so referenced tables exist
	// first; cyclic FK edges are deferred to ALTER TABLE after creation.
	var toCreate []string
	for _, t := range desired.Tables {
		if !actualNames[t.Name] {
			toCreate = append(toCreate, t.Name)
		}
	}
	ordered, deferred := topoOrderCreates(desired, toCreate)
	for _, name := range ordered {
		t := desired.Table(name)
		deferFKs := deferred[name]
		result.Up = append(result.Up, createTableWithDeferredFKs(*t, deferFKs))
	}
	for _, name := range ordered {
		t := desired.Table(name)
		for _, idx := range t.Indexes {
			result.Up = append(result.Up, indexDDL(*t, idx))
		}
		for _, colName := range deferred[name] {
			result.Up = append(result.Up, addForeignKeySQL(t.Name, *t.Column(colName)))
		}
		result.Down = append(result.Down, fmt.Sprintf("drop table if exists %s", quoteIdent(name)))
	}

	// Tables to drop: never for neutron-internal or extension-owned tables;
	// only with explicit destructive acknowledgement for anything else.
	var toDrop []string
	for _, t := range actual.Tables {
		if desiredNames[t.Name] {
			continue
		}
		switch {
		case isProtectedTableName(t.Name):
			result.warn("table %q is neutron-internal metadata: always left untouched", t.Name)
		case t.ExtensionOwned:
			owner := t.ExtensionOwner
			if owner == "" {
				result.warn("table %q is owned by a database extension: left untouched", t.Name)
			} else {
				result.warn("table %q is owned by a database extension (%s): left untouched", t.Name, owner)
			}
		case !opts.AllowDestructive:
			result.warn("table %q exists in the database but not in the schema: left untouched (dropping requires explicit destructive acknowledgement, --allow-destructive)", t.Name)
		default:
			toDrop = append(toDrop, t.Name)
		}
	}
	orderedDrops, err := orderDropsByFK(actual, toDrop)
	if err != nil {
		return result, err
	}
	for _, name := range orderedDrops {
		result.warn("table %q exists in the database but not in the schema: it will be dropped (all rows lost)", name)
		result.Up = append(result.Up, fmt.Sprintf("drop table if exists %s", quoteIdent(name)))
		t := actual.Table(name)
		if len(t.UnsupportedCatalog) > 0 || tableHasUnsupportedColumnType(t) {
			result.warn("table %q carries unsupported structures or column types: the drop is irreversible (no down SQL can re-create it faithfully)", name)
			result.Down = append(result.Down, fmt.Sprintf(
				"-- IRREVERSIBLE: table %q carries unsupported structures or column types; no down statement can re-create it",
				name))
		} else {
			result.Down = append(result.Down, createTable(*t))
		}
	}

	// Column-level diffs on shared tables.
	for _, dt := range desired.Tables {
		at := actual.Table(dt.Name)
		if at == nil {
			continue
		}
		diffColumns(&result, dt, *at, opts, opts.Renames)
		diffIndexes(&result, dt, *at, opts)
	}

	return result, nil
}

// validateRenames checks explicit rename intent against both sides: the
// source column must exist in the introspected table and the target column in
// the desired table. Forged or stale rename flags fail loudly instead of
// generating no-op or wrong-direction SQL.
func validateRenames(desired, actual Schema, renames map[string]string) error {
	if len(renames) == 0 {
		return nil
	}
	seenSources := make(map[string]string) // source "table.old" -> target
	for to, from := range renames {
		table := strings.SplitN(to, ".", 2)[0]
		dt := desired.Table(table)
		if dt == nil {
			return fmt.Errorf("--rename %s.%s>%s: table %q is not in the desired schema", table, from, to, table)
		}
		if dt.Column(strings.SplitN(to, ".", 2)[1]) == nil {
			return fmt.Errorf("--rename %s.%s>%s: target column %q does not exist in the desired schema", table, from, to, to)
		}
		at := actual.Table(table)
		if at == nil {
			return fmt.Errorf("--rename %s.%s>%s: table %q does not exist in the database", table, from, to, table)
		}
		if at.Column(from) == nil {
			return fmt.Errorf("--rename %s.%s>%s: source column %q does not exist in table %q in the database", table, from, to, from, table)
		}
		sourceKey := table + "." + from
		if prev, dup := seenSources[sourceKey]; dup {
			return fmt.Errorf("--rename: column %q is renamed to both %q and %q; a column has one rename", sourceKey, prev, to)
		}
		seenSources[sourceKey] = to
	}
	return nil
}

// checkSharedTables rejects desired/actual table pairs whose live catalog
// structure the current diff engine cannot faithfully represent or whose
// declared changes it cannot plan. Silently ignoring any of these would let
// `db push` report success while the database diverges from the schema.
func checkSharedTables(desired, actual Schema) error {
	// supportedIntrospectedTypes = desired vocabulary + "serial" (integer
	// with a nextval default introspects as serial).
	for _, dt := range desired.Tables {
		at := actual.Table(dt.Name)
		if at == nil {
			continue
		}
		if at.ExtensionOwned {
			owner := at.ExtensionOwner
			if owner == "" {
				owner = "unknown extension"
			}
			return fmt.Errorf(
				"table %q exists in the database as an extension-owned table (owning extension: %s) — "+
					"extension-owned objects are never managed or modified; remove the table from the schema JSON",
				dt.Name, owner)
		}
		if len(at.UnsupportedCatalog) > 0 {
			return fmt.Errorf(
				"table %q carries catalog structure this diff engine cannot represent faithfully: %s — refusing to claim synchronization until supported",
				dt.Name, strings.Join(at.UnsupportedCatalog, "; "))
		}
		for _, dc := range dt.Columns {
			ac := at.Column(dc.Name)
			if ac == nil {
				continue
			}
			if !supportedColumnTypes[ac.Type] {
				return fmt.Errorf(
					"column %s.%s has unsupported type %q in the database — this tool cannot plan changes around it; manage it manually",
					dt.Name, dc.Name, ac.Type)
			}
			if dc.PrimaryKey != ac.PrimaryKey {
				return fmt.Errorf(
					"column %s.%s: primary key changes on existing tables are not supported yet — desired says primaryKey=%v, database says %v",
					dt.Name, dc.Name, dc.PrimaryKey, ac.PrimaryKey)
			}
			if err := checkForeignKeyMatch(dt.Name, dc, *ac); err != nil {
				return err
			}
		}
		for _, idx := range dt.Indexes {
			ai := at.Index(idx.Name)
			if ai == nil {
				continue
			}
			if ai.Unique != idx.Unique || !sameColumns(ai.Columns, idx.Columns) {
				return fmt.Errorf(
					"index %q on table %q exists with a different definition (database: unique=%v columns=%v; schema: unique=%v columns=%v) — same-name index changes are not supported yet",
					idx.Name, dt.Name, ai.Unique, ai.Columns, idx.Unique, idx.Columns)
			}
		}
	}
	return nil
}

// checkForeignKeyMatch rejects FK drift on existing columns: the desired
// schema and the database must agree, because FK additions/removals on
// existing tables are not planned by this engine.
func checkForeignKeyMatch(table string, desired, actual ColumnDef) error {
	dHas, aHas := desired.ForeignKey != nil, actual.ForeignKey != nil
	if !dHas && !aHas {
		return nil
	}
	if !dHas {
		return fmt.Errorf(
			"column %s.%s has a foreign key in the database that the schema does not declare — foreign-key changes on existing tables are not supported yet",
			table, desired.Name)
	}
	if !aHas {
		return fmt.Errorf(
			"column %s.%s declares a foreign key that does not exist in the database — foreign-key changes on existing tables are not supported yet",
			table, desired.Name)
	}
	dOn := normalizeOnDelete(desired.ForeignKey.OnDelete)
	aOn := normalizeOnDelete(actual.ForeignKey.OnDelete)
	if desired.ForeignKey.Table != actual.ForeignKey.Table ||
		desired.ForeignKey.Column != actual.ForeignKey.Column ||
		dOn != aOn {
		return fmt.Errorf(
			"column %s.%s foreign key differs (database: %s(%s) on delete %q; schema: %s(%s) on delete %q) — foreign-key changes on existing tables are not supported yet",
			table, desired.Name,
			actual.ForeignKey.Table, actual.ForeignKey.Column, aOn,
			desired.ForeignKey.Table, desired.ForeignKey.Column, dOn)
	}
	return nil
}

func normalizeOnDelete(action string) string {
	if action == "no action" {
		return ""
	}
	return action
}

func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func diffColumns(result *DiffResult, desired, actual TableDef, opts DiffOptions, renames map[string]string) {
	desiredCols := map[string]bool{}
	for _, c := range desired.Columns {
		desiredCols[c.Name] = true
	}
	actualCols := map[string]bool{}
	for _, c := range actual.Columns {
		actualCols[c.Name] = true
	}

	// Added columns (check renames from actual columns missing in desired).
	for _, dc := range desired.Columns {
		if actualCols[dc.Name] {
			continue
		}
		key := desired.Name + "." + dc.Name
		if from, ok := renames[key]; ok {
			result.Up = append(result.Up, fmt.Sprintf("alter table %s rename column %s to %s", quoteIdent(desired.Name), quoteIdent(from), quoteIdent(dc.Name)))
			result.Down = append(result.Down, fmt.Sprintf("alter table %s rename column %s to %s", quoteIdent(desired.Name), quoteIdent(dc.Name), quoteIdent(from)))
			continue
		}
		// Suggest renames: added column with same type as a removed one.
		for _, ac := range actual.Columns {
			if desiredCols[ac.Name] {
				continue
			}
			if pgType(ac) == pgType(dc) {
				result.warn(
					"%s: column %q added and %q dropped with the same type %q — if this is a rename, rerun with --rename %s.%s>%s.%s",
					desired.Name, dc.Name, ac.Name, pgType(dc),
					desired.Name, ac.Name, desired.Name, dc.Name,
				)
			}
		}
		if dc.NotNull && !dc.HasDefault {
			result.warn("%s: adding not-null column %q without a default fails on tables with rows", desired.Name, dc.Name)
		}
		result.Up = append(result.Up, fmt.Sprintf("alter table %s add column %s", quoteIdent(desired.Name), columnDDL(dc, true)))
		result.Down = append(result.Down, fmt.Sprintf("alter table %s drop column if exists %s", quoteIdent(desired.Name), quoteIdent(dc.Name)))
	}

	// Dropped columns: destructive — only with explicit acknowledgement.
	for _, ac := range actual.Columns {
		if desiredCols[ac.Name] {
			continue
		}
		// Was this the source of an explicit rename? Then it is already handled.
		renamedAway := false
		for to, from := range renames {
			if from == ac.Name && strings.HasPrefix(to, desired.Name+".") {
				renamedAway = true
				break
			}
		}
		if renamedAway {
			continue
		}
		if !opts.AllowDestructive {
			result.warn("%s: column %q exists in the database but not in the schema: left untouched (dropping requires explicit destructive acknowledgement, --allow-destructive)", desired.Name, ac.Name)
			continue
		}
		result.warn("%s: column %q will be dropped (data lost unless it is a rename — see --rename)", desired.Name, ac.Name)
		result.Up = append(result.Up, fmt.Sprintf("alter table %s drop column if exists %s", quoteIdent(desired.Name), quoteIdent(ac.Name)))
		if supportedColumnTypes[ac.Type] {
			result.Down = append(result.Down, fmt.Sprintf("alter table %s add column %s", quoteIdent(desired.Name), columnDDL(ac, true)))
		} else {
			result.warn("%s: column %q has unsupported type %q — the drop is irreversible (no down SQL can re-create the type)",
				desired.Name, ac.Name, ac.Type)
			result.Down = append(result.Down, fmt.Sprintf(
				"-- IRREVERSIBLE: column %s.%q has unsupported type %q; no down statement can re-create it",
				desired.Name, ac.Name, ac.Type))
		}
	}

	// Modified columns.
	for _, dc := range desired.Columns {
		acPtr := actual.Column(dc.Name)
		if acPtr == nil {
			continue
		}
		ac := *acPtr
		table := quoteIdent(desired.Name)
		col := quoteIdent(dc.Name)
		if pgType(dc) != pgType(ac) {
			result.warn("%s: column %q type changes %s -> %s via USING cast; verify values convert", desired.Name, dc.Name, pgType(ac), pgType(dc))
			cast := fmt.Sprintf("alter table %s alter column %s type %s using %s::%s", table, col, pgType(dc), col, pgType(dc))
			result.Up = append(result.Up, cast)
			result.Down = append(result.Down, fmt.Sprintf("alter table %s alter column %s type %s using %s::%s", table, col, pgType(ac), col, pgType(ac)))
		}
		// notNull drift between two PK columns is not real drift: Postgres
		// marks primary-key columns NOT NULL implicitly, and hand-written
		// JSON may omit the flag (validation normalizes the desired side;
		// synthetic introspection input may omit it on the actual side).
		if dc.NotNull != ac.NotNull && !(dc.PrimaryKey && ac.PrimaryKey) {
			if dc.NotNull {
				result.Up = append(result.Up, fmt.Sprintf("alter table %s alter column %s set not null", table, col))
				result.Down = append(result.Down, fmt.Sprintf("alter table %s alter column %s drop not null", table, col))
			} else {
				result.Up = append(result.Up, fmt.Sprintf("alter table %s alter column %s drop not null", table, col))
				result.Down = append(result.Down, fmt.Sprintf("alter table %s alter column %s set not null", table, col))
			}
		}
		if dc.Unique != ac.Unique {
			if dc.Unique {
				conName := desired.Name + "_" + dc.Name + "_key"
				result.Up = append(result.Up, fmt.Sprintf("alter table %s add constraint %s unique (%s)", table, quoteIdent(conName), col))
				result.Down = append(result.Down, fmt.Sprintf("alter table %s drop constraint if exists %s", table, quoteIdent(conName)))
			} else {
				conName := ac.UniqueName
				if conName == "" {
					conName = desired.Name + "_" + dc.Name + "_key"
				}
				result.Up = append(result.Up, fmt.Sprintf("alter table %s drop constraint if exists %s", table, quoteIdent(conName)))
				result.Down = append(result.Down, fmt.Sprintf("alter table %s add constraint %s unique (%s)", table, quoteIdent(conName), col))
			}
		}
		if defaultLiteral(dc) != defaultLiteral(ac) {
			if dc.HasDefault {
				result.Up = append(result.Up, fmt.Sprintf("alter table %s alter column %s set default %s", table, col, defaultLiteral(dc)))
			} else {
				result.Up = append(result.Up, fmt.Sprintf("alter table %s alter column %s drop default", table, col))
			}
			if ac.HasDefault {
				result.Down = append(result.Down, fmt.Sprintf("alter table %s alter column %s set default %s", table, col, defaultLiteral(ac)))
			} else {
				result.Down = append(result.Down, fmt.Sprintf("alter table %s alter column %s drop default", table, col))
			}
		}
	}
}

func diffIndexes(result *DiffResult, desired, actual TableDef, opts DiffOptions) {
	desiredIdx := map[string]bool{}
	for _, idx := range desired.Indexes {
		desiredIdx[idx.Name] = true
	}
	actualIdx := map[string]bool{}
	for _, idx := range actual.Indexes {
		actualIdx[idx.Name] = true
	}
	for _, idx := range desired.Indexes {
		if actualIdx[idx.Name] {
			continue
		}
		result.Up = append(result.Up, indexDDL(desired, idx))
		result.Down = append(result.Down, fmt.Sprintf("drop index if exists %s", quoteIdent(idx.Name)))
	}
	// Dropped indexes: destructive — only with explicit acknowledgement.
	for _, idx := range actual.Indexes {
		if desiredIdx[idx.Name] {
			continue
		}
		if !opts.AllowDestructive {
			result.warn("index %q on table %q exists in the database but not in the schema: left untouched (dropping requires explicit destructive acknowledgement, --allow-destructive)", idx.Name, desired.Name)
			continue
		}
		result.Up = append(result.Up, fmt.Sprintf("drop index if exists %s", quoteIdent(idx.Name)))
		result.Down = append(result.Down, indexDDL(actual, idx))
	}
}

// orderDropsByFK orders destructive table drops so referencing tables drop
// before the tables they reference (single-column FK edges from the
// introspected schema), instead of alphabetically — a referenced table
// dropping first makes PostgreSQL reject the whole plan. Foreign-key cycles
// among the dropped tables are an explicit error, never an infinite loop or
// a partial ordering.
func orderDropsByFK(actual Schema, toDrop []string) ([]string, error) {
	inSet := make(map[string]bool, len(toDrop))
	for _, n := range toDrop {
		inSet[n] = true
	}
	sorted := append([]string(nil), toDrop...)
	sort.Strings(sorted)

	const (
		unvisited = iota
		visiting
		done
	)
	state := make(map[string]int, len(toDrop))
	var stack []string
	var order []string // referenced tables first (create order)
	var visit func(name string) error
	visit = func(name string) error {
		switch state[name] {
		case done:
			return nil
		case visiting:
			return fmt.Errorf(
				"cannot order destructive table drops: foreign-key cycle among dropped tables (%s) — drop these tables manually, e.g. with DROP ... CASCADE",
				strings.Join(append(stack, name), " -> "))
		}
		state[name] = visiting
		stack = append(stack, name)
		if t := actual.Table(name); t != nil {
			for _, c := range t.Columns {
				if c.ForeignKey != nil && inSet[c.ForeignKey.Table] {
					if err := visit(c.ForeignKey.Table); err != nil {
						return err
					}
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[name] = done
		order = append(order, name)
		return nil
	}
	for _, n := range sorted {
		if err := visit(n); err != nil {
			return nil, err
		}
	}

	// Reverse create order => referencing tables drop first.
	out := make([]string, len(order))
	for i, n := range order {
		out[len(order)-1-i] = n
	}
	return out, nil
}

// tableHasUnsupportedColumnType reports whether any column of t has a type
// outside the vocabulary this engine can emit faithfully.
func tableHasUnsupportedColumnType(t *TableDef) bool {
	for _, c := range t.Columns {
		if !supportedColumnTypes[c.Type] {
			return true
		}
	}
	return false
}

// topoOrderCreates orders table names so FK targets precede referrers.
// Returns (ordered names, per-table deferred column names for cyclic edges).
func topoOrderCreates(desired Schema, names []string) ([]string, map[string][]string) {
	inSet := make(map[string]bool, len(names))
	for _, n := range names {
		inSet[n] = true
	}
	deferred := make(map[string][]string)
	visited := make(map[string]bool)
	visiting := make(map[string]bool)
	var order []string

	var visit func(name string)
	visit = func(name string) {
		if visited[name] || !inSet[name] {
			return
		}
		if visiting[name] {
			return // cycle handled by the caller marking deferrals
		}
		visiting[name] = true
		t := desired.Table(name)
		if t != nil {
			for _, c := range t.Columns {
				if c.ForeignKey == nil || !inSet[c.ForeignKey.Table] {
					continue
				}
				if visiting[c.ForeignKey.Table] {
					deferred[name] = append(deferred[name], c.Name)
					continue
				}
				visit(c.ForeignKey.Table)
			}
		}
		visiting[name] = false
		visited[name] = true
		order = append(order, name)
	}

	for _, n := range names {
		visit(n)
	}
	return order, deferred
}

// createTableWithDeferredFKs renders CREATE TABLE omitting the named FK
// columns' references clauses (they are added via ALTER afterwards).
func createTableWithDeferredFKs(t TableDef, deferredColumns []string) string {
	if len(deferredColumns) == 0 {
		return createTable(t)
	}
	skip := make(map[string]bool, len(deferredColumns))
	for _, c := range deferredColumns {
		skip[c] = true
	}
	lines := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		includeFK := !skip[c.Name]
		lines = append(lines, "  "+columnDDL(c, includeFK))
	}
	return fmt.Sprintf("create table %s (\n%s\n)", quoteIdent(t.Name), strings.Join(lines, ",\n"))
}

// addForeignKeySQL emits an ALTER for a column's FK constraint.
func addForeignKeySQL(tableName string, c ColumnDef) string {
	constraint := fmt.Sprintf("%s_%s_fkey", tableName, c.Name)
	ref := fmt.Sprintf("references %s (%s)", quoteIdent(c.ForeignKey.Table), quoteIdent(c.ForeignKey.Column))
	if c.ForeignKey.OnDelete != "" {
		ref += " on delete " + c.ForeignKey.OnDelete
	}
	return fmt.Sprintf("alter table %s add constraint %s foreign key (%s) %s",
		quoteIdent(tableName), quoteIdent(constraint), quoteIdent(c.Name), ref)
}
