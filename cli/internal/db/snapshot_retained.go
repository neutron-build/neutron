package db

// Target snapshots record what a plan leaves in place (M08).
//
// A snapshot plan without --allow-destructive leaves every object that is in
// the planning base but absent from the desired document where it is: the
// table, column, index, view or enum stays in the database. The target
// snapshot must record it, or the chain stops matching the database the
// moment the migration applies — the drift gate then refuses every later
// run, and a later --allow-destructive plan has nothing left to drop.
//
// SnapshotTarget derives what a plan left in place from the plan's own up
// statements: an object of the base that the desired document does not
// declare stays unless an up statement drops it (the statement the planner
// renders for that drop) or renames it. The desired document's schemas are
// the managed scope, exactly as for the planner; objects of other schemas
// are out of scope and not recorded. Constraints are not carried: the
// planner reconciles them in every mode, so a base constraint the desired
// document omits is always dropped.
//
// The same derivation reads chains written by earlier CLIs, whose target
// snapshots omitted those objects (LoadSnapshotChain): each migration
// snapshot is read as its recorded document plus what its up file left in
// place from the previous snapshot.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// RetainedObject is one object a plan leaves in place although the desired
// document does not declare it.
type RetainedObject struct {
	Kind     string     // table | column | index | view | enum
	Identity V2Identity // the object; for a column, its table
	Column   string     // column name (kind column)
	Table    V2Identity // the index's table (kind index)
}

func (r RetainedObject) String() string {
	switch r.Kind {
	case "column":
		return fmt.Sprintf("column %s.%s", r.Identity, r.Column)
	case "index":
		return fmt.Sprintf("index %s on table %s", r.Identity, r.Table)
	}
	return r.Kind + " " + r.Identity.String()
}

// RetainedList renders retained objects for messages.
func RetainedList(objs []RetainedObject) string {
	parts := make([]string, len(objs))
	for i, o := range objs {
		parts[i] = o.String()
	}
	return strings.Join(parts, ", ")
}

// SnapshotRetained lists what an earlier CLI's snapshot omitted although
// its migration left it in place; the chain reads it as recorded.
type SnapshotRetained struct {
	Stem    string
	Objects []RetainedObject
}

// SnapshotTarget returns the target snapshot of a plan from base to desired
// whose up statements are up: the desired document plus every object of
// base the plan leaves in place, carried verbatim from base, and the list
// of those objects (empty when the target is desired itself). A carried
// column keeps its position after its nearest preceding base column that
// the desired table keeps, which is where it stays in the database.
func SnapshotTarget(base, desired *V2Document, up []string) (*V2Document, []RetainedObject, error) {
	// Canonical order, so the result never depends on how a document was
	// spelled on input.
	var bm, dm V2DocumentModel
	if err := json.Unmarshal(base.Canonical, &bm); err != nil {
		return nil, nil, fmt.Errorf("decode planning base: %w", err)
	}
	if err := json.Unmarshal(desired.Canonical, &dm); err != nil {
		return nil, nil, fmt.Errorf("decode schema document: %w", err)
	}
	scope := map[string]bool{}
	for _, s := range dm.Schemas {
		scope[s.Name] = true
	}
	stmts := map[string]bool{}
	for _, s := range up {
		stmts[statementKey(s)] = true
	}
	dropped := func(format string, args ...any) bool {
		return stmts[statementKey(fmt.Sprintf(format, args...))]
	}
	renames := plannedRenames(up)
	declared := func(t *V2Table) bool { return t != nil && t.Managed }

	var retained []RetainedObject
	for _, bt := range bm.Tables {
		id := bt.Identity
		if !scope[id.Schema] || isProtectedTableName(id.Name) {
			continue
		}
		dt := dm.Table(id)
		if !declared(dt) {
			if !dropped("drop table if exists %s", qualifiedNameSQL(id)) {
				retained = append(retained, RetainedObject{Kind: "table", Identity: id})
			}
			continue
		}
		for _, bc := range bt.Columns {
			if dt.Column(bc.Name) != nil || renames[id][bc.Name] != "" {
				continue
			}
			if !dropped("alter table %s drop column if exists %s", qualifiedNameSQL(id), quoteIdent(bc.Name)) {
				retained = append(retained, RetainedObject{Kind: "column", Identity: id, Column: bc.Name})
			}
		}
		for _, bi := range bt.Indexes {
			if dt.Index(bi.Identity.Name) != nil {
				continue
			}
			if !dropped("drop index if exists %s", qualifiedNameSQL(bi.Identity)) {
				retained = append(retained, RetainedObject{Kind: "index", Identity: bi.Identity, Table: id})
			}
		}
	}
	for _, bv := range bm.Views {
		if !scope[bv.Identity.Schema] {
			continue
		}
		if dv := dm.View(bv.Identity); dv != nil && dv.Managed {
			continue
		}
		if !dropped("drop view if exists %s", qualifiedNameSQL(bv.Identity)) {
			retained = append(retained, RetainedObject{Kind: "view", Identity: bv.Identity})
		}
	}
	for _, be := range bm.Enums {
		if !scope[be.Identity.Schema] {
			continue
		}
		if de := dm.Enum(be.Identity); de != nil && de.Managed {
			continue
		}
		if !dropped("drop type if exists %s", qualifiedNameSQL(be.Identity)) {
			retained = append(retained, RetainedObject{Kind: "enum", Identity: be.Identity})
		}
	}
	if len(retained) == 0 {
		return desired, nil, nil
	}

	target, err := carryRetained(base, desired, bm, retained, renames)
	if err != nil {
		return nil, nil, err
	}
	return target, retained, nil
}

// carryRetained builds the target document: desired plus the retained base
// entries, copied from the base's canonical tree so every carried entry
// keeps its recorded form. Column names a renamed table's structured
// references use (index keys and includes, foreign-key columns) follow the
// rename; expressions that may name a renamed column cannot be rewritten
// offline and refuse.
func carryRetained(base, desired *V2Document, bm V2DocumentModel, retained []RetainedObject, renames map[V2Identity]map[string]string) (*V2Document, error) {
	var baseRoot, root map[string]any
	if err := json.Unmarshal(base.Canonical, &baseRoot); err != nil {
		return nil, fmt.Errorf("decode planning base: %w", err)
	}
	if err := json.Unmarshal(desired.Canonical, &root); err != nil {
		return nil, fmt.Errorf("decode schema document: %w", err)
	}
	baseEntry := func(key string, id V2Identity) map[string]any {
		for _, e := range asList(baseRoot[key]) {
			if m, ok := e.(map[string]any); ok && entryIdentity(m) == id {
				return m
			}
		}
		return nil
	}
	// place replaces an unmanaged desired entry of the same identity (the
	// planner treats it as undeclared) or appends.
	place := func(key string, entry map[string]any) {
		id := entryIdentity(entry)
		list := asList(root[key])
		out := make([]any, 0, len(list)+1)
		for _, e := range list {
			if m, ok := e.(map[string]any); ok && entryIdentity(m) == id {
				continue
			}
			out = append(out, e)
		}
		root[key] = append(out, entry)
	}
	desiredTable := func(id V2Identity) map[string]any {
		for _, e := range asList(root["tables"]) {
			if m, ok := e.(map[string]any); ok && entryIdentity(m) == id {
				return m
			}
		}
		return nil
	}
	var renamedNames []string
	for _, cols := range renames {
		for old := range cols {
			renamedNames = append(renamedNames, old)
		}
	}
	sort.Strings(renamedNames)

	columnsByTable := map[V2Identity]map[string]bool{}
	for _, r := range retained {
		switch r.Kind {
		case "table":
			entry := baseEntry("tables", r.Identity)
			for _, c := range asList(entry["constraints"]) {
				con, _ := c.(map[string]any)
				ref, _ := con["references"].(map[string]any)
				if ref == nil {
					continue
				}
				refTable, _ := ref["table"].(map[string]any)
				if to := renames[identityFrom(refTable)]; len(to) > 0 {
					ref["columns"] = renameNames(ref["columns"], to)
				}
			}
			place("tables", entry)
		case "view":
			entry := baseEntry("views", r.Identity)
			def, _ := entry["definition"].(string)
			if name := firstMentioned(def, renamedNames); name != "" {
				return nil, fmt.Errorf("view %s is left in place, and its definition may reference column %q, which this plan renames — an offline plan cannot rewrite the definition, so the target snapshot could not record the view as the database keeps it; declare the view in the schema document, or drop it with --allow-destructive", r.Identity, name)
			}
			place("views", entry)
		case "enum":
			place("enums", baseEntry("enums", r.Identity))
		case "column":
			if columnsByTable[r.Identity] == nil {
				columnsByTable[r.Identity] = map[string]bool{}
			}
			columnsByTable[r.Identity][r.Column] = true
		case "index":
			bt := baseEntry("tables", r.Table)
			var entry map[string]any
			for _, e := range asList(bt["indexes"]) {
				if m, ok := e.(map[string]any); ok && entryIdentity(m) == r.Identity {
					entry = m
				}
			}
			if to := renames[r.Table]; len(to) > 0 {
				old := sortedKeys(to)
				var exprs []string
				if w, ok := entry["where"].(string); ok {
					exprs = append(exprs, w)
				}
				for _, k := range asList(entry["key"]) {
					part, _ := k.(map[string]any)
					if col, ok := part["column"].(string); ok && to[col] != "" {
						part["column"] = to[col]
					}
					if e, ok := part["expression"].(string); ok {
						exprs = append(exprs, e)
					}
				}
				for _, e := range exprs {
					if name := firstMentioned(e, old); name != "" {
						return nil, fmt.Errorf("index %s on table %s is left in place, and its expression or predicate may reference column %q, which this plan renames — an offline plan cannot rewrite it, so the target snapshot could not record the index as the database keeps it; declare the index in the schema document, or drop it with --allow-destructive", r.Identity, r.Table, name)
					}
				}
				if inc, ok := entry["include"]; ok {
					entry["include"] = renameNames(inc, to)
				}
			}
			dt := desiredTable(r.Table)
			dt["indexes"] = append(asList(dt["indexes"]), entry)
		}
	}

	tables := make([]V2Identity, 0, len(columnsByTable))
	for id := range columnsByTable {
		tables = append(tables, id)
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].String() < tables[j].String() })
	for _, id := range tables {
		keep := columnsByTable[id]
		bt := bm.Table(id)
		rawCols := asList(baseEntry("tables", id)["columns"])
		dt := desiredTable(id)
		dcols := asList(dt["columns"])
		at := map[string]int{}
		for i, c := range dcols {
			m, _ := c.(map[string]any)
			name, _ := m["name"].(string)
			at[name] = i
		}
		to := renames[id]
		after := map[int][]any{}
		anchor := -1
		for i, bc := range bt.Columns {
			if keep[bc.Name] {
				col, _ := rawCols[i].(map[string]any)
				if g, ok := col["generated"].(map[string]any); ok && len(to) > 0 {
					expr, _ := g["expression"].(string)
					if name := firstMentioned(expr, sortedKeys(to)); name != "" {
						return nil, fmt.Errorf("column %s.%s is left in place, and its generation expression may reference column %q, which this plan renames — an offline plan cannot rewrite it, so the target snapshot could not record the column as the database keeps it; declare the column in the schema document, or drop it with --allow-destructive", id, bc.Name, name)
					}
				}
				after[anchor] = append(after[anchor], rawCols[i])
				continue
			}
			name := bc.Name
			if n := to[name]; n != "" {
				name = n
			}
			if j, ok := at[name]; ok {
				anchor = j
			}
		}
		out := append([]any(nil), after[-1]...)
		for j, c := range dcols {
			out = append(out, c)
			out = append(out, after[j]...)
		}
		dt["columns"] = out
	}

	raw, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("encode target snapshot: %w", err)
	}
	doc, err := ParseV2Document(raw)
	if err != nil {
		return nil, fmt.Errorf("recording the objects this plan leaves in place (%s) makes the target snapshot an invalid schema document: %w", RetainedList(retained), err)
	}
	// Re-read the canonical bytes, so the next plan from this document
	// walks it in canonical order, exactly as when it is read from disk.
	return ParseV2Document(doc.Canonical)
}

// readRetained reads each migration snapshot as its recorded document plus
// what its up file left in place from the previous snapshot. Snapshots
// written by this CLI already record those objects and read unchanged;
// earlier CLIs recorded the desired document only. The recorded target
// hash still anchors the chain, and no file is rewritten. A snapshot that
// cannot be read this way keeps its recorded document (and the drift gate
// then reports the difference, as before); the reason is kept in
// RetainedErrors.
func (c *SnapshotChain) readRetained(migrationsDir string) error {
	var prev *V2Document
	var err error
	if c.Baseline != nil {
		prev, err = ParseV2Document(c.Baseline.Document)
	} else {
		prev, err = EmptyV2Document()
	}
	if err != nil {
		return err
	}
	for i := range c.Snapshots {
		s := &c.Snapshots[i]
		recorded, err := ParseV2Document(s.Document)
		if err != nil {
			return fmt.Errorf("snapshot %s document is invalid: %w", s.Stem(), err)
		}
		upSQL, err := os.ReadFile(filepath.Join(migrationsDir, s.Stem()+".up.sql"))
		if err != nil {
			return fmt.Errorf("read %s.up.sql: %w", s.Stem(), err)
		}
		target, retained, err := SnapshotTarget(prev, recorded, SplitSQLStatements(string(upSQL)))
		if err != nil {
			c.RetainedErrors = append(c.RetainedErrors, fmt.Sprintf("snapshot %s is read as recorded: %v", s.Stem(), err))
			prev = recorded
			continue
		}
		if len(retained) > 0 {
			s.Document = append(json.RawMessage(nil), target.Canonical...)
			c.Retained = append(c.Retained, SnapshotRetained{Stem: s.Stem(), Objects: retained})
		}
		prev = target
	}
	return nil
}

// statementKey is a statement's significant token stream (comments and a
// trailing semicolon dropped, keywords lowercased by the tokenizer, quoted
// identifiers unescaped and case-preserved), for exact statement matching
// independent of layout.
func statementKey(sql string) string {
	var b strings.Builder
	for _, t := range significantTokens(sql) {
		if t.kind == 'p' && t.text == ";" {
			continue
		}
		b.WriteByte(t.kind)
		b.WriteString(t.text)
		b.WriteByte(0)
	}
	return b.String()
}

// plannedRenames reads the column renames among up statements, in the form
// the planner renders them: alter table "s"."t" rename column "old" to "new".
// The result maps table -> old name -> new name.
func plannedRenames(up []string) map[V2Identity]map[string]string {
	out := map[V2Identity]map[string]string{}
	for _, s := range up {
		toks := significantTokens(s)
		if len(toks) > 0 && toks[len(toks)-1].kind == 'p' && toks[len(toks)-1].text == ";" {
			toks = toks[:len(toks)-1]
		}
		if len(toks) != 10 {
			continue
		}
		want := []struct {
			kind byte
			text string
		}{{'w', "alter"}, {'w', "table"}, {'q', ""}, {'p', "."}, {'q', ""}, {'w', "rename"}, {'w', "column"}, {'q', ""}, {'w', "to"}, {'q', ""}}
		match := true
		for i, w := range want {
			if toks[i].kind != w.kind || (w.text != "" && toks[i].text != w.text) {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		id := V2Identity{Schema: toks[2].text, Name: toks[4].text}
		if out[id] == nil {
			out[id] = map[string]string{}
		}
		out[id][toks[7].text] = toks[9].text
	}
	return out
}

// firstMentioned returns the first of names that expr may reference as an
// identifier: quoted with the exact spelling, or unquoted (which folds to
// lower case). Conservative — a match inside an unrelated qualified name
// also counts.
func firstMentioned(expr string, names []string) string {
	if expr == "" || len(names) == 0 {
		return ""
	}
	toks := significantTokens(expr)
	for _, n := range names {
		for _, t := range toks {
			if (t.kind == 'q' || t.kind == 'w') && t.text == n {
				return n
			}
		}
	}
	return ""
}

func renameNames(v any, to map[string]string) []any {
	list := asList(v)
	out := make([]any, len(list))
	for i, e := range list {
		if s, ok := e.(string); ok && to[s] != "" {
			out[i] = to[s]
			continue
		}
		out[i] = e
	}
	return out
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

func entryIdentity(m map[string]any) V2Identity {
	ident, _ := m["identity"].(map[string]any)
	return identityFrom(ident)
}

func identityFrom(ident map[string]any) V2Identity {
	schema, _ := ident["schema"].(string)
	name, _ := ident["name"].(string)
	return V2Identity{Schema: schema, Name: name}
}
