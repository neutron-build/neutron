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
// document omits is always dropped. An object the desired document declares
// with managed: false is not neutron's and is recorded as the document has
// it; one the chain records that way and the document omits is carried
// forward and not reported as left in place.
//
// It runs on the plan the planner just produced, when the snapshot is
// written. The chain is always read as recorded: snapshots written before
// M08 that omit such objects recover by re-baselining (the drift refusal
// names it).

import (
	"encoding/json"
	"fmt"
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

// flagDropsNote completes a refusal that recommends --allow-destructive:
// with the flag, the plan drops every object it would leave in place, not
// only the one the refusal names (Q12 review-1 F2).
func flagDropsNote(retained []RetainedObject) string {
	if len(retained) <= 1 {
		return ""
	}
	return ". Note that the flag drops every object this plan leaves in place, which here is: " + RetainedList(retained) + "; declare in the schema what must stay before using it"
}

// RetainedList renders retained objects for messages.
func RetainedList(objs []RetainedObject) string {
	parts := make([]string, len(objs))
	for i, o := range objs {
		parts[i] = o.String()
	}
	return strings.Join(parts, ", ")
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

	var retained, carried []RetainedObject
	for _, bt := range bm.Tables {
		id := bt.Identity
		if !scope[id.Schema] || isProtectedTableName(id.Name) {
			continue
		}
		dt := dm.Table(id)
		if dt != nil && !dt.Managed {
			continue // declared managed: false: recorded as the document has it
		}
		if dt == nil && !bt.Managed {
			// The chain records it managed: false and the document omits it:
			// still not neutron's, so it stays and is carried without being
			// reported as left in place.
			carried = append(carried, RetainedObject{Kind: "table", Identity: id})
			continue
		}
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
		if dv := dm.View(bv.Identity); dv != nil {
			continue // declared (managed: false is recorded as the document has it)
		}
		if !bv.Managed {
			carried = append(carried, RetainedObject{Kind: "view", Identity: bv.Identity})
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
		if de := dm.Enum(be.Identity); de != nil {
			continue // declared (managed: false is recorded as the document has it)
		}
		if !be.Managed {
			carried = append(carried, RetainedObject{Kind: "enum", Identity: be.Identity})
			continue
		}
		if !dropped("drop type if exists %s", qualifiedNameSQL(be.Identity)) {
			retained = append(retained, RetainedObject{Kind: "enum", Identity: be.Identity})
		}
	}
	if len(retained) == 0 && len(carried) == 0 {
		return desired, nil, nil
	}

	target, err := carryRetained(base, desired, bm, append(append([]RetainedObject(nil), retained...), carried...), renames)
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
	// place replaces a desired entry of the same identity or appends.
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
				return nil, fmt.Errorf("view %s is left in place, and its definition may reference column %q, which this plan renames — an offline plan cannot rewrite the definition, so the target snapshot could not record the view as the database keeps it; declare the view in the schema document, or drop it with --allow-destructive%s", r.Identity, name, flagDropsNote(retained))
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
						return nil, fmt.Errorf("index %s on table %s is left in place, and its expression or predicate may reference column %q, which this plan renames — an offline plan cannot rewrite it, so the target snapshot could not record the index as the database keeps it; declare the index in the schema document, or drop it with --allow-destructive%s", r.Identity, r.Table, name, flagDropsNote(retained))
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
						return nil, fmt.Errorf("column %s.%s is left in place, and its generation expression may reference column %q, which this plan renames — an offline plan cannot rewrite it, so the target snapshot could not record the column as the database keeps it; declare the column in the schema document, or drop it with --allow-destructive%s", id, bc.Name, name, flagDropsNote(retained))
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

	if err := undeclaredReference(root, retained); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("encode target snapshot: %w", err)
	}
	doc, err := ParseV2Document(raw)
	if err != nil {
		return nil, fmt.Errorf("recording the objects this plan leaves in place (%s) makes the target snapshot an invalid schema document: %w — declare them in the schema document, or drop them with --allow-destructive%s", RetainedList(retained), err, flagDropsNote(retained))
	}
	// Re-read the canonical bytes, so the next plan from this document
	// walks it in canonical order, exactly as when it is read from disk.
	return ParseV2Document(doc.Canonical)
}

// undeclaredReference refuses a carried table whose foreign key, or a
// carried column whose enum type, points at an object the target does not
// declare (typically in a schema outside the document). A snapshot cannot
// record the object without its target, and the way out is to declare the
// target, not the carried object.
func undeclaredReference(root map[string]any, retained []RetainedObject) error {
	schemas := map[string]bool{}
	for _, e := range asList(root["schemas"]) {
		m, _ := e.(map[string]any)
		name, _ := m["name"].(string)
		schemas[name] = true
	}
	declared := func(key string, id V2Identity) bool {
		for _, e := range asList(root[key]) {
			if m, ok := e.(map[string]any); ok && entryIdentity(m) == id {
				return true
			}
		}
		return false
	}
	advice := func(kind string, target V2Identity, object string) string {
		what := kind + " " + target.String()
		if !schemas[target.Schema] {
			what = "schema " + target.Schema + " and " + what
		}
		return fmt.Sprintf("declare %s in the schema document as they are in the database, or drop %s with --allow-destructive%s", what, object, flagDropsNote(retained))
	}
	table := func(id V2Identity) map[string]any {
		for _, e := range asList(root["tables"]) {
			if m, ok := e.(map[string]any); ok && entryIdentity(m) == id {
				return m
			}
		}
		return nil
	}
	enumOf := func(col map[string]any) (V2Identity, bool) {
		typ, _ := col["type"].(map[string]any)
		enum, ok := typ["enum"].(map[string]any)
		return identityFrom(enum), ok
	}
	for _, r := range retained {
		switch r.Kind {
		case "table":
			t := table(r.Identity)
			for _, c := range asList(t["constraints"]) {
				con, _ := c.(map[string]any)
				ref, _ := con["references"].(map[string]any)
				if ref == nil {
					continue
				}
				target := identityFrom(asMap(ref["table"]))
				if !declared("tables", target) {
					name, _ := con["name"].(string)
					return fmt.Errorf("table %s is left in place, and its foreign key %s references table %s, which the schema document does not declare — the target snapshot cannot record the table without it; %s", r.Identity, name, target, advice("table", target, "table "+r.Identity.String()))
				}
			}
			for _, c := range asList(t["columns"]) {
				col, _ := c.(map[string]any)
				if enum, ok := enumOf(col); ok && !declared("enums", enum) {
					return fmt.Errorf("table %s is left in place, and its column %v has type enum %s, which the schema document does not declare — the target snapshot cannot record the table without it; %s", r.Identity, col["name"], enum, advice("enum", enum, "table "+r.Identity.String()))
				}
			}
		case "column":
			for _, c := range asList(table(r.Identity)["columns"]) {
				col, _ := c.(map[string]any)
				if name, _ := col["name"].(string); name != r.Column {
					continue
				}
				if enum, ok := enumOf(col); ok && !declared("enums", enum) {
					return fmt.Errorf("column %s.%s is left in place, and its type is enum %s, which the schema document does not declare — the target snapshot cannot record the column without it; %s", r.Identity, r.Column, enum, advice("enum", enum, "the column"))
				}
			}
		}
	}
	return nil
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
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
