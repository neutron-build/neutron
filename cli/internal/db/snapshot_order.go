package db

// Column order in snapshot planning (M08).
//
// PostgreSQL appends an added column after every existing one and cannot
// reorder columns without a rewrite. A schema document that declares a new
// column between existing ones therefore describes an order the database
// never reaches. Before the planner runs, AlignColumnOrder moves the new
// columns after the ones the planning base already has, in the order the
// document declares them, which is the order the plan adds them. The
// target snapshot then records the order the database holds, and the next
// plan compares against it without churn or an order refusal. A changed
// relative order of existing columns that entered the table together is
// still refused, with the planner's message; the chain's column
// generations (ColumnGenerations) tell them apart from a column a later
// migration appended. The planner's own rule (DiffV2Document refuses a reorder of
// matched columns, diff_v2.go planSharedTables) is unchanged: it still
// applies to live planning, db push and the drift gate, where the expected
// document is a snapshot in database order.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ColumnGenerations records, per table, which chain entry each column
// entered the table in: 0 for the baseline (or a table's creation), then
// the position of the migration snapshot that added it. PostgreSQL appends
// an added column, so the database order is the generations in turn; only
// columns of one generation have a relative order the document must keep.
// A nil map puts every column in one generation (the strict rule).
type ColumnGenerations map[V2Identity]map[string]int

// nextColumnGenerations derives the generations of doc, the state after the
// chain entry at index, from prev's: a column prev has (under its old name
// when the entry renames it) keeps its generation; any other is new.
func nextColumnGenerations(prevGens ColumnGenerations, prev, doc *V2Document, renames map[V2Identity]map[string]string, index int) ColumnGenerations {
	var pm, dm V2DocumentModel
	_ = json.Unmarshal(prev.Canonical, &pm)
	_ = json.Unmarshal(doc.Canonical, &dm)
	out := ColumnGenerations{}
	for _, t := range dm.Tables {
		from := map[string]string{}
		for old, n := range renames[t.Identity] {
			from[n] = old
		}
		pt := pm.Table(t.Identity)
		gens := map[string]int{}
		for _, c := range t.Columns {
			old := c.Name
			if o, ok := from[c.Name]; ok {
				old = o
			}
			if pt != nil && pt.Column(old) != nil {
				gens[c.Name] = prevGens[t.Identity][old]
			} else {
				gens[c.Name] = index
			}
		}
		out[t.Identity] = gens
	}
	return out
}

// ColumnOrderNote is a table whose declared column order differs from the
// order the database holds (or will hold after the plan).
type ColumnOrderNote struct {
	Table    V2Identity
	Declared []string
	Recorded []string
}

func (n ColumnOrderNote) String() string {
	return fmt.Sprintf("table %s: the schema declares columns (%s), the database holds them as (%s) — PostgreSQL appends added columns, so the snapshot records the database order (declare new columns last to match it)",
		n.Table, strings.Join(n.Declared, ", "), strings.Join(n.Recorded, ", "))
}

// RenamesByTable converts --rename intent ("schema.table.new" -> "old")
// into table -> old name -> new name.
func RenamesByTable(renames map[string]string) map[V2Identity]map[string]string {
	out := map[V2Identity]map[string]string{}
	for target, source := range renames {
		dot := strings.IndexByte(target, '.')
		if dot < 0 {
			continue
		}
		rest := target[dot+1:]
		dot2 := strings.IndexByte(rest, '.')
		if dot2 < 0 {
			continue
		}
		id := V2Identity{Schema: target[:dot], Name: rest[:dot2]}
		if out[id] == nil {
			out[id] = map[string]string{}
		}
		out[id][source] = rest[dot2+1:]
	}
	return out
}

// AlignColumnOrder returns desired with the columns of every managed table
// the base also has in database order: the columns the base has (matched
// by name, or by rename) in base order, then the others in declared order.
// Only new columns move. A document that changes the relative order of
// existing columns that entered the table together (gens: in the base
// state, or through one migration) is refused with the planner's reorder
// message, exactly as live planning and db push refuse it (that order stays
// schema state, contracts/data/CANONICAL.md); a column a later migration
// appended may be declared anywhere, which is how it got appended. Every
// other entry keeps its bytes; desired is returned as is when no table
// changes. The notes name the tables whose order changed.
func AlignColumnOrder(desired, base *V2Document, renames map[V2Identity]map[string]string, gens ColumnGenerations) (*V2Document, []ColumnOrderNote, error) {
	var bm V2DocumentModel
	if err := json.Unmarshal(base.Canonical, &bm); err != nil {
		return nil, nil, fmt.Errorf("decode planning base: %w", err)
	}
	var root map[string]any
	if err := json.Unmarshal(desired.Canonical, &root); err != nil {
		return nil, nil, fmt.Errorf("decode schema document: %w", err)
	}
	var notes []ColumnOrderNote
	for _, e := range asList(root["tables"]) {
		dt, _ := e.(map[string]any)
		if managed, _ := dt["managed"].(bool); !managed {
			continue
		}
		id := entryIdentity(dt)
		bt := bm.Table(id)
		if bt == nil {
			continue
		}
		cols := asList(dt["columns"])
		byName := map[string]any{}
		var declared []string
		for _, c := range cols {
			m, _ := c.(map[string]any)
			name, _ := m["name"].(string)
			byName[name] = c
			declared = append(declared, name)
		}
		// Existing columns that entered the table together (in the base
		// state, or through one migration) keep their relative order; a
		// column a later migration appended may be declared anywhere.
		basePos := map[string]int{}
		baseGen := map[string]int{}
		for i, bc := range bt.Columns {
			name := bc.Name
			if n := renames[id][name]; n != "" {
				name = n
			}
			basePos[name] = i
			baseGen[name] = gens[id][bc.Name]
		}
		last := map[int]int{}
		for _, name := range declared {
			pos, ok := basePos[name]
			if !ok {
				continue
			}
			if prev, seen := last[baseGen[name]]; seen && pos < prev {
				return nil, nil, fmt.Errorf(
					"table %s: the desired column order differs from the planning-base table (attnum order %v) — PostgreSQL cannot reorder columns without rewriting the table; align the document order or plan a manual migration",
					id, columnNames(*bt))
			}
			last[baseGen[name]] = pos
		}
		used := map[string]bool{}
		var ordered []any
		var recorded []string
		for _, bc := range bt.Columns {
			name := bc.Name
			if n := renames[id][name]; n != "" {
				name = n
			}
			if c, ok := byName[name]; ok && !used[name] {
				used[name] = true
				ordered = append(ordered, c)
				recorded = append(recorded, name)
			}
		}
		for i, c := range cols {
			if !used[declared[i]] {
				ordered = append(ordered, c)
				recorded = append(recorded, declared[i])
			}
		}
		if strings.Join(recorded, "\x00") == strings.Join(declared, "\x00") {
			continue
		}
		dt["columns"] = ordered
		notes = append(notes, ColumnOrderNote{Table: id, Declared: declared, Recorded: recorded})
	}
	if len(notes) == 0 {
		return desired, nil, nil
	}
	sort.Slice(notes, func(i, j int) bool { return notes[i].Table.String() < notes[j].Table.String() })
	raw, err := json.Marshal(root)
	if err != nil {
		return nil, nil, fmt.Errorf("encode schema document: %w", err)
	}
	doc, err := ParseV2Document(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("the schema document in database column order is invalid: %w", err)
	}
	doc, err = ParseV2Document(doc.Canonical)
	if err != nil {
		return nil, nil, err
	}
	return doc, notes, nil
}
