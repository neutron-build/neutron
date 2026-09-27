package db

// Column order in snapshot planning (M08).
//
// PostgreSQL appends an added column after every existing one and cannot
// reorder columns without rebuilding the table. A schema document that
// declares a column elsewhere therefore describes an order the database
// never reaches, and no plan can reach it. Column order is informational in
// every mode: the planner notes an order difference and plans nothing for
// it (diff_v2.go planSharedTables). For snapshot planning, AlignColumnOrder
// additionally puts the columns the planning base has in its order and the
// new ones after them, in declared order (the order the plan adds them), so
// the target snapshot records the order the database holds.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ColumnOrderNote is a table whose declared column order differs from the
// order the database holds (or will hold after the plan).
type ColumnOrderNote struct {
	Table    V2Identity
	Declared []string
	Recorded []string
}

func (n ColumnOrderNote) String() string {
	return fmt.Sprintf("table %s: the schema declares columns (%s), the database holds them as (%s) — PostgreSQL appends added columns and cannot reorder existing ones, so the snapshot records the database order",
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
// It never refuses: an order difference is informational. Every other
// entry keeps its bytes; desired is returned as is when no table changes.
// The notes name the tables whose declared order differs.
func AlignColumnOrder(desired, base *V2Document, renames map[V2Identity]map[string]string) (*V2Document, []ColumnOrderNote, error) {
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
