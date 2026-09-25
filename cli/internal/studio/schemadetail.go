package studio

// S05 schema navigation: one relation's detail from the live schema
// document v2 (IntrospectV2 — the document `neutron schema pull` writes), so
// the inspector and the designer show the CLI's own object identities and
// type spellings. Plans for visual changes live in schemaplan.go.
//
// Every request re-introspects fresh: a relation dropped or renamed
// concurrently answers 404 instead of a stale shape.

import (
	"fmt"
	"log"
	"net/http"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// --- GET /api/schema/object ---

// objectColumn, objectConstraint, objectIndex, objectFKEdge are the wire
// projections of one introspected table's metadata.
type objectColumn struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	NotNull bool   `json:"notNull"`
	HasPK   bool   `json:"isPrimaryKey"`
	Default *struct {
		Kind string  `json:"kind"`
		SQL  *string `json:"sql,omitempty"`
	} `json:"default,omitempty"`
	Generated *struct {
		Expression string `json:"expression"`
	} `json:"generated,omitempty"`
}

type objectIndexKeyPart struct {
	Column     *string `json:"column,omitempty"`
	Expression *string `json:"expression,omitempty"`
	Order      *string `json:"order,omitempty"`
	Nulls      *string `json:"nulls,omitempty"`
	Opclass    *string `json:"opclass,omitempty"`
}

type objectIndex struct {
	Name    string               `json:"name"`
	Unique  bool                 `json:"unique"`
	Method  string               `json:"method"`
	Key     []objectIndexKeyPart `json:"key"`
	Where   *string              `json:"where,omitempty"`
	Include []string             `json:"include,omitempty"`
}

type objectConstraint struct {
	Name       string   `json:"name"`
	Type       string   `json:"type"`
	Columns    []string `json:"columns,omitempty"`
	Expression *string  `json:"expression,omitempty"`
	References *struct {
		Table    string   `json:"table"`
		Columns  []string `json:"columns"`
		OnDelete *string  `json:"onDelete,omitempty"`
		OnUpdate *string  `json:"onUpdate,omitempty"`
		Match    *string  `json:"match,omitempty"`
	} `json:"references,omitempty"`
	Deferrable        *bool `json:"deferrable,omitempty"`
	InitiallyDeferred *bool `json:"initiallyDeferred,omitempty"`
}

// objectFKEdge is one foreign-key relationship seen from the inspected
// table. Columns are the referencing side's columns; RefColumns the
// referenced side's (a composite key stays an ordered tuple on both sides).
type objectFKEdge struct {
	Constraint string   `json:"constraint"`
	Schema     string   `json:"schema"` // the OTHER table's identity
	Name       string   `json:"name"`
	Columns    []string `json:"columns"`
	RefColumns []string `json:"refColumns"`
}

// handleSchemaObject returns one table's or view's catalog metadata from
// the live introspection document. A relation that introspection inventoried
// as opaque (extension-owned, partitioned, RLS-bearing, ...) is reported as
// such — never flattened into a shape that implies it is editable.
func (s *Server) handleSchemaObject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	connID := q.Get("connectionId")
	schemaName := q.Get("schema")
	tableName := q.Get("table")
	if connID == "" || schemaName == "" || tableName == "" {
		writeError(w, http.StatusBadRequest, "connectionId, schema and table are required")
		return
	}
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	if isNucleus, _, err := client.IsNucleus(r.Context()); err == nil && isNucleus {
		writeError(w, http.StatusUnprocessableEntity,
			"object detail uses the schema contract v2 introspection, which is verified on PostgreSQL only; Nucleus catalog conformance is not established (X00)")
		return
	}

	doc, err := client.IntrospectV2(r.Context())
	if err != nil {
		log.Printf("studio: introspect v2 error: %v", err)
		writeError(w, http.StatusBadGateway, "introspection failed: "+sanitizeError(err))
		return
	}
	model, err := db.ModelFromRoot(doc.Root)
	if err != nil {
		writeError(w, http.StatusBadGateway, "introspection decode failed: "+sanitizeError(err))
		return
	}
	id := db.V2Identity{Schema: schemaName, Name: tableName}

	out := map[string]any{
		"schema":         schemaName,
		"name":           tableName,
		"source":         "introspection-v2",
		"documentSHA256": doc.SHA256Hex,
	}

	if v := model.View(id); v != nil {
		out["kind"] = "view"
		view := map[string]any{"definition": v.Definition}
		if v.CheckOption != nil {
			view["checkOption"] = *v.CheckOption
		}
		if v.SecurityInvoker != nil {
			view["securityInvoker"] = *v.SecurityInvoker
		}
		out["view"] = view
		writeJSON(w, http.StatusOK, out)
		return
	}
	for _, o := range model.Opaque {
		if o.Identity == id {
			out["kind"] = "opaque"
			out["opaque"] = map[string]any{
				"opaqueKind": o.Kind,
				"reason":     o.Reason,
				"owner":      o.Owner,
			}
			writeJSON(w, http.StatusOK, out)
			return
		}
	}
	t := model.Table(id)
	if t == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": fmt.Sprintf(
				"no table or view %s in the live catalog (dropped, renamed, a different kind of relation, or not visible to this role); refresh the schema tree",
				id),
		})
		return
	}

	pk := map[string]bool{}
	if c := t.PrimaryKey(); c != nil {
		for _, col := range c.Columns {
			pk[col] = true
		}
	}
	cols := make([]objectColumn, 0, len(t.Columns))
	for _, c := range t.Columns {
		oc := objectColumn{Name: c.Name, NotNull: c.NotNull || pk[c.Name], HasPK: pk[c.Name]}
		if ddl, err := db.RenderV2TypeDDL(c.Type); err == nil {
			oc.Type = ddl
		} else {
			oc.Type = c.Type.Name
		}
		if c.Default != nil {
			oc.Default = &struct {
				Kind string  `json:"kind"`
				SQL  *string `json:"sql,omitempty"`
			}{Kind: c.Default.Kind, SQL: c.Default.SQL}
		}
		if c.Generated != nil {
			oc.Generated = &struct {
				Expression string `json:"expression"`
			}{Expression: c.Generated.Expression}
		}
		cols = append(cols, oc)
	}
	cons := make([]objectConstraint, 0, len(t.Constraints))
	outgoing := []objectFKEdge{}
	for _, c := range t.Constraints {
		oc := objectConstraint{Name: c.Name, Type: c.Type, Columns: c.Columns, Expression: c.Expression, Deferrable: c.Deferrable, InitiallyDeferred: c.InitiallyDeferred}
		if c.References != nil {
			ref := &struct {
				Table    string   `json:"table"`
				Columns  []string `json:"columns"`
				OnDelete *string  `json:"onDelete,omitempty"`
				OnUpdate *string  `json:"onUpdate,omitempty"`
				Match    *string  `json:"match,omitempty"`
			}{Table: c.References.Table.String(), Columns: c.References.Columns, OnDelete: c.References.OnDelete, OnUpdate: c.References.OnUpdate, Match: c.References.Match}
			oc.References = ref
			outgoing = append(outgoing, objectFKEdge{Constraint: c.Name, Schema: c.References.Table.Schema, Name: c.References.Table.Name, Columns: c.Columns, RefColumns: c.References.Columns})
		}
		cons = append(cons, oc)
	}
	idxs := make([]objectIndex, 0, len(t.Indexes))
	for _, ix := range t.Indexes {
		oi := objectIndex{Name: ix.Identity.Name, Unique: ix.Unique, Method: ix.Method, Where: ix.Where, Include: ix.Include}
		for _, k := range ix.Key {
			oi.Key = append(oi.Key, objectIndexKeyPart{Column: k.Column, Expression: k.Expression, Order: k.Order, Nulls: k.Nulls, Opclass: k.Opclass})
		}
		idxs = append(idxs, oi)
	}
	// Incoming foreign keys: relationships point both ways for navigation.
	incoming := []objectFKEdge{}
	for _, other := range model.Tables {
		if other.Identity == id {
			continue
		}
		for _, c := range other.Constraints {
			if c.Type == "foreign-key" && c.References != nil && c.References.Table == id {
				incoming = append(incoming, objectFKEdge{Constraint: c.Name, Schema: other.Identity.Schema, Name: other.Identity.Name, Columns: c.Columns, RefColumns: c.References.Columns})
			}
		}
	}

	out["kind"] = "table"
	out["table"] = map[string]any{
		"columns":      cols,
		"constraints":  cons,
		"indexes":      idxs,
		"references":   outgoing,
		"referencedBy": incoming,
	}
	writeJSON(w, http.StatusOK, out)
}
