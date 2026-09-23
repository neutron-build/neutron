package db

// Typed model for schema document v2 (contracts/data/schema-v2.json). The
// canonical form, validation and hashing live in schema_v2.go and operate on
// generic JSON trees; this file provides struct-shaped access for
// introspection (M02) and diff/planning. Documents travel between the two
// representations through encoding/json, so the struct tags and optionality
// below must match the contract exactly: only contract fields exist, and
// optional fields are pointers (absent, never null, in a valid document).

import (
	"encoding/json"
	"fmt"
)

type V2Identity struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
}

func (i V2Identity) String() string { return i.Schema + "." + i.Name }

type V2SchemaDecl struct {
	Name string `json:"name"`
}

type V2EnumDecl struct {
	Identity V2Identity `json:"identity"`
	Managed  bool       `json:"managed"`
	Values   []string   `json:"values"`
}

// V2ColumnDefault is the tagged default of a column. Exactly one payload
// shape is valid per kind, enforced by the contract validator:
// literal/expression carry SQL text; identity carries the generation clause
// (and optionally an explicit sequence identity); sequence carries the
// sequence identity.
type V2ColumnDefault struct {
	Kind      string      `json:"kind"` // literal | expression | identity | sequence
	SQL       *string     `json:"sql,omitempty"`
	Generated *string     `json:"generated,omitempty"` // identity: always | by default
	Sequence  *V2Identity `json:"sequence,omitempty"`
}

// V2ColumnDefaultPayloads for comparison: two defaults are equal when kind
// and payload match. Used by diff; spelling of SQL text is compared through
// the twin normalizer, not here.
func (d V2ColumnDefault) SameAs(o V2ColumnDefault) bool {
	if d.Kind != o.Kind {
		return false
	}
	switch d.Kind {
	case "literal", "expression":
		return d.SQL != nil && o.SQL != nil && *d.SQL == *o.SQL
	case "identity":
		if d.Generated == nil || o.Generated == nil || *d.Generated != *o.Generated {
			return false
		}
		if (d.Sequence == nil) != (o.Sequence == nil) {
			return false
		}
		if d.Sequence != nil && *d.Sequence != *o.Sequence {
			return false
		}
		return true
	case "sequence":
		return d.Sequence != nil && o.Sequence != nil && *d.Sequence == *o.Sequence
	}
	return false
}

type V2ColumnType struct {
	Name   string           `json:"name"`
	Params map[string]int64 `json:"params,omitempty"`
	Array  bool             `json:"array,omitempty"`
	Codec  string           `json:"codec"`
	Enum   *V2Identity      `json:"enum,omitempty"`
}

func (t V2ColumnType) SameAs(o V2ColumnType) bool {
	if t.Name != o.Name || t.Array != o.Array || t.Codec != o.Codec {
		return false
	}
	if (t.Enum == nil) != (o.Enum == nil) {
		return false
	}
	if t.Enum != nil && *t.Enum != *o.Enum {
		return false
	}
	if len(t.Params) != len(o.Params) {
		return false
	}
	for k, v := range t.Params {
		if ov, ok := o.Params[k]; !ok || ov != v {
			return false
		}
	}
	return true
}

type V2Column struct {
	Name    string           `json:"name"`
	Type    V2ColumnType     `json:"type"`
	NotNull bool             `json:"notNull"`
	Default *V2ColumnDefault `json:"default,omitempty"`
}

type V2FKReference struct {
	Table    V2Identity `json:"table"`
	Columns  []string   `json:"columns"`
	OnDelete *string    `json:"onDelete,omitempty"`
	OnUpdate *string    `json:"onUpdate,omitempty"`
	Match    *string    `json:"match,omitempty"`
}

type V2Constraint struct {
	Name              string         `json:"name"`
	Type              string         `json:"type"` // primary-key | unique | check | foreign-key
	Columns           []string       `json:"columns,omitempty"`
	Expression        *string        `json:"expression,omitempty"`
	References        *V2FKReference `json:"references,omitempty"`
	Deferrable        *bool          `json:"deferrable,omitempty"`
	InitiallyDeferred *bool          `json:"initiallyDeferred,omitempty"`
}

type V2IndexKeyPart struct {
	Column     *string `json:"column,omitempty"`
	Expression *string `json:"expression,omitempty"`
}

type V2Index struct {
	Identity V2Identity       `json:"identity"`
	Unique   bool             `json:"unique"`
	Method   string           `json:"method"`
	Key      []V2IndexKeyPart `json:"key"`
	Where    *string          `json:"where,omitempty"`
	Include  []string         `json:"include,omitempty"`
}

type V2Table struct {
	Identity    V2Identity     `json:"identity"`
	Managed     bool           `json:"managed"`
	Columns     []V2Column     `json:"columns"`
	Constraints []V2Constraint `json:"constraints"`
	Indexes     []V2Index      `json:"indexes"`
}

func (t *V2Table) Column(name string) *V2Column {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i]
		}
	}
	return nil
}

func (t *V2Table) Constraint(name string) *V2Constraint {
	for i := range t.Constraints {
		if t.Constraints[i].Name == name {
			return &t.Constraints[i]
		}
	}
	return nil
}

func (t *V2Table) PrimaryKey() *V2Constraint {
	for i := range t.Constraints {
		if t.Constraints[i].Type == "primary-key" {
			return &t.Constraints[i]
		}
	}
	return nil
}

func (t *V2Table) Index(name string) *V2Index {
	for i := range t.Indexes {
		if t.Indexes[i].Identity.Name == name {
			return &t.Indexes[i]
		}
	}
	return nil
}

type V2View struct {
	Identity        V2Identity `json:"identity"`
	Managed         bool       `json:"managed"`
	Definition      string     `json:"definition"`
	CheckOption     *string    `json:"checkOption,omitempty"`
	SecurityInvoker *bool      `json:"securityInvoker,omitempty"`
}

type V2Opaque struct {
	Kind     string     `json:"kind"` // extension-table | extension-object | unsupported-table | unsupported-object
	Identity V2Identity `json:"identity"`
	Owner    string     `json:"owner,omitempty"`
	Reason   string     `json:"reason"`
}

// V2DocumentModel is the struct shape of a whole schema document v2.
type V2DocumentModel struct {
	Version      int            `json:"version"`
	Dialect      string         `json:"dialect"`
	Capabilities []string       `json:"capabilities"`
	Schemas      []V2SchemaDecl `json:"schemas"`
	Tables       []V2Table      `json:"tables"`
	Enums        []V2EnumDecl   `json:"enums"`
	Views        []V2View       `json:"views"`
	Opaque       []V2Opaque     `json:"opaque"`
}

func (m *V2DocumentModel) Table(id V2Identity) *V2Table {
	for i := range m.Tables {
		if m.Tables[i].Identity == id {
			return &m.Tables[i]
		}
	}
	return nil
}

func (m *V2DocumentModel) Enum(id V2Identity) *V2EnumDecl {
	for i := range m.Enums {
		if m.Enums[i].Identity == id {
			return &m.Enums[i]
		}
	}
	return nil
}

func (m *V2DocumentModel) View(id V2Identity) *V2View {
	for i := range m.Views {
		if m.Views[i].Identity == id {
			return &m.Views[i]
		}
	}
	return nil
}

func (m *V2DocumentModel) OpaqueEntry(kind string, id V2Identity) *V2Opaque {
	for i := range m.Opaque {
		if m.Opaque[i].Kind == kind && m.Opaque[i].Identity == id {
			return &m.Opaque[i]
		}
	}
	return nil
}

// ModelFromRoot decodes a validated v2 document tree into the typed model.
func ModelFromRoot(root map[string]any) (V2DocumentModel, error) {
	raw, err := json.Marshal(root)
	if err != nil {
		return V2DocumentModel{}, fmt.Errorf("re-encode v2 document: %w", err)
	}
	var m V2DocumentModel
	if err := json.Unmarshal(raw, &m); err != nil {
		return V2DocumentModel{}, fmt.Errorf("decode v2 document model: %w", err)
	}
	return m, nil
}

// RootFromModel encodes the typed model back into a generic document tree
// (the representation ParseV2Document validates and canonicalizes).
func RootFromModel(m V2DocumentModel) (map[string]any, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode v2 document model: %w", err)
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, fmt.Errorf("decode v2 document tree: %w", err)
	}
	return root, nil
}

func boolPtr(b bool) *bool { return &b }
