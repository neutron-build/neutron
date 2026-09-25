package studio

// S05 reviewable migration plans for visual schema changes.
//
// The designer never builds DDL. It sends structured edits; the server
// applies them to a copy of the FRESH live schema document (IntrospectV2,
// the document `neutron schema pull` writes) and plans the transition with
// the CLI's own diff (DiffV2Document + the live twin normalizer, exactly the
// inputs `neutron db push` uses) and classifies it with the M03 plan
// builder (BuildPlanArtifact: per-operation destructive/data-loss flags and
// reversibility). The response carries the target document and the
// equivalent CLI invocation, so the same plan can be reproduced — and
// checked — outside Studio.
//
// Apply is bound to the reviewed plan: it takes the migration runner's
// advisory-lock session (M04), refuses migration-managed databases the way
// `neutron db push` does, re-plans from a fresh introspection under the
// lock and executes only when the fresh plan is byte-identical to the one
// the user reviewed (planId). A concurrent schema change between review and
// apply therefore surfaces as a stale plan, never as unreviewed DDL.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// SchemaColumnInput is one column of a create-table change.
type SchemaColumnInput struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	NotNull      bool    `json:"notNull"`
	Default      *string `json:"default"`
	IsPrimaryKey bool    `json:"isPrimaryKey"`
}

// SchemaChange is one visual designer edit. The vocabulary is deliberately
// small and fully representable in schema document v2; anything richer
// belongs in the SQL editor, where no silent approximation happens.
type SchemaChange struct {
	Op     string `json:"op"`
	Schema string `json:"schema"`
	Table  string `json:"table"`

	Column string `json:"column,omitempty"`
	From   string `json:"from,omitempty"` // rename-column source
	To     string `json:"to,omitempty"`   // rename-column target
	Type   string `json:"type,omitempty"` // add-column / alter-column-type
	// NotNull rides along add-column (create-table columns carry their own).
	NotNull bool `json:"notNull,omitempty"`

	Default *string `json:"default,omitempty"` // add-column / set-default

	Index   string              `json:"index,omitempty"`   // add-index / drop-index
	Unique  bool                `json:"unique,omitempty"`  // add-index
	Columns []SchemaColumnInput `json:"columns,omitempty"` // create-table
}

// StudioPlan is a reviewable plan for a set of designer changes.
type StudioPlan struct {
	// PlanID binds an apply to exactly this plan's effect: SHA-256 over the
	// renames and every up/down statement (see planFingerprint).
	PlanID          string             `json:"planId"`
	BaseSHA256      string             `json:"baseSha256"`
	TargetSHA256    string             `json:"targetSha256"`
	Up              []string           `json:"up"`
	Down            []string           `json:"down"`
	Warnings        []string           `json:"warnings"`
	Operations      []db.PlanOperation `json:"operations"`
	Risk            db.PlanRisk        `json:"risk"`
	TransactionMode string             `json:"transactionMode"`
	// RenameFlags are the `--rename` values reproducing this plan's renames.
	RenameFlags []string `json:"renameFlags"`
	// DesignerNotes name objects PostgreSQL removes together with a dropped
	// column (the desired document drops them explicitly, so the plan shows
	// them instead of letting them vanish implicitly).
	DesignerNotes []string `json:"designerNotes"`
	// CLIEquivalent reproduces the plan with the target document saved as
	// target.schema.json.
	CLIEquivalent string          `json:"cliEquivalent"`
	Target        json.RawMessage `json:"target"`

	desired       *db.V2Document
	explicitDrops bool
}

// maxSchemaChanges bounds one plan request (bounded diff discipline).
const maxSchemaChanges = 100

// schemaApplyLockWait bounds how long an apply waits for the migration
// advisory lock before reporting that a runner holds it.
var schemaApplyLockWait = 10 * time.Second

// errUnsupportedEngine marks a connection whose engine the v2 planner is not
// verified on (Nucleus, X00).
type errUnsupportedEngine struct{ msg string }

func (e errUnsupportedEngine) Error() string { return e.msg }

// columnDependent is one catalog object depending on a table column
// (pg_depend), classified for the designer's drop/rename rules.
type columnDependent struct {
	Kind      string // index | constraint | view | default | sequence | trigger | policy | statistics | relation | other
	Name      string // index/constraint name when applicable
	Describe  string // pg_describe_object text
	Auto      bool   // deptype 'a': PostgreSQL drops it together with the column
	SameTable bool
	Own       bool // the column's own default
	HasExpr   bool // carries SQL text that a column rename rewrites
}

// dependentsFunc looks up the live dependents of schema.table.column.
type dependentsFunc func(schema, table, column string) ([]columnDependent, error)

// PlanSchemaChanges introspects the live catalog, applies changes to a copy
// of it and plans the transition exactly as the CLI would plan the
// resulting target document against the same database.
func PlanSchemaChanges(ctx context.Context, client *db.Client, changes []SchemaChange) (*StudioPlan, error) {
	if isNucleus, _, err := client.IsNucleus(ctx); err == nil && isNucleus {
		return nil, errUnsupportedEngine{"migration planning uses the schema contract v2 diff, which is verified on PostgreSQL only; Nucleus planning conformance is not established (X00)"}
	}
	live, err := client.IntrospectV2(ctx)
	if err != nil {
		return nil, errIntrospection{err}
	}
	model, err := db.ModelFromRoot(live.Root)
	if err != nil {
		return nil, errIntrospection{err}
	}
	// The desired document is edited through the typed model; a model that
	// could not reproduce the live document byte-for-byte would smuggle
	// unrequested differences into the plan. Refuse instead.
	if roundTrip, err := documentFromModel(model); err != nil || roundTrip.SHA256Hex != live.SHA256Hex {
		return nil, fmt.Errorf("the live catalog is not losslessly representable by the designer's document model (round-trip changed it); plan this change with `neutron schema pull` + `neutron db push --dry-run` instead")
	}
	// Neutron-internal tables never appear in a desired document (the diff
	// refuses them); the live side stays complete, exactly like the CLI's.
	desiredModel := copyModel(model, func(t db.V2Table) bool { return !db.IsProtectedTableName(t.Identity.Name) })

	deps := func(schema, table, column string) ([]columnDependent, error) {
		return fetchColumnDependents(ctx, client, schema, table, column)
	}
	renames := map[string]string{}
	notes, err := applySchemaChanges(&desiredModel, changes, renames, deps)
	if err != nil {
		return nil, err
	}
	desired, err := documentFromModel(desiredModel)
	if err != nil {
		return nil, mutationDomainError{msg: fmt.Sprintf("the change set does not produce a valid schema document: %v", err)}
	}

	norm, err := client.NewTwinNormalizer(ctx)
	if err != nil {
		return nil, fmt.Errorf("expression normalizer: %w", err)
	}
	defer norm.Close()
	// AllowDestructive is safe here: the desired document is the live one
	// plus exactly the request's edits, so drops only arise from explicit
	// drop edits. The risk report gates apply instead.
	result, err := db.DiffV2Document(ctx, desired, live, db.DiffV2Options{
		Renames:          renames,
		AllowDestructive: true,
		Normalizer:       norm,
	})
	if err != nil {
		return nil, mutationDomainError{msg: "the planner refused the change set: " + sanitizeError(err)}
	}
	artifact, err := db.BuildPlanArtifact("", "", "live-catalog", live.SHA256Hex, desired, renames, result)
	if err != nil {
		return nil, err
	}

	plan := &StudioPlan{
		BaseSHA256:      live.SHA256Hex,
		TargetSHA256:    desired.SHA256Hex,
		Up:              nonNil(result.Up),
		Down:            nonNil(result.Down),
		Warnings:        nonNil(result.Warnings),
		Operations:      artifact.Operations,
		Risk:            artifact.Risk,
		TransactionMode: artifact.TransactionMode,
		RenameFlags:     renameFlags(renames),
		DesignerNotes:   nonNil(notes),
		Target:          json.RawMessage(desired.Canonical),
		desired:         desired,
	}
	if plan.Operations == nil {
		plan.Operations = []db.PlanOperation{}
	}
	for _, ch := range changes {
		if strings.HasPrefix(ch.Op, "drop-") && ch.Op != "drop-not-null" && ch.Op != "drop-default" {
			plan.explicitDrops = true
		}
	}
	plan.PlanID = planFingerprint(plan)
	plan.CLIEquivalent = cliEquivalent(plan)
	return plan, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// planFingerprint hashes everything that determines what an apply would
// execute: the renames and every up/down statement. The base/target document
// hashes are deliberately NOT part of it — an unrelated concurrent change
// (a new table elsewhere) moves both hashes but leaves the statements, and
// therefore the reviewed effect, identical.
func planFingerprint(p *StudioPlan) string {
	payload, _ := json.Marshal(struct {
		Renames []string `json:"renames"`
		Up      []string `json:"up"`
		Down    []string `json:"down"`
	}{p.RenameFlags, p.Up, p.Down})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// renameFlags renders the diff's rename map ("schema.table.new" -> old) as
// the CLI's --rename values, sorted.
func renameFlags(renames map[string]string) []string {
	out := make([]string, 0, len(renames))
	for key, from := range renames {
		i := strings.LastIndexByte(key, '.')
		out = append(out, key[:i+1]+from+">"+key)
	}
	sort.Strings(out)
	return out
}

func cliEquivalent(p *StudioPlan) string {
	parts := []string{"neutron", "db", "push", "--dry-run", "--schema", "target.schema.json"}
	for _, f := range p.RenameFlags {
		parts = append(parts, "--rename", shellQuote(f))
	}
	// --allow-destructive only changes a push plan when the target document
	// lacks live objects, i.e. for explicit drop edits (view drop/recreate
	// around table alterations is planned without it).
	if p.explicitDrops {
		parts = append(parts, "--allow-destructive")
	}
	return strings.Join(parts, " ")
}

// shellQuote single-quotes a POSIX shell word.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

// documentFromModel validates a model back into a canonical V2Document.
func documentFromModel(m db.V2DocumentModel) (*db.V2Document, error) {
	root, err := db.RootFromModel(m)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	return db.ParseV2Document(raw)
}

// copyModel deep-copies a document model's tables (every slice an edit may
// touch gets its own backing array) so edits to the copy can never bleed
// into the source; keep filters tables through the optional predicate.
func copyModel(m db.V2DocumentModel, keep ...func(db.V2Table) bool) db.V2DocumentModel {
	out := m
	// Non-nil copies: the contract requires every collection as an array.
	out.Schemas = append(make([]db.V2SchemaDecl, 0, len(m.Schemas)), m.Schemas...)
	out.Enums = append(make([]db.V2EnumDecl, 0, len(m.Enums)), m.Enums...)
	out.Views = append(make([]db.V2View, 0, len(m.Views)), m.Views...)
	out.Opaque = append(make([]db.V2Opaque, 0, len(m.Opaque)), m.Opaque...)
	out.Tables = make([]db.V2Table, 0, len(m.Tables))
	for _, t := range m.Tables {
		if len(keep) == 1 && !keep[0](t) {
			continue
		}
		out.Tables = append(out.Tables, copyTable(t))
	}
	return out
}

func copyTable(t db.V2Table) db.V2Table {
	ct := t
	// The contract requires every collection present as an array; nil
	// slices would marshal to null and invalidate the document.
	ct.Columns = append(make([]db.V2Column, 0, len(t.Columns)), t.Columns...)
	ct.Constraints = make([]db.V2Constraint, 0, len(t.Constraints))
	for _, c := range t.Constraints {
		cc := c
		if c.Columns != nil {
			cc.Columns = append([]string(nil), c.Columns...)
		}
		if c.References != nil {
			ref := *c.References
			ref.Columns = append([]string(nil), c.References.Columns...)
			cc.References = &ref
		}
		ct.Constraints = append(ct.Constraints, cc)
	}
	ct.Indexes = make([]db.V2Index, 0, len(t.Indexes))
	for _, ix := range t.Indexes {
		ci := ix
		ci.Key = append(make([]db.V2IndexKeyPart, 0, len(ix.Key)), ix.Key...)
		if ix.Include != nil {
			ci.Include = append([]string(nil), ix.Include...)
		}
		ct.Indexes = append(ct.Indexes, ci)
	}
	return ct
}

// applySchemaChanges applies the request's edits to the desired model in
// place, validating every target against the live state the model was
// copied from. renames collects rename intent in the diff's key format;
// deps reports live column dependents (pg_depend). Returns designer notes
// naming dependents a dropped column takes with it. Errors name the
// offending change and are mutationDomainError (the request is wrong or
// unrepresentable).
func applySchemaChanges(model *db.V2DocumentModel, changes []SchemaChange, renames map[string]string, deps dependentsFunc) ([]string, error) {
	var notes []string
	if len(changes) == 0 {
		return nil, mutationDomainError{msg: "changes must name at least one edit"}
	}
	if len(changes) > maxSchemaChanges {
		return nil, mutationDomainError{msg: fmt.Sprintf("at most %d changes per plan request", maxSchemaChanges)}
	}
	// liveName maps a desired column name back to its live name through the
	// renames recorded so far (dependents are looked up in the live catalog).
	liveName := func(schema, table, column string) string {
		if from, ok := renames[schema+"."+table+"."+column]; ok {
			return from
		}
		return column
	}
	for i, ch := range changes {
		at := func(format string, args ...any) error {
			return mutationDomainError{msg: fmt.Sprintf("changes[%d] (%s): %s", i, ch.Op, fmt.Sprintf(format, args...))}
		}
		if ch.Schema == "" || ch.Table == "" {
			return nil, at("schema and table are required")
		}
		id := db.V2Identity{Schema: ch.Schema, Name: ch.Table}
		if ch.Op != "create-table" && db.IsProtectedTableName(ch.Table) {
			return nil, at("table %s is neutron-internal metadata and is never planned", id)
		}
		var t *db.V2Table
		if ch.Op != "create-table" {
			t = model.Table(id)
			if t == nil {
				return nil, at("table %s does not exist in the live catalog (dropped or renamed concurrently? refresh the schema)", id)
			}
		}
		column := func(name string) (*db.V2Column, error) {
			if name == "" {
				return nil, at("column is required")
			}
			c := t.Column(name)
			if c == nil {
				return nil, at("column %q does not exist on %s", name, id)
			}
			return c, nil
		}
		switch ch.Op {
		case "create-table":
			if err := applyCreateTable(model, ch, at); err != nil {
				return nil, err
			}
		case "drop-table":
			for _, other := range model.Tables {
				if other.Identity == id {
					continue
				}
				for _, c := range other.Constraints {
					if c.References != nil && c.References.Table == id {
						return nil, at("table %s is referenced by foreign key %q on %s; drop or change that constraint first (the designer never cascades)", id, c.Name, other.Identity)
					}
				}
			}
			for idx := range model.Tables {
				if model.Tables[idx].Identity == id {
					model.Tables = append(model.Tables[:idx], model.Tables[idx+1:]...)
					break
				}
			}
		case "add-column":
			if ch.Column == "" || ch.Type == "" {
				return nil, at("add-column needs column and type")
			}
			if t.Column(ch.Column) != nil {
				return nil, at("column %q already exists on %s", ch.Column, id)
			}
			col, err := columnFromInput(ch.Column, ch.Type, ch.NotNull, ch.Default)
			if err != nil {
				return nil, at("%v", err)
			}
			t.Columns = append(t.Columns, *col)
		case "drop-column":
			if _, err := column(ch.Column); err != nil {
				return nil, err
			}
			if pk := t.PrimaryKey(); pk != nil && containsString(pk.Columns, ch.Column) {
				return nil, at("column %q is part of the primary key; change the key in the SQL editor", ch.Column)
			}
			if len(t.Columns) == 1 {
				return nil, at("column %q is the table's only column; drop the table instead", ch.Column)
			}
			dropNotes, err := dropColumn(model, t, ch.Column, liveName(ch.Schema, ch.Table, ch.Column), deps)
			if err != nil {
				return nil, at("%v", err)
			}
			notes = append(notes, dropNotes...)
		case "rename-column":
			if ch.From == "" || ch.To == "" {
				return nil, at("rename-column needs from and to")
			}
			if _, err := column(ch.From); err != nil {
				return nil, err
			}
			if t.Column(ch.To) != nil {
				return nil, at("column %q already exists on %s", ch.To, id)
			}
			key := ch.Schema + "." + ch.Table + "."
			if _, renamedBefore := renames[key+ch.From]; renamedBefore {
				return nil, at("column %q was already renamed in this change set; rename it once", ch.From)
			}
			if deps != nil {
				ds, err := deps(ch.Schema, ch.Table, ch.From)
				if err != nil {
					return nil, at("dependency lookup failed: %v", err)
				}
				for _, d := range ds {
					if d.HasExpr && !d.Own {
						return nil, at("%s references column %q in SQL text that PostgreSQL rewrites on rename, which the schema document cannot follow; rename it in the SQL editor", d.Describe, ch.From)
					}
				}
			}
			renameColumnInModel(model, id, ch.From, ch.To)
			renames[key+ch.To] = ch.From
		case "alter-column-type":
			c, err := column(ch.Column)
			if err != nil {
				return nil, err
			}
			if c.Generated != nil {
				return nil, at("generated column %q cannot change type through the designer (PostgreSQL requires dropping and re-adding it)", ch.Column)
			}
			typ, err := db.ParseV2TypeText(ch.Type)
			if err != nil {
				return nil, at("%v", err)
			}
			c.Type = typ
		case "set-not-null", "drop-not-null":
			c, err := column(ch.Column)
			if err != nil {
				return nil, err
			}
			if ch.Op == "set-not-null" {
				c.NotNull = true
				break
			}
			if pk := t.PrimaryKey(); pk != nil && containsString(pk.Columns, ch.Column) {
				return nil, at("primary-key column %q is always NOT NULL", ch.Column)
			}
			c.NotNull = false
		case "set-default":
			c, err := column(ch.Column)
			if err != nil {
				return nil, err
			}
			if c.Default != nil && (c.Default.Kind == "identity" || c.Default.Kind == "sequence") {
				return nil, at("column %q has a database-generated %s default; change it in the SQL editor", ch.Column, c.Default.Kind)
			}
			if c.Generated != nil {
				return nil, at("column %q is generated; it has no default", ch.Column)
			}
			d, err := db.V2DefaultFromText(deref(ch.Default))
			if err != nil {
				return nil, at("%v", err)
			}
			c.Default = d
		case "drop-default":
			c, err := column(ch.Column)
			if err != nil {
				return nil, err
			}
			if c.Default != nil && (c.Default.Kind == "identity" || c.Default.Kind == "sequence") {
				return nil, at("column %q has a database-generated %s default; dropping it changes the column's meaning — use the SQL editor", ch.Column, c.Default.Kind)
			}
			c.Default = nil
		case "add-index":
			if ch.Index == "" || ch.Column == "" {
				return nil, at("add-index needs index and column")
			}
			if what := relationNameTaken(model, ch.Schema, ch.Index); what != "" {
				return nil, at("the name %q is already used by %s in schema %s", ch.Index, what, ch.Schema)
			}
			if _, err := column(ch.Column); err != nil {
				return nil, err
			}
			col := ch.Column
			t.Indexes = append(t.Indexes, db.V2Index{
				Identity: db.V2Identity{Schema: ch.Schema, Name: ch.Index},
				Unique:   ch.Unique,
				Method:   "btree",
				Key:      []db.V2IndexKeyPart{{Column: &col}},
			})
		case "drop-index":
			idx := -1
			for k := range t.Indexes {
				if t.Indexes[k].Identity.Name == ch.Index {
					idx = k
					break
				}
			}
			if idx < 0 {
				if t.Constraint(ch.Index) != nil {
					return nil, at("%q backs a constraint; drop the constraint in the SQL editor", ch.Index)
				}
				return nil, at("index %q does not exist on %s", ch.Index, id)
			}
			t.Indexes = append(t.Indexes[:idx], t.Indexes[idx+1:]...)
		default:
			return nil, at("unknown op (supported: create-table, drop-table, add-column, drop-column, rename-column, alter-column-type, set-not-null, drop-not-null, set-default, drop-default, add-index, drop-index)")
		}
	}
	return notes, nil
}

// dropColumn removes a column from the desired table following PostgreSQL's
// own DROP COLUMN semantics, made explicit: indexes and constraints of the
// table that involve the column are dropped WHOLE (PostgreSQL never narrows
// a multi-column index or constraint), and anything that would need CASCADE
// (foreign keys from other tables, views, generated columns, triggers,
// policies) refuses the change.
func dropColumn(model *db.V2DocumentModel, t *db.V2Table, column, live string, deps dependentsFunc) ([]string, error) {
	var notes []string
	drop := map[string]bool{}
	// Document-level involvement (covers columns added in this change set
	// and keeps the rule independent of catalog wording).
	for _, c := range t.Constraints {
		if containsString(c.Columns, column) {
			drop[c.Name] = true
		}
	}
	for _, ix := range t.Indexes {
		if containsString(ix.Include, column) {
			drop[ix.Identity.Name] = true
		}
		for _, k := range ix.Key {
			if k.Column != nil && *k.Column == column {
				drop[ix.Identity.Name] = true
			}
		}
	}
	for _, other := range model.Tables {
		for _, c := range other.Constraints {
			if c.References != nil && c.References.Table == t.Identity && containsString(c.References.Columns, column) {
				return nil, fmt.Errorf("column %q is referenced by foreign key %q on %s; the designer never cascades — change that constraint first", column, c.Name, other.Identity)
			}
		}
	}
	if deps != nil && t.Column(column) != nil {
		ds, err := deps(t.Identity.Schema, t.Identity.Name, live)
		if err != nil {
			return nil, fmt.Errorf("dependency lookup failed: %v", err)
		}
		for _, d := range ds {
			switch {
			case d.Own || d.Kind == "sequence":
				// The column's own default / owned sequence go with it.
			case d.Kind == "statistics" && d.Auto:
				notes = append(notes, fmt.Sprintf("%s is removed by PostgreSQL with column %q (extended statistics are outside the schema document)", d.Describe, column))
			case (d.Kind == "index" || d.Kind == "constraint") && d.SameTable:
				// PostgreSQL drops the table's own indexes and constraints
				// involving the column, whole (a CHECK expression records a
				// normal dependency next to its automatic key dependency).
				drop[d.Name] = true
			default:
				return nil, fmt.Errorf("%s depends on column %q (dropping it would need CASCADE); the designer never cascades — change the dependent object in the SQL editor first", d.Describe, column)
			}
		}
	}
	keepCons := t.Constraints[:0]
	for _, c := range t.Constraints {
		if drop[c.Name] {
			notes = append(notes, fmt.Sprintf("%s constraint %q on %s involves column %q and is dropped with it", c.Type, c.Name, t.Identity, column))
			continue
		}
		keepCons = append(keepCons, c)
	}
	t.Constraints = keepCons
	keepIdx := t.Indexes[:0]
	for _, ix := range t.Indexes {
		if drop[ix.Identity.Name] {
			notes = append(notes, fmt.Sprintf("index %q on %s involves column %q and is dropped with it", ix.Identity.Name, t.Identity, column))
			continue
		}
		keepIdx = append(keepIdx, ix)
	}
	t.Indexes = keepIdx
	keepCols := t.Columns[:0]
	for _, c := range t.Columns {
		if c.Name != column {
			keepCols = append(keepCols, c)
		}
	}
	t.Columns = keepCols
	return notes, nil
}

// renameColumnInModel renames a column in its table's columns, constraints
// and index keys, and in every foreign key that references it.
func renameColumnInModel(model *db.V2DocumentModel, id db.V2Identity, from, to string) {
	rename := func(list []string) {
		for k := range list {
			if list[k] == from {
				list[k] = to
			}
		}
	}
	for ti := range model.Tables {
		t := &model.Tables[ti]
		for ci := range t.Constraints {
			if ref := t.Constraints[ci].References; ref != nil && ref.Table == id {
				rename(ref.Columns)
			}
		}
		if t.Identity != id {
			continue
		}
		for ci := range t.Columns {
			if t.Columns[ci].Name == from {
				t.Columns[ci].Name = to
			}
		}
		for ci := range t.Constraints {
			rename(t.Constraints[ci].Columns)
		}
		for ii := range t.Indexes {
			rename(t.Indexes[ii].Include)
			for ki := range t.Indexes[ii].Key {
				if c := t.Indexes[ii].Key[ki].Column; c != nil && *c == from {
					name := to
					t.Indexes[ii].Key[ki].Column = &name
				}
			}
		}
	}
}

// relationNameTaken reports what already uses name in schema (index and
// relation names share one namespace), or "".
func relationNameTaken(model *db.V2DocumentModel, schema, name string) string {
	id := db.V2Identity{Schema: schema, Name: name}
	if model.Table(id) != nil {
		return "a table"
	}
	if model.View(id) != nil {
		return "a view"
	}
	for _, t := range model.Tables {
		if t.Identity.Schema != schema {
			continue
		}
		for _, ix := range t.Indexes {
			if ix.Identity.Name == name {
				return "an index on " + t.Identity.String()
			}
		}
		for _, c := range t.Constraints {
			if (c.Type == "primary-key" || c.Type == "unique") && c.Name == name {
				return "the index backing constraint " + c.Name + " on " + t.Identity.String()
			}
		}
	}
	for _, o := range model.Opaque {
		if o.Identity == id {
			return "an object the planner does not manage"
		}
	}
	return ""
}

// applyCreateTable appends a new table to the desired model.
func applyCreateTable(model *db.V2DocumentModel, ch SchemaChange, at func(string, ...any) error) error {
	if db.IsProtectedTableName(ch.Table) {
		return at("table name %q is reserved for neutron-internal metadata", ch.Table)
	}
	if what := relationNameTaken(model, ch.Schema, ch.Table); what != "" {
		return at("the name %s.%s is already used by %s", ch.Schema, ch.Table, what)
	}
	schemaKnown := false
	for _, sd := range model.Schemas {
		if sd.Name == ch.Schema {
			schemaKnown = true
		}
	}
	if !schemaKnown {
		return at("schema %q does not exist in the live catalog; create schemas in the SQL editor", ch.Schema)
	}
	if len(ch.Columns) == 0 {
		return at("create-table needs at least one column")
	}
	seen := map[string]bool{}
	var pkCols []string
	cols := make([]db.V2Column, 0, len(ch.Columns))
	for _, ci := range ch.Columns {
		if ci.Name == "" || ci.Type == "" {
			return at("every column needs name and type")
		}
		if seen[ci.Name] {
			return at("column %q appears twice", ci.Name)
		}
		seen[ci.Name] = true
		col, err := columnFromInput(ci.Name, ci.Type, ci.NotNull || ci.IsPrimaryKey, ci.Default)
		if err != nil {
			return at("column %q: %v", ci.Name, err)
		}
		if ci.IsPrimaryKey {
			pkCols = append(pkCols, ci.Name)
		}
		cols = append(cols, *col)
	}
	t := db.V2Table{
		Identity:    db.V2Identity{Schema: ch.Schema, Name: ch.Table},
		Managed:     true,
		Columns:     cols,
		Constraints: []db.V2Constraint{},
		Indexes:     []db.V2Index{},
	}
	if len(pkCols) > 0 {
		t.Constraints = append(t.Constraints, db.V2Constraint{
			Name:    ch.Table + "_pkey",
			Type:    "primary-key",
			Columns: pkCols,
		})
	}
	model.Tables = append(model.Tables, t)
	return nil
}

func columnFromInput(name, typeText string, notNull bool, defText *string) (*db.V2Column, error) {
	typ, err := db.ParseV2TypeText(typeText)
	if err != nil {
		return nil, err
	}
	col := &db.V2Column{Name: name, Type: typ, NotNull: notNull}
	if defText != nil && strings.TrimSpace(*defText) != "" {
		d, err := db.V2DefaultFromText(*defText)
		if err != nil {
			return nil, err
		}
		col.Default = d
	}
	return col, nil
}

func containsString(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// fetchColumnDependents reads pg_depend for one live column: every object
// PostgreSQL either drops with the column (deptype 'a') or refuses to drop
// it without CASCADE for (deptype 'n'), classified for the designer rules.
func fetchColumnDependents(ctx context.Context, client *db.Client, schema, table, column string) ([]columnDependent, error) {
	rows, err := client.Query(ctx, `
		SELECT pg_describe_object(d.classid, d.objid, d.objsubid),
		       d.deptype = 'a',
		       CASE d.classid
		         WHEN 'pg_class'::regclass THEN (SELECT CASE WHEN c.relkind IN ('i','I') THEN 'index'
		                                                     WHEN c.relkind = 'S' THEN 'sequence'
		                                                     ELSE 'relation' END
		                                         FROM pg_class c WHERE c.oid = d.objid)
		         WHEN 'pg_constraint'::regclass THEN 'constraint'
		         WHEN 'pg_rewrite'::regclass THEN 'view'
		         WHEN 'pg_attrdef'::regclass THEN 'default'
		         WHEN 'pg_trigger'::regclass THEN 'trigger'
		         WHEN 'pg_policy'::regclass THEN 'policy'
		         WHEN 'pg_statistic_ext'::regclass THEN 'statistics'
		         ELSE 'other' END,
		       COALESCE(CASE d.classid
		         WHEN 'pg_class'::regclass THEN (SELECT c.relname::text FROM pg_class c WHERE c.oid = d.objid)
		         WHEN 'pg_constraint'::regclass THEN (SELECT c.conname::text FROM pg_constraint c WHERE c.oid = d.objid)
		         END, ''),
		       COALESCE(CASE d.classid
		         WHEN 'pg_class'::regclass THEN (SELECT i.indrelid = a.attrelid FROM pg_index i WHERE i.indexrelid = d.objid)
		         WHEN 'pg_constraint'::regclass THEN (SELECT c.conrelid = a.attrelid FROM pg_constraint c WHERE c.oid = d.objid)
		         WHEN 'pg_attrdef'::regclass THEN (SELECT ad.adrelid = a.attrelid FROM pg_attrdef ad WHERE ad.oid = d.objid)
		         WHEN 'pg_rewrite'::regclass THEN (SELECT r.ev_class = a.attrelid FROM pg_rewrite r WHERE r.oid = d.objid)
		         END, false),
		       COALESCE(CASE d.classid
		         WHEN 'pg_attrdef'::regclass THEN (SELECT ad.adrelid = a.attrelid AND ad.adnum = a.attnum FROM pg_attrdef ad WHERE ad.oid = d.objid)
		         END, false),
		       COALESCE(CASE d.classid
		         WHEN 'pg_class'::regclass THEN (SELECT i.indexprs IS NOT NULL OR i.indpred IS NOT NULL FROM pg_index i WHERE i.indexrelid = d.objid)
		         WHEN 'pg_constraint'::regclass THEN (SELECT c.contype = 'c' FROM pg_constraint c WHERE c.oid = d.objid)
		         WHEN 'pg_attrdef'::regclass THEN true
		         WHEN 'pg_rewrite'::regclass THEN true
		         WHEN 'pg_trigger'::regclass THEN true
		         WHEN 'pg_policy'::regclass THEN true
		         END, false)
		FROM pg_attribute a
		JOIN pg_depend d
		  ON d.refclassid = 'pg_class'::regclass AND d.refobjid = a.attrelid AND d.refobjsubid = a.attnum
		WHERE a.attrelid = to_regclass(format('%I.%I', $1::text, $2::text))
		  AND a.attname = $3 AND NOT a.attisdropped
		  AND d.deptype IN ('n', 'a')
		ORDER BY 1`, schema, table, column)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []columnDependent
	for rows.Next() {
		var d columnDependent
		if err := rows.Scan(&d.Describe, &d.Auto, &d.Kind, &d.Name, &d.SameTable, &d.Own, &d.HasExpr); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// --- HTTP ---

type schemaPlanRequest struct {
	ConnectionID string         `json:"connectionId"`
	Changes      []SchemaChange `json:"changes"`
	// Apply only: the reviewed plan and the data-loss acknowledgement.
	PlanID           string `json:"planId,omitempty"`
	AllowDestructive bool   `json:"allowDestructive,omitempty"`
}

// planResponse is the wire shape of a preview or apply.
type planResponse struct {
	*StudioPlan
	Applied      bool     `json:"applied"`
	Verification string   `json:"verification,omitempty"`
	Residual     []string `json:"residual,omitempty"`
}

// handleSchemaPlan previews the plan for the request's visual changes.
// Nothing executes and no lock is taken.
func (s *Server) handleSchemaPlan(w http.ResponseWriter, r *http.Request) {
	req, client, ok := s.readSchemaPlanRequest(w, r)
	if !ok {
		return
	}
	if req.PlanID != "" || req.AllowDestructive {
		writeError(w, http.StatusBadRequest, "planId and allowDestructive belong to /api/schema/apply; a preview never executes")
		return
	}
	plan, err := PlanSchemaChanges(r.Context(), client, req.Changes)
	if err != nil {
		writePlanError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, planResponse{StudioPlan: plan})
}

// handleSchemaApply applies a reviewed plan: lock, refuse migration-managed
// databases, re-plan fresh under the lock, require the identical planId and
// the data-loss acknowledgement, execute in one transaction, verify.
func (s *Server) handleSchemaApply(w http.ResponseWriter, r *http.Request) {
	req, client, ok := s.readSchemaPlanRequest(w, r)
	if !ok {
		return
	}
	if req.PlanID == "" {
		writeError(w, http.StatusBadRequest, "planId is required: apply executes only a plan you reviewed (POST /api/schema/plan first)")
		return
	}
	if isNucleus, _, err := client.IsNucleus(r.Context()); err == nil && isNucleus {
		writePlanError(w, errUnsupportedEngine{"schema apply uses the schema contract v2 diff, which is verified on PostgreSQL only (X00)"})
		return
	}

	lockCtx, cancelLock := context.WithTimeout(r.Context(), schemaApplyLockWait)
	sess, err := client.LockMigrations(lockCtx)
	cancelLock()
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "the migration lock is held (a migration or push is running) or could not be taken; nothing was applied: " + sanitizeError(err),
			"state": "locked",
		})
		return
	}
	defer sess.Release()

	has, err := client.HasMigrationHistory(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "migration history check failed; nothing was applied: "+sanitizeError(err))
		return
	}
	if has {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "this database has a migration history (_neutron_migrations): schema changes belong in migration files, exactly like `neutron db push` refuses here. Download the target document and run `neutron migrate generate --schema target.schema.json --name <name>`, then `neutron migrate`. Nothing was applied.",
			"state": "migration-managed",
		})
		return
	}

	plan, err := PlanSchemaChanges(r.Context(), client, req.Changes)
	if err != nil {
		writePlanError(w, err)
		return
	}
	if plan.PlanID != req.PlanID {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "the live catalog changed since this plan was reviewed, so it now plans different statements; nothing was applied. Review the fresh plan.",
			"state": "stale-plan",
			"plan":  planResponse{StudioPlan: plan},
		})
		return
	}
	if (plan.Risk.HasDestructive || plan.Risk.HasDataLoss) && !req.AllowDestructive {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "this plan drops objects or can lose data; nothing was applied. Resend with allowDestructive=true to acknowledge it explicitly.",
			"state": "destructive-unacknowledged",
		})
		return
	}
	if len(plan.Up) == 0 {
		writeJSON(w, http.StatusOK, planResponse{StudioPlan: plan, Verification: "in-sync"})
		return
	}

	if err := sess.ApplyStatementsTx(r.Context(), plan.Up, nil); err != nil {
		log.Printf("studio: schema apply failed: %v", err)
		if strings.HasPrefix(err.Error(), "commit:") {
			writeJSON(w, http.StatusBadGateway, map[string]any{
				"error": "the commit did not complete and its outcome is unknown (PostgreSQL DDL is transactional: either every statement applied or none did). Refresh the schema and inspect before retrying: " + sanitizeError(err),
				"state": "outcome-unknown",
			})
			return
		}
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"error": "the plan failed and rolled back as a whole; nothing was applied: " + sanitizeError(err),
			"state": "sql-error",
		})
		return
	}

	resp := planResponse{StudioPlan: plan, Applied: true}
	residual, verr := verifyPlanConvergence(r.Context(), client, plan.desired)
	switch {
	case verr != nil:
		resp.Verification = "unverified: " + sanitizeError(verr)
	case len(residual) == 0:
		resp.Verification = "in-sync"
	default:
		resp.Verification = "drift"
		resp.Residual = residual
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) readSchemaPlanRequest(w http.ResponseWriter, r *http.Request) (*schemaPlanRequest, *db.Client, bool) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return nil, nil, false
	}
	var req schemaPlanRequest
	if !s.readMutationBody(w, r, &req) {
		return nil, nil, false
	}
	if req.ConnectionID == "" {
		writeError(w, http.StatusBadRequest, "connectionId is required")
		return nil, nil, false
	}
	if len(req.Changes) == 0 {
		writeError(w, http.StatusBadRequest, "changes must name at least one edit")
		return nil, nil, false
	}
	if len(req.Changes) > maxSchemaChanges {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("at most %d changes per plan request", maxSchemaChanges))
		return nil, nil, false
	}
	client, ok := s.clientFor(req.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return nil, nil, false
	}
	return &req, client, true
}

func writePlanError(w http.ResponseWriter, err error) {
	status := http.StatusUnprocessableEntity
	var domain mutationDomainError
	var introspectFail errIntrospection
	var engine errUnsupportedEngine
	switch {
	case errors.As(err, &domain):
		status = http.StatusBadRequest
	case errors.As(err, &engine):
		status = http.StatusUnprocessableEntity
	case errors.As(err, &introspectFail):
		status = http.StatusBadGateway
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		status = http.StatusBadGateway
	default:
		log.Printf("studio: schema plan error: %v", err)
	}
	writeJSON(w, status, map[string]any{"error": sanitizeError(err), "state": "plan-refused"})
}

// verifyPlanConvergence re-introspects after an apply and diffs the desired
// document against it; any residual statement means the catalog is not in
// the planned state (a concurrent change, or a non-converging statement).
func verifyPlanConvergence(ctx context.Context, client *db.Client, desired *db.V2Document) ([]string, error) {
	after, err := client.IntrospectV2(ctx)
	if err != nil {
		return nil, fmt.Errorf("re-introspection failed: %w", err)
	}
	norm, err := client.NewTwinNormalizer(ctx)
	if err != nil {
		return nil, err
	}
	defer norm.Close()
	residual, err := db.DiffV2Document(ctx, desired, after, db.DiffV2Options{AllowDestructive: true, Normalizer: norm})
	if err != nil {
		return nil, err
	}
	return residual.Up, nil
}
