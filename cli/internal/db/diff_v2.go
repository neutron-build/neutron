package db

// Structural diff of two schema documents v2 (M02).
//
// DiffV2Document compares a validated DESIRED document against the
// INTROSPECTED actual document and emits forward/reverse SQL plus warnings,
// or a clear error when a change is not representable faithfully. Rules:
//
//   - Managed scope is the schemas the desired document declares. Objects
//     in other schemas are untouched and reported.
//   - Extension-owned and other opaque objects in the actual document are
//     never modified or dropped, and declaring them as managed desired
//     objects is an error (they keep their B03 protection).
//   - Desired tables whose live counterpart is unrepresentable (opaque
//     unsupported-table) block planning with the introspection reasons.
//   - Drops need opts.AllowDestructive; neutron-internal names and
//     extension/opaque objects are exempt from drops regardless.
//   - Creates are dependency-ordered (enums -> tables topologically by
//     foreign keys, cyclic FK edges deferred to ALTER TABLE) and drops run
//     in reverse dependency order (views -> cyclic FK constraints -> tables
//     -> enums).
//   - Expression-bearing fields (defaults, checks, index predicates and key
//     expressions, view definitions) compare through the optional
//     Normalizer (catalog-aware equivalence). Without one, comparison is
//     strict text and any resulting change is flagged as unverified.
//   - Column order is part of a document (contract §4.1/attnum;
//     introspection never normalizes it away), but a plan cannot change
//     it: PostgreSQL cannot reorder columns without rebuilding the table
//     and appends added ones. A desired table whose matched columns are
//     ordered differently from the live table is noted (ColumnOrderUnplanned)
//     and nothing is planned for the order; added columns are appended.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// DiffV2Options parameterizes DiffV2Document. Renames maps the qualified
// desired column ("schema.table.newcol") to the actual column it renames.
// AllowDestructive is the explicit acknowledgement required before drops of
// managed objects are planned. SnapshotBase relabels the comparison base in
// user-facing errors and warnings: offline snapshot planning (M03) compares
// against a recorded snapshot, not a live catalog, and its messages must
// say so. Live callers leave it unset and every message keeps the
// historical database wording byte-for-byte.
//
// ServerMajor is the connected server's PostgreSQL major version for plans
// that will be applied to that server (db push, live migrate generate); 0
// means unknown (offline snapshot planning, drift reports). With a known
// version the planner refuses statements the server cannot run instead of
// emitting a plan that is certain to fail at apply.
type DiffV2Options struct {
	Renames          map[string]string
	AllowDestructive bool
	Normalizer       V2Normalizer
	SnapshotBase     bool
	ServerMajor      int
}

// InternalMetadataNote ends the plan note for a neutron-internal table
// (migration history): informational, never drift.
const InternalMetadataNote = "is neutron-internal metadata: always left untouched"

// ColumnOrderUnplanned ends the plan note for a table whose declared column
// order differs from the database's: informational, never drift.
const ColumnOrderUnplanned = "PostgreSQL cannot reorder columns without rebuilding the table, so the database keeps its order and nothing is planned for it"

// HasDrift reports whether any plan warning names an object that is out of
// sync with the schema, as opposed to the internal-metadata and
// column-order notes.
func HasDrift(warnings []string) bool {
	for _, w := range warnings {
		if !strings.HasSuffix(w, InternalMetadataNote) && !strings.HasSuffix(w, ColumnOrderUnplanned) {
			return true
		}
	}
	return false
}

// DiffV2Document produces the up/down SQL moving the database described by
// `actual` to `desired`. Both documents must be validated v2 documents.
func DiffV2Document(ctx context.Context, desired, actual *V2Document, opts DiffV2Options) (DiffResult, error) {
	var result DiffResult

	d, err := ModelFromRoot(desired.Root)
	if err != nil {
		return result, err
	}
	a, err := ModelFromRoot(actual.Root)
	if err != nil {
		return result, err
	}

	pl := &v2Planner{
		ctx:              ctx,
		desired:          d,
		actual:           a,
		opts:             opts,
		scope:            map[string]bool{},
		desiredTables:    map[V2Identity]bool{},
		actualTables:     map[V2Identity]bool{},
		desiredEnums:     map[V2Identity]bool{},
		actualEnums:      map[V2Identity]bool{},
		desiredViews:     map[V2Identity]bool{},
		actualViews:      map[V2Identity]bool{},
		normalizedTables: map[V2Identity]*V2Table{},
		normalizedViews:  map[V2Identity]*V2View{},
		twinFailedTables: map[V2Identity]bool{},
		twinFailedElems:  map[V2Identity]map[string]bool{},
		twinFailedViews:  map[V2Identity]bool{},
		unrenamedTables:  map[V2Identity]string{},
		renameBlocked:    map[V2Identity]map[string]string{},
	}

	for _, s := range d.Schemas {
		pl.scope[s.Name] = true
	}
	for _, t := range d.Tables {
		pl.desiredTables[t.Identity] = t.Managed
	}
	for _, e := range d.Enums {
		pl.desiredEnums[e.Identity] = e.Managed
	}
	for _, v := range d.Views {
		pl.desiredViews[v.Identity] = v.Managed
	}
	for _, t := range a.Tables {
		pl.actualTables[t.Identity] = true
	}
	for _, e := range a.Enums {
		pl.actualEnums[e.Identity] = true
	}
	for _, v := range a.Views {
		pl.actualViews[v.Identity] = true
	}

	if err := pl.checkBlockers(); err != nil {
		return pl.result, err
	}
	if err := pl.validateRenames(); err != nil {
		return pl.result, err
	}
	pl.planSchemas()
	if err := pl.planEnums(); err != nil {
		return pl.result, err
	}
	if err := pl.planTables(); err != nil {
		return pl.result, err
	}
	pl.reportOutOfScope()
	pl.reportUnverified()
	return pl.result, nil
}

// v2Planner carries diff state between the per-collection planning passes.
type v2Planner struct {
	ctx     context.Context
	desired V2DocumentModel
	actual  V2DocumentModel
	opts    DiffV2Options
	scope   map[string]bool

	desiredTables map[V2Identity]bool // identity -> managed
	actualTables  map[V2Identity]bool
	desiredEnums  map[V2Identity]bool
	actualEnums   map[V2Identity]bool
	desiredViews  map[V2Identity]bool
	actualViews   map[V2Identity]bool

	result           DiffResult
	upOps            []string
	downOps          []string
	alterUps         []string
	alterDowns       []string
	bufferAlters     bool
	normalizedTables map[V2Identity]*V2Table
	normalizedViews  map[V2Identity]*V2View
	twinFailedTables map[V2Identity]bool
	twinFailedElems  map[V2Identity]map[string]bool
	twinFailedViews  map[V2Identity]bool
	unrenamedTables  map[V2Identity]string            // live text kept its pre-rename names
	renameBlocked    map[V2Identity]map[string]string // element -> what depends on its pre-rename text
	unverified       []string                         // rendered "equivalence not verified" lines
}

func (p *v2Planner) warn(format string, args ...any) {
	p.result.warn(format, args...)
}

// baseNoun names the comparison base in user-facing messages: the live
// database, or the planning-base snapshot for offline snapshot planning.
func (p *v2Planner) baseNoun() string {
	if p.opts.SnapshotBase {
		return "the planning base (snapshot)"
	}
	return "the database"
}

// baseBareNoun is the adjectival form for phrases like "the database table".
func (p *v2Planner) baseBareNoun() string {
	if p.opts.SnapshotBase {
		return "planning-base"
	}
	return "database"
}

// baseTableNoun names the base relation whose column order matters.
func (p *v2Planner) baseTableNoun() string {
	if p.opts.SnapshotBase {
		return "the planning-base table"
	}
	return "the live table"
}

// baseLiveNoun is the bare adjective for order comparisons ("live order").
func (p *v2Planner) baseLiveNoun() string {
	if p.opts.SnapshotBase {
		return "planning-base"
	}
	return "live"
}

func (p *v2Planner) emit(up, down string) {
	if p.bufferAlters {
		// Buffered phase: shared-table alterations wait until view
		// handling decides whether views must drop first (a column type
		// change under a view cannot run in PostgreSQL).
		if up != "" {
			p.alterUps = append(p.alterUps, up)
		}
		if down != "" {
			p.alterDowns = append(p.alterDowns, down)
		}
		return
	}
	p.upOps = append(p.upOps, up)
	if down != "" {
		p.downOps = append(p.downOps, down)
	}
}

func (p *v2Planner) emitRaw(up, down string) {
	p.upOps = append(p.upOps, up)
	if down != "" {
		p.downOps = append(p.downOps, down)
	}
}

// desiredTable returns the desired table for planning, twin-normalized on
// first use so expression comparisons are catalog-canonical. Falls back to
// the document text when normalization is unavailable; comparisons then
// record an unverified note if they detect a textual difference.
func (p *v2Planner) desiredTable(id V2Identity) *V2Table {
	t := p.desired.Table(id)
	if t == nil {
		return nil
	}
	if n, ok := p.normalizedTables[id]; ok {
		return n
	}
	var normalized *V2Table
	if p.opts.Normalizer != nil && tableHasComparableExpressions(t) {
		n, err := p.opts.Normalizer.NormalizeTable(p.ctx, *t)
		var partial *PartialNormalizationError
		switch {
		case err == nil:
			normalized = &n
		case errors.As(err, &partial):
			// Elements normalized one by one; textual differences of
			// the elements that failed stay flagged unverified.
			normalized = &partial.Table
			failed := make(map[string]bool, len(partial.Failed))
			for _, e := range partial.Failed {
				failed[e] = true
			}
			p.twinFailedElems[id] = failed
		default:
			p.twinFailedTables[id] = true
		}
	}
	if normalized == nil {
		normalized = t
	}
	p.normalizedTables[id] = normalized
	return normalized
}

func (p *v2Planner) desiredView(id V2Identity) *V2View {
	v := p.desired.View(id)
	if v == nil {
		return nil
	}
	if n, ok := p.normalizedViews[id]; ok {
		return n
	}
	var normalized *V2View
	if p.opts.Normalizer != nil {
		if n, err := p.opts.Normalizer.NormalizeView(p.ctx, *v); err == nil {
			cp := n
			cp.Identity = v.Identity
			normalized = &cp
		} else {
			p.twinFailedViews[id] = true
		}
	}
	if normalized == nil {
		normalized = v
	}
	p.normalizedViews[id] = normalized
	return normalized
}

func tableHasComparableExpressions(t *V2Table) bool {
	for _, c := range t.Columns {
		if c.Default != nil && (c.Default.Kind == "literal" || c.Default.Kind == "expression") {
			return true
		}
		if c.Generated != nil {
			return true
		}
	}
	for _, con := range t.Constraints {
		if con.Type == "check" && con.Expression != nil {
			return true
		}
	}
	for _, idx := range t.Indexes {
		if idx.Where != nil {
			return true
		}
		for _, k := range idx.Key {
			if k.Expression != nil {
				return true
			}
		}
	}
	return false
}

// checkBlockers rejects desired objects that must never be planned against
// their actual counterpart: extension-owned objects, unrepresentable tables
// and object-kind conflicts (desired table vs actual view and vice versa).
func (p *v2Planner) checkBlockers() error {
	for _, t := range p.desired.Tables {
		if !t.Managed {
			continue
		}
		id := t.Identity
		if o := p.actual.OpaqueEntry("extension-table", id); o != nil {
			return fmt.Errorf("table %s exists in %s as an extension-owned table (owning extension: %s) — extension-owned objects are never managed or modified; remove the table from the schema document", id, p.baseNoun(), o.Owner)
		}
		if o := p.actual.OpaqueEntry("extension-object", id); o != nil {
			return fmt.Errorf("table %s exists in %s as an extension-owned object (owning extension: %s) — extension-owned objects are never managed or modified; remove the table from the schema document", id, p.baseNoun(), o.Owner)
		}
		if o := p.actual.OpaqueEntry("unsupported-table", id); o != nil {
			return fmt.Errorf("table %s carries catalog structure this diff cannot represent faithfully: %s — refusing to claim synchronization until supported", id, o.Reason)
		}
		if o := p.actual.OpaqueEntry("unsupported-object", id); o != nil {
			return fmt.Errorf("object %s exists in %s as an unrepresentable %s — a table with that identity cannot be managed; resolve the conflict manually", id, p.baseNoun(), unsupportedObjectKind(o.Reason))
		}
		if p.actualViews[id] {
			return fmt.Errorf("object %s is a VIEW in %s but a table in the desired schema — resolve the conflict manually", id, p.baseNoun())
		}
		if p.actualEnums[id] {
			return fmt.Errorf("object %s is an enum type in %s but a table in the desired schema — resolve the conflict manually", id, p.baseNoun())
		}
		if isProtectedTableName(t.Identity.Name) {
			return fmt.Errorf("table %s is neutron-internal metadata and is managed automatically — remove it from the schema document; internal tables are never part of a diff or plan", id)
		}
	}
	for _, e := range p.desired.Enums {
		if !e.Managed {
			continue
		}
		id := e.Identity
		if o := p.actual.OpaqueEntry("extension-object", id); o != nil {
			return fmt.Errorf("enum %s exists in %s as an extension-owned type (owning extension: %s) — extension-owned objects are never managed; remove the enum from the schema document", id, p.baseNoun(), o.Owner)
		}
		if p.actualTables[id] {
			return fmt.Errorf("object %s is a table in %s but an enum in the desired schema — resolve the conflict manually", id, p.baseNoun())
		}
		if p.actualViews[id] {
			return fmt.Errorf("object %s is a view in %s but an enum in the desired schema — resolve the conflict manually", id, p.baseNoun())
		}
	}
	for _, v := range p.desired.Views {
		if !v.Managed {
			continue
		}
		id := v.Identity
		if o := p.actual.OpaqueEntry("extension-object", id); o != nil {
			return fmt.Errorf("view %s exists in %s as an extension-owned object (owning extension: %s) — extension-owned objects are never managed; remove the view from the schema document", id, p.baseNoun(), o.Owner)
		}
		if o := p.actual.OpaqueEntry("extension-table", id); o != nil {
			return fmt.Errorf("view %s exists in %s as an extension-owned table (owning extension: %s) — remove the view from the schema document", id, p.baseNoun(), o.Owner)
		}
		if o := p.actual.OpaqueEntry("unsupported-object", id); o != nil {
			return fmt.Errorf("view %s cannot be managed: the %s object with that identity is unrepresentable (%s) — resolve the conflict manually", id, p.baseBareNoun(), o.Reason)
		}
		if o := p.actual.OpaqueEntry("unsupported-table", id); o != nil {
			return fmt.Errorf("view %s exists in %s as an unrepresentable table (%s) — a view with that identity cannot be managed; resolve the conflict manually", id, p.baseNoun(), o.Reason)
		}
		if p.actualTables[id] {
			return fmt.Errorf("object %s is a table in %s but a view in the desired schema — resolve the conflict manually", id, p.baseNoun())
		}
		if p.actualEnums[id] {
			return fmt.Errorf("object %s is an enum type in %s but a view in the desired schema — resolve the conflict manually", id, p.baseNoun())
		}
	}
	return nil
}

func unsupportedObjectKind(reason string) string {
	// Opaque reasons are "<kind> (detail)"; keep the kind phrase.
	if i := strings.Index(reason, " ("); i > 0 {
		return reason[:i]
	}
	if i := strings.IndexByte(reason, ' '); i > 0 {
		return reason[:i]
	}
	return reason
}

// validateRenames checks explicit rename intent: the target column must
// exist in the desired table, the source in the actual table, the source
// must not still be declared in desired (that would be ambiguous), the
// target must not exist in actual (rename collision), and each source and
// target may participate in exactly one rename.
func (p *v2Planner) validateRenames() error {
	seenSource := map[string]string{}
	seenTarget := map[string]string{}
	for target, source := range p.opts.Renames {
		table := V2Identity{Schema: target[:strings.IndexByte(target, '.')], Name: ""}
		rest := target[strings.IndexByte(target, '.')+1:]
		dot := strings.IndexByte(rest, '.')
		if dot < 0 {
			return fmt.Errorf("rename keys must be schema-qualified (schema.table.column), got %q", target)
		}
		table.Name = rest[:dot]
		newCol := rest[dot+1:]

		dt := p.desired.Table(table)
		if dt == nil || !dt.Managed {
			return fmt.Errorf("--rename %s.%s>%s: table %s is not a managed table in the desired schema", table, source, newCol, table)
		}
		if dt.Column(newCol) == nil {
			return fmt.Errorf("--rename %s.%s>%s: target column %q does not exist in the desired schema", table, source, newCol, newCol)
		}
		at := p.actual.Table(table)
		if at == nil {
			return fmt.Errorf("--rename %s.%s>%s: table %s does not exist in %s", table, source, newCol, table, p.baseNoun())
		}
		if at.Column(source) == nil {
			return fmt.Errorf("--rename %s.%s>%s: source column %q does not exist in table %s in %s", table, source, newCol, source, table, p.baseNoun())
		}
		if dt.Column(source) != nil {
			return fmt.Errorf("--rename %s.%s>%s: ambiguous — source column %q is also still declared in the desired schema; a rename replaces the old name", table, source, newCol, source)
		}
		if at.Column(newCol) != nil {
			return fmt.Errorf("--rename %s.%s>%s: ambiguous — target column %q already exists in the %s table", table, source, newCol, newCol, p.baseBareNoun())
		}
		srcKey := table.String() + "." + source
		if prev, dup := seenSource[srcKey]; dup {
			return fmt.Errorf("--rename: column %q is renamed to both %q and %q; a column has one rename", srcKey, prev, newCol)
		}
		seenSource[srcKey] = newCol
		if prev, dup := seenTarget[target]; dup {
			return fmt.Errorf("--rename: two renames claim target %q (%s and %s)", target, prev, source)
		}
		seenTarget[target] = source
	}
	return nil
}

func (p *v2Planner) planSchemas() {
	for _, s := range p.desired.Schemas {
		if p.actualSchemaExists(s.Name) {
			continue
		}
		p.emit(
			fmt.Sprintf("create schema if not exists %s", quoteIdent(s.Name)),
			fmt.Sprintf("-- schema %s was created by this plan; schemas are not dropped automatically", quoteIdent(s.Name)),
		)
		p.warn("schema %q does not exist in %s: it will be created", s.Name, p.baseNoun())
	}
}

func (p *v2Planner) actualSchemaExists(name string) bool {
	for _, s := range p.actual.Schemas {
		if s.Name == name {
			return true
		}
	}
	return false
}

// planEnums creates missing enums, appends newly added values (the only
// ALTER PostgreSQL supports) and collects destructive drops for later
// (after tables). Removal or reordering of values is an error: no faithful
// plan exists.
func (p *v2Planner) planEnums() error {
	for _, de := range p.desired.Enums {
		if !de.Managed {
			continue
		}
		ae := p.actual.Enum(de.Identity)
		if ae == nil {
			values := make([]string, 0, len(de.Values))
			for _, v := range de.Values {
				values = append(values, "'"+strings.ReplaceAll(v, "'", "''")+"'")
			}
			p.emit(
				fmt.Sprintf("create type %s as enum (%s)", qualifiedNameSQL(de.Identity), strings.Join(values, ", ")),
				fmt.Sprintf("drop type if exists %s", qualifiedNameSQL(de.Identity)),
			)
			continue
		}
		if err := planEnumValues(p, de, *ae); err != nil {
			return err
		}
	}
	return nil
}

func planEnumValues(p *v2Planner, de, ae V2EnumDecl) error {
	actualSet := map[string]bool{}
	for _, v := range ae.Values {
		actualSet[v] = true
	}
	for _, v := range ae.Values {
		found := false
		for _, dv := range de.Values {
			if dv == v {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf(
				"enum %s: value %q exists in %s but not in the desired schema — PostgreSQL cannot remove enum values; recreate the type manually (new type + column migration) if this is intentional",
				de.Identity, v, p.baseNoun())
		}
	}
	// Actual values must appear in desired order (subsequence check).
	i := 0
	for j := 0; j < len(de.Values) && i < len(ae.Values); j++ {
		if de.Values[j] == ae.Values[i] {
			i++
		}
	}
	if i != len(ae.Values) {
		return fmt.Errorf(
			"enum %s: desired value order conflicts with the %s type (%s order %v) — PostgreSQL cannot reorder enum values; recreate the type manually if this is intentional",
			de.Identity, p.baseLiveNoun(), p.baseLiveNoun(), ae.Values)
	}
	for idx, v := range de.Values {
		if actualSet[v] {
			continue
		}
		// Find the next live value after this position to anchor BEFORE.
		anchor := ""
		for j := idx + 1; j < len(de.Values); j++ {
			if actualSet[de.Values[j]] {
				anchor = de.Values[j]
				break
			}
		}
		lit := "'" + strings.ReplaceAll(v, "'", "''") + "'"
		stmt := fmt.Sprintf("alter type %s add value %s", qualifiedNameSQL(de.Identity), lit)
		if anchor != "" {
			stmt += fmt.Sprintf(" before '%s'", strings.ReplaceAll(anchor, "'", "''"))
		}
		p.emit(stmt, fmt.Sprintf("-- enum value %q added to %s by this plan; PostgreSQL cannot remove enum values", v, de.Identity))
		note := ""
		if anchor != "" {
			note = " before \"" + anchor + "\""
		}
		p.warn("enum %s: value %q will be added%s (adding enum values is not reversible by generated down SQL)",
			de.Identity, v, note)
		p.result.EnumAdditions = append(p.result.EnumAdditions, EnumAddition{
			Statement: stmt,
			Warning:   p.result.Warnings[len(p.result.Warnings)-1],
		})
	}
	return nil
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

func (p *v2Planner) planTables() error {
	// Creates, topologically ordered; cyclic FK edges deferred.
	var toCreate []V2Identity
	for _, t := range p.desired.Tables {
		if t.Managed && !p.actualTables[t.Identity] {
			toCreate = append(toCreate, t.Identity)
		}
	}
	sort.Slice(toCreate, func(i, j int) bool { return toCreate[i].String() < toCreate[j].String() })
	ordered, deferredFKs := orderV2Creates(&p.desired, toCreate)
	for _, id := range ordered {
		t := p.desired.Table(id)
		// Sequence defaults (serial columns) reference a sequence the
		// document implies but never declares as an object: create it
		// before the table (the default's nextval resolves at create
		// time) and attach ownership after, so the sequence drops with
		// its column exactly like a native serial sequence.
		var ownedSeqs []struct{ seq, stmt string }
		for _, c := range t.Columns {
			if c.Default != nil && c.Default.Kind == "sequence" && c.Default.Sequence != nil {
				seq := *c.Default.Sequence
				seqQ := qualifiedNameSQL(seq)
				// Idempotent: the sequence may pre-exist as a standalone
				// (or extension-owned) object the document only
				// references. Ownership is attached only when the sequence
				// is not already an inventoried opaque object — standalone
				// sequences stay standalone and never drop with the table.
				p.emitRaw(fmt.Sprintf("create sequence if not exists %s", seqQ), fmt.Sprintf("-- sequence %s: no down statement (it may predate this plan)", seqQ))
				preexisting := p.actual.OpaqueEntry("unsupported-object", seq) != nil || p.actual.OpaqueEntry("extension-object", seq) != nil
				if !preexisting {
					ownedSeqs = append(ownedSeqs, struct{ seq, stmt string }{seqQ, fmt.Sprintf("alter sequence %s owned by %s.%s", seqQ, qualifiedNameSQL(id), quoteIdent(c.Name))})
				}
			}
		}
		ddl, err := createV2TableSQL(*t, deferredFKs[id])
		if err != nil {
			return err
		}
		p.emitRaw(ddl, fmt.Sprintf("drop table if exists %s", qualifiedNameSQL(id)))
		for _, own := range ownedSeqs {
			p.emitRaw(own.stmt, fmt.Sprintf("alter sequence %s owned by none", own.seq))
		}
		for _, idx := range t.Indexes {
			idxDDL, err := createV2IndexSQL(*t, idx)
			if err != nil {
				return err
			}
			p.emitRaw(idxDDL, fmt.Sprintf("drop index if exists %s", qualifiedNameSQL(idx.Identity)))
		}
	}
	for _, id := range ordered {
		t := p.desired.Table(id)
		for _, conName := range deferredFKs[id] {
			con := t.Constraint(conName)
			if con == nil {
				continue
			}
			stmt, err := addV2ConstraintSQL(*t, *con)
			if err != nil {
				return err
			}
			p.emitRaw(stmt, fmt.Sprintf("alter table %s drop constraint if exists %s", qualifiedNameSQL(id), quoteIdent(conName)))
		}
	}

	// Shared-table alterations are buffered: when any exist, dependent
	// views drop first and (re)create after (a column type change under a
	// view cannot run in PostgreSQL).
	p.bufferAlters = true
	if err := p.planSharedTables(); err != nil {
		return err
	}
	p.bufferAlters = false
	p.planViewsAroundAlters()

	// Destructive drops, reverse dependency order.
	p.planTableDrops()
	return nil
}

// planViewsAroundAlters emits view changes positioned around the buffered
// table alterations. With alterations present, ANY in-scope view may sit on
// an altered column (PostgreSQL refuses type changes under a view), so
// every existing in-scope view drops first and the desired ones (re)create
// after. Unchanged views recreated this way are WARNED about explicitly:
// privileges, comments and other unmodeled properties do not survive the
// round-trip. Without alterations, only genuinely changed/new/removed
// views are planned.
func (p *v2Planner) planViewsAroundAlters() {
	type stmtPair struct{ up, down string }
	dropIds := map[V2Identity]bool{}
	createIds := map[V2Identity]bool{}
	var drops, creates []stmtPair

	for _, dv := range p.desired.Views {
		if !dv.Managed {
			continue
		}
		av := p.actual.View(dv.Identity)
		dvn := p.desiredView(dv.Identity)
		if av == nil {
			if p.opts.Normalizer != nil && p.twinFailedViews[dv.Identity] {
				p.warn("view %s definition could not be normalized against the catalog; it will be created verbatim — schema-qualify relation references in the definition (introspection always emits schema-qualified SQL)", dv.Identity)
			}
			creates = append(creates, stmtPair{createV2ViewSQL(*dvn), fmt.Sprintf("drop view if exists %s", qualifiedNameSQL(dv.Identity))})
			createIds[dv.Identity] = true
			continue
		}
		equal := p.viewEqual(*dvn, *av)
		if equal && len(p.alterUps) == 0 {
			continue
		}
		if !dropIds[dv.Identity] {
			drops = append(drops, stmtPair{fmt.Sprintf("drop view if exists %s", qualifiedNameSQL(dv.Identity)), createV2ViewSQL(*av)})
			dropIds[dv.Identity] = true
		}
		if !createIds[dv.Identity] {
			creates = append(creates, stmtPair{createV2ViewSQL(*dvn), fmt.Sprintf("drop view if exists %s", qualifiedNameSQL(dv.Identity))})
			createIds[dv.Identity] = true
		}
		if !equal {
			p.warn("view %s exists with a different definition; it will be dropped and recreated", dv.Identity)
		} else {
			p.warn("view %s is unchanged but is dropped and recreated around the table alterations in this plan; privileges, comments and other properties not represented in the schema document do not survive the round-trip — re-apply them if needed", dv.Identity)
		}
	}
	for _, av := range p.actual.Views {
		if !p.scope[av.Identity.Schema] || p.desiredViews[av.Identity] {
			continue
		}
		if !p.opts.AllowDestructive {
			p.warn("view %s exists in %s but not in the schema: left untouched (dropping requires explicit destructive acknowledgement, --allow-destructive)", av.Identity, p.baseNoun())
			continue
		}
		p.warn("view %s will be dropped", av.Identity)
		if !dropIds[av.Identity] {
			drops = append(drops, stmtPair{fmt.Sprintf("drop view if exists %s", qualifiedNameSQL(av.Identity)), createV2ViewSQL(av)})
			dropIds[av.Identity] = true
		}
	}

	emitAll := func(pairs []stmtPair) {
		for _, s := range pairs {
			p.emitRaw(s.up, s.down)
		}
	}
	emitAll(drops)
	for i, up := range p.alterUps {
		down := ""
		if i < len(p.alterDowns) {
			down = p.alterDowns[i]
		}
		p.emitRaw(up, down)
	}
	emitAll(creates)
}

func (c V2Column) HasDefaultLike() bool {
	return c.Default != nil
}

// planSharedTables diffs tables present in both documents. Column-level
// passes run globally in phases (renames -> adds -> attribute changes ->
// constraint drops/adds -> column drops) so cross-table dependencies (FKs
// onto renamed/re-typed columns) settle before dependent statements run.
func (p *v2Planner) planSharedTables() error {
	var shared []V2Table
	for _, t := range p.desired.Tables {
		if !t.Managed || !p.actualTables[t.Identity] {
			continue
		}
		if isProtectedTableName(t.Identity.Name) {
			continue // unreachable for validated input; belt and braces
		}
		shared = append(shared, t)
	}
	sort.Slice(shared, func(i, j int) bool { return shared[i].Identity.String() < shared[j].Identity.String() })

	// Column order is informational (M08 review-2): PostgreSQL cannot
	// reorder columns without rebuilding the table, and appends added ones.
	// No document can tell a column a later plan appended but declared
	// elsewhere from a wish to reorder, and refusing either is a permanent
	// dead end, so an order difference of the MATCHED columns (present in
	// both documents, renames resolved) is noted and never planned; added
	// columns are appended.
	for _, dt := range shared {
		at := p.actual.Table(dt.Identity)
		actualPos := make(map[string]int, len(at.Columns))
		for i, c := range at.Columns {
			actualPos[c.Name] = i
		}
		positions := make([]int, 0, len(dt.Columns))
		for _, dc := range dt.Columns {
			acName := p.matchedActualName(dt.Identity, at, dc.Name)
			if acName == "" {
				continue // added column: appended by the plan, order free
			}
			positions = append(positions, actualPos[acName])
		}
		ordered := true
		for i := 1; i < len(positions); i++ {
			if positions[i] < positions[i-1] {
				ordered = false
				break
			}
		}
		if !ordered {
			p.warn("table %s: the desired column order differs from %s (attnum order %v) — %s",
				dt.Identity, p.baseTableNoun(), columnNames(*at), ColumnOrderUnplanned)
		}
	}

	for _, dt := range shared {
		p.renameActual(dt.Identity)
	}
	p.renameStructure()

	// Phase 1: renames.
	for _, dt := range shared {
		for _, dc := range dt.Columns {
			key := dt.Identity.String() + "." + dc.Name
			old, ok := p.opts.Renames[key]
			if !ok {
				continue
			}
			p.emit(
				fmt.Sprintf("alter table %s rename column %s to %s", qualifiedNameSQL(dt.Identity), quoteIdent(old), quoteIdent(dc.Name)),
				fmt.Sprintf("alter table %s rename column %s to %s", qualifiedNameSQL(dt.Identity), quoteIdent(dc.Name), quoteIdent(old)),
			)
		}
	}

	// Phase 2: added columns.
	for _, dt := range shared {
		at := p.actual.Table(dt.Identity)
		dtn := p.desiredTable(dt.Identity)
		for _, dc := range dtn.Columns {
			if p.matchedActualName(dt.Identity, at, dc.Name) != "" {
				continue
			}
			ddl, err := v2ColumnDDL(dc)
			if err != nil {
				return err
			}
			p.emit(
				fmt.Sprintf("alter table %s add column %s", qualifiedNameSQL(dt.Identity), ddl),
				fmt.Sprintf("alter table %s drop column if exists %s", qualifiedNameSQL(dt.Identity), quoteIdent(dc.Name)),
			)
			if dc.NotNull && !dc.HasDefaultLike() {
				p.warn("table %s: adding not-null column %q without a default fails on tables with rows", dt.Identity, dc.Name)
			}
		}
	}

	// Phase 3: attribute changes on matched columns.
	for _, dt := range shared {
		at := p.actual.Table(dt.Identity)
		dtn := p.desiredTable(dt.Identity)
		for _, dc := range dtn.Columns {
			acName := p.matchedActualName(dt.Identity, at, dc.Name)
			if acName == "" {
				continue
			}
			ac := at.Column(acName)
			if err := p.planColumnAttributes(dt.Identity, dc, *ac); err != nil {
				return err
			}
		}
	}

	// Phase 4/5: constraint drops then adds.
	for _, dt := range shared {
		at := p.actual.Table(dt.Identity)
		if err := p.planConstraintChanges(dt.Identity, p.desiredTable(dt.Identity), at); err != nil {
			return err
		}
	}

	// Phase 6: dropped columns (destructive).
	for _, dt := range shared {
		at := p.actual.Table(dt.Identity)
		dtn := p.desiredTable(dt.Identity)
		for _, ac := range at.Columns {
			if dtn.Column(ac.Name) != nil {
				continue
			}
			renamedAway := false
			for target, source := range p.opts.Renames {
				if source == ac.Name && strings.HasPrefix(target, dt.Identity.String()+".") {
					renamedAway = true
					break
				}
			}
			if renamedAway {
				continue
			}
			if !p.opts.AllowDestructive {
				p.warn("table %s: column %q exists in %s but not in the schema: left untouched (dropping requires explicit destructive acknowledgement, --allow-destructive)", dt.Identity, ac.Name, p.baseNoun())
				continue
			}
			if ac.Generated != nil {
				p.blockOnRename(dt.Identity, v2GeneratedElement(ac.Name), fmt.Sprintf("generated column %s is dropped, and its down statement re-adds it as %q", ac.Name, ac.Generated.Expression), ac.Generated.Expression)
			}
			p.warn("table %s: column %q will be dropped (data lost unless it is a rename — see --rename)", dt.Identity, ac.Name)
			acDDL, err := v2ColumnDDL(ac)
			if err != nil {
				acDDL = "-- column " + quoteIdent(ac.Name) + " (unrepresentable type; no down statement)"
			}
			p.emit(
				fmt.Sprintf("alter table %s drop column if exists %s", qualifiedNameSQL(dt.Identity), quoteIdent(ac.Name)),
				fmt.Sprintf("alter table %s add column %s", qualifiedNameSQL(dt.Identity), acDDL),
			)
		}
	}

	// Indexes on shared tables.
	for _, dt := range shared {
		at := p.actual.Table(dt.Identity)
		if err := p.planIndexChanges(dt.Identity, p.desiredTable(dt.Identity), at); err != nil {
			return err
		}
	}
	return p.renameRefusal()
}

// renameActual replaces a renamed table's live expressions with the
// catalog's deparse after the planned RENAME COLUMNs: PostgreSQL rewrites
// generation expressions, checks and index definitions on rename, so a
// rename alone compares equal and a real change still differs. Down
// statements run before the renames are reverted and need the renamed
// spelling too. Without a normalizer (offline snapshot planning) or when
// the twin fails, the live text keeps its old names, and the plan is
// refused if it depends on text that may name a renamed column
// (blockOnRename).
func (p *v2Planner) renameActual(table V2Identity) {
	prefix := table.String() + "."
	renames := map[string]string{}
	var described []string
	for target, source := range p.opts.Renames {
		if strings.HasPrefix(target, prefix) {
			renames[source] = target[len(prefix):]
			described = append(described, source+" to "+target[len(prefix):])
		}
	}
	at := p.actual.Table(table)
	if len(renames) == 0 || at == nil || !tableHasComparableExpressions(at) {
		return
	}
	if p.opts.Normalizer != nil {
		if renamed, err := p.opts.Normalizer.RenameTable(p.ctx, *at, renames); err == nil {
			*at = renamed
			return
		}
	}
	sort.Strings(described)
	p.unrenamedTables[table] = strings.Join(described, ", ")
}

// renameStructure maps the column-name lists of the live model through the
// planned renames: constraint columns, foreign-key referenced columns (by
// the referenced table's renames), index key columns and INCLUDE lists.
// PostgreSQL rewrites all of them on RENAME COLUMN, and every down
// statement built from them runs before the renames are reverted, so it
// must name the new columns. These are structural lists, not expression
// text. Comparisons are unaffected: they translate old names to new ones,
// and a new name is never a live name (validateRenames).
func (p *v2Planner) renameStructure() {
	if len(p.opts.Renames) == 0 {
		return
	}
	mapped := func(table V2Identity, cols []string) []string {
		if cols == nil {
			return nil
		}
		out := make([]string, len(cols))
		for i, c := range cols {
			out[i] = p.actualToDesiredName(table, c)
		}
		return out
	}
	for ti := range p.actual.Tables {
		t := &p.actual.Tables[ti]
		t.Constraints = append([]V2Constraint(nil), t.Constraints...)
		for ci := range t.Constraints {
			con := &t.Constraints[ci]
			con.Columns = mapped(t.Identity, con.Columns)
			if con.References != nil {
				ref := *con.References
				ref.Columns = mapped(ref.Table, ref.Columns)
				con.References = &ref
			}
		}
		t.Indexes = append([]V2Index(nil), t.Indexes...)
		for ii := range t.Indexes {
			idx := &t.Indexes[ii]
			idx.Key = append([]V2IndexKeyPart(nil), idx.Key...)
			for ki := range idx.Key {
				if c := idx.Key[ki].Column; c != nil {
					name := p.actualToDesiredName(t.Identity, *c)
					idx.Key[ki].Column = &name
				}
			}
			idx.Include = mapped(t.Identity, idx.Include)
		}
	}
}

// blockOnRename records that the plan depends on an element of a table
// whose live text still predates its renames (no catalog, or the rename
// copy failed): a difference cannot be told from the rename, and a down
// statement carrying the text would name the old column before the rename
// is reverted. texts are the element's live expression texts; the element
// is recorded only when one of them may name a renamed column
// (textMayNameRename). planSharedTables refuses the plan when any is
// recorded.
func (p *v2Planner) blockOnRename(table V2Identity, element, what string, texts ...string) bool {
	if p.unrenamedTables[table] == "" {
		return false
	}
	names := false
	for _, text := range texts {
		if p.textMayNameRename(table, text) {
			names = true
			break
		}
	}
	if !names {
		return false
	}
	if p.renameBlocked[table] == nil {
		p.renameBlocked[table] = map[string]string{}
	}
	if _, seen := p.renameBlocked[table][element]; !seen {
		p.renameBlocked[table][element] = what
	}
	return true
}

// textMayNameRename reports whether live expression text of a table may
// name one of the table's renamed columns under its old name. It reads the
// text's identifiers as PostgreSQL does (quoted ones exactly, unquoted ones
// folded to lower case; string literals and comments are skipped). The
// table's own name counts too: a whole-row reference (t.*) carries every
// column, and its meaning can depend on their names. Text it cannot read
// counts as naming one. A rename leaves text that names none of these
// unchanged, so it compares and reverts as written.
func (p *v2Planner) textMayNameRename(table V2Identity, text string) bool {
	idents, ok := sqlIdentifiers(text)
	if !ok {
		return true
	}
	prefix := table.String() + "."
	for _, ident := range idents {
		// PostgreSQL truncates identifiers to 63 bytes, so a longer
		// spelling names the same column.
		ident = truncateIdentifier(ident)
		if ident == table.Name {
			return true
		}
		for target, source := range p.opts.Renames {
			if source == ident && strings.HasPrefix(target, prefix) {
				return true
			}
		}
	}
	return false
}

// truncateIdentifier cuts an identifier to PostgreSQL's 63-byte limit
// (NAMEDATALEN - 1) at a UTF-8 boundary, as the server does.
func truncateIdentifier(ident string) string {
	const maxIdentifierBytes = 63
	if len(ident) <= maxIdentifierBytes {
		return ident
	}
	n := maxIdentifierBytes
	for n > 0 && !utf8.RuneStart(ident[n]) {
		n--
	}
	return ident[:n]
}

// sqlIdentifiers returns the identifiers of SQL expression text: quoted
// identifiers unescaped, unquoted ones (keywords and function names
// included) with ASCII letters folded to lower case, as PostgreSQL's lexer
// reads them. String literals (escape, bit, hex and dollar-quoted ones
// too) and comments contribute nothing. ok is false for text it does not
// fully read: an unterminated quote or comment, a Unicode-escaped string or
// identifier (U&), a parameter ($1), or a number run into an identifier.
func sqlIdentifiers(text string) (idents []string, ok bool) {
	identStart := func(c byte) bool {
		return c == '_' || c >= 0x80 || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
	}
	identPart := func(c byte) bool {
		return identStart(c) || c == '$' || (c >= '0' && c <= '9')
	}
	digit := func(c byte) bool { return c >= '0' && c <= '9' }
	// skipString returns the index after the literal opening at i.
	skipString := func(i int, escapes bool) (int, bool) {
		for j := i + 1; j < len(text); j++ {
			switch {
			case escapes && text[j] == '\\':
				j++
			case text[j] == '\'':
				if j+1 < len(text) && text[j+1] == '\'' {
					j++
					continue
				}
				return j + 1, true
			}
		}
		return 0, false
	}
	for i := 0; i < len(text); {
		c := text[i]
		switch {
		case c == '\'':
			end, ok := skipString(i, false)
			if !ok {
				return nil, false
			}
			i = end
		case c == '"':
			var b strings.Builder
			j := i + 1
			for {
				if j >= len(text) {
					return nil, false
				}
				if text[j] == '"' {
					if j+1 < len(text) && text[j+1] == '"' {
						b.WriteByte('"')
						j += 2
						continue
					}
					break
				}
				b.WriteByte(text[j])
				j++
			}
			idents = append(idents, b.String())
			i = j + 1
		case c == '-' && i+1 < len(text) && text[i+1] == '-':
			for i < len(text) && text[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(text) && text[i+1] == '*':
			depth := 0
			for {
				if i+1 >= len(text) {
					return nil, false
				}
				if text[i] == '/' && text[i+1] == '*' {
					depth++
					i += 2
					continue
				}
				if text[i] == '*' && text[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
					continue
				}
				i++
			}
		case c == '$':
			j := i + 1
			if j < len(text) && digit(text[j]) {
				return nil, false
			}
			for j < len(text) && text[j] != '$' {
				if !identPart(text[j]) || (j == i+1 && !identStart(text[j])) {
					return nil, false
				}
				j++
			}
			if j >= len(text) {
				return nil, false
			}
			tag := text[i : j+1]
			end := strings.Index(text[j+1:], tag)
			if end < 0 {
				return nil, false
			}
			i = j + 1 + end + len(tag)
		case digit(c) || (c == '.' && i+1 < len(text) && digit(text[i+1])):
			j := i
			if c == '0' && i+1 < len(text) && strings.ContainsRune("xXoObB", rune(text[i+1])) {
				j += 2
				for j < len(text) && (text[j] == '_' || digit(text[j]) || (text[j] >= 'a' && text[j] <= 'f') || (text[j] >= 'A' && text[j] <= 'F')) {
					j++
				}
			} else {
				for j < len(text) && (digit(text[j]) || text[j] == '_' || text[j] == '.') {
					j++
				}
				if j < len(text) && (text[j] == 'e' || text[j] == 'E') {
					k := j + 1
					if k < len(text) && (text[k] == '+' || text[k] == '-') {
						k++
					}
					if k < len(text) && digit(text[k]) {
						j = k
						for j < len(text) && (digit(text[j]) || text[j] == '_') {
							j++
						}
					}
				}
			}
			if j < len(text) && identPart(text[j]) {
				return nil, false
			}
			i = j
		case identStart(c):
			j := i
			for j < len(text) && identPart(text[j]) {
				j++
			}
			word := text[i:j]
			if j < len(text) && text[j] == '\'' && len(word) == 1 && strings.ContainsRune("eEbBxXnN", rune(word[0])) {
				end, ok := skipString(j, word == "e" || word == "E")
				if !ok {
					return nil, false
				}
				i = end
				continue
			}
			if (word == "u" || word == "U") && j < len(text) && text[j] == '&' {
				return nil, false
			}
			idents = append(idents, strings.Map(func(r rune) rune {
				if r >= 'A' && r <= 'Z' {
					return r + ('a' - 'A')
				}
				return r
			}, word))
			i = j
		default:
			i++
		}
	}
	return idents, true
}

// renameRefusal is the error for the tables blockOnRename recorded, or nil.
func (p *v2Planner) renameRefusal() error {
	if len(p.renameBlocked) == 0 {
		return nil
	}
	tables := make([]V2Identity, 0, len(p.renameBlocked))
	for t := range p.renameBlocked {
		tables = append(tables, t)
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].String() < tables[j].String() })
	var msgs []string
	for _, table := range tables {
		var items []string
		for _, what := range p.renameBlocked[table] {
			items = append(items, "  "+what)
		}
		sort.Strings(items)
		var fix string
		if p.opts.Normalizer != nil {
			// Worded for the CLI and Studio alike: Studio has no flags.
			fix = fmt.Sprintf("it could not be compared under the rename on this database. Rename the column by hand first: %s (PostgreSQL rewrites the expressions that reference it), then plan the remaining changes again, leaving out the rename of %s (the database already holds the new name; keep any other renames)", strings.Join(p.renameStatements(table), "; "), p.unrenamedTables[table])
		} else {
			// --mode live is no fix here: a snapshot plan with a rename
			// always has a chain, and the runner refuses a migration
			// without a snapshot in it. Two offline migrations never
			// compare text across the rename.
			fix = fmt.Sprintf("offline planning has no catalog to compare it under the rename. Plan it as two migrations instead: first generate one that keeps %s and leaves out the elements listed below, without --rename (removing a generated column needs --allow-destructive and recomputes its values when it is added back), then one with the --rename and the elements you keep written with the new name (a re-added column is placed last, so declare it last)", p.oldNamesKept(table))
		}
		msgs = append(msgs, fmt.Sprintf(
			"table %s: the plan depends on expression text in %s that predates the rename of %s. PostgreSQL rewrites that text on RENAME COLUMN, so without comparing it under the rename a rename alone cannot be told from a change, and a down statement carrying it would name the old column before the rename is reverted. The plan is refused; %s:\n%s",
			table, p.baseNoun(), p.unrenamedTables[table], fix, strings.Join(items, "\n")))
	}
	return errors.New(strings.Join(msgs, "\n"))
}

// oldNamesKept names a table's renamed columns under their old names, for
// the first migration of the offline fix: the column stays, so its data
// does.
func (p *v2Planner) oldNamesKept(table V2Identity) string {
	prefix := table.String() + "."
	var olds []string
	for target, source := range p.opts.Renames {
		if strings.HasPrefix(target, prefix) {
			olds = append(olds, source)
		}
	}
	sort.Strings(olds)
	if len(olds) == 1 {
		return "the column under its old name (" + olds[0] + ")"
	}
	return "the columns under their old names (" + strings.Join(olds, ", ") + ")"
}

// renameStatements are the planned RENAME COLUMN statements of a table,
// sorted.
func (p *v2Planner) renameStatements(table V2Identity) []string {
	prefix := table.String() + "."
	var stmts []string
	for target, source := range p.opts.Renames {
		if strings.HasPrefix(target, prefix) {
			stmts = append(stmts, fmt.Sprintf("alter table %s rename column %s to %s", qualifiedNameSQL(table), quoteIdent(source), quoteIdent(target[len(prefix):])))
		}
	}
	sort.Strings(stmts)
	return stmts
}

func columnNames(t V2Table) []string {
	out := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		out = append(out, c.Name)
	}
	return out
}

// actualToDesiredName translates an actual column name to its desired
// spelling for a table: PostgreSQL automatically rewrites constraint
// columns, index columns and foreign-key references when a column is
// renamed, so after a planned rename the live names are the DESIRED ones —
// comparisons must translate, not flag drift.
func (p *v2Planner) actualToDesiredName(table V2Identity, actualName string) string {
	for target, source := range p.opts.Renames {
		if source == actualName && strings.HasPrefix(target, table.String()+".") {
			return target[len(table.String())+1:]
		}
	}
	return actualName
}

// matchedActualName resolves which actual column a desired column pairs
// with: its rename source when renamed, otherwise the same name.
func (p *v2Planner) matchedActualName(table V2Identity, actual *V2Table, desiredCol string) string {
	if old, ok := p.opts.Renames[table.String()+"."+desiredCol]; ok {
		if actual.Column(old) != nil {
			return old
		}
	}
	if actual.Column(desiredCol) != nil {
		return desiredCol
	}
	return ""
}

// planColumnAttributes plans type, default and nullability transitions for
// one matched column pair. Each up statement carries its own inverse in the
// same emit, so the reversed down list replays correctly.
func (p *v2Planner) planColumnAttributes(table V2Identity, dc, ac V2Column) error {
	tq := qualifiedNameSQL(table)
	cq := quoteIdent(dc.Name)
	typeChanged := !dc.Type.SameAs(ac.Type)

	// Generated-column transitions (Q07c). PostgreSQL cannot attach a
	// generation expression to an existing column; converting away from
	// generated keeps the computed values as plain data and is explicitly
	// irreversible (no down statement can restore the expression).
	switch {
	case dc.Generated != nil && ac.Generated == nil:
		return fmt.Errorf(
			"table %s: column %q cannot become a generated column — PostgreSQL cannot attach a generation expression to an existing column; add a new generated column and backfill instead",
			table, dc.Name)
	case dc.Generated == nil && ac.Generated != nil:
		p.warn("table %s: column %q drops its generation expression; the last computed values are kept as plain data and the expression is lost (irreversible)", table, dc.Name)
		p.emit(
			fmt.Sprintf("alter table %s alter column %s drop expression", tq, cq),
			fmt.Sprintf("-- column %s: no down statement — PostgreSQL cannot attach a generation expression to an existing column (was: generated always as (%s) stored)", cq, ac.Generated.Expression),
		)
	case dc.Generated != nil && ac.Generated != nil:
		if !p.textEqual(table, v2GeneratedElement(dc.Name), "column "+dc.Name+" generation expression", &dc.Generated.Expression, &ac.Generated.Expression) && p.renameBlocked[table][v2GeneratedElement(dc.Name)] == "" {
			if p.opts.ServerMajor > 0 && p.opts.ServerMajor < SetExpressionMinServerMajor {
				if !p.comparisonVerified(table, v2GeneratedElement(dc.Name)) {
					// No catalog oracle for this expression: the difference
					// may be spelling only. Say so, and carry the unverified
					// notes the refusal would otherwise drop.
					fix := fmt.Sprintf("If the expression is unchanged, write it as %s spells it; if it changed, upgrade the server to PostgreSQL %d+, or replace the column explicitly in two steps: remove it from the schema and apply with --allow-destructive (its stored values are dropped), then add it back with the new expression (values are recomputed; a re-added column is placed last, so declare it last)",
						p.baseNoun(), SetExpressionMinServerMajor)
					return fmt.Errorf(
						"table %s: generated column %q could not be verified: the schema writes its expression %q and %s holds %q, and without a catalog comparison the difference may be spelling only. A real change needs ALTER COLUMN ... SET EXPRESSION (PostgreSQL %d+), which the connected PostgreSQL %d cannot run, so the plan is refused. %s\n%s",
						table, dc.Name, dc.Generated.Expression, p.baseNoun(), ac.Generated.Expression, SetExpressionMinServerMajor, p.opts.ServerMajor, fix, p.unverifiedNotes())
				}
				return fmt.Errorf(
					"table %s: generated column %q changes its expression, which needs ALTER COLUMN ... SET EXPRESSION (PostgreSQL %d+); the connected server is PostgreSQL %d, so the plan would fail. Upgrade the server to PostgreSQL %d+, or replace the column explicitly in two steps: remove it from the schema and apply with --allow-destructive (its stored values are dropped), then add it back with the new expression (values are recomputed; a re-added column is placed last, so declare it last)",
					table, dc.Name, SetExpressionMinServerMajor, p.opts.ServerMajor, SetExpressionMinServerMajor)
			}
			p.warn("table %s: generated column %q changes its expression via ALTER COLUMN ... SET EXPRESSION (requires PostgreSQL %d+) — the table is rewritten to recompute values", table, dc.Name, SetExpressionMinServerMajor)
			p.emit(
				fmt.Sprintf("alter table %s alter column %s set expression as (%s)", tq, cq, dc.Generated.Expression),
				fmt.Sprintf("alter table %s alter column %s set expression as (%s)", tq, cq, ac.Generated.Expression),
			)
		}
	}
	if typeChanged && (dc.Generated != nil || ac.Generated != nil) {
		return fmt.Errorf(
			"table %s: generated column %q cannot change type — PostgreSQL requires dropping and re-adding the column; plan it explicitly",
			table, dc.Name)
	}

	if typeChanged {
		newType, err := v2TypeDDL(dc.Type)
		if err != nil {
			return err
		}
		oldType, _ := v2TypeDDL(ac.Type)
		p.warn("table %s: column %q type changes %s -> %s via USING cast; verify values convert", table, dc.Name, oldType, newType)
		if ac.Default != nil {
			p.emit(
				fmt.Sprintf("alter table %s alter column %s drop default", tq, cq),
				fmt.Sprintf("alter table %s alter column %s set default %s", tq, cq, defaultSQL(*ac.Default)),
			)
		}
		p.emit(
			fmt.Sprintf("alter table %s alter column %s type %s using %s::%s", tq, cq, newType, cq, newType),
			fmt.Sprintf("alter table %s alter column %s type %s using %s::%s", tq, cq, oldType, cq, oldType),
		)
		if dc.Default != nil {
			if dc.Default.Kind == "identity" {
				p.emit(
					fmt.Sprintf("alter table %s alter column %s add %s", tq, cq, identityClauseSQL(*dc.Default)),
					fmt.Sprintf("alter table %s alter column %s drop identity", tq, cq),
				)
			} else {
				p.emit(
					fmt.Sprintf("alter table %s alter column %s set default %s", tq, cq, defaultSQL(*dc.Default)),
					fmt.Sprintf("alter table %s alter column %s drop default", tq, cq),
				)
			}
		}
	} else if !p.defaultsEqual(table, dc.Name, dc.Default, ac.Default) {
		if err := p.planDefaultChange(table, dc, ac); err != nil {
			return err
		}
	}

	if dc.NotNull != ac.NotNull {
		if dc.NotNull {
			p.emit(
				fmt.Sprintf("alter table %s alter column %s set not null", tq, cq),
				fmt.Sprintf("alter table %s alter column %s drop not null", tq, cq),
			)
		} else {
			p.emit(
				fmt.Sprintf("alter table %s alter column %s drop not null", tq, cq),
				fmt.Sprintf("alter table %s alter column %s set not null", tq, cq),
			)
		}
	}
	return nil
}

// planDefaultChange plans default transitions (no type change involved):
// none/plain-default/identity in any direction.
func (p *v2Planner) planDefaultChange(table V2Identity, dc, ac V2Column) error {
	tq := qualifiedNameSQL(table)
	cq := quoteIdent(dc.Name)
	dd, ad := dc.Default, ac.Default

	setDefault := func(newDef *V2ColumnDefault, inverse string) {
		if newDef.Kind == "sequence" && newDef.Sequence != nil {
			// The sequence is implied by the default, never declared as an
			// object; it may not exist yet on an altered table. If-not-
			// exists, no down statement (a pre-existing sequence must not
			// be dropped by rolling this plan back).
			seqQ := qualifiedNameSQL(*newDef.Sequence)
			p.emitRaw(fmt.Sprintf("create sequence if not exists %s", seqQ), fmt.Sprintf("-- sequence %s: no down statement (it may predate this plan)", seqQ))
			// Mirror the create-table path: a sequence this plan had to
			// create becomes OWNED BY the column, so introspection reads
			// it as implied by the default (not a standalone unsupported
			// object) and it drops with the table. Pre-existing
			// inventoried opaque sequences stay standalone (M02).
			preexisting := p.actual.OpaqueEntry("unsupported-object", *newDef.Sequence) != nil || p.actual.OpaqueEntry("extension-object", *newDef.Sequence) != nil
			if !preexisting {
				p.emitRaw(fmt.Sprintf("alter sequence %s owned by %s.%s", seqQ, tq, cq), fmt.Sprintf("alter sequence %s owned by none", seqQ))
			}
		}
		p.emit(
			fmt.Sprintf("alter table %s alter column %s set default %s", tq, cq, defaultSQL(*newDef)),
			inverse,
		)
	}
	inverseOfActual := func() string {
		// inverse of "set default <new>" is restoring the actual default
		if ad == nil {
			return fmt.Sprintf("alter table %s alter column %s drop default", tq, cq)
		}
		if ad.Kind == "identity" {
			return fmt.Sprintf("alter table %s alter column %s drop default", tq, cq)
		}
		return fmt.Sprintf("alter table %s alter column %s set default %s", tq, cq, defaultSQL(*ad))
	}

	switch {
	case dd == nil && ad == nil:
		return nil
	case dd == nil:
		if ad.Kind == "identity" {
			p.emit(
				fmt.Sprintf("alter table %s alter column %s drop identity", tq, cq),
				fmt.Sprintf("alter table %s alter column %s add %s", tq, cq, identityClauseSQL(*ad)),
			)
		} else {
			p.emit(
				fmt.Sprintf("alter table %s alter column %s drop default", tq, cq),
				fmt.Sprintf("alter table %s alter column %s set default %s", tq, cq, defaultSQL(*ad)),
			)
		}
	case ad == nil:
		if dd.Kind == "identity" {
			p.emit(
				fmt.Sprintf("alter table %s alter column %s add %s", tq, cq, identityClauseSQL(*dd)),
				fmt.Sprintf("alter table %s alter column %s drop identity", tq, cq),
			)
		} else {
			setDefault(dd, inverseOfActual())
		}
	case ad.Kind == "identity" && dd.Kind == "identity":
		if *ad.Generated != *dd.Generated {
			p.emit(
				fmt.Sprintf("alter table %s alter column %s set generated %s", tq, cq, *dd.Generated),
				fmt.Sprintf("alter table %s alter column %s set generated %s", tq, cq, *ad.Generated),
			)
		}
	case ad.Kind == "identity":
		p.emit(
			fmt.Sprintf("alter table %s alter column %s drop identity", tq, cq),
			fmt.Sprintf("alter table %s alter column %s add %s", tq, cq, identityClauseSQL(*ad)),
		)
		setDefault(dd, fmt.Sprintf("alter table %s alter column %s drop default", tq, cq))
	case dd.Kind == "identity":
		p.emit(
			fmt.Sprintf("alter table %s alter column %s drop default", tq, cq),
			fmt.Sprintf("alter table %s alter column %s set default %s", tq, cq, defaultSQL(*ad)),
		)
		p.emit(
			fmt.Sprintf("alter table %s alter column %s add %s", tq, cq, identityClauseSQL(*dd)),
			fmt.Sprintf("alter table %s alter column %s drop identity", tq, cq),
		)
	default:
		setDefault(dd, inverseOfActual())
	}
	return nil
}

// planConstraintChanges pairs constraints by name within one shared table.
// The primary key is paired by semantic slot (its name is usually
// catalog-generated): a desired PK pairs with the live PK whatever their
// names, so a PK definition change is a drop+add, never a duplicate-PK plan.
func (p *v2Planner) planConstraintChanges(table V2Identity, desired, actual *V2Table) error {
	tq := qualifiedNameSQL(table)

	actualPKName := ""
	if apk := actual.PrimaryKey(); apk != nil {
		actualPKName = apk.Name
	}
	desiredPKName := ""
	if dpk := desired.PrimaryKey(); dpk != nil {
		desiredPKName = dpk.Name
	}

	var drops []string
	var adds []V2Constraint
	for _, dc := range desired.Constraints {
		ac := actual.Constraint(dc.Name)
		if ac == nil && dc.Type == "primary-key" && actualPKName != "" {
			ac = actual.Constraint(actualPKName)
		}
		if ac == nil {
			adds = append(adds, dc)
			continue
		}
		if p.constraintsEqualAfterRenames(table, dc, *ac) {
			continue
		}
		drops = append(drops, ac.Name)
		adds = append(adds, dc)
	}
	for _, ac := range actual.Constraints {
		if desired.Constraint(ac.Name) != nil {
			continue
		}
		if ac.Type == "primary-key" && desiredPKName != "" {
			continue // paired with the desired PK by slot above
		}
		drops = append(drops, ac.Name)
	}

	for _, name := range drops {
		// A dropped or replaced check is re-added from its live text by the
		// down statement, whatever replaced it (a changed check whose text
		// differs was already recorded by the comparison).
		if ac := actual.Constraint(name); ac != nil && ac.Expression != nil {
			verb := "dropped"
			if desired.Constraint(name) != nil {
				verb = "replaced"
			}
			p.blockOnRename(table, v2CheckElement(name), fmt.Sprintf("check constraint %s is %s, and its down statement re-adds %q", name, verb, *ac.Expression), *ac.Expression)
		}
		p.emit(
			fmt.Sprintf("alter table %s drop constraint if exists %s", tq, quoteIdent(name)),
			fmt.Sprintf("alter table %s add constraint %s %s", tq, quoteIdent(name), p.constraintFragmentFor(table, actual, name)),
		)
	}
	for _, con := range adds {
		stmt, err := addV2ConstraintSQL(*desired, con)
		if err != nil {
			return err
		}
		p.emit(stmt, fmt.Sprintf("alter table %s drop constraint if exists %s", tq, quoteIdent(con.Name)))
		if con.Type == "primary-key" {
			p.warn("table %s: primary key %q will be (re)created — PostgreSQL scans the table and requires the key columns to be NOT NULL and unique", table, con.Name)
		}
	}
	return nil
}

func (p *v2Planner) constraintFragmentFor(table V2Identity, actual *V2Table, name string) string {
	con := actual.Constraint(name)
	if con == nil {
		return "-- constraint " + quoteIdent(name) + " (no recorded definition; no down statement)"
	}
	frag, err := v2ConstraintFragment(*con)
	if err != nil {
		return "-- constraint " + quoteIdent(name) + " (unrepresentable; no down statement)"
	}
	return frag
}

// constraintsEqualAfterRenames compares a desired constraint against the
// live one with the live column names translated through this table's
// rename map (the database follows renames automatically).
func (p *v2Planner) constraintsEqualAfterRenames(table V2Identity, dc V2Constraint, ac V2Constraint) bool {
	translated := ac
	translated.Columns = nil
	for _, cn := range ac.Columns {
		translated.Columns = append(translated.Columns, p.actualToDesiredName(table, cn))
	}
	if ac.References != nil {
		ref := *ac.References
		ref.Columns = nil
		for _, cn := range ac.References.Columns {
			ref.Columns = append(ref.Columns, p.actualToDesiredName(ac.References.Table, cn))
		}
		translated.References = &ref
	}
	return p.constraintsEqual(table, dc, translated)
}

// canonicalV2FKAction maps the contract's "no action" spelling onto the
// canonical absent form: the PostgreSQL catalog cannot distinguish an
// explicit NO ACTION from an omitted clause (confdeltype/confupdtype 'a'
// IS the default), so introspection emits absent and a desired "no action"
// must compare equal to it or the diff never converges.
func canonicalV2FKAction(a *string) *string {
	if a == nil || *a == "" || *a == "no action" {
		return nil
	}
	return a
}

// canonicalV2FKMatch does the same for MATCH SIMPLE (confmatchtype 's',
// the catalog default): a desired "simple" compares equal to absent.
func canonicalV2FKMatch(m *string) *string {
	if m == nil || *m == "" || *m == "simple" {
		return nil
	}
	return m
}

// constraintsEqual compares two constraints of one table. Check expression
// text routes through textEqual so a textual difference without a catalog
// oracle is flagged as unverified instead of silently planning a spurious
// drop+add of an equivalent constraint.
func (p *v2Planner) constraintsEqual(table V2Identity, a, b V2Constraint) bool {
	if a.Type != b.Type || a.Name != b.Name {
		return false
	}
	if !equalStringSlices(a.Columns, b.Columns) {
		return false
	}
	if !p.textEqual(table, v2CheckElement(a.Name), "check constraint "+a.Name+" expression", a.Expression, b.Expression) {
		return false
	}
	if (a.References == nil) != (b.References == nil) {
		return false
	}
	if a.References != nil {
		if a.References.Table != b.References.Table ||
			!equalStringSlices(a.References.Columns, b.References.Columns) ||
			!equalStrPtrs(canonicalV2FKAction(a.References.OnDelete), canonicalV2FKAction(b.References.OnDelete)) ||
			!equalStrPtrs(canonicalV2FKAction(a.References.OnUpdate), canonicalV2FKAction(b.References.OnUpdate)) ||
			!equalStrPtrs(canonicalV2FKMatch(a.References.Match), canonicalV2FKMatch(b.References.Match)) {
			return false
		}
	}
	if !equalBoolPtrs(a.Deferrable, b.Deferrable) || !equalBoolPtrs(a.InitiallyDeferred, b.InitiallyDeferred) {
		return false
	}
	return true
}

func equalStringSlices(a, b []string) bool {
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

func equalStrPtrs(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func equalBoolPtrs(a, b *bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func (p *v2Planner) planIndexChanges(table V2Identity, desired, actual *V2Table) error {
	for _, di := range desired.Indexes {
		ai := actual.Index(di.Identity.Name)
		if ai == nil {
			stmt, err := createV2IndexSQL(*desired, di)
			if err != nil {
				return err
			}
			p.emit(stmt, fmt.Sprintf("drop index if exists %s", qualifiedNameSQL(di.Identity)))
			continue
		}
		if p.indexEqualAfterRenames(table, di, *ai) {
			continue
		}
		// Same-name index with a changed definition: drop and recreate.
		oldDDL, err := createV2IndexSQL(*actual, *ai)
		if err != nil {
			oldDDL = fmt.Sprintf("-- index %s (unrepresentable old definition; no down statement)", di.Identity)
		}
		// The down statement re-creates the live definition, whatever the
		// difference was (key parts, predicate, method...).
		p.blockOnRename(table, v2IndexElement(ai.Identity.Name), fmt.Sprintf("index %s is re-created, and its down statement re-creates it as %q", ai.Identity.Name, oldDDL), indexTexts(*ai)...)
		newDDL, err := createV2IndexSQL(*desired, di)
		if err != nil {
			return err
		}
		p.emit(fmt.Sprintf("drop index if exists %s", qualifiedNameSQL(di.Identity)), oldDDL)
		p.emit(newDDL, fmt.Sprintf("drop index if exists %s", qualifiedNameSQL(di.Identity)))
		p.warn("table %s: index %q exists with a different definition; it will be dropped and recreated", table, di.Identity.Name)
	}
	for _, ai := range actual.Indexes {
		if desired.Index(ai.Identity.Name) != nil {
			continue
		}
		if !p.opts.AllowDestructive {
			p.warn("index %q on table %s exists in %s but not in the schema: left untouched (dropping requires explicit destructive acknowledgement, --allow-destructive)", ai.Identity.Name, table, p.baseNoun())
			continue
		}
		oldDDL, err := createV2IndexSQL(*actual, ai)
		if err != nil {
			return err
		}
		p.blockOnRename(table, v2IndexElement(ai.Identity.Name), fmt.Sprintf("index %s is dropped, and its down statement re-creates it as %q", ai.Identity.Name, oldDDL), indexTexts(ai)...)
		p.emit(fmt.Sprintf("drop index if exists %s", qualifiedNameSQL(ai.Identity)), oldDDL)
		p.warn("index %q on table %s will be dropped", ai.Identity.Name, table)
	}
	return nil
}

// comparisonVerified reports whether the comparison of one expression
// element (v2DefaultElement and friends) of this table had a catalog oracle:
// a live twin normalizer that normalized that element.
func (p *v2Planner) comparisonVerified(table V2Identity, element string) bool {
	return p.opts.Normalizer != nil && !p.twinFailedTables[table] && !p.twinFailedElems[table][element]
}

// indexEqualAfterRenames compares index metadata with the live column
// names translated through the rename map. Every expression-bearing field
// is evaluated (not only the first difference) so the unverified notes
// cover all textual comparisons that ran without a catalog oracle.
func (p *v2Planner) indexEqualAfterRenames(table V2Identity, di, ai V2Index) bool {
	equal := di.Unique == ai.Unique && di.Method == ai.Method
	aiInclude := make([]string, 0, len(ai.Include))
	for _, c := range ai.Include {
		aiInclude = append(aiInclude, p.actualToDesiredName(table, c))
	}
	if !equalStringSlices(di.Include, aiInclude) {
		equal = false
	}
	if len(di.Key) != len(ai.Key) {
		equal = false
	} else {
		for i := range di.Key {
			if !sameKeyPartOrdering(di.Key[i], ai.Key[i]) {
				equal = false
			}
			if di.Key[i].Column != nil && ai.Key[i].Column != nil {
				if *di.Key[i].Column != p.actualToDesiredName(table, *ai.Key[i].Column) {
					equal = false
				}
				continue
			}
			if !p.textEqual(table, v2IndexElement(di.Identity.Name), "index "+di.Identity.String()+" key part", di.Key[i].Expression, ai.Key[i].Expression) {
				equal = false
			}
		}
	}
	if !p.textEqual(table, v2IndexElement(di.Identity.Name), "index "+di.Identity.String()+" predicate", di.Where, ai.Where) {
		equal = false
	}
	return equal
}

// canonicalKeyPartOrdering reduces a key part's ordering to its effective
// PostgreSQL meaning: (descending, nullsFirst). An explicit "asc", or a
// nulls placement equal to the direction default (ASC -> NULLS LAST, DESC ->
// NULLS FIRST), is the same catalog state as omission — introspection writes
// the minimal form, so desired documents spelling the defaults explicitly
// must compare equal or the diff never converges.
func canonicalKeyPartOrdering(k V2IndexKeyPart) (desc bool, nullsFirst bool) {
	desc = k.Order != nil && *k.Order == "desc"
	nullsFirst = desc
	if k.Nulls != nil {
		nullsFirst = *k.Nulls == "first"
	}
	return desc, nullsFirst
}

func sameKeyPartOrdering(a, b V2IndexKeyPart) bool {
	ad, an := canonicalKeyPartOrdering(a)
	bd, bn := canonicalKeyPartOrdering(b)
	return ad == bd && an == bn
}

func (p *v2Planner) textEqual(table V2Identity, element, what string, desired, actual *string) bool {
	if desired == nil || actual == nil {
		return desired == nil && actual == nil
	}
	if *desired == *actual {
		return true
	}
	// A column default cannot reference a column, so only the other
	// expression elements can differ because of a rename.
	isDefault := strings.HasPrefix(element, "column ") && strings.HasSuffix(element, " default")
	if !isDefault && p.blockOnRename(table, element, fmt.Sprintf("%s: %q (desired) vs %q (%s)", what, *desired, *actual, p.baseLiveNoun()), *actual) {
		return false
	}
	if !p.comparisonVerified(table, element) {
		note := fmt.Sprintf("%s: %q (desired) vs %q (live) — compared textually without a catalog oracle", what, *desired, *actual)
		p.unverified = append(p.unverified, fmt.Sprintf(unverifiedFormat, note, unverifiedFix))
	}
	return false
}

// indexTexts are an index's expression texts: key expressions and the
// predicate.
func indexTexts(idx V2Index) []string {
	var texts []string
	for _, k := range idx.Key {
		if k.Expression != nil {
			texts = append(texts, *k.Expression)
		}
	}
	if idx.Where != nil {
		texts = append(texts, *idx.Where)
	}
	return texts
}

// defaultsEqual compares defaults structurally; literal/expression SQL
// text routes through textEqual so a textual difference without a catalog
// oracle (no normalizer, or a failed twin) is flagged as unverified in
// warnings instead of silently planning a spurious default rewrite.
func (p *v2Planner) defaultsEqual(table V2Identity, column string, d, a *V2ColumnDefault) bool {
	if d == nil || a == nil {
		return d == nil && a == nil
	}
	if d.Kind != a.Kind {
		return false
	}
	if d.Kind == "literal" || d.Kind == "expression" {
		return p.textEqual(table, v2DefaultElement(column), "column "+table.String()+"."+column+" default", d.SQL, a.SQL)
	}
	return d.SameAs(*a)
}

// orderV2Creates returns creation order (FK targets first) and, per table,
// the FK constraint names that must be deferred to post-create ALTERs
// because they close a dependency cycle.
func orderV2Creates(desired *V2DocumentModel, names []V2Identity) ([]V2Identity, map[V2Identity][]string) {
	inSet := map[V2Identity]bool{}
	for _, n := range names {
		inSet[n] = true
	}
	deferred := map[V2Identity][]string{}
	visited := map[V2Identity]bool{}
	visiting := map[V2Identity]bool{}
	var order []V2Identity

	var visit func(id V2Identity)
	visit = func(id V2Identity) {
		if visited[id] || !inSet[id] {
			return
		}
		if visiting[id] {
			return // cycle edge handled by the deferral marking below
		}
		visiting[id] = true
		t := desired.Table(id)
		if t != nil {
			for _, con := range t.Constraints {
				if con.Type != "foreign-key" || con.References == nil {
					continue
				}
				target := con.References.Table
				if !inSet[target] {
					continue
				}
				if visiting[target] {
					deferred[id] = append(deferred[id], con.Name)
					continue
				}
				visit(target)
			}
		}
		visiting[id] = false
		visited[id] = true
		order = append(order, id)
	}
	for _, n := range names {
		visit(n)
	}
	return order, deferred
}

// planTableDrops plans destructive drops of managed-scope tables that are
// absent from the desired document, in reverse dependency order (cycles
// resolved by dropping the intra-cycle FK constraints first).
func (p *v2Planner) planTableDrops() {
	var toDrop []V2Table
	droppedInPlan := map[V2Identity]bool{}
	for _, t := range p.actual.Tables {
		if !p.scope[t.Identity.Schema] {
			continue
		}
		if p.desiredTables[t.Identity] {
			continue
		}
		if isProtectedTableName(t.Identity.Name) {
			p.warn("table %s %s", t.Identity, InternalMetadataNote)
			continue
		}
		if !p.opts.AllowDestructive {
			p.warn("table %s exists in %s but not in the schema: left untouched (dropping requires explicit destructive acknowledgement, --allow-destructive)", t.Identity, p.baseNoun())
			continue
		}
		toDrop = append(toDrop, t)
		droppedInPlan[t.Identity] = true
	}
	if len(toDrop) == 0 {
		p.planEnumDrops(nil)
		return
	}
	sort.Slice(toDrop, func(i, j int) bool { return toDrop[i].Identity.String() < toDrop[j].Identity.String() })

	remaining := map[V2Identity]bool{}
	for _, t := range toDrop {
		remaining[t.Identity] = true
	}
	references := func(u V2Identity) []V2Constraint {
		t := p.actual.Table(u)
		if t == nil {
			return nil
		}
		var fks []V2Constraint
		for _, con := range t.Constraints {
			if con.Type == "foreign-key" && con.References != nil && remaining[con.References.Table] {
				fks = append(fks, con)
			}
		}
		return fks
	}
	referencedBy := func(t V2Identity) bool {
		for u := range remaining {
			if u == t {
				continue
			}
			for _, con := range references(u) {
				if con.References.Table == t {
					return true
				}
			}
		}
		return false
	}

	for len(remaining) > 0 {
		var droppable []V2Identity
		for id := range remaining {
			if !referencedBy(id) {
				droppable = append(droppable, id)
			}
		}
		if len(droppable) == 0 {
			// Dependency cycle among the dropped tables: remove every
			// intra-set foreign key constraint first, then the tables
			// drop in deterministic order.
			var ids []V2Identity
			for id := range remaining {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
			for _, id := range ids {
				for _, con := range references(id) {
					p.emit(
						fmt.Sprintf("alter table %s drop constraint if exists %s", qualifiedNameSQL(id), quoteIdent(con.Name)),
						fmt.Sprintf("alter table %s add constraint %s %s", qualifiedNameSQL(id), quoteIdent(con.Name), p.constraintFragmentFor(id, p.actual.Table(id), con.Name)),
					)
				}
			}
			droppable = ids
		}
		sort.Slice(droppable, func(i, j int) bool { return droppable[i].String() < droppable[j].String() })
		for _, id := range droppable {
			p.warn("table %s exists in %s but not in the schema: it will be dropped (all rows lost)", id, p.baseNoun())
			ddl, err := createV2TableSQL(*p.actual.Table(id), nil)
			if err != nil {
				ddl = fmt.Sprintf("-- IRREVERSIBLE: table %s carries unrepresentable structure; no down statement can re-create it", id)
			}
			p.emit(fmt.Sprintf("drop table if exists %s", qualifiedNameSQL(id)), ddl)
			delete(remaining, id)
		}
	}

	p.planEnumDrops(droppedInPlan)
}

// planEnumDrops plans destructive drops of enums absent from the desired
// document, after the tables (tables dropping in the same plan do not
// block their enums).
func (p *v2Planner) planEnumDrops(droppedInPlan map[V2Identity]bool) {
	for _, e := range p.actual.Enums {
		if !p.scope[e.Identity.Schema] || p.desiredEnums[e.Identity] {
			continue
		}
		if !p.opts.AllowDestructive {
			p.warn("enum %s exists in %s but not in the schema: left untouched (dropping requires explicit destructive acknowledgement, --allow-destructive)", e.Identity, p.baseNoun())
			continue
		}
		usedBy := ""
		for _, t := range p.actual.Tables {
			if droppedInPlan[t.Identity] {
				continue // the using table drops earlier in this same plan
			}
			for _, c := range t.Columns {
				if c.Type.Enum != nil && *c.Type.Enum == e.Identity {
					usedBy = t.Identity.String() + "." + c.Name
					break
				}
			}
			if usedBy != "" {
				break
			}
		}
		if usedBy != "" {
			p.warn("enum %s is still used by column %s: not dropped", e.Identity, usedBy)
			continue
		}
		values := make([]string, 0, len(e.Values))
		for _, v := range e.Values {
			values = append(values, "'"+strings.ReplaceAll(v, "'", "''")+"'")
		}
		p.emit(
			fmt.Sprintf("drop type if exists %s", qualifiedNameSQL(e.Identity)),
			fmt.Sprintf("create type %s as enum (%s)", qualifiedNameSQL(e.Identity), strings.Join(values, ", ")),
		)
		p.warn("enum %s will be dropped", e.Identity)
	}
}

// securityInvokerOn reads the effective security_invoker setting: absent
// and explicit false are the same PostgreSQL state (the default).
func securityInvokerOn(b *bool) bool { return b != nil && *b }

func (p *v2Planner) viewEqual(dv, av V2View) bool {
	if !equalStrPtrs(dv.CheckOption, av.CheckOption) || securityInvokerOn(dv.SecurityInvoker) != securityInvokerOn(av.SecurityInvoker) {
		return false
	}
	if dv.Definition == av.Definition {
		return true
	}
	if p.opts.Normalizer == nil || p.twinFailedViews[dv.Identity] {
		p.unverified = append(p.unverified, fmt.Sprintf(unverifiedFormat, fmt.Sprintf("view %s definition: %q (desired) vs %q (live) — compared textually without a catalog oracle", dv.Identity, dv.Definition, av.Definition), unverifiedFix))
	}
	return false
}

// reportOutOfScope reports objects living in schemas the desired document
// does not declare: they are outside the managed scope and stay untouched.
func (p *v2Planner) reportOutOfScope() {
	for _, t := range p.actual.Tables {
		if !p.scope[t.Identity.Schema] && !p.desiredTables[t.Identity] {
			p.warn("table %s is outside the schemas managed by this document: left untouched", t.Identity)
		}
	}
	for _, e := range p.actual.Enums {
		if !p.scope[e.Identity.Schema] && !p.desiredEnums[e.Identity] {
			p.warn("enum %s is outside the schemas managed by this document: left untouched", e.Identity)
		}
	}
	for _, v := range p.actual.Views {
		if !p.scope[v.Identity.Schema] && !p.desiredViews[v.Identity] {
			p.warn("view %s is outside the schemas managed by this document: left untouched", v.Identity)
		}
	}
	for _, o := range p.actual.Opaque {
		if !p.scope[o.Identity.Schema] {
			continue
		}
		switch o.Kind {
		case "extension-table", "extension-object":
			p.warn("%s %s is extension-owned (%s): never managed or modified by generated plans", o.Kind, o.Identity, o.Owner)
		case "unsupported-table", "unsupported-object":
			p.warn("%s %s is not representable in schema document v2 (%s): left untouched", o.Kind, o.Identity, o.Reason)
		}
	}
}

const (
	unverifiedFormat = "equivalence not verified for %s — %s"
	unverifiedFix    = "re-run with a live normalizer or align spellings"
)

// unverifiedNotes renders the unverified notes collected so far, one per
// line, for a refusal that returns before reportUnverified runs.
func (p *v2Planner) unverifiedNotes() string {
	lines := make([]string, 0, len(p.unverified))
	for _, u := range p.unverified {
		lines = append(lines, "  "+u)
	}
	return strings.Join(lines, "\n")
}

func (p *v2Planner) reportUnverified() {
	for _, u := range p.unverified {
		p.warn("%s", u)
	}
	p.result.Up = p.upOps
	p.result.Down = p.downOps
}

// ---------------------------------------------------------------------------
// DDL rendering
// ---------------------------------------------------------------------------

func qualifiedNameSQL(id V2Identity) string {
	return quoteIdent(id.Schema) + "." + quoteIdent(id.Name)
}

var v2DDLTypes = map[string]string{
	"int2": "smallint", "int4": "integer", "int8": "bigint",
	"float4": "real", "float8": "double precision",
	"numeric": "numeric", "bool": "boolean", "text": "text",
	"varchar": "varchar", "timestamp": "timestamp", "timestamptz": "timestamptz",
	"date": "date", "bytea": "bytea", "uuid": "uuid",
	"json": "json", "jsonb": "jsonb", "vector": "vector", "tsvector": "tsvector",
}

// v2TypeDDL renders the SQL type for a contract column type.
func v2TypeDDL(t V2ColumnType) (string, error) {
	base, ok := v2DDLTypes[t.Name]
	if !ok {
		if t.Name != "enum" {
			return "", fmt.Errorf("type %q has no SQL rendering", t.Name)
		}
		if t.Enum == nil {
			return "", fmt.Errorf("enum type without an enum reference")
		}
		base = qualifiedNameSQL(*t.Enum)
	}
	params := ""
	if t.Params != nil {
		switch t.Name {
		case "varchar":
			params = fmt.Sprintf("(%d)", t.Params["length"])
		case "numeric":
			if _, hasP := t.Params["precision"]; hasP {
				params = fmt.Sprintf("(%d,%d)", t.Params["precision"], t.Params["scale"])
			}
		case "timestamp", "timestamptz":
			params = fmt.Sprintf("(%d)", t.Params["precision"])
		case "vector":
			params = fmt.Sprintf("(%d)", t.Params["dimensions"])
		default:
			return "", fmt.Errorf("type %q accepts no parameters", t.Name)
		}
	}
	if t.Array {
		return base + params + "[]", nil
	}
	return base + params, nil
}

func identityClauseSQL(d V2ColumnDefault) string {
	if d.Generated != nil && *d.Generated == "by default" {
		return "generated by default as identity"
	}
	return "generated always as identity"
}

func defaultSQL(d V2ColumnDefault) string {
	switch d.Kind {
	case "identity":
		return identityClauseSQL(d)
	case "sequence":
		return fmt.Sprintf("nextval(%s::regclass)", literalQualifiedName(*d.Sequence))
	case "literal", "expression":
		return *d.SQL
	}
	return ""
}

// literalQualifiedName renders a quoted, schema-qualified name usable
// inside a SQL string literal context (regclass reference).
func literalQualifiedName(id V2Identity) string {
	return "'" + strings.ReplaceAll(qualifiedNameSQL(id), "'", "''") + "'"
}

func v2ColumnDDL(c V2Column) (string, error) {
	typeSQL, err := v2TypeDDL(c.Type)
	if err != nil {
		return "", fmt.Errorf("column %q: %w", c.Name, err)
	}
	parts := []string{quoteIdent(c.Name), typeSQL}
	if c.Generated != nil {
		parts = append(parts, "generated always as ("+c.Generated.Expression+") stored")
	}
	if c.Default != nil {
		switch c.Default.Kind {
		case "identity":
			parts = append(parts, identityClauseSQL(*c.Default))
		default:
			parts = append(parts, "default "+defaultSQL(*c.Default))
		}
	}
	if c.NotNull {
		parts = append(parts, "not null")
	}
	return strings.Join(parts, " "), nil
}

func deferrableClause(con V2Constraint) string {
	if con.Deferrable == nil || !*con.Deferrable {
		return ""
	}
	if con.InitiallyDeferred != nil && *con.InitiallyDeferred {
		return " deferrable initially deferred"
	}
	return " deferrable"
}

func v2ConstraintFragment(con V2Constraint) (string, error) {
	quoted := make([]string, 0, len(con.Columns))
	for _, c := range con.Columns {
		quoted = append(quoted, quoteIdent(c))
	}
	switch con.Type {
	case "primary-key":
		return "primary key (" + strings.Join(quoted, ", ") + ")" + deferrableClause(con), nil
	case "unique":
		return "unique (" + strings.Join(quoted, ", ") + ")" + deferrableClause(con), nil
	case "check":
		if con.Expression == nil {
			return "", fmt.Errorf("check constraint %q has no expression", con.Name)
		}
		return "check (" + *con.Expression + ")" + deferrableClause(con), nil
	case "foreign-key":
		if con.References == nil {
			return "", fmt.Errorf("foreign key %q has no references", con.Name)
		}
		refQuoted := make([]string, 0, len(con.References.Columns))
		for _, c := range con.References.Columns {
			refQuoted = append(refQuoted, quoteIdent(c))
		}
		sql := "foreign key (" + strings.Join(quoted, ", ") + ") references " +
			qualifiedNameSQL(con.References.Table) + " (" + strings.Join(refQuoted, ", ") + ")"
		// PostgreSQL grammar order: MATCH precedes the action clauses.
		if m := con.References.Match; m != nil && *m != "" {
			sql += " match " + *m
		}
		if a := con.References.OnDelete; a != nil && *a != "" {
			sql += " on delete " + *a
		}
		if a := con.References.OnUpdate; a != nil && *a != "" {
			sql += " on update " + *a
		}
		return sql + deferrableClause(con), nil
	}
	return "", fmt.Errorf("unknown constraint type %q", con.Type)
}

func addV2ConstraintSQL(t V2Table, con V2Constraint) (string, error) {
	frag, err := v2ConstraintFragment(con)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("alter table %s add constraint %s %s", qualifiedNameSQL(t.Identity), quoteIdent(con.Name), frag), nil
}

// createV2TableSQL renders CREATE TABLE for a desired table. deferredFKs
// names FK constraints omitted from the inline definition (cycle edges,
// added via ALTER after all tables exist).
func createV2TableSQL(t V2Table, deferredFKs []string) (string, error) {
	deferSkip := map[string]bool{}
	for _, n := range deferredFKs {
		deferSkip[n] = true
	}
	lines := make([]string, 0, len(t.Columns)+len(t.Constraints))
	for _, c := range t.Columns {
		ddl, err := v2ColumnDDL(c)
		if err != nil {
			return "", err
		}
		lines = append(lines, "  "+ddl)
	}
	for _, con := range t.Constraints {
		if deferSkip[con.Name] {
			continue
		}
		frag, err := v2ConstraintFragment(con)
		if err != nil {
			return "", err
		}
		lines = append(lines, "  constraint "+quoteIdent(con.Name)+" "+frag)
	}
	return fmt.Sprintf("create table %s (\n%s\n)", qualifiedNameSQL(t.Identity), strings.Join(lines, ",\n")), nil
}

func createV2IndexSQL(t V2Table, idx V2Index) (string, error) {
	body, err := indexBodySQL(idx)
	if err != nil {
		return "", err
	}
	sql := "create "
	if idx.Unique {
		sql += "unique "
	}
	// CREATE INDEX names cannot be schema-qualified (the index always lands
	// in the table's schema); DROP INDEX is qualified to stay schema-safe.
	return sql + "index " + quoteIdent(idx.Identity.Name) + " on " + qualifiedNameSQL(t.Identity) + " " + body, nil
}

// indexBodySQL renders everything after the ON clause: method, key parts
// (with operator classes, X01), INCLUDE list, access-method parameters and
// predicate.
func indexBodySQL(idx V2Index) (string, error) {
	parts := make([]string, 0, len(idx.Key))
	for _, k := range idx.Key {
		var body string
		if k.Column != nil {
			body = quoteIdent(*k.Column)
		} else if k.Expression != nil {
			body = "(" + *k.Expression + ")"
		} else {
			return "", fmt.Errorf("index %q has a key part with neither column nor expression", idx.Identity.Name)
		}
		// Key-part ordering is stored in minimal form: order written only
		// for desc, nulls only when non-default for the direction.
		desc, nullsFirst := canonicalKeyPartOrdering(k)
		if desc {
			body += " desc"
		}
		if nullsFirst != desc {
			// Non-default placement for the direction.
			if nullsFirst {
				body += " nulls first"
			} else {
				body += " nulls last"
			}
		}
		// Explicit operator class (X01): `col vector_cosine_ops`. The name
		// is carried, never resolved — a class the backend lacks fails at
		// DDL time with the server's own error.
		if k.Opclass != nil && *k.Opclass != "" {
			body += " " + *k.Opclass
		}
		parts = append(parts, body)
	}
	sql := "using " + idx.Method + " (" + strings.Join(parts, ", ") + ")"
	if len(idx.Include) > 0 {
		inc := make([]string, 0, len(idx.Include))
		for _, c := range idx.Include {
			inc = append(inc, quoteIdent(c))
		}
		sql += " include (" + strings.Join(inc, ", ") + ")"
	}
	if len(idx.With) > 0 {
		// Deterministic order: sorted parameter names, integers only.
		names := make([]string, 0, len(idx.With))
		for k := range idx.With {
			names = append(names, k)
		}
		sort.Strings(names)
		params := make([]string, 0, len(names))
		for _, n := range names {
			params = append(params, fmt.Sprintf("%s = %d", n, idx.With[n]))
		}
		sql += " with (" + strings.Join(params, ", ") + ")"
	}
	if idx.Where != nil {
		sql += " where " + *idx.Where
	}
	return sql, nil
}

func createV2ViewSQL(v V2View) string {
	sql := "create view " + qualifiedNameSQL(v.Identity)
	var opts []string
	if v.CheckOption != nil {
		opts = append(opts, "check_option = "+*v.CheckOption)
	}
	if v.SecurityInvoker != nil && *v.SecurityInvoker {
		opts = append(opts, "security_invoker = true")
	}
	if len(opts) > 0 {
		sql += " with (" + strings.Join(opts, ", ") + ")"
	}
	return sql + " as " + v.Definition
}
