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
//   - Column order is semantic (contract §4.1/attnum): a desired table
//     whose columns are reordered relative to the live table is rejected —
//     PostgreSQL cannot reorder columns without a rewrite (M03 mirrors this
//     for snapshot planning; introspection never normalizes it away).

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// DiffV2Options parameterizes DiffV2Document. Renames maps the qualified
// desired column ("schema.table.newcol") to the actual column it renames.
// AllowDestructive is the explicit acknowledgement required before drops of
// managed objects are planned. SnapshotBase relabels the comparison base in
// user-facing errors and warnings: offline snapshot planning (M03) compares
// against a recorded snapshot, not a live catalog, and its messages must
// say so. Live callers leave it unset and every message keeps the
// historical database wording byte-for-byte.
type DiffV2Options struct {
	Renames          map[string]string
	AllowDestructive bool
	Normalizer       V2Normalizer
	SnapshotBase     bool
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
		twinFailedViews:  map[V2Identity]bool{},
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
	twinFailedViews  map[V2Identity]bool
	unverified       []string
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
		if n, err := p.opts.Normalizer.NormalizeTable(p.ctx, *t); err == nil {
			normalized = &n
		} else {
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
		ddl, err := createV2TableSQL(*t, deferredFKs[id])
		if err != nil {
			return err
		}
		p.emitRaw(ddl, fmt.Sprintf("drop table if exists %s", qualifiedNameSQL(id)))
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

	// Column-order rejection: PostgreSQL cannot reorder columns without a
	// rewrite, so the MATCHED columns (present in both documents, renames
	// resolved) must keep their relative order — including when columns
	// are added or dropped simultaneously. A reorder smuggled past a
	// same-length guard would apply a non-converging plan and surface the
	// error only on the next run.
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
			return fmt.Errorf(
				"table %s: the desired column order differs from %s (attnum order %v) — PostgreSQL cannot reorder columns without rewriting the table; align the document order or plan a manual migration",
				dt.Identity, p.baseTableNoun(), columnNames(*at))
		}
	}

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
	return nil
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
			return target[strings.LastIndexByte(target, '.')+1:]
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
	if !p.textEqual(table, "check constraint "+a.Name+" expression", a.Expression, b.Expression) {
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
		p.emit(fmt.Sprintf("drop index if exists %s", qualifiedNameSQL(ai.Identity)), oldDDL)
		p.warn("index %q on table %s will be dropped", ai.Identity.Name, table)
	}
	return nil
}

// comparisonVerified reports whether expression comparisons for this table
// had a catalog oracle (live twin normalizer that succeeded).
func (p *v2Planner) comparisonVerified(table V2Identity) bool {
	return p.opts.Normalizer != nil && !p.twinFailedTables[table]
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
			if di.Key[i].Column != nil && ai.Key[i].Column != nil {
				if *di.Key[i].Column != p.actualToDesiredName(table, *ai.Key[i].Column) {
					equal = false
				}
				continue
			}
			if !p.textEqual(table, "index "+di.Identity.String()+" key part", di.Key[i].Expression, ai.Key[i].Expression) {
				equal = false
			}
		}
	}
	if !p.textEqual(table, "index "+di.Identity.String()+" predicate", di.Where, ai.Where) {
		equal = false
	}
	return equal
}

func (p *v2Planner) textEqual(table V2Identity, what string, desired, actual *string) bool {
	if desired == nil || actual == nil {
		return desired == nil && actual == nil
	}
	if *desired == *actual {
		return true
	}
	if !p.comparisonVerified(table) {
		p.unverified = append(p.unverified, fmt.Sprintf("%s: %q (desired) vs %q (live) — compared textually without a catalog oracle", what, *desired, *actual))
	}
	return false
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
		return p.textEqual(table, "column "+table.String()+"."+column+" default", d.SQL, a.SQL)
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
			p.warn("table %s is neutron-internal metadata: always left untouched", t.Identity)
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

func (p *v2Planner) viewEqual(dv, av V2View) bool {
	if !equalStrPtrs(dv.CheckOption, av.CheckOption) || !equalBoolPtrs(dv.SecurityInvoker, av.SecurityInvoker) {
		return false
	}
	if dv.Definition == av.Definition {
		return true
	}
	if p.opts.Normalizer == nil || p.twinFailedViews[dv.Identity] {
		p.unverified = append(p.unverified, fmt.Sprintf("view %s definition: %q (desired) vs %q (live) — compared textually without a catalog oracle", dv.Identity, dv.Definition, av.Definition))
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

func (p *v2Planner) reportUnverified() {
	for _, u := range p.unverified {
		p.warn("equivalence not verified for %s — re-run with a live normalizer or align spellings", u)
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
	"json": "json", "jsonb": "jsonb", "vector": "vector",
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

// indexBodySQL renders everything after the ON clause: method, key parts,
// INCLUDE list and predicate.
func indexBodySQL(idx V2Index) (string, error) {
	parts := make([]string, 0, len(idx.Key))
	for _, k := range idx.Key {
		if k.Column != nil {
			parts = append(parts, quoteIdent(*k.Column))
		} else if k.Expression != nil {
			parts = append(parts, "("+*k.Expression+")")
		} else {
			return "", fmt.Errorf("index %q has a key part with neither column nor expression", idx.Identity.Name)
		}
	}
	sql := "using " + idx.Method + " (" + strings.Join(parts, ", ") + ")"
	if len(idx.Include) > 0 {
		inc := make([]string, 0, len(idx.Include))
		for _, c := range idx.Include {
			inc = append(inc, quoteIdent(c))
		}
		sql += " include (" + strings.Join(inc, ", ") + ")"
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
