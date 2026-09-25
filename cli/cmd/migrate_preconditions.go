package cmd

// Apply-time preconditions for the migration runner (M05). Everything here
// runs under M04's pinned advisory-lock session, before any DDL: history
// and managed-drift validation, plan.json staleness, destructive
// acknowledgement, protected-object guarding, and nontransactional
// classification. The drift logic reuses schema check's helpers
// (unchainedAppliedVersions, expectedAppliedDocument) rather than forking
// them; the risk vocabulary is the M03 planner's own classifiers.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/ui"
)

// pendingMigration is one not-yet-applied migration with its apply-time
// analysis: the statements the server would run, its plan artifact (when
// the snapshot workflow generated it), and its risk classification.
type pendingMigration struct {
	File             db.MigrationFile
	DownFile         *db.MigrationFile
	Plan             *db.PlanArtifact
	Statements       []string
	Nontransactional bool
	// Journal is the parsed journal of a `-- neutron:journaled` migration
	// (M06 operational steps); nil for ordinary files. Journal validation
	// happens at parse time, so a malformed journal fails analysis before
	// any precondition or statement runs.
	Journal *db.JournaledFile
	// RiskyStatements are the first lines of statements the M03 planners
	// classify as destructive or data-losing.
	RiskyStatements []string
	HasDataLoss     bool
}

// planPathFor is the plan artifact path M03's generator writes next to a
// migration file.
func planPathFor(dir string, f db.MigrationFile) string {
	return filepath.Join(dir, f.Version+"_"+f.Name+".plan.json")
}

// analyzeMigrations loads and validates the apply-time analysis for the
// pending migrations. A plan.json that exists but disagrees with its up.sql
// (stale) is a hard error: its reversibility data gates down operations and
// must not be trusted after a hand edit.
func analyzeMigrations(dir string, pending []db.MigrationFile) ([]pendingMigration, error) {
	downFiles, err := db.ReadDownMigrationFiles(dir)
	if err != nil {
		return nil, err
	}
	downByVersion := make(map[string]db.MigrationFile, len(downFiles))
	for _, f := range downFiles {
		downByVersion[f.Version] = f
	}

	out := make([]pendingMigration, 0, len(pending))
	for _, f := range pending {
		p := pendingMigration{File: f}
		if d, ok := downByVersion[f.Version]; ok {
			p.DownFile = &d
		}
		if planPath := planPathFor(dir, f); fileExists(planPath) {
			plan, err := db.LoadPlanArtifact(planPath)
			if err != nil {
				return nil, err
			}
			if err := db.VerifyPlanIdentity(plan, f.Version, f.Name); err != nil {
				return nil, err
			}
			if err := db.PlanMatchesUpSQL(plan, f.SQL); err != nil {
				return nil, err
			}
			p.Plan = plan
		}
		journal, err := db.ParseJournaledFile(f.SQL)
		if err != nil {
			return nil, fmt.Errorf("%s_%s: %w", f.Version, f.Name, err)
		}
		p.Journal = journal
		for _, stmt := range db.SplitSQLStatements(f.SQL) {
			p.Statements = append(p.Statements, stmt)
			if !hasExecutableStmt(stmt) {
				continue
			}
			if db.IsNontransactionalStatement(stmt) {
				p.Nontransactional = true
			}
			destructive, dataLoss := db.ClassifyStatementRisk(stmt)
			if dataLoss {
				p.HasDataLoss = true
			}
			if destructive || dataLoss {
				p.RiskyStatements = append(p.RiskyStatements, firstLine(stmt))
			}
		}
		out = append(out, p)
	}
	return out, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// hasExecutableStmt mirrors the db package's comment-only filter for cmd
// display purposes — both comment kinds, via the single tokenizer.
func hasExecutableStmt(stmt string) bool {
	return db.HasExecutableSQL(stmt)
}

// chainIfPresent loads the snapshot chain when the migrations directory
// carries one. Directories without snapshots (the legacy file-only
// workflow) return a nil chain and no error: chain-based gates do not
// apply to them. A snapshots directory that exists but does not validate
// is a hard error — a broken chain must be reconciled before its
// migrations run.
func chainIfPresent(dir string) (*db.SnapshotChain, error) {
	entries, err := os.ReadDir(filepath.Join(dir, db.SnapshotDir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	hasSnapshots := false
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".snapshot.json") {
			hasSnapshots = true
			break
		}
	}
	if !hasSnapshots {
		return nil, nil
	}
	return db.LoadSnapshotChain(dir)
}

// managedDriftUnderLock validates the applied state against the snapshot
// chain and the live catalog, reusing the schema check helpers: applied
// versions the chain cannot attribute are refused, then the database is
// introspected and diffed against the newest applied migration's snapshot
// (or the baseline). Any managed-scope difference is drift and aborts the
// run before any new DDL. With nothing applied and no baseline there is no
// expected state to compare against — the first migration creates the
// managed scope — so the gate is skipped.
func managedDriftUnderLock(ctx context.Context, client *db.Client, chain *db.SnapshotChain, applied []db.MigrationRecord) error {
	if chain == nil || chain.Empty() {
		return nil
	}
	appliedVersions := make([]string, len(applied))
	for i, r := range applied {
		appliedVersions[i] = r.Version
	}
	if unknown := unchainableAppliedVersions(chain, appliedVersions); len(unknown) > 0 {
		return fmt.Errorf(
			"applied history version(s) %s are unknown to the snapshot chain — no snapshot carries them and the baseline's covered files do not list them; refusing to apply over unattributed state. Reconcile the history with the chain (`neutron schema check --live` reports the same hole)",
			strings.Join(unknown, ", "))
	}

	// Skip condition mirrored from expectedAppliedDocument: nothing
	// applied and no baseline means no expected document exists.
	appliedSet := make(map[string]bool, len(appliedVersions))
	for _, v := range appliedVersions {
		appliedSet[v] = true
	}
	newest := ""
	for _, s := range chain.Snapshots {
		if appliedSet[s.Version] && (newest == "" || db.CompareVersions(s.Version, newest) > 0) {
			newest = s.Version
		}
	}
	if newest == "" && chain.Baseline == nil {
		return nil
	}

	expected, _, expectedRef, err := expectedAppliedDocument(chain, appliedVersions)
	if err != nil {
		return err
	}
	actual, err := client.IntrospectV2(ctx)
	if err != nil {
		return fmt.Errorf("introspect: %w", err)
	}
	norm, err := client.NewTwinNormalizer(ctx)
	if err != nil {
		return err
	}
	defer norm.Close()
	result, err := db.DiffV2Document(ctx, expected, actual, db.DiffV2Options{
		AllowDestructive: true, // surface every reconciliation statement as drift; nothing is executed
		Normalizer:       norm,
	})
	if err != nil {
		return err
	}
	if len(result.Up) == 0 {
		return nil
	}
	var lines []string
	lines = append(lines, fmt.Sprintf(
		"managed schema drift detected before apply: the database differs from snapshot %s in %d statement(s) — changes made outside migration files must be captured before anything new runs:",
		expectedRef, len(result.Up)))
	for _, stmt := range result.Up {
		lines = append(lines, "  "+firstLine(stmt)+";")
	}
	return fmt.Errorf("%s", strings.Join(lines, "\n"))
}

// validateStatementAllowlist refuses migrations containing any statement
// kind outside the allowlist (pass-3 escalation): a migration body may
// contain only SELECT (incl. WITH), INSERT/UPDATE/DELETE/MERGE, TRUNCATE,
// CREATE/ALTER/DROP of schema objects and SET LOCAL — EXPLAIN, PREPARE/
// EXECUTE, DO, CALL, COPY, CREATE FUNCTION/PROCEDURE/RULE/TRIGGER and
// every other kind are refused with a named error. It runs over every
// statement of every pending migration before any statement executes:
// the failure is atomic (nothing in the batch applies, including legal
// statements earlier in the same file).
func validateStatementAllowlist(pendings []pendingMigration) error {
	for _, p := range pendings {
		for i, stmt := range p.Statements {
			if err := db.CheckStatementAllowlist(stmt); err != nil {
				return fmt.Errorf("%s_%s statement %d: %w", p.File.Version, p.File.Name, i+1, err)
			}
		}
	}
	return nil
}

// validateDownStatementAllowlist applies the same statement-kind
// allowlist to a down file before its SQL executes (migrate down,
// resolve --abort): down files are migration files too, and their
// EXPLAIN/PREPARE/COPY spellings are exactly as refused as up ones.
func validateDownStatementAllowlist(p pendingMigration) error {
	if p.DownFile == nil {
		return nil
	}
	for i, stmt := range db.SplitSQLStatements(p.DownFile.SQL) {
		if err := db.CheckStatementAllowlist(stmt); err != nil {
			return fmt.Errorf("down file for %s_%s statement %d: %w", p.File.Version, p.File.Name, i+1, err)
		}
	}
	return nil
}

// guardProtectedObjects refuses migration SQL (up, or down when reverting)
// that touches neutron-internal metadata or extension-owned objects. This
// is the B03-L1 closure: generation-time protection is enforced at apply
// time too, over the full guard vocabulary — every DROP form PostgreSQL
// defines, TRUNCATE, ALTER (destructive or not), row-writing DML including
// data-modifying CTEs (WITH ... INSERT/UPDATE/DELETE/MERGE), DROP
// EXTENSION with members, DROP OWNED, and CASCADE drops whose transitive
// dependents include protected objects. Classification reads the single
// tokenizer's significant tokens, so comments cannot hide a target. Within
// the allowlisted statement kinds this guard holds under every spelling
// the tokenizer can see; no flag bypasses it: the destructive
// acknowledgement gates data loss on legitimately managed objects, never
// metadata/extension protection.
func guardProtectedObjects(ctx context.Context, client *db.Client, pendings []pendingMigration, includeDown bool) error {
	var targets []db.GuardTarget
	collect := func(sql string) {
		targets = append(targets, db.GuardStatementTargets(sql)...)
	}
	for _, p := range pendings {
		for _, stmt := range p.Statements {
			collect(stmt)
		}
		if includeDown && p.DownFile != nil {
			for _, stmt := range db.SplitSQLStatements(p.DownFile.SQL) {
				collect(stmt)
			}
		}
	}
	if len(targets) == 0 {
		return nil
	}
	violations, err := client.ProtectedTargetReasons(ctx, targets)
	if err != nil {
		return err
	}
	if len(violations) == 0 {
		return nil
	}
	var names []string
	for name, reason := range violations {
		names = append(names, fmt.Sprintf("  %s (%s)", name, reason))
	}
	sort.Strings(names)
	return fmt.Errorf(
		"migration SQL targets protected object(s) — neutron-internal metadata and extension-owned objects are never touched: within the allowlisted statement kinds, no drop form, cascade, alteration, row write or WITH-wrapped data-modifying CTE reaches them through comments or quoted spellings, whatever flags are passed:\n%s\nedit the migration file(s) to exclude them",
		strings.Join(names, "\n"))
}

// requireDestructiveAcknowledgement refuses destructive/data-loss
// migrations without the explicit --allow-destructive acknowledgement,
// listing the classified statements (the M02/M03 gating propagated to the
// apply path; the classification vocabulary is unchanged).
// verifyExtensionCapabilities is the X01 fail-closed gate: a pending
// migration whose plan requires the pgvector extension is refused BEFORE
// any statement runs when the connected database does not have the
// extension installed. A migration must fail rather than skip a queried
// column; without this check the server error would only surface mid-plan
// (still atomic, but late and vague).
func verifyExtensionCapabilities(ctx context.Context, client *db.Client, pendings []pendingMigration) error {
	need := false
	for _, p := range pendings {
		if p.Plan == nil {
			continue
		}
		for _, c := range p.Plan.Capabilities {
			if c == "pgvector" {
				need = true
			}
		}
	}
	if !need {
		return nil
	}
	st, err := client.Extension(ctx, "vector")
	if err != nil {
		return fmt.Errorf("verify pgvector extension precondition: %w", err)
	}
	if st.Installed {
		return nil
	}
	var names []string
	for _, p := range pendings {
		if p.Plan == nil {
			continue
		}
		for _, c := range p.Plan.Capabilities {
			if c == "pgvector" {
				names = append(names, p.File.Version+"_"+p.File.Name)
			}
		}
	}
	if st.Available {
		return fmt.Errorf(
			"migration(s) %s require the pgvector extension (vector columns/indexes) but it is not installed in this database — the server has pgvector %s available: run \"create extension vector\" (or add it as an explicit migration step) and re-run. Refusing to run any statement: vector objects are never skipped",
			strings.Join(names, ", "), st.DefaultVersion)
	}
	return fmt.Errorf(
		"migration(s) %s require the pgvector extension (vector columns/indexes) but this server does not carry it at all — install the pgvector extension package on the server, then \"create extension vector\". Refusing to run any statement: vector objects are never skipped",
		strings.Join(names, ", "))
}

// verifyDocumentExtensionCapabilities is the db-push variant of the X01
// extension gate: a pushed schema document carrying the pgvector capability
// is refused before any statement runs when the extension is not installed.
func verifyDocumentExtensionCapabilities(ctx context.Context, client *db.Client, loaded loadedSchema) error {
	if loaded.V2 == nil {
		return nil
	}
	m, err := db.ModelFromRoot(loaded.V2.Root)
	if err != nil {
		return err
	}
	hasPgvector := false
	for _, c := range m.Capabilities {
		if c == "pgvector" {
			hasPgvector = true
		}
	}
	if !hasPgvector {
		return nil
	}
	st, err := client.Extension(ctx, "vector")
	if err != nil {
		return fmt.Errorf("verify pgvector extension precondition: %w", err)
	}
	if st.Installed {
		return nil
	}
	if st.Available {
		return fmt.Errorf(
			"the schema document requires the pgvector extension (vector columns/indexes) but it is not installed in this database — the server has pgvector %s available: run \"create extension vector\" first. Refusing to push: vector objects are never skipped",
			st.DefaultVersion)
	}
	return fmt.Errorf(
		"the schema document requires the pgvector extension (vector columns/indexes) but this server does not carry it at all — install the pgvector extension package on the server, then \"create extension vector\". Refusing to push: vector objects are never skipped")
}

func requireDestructiveAcknowledgement(pendings []pendingMigration, acknowledged bool) error {
	if acknowledged {
		return nil
	}
	var lines []string
	count := 0
	for _, p := range pendings {
		if len(p.RiskyStatements) == 0 {
			continue
		}
		count += len(p.RiskyStatements)
		lines = append(lines, fmt.Sprintf("  %s_%s:", p.File.Version, p.File.Name))
		for _, s := range p.RiskyStatements {
			lines = append(lines, "    "+s)
		}
	}
	if count == 0 {
		return nil
	}
	return fmt.Errorf(
		"%d pending statement(s) are destructive or data-losing — dropping tables, columns, indexes or types requires an explicit acknowledgement:\n%s\nre-run with --allow-destructive to apply them",
		count, strings.Join(lines, "\n"))
}

// refuseNontransactionalDataChanges refuses nontransactional migrations
// that contain row-data changes: their partial outcomes could not be
// recovered without replaying statements whose effect on data is unknown.
// Structural statements only — journaled migrations (M06) carry per-step
// verification and route through the journal executor instead.
func refuseNontransactionalDataChanges(pendings []pendingMigration) error {
	for _, p := range pendings {
		if !p.Nontransactional || p.Journal != nil {
			continue
		}
		for _, stmt := range p.Statements {
			if db.ChangesRowData(stmt) {
				return fmt.Errorf(
					"migration %s_%s mixes nontransactional operations (concurrent indexes) with data-changing statements — a partial failure could not be recovered without replaying unknown-outcome data changes. Split the data changes into their own transactional migration, or declare the journal (a `-- neutron:journaled` header with per-step verify annotations) so every step is verifiably recoverable",
					p.File.Version, p.File.Name)
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Postcondition inspection (status + resolve)
// ---------------------------------------------------------------------------

// statementEffect is one statement's durable-state verdict.
type statementEffect struct {
	Index        int
	FirstLine    string
	Kind         string
	State        string // "satisfied" | "unsatisfied" | "invalid" | "unverifiable"
	CreationSide bool
}

// effectsReport summarizes a migration's postcondition inspection.
type effectsReport struct {
	Effects []statementEffect
	// CreationsPresent / CreationsTotal count creation-side statements.
	CreationsPresent int
	CreationsTotal   int
	// HasInvalid marks concurrent-index debris (exists, not valid).
	HasInvalid bool
	// Unverifiable counts statements with no checkable postcondition.
	Unverifiable int
	// Unevaluable counts journaled steps whose verification could not be
	// EVALUATED against the live state (index identity unresolvable, or
	// the probe/verify query errored). Never counted as absent: a verdict
	// must not rest on a probe that could not run (M06 rework, review-1
	// MAJOR-2).
	Unevaluable int
}

// Verdict classifies what is durably knowable about a pending migration:
//
//   - clean:     no creation-side effect present — nothing ran.
//   - partial:   some creation-side effects present, some missing.
//   - complete:  every creation-side effect present, none invalid.
//   - invalid:   a concurrent index exists but is invalid (failed build debris).
//
// A "clean" verdict alone is NOT proof that nothing ran: statements
// without a checkable postcondition (ALTER, DML) may have executed
// without leaving a catalog trace. ProvenClean is the provable variant.
func (r *effectsReport) Verdict() string {
	switch {
	case r.HasInvalid:
		return "invalid"
	case r.CreationsTotal > 0 && r.CreationsPresent == r.CreationsTotal:
		return "complete"
	case r.CreationsPresent > 0:
		return "partial"
	default:
		return "clean"
	}
}

// ProvenClean reports whether the catalog PROVES nothing ran: no
// creation-side effect present, no unverifiable statement (an ALTER or
// DML statement may have run without leaving a checkable trace — absence
// of evidence is not evidence of absence for those), and no step whose
// evaluation could not run.
func (r *effectsReport) ProvenClean() bool {
	return r.Verdict() == "clean" && r.Unverifiable == 0 && r.Unevaluable == 0
}

// MarkAppliedAllowed reports whether every statement's postcondition is
// checkable and satisfied — the only state in which recording the version
// as applied without running SQL is honest.
func (r *effectsReport) MarkAppliedAllowed() bool {
	if r.Unverifiable > 0 || r.Unevaluable > 0 || r.HasInvalid {
		return false
	}
	for _, e := range r.Effects {
		if e.State != "satisfied" {
			return false
		}
	}
	return true
}

// inspectEffects evaluates every statement's postcondition against the
// live catalog. Read-only; usable from lockless status and locked resolve
// alike. Journaled migrations (M06) evaluate their per-step verification —
// declared verify queries and built-in postconditions — so every step of a
// journaled file is provably present, absent or invalid (the journal
// contract refused unverifiable steps at parse time).
func inspectEffects(ctx context.Context, client *db.Client, f db.MigrationFile) (*effectsReport, error) {
	if db.HasJournaledMarker(f.SQL) {
		jf, err := db.ParseJournaledFile(f.SQL)
		if err != nil {
			return nil, err
		}
		return inspectJournaledEffects(ctx, client, jf)
	}
	report := &effectsReport{}
	for i, stmt := range db.SplitSQLStatements(f.SQL) {
		if !hasExecutableStmt(stmt) {
			continue
		}
		effect := statementEffect{Index: i + 1, FirstLine: firstLine(stmt), State: "unverifiable"}
		if post, ok := db.StatementPostconditionOf(stmt); ok {
			effect.Kind = post.Kind
			effect.CreationSide = post.IsCreationSide()
			state, err := client.EvaluatePostcondition(ctx, post)
			if err != nil {
				return nil, err
			}
			effect.State = state
			if post.IsCreationSide() {
				report.CreationsTotal++
				if state == "satisfied" {
					report.CreationsPresent++
				}
			}
			if state == "invalid" {
				report.HasInvalid = true
			}
		} else {
			report.Unverifiable++
		}
		report.Effects = append(report.Effects, effect)
	}
	return report, nil
}

// renderEffectsReport prints the per-statement durable state.
func renderEffectsReport(version string, report *effectsReport) {
	ui.Infof("Durable effects of %s (what the catalog proves):", version)
	for _, e := range report.Effects {
		state := e.State
		if !e.CreationSide && e.State == "satisfied" && e.Kind != "" {
			state = "satisfied (object absent)"
		}
		ui.Infof("  statement %d [%s] %s: %s", e.Index, state, effectKindLabel(e.Kind), e.FirstLine)
	}
	if report.Unverifiable > 0 {
		ui.Warnf("  %d statement(s) have no checkable postcondition (ALTER, DML, ...) — their outcome cannot be proven from the catalog", report.Unverifiable)
	}
}

func effectKindLabel(kind string) string {
	if kind == "" {
		return "unverifiable"
	}
	return kind
}
