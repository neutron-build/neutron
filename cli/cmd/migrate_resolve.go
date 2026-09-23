package cmd

// neutron migrate resolve (M05): inspect the durable state of an
// interrupted migration and run an EXPLICIT recovery path. Recovery is
// never a silent replay: --retry re-runs only statements whose effects are
// provably absent (and refuses unverifiable ones after partial effects),
// --mark-applied records history only when every postcondition is provably
// satisfied, and --abort runs the down SQL to remove partial effects.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	migrateResolveCmd.Flags().String("dir", "migrations", "migrations directory")
	migrateResolveCmd.Flags().Duration("timeout", 60*time.Second, "time budget for the recovery (0 = no deadline)")
	migrateResolveCmd.Flags().Bool("retry", false, "re-run the migration, skipping statements whose effects are provably present")
	migrateResolveCmd.Flags().Bool("mark-applied", false, "record the version as applied WITHOUT running SQL (requires every effect provably present)")
	migrateResolveCmd.Flags().Bool("abort", false, "run the down SQL to remove partial effects (history stays empty)")
	migrateCmd.AddCommand(migrateResolveCmd)
}

var migrateResolveCmd = &cobra.Command{
	Use:   "resolve <version>",
	Short: "Inspect and recover an interrupted migration",
	Long: `Inspects what is durably knowable about a pending migration's effects and
offers the explicit recovery paths for an interrupted or uncertain state.

Statement postconditions are checked against the live catalog: created
tables/views/indexes/types/schemas (a concurrent index additionally must be
valid), drops (object absent). Statements without a checkable postcondition
(ALTER, DML) are reported unverifiable — their outcome is never guessed.

What is durably knowable falls into three states. (1) Provably nothing ran:
every statement has a checkable postcondition and each is absent — a plain
` + "`neutron migrate`" + ` retries safely. (2) Effects provably present, partial or
complete — pick the matching path below. (3) Statements whose outcome
cannot be proven (ALTER, DML) alongside absent checkable effects — the
catalog cannot prove they did not run; they are never silently replayed,
and --abort is the explicit recovery path for them.

Actions (exactly one):
  --retry          re-run the migration on the locked session, skipping
                    statements whose effects are provably present. Refused
                    when an unverifiable statement exists (its outcome
                    cannot be proven after a partial run) or when an INVALID
                    concurrent index remains (drop it first).
  --mark-applied   record the history row without running SQL. Allowed only
                    when EVERY statement has a checkable, satisfied
                    postcondition — for anything else the truth is not
                    knowable and the flag refuses.
  --abort          run the down SQL in one transaction to remove partial
                    effects. The history row stays absent (there is none);
                    a plain ` + "`neutron migrate`" + ` afterwards retries cleanly.
                    Allowed whenever cleanliness is NOT provable — partial,
                    complete, or unverifiable-outcome states alike (the
                    action is explicit, guarded and transactional); refused
                    only when nothing ran is PROVEN.

No action prints the inspection report only. Transactional migrations that
failed mid-run need none of this: their transaction rolled back atomically —
plain ` + "`neutron migrate`" + ` retries them safely.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrateResolve(cmd, args)) },
}

func runMigrateResolve(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	retry, _ := cmd.Flags().GetBool("retry")
	markApplied, _ := cmd.Flags().GetBool("mark-applied")
	abort, _ := cmd.Flags().GetBool("abort")
	version := args[0]

	actions := 0
	for _, b := range []bool{retry, markApplied, abort} {
		if b {
			actions++
		}
	}
	if actions > 1 {
		return fmt.Errorf("pass exactly one of --retry, --mark-applied, --abort (got %d)", actions)
	}

	ctx, cancel := commandContext(cmd)
	defer cancel()

	client, files, sess, release, err := migrateSessionGuard(ctx, dir)
	if err != nil {
		return err
	}
	defer release()

	var file db.MigrationFile
	found := false
	for _, f := range files {
		if f.Version == version {
			file, found = f, true
			break
		}
	}
	if !found {
		return fmt.Errorf("version %s has no migration file in %s — `neutron migrate status` lists the directory's versions", version, dir)
	}

	applied, err := prepareHistoryRun(ctx, client, sess, files)
	if err != nil {
		return err
	}
	for _, r := range applied {
		if r.Version == version {
			ui.Successf("%s is applied (recorded %s) — nothing to resolve", version, r.AppliedAt.Format("2006-01-02 15:04:05"))
			return nil
		}
	}
	if _, err := db.VerifyAppliedChecksums(files, applied); err != nil {
		return err
	}

	analyses, err := analyzeMigrations(dir, []db.MigrationFile{file})
	if err != nil {
		return err
	}
	pending := analyses[0]

	report, err := inspectEffects(ctx, client, file)
	if err != nil {
		return err
	}
	renderEffectsReport(version, report)

	switch {
	case markApplied:
		return resolveMarkApplied(ctx, sess, pending, report)
	case abort:
		return resolveAbort(ctx, client, sess, pending, report)
	case retry:
		return resolveRetry(ctx, client, sess, pending, report)
	}

	// Report only: state the honest next step per provable state (the
	// trichotomy: provably-clean / effects provable / unverifiable).
	switch {
	case report.ProvenClean():
		ui.Successf("No effects present — nothing ran. A plain `neutron migrate` retries safely")
	case report.Verdict() == "clean":
		ui.Warnf("No checkable effects are present, but %d statement(s) have no checkable postcondition — whether they ran is not provable. If any ran, `neutron migrate resolve %s --abort` removes them via the guarded down SQL (history stays empty); a plain `neutron migrate` also retries and fails loudly on the first conflict", report.Unverifiable, version)
	case report.Verdict() == "partial":
		ui.Warnf("Partial effects present. Recover with `neutron migrate resolve %s --retry` (skip-verified), `--abort` (run the down SQL), or reconcile by hand", version)
	case report.Verdict() == "complete":
		ui.Warnf("All checkable effects present but unrecorded. Verify and close with `neutron migrate resolve %s --mark-applied`", version)
	case report.Verdict() == "invalid":
		ui.Errorf("An INVALID concurrent index remains (a failed build leaves debris). Drop it by hand or with `--abort`, then retry")
	}
	if report.Unverifiable > 0 && report.CreationsPresent > 0 {
		ui.Warnf("Unverifiable statements exist alongside present effects — their outcome is unknown; this runner never replays them (M06 owns journaled data steps)")
	}
	return nil
}

// resolveMarkApplied records history without running SQL — only when every
// effect is provably present.
func resolveMarkApplied(ctx context.Context, sess *db.MigrationSession, p pendingMigration, report *effectsReport) error {
	if !report.MarkAppliedAllowed() {
		reasons := []string{}
		if report.Unverifiable > 0 {
			reasons = append(reasons, fmt.Sprintf("%d statement(s) have no checkable postcondition", report.Unverifiable))
		}
		if report.HasInvalid {
			reasons = append(reasons, "an INVALID concurrent index is present")
		}
		missing := 0
		for _, e := range report.Effects {
			if e.State == "unsatisfied" {
				missing++
			}
		}
		if missing > 0 {
			reasons = append(reasons, fmt.Sprintf("%d statement effect(s) are absent", missing))
		}
		return fmt.Errorf(
			"cannot mark %s applied: not everything is durably knowable (%s) — marking now would assert an unproven state; reconcile the effects by hand or --abort and re-run",
			p.File.Version, strings.Join(reasons, ", "))
	}
	if err := sess.RecordAppliedVersion(ctx, p.File); err != nil {
		return err
	}
	ui.Successf("Recorded %s as applied (checksum %s) after verifying every effect present", p.File.Version, shortHashCLI(db.MigrationChecksum(p.File.SQL)))
	return nil
}

// resolveAbort removes partial effects by running the down SQL in one
// transaction. History stays empty; a plain retry follows. Gated on
// PROVABLE cleanliness (M05 review MAJOR-2): a "clean" verdict with
// unverifiable statements present is not proof that nothing ran — an
// ALTER executed in the kill window leaves no checkable trace — and the
// abort path is exactly the explicit, down-guarded, transactional recovery
// for that state.
func resolveAbort(ctx context.Context, client *db.Client, sess *db.MigrationSession, p pendingMigration, report *effectsReport) error {
	if report.ProvenClean() {
		return fmt.Errorf("nothing to abort: no effects of %s are provably present — a plain `neutron migrate` retries safely", p.File.Version)
	}
	if err := validateDownForRecovery(p); err != nil {
		return err
	}
	if err := validateDownStatementAllowlist(p); err != nil {
		return err
	}
	if err := guardProtectedObjects(ctx, client, []pendingMigration{p}, true); err != nil {
		return err
	}
	ui.Infof("Running the down SQL for %s in one transaction — partial effects are removed, history stays empty", p.File.Version)
	if err := sess.ApplyStatementsTx(ctx, db.SplitSQLStatements(p.DownFile.SQL), func(stmt string) {
		ui.Infof("undone: %s", firstLine(stmt))
	}); err != nil {
		return fmt.Errorf("abort failed (nothing was rolled back implicitly — re-inspect with `neutron migrate resolve %s`): %w", p.File.Version, err)
	}
	ui.Successf("Aborted %s: effects removed by the down SQL. A plain `neutron migrate` now retries cleanly", p.File.Version)
	return nil
}

// resolveRetry re-runs the migration, skipping statements whose effects
// are provably present. Unverifiable statements after a partial run are
// refused: replaying them could repeat unknown-outcome work.
func resolveRetry(ctx context.Context, client *db.Client, sess *db.MigrationSession, p pendingMigration, report *effectsReport) error {
	if err := validateStatementAllowlist([]pendingMigration{p}); err != nil {
		return err
	}
	if err := guardProtectedObjects(ctx, client, []pendingMigration{p}, false); err != nil {
		return err
	}
	if report.HasInvalid {
		return fmt.Errorf(
			"an INVALID concurrent index remains from the failed run — a retry would collide. Drop it by hand or with `neutron migrate resolve %s --abort` first",
			p.File.Version)
	}
	if p.Nontransactional {
		if report.Unverifiable > 0 {
			// M05 review MINOR-1: the refusal holds for both effect
			// states (an unverifiable statement's outcome is unknowable
			// whether or not other effects are present), but the message
			// must say so honestly instead of claiming present effects.
			return fmt.Errorf(
				"migration %s contains %d statement(s) with no checkable postcondition — after an interruption this runner cannot prove whether they ran and never replays statements of unknown outcome (M06 owns journaled steps). If nothing ran, a plain `neutron migrate` starts clean; otherwise reconcile by hand or `neutron migrate resolve %s --abort`",
				p.File.Version, report.Unverifiable, p.File.Version)
		}
		var remaining []string
		for _, e := range report.Effects {
			if e.State == "satisfied" {
				ui.Infof("skipping statement %d — effect already present: %s", e.Index, e.FirstLine)
				continue
			}
			remaining = append(remaining, p.Statements[e.Index-1])
		}
		ui.Infof("Re-running %d remaining statement(s) of %s outside a transaction (concurrent operations)", len(remaining), p.File.Version)
		if err := sess.ApplyNontransactionalMigration(ctx, p.File, remaining, func(i int, stmt string) {
			ui.Infof("applied: %s", firstLine(stmt))
		}); err != nil {
			return fmt.Errorf("retry of %s failed: %w\ninspect the durable state before trying again: `neutron migrate resolve %s`", p.File.Version, err, p.File.Version)
		}
		ui.Successf("Retried %s — history recorded", p.File.Version)
		return nil
	}
	if report.CreationsPresent > 0 {
		return fmt.Errorf(
			"transactional migration %s shows present effects with no history row — that state cannot come from this runner's atomic transactions; reconcile by hand or --abort before retrying",
			p.File.Version)
	}
	ui.Infof("Retrying %s atomically (its failed transaction rolled back)", p.File.Version)
	if err := sess.ApplyMigration(ctx, p.File); err != nil {
		return fmt.Errorf("retry of %s failed: %w", p.File.Version, err)
	}
	ui.Successf("Retried %s — history recorded", p.File.Version)
	return nil
}

// validateDownForRecovery enforces the down-side reversibility limits for
// recovery: an executable, non-concurrent down file and — when a plan
// artifact exists — one the plan does not mark irreversible.
func validateDownForRecovery(p pendingMigration) error {
	if p.DownFile == nil {
		return fmt.Errorf("no down migration file for %s — nothing can be aborted automatically", p.File.Version)
	}
	if err := validateDownReversibility(p); err != nil {
		return err
	}
	return nil
}
