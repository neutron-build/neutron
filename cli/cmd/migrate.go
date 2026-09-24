package cmd

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	migrateCmd.Flags().String("dir", "migrations", "migrations directory")
	migrateCmd.Flags().Duration("timeout", 60*time.Second, "total time budget for the migration batch (0 = no deadline)")
	migrateCmd.Flags().Bool("allow-destructive", false, "acknowledge data loss: apply pending migrations whose statements drop tables, columns, indexes or types (migrations may contain only allowlisted statement kinds — SELECT/INSERT/UPDATE/DELETE/MERGE, TRUNCATE, CREATE/ALTER/DROP of schema objects, SET LOCAL; anything else is refused whatever flags are passed, and within those kinds neutron-internal metadata and extension-owned objects are never touched by any drop form, cascade, alteration, row write or WITH-wrapped data-modifying CTE)")

	migrateStatusCmd.Flags().String("dir", "migrations", "migrations directory")
	migrateStatusCmd.Flags().Duration("timeout", 10*time.Second, "time budget for the status query (0 = no deadline)")

	migrateCreateCmd.Flags().String("dir", "migrations", "migrations directory")

	migrateAdoptCmd.Flags().String("dir", "migrations", "migrations directory")
	migrateAdoptCmd.Flags().Duration("timeout", 60*time.Second, "time budget for the adoption (0 = no deadline)")

	migrateDownCmd.Flags().String("dir", "migrations", "migrations directory")
	migrateDownCmd.Flags().Duration("timeout", 60*time.Second, "total time budget for the rollback batch (0 = no deadline)")

	migrateCmd.AddCommand(migrateStatusCmd)
	migrateCmd.AddCommand(migrateCreateCmd)
	migrateCmd.AddCommand(migrateAdoptCmd)
	migrateCmd.AddCommand(migrateDownCmd)
	rootCmd.AddCommand(migrateCmd)
}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Run database migrations",
	Long: `Apply pending SQL migration files to the database.

Execution is serialized across runners with a PostgreSQL session-level
advisory lock held on a dedicated connection from the history read through
the final apply; each migration's DDL and its history row commit atomically.
Applied migrations are checksum-verified (SHA-256 over the file's SQL) before
anything new runs. Transaction-pooled proxies (e.g. PgBouncer transaction
mode) are unsupported for migration connections: use a direct or
session-pooled connection.

Before any DDL runs, under the lock, the runner validates history shape and
checksums, managed drift when the migrations directory carries a snapshot
chain (changes made outside migration files abort the run; ` + "`neutron schema check --live`" + ` reports them), plan.json staleness for snapshot-workflow migrations, the statement-kind allowlist, and protected objects.

Migrations may contain only these statement kinds: SELECT (including WITH;
data-modifying CTEs are target-guarded), INSERT/UPDATE/DELETE/MERGE,
TRUNCATE, CREATE/ALTER/DROP of schema objects (tables, views, materialized
views, indexes including CONCURRENTLY, sequences, types, domains, schemas,
and extensions — extensions may be CREATEd and DROPped only when member-free;
every ALTER EXTENSION is refused — the schema-object subset of the guard
vocabulary; database- and role-wide kinds such as DATABASE, TABLESPACE or
ROLE/USER/GROUP are refused like any other out-of-allowlist kind), and
SET LOCAL. Anything
else — EXPLAIN, PREPARE/EXECUTE, DO, CALL, COPY, CREATE FUNCTION/PROCEDURE/
RULE/TRIGGER, GRANT/REVOKE, COMMENT ON, VACUUM/ANALYZE, REINDEX, LOCK,
SAVEPOINT, BEGIN/COMMIT, session-level SET, CREATE/ALTER/DROP DATABASE or
ROLE — is refused with an error naming the statement kind and the reason,
before any statement in the file executes
(atomically: nothing in the batch applies). No flag bypasses this.

Within the allowlisted kinds, hand-edited files are re-checked at apply time:
statements that drop, alter or write to neutron-internal _neutron_* metadata
or extension-owned objects are refused under every spelling the tokenizer
sees through — comments, quoted identifiers, data-modifying CTEs — and so
are CASCADE drops of EVERY allowlisted kind (relations, types, domains,
schemas, routines, aggregates, operators, operator classes and families,
collations, conversions, statistics, text-search objects, policies,
triggers, rules) whose transitive dependents include protected objects —
or metadata attached to protected objects, like constraints and column
defaults — in any schema, and creates of
_neutron_-prefixed names (the namespace is reserved; a planted lookalike
would defeat the cascade protection). Statements the
planner classifies as destructive or data-losing require --allow-destructive
as an explicit acknowledgement.

Migrations containing concurrent index operations (CREATE INDEX CONCURRENTLY)
cannot run in a transaction: they execute statement-by-statement on the
locked session, structure-changing statements only (data changes are
refused — split them into their own transactional migration), with the
history row recorded after the last statement. A failure or kill mid-file
leaves partial effects and no history row: inspect and recover explicitly
with ` + "`neutron migrate resolve <version>`" + ` — never a silent replay.

Operational migrations (concurrent indexes over big tables, bounded
backfills, expand→backfill→contract steps) can declare the journal instead:
a ` + "`-- neutron:journaled`" + ` line before the first statement. Every step then
carries a verification — a built-in structural postcondition (created/
dropped tables, indexes, types, schemas) or a declared one:

  -- neutron:step verify="SELECT count(*) FROM t WHERE col IS NULL" expect="0"
  UPDATE t SET col = 0 WHERE col IS NULL AND id <= 500;

Verified effects are skipped on apply and on ` + "`migrate resolve --retry`" + ` (interrupted
backfills resume from their data checkpoint; re-runs are idempotent), and
` + "`resolve --mark-applied`" + ` can close a fully-verified unrecorded state. Each step
runs with lock_timeout (default 10s) and statement_timeout (default 0 =
unbounded) knobs — per-step overridable via lock_timeout=/statement_timeout=
annotations, applied with SET LOCAL inside the step's transaction (session
level around concurrent-index statements, which cannot run in one). Index
steps verify their effect by full identity (schema+table+name, never a bare
index name): unqualified tables resolve through the search path exactly as
the statement itself resolves them, and a table that cannot be resolved
REFUSES the step before execution with a demand to qualify it. A failed
concurrent index build leaves INVALID debris: journaled index steps detect
it and recover by DROP INDEX CONCURRENTLY + re-CREATE (REINDEX is not used:
it is outside the migration allowlist and its own failures leave a second
debris class), while building progress is reported from
pg_stat_progress_create_index where available. DML without per-step
verification stays refused in unmarked concurrent files. Down files run in
one transaction and never carry the journal marker; a backfill that
rewrites values must ship an IRREVERSIBLE down stub, which this runner
refuses honestly instead of pretending to roll back.

Targets PostgreSQL. Nucleus migration runners live in the language SDKs and
remain experimental. Databases with pre-protocol histories (older CLI, or
Go/TS SDK integer-version tables) are refused until adopted once with
` + "`neutron migrate adopt`" + `.`,
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrate(cmd, args)) },
}

var migrateStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	RunE:  func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrateStatus(cmd, args)) },
}

var migrateCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new migration file",
	Args:  cobra.ExactArgs(1),
	RunE:  func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrateCreate(cmd, args)) },
}

var migrateAdoptCmd = &cobra.Command{
	Use:   "adopt",
	Short: "Adopt a legacy migration history into protocol v2",
	Long: `Explicitly graduate a legacy _neutron_migrations history into the v2
protocol, in one transaction under the migration lock.

Adoption matches existing history rows to the supplied files by exact text ID
("001" and "1" are never treated as the same migration; numerically equal but
textually distinct IDs are a reconciliation error). Rows whose recorded
content can be proven — a legacy Go SDK checksum that reproduces from the
supplied file — are adopted as verified. Everything else is adopted as
UNVERIFIED: the checksum stays empty and the row is reported. Unknown
checksums are unverified history, not proof of integrity — nothing is ever
silently baselined.`,
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrateAdopt(cmd, args)) },
}

var migrateDownCmd = &cobra.Command{
	Use:   "down [N]",
	Short: "Revert N migrations (default 1)",
	Args:  cobra.MaximumNArgs(1),
	RunE:  func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrateDown(cmd, args)) },
}

// commandContext derives the operation context from the command's own
// context, so caller cancellation (signals, parent tooling) propagates into
// the queries, with the --timeout flag as an optional overall budget.
func commandContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	parent := cmd.Context()
	if parent == nil {
		parent = context.Background()
	}
	timeout, err := cmd.Flags().GetDuration("timeout")
	if err != nil || timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

// migrateSessionGuard connects, refuses Nucleus targets (SDK runners own
// Nucleus migrations; the CLI file workflow targets PostgreSQL), reads the
// migration files, then acquires the advisory-lock session. Files are read
// before locking so the lock is never held across local disk I/O; the lock
// wait itself honors the run's deadline/cancellation. The returned release
// function must be deferred by the caller.
func migrateSessionGuard(ctx context.Context, dir string) (*db.Client, []db.MigrationFile, *db.MigrationSession, func(), error) {
	url := config.DatabaseURL()

	client, err := db.Connect(ctx, url)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("connect: %w", err)
	}
	closeAll := func() { client.Close() }

	if isNucleus, _, err := client.IsNucleus(ctx); err == nil && isNucleus {
		return nil, nil, nil, closeAll, fmt.Errorf(
			"this database is a Nucleus server: `neutron migrate` targets PostgreSQL — " +
				"Nucleus migration runners are the language SDKs (go/nucleus, @neutron-build/nucleus) and stay experimental")
	}

	files, err := db.ReadMigrationFiles(dir)
	if err != nil {
		return nil, nil, nil, closeAll, err
	}

	sess, err := client.LockMigrations(ctx)
	if err != nil {
		return nil, nil, nil, closeAll, err
	}
	release := func() {
		sess.Release()
		client.Close()
	}
	return client, files, sess, release, nil
}

// prepareHistoryRun validates the history shape and returns the applied
// records for a run. Every refusal happens BEFORE any mutation, and the
// identity-collision check runs before a fresh database's history table is
// even created.
func prepareHistoryRun(ctx context.Context, client *db.Client, sess *db.MigrationSession, files []db.MigrationFile) ([]db.MigrationRecord, error) {
	shape, err := client.InspectMigrationHistory(ctx)
	if err != nil {
		return nil, err
	}
	switch shape {
	case db.HistoryAbsent:
		if err := db.DetectMigrationCollisions(files, nil); err != nil {
			return nil, err
		}
		if err := sess.EnsureMigrationTableV2(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	case db.HistoryV2Text:
		applied, err := sess.AppliedMigrations(ctx)
		if err != nil {
			return nil, err
		}
		// A v2-shaped table with a NULL-format row is pre-protocol state:
		// refuse until adopted (§4/§7), never treat it as unverified history.
		if err := db.VerifyHistoryFormats(applied); err != nil {
			return nil, err
		}
		if err := db.DetectMigrationCollisions(files, applied); err != nil {
			return nil, err
		}
		return applied, nil
	case db.HistoryLegacyText, db.HistoryLegacyInteger:
		return nil, fmt.Errorf(
			"migration history is in the legacy %s shape and must be adopted once before it can be run on — "+
				"run `neutron migrate adopt` (explicit, transactional; unprovable rows stay unverified)",
			shape)
	case db.HistoryV2Integer:
		return nil, fmt.Errorf(
			"migration history uses the SDK integer-version shape (written by the Go/TS Nucleus SDKs); " +
				"the CLI's canonical history uses text IDs — adopt it with `neutron migrate adopt` to convert, " +
				"or keep using the SDK runners that wrote it")
	default:
		return nil, fmt.Errorf("migration history has an incompatible shape (%s); refusing before any mutation", shape)
	}
}

func runMigrate(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	allowDestructive, _ := cmd.Flags().GetBool("allow-destructive")

	ctx, cancel := commandContext(cmd)
	defer cancel()

	client, files, sess, release, err := migrateSessionGuard(ctx, dir)
	if err != nil {
		return err
	}
	defer release()

	if len(files) == 0 {
		ui.Warnf("No migration files found in %s", dir)
		return nil
	}

	applied, err := prepareHistoryRun(ctx, client, sess, files)
	if err != nil {
		return err
	}

	// Recorded-content drift is refused before any new mutation.
	unverified, err := db.VerifyAppliedChecksums(files, applied)
	if err != nil {
		return err
	}

	appliedSet := make(map[string]bool)
	for _, r := range applied {
		appliedSet[r.Version] = true
	}
	var pendingFiles []db.MigrationFile
	for _, f := range files {
		if !appliedSet[f.Version] {
			pendingFiles = append(pendingFiles, f)
		}
	}
	if len(pendingFiles) == 0 {
		if len(unverified) > 0 {
			ui.Warnf("%d applied migration(s) have unverified history (no recorded checksum): %s",
				len(unverified), strings.Join(unverified, ", "))
		}
		ui.Successf("Database is up to date (%d migrations applied)", len(applied))
		return nil
	}

	// Everything below is the M05 precondition layer: it all runs under
	// the lock, before any DDL.
	chain, err := chainIfPresent(dir)
	if err != nil {
		return err
	}
	if err := managedDriftUnderLock(ctx, client, chain, applied); err != nil {
		return err
	}

	pendings, err := analyzeMigrations(dir, pendingFiles)
	if err != nil {
		return err
	}
	if err := validateStatementAllowlist(pendings); err != nil {
		return err
	}
	if err := guardProtectedObjects(ctx, client, pendings, false); err != nil {
		return err
	}
	if err := requireDestructiveAcknowledgement(pendings, allowDestructive); err != nil {
		return err
	}
	if err := refuseNontransactionalDataChanges(pendings); err != nil {
		return err
	}
	if err := verifyExtensionCapabilities(ctx, client, pendings); err != nil {
		return err
	}

	var count int
	for _, p := range pendings {
		if p.Journal != nil {
			if err := applyJournaledMigration(ctx, client, sess, p, count, len(pendings)); err != nil {
				return err
			}
			count++
			continue
		}
		if p.Nontransactional {
			// One spinner stop per spinner (StopWithMessage is not
			// re-entrant): per-statement progress is not reported while
			// the run is in flight; resolve re-inspects afterwards.
			spinner := ui.NewSpinner(fmt.Sprintf("Applying %s_%s (nontransactional)...", p.File.Version, p.File.Name))
			err := sess.ApplyNontransactionalMigration(ctx, p.File, p.Statements, nil)
			if err != nil {
				var partial *db.NontransactionalPartialError
				if errors.As(err, &partial) {
					spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Failed %s_%s: %v", p.File.Version, p.File.Name, err))
					return fmt.Errorf(
						"interrupted after %d of %d pending migration(s) (%s_%s failed MID-FILE outside any transaction; its earlier statements' effects REMAIN — no rollback is pretended):\n%v\ninspect and recover explicitly: `neutron migrate resolve %s`",
						count, len(pendings), p.File.Version, p.File.Name, err, p.File.Version)
				}
				spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Failed %s_%s: %v", p.File.Version, p.File.Name, err))
				return err
			}
			spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Applied %s_%s", p.File.Version, p.File.Name))
			count++
			continue
		}

		spinner := ui.NewSpinner(fmt.Sprintf("Applying %s_%s...", p.File.Version, p.File.Name))
		if err := sess.ApplyMigration(ctx, p.File); err != nil {
			spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Failed %s_%s: %v", p.File.Version, p.File.Name, err))
			// Name the interruption boundary: a partial batch is a
			// different operational state than an untouched one.
			return fmt.Errorf("interrupted after %d of %d pending migration(s) (failed at %s_%s): %w",
				count, len(pendings), p.File.Version, p.File.Name, err)
		}
		spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Applied %s_%s", p.File.Version, p.File.Name))
		count++
	}

	if len(unverified) > 0 {
		ui.Warnf("%d applied migration(s) have unverified history (no recorded checksum): %s",
			len(unverified), strings.Join(unverified, ", "))
	}

	if count == 0 {
		ui.Successf("Database is up to date (%d migrations applied)", len(applied))
	} else {
		ui.Successf("Applied %d migration(s)", count)
	}

	return nil
}

func runMigrateAdopt(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")

	ctx, cancel := commandContext(cmd)
	defer cancel()

	client, files, sess, release, err := migrateSessionGuard(ctx, dir)
	if err != nil {
		return err
	}
	defer release()

	shape, err := client.InspectMigrationHistory(ctx)
	if err != nil {
		return err
	}
	switch shape {
	case db.HistoryAbsent:
		return fmt.Errorf("nothing to adopt: this database has no migration history (the first `neutron migrate` creates a v2 history)")
	case db.HistoryV2Text:
		ui.Infof("History is already in the v2 shape; adopting is a no-op report")
	}

	res, err := sess.AdoptMigrationHistory(ctx, files)
	if err != nil {
		return err
	}

	if res.ConvertedFromInteger {
		ui.Infof("Converted the version column INTEGER -> TEXT (explicit adoption step)")
	}
	for _, v := range res.Verified {
		ui.Successf("adopted %s: verified (recorded checksum reproduced from the supplied file)", v)
	}
	for _, v := range res.Unverified {
		ui.Warnf("adopted %s: UNVERIFIED — no proof of what was applied (checksum left empty)", v)
	}
	ui.Successf("Adoption complete: %d verified, %d unverified", len(res.Verified), len(res.Unverified))
	return nil
}

func runMigrateStatus(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	url := config.DatabaseURL()

	ctx, cancel := commandContext(cmd)
	defer cancel()

	client, err := db.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	shape, err := client.InspectMigrationHistory(ctx)
	if err != nil {
		return err
	}
	switch shape {
	case db.HistoryLegacyText, db.HistoryLegacyInteger:
		ui.Warnf("History is in the legacy %s shape; `neutron migrate` will refuse until `neutron migrate adopt` runs once", shape)
	case db.HistoryV2Integer:
		ui.Warnf("History uses the SDK integer-version shape; CLI migrations refuse — use the SDK runners or `neutron migrate adopt`")
	case db.HistoryIncompatible:
		ui.Warnf("History has an incompatible shape; refusing to interpret it as applied state")
	}

	statuses, err := client.MigrationStatuses(ctx, dir)
	if err != nil {
		return err
	}

	if len(statuses) == 0 {
		ui.Warnf("No migrations found in %s", dir)
		return nil
	}

	// Failed/uncertain reporting (M05): a pending migration whose durable
	// effects are partially or fully present is an interrupted or
	// unrecorded state, not an ordinary pending one. Read-only catalog
	// inspection; the resolve command owns recovery.
	appliedSet := map[string]bool{}
	for _, s := range statuses {
		if s.Applied {
			appliedSet[s.Version] = true
		}
	}
	verdicts := map[string]string{}
	if files, err := db.ReadMigrationFiles(dir); err == nil {
		for _, f := range files {
			if appliedSet[f.Version] {
				continue
			}
			report, err := inspectEffects(ctx, client, f)
			if err != nil {
				return err
			}
			switch report.Verdict() {
			case "partial":
				verdicts[f.Version] = "INTERRUPTED: partial effects present — `neutron migrate resolve " + f.Version + "`"
			case "complete":
				verdicts[f.Version] = "EFFECTS PRESENT, UNRECORDED — `neutron migrate resolve " + f.Version + " --mark-applied` after verifying"
			case "invalid":
				verdicts[f.Version] = "INTERRUPTED: invalid concurrent index remains — `neutron migrate resolve " + f.Version + "`"
			}
		}
	}

	tbl := ui.NewTable("Version", "Name", "Status", "Applied At")
	for _, s := range statuses {
		status := "pending"
		appliedAt := ""
		if s.Applied {
			status = "applied"
			if s.Unverified {
				status = "applied (unverified)"
			}
			if s.Missing {
				status += ", file missing"
			}
			appliedAt = s.AppliedAt.Format("2006-01-02 15:04:05")
		} else if d, ok := verdicts[s.Version]; ok {
			status = d
		}
		tbl.AddRow(s.Version, s.Name, status, appliedAt)
	}
	tbl.Render()
	return nil
}

func runMigrateCreate(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	name := args[0]

	upPath, downPath, err := db.CreateMigrationFiles(dir, name)
	if err != nil {
		return err
	}

	ui.Successf("Created migration files:")
	fmt.Printf("  %s\n", upPath)
	fmt.Printf("  %s\n", downPath)
	return nil
}

// parseRevertCount parses the optional `down [N]` argument with strict
// full-string numeric validation: fmt.Sscanf accepted integer prefixes
// like "1junk" (audit neutron-03).
func parseRevertCount(args []string) (int, error) {
	if len(args) == 0 {
		return 1, nil
	}
	count, err := strconv.Atoi(args[0])
	if err != nil {
		return 0, fmt.Errorf("invalid count: %s", args[0])
	}
	if count < 1 {
		return 0, fmt.Errorf("count must be >= 1")
	}
	return count, nil
}

func runMigrateDown(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")

	count, err := parseRevertCount(args)
	if err != nil {
		return err
	}

	ctx, cancel := commandContext(cmd)
	defer cancel()

	client, _, sess, release, err := migrateSessionGuard(ctx, dir)
	if err != nil {
		return err
	}
	defer release()

	downFiles, err := db.ReadDownMigrationFiles(dir)
	if err != nil {
		return err
	}
	// Up files are needed for checksum verification of what is being
	// reverted: rolling back under a modified script record is exactly the
	// drift the checksum exists to catch.
	upFiles, err := db.ReadMigrationFiles(dir)
	if err != nil {
		return err
	}

	applied, err := prepareHistoryRun(ctx, client, sess, upFiles)
	if err != nil {
		return err
	}

	if _, err := db.VerifyAppliedChecksums(upFiles, applied); err != nil {
		return err
	}

	// Preflight before executing any SQL: the newest `count` applied
	// migrations — by version order, not by which down files happen to be
	// present — must each have a non-empty down file. Otherwise a missing
	// down file for a newer migration would be silently skipped and an older
	// one reverted beneath it.
	toRevert, err := selectRevertFrontier(applied, downFiles, count)
	if err != nil {
		return err
	}

	if len(toRevert) == 0 {
		ui.Infof("No migrations to revert")
		return nil
	}

	// Reversibility limits (M05): refuse downs the plan marks irreversible
	// and downs with no executable SQL (an IRREVERSIBLE comment stub is not
	// a restoration — running it would only delete the history row); guard
	// the down SQL's targets like the up path; refuse concurrent drops.
	upByVersion := make(map[string]db.MigrationFile, len(upFiles))
	for _, f := range upFiles {
		upByVersion[f.Version] = f
	}
	var revertAnalysis []pendingMigration
	for _, f := range toRevert {
		up, ok := upByVersion[f.Version]
		if !ok {
			return fmt.Errorf("applied version %s has no up migration file — cannot evaluate reversibility", f.Version)
		}
		p, err := analyzeMigrations(dir, []db.MigrationFile{up})
		if err != nil {
			return err
		}
		p[0].DownFile = &f
		if err := validateDownReversibility(p[0]); err != nil {
			return err
		}
		if err := validateDownStatementAllowlist(p[0]); err != nil {
			return err
		}
		revertAnalysis = append(revertAnalysis, p[0])
	}
	if err := guardProtectedObjects(ctx, client, revertAnalysis, true); err != nil {
		return err
	}

	// Revert migrations
	for _, f := range toRevert {
		spinner := ui.NewSpinner(fmt.Sprintf("Reverting %s_%s...", f.Version, f.Name))
		if err := sess.RevertMigration(ctx, f); err != nil {
			spinner.StopWithMessage(ui.CrossMark, fmt.Sprintf("Failed %s_%s: %v", f.Version, f.Name, err))
			return err
		}
		spinner.StopWithMessage(ui.CheckMark, fmt.Sprintf("Reverted %s_%s", f.Version, f.Name))
	}

	ui.Successf("Reverted %d migration(s)", len(toRevert))
	return nil
}

// validateDownReversibility enforces the down-side limits: the down file
// must contain executable SQL (a comment-only IRREVERSIBLE stub must not be
// "run" to a silent history deletion), must not use concurrent operations,
// must not carry a journal marker (down runs in one transaction; per-step
// journal semantics do not apply to it), and — when a plan artifact exists
// — must not be marked irreversible by the M03 risk report.
func validateDownReversibility(p pendingMigration) error {
	if p.DownFile == nil {
		return fmt.Errorf("aborting rollback: applied migration %s has no down migration file", p.File.Version)
	}
	if err := db.RefuseJournaledDown(p.DownFile.SQL); err != nil {
		return fmt.Errorf("aborting rollback: down migration for %s: %w", p.File.Version, err)
	}
	executable := false
	for _, stmt := range db.SplitSQLStatements(p.DownFile.SQL) {
		if !hasExecutableStmt(stmt) {
			continue
		}
		executable = true
		if db.IsNontransactionalStatement(stmt) {
			return fmt.Errorf(
				"aborting rollback: down migration for %s uses concurrent operations — this runner does not revert through DROP INDEX CONCURRENTLY; drop the index by hand",
				p.File.Version)
		}
	}
	if !executable {
		return fmt.Errorf(
			"aborting rollback: down migration for applied version %s contains no executable SQL (an IRREVERSIBLE marker, not a restoration) — the plan classifies it irreversible; forward-fix instead of pretending to roll back",
			p.File.Version)
	}
	if p.Plan != nil && p.Plan.Risk.OverallReversibility == db.ReversibilityIrreversible {
		return fmt.Errorf(
			"aborting rollback: migration %s is marked irreversible by its plan report (%d irreversible operation(s)) — down SQL is not data restoration; forward-fix instead",
			p.File.Version, p.Plan.Risk.IrreversibleCount)
	}
	return nil
}

// selectRevertFrontier returns the down migrations to revert: the newest
// `count` applied versions in version order, newest first. Every one of them
// must have a non-empty down file — otherwise the rollback is aborted before
// any SQL runs, because skipping a newer migration and reverting an older one
// beneath it corrupts the schema.
func selectRevertFrontier(applied []db.MigrationRecord, downFiles []db.MigrationFile, count int) ([]db.MigrationFile, error) {
	downByVersion := make(map[string]db.MigrationFile, len(downFiles))
	for _, f := range downFiles {
		downByVersion[f.Version] = f
	}

	sorted := append([]db.MigrationRecord(nil), applied...)
	sort.Slice(sorted, func(i, j int) bool {
		return db.CompareVersions(sorted[i].Version, sorted[j].Version) > 0
	})

	var toRevert []db.MigrationFile
	for _, rec := range sorted {
		if len(toRevert) == count {
			break
		}
		f, ok := downByVersion[rec.Version]
		if !ok {
			return nil, fmt.Errorf("aborting rollback: applied migration %s has no down migration file", rec.Version)
		}
		if strings.TrimSpace(f.SQL) == "" {
			return nil, fmt.Errorf("aborting rollback: down migration for applied version %s is empty", rec.Version)
		}
		toRevert = append(toRevert, f)
	}
	return toRevert, nil
}
