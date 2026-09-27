package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	schemaExportCmd.Flags().String("module", "", "compiled schema module that prints one schema document v2 to stdout (default export-schema.mjs, or [migrations].module)")
	schemaExportCmd.Flags().String("out", "", "output path for the canonical schema document (default neutron.schema.json, or [migrations].schema)")
	schemaExportCmd.Flags().Duration("timeout", 120*time.Second, "time budget for the export module")

	schemaPullCmd.Flags().String("out", "neutron.schema.pulled.json", "output path for the pulled document")
	schemaPullCmd.Flags().Duration("timeout", 30*time.Second, "time budget for introspection")

	schemaCheckCmd.Flags().String("schema", "", "desired schema document (default neutron.schema.json, or [migrations].schema)")
	schemaCheckCmd.Flags().String("dir", "", "migrations directory (default migrations, or [migrations].dir)")
	schemaCheckCmd.Flags().Bool("live", false, "check the live database against the applied snapshot chain (drift detection) instead of the desired document against the chain head")
	schemaCheckCmd.Flags().Duration("timeout", 30*time.Second, "time budget for the live check")

	schemaBaselineCmd.Flags().String("dir", "", "migrations directory (default migrations, or [migrations].dir)")
	schemaBaselineCmd.Flags().Duration("timeout", 30*time.Second, "time budget for connecting, waiting for the migration lock and introspection")

	schemaCmd.AddCommand(schemaExportCmd)
	schemaCmd.AddCommand(schemaPullCmd)
	schemaCmd.AddCommand(schemaCheckCmd)
	schemaCmd.AddCommand(schemaBaselineCmd)
	rootCmd.AddCommand(schemaCmd)
}

var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Export, pull, check and baseline schema documents",
	Long: `Schema document workflow on the cross-language schema contract v2 (contracts/data/).

export   run the project's compiled schema module and write the canonical desired document
pull     introspect the live database into a document (read-only)
check    compare the desired document (offline) or the live database (--live) against the snapshot chain
baseline record an existing database as the chain root: read-only inspection, an ownership manifest, never a reset`,
}

var schemaExportCmd = &cobra.Command{
	Use:   "export",
	Short: "Run the schema export module and write the canonical desired document",
	Long: `Runs the project's compiled schema module (default export-schema.mjs) with node as a
direct child process — no shell, no database connection — and validates its stdout as a
schema document v2. The canonical form is written atomically to the output path, so a
failed or invalid export never replaces the previous document.

The module contract: print exactly one schema document v2 (JSON) to stdout and exit 0.
It runs trusted project code; TypeScript compilation stays with the project's tooling.`,
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runSchemaExport(cmd, args)) },
}

var schemaPullCmd = &cobra.Command{
	Use:   "pull",
	Short: "Introspect the live database into a schema document (read-only)",
	Long: `Introspects the live PostgreSQL catalog into a canonical schema document v2
(the same introspection the diff engine uses) and writes it atomically. Objects the
contract cannot represent faithfully are recorded as a read-only opaque inventory in
the document and listed in the output. Neutron-internal metadata tables (_neutron_*)
are left out: they are managed automatically and a schema document may not list them.
A user table with a foreign key into one of them (for example to _neutron_jobs, the
job queues' table) cannot be described by any schema document: pull refuses, names
the table, the key and its target, and writes nothing. Drop the key or point it at a
table you own, or keep that database off the schema document workflow.
Pull never modifies the database.`,
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runSchemaPull(cmd, args)) },
}

var schemaCheckCmd = &cobra.Command{
	Use:   "check",
	Short: "Check the schema against the snapshot chain",
	Long: `Offline (default): diffs the desired schema document against the chain head
snapshot — a non-empty diff means pending schema changes, reported and exit 1.
Objects a migration left in place because the document no longer declares them
are recorded in the chain, so their drops stay listed as pending until a plan
with --allow-destructive drops them or the document declares them again.
The chain records the column order the database holds: a new column declared
between existing ones is noted, not a pending change; a changed relative order
of existing columns is refused, as PostgreSQL cannot reorder columns.

With --live: introspects the database and compares it against the snapshot of the
newest APPLIED migration (or the baseline). Any managed-scope difference is drift:
reported and exit 1 before you apply anything. The check is read-only; it reads the
migration history but never writes, locks or adopts it.`,
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runSchemaCheck(cmd, args)) },
}

var schemaBaselineCmd = &cobra.Command{
	Use:   "baseline",
	Short: "Record an existing database as the snapshot chain root",
	Long: `Initial ownership workflow for an existing database: introspects the database
(read-only), writes migrations/snapshots/000_baseline.snapshot.json as the chain root,
and reports unmanaged objects. Neutron-internal metadata tables (_neutron_*) are left
out of the baseline: they are managed automatically and never part of a plan (a
baseline written by an earlier CLI that lists them still works — they are ignored on
read). Existing migration files at baseline time are recorded as covered by the
baseline, so every one of them must already be applied: a file the migration history
does not list refuses the baseline (apply it with neutron migrate, record changes
already made by other means with neutron migrate resolve <version> --mark-applied, or
move the file out), and so does a legacy or SDK history while any file exists (adopt
it first). Migration history is OBSERVED and reported, never adopted or upgraded —
history graduation is neutron migrate adopt's job. Nothing in the database is
created, altered or dropped. The baseline holds the migration lock while it reads
the database and its history, so a concurrent neutron migrate waits for it (and it
waits for a running one, within --timeout; unlike the runners, --timeout 0 does not
mean no deadline). Holding the lock takes a second database connection: a pool or
role limited to one connection fails at the deadline. A user table with a foreign key into a
_neutron_* table (for example to _neutron_jobs, the job queues' table) cannot be
described by any schema document: the baseline refuses, names the table, the key and
its target, and writes nothing.`,
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runSchemaBaseline(cmd, args)) },
}

func runSchemaExport(cmd *cobra.Command, args []string) error {
	module, _ := cmd.Flags().GetString("module")
	out, _ := cmd.Flags().GetString("out")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	if module == "" {
		module = config.MigrationsExportModule()
	}
	if out == "" {
		out = config.MigrationsSchemaSource()
	}

	if _, err := os.Stat(module); err != nil {
		return fmt.Errorf("schema export module %s not found — compile the schema module first (the CLI runs it with node; it embeds no TypeScript runtime)", module)
	}
	node, err := exec.LookPath("node")
	if err != nil {
		return fmt.Errorf("node not found in PATH — schema export runs the project's compiled schema module with node; the CLI embeds no runtime")
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()

	var stdout bytes.Buffer
	run := exec.CommandContext(ctx, node, module)
	run.Stdout = &stdout
	run.Stderr = os.Stderr
	if err := run.Run(); err != nil {
		return fmt.Errorf("schema export module %s failed (%d bytes stdout): %w", module, stdout.Len(), err)
	}

	doc, err := db.ParseV2Document(stdout.Bytes())
	if err != nil {
		return fmt.Errorf("schema export module %s did not print a valid schema document v2: %w", module, err)
	}
	if err := db.WriteAtomicReplace(out, append([]byte(nil), doc.Canonical...)); err != nil {
		return err
	}

	ui.Successf("Exported schema document: %s", out)
	ui.Infof("canonical SHA-256 %s", doc.SHA256Hex)
	return nil
}

func runSchemaPull(cmd *cobra.Command, args []string) error {
	out, _ := cmd.Flags().GetString("out")
	timeout, _ := cmd.Flags().GetDuration("timeout")

	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()

	client, err := connectPostgresOnly(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	doc, err := client.IntrospectV2(ctx)
	if err != nil {
		return fmt.Errorf("introspect: %w", err)
	}
	doc, internal, err := db.WithoutInternalMetadata(doc)
	if err != nil {
		return withInternalReferenceOptions(err)
	}
	if err := db.WriteAtomicReplace(out, append([]byte(nil), doc.Canonical...)); err != nil {
		return err
	}
	reportDocumentSummary(doc)
	reportInternalExcluded(internal)
	ui.Successf("Pulled schema document: %s", out)
	return nil
}

// withInternalReferenceOptions adds the ways out to a pull or baseline
// refused because user tables keep foreign keys into neutron-internal
// tables; other errors pass through.
func withInternalReferenceOptions(err error) error {
	var ref *db.InternalReferenceError
	if !errors.As(err, &ref) {
		return err
	}
	var tables []string
	seen := map[db.V2Identity]bool{}
	for _, r := range ref.Refs {
		if !seen[r.Table] {
			seen[r.Table] = true
			tables = append(tables, r.Table.String())
		}
	}
	return fmt.Errorf("%w. Options: drop the foreign key or replace it with one to a table you own, then run this command again; or leave %s unmanaged — no schema document can describe this database while the key exists, so keep it on hand-written migrations (neutron migrate) without schema pull or baseline", err, strings.Join(tables, ", "))
}

// reportInternalExcluded names the neutron-internal tables left out of a
// document written from introspection.
func reportInternalExcluded(internal []db.V2Identity) {
	if len(internal) == 0 {
		return
	}
	ui.Infof("neutron-internal metadata left out of the document (managed automatically, never part of a plan): %s", db.IdentityList(internal))
}

// reportBaselineInternal notes internal tables a baseline written by an
// earlier CLI still lists; the chain loader ignores them.
func reportBaselineInternal(chain *db.SnapshotChain) {
	if chain == nil || len(chain.BaselineInternal) == 0 {
		return
	}
	ui.Infof("baseline %s_%s lists neutron-internal metadata %s (written by an earlier CLI): ignored — internal tables are never managed", db.BaselineVersion, db.BaselineName, db.IdentityList(chain.BaselineInternal))
}

// reportChainRetained notes migration snapshots that do not record what
// their up files leave in place, or record columns in declared order (CLIs
// before M08 wrote them so); the chain loader reads them as the up files
// leave the database.
func reportChainRetained(chain *db.SnapshotChain) {
	if chain == nil {
		return
	}
	if n := len(chain.Retained); n > 0 {
		stems := make([]string, n)
		for i, r := range chain.Retained {
			stems[i] = r.Stem
		}
		last := chain.Retained[n-1]
		ui.Infof("snapshot(s) %s do not record the state their up files leave (%s: %s) — CLIs before M08 omitted it; read as the up files leave it. The snapshot files are unchanged; `neutron migrate` and `neutron schema check --live` verify applied up files against their recorded checksums first", strings.Join(stems, ", "), last.Stem, last.Describe())
	}
	for _, e := range chain.RetainedErrors {
		ui.Warnf("%s — read as recorded; comparing a database against it refuses", e)
	}
}

// retainedDriftHint is the way out when drift meets snapshots read with
// what their up files left in place (written before M08), so
// objects dropped by hand since show as drift that re-creates them, and no
// migration can record a state its own chain says it never reached.
func retainedDriftHint(chain *db.SnapshotChain) string {
	if chain == nil || len(chain.Retained) == 0 {
		return ""
	}
	return "snapshots are read with what their up files left in place (noted above). If the drift re-creates objects that were dropped by hand since, re-baseline at that state: apply the pending migration files or move them out, delete the snapshots directory, run `neutron schema baseline`, then generate again"
}

func reportDocumentSummary(doc *db.V2Document) {
	var model db.V2DocumentModel
	if err := json.Unmarshal(doc.Canonical, &model); err == nil {
		ui.Infof("%d schema(s), %d table(s), %d enum(s), %d view(s), %d opaque object(s)",
			len(model.Schemas), len(model.Tables), len(model.Enums), len(model.Views), len(model.Opaque))
		for _, o := range model.Opaque {
			ui.Warnf("opaque %s %s: %s — read-only inventory, never managed", o.Kind, o.Identity, o.Reason)
		}
	}
}

func runSchemaCheck(cmd *cobra.Command, args []string) error {
	schemaPath, _ := cmd.Flags().GetString("schema")
	dir, _ := cmd.Flags().GetString("dir")
	live, _ := cmd.Flags().GetBool("live")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	if schemaPath == "" {
		schemaPath = config.MigrationsSchemaSource()
	}
	if dir == "" {
		dir = config.MigrationsDir()
	}

	chain, err := db.LoadSnapshotChain(dir)
	if err != nil {
		return err
	}
	reportBaselineInternal(chain)
	reportChainRetained(chain)
	if chain.Empty() {
		return fmt.Errorf("no snapshot chain in %s — generate a migration (`neutron migrate generate --mode snapshot`) or baseline an existing database (`neutron schema baseline`) first", dir)
	}

	if !live {
		loaded, err := loadSchemaDocument(schemaPath)
		if err != nil {
			return err
		}
		if loaded.V2 == nil {
			return fmt.Errorf("snapshot checking requires a schema document v2; %s is the legacy version 1 shape (re-export with exportSchemaV2 or neutron schema export)", schemaPath)
		}
		base, err := chain.HeadDocument()
		if err != nil {
			return err
		}
		desired, orderNotes, err := db.AlignColumnOrder(loaded.V2, base, nil, chain.HeadColumnGenerations())
		if err != nil {
			return err
		}
		for _, n := range orderNotes {
			ui.Infof("%s", n)
		}
		result, err := db.DiffV2Document(cmd.Context(), desired, base, db.DiffV2Options{
			AllowDestructive: true, // surface drops as pending changes; nothing is executed
			SnapshotBase:     true, // the comparison base is a chain snapshot, not a live catalog
		})
		if err != nil {
			return err
		}
		for _, w := range result.Warnings {
			ui.Warnf("%s", w)
		}
		if len(result.Up) == 0 {
			ui.Successf("Desired schema is in sync with snapshot %s (canonical SHA-256 %s)", chain.HeadRef, shortHashCLI(chain.HeadSHA256))
			return nil
		}
		// What a plan without --allow-destructive would do: the rest are
		// drops of objects the chain records and the schema does not
		// declare (left in place), which a plain generate never plans.
		plain, err := db.DiffV2Document(cmd.Context(), desired, base, db.DiffV2Options{SnapshotBase: true})
		if err != nil {
			return err
		}
		switch {
		case len(plain.Up) == 0:
			ui.Warnf("Desired schema has %d pending change(s) vs snapshot %s, all drops of objects the chain records but the schema does not declare (left in place) — declare them in the schema, or plan their drop with `neutron migrate generate --mode snapshot --allow-destructive`:", len(result.Up), chain.HeadRef)
		case len(plain.Up) < len(result.Up):
			ui.Warnf("Desired schema has %d pending change(s) vs snapshot %s — generate with `neutron migrate generate --mode snapshot`; the drops of objects the schema does not declare (left in place) need `--allow-destructive`, or declare them in the schema:", len(result.Up), chain.HeadRef)
		default:
			ui.Warnf("Desired schema has %d pending change(s) vs snapshot %s — generate with `neutron migrate generate --mode snapshot`:", len(result.Up), chain.HeadRef)
		}
		for _, stmt := range result.Up {
			fmt.Printf("  %s;\n", firstLine(stmt))
		}
		return fmt.Errorf("schema is not in sync with the snapshot chain")
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()

	client, err := connectPostgresOnly(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	applied, shape, err := client.AppliedVersionsReadOnly(ctx)
	if err != nil {
		return fmt.Errorf("read migration history: %w", err)
	}
	switch shape {
	case db.HistoryV2Text, db.HistoryAbsent:
	case db.HistoryLegacyText, db.HistoryLegacyInteger:
		return fmt.Errorf("migration history is in the %s shape — applied state is unknown until adopted (`neutron migrate adopt`) or re-baselined; refusing to guess", shape)
	case db.HistoryV2Integer:
		return fmt.Errorf("migration history uses the SDK integer-version shape — the CLI file workflow cannot identify applied state; refusing to guess")
	default:
		return fmt.Errorf("migration history has an incompatible shape (%s); refusing to guess applied state", shape)
	}

	// Applied versions the chain cannot account for are a hard failure,
	// not a silent drop: the newest-applied attribution below would guess
	// (review-1 MINOR-1). Foreign-branch history rows and pre-baseline
	// files missing from the baseline's covers both land here.
	if unknown := unchainableAppliedVersions(chain, applied); len(unknown) > 0 {
		return fmt.Errorf("applied history version(s) %s are unknown to the snapshot chain — no snapshot carries them and the baseline's covered files do not list them (foreign-branch history or unrecorded pre-baseline migrations); refusing to guess the applied state. Reconcile the history with the chain, or delete the baseline and re-baseline at the true applied state", strings.Join(unknown, ", "))
	}

	// The expected state may be read with what applied up files left in
	// place, so those files must be the ones that ran: verify them against
	// their recorded checksums exactly as `neutron migrate` does, before
	// anything is compared (M08 review-1).
	if shape == db.HistoryV2Text {
		files, err := db.ReadMigrationFiles(dir)
		if err != nil {
			return err
		}
		records, err := client.AppliedMigrations(ctx)
		if err != nil {
			return fmt.Errorf("read migration history: %w", err)
		}
		if _, err := db.VerifyAppliedChecksums(files, records); err != nil {
			return err
		}
		if err := retainedAnchorError(chain, records); err != nil {
			return err
		}
	}

	expected, pending, expectedRef, err := expectedAppliedDocument(chain, applied)
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
	for _, w := range result.Warnings {
		ui.Warnf("%s", w)
	}
	if len(result.Up) == 0 {
		ui.Successf("Database matches snapshot %s (%d applied, %d pending)", expectedRef, len(applied), pending)
		return nil
	}
	ui.Errorf("Drift: the database differs from snapshot %s in %d statement(s) — changes made outside migration files must be captured before apply:", expectedRef, len(result.Up))
	for _, stmt := range result.Up {
		fmt.Printf("  %s;\n", firstLine(stmt))
	}
	if hint := retainedDriftHint(chain); hint != "" {
		ui.Infof("%s", hint)
	}
	return fmt.Errorf("managed schema drift detected (run this check before `neutron migrate`)")
}

// unchainableAppliedVersions lists applied history versions that no chain
// snapshot and no baseline covers entry accounts for: applied state that
// cannot be attributed to the chain (review-1 MINOR-1). Exact text match —
// version identity is text throughout the workflow.
func unchainableAppliedVersions(chain *db.SnapshotChain, applied []string) []string {
	known := map[string]bool{}
	for _, s := range chain.Snapshots {
		known[s.Version] = true
	}
	for v := range chain.Covers {
		known[v] = true
	}
	var out []string
	for _, v := range applied {
		if !known[v] {
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

// expectedAppliedDocument picks the snapshot the applied history should be in:
// the newest applied migration's snapshot, else the baseline, else nothing.
func expectedAppliedDocument(chain *db.SnapshotChain, applied []string) (*db.V2Document, int, string, error) {
	appliedSet := map[string]bool{}
	for _, v := range applied {
		appliedSet[v] = true
	}
	newest := ""
	for _, s := range chain.Snapshots {
		if appliedSet[s.Version] && (newest == "" || db.CompareVersions(s.Version, newest) > 0) {
			newest = s.Version
		}
	}
	pending := 0
	for _, s := range chain.Snapshots {
		if !appliedSet[s.Version] {
			pending++
		}
	}
	if newest != "" {
		snap := chain.SnapshotForVersion(newest)
		doc, err := db.ParseV2Document(snap.Document)
		if err != nil {
			return nil, 0, "", fmt.Errorf("snapshot %s document is invalid: %w", snap.Stem(), err)
		}
		return doc, pending, snap.Stem(), nil
	}
	if chain.Baseline != nil {
		doc, err := db.ParseV2Document(chain.Baseline.Document)
		if err != nil {
			return nil, 0, "", fmt.Errorf("baseline snapshot document is invalid: %w", err)
		}
		return doc, pending, db.BaselineVersion + "_" + db.BaselineName, nil
	}
	return nil, 0, "", fmt.Errorf("no applied migration carries a snapshot and no baseline exists — nothing to check the database against; run `neutron schema baseline` or apply a generated migration first")
}

func runSchemaBaseline(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	if dir == "" {
		dir = config.MigrationsDir()
	}

	baselinePath := filepath.Join(dir, db.SnapshotDir, db.BaselineVersion+"_"+db.BaselineName+".snapshot.json")
	if _, err := os.Stat(baselinePath); err == nil {
		return fmt.Errorf("baseline snapshot already exists (%s) — a chain has exactly one root; delete it explicitly to re-baseline", baselinePath)
	}

	// Covers are computed (and duplicate-version files refused) before any
	// connection is made: a covers list keyed by version cannot represent
	// two files claiming one version, and the chain would refuse to load
	// the moment the baseline exists — refuse at the source instead.
	covers, err := readMigrationVersionsSorted(dir)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()

	client, err := connectPostgresOnly(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	// The document and the history must describe one state: a runner that
	// commits a migration between the two reads would leave a baseline that
	// covers a file its document does not reflect. Every runner (migrate,
	// adopt, resolve, down, db push, Studio apply) takes the migration lock,
	// so hold it across both reads; like them, wait for it within --timeout.
	sess, err := client.LockMigrations(ctx)
	if err != nil {
		return err
	}
	defer sess.Release()

	doc, err := client.IntrospectV2(ctx)
	if err != nil {
		return fmt.Errorf("introspect: %w", err)
	}
	doc, internal, err := db.WithoutInternalMetadata(doc)
	if err != nil {
		return withInternalReferenceOptions(err)
	}
	if schemaBaselineAfterIntrospect != nil {
		schemaBaselineAfterIntrospect()
	}

	applied, shape, err := client.AppliedVersionsReadOnly(ctx)
	if err != nil {
		return fmt.Errorf("read migration history: %w", err)
	}
	if err := baselineCoversApplied(covers, applied, shape); err != nil {
		return err
	}

	history := db.BaselineHistory{Shape: shape.String(), AppliedVersions: applied}
	switch shape {
	case db.HistoryAbsent:
		history.Note = "no migration history — a fresh workflow starts at this baseline"
	case db.HistoryV2Text:
		history.Note = "history observed read-only; not modified"
	case db.HistoryLegacyText, db.HistoryLegacyInteger:
		history.Note = "UNKNOWN history (legacy " + shape.String() + "): observed and reported only — not adopted; run `neutron migrate adopt` separately to graduate it"
	case db.HistoryV2Integer:
		history.Note = "SDK integer-version history: observed and reported only — not adopted"
	default:
		history.Note = "incompatible history shape: observed and reported only"
	}

	snap := db.BaselineSnapshotFor(doc, covers, history)
	content, err := db.MarshalSnapshotJSON(snap)
	if err != nil {
		return err
	}
	if err := db.WriteArtifactSet([]db.ArtifactFile{{Path: baselinePath, Content: content}}); err != nil {
		return err
	}

	reportDocumentSummary(doc)
	reportInternalExcluded(internal)
	if len(covers) > 0 {
		ui.Infof("existing migration files recorded as covered by the baseline: %s", strings.Join(covers, ", "))
	}
	switch shape {
	case db.HistoryAbsent:
		ui.Infof("No migration history on this database (nothing was created — baselining never writes).")
	case db.HistoryV2Text:
		ui.Infof("History: %d applied (%s) — observed read-only, not modified.", len(applied), strings.Join(applied, ", "))
	default:
		ui.Warnf("History is %s: UNKNOWN — not adopted, not reset. %s", shape, history.Note)
	}
	ui.Successf("Baseline written: %s (canonical SHA-256 %s)", baselinePath, shortHashCLI(doc.SHA256Hex))
	ui.Infof("The baseline document is your ownership manifest: everything it lists is managed from here on. Remove objects you do not own before generating.")
	return nil
}

// schemaBaselineAfterIntrospect is a test seam: when set, it runs between
// the baseline's introspection and its history read.
var schemaBaselineAfterIntrospect func()

// baselineCoversApplied refuses a baseline whose covers would include a
// migration file that is not applied. The baseline document is the applied
// state and its covers are the files already reflected in it; a covered
// file that runs later is never recorded in the chain, so snapshot planning
// from the baseline would plan its changes again and the drift gate would
// then refuse (R03 attempt-3). Applied state is read only from a history
// the CLI runner can use; a legacy or SDK history is unknown until adopted.
func baselineCoversApplied(covers, applied []string, shape db.HistoryShape) error {
	if len(covers) == 0 {
		return nil
	}
	switch shape {
	case db.HistoryV2Text, db.HistoryAbsent:
	case db.HistoryLegacyText, db.HistoryLegacyInteger:
		return fmt.Errorf("migration history is in the %s shape, so which migration files (%s) are applied is unknown — a baseline covers only applied migrations and never guesses; run `neutron migrate adopt` first, then baseline", shape, strings.Join(covers, ", "))
	default:
		return fmt.Errorf("migration history has the %s shape, which the CLI file workflow cannot read as applied state — a baseline covers only applied migrations and never guesses (migration files: %s)", shape, strings.Join(covers, ", "))
	}
	appliedSet := make(map[string]bool, len(applied))
	for _, v := range applied {
		appliedSet[v] = true
	}
	var pending []string
	for _, v := range covers {
		if !appliedSet[v] {
			pending = append(pending, v)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	return fmt.Errorf("migration file(s) %s are not applied to this database — a baseline records the applied state and covers only applied migrations (a covered file that ran later would be planned again). Apply them first (`neutron migrate`), record changes already made by other means with `neutron migrate resolve <version> --mark-applied`, or move the files out of the migrations directory, then baseline", strings.Join(pending, ", "))
}

// connectPostgresOnly connects and refuses Nucleus servers: the schema
// document workflow (pull/check/baseline) targets PostgreSQL catalogs; the
// Nucleus runners own that engine.
func connectPostgresOnly(ctx context.Context) (*db.Client, error) {
	client, err := db.Connect(ctx, config.DatabaseURL())
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if isNucleus, _, err := client.IsNucleus(ctx); err == nil && isNucleus {
		client.Close()
		return nil, fmt.Errorf("this database is a Nucleus server: schema pull/check/baseline target PostgreSQL — Nucleus schema workflows remain with the SDK runners")
	}
	return client, nil
}

// readMigrationVersionsSorted lists the distinct version texts of .up.sql
// files for the baseline covers list, sorted. Duplicate version texts
// among files are refused: covers are version-keyed and cannot represent
// two files claiming one version (the chain would reject its own baseline).
func readMigrationVersionsSorted(dir string) ([]string, error) {
	files, err := db.ReadMigrationFiles(dir)
	if err != nil {
		return nil, nil // missing directory: nothing to cover
	}
	byVersion := map[string]string{}
	seen := map[string]bool{}
	var out []string
	for _, f := range files {
		stem := f.Version + "_" + f.Name
		if prev, dup := byVersion[f.Version]; dup {
			a, b := prev, stem
			if b < a {
				a, b = b, a
			}
			return nil, fmt.Errorf("migration version %q is claimed by two migration files (%s.up.sql and %s.up.sql): duplicate migration IDs are a reconciliation error, never auto-resolved", f.Version, a, b)
		}
		byVersion[f.Version] = stem
		if seen[f.Version] {
			continue
		}
		seen[f.Version] = true
		out = append(out, f.Version)
	}
	sort.Strings(out)
	return out, nil
}

func shortHashCLI(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}
