package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	migrateGenerateCmd.Flags().String("dir", "migrations", "migrations directory")
	migrateGenerateCmd.Flags().String("schema", "neutron.schema.json", "schema JSON exported from @neutron-build/sql (exportSchema)")
	migrateGenerateCmd.Flags().String("name", "", "migration name (default: generated)")
	migrateGenerateCmd.Flags().StringArray("rename", nil, "explicit column rename (repeatable): schema.table.old>schema.table.new for v2 documents (two-part table.old>table.new resolves when the table name is unambiguous; v1 documents keep table.old>table.new)")
	migrateGenerateCmd.Flags().Duration("timeout", 30*time.Second, "time budget for introspection")
	migrateGenerateCmd.Flags().Bool("allow-destructive", false, "acknowledge data loss: include DROP statements for tables, columns, and indexes that exist in the database but are absent from the schema (neutron-internal metadata and extension-owned objects are never touched)")
	migrateGenerateCmd.Flags().String("mode", "", "planning mode: live (diff against the database) or snapshot (offline, from the last accepted snapshot; default: [migrations].snapshots in neutron.toml, else live)")
	migrateCmd.AddCommand(migrateGenerateCmd)
}

var migrateGenerateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate a migration by diffing the schema against the database",
	Long: `Compares the desired schema document with the live database and writes an .up.sql/.down.sql pair runnable by ` + "`neutron migrate`" + `.

Schema documents: version 2 (the cross-language contract in contracts/data/) plans through full catalog introspection — qualified schemas, composite PK/unique/check/foreign-key constraints, indexes with predicates and expressions, enums, arrays and views; version 1 (legacy @neutron-build/sql exportSchema output) keeps its historical behavior.

Planning modes: --mode live (the default) diffs against the live database. --mode snapshot plans fully OFFLINE from the last accepted snapshot to the desired document, accounting for pending migrations — the second unapplied migration plans against the first's snapshot, no database connection is made, and nothing is written to the database. Each snapshot-mode migration also records a .plan.json risk/reversibility report and a target snapshot under migrations/snapshots/. Snapshot mode requires a schema document v2.

The generated SQL never drops neutron-internal tables (_neutron_*), extension-owned objects, or anything absent from the schema unless --allow-destructive is passed as an explicit acknowledgement of data loss. Catalog structures this diff engine cannot represent faithfully are rejected with an error instead of producing a migration that falsely claims synchronization.`,
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runMigrateGenerate(cmd, args)) },
}

// reportRunE surfaces RunE errors: main() only exits 1 without printing.
func reportRunE(err error) error {
	if err != nil {
		ui.Errorf("%v", err)
	}
	return err
}

// parseSchemaRenames dispatches rename-flag parsing per document format:
// version 2 wants schema-qualified values (with an unambiguous-name
// resolver over the document's tables), version 1 keeps its two-part form.
func parseSchemaRenames(flags []string, loaded loadedSchema) (map[string]string, error) {
	if loaded.V2 != nil {
		model, err := db.ModelFromRoot(loaded.V2.Root)
		if err != nil {
			return nil, err
		}
		unambiguous := func(table string) (string, bool) {
			schema := ""
			for _, t := range model.Tables {
				if t.Identity.Name != table {
					continue
				}
				if schema != "" {
					return "", false
				}
				schema = t.Identity.Schema
			}
			return schema, schema != ""
		}
		return parseRenamesV2(flags, unambiguous)
	}
	return parseRenames(flags)
}

func runMigrateGenerate(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	schemaPath, _ := cmd.Flags().GetString("schema")
	name, _ := cmd.Flags().GetString("name")
	renameFlags, _ := cmd.Flags().GetStringArray("rename")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	allowDestructive, _ := cmd.Flags().GetBool("allow-destructive")
	modeFlag, _ := cmd.Flags().GetString("mode")

	mode := modeFlag
	if mode == "" {
		if config.MigrationsSnapshotMode() {
			mode = "snapshot"
		} else {
			mode = "live"
		}
	}
	switch mode {
	case "live":
	case "snapshot":
		// Offline planning makes no connection and waits on nothing; an
		// explicitly passed --timeout would be silently ignored — reject
		// it instead (review-1 LOW-2). The default value passes through.
		if cmd.Flags().Changed("timeout") {
			return fmt.Errorf("--timeout has no effect in snapshot mode: offline planning makes no database connection and waits on nothing; drop the flag (or use --mode live)")
		}
		return runMigrateGenerateSnapshot(cmd, dir, schemaPath, name, renameFlags, allowDestructive)
	default:
		return fmt.Errorf("--mode must be live or snapshot, got %q", mode)
	}

	if name == "" {
		name = "generated"
	}

	loaded, err := loadSchemaDocument(schemaPath)
	if err != nil {
		return err
	}

	renames, err := parseSchemaRenames(renameFlags, loaded)
	if err != nil {
		return err
	}

	url := config.DatabaseURL()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := db.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	result, err := computeSchemaPlan(ctx, client, loaded, renames, allowDestructive)
	if err != nil {
		return err
	}

	for _, w := range result.Warnings {
		ui.Warnf("%s", w)
	}

	if len(result.Up) == 0 {
		if len(result.Warnings) > 0 {
			ui.Infof("No applicable changes; see the notes above — objects reported as left untouched are not in sync with the schema.")
		} else {
			ui.Infof("No schema changes detected.")
		}
		return nil
	}

	upSQL := strings.Join(result.Up, ";\n") + ";"
	downSQL := strings.Join(reverseStrings(result.Down), ";\n") + ";"

	upPath, downPath, err := db.CreateMigrationFilesWithContent(dir, name, upSQL, downSQL)
	if err != nil {
		return err
	}

	ui.Successf("Generated migration with %d statement(s):", len(result.Up))
	fmt.Printf("  %s\n", upPath)
	fmt.Printf("  %s\n", downPath)
	fmt.Println()
	fmt.Println("Review the SQL, then apply with `neutron migrate`.")
	return nil
}

// runMigrateGenerateSnapshot is the M03 offline path: plan from the last
// accepted snapshot to the desired document, accounting for pending
// migrations. No database connection, no history write, no locks; the
// serialized runner (neutron migrate) owns apply-time behavior. All four
// artifacts (up/down SQL, plan report, target snapshot) are written
// all-or-nothing and never overwrite existing files.
func runMigrateGenerateSnapshot(cmd *cobra.Command, dir, schemaPath, name string, renameFlags []string, allowDestructive bool) error {
	if name == "" {
		name = "generated"
	}

	loaded, err := loadSchemaDocument(schemaPath)
	if err != nil {
		return err
	}
	if loaded.V2 == nil {
		return fmt.Errorf("snapshot planning requires a schema document v2; %s is the legacy version 1 shape — re-export with exportSchemaV2 or `neutron schema export`", schemaPath)
	}

	chain, err := db.LoadSnapshotChain(dir)
	if err != nil {
		return err
	}
	baseDoc, err := chain.HeadDocument()
	if err != nil {
		return err
	}

	renames, err := parseSchemaRenames(renameFlags, loaded)
	if err != nil {
		return err
	}

	// Offline: no normalizer. Expression-bearing comparisons fall back to
	// strict text and surface as explicit caveats in the plan artifact —
	// equivalence decisions need the catalog (live check has it).
	result, err := db.DiffV2Document(cmd.Context(), loaded.V2, baseDoc, db.DiffV2Options{
		Renames:          renames,
		AllowDestructive: allowDestructive,
		SnapshotBase:     true, // messages name the planning base, not "the database"
	})
	if err != nil {
		return err
	}

	for _, w := range result.Warnings {
		ui.Warnf("%s", w)
	}

	if len(result.Up) == 0 {
		if len(result.Warnings) > 0 {
			ui.Infof("No applicable changes; see the notes above — objects reported as left untouched are not in sync with the schema.")
		} else {
			ui.Infof("No schema changes detected (planning base: snapshot %s).", chain.HeadRef)
		}
		return nil
	}

	version, err := db.NextMigrationVersion(dir)
	if err != nil {
		return err
	}
	plan, err := db.BuildPlanArtifact(version, name, chain.HeadRef, chain.HeadSHA256, loaded.V2, renames, result)
	if err != nil {
		return err
	}

	upSQL := strings.Join(result.Up, ";\n") + ";"
	downSQL := strings.Join(reverseStrings(result.Down), ";\n") + ";"
	files, err := db.MigrationArtifactSet(dir, version, name, plan, loaded.V2, upSQL, downSQL)
	if err != nil {
		return err
	}
	if err := db.WriteArtifactSet(files); err != nil {
		return err
	}

	ui.Infof("Planning base: snapshot %s (canonical SHA-256 %s)", chain.HeadRef, shortHashCLI(chain.HeadSHA256))
	destructiveCount := 0
	for _, op := range plan.Operations {
		if op.Destructive {
			destructiveCount++
		}
	}
	ui.Infof("Risk report: %d statement(s), %d destructive, %d irreversible — overall reversibility %s",
		plan.Risk.StatementCount, destructiveCount, plan.Risk.IrreversibleCount, plan.Risk.OverallReversibility)
	for _, op := range plan.Operations {
		if op.Destructive || op.DataLoss {
			ui.Warnf("operation %d is %s: %s", op.Index, riskLabel(op), firstLine(op.SQL))
		}
	}
	ui.Successf("Generated migration %s with %d statement(s):", version+"_"+name, len(result.Up))
	for _, f := range files {
		fmt.Printf("  %s\n", f.Path)
	}
	fmt.Println()
	fmt.Println("Review the SQL and plan report, then apply with `neutron migrate`.")
	return nil
}

func riskLabel(op db.PlanOperation) string {
	switch {
	case op.DataLoss:
		return "data-loss"
	case op.Destructive:
		return "destructive"
	default:
		return "risky"
	}
}

// loadedSchema is a desired-schema document in either supported format:
// exactly one field is non-nil. Version 1 is the legacy exportSchema shape;
// version 2 is the cross-language contract (contracts/data/).
type loadedSchema struct {
	V1 *db.Schema
	V2 *db.V2Document
}

func loadSchemaDocument(path string) (loadedSchema, error) {
	var out loadedSchema
	data, err := os.ReadFile(path)
	if err != nil {
		return out, fmt.Errorf("read schema %s: %w", path, err)
	}
	if version, vErr := db.DetectSchemaVersion(data); vErr == nil && version == db.SchemaDocumentVersionV2 {
		doc, err := db.ParseV2Document(data)
		if err != nil {
			return out, fmt.Errorf("invalid schema %s (version 2): %w", path, err)
		}
		out.V2 = doc
		return out, nil
	}
	var schema db.Schema
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&schema); err != nil {
		return out, fmt.Errorf("parse schema %s: %w (schema JSON must match the version 1 format exactly — check for misspelled fields)", path, err)
	}
	if err := db.ValidateSchema(&schema); err != nil {
		return out, fmt.Errorf("invalid schema %s: %w", path, err)
	}
	out.V1 = &schema
	return out, nil
}

// loadSchemaJSON loads a version 1 schema document (the legacy exportSchema
// shape). Version 2 documents are rejected here; commands that support both
// formats use loadSchemaDocument.
func loadSchemaJSON(path string) (db.Schema, error) {
	loaded, err := loadSchemaDocument(path)
	if err != nil {
		return db.Schema{}, err
	}
	if loaded.V2 != nil {
		return db.Schema{}, fmt.Errorf(
			"schema %s is a schema document v2 (canonical SHA-256 %s); this path requires the version 1 exportSchema output",
			path, loaded.V2.SHA256Hex)
	}
	return *loaded.V1, nil
}

// computeSchemaPlan diffs the desired document (either format) against the
// live database. Version 1 keeps the exact historical behavior; version 2
// uses introspection + diff over the schema contract v2 with a live twin
// normalizer for expression equivalence.
func computeSchemaPlan(ctx context.Context, client *db.Client, loaded loadedSchema, renames map[string]string, allowDestructive bool) (db.DiffResult, error) {
	if loaded.V1 != nil {
		actual, err := client.IntrospectSchema(ctx)
		if err != nil {
			return db.DiffResult{}, fmt.Errorf("introspect: %w", err)
		}
		return db.DiffSchema(*loaded.V1, actual, db.DiffOptions{
			Renames:          renames,
			AllowDestructive: allowDestructive,
		})
	}

	actual, err := client.IntrospectV2(ctx)
	if err != nil {
		return db.DiffResult{}, fmt.Errorf("introspect: %w", err)
	}
	norm, err := client.NewTwinNormalizer(ctx)
	if err != nil {
		return db.DiffResult{}, err
	}
	defer norm.Close()
	return db.DiffV2Document(ctx, loaded.V2, actual, db.DiffV2Options{
		Renames:          renames,
		AllowDestructive: allowDestructive,
		Normalizer:       norm,
	})
}

// parseRenames converts "table.old>table.new" flags into the diff map keyed
// by "table.new" (the desired column name). Version 2 documents use the
// schema-qualified form "schema.table.old>schema.table.new".
func parseRenames(flags []string) (map[string]string, error) {
	if len(flags) == 0 {
		return nil, nil
	}
	renames := make(map[string]string, len(flags))
	for _, f := range flags {
		parts := strings.SplitN(f, ">", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("--rename expects table.old>table.new, got %q", f)
		}
		from := strings.TrimSpace(parts[0])
		to := strings.TrimSpace(parts[1])
		if !strings.Contains(from, ".") || !strings.Contains(to, ".") {
			return nil, fmt.Errorf("--rename values must include the table name (table.column), got %q", f)
		}
		if !strings.HasPrefix(to, strings.SplitN(from, ".", 2)[0]+".") {
			return nil, fmt.Errorf("--rename %q changes tables; renames stay within one table", f)
		}
		if prev, dup := renames[to]; dup {
			return nil, fmt.Errorf("--rename: duplicate rename target %q (already %s)", to, prev)
		}
		renames[to] = strings.SplitN(from, ".", 2)[1]
	}
	return renames, nil
}

// parseRenamesV2 parses schema-qualified rename flags for schema document
// v2: "schema.table.old>schema.table.new". Unqualified two-part values are
// accepted only when unambiguous is non-nil and exactly one candidate table
// matches.
func parseRenamesV2(flags []string, unambiguous func(table string) (string, bool)) (map[string]string, error) {
	if len(flags) == 0 {
		return nil, nil
	}
	renames := make(map[string]string, len(flags))
	for _, f := range flags {
		parts := strings.SplitN(f, ">", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("--rename expects schema.table.old>schema.table.new, got %q", f)
		}
		from := strings.TrimSpace(parts[0])
		to := strings.TrimSpace(parts[1])
		fromParts, toParts := strings.Split(from, "."), strings.Split(to, ".")
		if len(fromParts) != len(toParts) {
			return nil, fmt.Errorf("--rename %q mixes qualified and unqualified names", f)
		}
		switch len(fromParts) {
		case 3:
			// qualified: schema.table.column
		case 2:
			if unambiguous == nil {
				return nil, fmt.Errorf("--rename %q: schema document v2 needs schema-qualified renames (schema.table.column)", f)
			}
			schema, ok := unambiguous(fromParts[0])
			if !ok {
				return nil, fmt.Errorf("--rename %q: table %q is ambiguous or unknown across the document's schemas; use schema.table.column", f, fromParts[0])
			}
			fromParts = append([]string{schema}, fromParts...)
			toParts = append([]string{schema}, toParts...)
		default:
			return nil, fmt.Errorf("--rename expects schema.table.old>schema.table.new, got %q", f)
		}
		if fromParts[0] != toParts[0] || fromParts[1] != toParts[1] {
			return nil, fmt.Errorf("--rename %q changes tables; renames stay within one table", f)
		}
		key := toParts[0] + "." + toParts[1] + "." + toParts[2]
		source := fromParts[2]
		if prev, dup := renames[key]; dup {
			return nil, fmt.Errorf("--rename: duplicate rename target %q (already %s)", key, prev)
		}
		renames[key] = source
	}
	return renames, nil
}

func reverseStrings(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[len(in)-1-i] = s
	}
	return out
}
