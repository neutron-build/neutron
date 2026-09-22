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
	migrateGenerateCmd.Flags().StringArray("rename", nil, "explicit column rename: table.old>table.new (repeatable)")
	migrateGenerateCmd.Flags().Duration("timeout", 30*time.Second, "time budget for introspection")
	migrateGenerateCmd.Flags().Bool("allow-destructive", false, "acknowledge data loss: include DROP statements for tables, columns, and indexes that exist in the database but are absent from the schema (neutron-internal metadata and extension-owned objects are never touched)")
	migrateCmd.AddCommand(migrateGenerateCmd)
}

var migrateGenerateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate a migration by diffing the schema against the database",
	Long: `Compares the exported schema JSON with the live database (information_schema) and writes an .up.sql/.down.sql pair runnable by ` + "`neutron migrate`" + `.

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

func runMigrateGenerate(cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	schemaPath, _ := cmd.Flags().GetString("schema")
	name, _ := cmd.Flags().GetString("name")
	renameFlags, _ := cmd.Flags().GetStringArray("rename")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	allowDestructive, _ := cmd.Flags().GetBool("allow-destructive")

	if name == "" {
		name = "generated"
	}

	desired, err := loadSchemaJSON(schemaPath)
	if err != nil {
		return err
	}

	renames, err := parseRenames(renameFlags)
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

	actual, err := client.IntrospectSchema(ctx)
	if err != nil {
		return fmt.Errorf("introspect: %w", err)
	}

	result, err := db.DiffSchema(desired, actual, db.DiffOptions{
		Renames:          renames,
		AllowDestructive: allowDestructive,
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

func loadSchemaJSON(path string) (db.Schema, error) {
	var schema db.Schema
	data, err := os.ReadFile(path)
	if err != nil {
		return schema, fmt.Errorf("read schema %s: %w", path, err)
	}
	// Schema documents declaring version 2 are validated against the
	// cross-language contract (contracts/data/), but the diff/planning
	// engine still consumes version 1 exports; refuse v2 clearly instead of
	// failing on unknown fields. Version detection lives in internal/db
	// alongside the contract implementation.
	if version, vErr := db.DetectSchemaVersion(data); vErr == nil && version == db.SchemaDocumentVersionV2 {
		check, err := db.ValidateSchemaDocument(data)
		if err != nil {
			return schema, fmt.Errorf("invalid schema %s (version 2): %w", path, err)
		}
		return schema, fmt.Errorf(
			"schema %s is a valid schema document v2 (canonical SHA-256 %s), but planning from version 2 documents is not implemented yet — keep the version 1 exportSchema output for migrate generate and db push",
			path, check.SHA256)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&schema); err != nil {
		return schema, fmt.Errorf("parse schema %s: %w (schema JSON must match the version 1 format exactly — check for misspelled fields)", path, err)
	}
	if err := db.ValidateSchema(&schema); err != nil {
		return schema, fmt.Errorf("invalid schema %s: %w", path, err)
	}
	return schema, nil
}

// parseRenames converts "table.old>table.new" flags into the diff map keyed
// by "table.new" (the desired column name).
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

func reverseStrings(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[len(in)-1-i] = s
	}
	return out
}
