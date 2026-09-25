package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/ui"
	"github.com/spf13/cobra"
)

func init() {
	dbPushCmd.Flags().String("schema", "neutron.schema.json", "schema JSON exported from @neutron-build/sql (exportSchema)")
	dbPushCmd.Flags().StringArray("rename", nil, "explicit column rename: table.old>table.new (repeatable)")
	dbPushCmd.Flags().Duration("timeout", 60*time.Second, "time budget for the push")
	dbPushCmd.Flags().Bool("force", false, "push even when a migration history exists (does NOT permit destructive changes or bypass any protection)")
	dbPushCmd.Flags().Bool("allow-destructive", false, "acknowledge data loss: permit dropping tables, columns, and indexes that exist in the database but are absent from the schema (neutron-internal metadata and extension-owned objects are never touched)")
	dbPushCmd.Flags().Bool("dry-run", false, "print the SQL without applying")
	dbCmd.AddCommand(dbPushCmd)
}

var dbPushCmd = &cobra.Command{
	Use:   "push",
	Short: "Push the schema directly to the database (no migration files)",
	Long: `For prototyping: diffs the desired schema document against the live database and applies the changes immediately, in a single transaction (a mid-plan failure rolls everything back). Refuses to run when a migration history exists unless --force.

Push takes the migration runner's pinned advisory-lock session for the history check, plan and apply: a push never interleaves with a running migration. Dry-run stays lockless (it reports only).

Schema documents: version 2 (the cross-language contract in contracts/data/) plans through full catalog introspection — qualified schemas, composite PK/unique/check/foreign-key constraints, indexes with predicates and expressions, enums, arrays and views; version 1 (legacy @neutron-build/sql exportSchema output) keeps its historical behavior.

Safety rails (not bypassed by any flag): neutron-internal tables (_neutron_*), extension-owned objects, and schema metadata are never dropped or modified; objects absent from the schema are only dropped with --allow-destructive as an explicit acknowledgement of data loss; catalog structures this diff engine cannot represent faithfully are rejected with an error instead of being silently "synchronized".`,
	RunE: func(cmd *cobra.Command, args []string) error { return reportRunE(runDBPush(cmd, args)) },
}

func runDBPush(cmd *cobra.Command, args []string) error {
	schemaPath, _ := cmd.Flags().GetString("schema")
	renameFlags, _ := cmd.Flags().GetStringArray("rename")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	force, _ := cmd.Flags().GetBool("force")
	allowDestructive, _ := cmd.Flags().GetBool("allow-destructive")
	dryRun, _ := cmd.Flags().GetBool("dry-run")

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

	// Push is an apply flow: it takes the same pinned advisory-lock session
	// the migration runner uses (M04 protocol — never bypassed, never
	// re-implemented), so a push can never interleave with a running
	// migration's history read or DDL. Dry-run reports only and stays
	// lockless.
	if dryRun {
		return dbPushDryRun(ctx, client, loaded, renames, allowDestructive)
	}

	sess, err := client.LockMigrations(ctx)
	if err != nil {
		return err
	}
	defer sess.Release()

	if !force {
		has, err := client.HasMigrationHistory(ctx)
		if err != nil {
			return fmt.Errorf("check migration history: %w", err)
		}
		if has {
			return fmt.Errorf("this database has a migration history (_neutron_migrations) — use `neutron migrate` or pass --force to push anyway")
		}
	}

	result, err := computeSchemaPlan(ctx, client, loaded, renames, allowDestructive)
	if err != nil {
		return err
	}
	if err := verifyDocumentExtensionCapabilities(ctx, client, loaded); err != nil {
		return err
	}

	for _, w := range result.Warnings {
		ui.Warnf("%s", w)
	}

	if len(result.Up) == 0 {
		if len(result.Warnings) > 0 {
			ui.Infof("No applicable changes; see the notes above — objects reported as left untouched are not in sync with the schema.")
		} else {
			ui.Infof("Schema is already in sync.")
		}
		return nil
	}

	// Apply on the locked session: the plan's transaction runs on the same
	// pinned connection that holds the advisory lock.
	if err := sess.ApplyStatementsTx(ctx, result.Up, func(stmt string) {
		ui.Infof("applied: %s", firstLine(stmt))
	}); err != nil {
		ui.Errorf("%v", err)
		ui.Errorf("Rolled back — the database is unchanged (the whole plan runs in one transaction).")
		return fmt.Errorf("push failed atomically")
	}

	ui.Successf("Pushed %d statement(s) in one transaction.", len(result.Up))
	return nil
}

func dbPushDryRun(ctx context.Context, client *db.Client, loaded loadedSchema, renames map[string]string, allowDestructive bool) error {
	result, err := computeSchemaPlan(ctx, client, loaded, renames, allowDestructive)
	if err != nil {
		return err
	}
	if err := verifyDocumentExtensionCapabilities(ctx, client, loaded); err != nil {
		return err
	}
	for _, w := range result.Warnings {
		ui.Warnf("%s", w)
	}
	if len(result.Up) == 0 {
		if len(result.Warnings) > 0 {
			ui.Infof("No applicable changes; see the notes above — objects reported as left untouched are not in sync with the schema.")
		} else {
			ui.Infof("Schema is already in sync.")
		}
		return nil
	}
	fmt.Println(strings.Join(result.Up, ";\n") + ";")
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
