package cmd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/spf13/cobra"
)

func TestMigrateCommand(t *testing.T) {
	if migrateCmd.Use != "migrate" {
		t.Errorf("migrateCmd.Use = %q, want %q", migrateCmd.Use, "migrate")
	}
	if migrateCmd.Short == "" {
		t.Error("migrateCmd.Short is empty")
	}
	if migrateCmd.RunE == nil {
		t.Error("migrateCmd.RunE is nil")
	}
}

func TestMigrateSubcommands(t *testing.T) {
	subCmds := map[string]bool{
		"status": false,
		"create": false,
	}

	for _, cmd := range migrateCmd.Commands() {
		if _, ok := subCmds[cmd.Use]; ok {
			subCmds[cmd.Use] = true
		} else {
			// create has the format "create <name>"
			for name := range subCmds {
				if cmd.Name() == name {
					subCmds[name] = true
				}
			}
		}
	}

	for name, found := range subCmds {
		if !found {
			t.Errorf("migrate subcommand %q not registered", name)
		}
	}
}

func TestMigrateDirFlag(t *testing.T) {
	flag := migrateCmd.Flags().Lookup("dir")
	if flag == nil {
		t.Fatal("migrateCmd missing --dir flag")
	}
	if flag.DefValue != "migrations" {
		t.Errorf("--dir default = %q, want %q", flag.DefValue, "migrations")
	}
}

func TestMigrateCreateRequiresName(t *testing.T) {
	// cobra.ExactArgs(1) should be set
	err := migrateCreateCmd.Args(migrateCreateCmd, []string{})
	if err == nil {
		t.Error("expected error when no migration name provided")
	}
	err = migrateCreateCmd.Args(migrateCreateCmd, []string{"add_users"})
	if err != nil {
		t.Errorf("expected no error for 1 arg, got: %v", err)
	}
}

func TestMigrateStatusDirFlag(t *testing.T) {
	flag := migrateStatusCmd.Flags().Lookup("dir")
	if flag == nil {
		t.Fatal("migrateStatusCmd missing --dir flag")
	}
	if flag.DefValue != "migrations" {
		t.Errorf("--dir default = %q, want %q", flag.DefValue, "migrations")
	}
}

func appliedRecords(versions ...string) []db.MigrationRecord {
	records := make([]db.MigrationRecord, len(versions))
	for i, v := range versions {
		records[i] = db.MigrationRecord{Version: v, Name: "migration_" + v}
	}
	return records
}

func downFilesMap(files map[string]string) []db.MigrationFile {
	result := make([]db.MigrationFile, 0, len(files))
	for version, sql := range files {
		result = append(result, db.MigrationFile{Version: version, Name: "migration_" + version, SQL: sql})
	}
	return result
}

func TestSelectRevertFrontierNewestFirst(t *testing.T) {
	applied := appliedRecords("001", "002", "003")
	downs := downFilesMap(map[string]string{
		"001": "DROP 1;",
		"002": "DROP 2;",
		"003": "DROP 3;",
	})

	toRevert, err := selectRevertFrontier(applied, downs, 2)
	if err != nil {
		t.Fatalf("selectRevertFrontier() error: %v", err)
	}
	if len(toRevert) != 2 {
		t.Fatalf("got %d migrations to revert, want 2", len(toRevert))
	}
	if toRevert[0].Version != "003" || toRevert[1].Version != "002" {
		t.Errorf("revert order = %s, %s; want 003 then 002 (newest first)", toRevert[0].Version, toRevert[1].Version)
	}
}

func TestSelectRevertFrontierCountExceedsApplied(t *testing.T) {
	applied := appliedRecords("001", "002")
	downs := downFilesMap(map[string]string{"001": "DROP 1;", "002": "DROP 2;"})

	toRevert, err := selectRevertFrontier(applied, downs, 5)
	if err != nil {
		t.Fatalf("selectRevertFrontier() error: %v", err)
	}
	if len(toRevert) != 2 {
		t.Errorf("got %d migrations to revert, want 2 (all applied)", len(toRevert))
	}
}

func TestSelectRevertFrontierNothingApplied(t *testing.T) {
	downs := downFilesMap(map[string]string{"001": "DROP 1;"})

	toRevert, err := selectRevertFrontier(nil, downs, 1)
	if err != nil {
		t.Fatalf("selectRevertFrontier() error: %v", err)
	}
	if len(toRevert) != 0 {
		t.Errorf("got %d migrations to revert, want 0", len(toRevert))
	}
}

// The core case: a newer applied migration whose down file is missing must
// abort the rollback, not silently let an older one be reverted beneath it.
func TestSelectRevertFrontierMissingDownAborts(t *testing.T) {
	applied := appliedRecords("001", "002", "003")
	downs := downFilesMap(map[string]string{
		"001": "DROP 1;",
		"002": "DROP 2;",
		// no down file for 003
	})

	_, err := selectRevertFrontier(applied, downs, 1)
	if err == nil {
		t.Fatal("expected error when newest applied migration has no down file")
	}
	if !strings.Contains(err.Error(), "003") {
		t.Errorf("error should name the missing version 003, got: %v", err)
	}
}

// A missing down file for an older migration must also abort when it is part
// of the requested frontier.
func TestSelectRevertFrontierMissingDownInMiddleAborts(t *testing.T) {
	applied := appliedRecords("001", "002", "003")
	downs := downFilesMap(map[string]string{
		"001": "DROP 1;",
		"003": "DROP 3;",
		// no down file for 002
	})

	_, err := selectRevertFrontier(applied, downs, 2)
	if err == nil {
		t.Fatal("expected error when a frontier migration has no down file")
	}
	if !strings.Contains(err.Error(), "002") {
		t.Errorf("error should name the missing version 002, got: %v", err)
	}
}

func TestSelectRevertFrontierEmptyDownAborts(t *testing.T) {
	applied := appliedRecords("001", "002")
	downs := downFilesMap(map[string]string{
		"001": "DROP 1;",
		"002": "   ",
	})

	_, err := selectRevertFrontier(applied, downs, 1)
	if err == nil {
		t.Fatal("expected error when frontier down migration is empty")
	}
	if !strings.Contains(err.Error(), "002") {
		t.Errorf("error should name the empty version 002, got: %v", err)
	}
}

// Pending (not applied) migrations with down files on disk must never be
// selected.
func TestSelectRevertFrontierIgnoresPending(t *testing.T) {
	applied := appliedRecords("001")
	downs := downFilesMap(map[string]string{"001": "DROP 1;", "002": "DROP 2;"})

	toRevert, err := selectRevertFrontier(applied, downs, 3)
	if err != nil {
		t.Fatalf("selectRevertFrontier() error: %v", err)
	}
	if len(toRevert) != 1 || toRevert[0].Version != "001" {
		t.Errorf("got %v, want only 001", toRevert)
	}
}

// Rollback order must also survive versions past the padding width — the
// frontier is newest-first numerically, not lexicographically (audit
// neutron-06).
func TestSelectRevertFrontierNumericOrderPastPaddingWidth(t *testing.T) {
	applied := appliedRecords("998", "999", "1000", "1001")
	downs := downFilesMap(map[string]string{
		"998":  "DROP 998;",
		"999":  "DROP 999;",
		"1000": "DROP 1000;",
		"1001": "DROP 1001;",
	})

	toRevert, err := selectRevertFrontier(applied, downs, 3)
	if err != nil {
		t.Fatalf("selectRevertFrontier() error: %v", err)
	}
	want := []string{"1001", "1000", "999"}
	for i, v := range want {
		if toRevert[i].Version != v {
			t.Errorf("revert order[%d] = %s, want %s (all: %v)", i, toRevert[i].Version, v, toRevert)
		}
	}
}

// The rollback count is strictly numeric: an integer prefix like "1junk"
// used to be accepted by fmt.Sscanf (audit neutron-03).
func TestParseRevertCountRejectsPartialNumbers(t *testing.T) {
	for _, arg := range []string{"1junk", "junk", "1.5", "-1", "0", "", " 2", "2 "} {
		t.Run(arg, func(t *testing.T) {
			if _, err := parseRevertCount([]string{arg}); err == nil {
				t.Errorf("parseRevertCount(%q) accepted a non-strict count", arg)
			}
		})
	}

	if n, err := parseRevertCount(nil); err != nil || n != 1 {
		t.Errorf("parseRevertCount(nil) = %d, %v; want 1, nil", n, err)
	}
	if n, err := parseRevertCount([]string{"3"}); err != nil || n != 3 {
		t.Errorf("parseRevertCount(3) = %d, %v; want 3, nil", n, err)
	}
}

// The migration commands carry a documented --timeout budget, and 0 means
// caller-controlled with no deadline (audit neutron-03).
func TestMigrateTimeoutFlags(t *testing.T) {
	for _, c := range []*cobra.Command{migrateCmd, migrateDownCmd, migrateStatusCmd} {
		flag := c.Flags().Lookup("timeout")
		if flag == nil {
			t.Errorf("%s missing --timeout flag", c.Name())
			continue
		}
		if flag.Usage == "" {
			t.Errorf("%s --timeout has no usage text", c.Name())
		}
	}
}

// commandContext must derive from the command's context so cancellation
// propagates, and honor the timeout budget when one is set.
func TestCommandContextDerivesFromCommandContext(t *testing.T) {
	parent, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	c := &cobra.Command{Use: "test"}
	c.SetContext(parent)

	gotCtx, gotCancel := commandContext(c)
	defer gotCancel()
	select {
	case <-gotCtx.Done():
		t.Fatal("context canceled before the parent canceled")
	default:
	}
	parentCancel()
	select {
	case <-gotCtx.Done():
	default:
		t.Error("child context did not observe parent cancellation")
	}
}

func TestCommandContextAppliesTimeoutBudget(t *testing.T) {
	c := &cobra.Command{Use: "test"}
	c.Flags().Duration("timeout", time.Minute, "")
	if err := c.ParseFlags(nil); err != nil {
		t.Fatal(err)
	}

	gotCtx, cancel := commandContext(c)
	defer cancel()

	deadline, ok := gotCtx.Deadline()
	if !ok {
		t.Fatal("no deadline set despite a positive --timeout")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > time.Minute {
		t.Errorf("deadline = %v, want within one minute", remaining)
	}
}
