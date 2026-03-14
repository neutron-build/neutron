package cmd

import (
	"testing"
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
