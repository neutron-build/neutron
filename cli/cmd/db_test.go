package cmd

import (
	"testing"
)

func TestDBCommand(t *testing.T) {
	if dbCmd.Use != "db" {
		t.Errorf("dbCmd.Use = %q, want %q", dbCmd.Use, "db")
	}
	if dbCmd.Short == "" {
		t.Error("dbCmd.Short is empty")
	}
}

func TestDBSubcommands(t *testing.T) {
	subCmds := map[string]*struct {
		found bool
	}{
		"start":  {},
		"stop":   {},
		"status": {},
		"reset":  {},
	}

	for _, cmd := range dbCmd.Commands() {
		if _, ok := subCmds[cmd.Use]; ok {
			subCmds[cmd.Use].found = true
		}
	}

	for name, info := range subCmds {
		if !info.found {
			t.Errorf("db subcommand %q not registered", name)
		}
	}
}

func TestDBStartFlags(t *testing.T) {
	tests := []struct {
		name     string
		flagName string
	}{
		{"port", "port"},
		{"data-dir", "data-dir"},
		{"memory", "memory"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag := dbStartCmd.Flags().Lookup(tt.flagName)
			if flag == nil {
				t.Errorf("dbStartCmd missing --%s flag", tt.flagName)
			}
		})
	}
}

func TestDBResetFlags(t *testing.T) {
	flag := dbResetCmd.Flags().Lookup("force")
	if flag == nil {
		t.Fatal("dbResetCmd missing --force flag")
	}
}

func TestDBStartCmd(t *testing.T) {
	if dbStartCmd.RunE == nil {
		t.Error("dbStartCmd.RunE is nil")
	}
	if dbStartCmd.Short == "" {
		t.Error("dbStartCmd.Short is empty")
	}
}

func TestDBStopCmd(t *testing.T) {
	if dbStopCmd.RunE == nil {
		t.Error("dbStopCmd.RunE is nil")
	}
}

func TestDBStatusCmd(t *testing.T) {
	if dbStatusCmd.RunE == nil {
		t.Error("dbStatusCmd.RunE is nil")
	}
}

func TestDBResetCmd(t *testing.T) {
	if dbResetCmd.RunE == nil {
		t.Error("dbResetCmd.RunE is nil")
	}
}
