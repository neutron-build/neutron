package cmd

import (
	"strings"
	"testing"
)

// TestS07R3MigrateHelpStatesWhenSettingsAreChecked: the help says when the
// session settings are verified, matching the pipelined path (S07 review-3
// R2), and lists work_mem among the SET LOCAL settings (R4).
func TestS07R3MigrateHelpStatesWhenSettingsAreChecked(t *testing.T) {
	help := strings.Join(strings.Fields(migrateCmd.Long), " ")
	for _, want := range []string{
		"checked before the first statement and after each statement, or after each pipelined batch inside a transaction, before commit",
		"maintenance_work_mem and work_mem",
	} {
		if !strings.Contains(help, want) {
			t.Errorf("migrate --help must say %q", want)
		}
	}
}
