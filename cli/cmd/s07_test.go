package cmd

import (
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// TestS07DownRefusalNamesThePlanGrade: a comment-only down file is refused
// with the grade its plan actually records, not "irreversible" for every
// plan (Q09 review-1 INFO 7).
func TestS07DownRefusalNamesThePlanGrade(t *testing.T) {
	stub := &db.MigrationFile{Version: "002", Name: "enum_values", SQL: "-- enum values cannot be removed\n"}
	cases := []struct {
		name    string
		plan    *db.PlanArtifact
		want    []string
		notWant string
	}{
		{"manual plan", &db.PlanArtifact{Risk: db.PlanRisk{StatementCount: 1, OverallReversibility: db.ReversibilityManual}},
			[]string{"its plan grades it manual", "write the down SQL by hand"}, "irreversible"},
		{"irreversible plan", &db.PlanArtifact{Risk: db.PlanRisk{StatementCount: 1, IrreversibleCount: 1, OverallReversibility: db.ReversibilityIrreversible}},
			[]string{"its plan grades it irreversible", "forward-fix"}, "by hand"},
		{"no plan", nil,
			[]string{"no plan report grades it", "write the down SQL by hand"}, "irreversible"},
		{"IRREVERSIBLE stub, no plan", nil,
			[]string{"the file itself marks it irreversible", "no plan report grades it", "forward-fix"}, "by hand"},
		// S07 review-1 F6: the word outside the marker form is not a marker.
		{"word in prose, no plan", nil,
			[]string{"no plan report grades it", "write the down SQL by hand"}, "marks it irreversible"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			down := stub
			if strings.HasPrefix(c.name, "IRREVERSIBLE") {
				down = &db.MigrationFile{Version: "002", Name: "enum_values", SQL: "-- IRREVERSIBLE: no restoration possible\n"}
			}
			if strings.HasPrefix(c.name, "word in prose") {
				down = &db.MigrationFile{Version: "002", Name: "enum_values", SQL: "-- enum value 'irreversible' cannot be removed; this change is not IRREVERSIBLE by nature\n"}
			}
			err := validateDownReversibility(pendingMigration{File: db.MigrationFile{Version: "002", Name: "enum_values"}, DownFile: down, Plan: c.plan})
			if err == nil {
				t.Fatal("a comment-only down must be refused")
			}
			msg := err.Error()
			for _, w := range c.want {
				if !strings.Contains(msg, w) {
					t.Errorf("refusal must say %q: %s", w, msg)
				}
			}
			if strings.Contains(msg, c.notWant) {
				t.Errorf("refusal must not say %q: %s", c.notWant, msg)
			}
		})
	}
}
