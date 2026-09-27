package cmd

import (
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

func TestBaselineCoversOnlyAppliedMigrations(t *testing.T) {
	cases := []struct {
		name    string
		covers  []string
		applied []string
		shape   db.HistoryShape
		refuse  []string // substrings of the refusal; nil = accepted
	}{
		{"no files, no history", nil, nil, db.HistoryAbsent, nil},
		{"no files, legacy history", nil, []string{"1", "2"}, db.HistoryLegacyInteger, nil},
		{"all applied", []string{"001", "002"}, []string{"001", "002"}, db.HistoryV2Text, nil},
		{"applied history without a file", []string{"002"}, []string{"001", "002"}, db.HistoryV2Text, nil},
		{"one unapplied", []string{"001", "002"}, []string{"001"}, db.HistoryV2Text, []string{"002 are not applied"}},
		{"no history", []string{"001"}, nil, db.HistoryAbsent, []string{"001 are not applied"}},
		{"version text is exact", []string{"001"}, []string{"1"}, db.HistoryV2Text, []string{"001 are not applied"}},
		{"legacy text", []string{"001"}, []string{"001"}, db.HistoryLegacyText, []string{"legacy-text", "neutron migrate adopt"}},
		{"legacy integer", []string{"001"}, []string{"1"}, db.HistoryLegacyInteger, []string{"legacy-integer", "neutron migrate adopt"}},
		{"sdk integer", []string{"001"}, []string{"1"}, db.HistoryV2Integer, []string{"v2-integer", "cannot read"}},
	}
	for _, c := range cases {
		err := baselineCoversApplied(c.covers, c.applied, c.shape)
		if c.refuse == nil {
			if err != nil {
				t.Errorf("%s: refused: %v", c.name, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: accepted, want a refusal", c.name)
			continue
		}
		for _, want := range c.refuse {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: refusal %q does not mention %q", c.name, err, want)
			}
		}
	}
}
