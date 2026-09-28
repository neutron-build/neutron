package studio

import "testing"

// Unit test for auto-assigned column detection, which needs no database.
// The v2 mutation protocol's live behavior is covered by the rows_v2 and
// commit_v2 end-to-end tests.

func TestAutoAssigned(t *testing.T) {
	cases := []struct {
		name string
		meta tableColumnMeta
		want bool
	}{
		{"plain column", tableColumnMeta{Name: "id"}, false},
		{"explicit value default", tableColumnMeta{Name: "flag", DefaultExpr: "true"}, false},
		{"identity always", tableColumnMeta{Name: "id", Identity: "a"}, true},
		{"identity by default", tableColumnMeta{Name: "id", Identity: "d"}, true},
		{"stored generated", tableColumnMeta{Name: "total", Generated: "s"}, true},
		{"serial nextval default", tableColumnMeta{Name: "id", DefaultExpr: "nextval('t_id_seq'::regclass)"}, true},
		{"nextval uppercase", tableColumnMeta{Name: "id", DefaultExpr: "NEXTVAL('t_id_seq'::regclass)"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.meta.autoAssigned(); got != tc.want {
				t.Errorf("autoAssigned() = %v, want %v", got, tc.want)
			}
		})
	}
}
