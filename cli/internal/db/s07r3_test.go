package db

import (
	"strings"
	"testing"
)

// TestS07R3SetLocalAllowlist: SET LOCAL may set lock_timeout,
// statement_timeout, maintenance_work_mem and work_mem (none changes
// lexing or name resolution); other settings and set_config stay refused
// (S07 review-3 R4).
func TestS07R3SetLocalAllowlist(t *testing.T) {
	for _, sql := range []string{
		"SET LOCAL lock_timeout = '5s'",
		"SET LOCAL statement_timeout = 0",
		"SET LOCAL maintenance_work_mem = '256MB'",
		"SET LOCAL work_mem = '64MB'",
		`SET LOCAL "work_mem" TO '64MB'`,
	} {
		if err := CheckStatementAllowlist(sql); err != nil {
			t.Errorf("%q refused: %v", sql, err)
		}
	}
	for _, sql := range []string{
		"SET LOCAL search_path = other",
		"SET LOCAL client_encoding = 'LATIN1'",
		"SET LOCAL role app",
		"SELECT set_config('work_mem', '64MB', true)",
	} {
		err := CheckStatementAllowlist(sql)
		if err == nil || !strings.Contains(err.Error(), "work_mem") {
			t.Errorf("%q: got %v, want a refusal naming the allowed settings", sql, err)
		}
	}
}
