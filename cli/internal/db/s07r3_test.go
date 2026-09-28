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

// TestS07R3DataLossOutsideTables: TRUNCATE, DROP MATERIALIZED VIEW,
// DROP DOMAIN ... CASCADE and a composite type's DROP/ALTER ATTRIBUTE are
// destructive and data-losing, so they need --allow-destructive (S07
// review-3 R5). Look-alikes that keep data are not.
func TestS07R3DataLossOutsideTables(t *testing.T) {
	for _, sql := range []string{
		"TRUNCATE t",
		"TRUNCATE TABLE s.t RESTART IDENTITY",
		"DROP MATERIALIZED VIEW mv",
		"DROP MATERIALIZED VIEW IF EXISTS s.mv",
		"DROP DOMAIN d CASCADE",
		"ALTER TYPE comp DROP ATTRIBUTE a",
		"ALTER TYPE s.comp DROP ATTRIBUTE IF EXISTS a CASCADE",
		"ALTER TYPE comp ALTER ATTRIBUTE a TYPE bigint",
		"ALTER TYPE comp ALTER ATTRIBUTE a SET DATA TYPE bigint CASCADE",
		"ALTER TYPE comp ADD ATTRIBUTE b int, DROP ATTRIBUTE a",
	} {
		if d, l := ClassifyStatementRisk(sql); !d || !l {
			t.Errorf("ClassifyStatementRisk(%q) = %v, %v; want destructive and data-losing", sql, d, l)
		}
	}
	for _, sql := range []string{
		"DROP DOMAIN d",
		"ALTER TYPE comp ADD ATTRIBUTE b int",
		"ALTER TYPE comp RENAME ATTRIBUTE a TO b",
		"ALTER TYPE mood ADD VALUE 'x'",
		"ALTER TYPE mood RENAME TO feeling",
		"SELECT 'truncate t'",
	} {
		if d, l := ClassifyStatementRisk(sql); d || l {
			t.Errorf("ClassifyStatementRisk(%q) = %v, %v; want neither", sql, d, l)
		}
	}
}
