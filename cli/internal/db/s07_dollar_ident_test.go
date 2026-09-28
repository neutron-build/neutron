package db

import (
	"reflect"
	"testing"
)

// TestS07DollarContinuesIdentifier: every scanner reads '$' after an
// identifier start as part of the identifier (PostgreSQL's ident_cont), so
// "a$x$" never opens a dollar quote that hides separators or keywords (S07
// review-1 F2). A '$' at a token start still opens one, and a tag cannot
// start with a digit.
func TestS07DollarContinuesIdentifier(t *testing.T) {
	const repro = "create view v as select 1 as a$x$; drop table if exists public.victim; select 1 as b$x$"
	var exec []string
	for _, s := range SplitSQLStatements(repro) {
		if hasExecutableSQL(s) {
			exec = append(exec, s)
		}
	}
	want := []string{"create view v as select 1 as a$x$", "drop table if exists public.victim", "select 1 as b$x$"}
	if !reflect.DeepEqual(exec, want) {
		t.Errorf("split = %q, want %q", exec, want)
	}
	// Non-ASCII bytes are identifier characters too.
	if got := len(SplitSQLStatements("select 1 as é$x$; drop table victim; select 1 as b$x$")); got != 3 {
		t.Errorf("non-ASCII identifier: %d statements, want 3", got)
	}
	// Quotes at a token start are unchanged.
	for _, sql := range []string{"select $$;$$", "select $q$;x;$q$", "select $é$;$é$"} {
		if got := len(SplitSQLStatements(sql)); got != 1 {
			t.Errorf("%q: %d statements, want 1 (dollar quote)", sql, got)
		}
	}
	// "$1$" is a parameter followed by '$', never a quote.
	if got := len(SplitSQLStatements("select $1$; select 2; select $1$")); got != 3 {
		t.Errorf("digit tag: %d statements, want 3", got)
	}
	// The drop is visible to the destructive classification and the guard.
	if targets := GuardStatementTargets("drop table if exists public.victim"); len(targets) != 1 {
		t.Errorf("guard targets = %v", targets)
	}

	// The version gate sees SET EXPRESSION after such an identifier (Q09
	// review-2 INFO 5, second half).
	if major, _ := StatementMinServerMajor(`alter table t alter column g$a$ set expression as (x * 2)`); major != SetExpressionMinServerMajor {
		t.Errorf("version gate after g$a$: %d, want %d", major, SetExpressionMinServerMajor)
	}

	// The rename scanner reads a$x$ as one identifier.
	idents, ok := sqlIdentifiers("a$x$ > 0 and b$x$ < 1")
	if !ok || !reflect.DeepEqual(idents, []string{"a$x$", "and", "b$x$"}) {
		t.Errorf("sqlIdentifiers = %q, %v", idents, ok)
	}

	// Unquoted identifiers fold ASCII only.
	if toks := significantTokens("DROP TABLE ÉtÉ"); len(toks) != 3 || toks[2].text != "ÉtÉ" {
		t.Errorf("non-ASCII word = %+v", toks)
	}
}
