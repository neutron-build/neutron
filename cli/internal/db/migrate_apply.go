package db

// Apply-time statement analysis and session execution primitives (M05).
//
// The runner executes migration files verbatim (M04 protocol), so every
// apply-time check here works on the same text the server will see. The
// single lexical analyzer below (tokenizeSQL) is the ONE source of truth
// for statement shape: it understands -- line comments, nested /* */
// block comments, dollar-quoted strings ($tag$...$tag$, body opaque),
// standard string literals with '' doubling, E'' escape strings and
// quoted identifiers with "" doubling. Every classifier — statement
// splitting, executable-SQL detection, the statement-kind allowlist,
// the ack classifiers, the protected-object guard and postcondition
// extraction — consumes its token stream, never raw text or regexes. A
// statement outside the allowlisted kinds is refused outright; a
// statement inside them is target-guarded, never assumed safe.

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// ---------------------------------------------------------------------------
// The one tokenizer
// ---------------------------------------------------------------------------

// sqlToken is one lexical token of the single SQL analyzer that backs
// every statement-shape decision in this package.
//
//   - kind 'w': bare word, text lowercased (keywords match case-insensitively)
//   - kind 'q': quoted identifier, text unescaped and case-preserved
//   - kind 's': opaque string literal — '...' (with ” doubling), E'...'
//     (with backslash escapes) or $tag$...$tag$; contents never match names
//   - kind 'c': comment — `--` line or /* */ block (nesting per PG rules)
//   - kind 'p': punctuation (single characters; "$" when not a dollar quote)
//
// start/end are byte offsets into the source (end exclusive) so the
// splitter can slice verbatim statement text between token boundaries.
type sqlToken struct {
	kind  byte
	text  string
	start int
	end   int
}

// tokenizeSQL splits SQL text into the token stream above. It is the only
// lexer in the migration path: SplitSQLStatements slices its spans, the
// guard/ack/postcondition classifiers consume its significant tokens, and
// hasExecutableSQL counts its non-comment tokens. Comments are returned as
// 'c' tokens (classifiers strip them via significantTokens); string and
// dollar-quoted bodies are opaque 's' tokens whose contents can never be
// mistaken for keywords or object names.
func tokenizeSQL(s string) []sqlToken {
	var toks []sqlToken
	emit := func(kind byte, text string, start, end int) {
		toks = append(toks, sqlToken{kind: kind, text: text, start: start, end: end})
	}
	i, n := 0, len(s)
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < n && s[i+1] == '-':
			j := strings.IndexByte(s[i:], '\n')
			if j < 0 {
				emit('c', s[i:], i, n)
				i = n
			} else {
				emit('c', s[i:i+j], i, i+j)
				i += j
			}
		case c == '/' && i+1 < n && s[i+1] == '*':
			// PostgreSQL nests block comments: /* /* */ still comment */.
			depth, j := 1, i+2
			for j < n && depth > 0 {
				switch {
				case s[j] == '/' && j+1 < n && s[j+1] == '*':
					depth++
					j += 2
				case s[j] == '*' && j+1 < n && s[j+1] == '/':
					depth--
					j += 2
				default:
					j++
				}
			}
			end := min(j, n)
			emit('c', s[i:end], i, end)
			i = end
		case c == '"':
			j, buf := i+1, strings.Builder{}
			for j < n {
				if s[j] == '"' {
					if j+1 < n && s[j+1] == '"' {
						buf.WriteByte('"')
						j += 2
						continue
					}
					break
				}
				buf.WriteByte(s[j])
				j++
			}
			end := min(j+1, n)
			emit('q', buf.String(), i, end)
			i = end
		case c == '\'' || ((c == 'e' || c == 'E') && i+1 < n && s[i+1] == '\''):
			// Standard-conforming literal ('it''s') or E'...' escape
			// string (backslash escapes: \' and \\).
			esc := c != '\''
			j := i + 1
			if esc {
				j++
			}
			for j < n {
				if esc && s[j] == '\\' {
					j += 2
					continue
				}
				if s[j] == '\'' {
					if j+1 < n && s[j+1] == '\'' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			end := min(j, n)
			emit('s', s[i:end], i, end)
			i = end
		case c == '$':
			if end, ok := dollarQuoteEnd(s, i); ok {
				emit('s', s[i:end], i, end)
				i = end
			} else {
				// Not a dollar-quote opener (e.g. a $1 parameter);
				// plain punctuation.
				emit('p', "$", i, i+1)
				i++
			}
		case isWordChar(c):
			j := i
			for j < n && isWordChar(s[j]) {
				j++
			}
			emit('w', strings.ToLower(s[i:j]), i, j)
			i = j
		default:
			emit('p', string(c), i, i+1)
			i++
		}
	}
	return toks
}

// significantTokens returns the token stream without comments — the stream
// every classifier reads. Comments can therefore never shift a leading
// keyword, split a name list or impersonate punctuation.
func significantTokens(s string) []sqlToken {
	all := tokenizeSQL(s)
	out := all[:0]
	for _, t := range all {
		if t.kind != 'c' {
			out = append(out, t)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Statement splitting
// ---------------------------------------------------------------------------

// SplitSQLStatements splits a SQL document into top-level statements by
// slicing the source at `;` tokens of the single tokenizer — semicolons
// inside strings, comments and dollar-quoted bodies are never split
// points, by construction of the lexer. Fragments keep their verbatim
// text (comments included); blank fragments are dropped and comment-only
// fragments are preserved (callers filter with hasExecutableSQL). A line
// comment that ends a comments-only buffer closes that fragment, so
// leading `--` headers become their own fragments instead of gluing onto
// the next statement.
func SplitSQLStatements(sql string) []string {
	var out []string
	fragStart := 0
	flush := func(end int) {
		if s := strings.TrimSpace(sql[fragStart:end]); s != "" {
			out = append(out, s)
		}
		fragStart = end
	}
	toks := tokenizeSQL(sql)
	for _, t := range toks {
		switch {
		case t.kind == 'p' && t.text == ";":
			// The fragment text excludes the semicolon; scanning resumes
			// after it.
			if s := strings.TrimSpace(sql[fragStart:t.start]); s != "" {
				out = append(out, s)
			}
			fragStart = t.end
		case t.kind == 'c' && strings.HasPrefix(t.text, "--"):
			// A line comment ending a comments-only buffer closes the
			// fragment (migration headers).
			if fragmentIsLineCommentsOnly(sql[fragStart:t.end]) {
				flush(t.end)
			}
		}
	}
	if fragStart < len(sql) {
		flush(len(sql))
	}
	return out
}

// fragmentIsLineCommentsOnly reports whether a fragment so far holds
// nothing but whitespace and `--` comment lines.
func fragmentIsLineCommentsOnly(s string) bool {
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "--") {
			return false
		}
	}
	return strings.TrimSpace(s) != ""
}

// dollarQuoteEnd returns the index just past the closing delimiter of the
// dollar-quoted block opening at start, if any. The tag may be empty ($$).
// An unterminated block consumes the rest (the server will reject it).
func dollarQuoteEnd(sql string, start int) (int, bool) {
	j := start + 1
	n := len(sql)
	for j < n && isDollarTagChar(sql[j]) {
		j++
	}
	if j >= n || sql[j] != '$' {
		return 0, false
	}
	delim := sql[start : j+1]
	if idx := strings.Index(sql[j+1:], delim); idx >= 0 {
		return j + 1 + idx + len(delim), true
	}
	return n, true
}

func isDollarTagChar(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func stripTrailingSemicolon(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, ";")
	return strings.TrimSpace(s)
}

// normalizedStatementText renders the significant token stream as the
// single-space-joined lowercase text the prefix/contains classifiers
// match on — the token-based replacement of the old raw-text
// normalization. Comments vanish (they cannot hide a keyword), string
// and dollar-quote lexemes and quoted identifiers keep their raw shape
// (lowercased, whitespace-collapsed) so containment semantics are
// unchanged, and a quoted identifier stays quoted so it can never match
// a keyword sequence.
func normalizedStatementText(sql string) string {
	toks := significantTokens(sql)
	parts := make([]string, 0, len(toks))
	for _, t := range toks {
		parts = append(parts, collapseSQLSpace(strings.ToLower(t.text)))
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

func collapseSQLSpace(s string) string {
	var b strings.Builder
	space := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			space = true
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteByte(c)
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------

// ClassifyStatementRisk applies the M03 planner's risk classifiers to one
// statement: the same vocabulary plan.json records, so apply-time gating
// propagates generation-time semantics instead of widening them.
func ClassifyStatementRisk(sql string) (destructive, dataLoss bool) {
	return statementDestructive(sql), statementDataLoss(sql)
}

// IsNontransactionalStatement reports whether a statement cannot run inside
// a transaction block (concurrent index operations). Such migrations run
// statement-by-statement on the pinned session with explicit recovery.
func IsNontransactionalStatement(sql string) bool {
	s := normalizedStatementText(sql)
	for _, prefix := range []string{"create index concurrently", "create unique index concurrently", "drop index concurrently"} {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	if strings.HasPrefix(s, "reindex") && strings.Contains(s, "concurrently") {
		return true
	}
	return false
}

// ChangesRowData reports whether a statement can change row data, by its
// deterministic leading tokens. Nontransactional migrations containing such
// statements are refused: their partial outcomes could not be recovered
// without replaying unknown-outcome data changes.
func ChangesRowData(sql string) bool {
	s := normalizedStatementText(sql)
	for _, prefix := range []string{
		"insert", "update", "delete", "merge", "truncate", "with", "copy", "do", "call",
	} {
		if strings.HasPrefix(s, prefix+" ") {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Statement-kind allowlist (pass-3 escalation)
// ---------------------------------------------------------------------------

// createAllowKinds are the CREATE object kinds a migration body may
// contain, from the audited corpus: everything the repo's own planner
// emits (schema, type, table, index, view — diff_v2's generated up/down
// SQL) and the E2E corpus exercises, plus the same relation kinds the
// guard vocabulary already covers for drop/alter (domain, materialized
// view, sequence) and extension creation (judgment call: additive, its
// members guarded on drop/alter — documented in the attempt-4 inventory).
var createAllowKinds = map[string]bool{
	"table": true, "index": true, "schema": true, "type": true,
	"domain": true, "view": true, "sequence": true, "extension": true,
}

// alterAllowKinds is the schema-object subset of alterKinds the
// allowlist accepts (review-4 BLOCKER-1): ALTER/DROP acceptance
// intersects the guard vocabulary with schema objects only, so
// database-wide and role-wide kinds (ROLE/USER/GROUP, PUBLICATION,
// SUBSCRIPTION, SERVER, LANGUAGE) are refused before the guard or any
// acknowledgement runs. The guard vocabulary itself stays untouched so
// protected-name detection remains wide for any future relaxation.
var alterAllowKinds = map[string]bool{
	"table": true, "view": true, "materialized-view": true, "index": true,
	"sequence": true, "type": true, "domain": true, "schema": true,
	"extension": true, "function": true, "procedure": true, "routine": true,
	"policy": true, "trigger": true, "rule": true, "collation": true,
	"statistics": true, "operator": true, "foreign-table": true,
	"operator-class": true, "operator-family": true,
	"text-search-config": true, "text-search-dictionary": true,
	"text-search-parser": true, "text-search-template": true,
}

// dropAllowKinds is the schema-object subset of dropKinds the allowlist
// accepts (review-4 BLOCKER-1): database-wide and role-wide kinds
// (DATABASE, TABLESPACE, ROLE/USER/GROUP, OWNED, PUBLICATION,
// SUBSCRIPTION, SERVER, LANGUAGE, CAST, TRANSFORM, EVENT TRIGGER,
// FOREIGN DATA WRAPPER, ACCESS METHOD, USER MAPPING) are refused. The
// guard vocabulary itself stays untouched.
var dropAllowKinds = map[string]bool{
	"table": true, "view": true, "materialized-view": true, "index": true,
	"sequence": true, "type": true, "domain": true, "schema": true,
	"extension": true, "function": true, "procedure": true, "routine": true,
	"aggregate": true, "collation": true, "conversion": true,
	"policy": true, "trigger": true, "rule": true, "statistics": true,
	"operator": true, "foreign-table": true,
	"operator-class": true, "operator-family": true,
	"text-search-config": true, "text-search-dictionary": true,
	"text-search-parser": true, "text-search-template": true,
}

// AllowlistedDropKinds returns the sorted object kinds whose DROP forms
// the migration allowlist accepts. It exists for the property-table
// regression (review-6 BLOCKER-1): tests enumerate every allowlisted
// drop kind and assert its CASCADE closure against a planted protected
// victim, so a kind added to the allowlist without a fixture fails the
// table instead of silently escaping the closure.
func AllowlistedDropKinds() []string {
	out := make([]string, 0, len(dropAllowKinds))
	for kind := range dropAllowKinds {
		out = append(out, kind)
	}
	sort.Strings(out)
	return out
}

// AllowlistedAlterKinds returns the sorted object kinds whose ALTER forms
// the migration allowlist accepts. It exists for the name-class ALTER
// property table (review-7 MAJOR-1): tests enumerate every allowlisted
// alter kind of the "name" guard target class and assert that an
// extension member of that kind cannot be ALTERed while a user object of
// the same kind stays legal, so a kind added to the allowlist without a
// fixture fails the table instead of silently escaping the membership
// arm.
func AllowlistedAlterKinds() []string {
	out := make([]string, 0, len(alterAllowKinds))
	for kind := range alterAllowKinds {
		out = append(out, kind)
	}
	sort.Strings(out)
	return out
}

// StatementKind classifies one statement into its canonical allowlist
// kind from the significant token stream, or "" when the statement kind
// is outside the allowlist vocabulary (the caller refuses it with a
// named error — never executes it). The allowlist model (pass-3
// escalation, replacing leading-word dispatch): a migration body may
// contain ONLY:
//
//   - SELECT (WITH-led statements included; their data-modifying CTEs
//     are target-guarded by withStatementTargets)
//   - INSERT / UPDATE / DELETE / MERGE (target-guarded)
//   - TRUNCATE (target-guarded)
//   - CREATE of the audited schema-object kinds above (modifiers OR
//     REPLACE, UNIQUE, TEMP/TEMPORARY, UNLOGGED and CONCURRENTLY are
//     transparent)
//   - ALTER / DROP of the schema-object subset of the guard
//     vocabulary (alterAllowKinds / dropAllowKinds; review-4
//     BLOCKER-1) — database-wide and role-wide kinds are refused
//     here, before the guard or any acknowledgement runs; the guard
//     vocabulary itself stays wider for protected-name detection
//   - SET LOCAL (transaction-scoped; session-level SET persists beyond
//     the migration's transaction and is refused)
//
// Everything else — EXPLAIN, PREPARE/EXECUTE/DEALLOCATE, DO, CALL, COPY,
// CREATE FUNCTION/PROCEDURE/RULE/TRIGGER/..., GRANT/REVOKE, COMMENT ON,
// VACUUM/ANALYZE, REINDEX, LOCK, SAVEPOINT, BEGIN/COMMIT, session SET,
// and CREATE/ALTER/DROP of database-wide or role-wide kinds (DATABASE,
// TABLESPACE, ROLE/USER/GROUP, OWNED, PUBLICATION, SUBSCRIPTION, ...) —
// classifies as "" and is refused before any statement in the file runs.
// Comment-only fragments classify as "" but are skipped by callers
// (hasExecutableSQL); classification reads the same tokenizer as every
// other guard, so comments cannot shift a kind.
func StatementKind(sql string) string {
	toks := significantTokens(sql)
	if len(toks) == 0 || toks[0].kind != 'w' {
		return ""
	}
	switch toks[0].text {
	case "select", "insert", "update", "delete", "merge", "with", "truncate":
		return toks[0].text
	case "set":
		if isWordAt(toks, 1, "local") {
			return "set local"
		}
		return ""
	case "create":
		i := 1
		for i < len(toks) && toks[i].kind == 'w' {
			switch toks[i].text {
			case "or":
				if isWordAt(toks, i+1, "replace") {
					i += 2
					continue
				}
			case "unique", "temp", "temporary", "unlogged":
				i++
				continue
			}
			break
		}
		if i >= len(toks) || toks[i].kind != 'w' {
			return ""
		}
		if toks[i].text == "materialized" && isWordAt(toks, i+1, "view") {
			return "create materialized view"
		}
		if createAllowKinds[toks[i].text] {
			return "create " + toks[i].text
		}
		return ""
	case "alter":
		if kind, _ := kindAt(toks, 1, alterKinds); kind != "" && alterAllowKinds[kind] {
			return "alter " + kind
		}
		return ""
	case "drop":
		if kind, _ := kindAt(toks, 1, dropKinds); kind != "" && dropAllowKinds[kind] {
			return "drop " + kind
		}
		return ""
	}
	return ""
}

// statementKindLabel renders the human-facing name of a refused
// statement's kind: the leading word, plus the object-kind words for
// CREATE/ALTER/DROP shapes ("CREATE FUNCTION", "DROP OWNED") so the
// refusal names precisely what was refused. CREATE skips the same
// modifier words StatementKind skips (OR REPLACE, UNIQUE, TEMP/
// TEMPORARY, UNLOGGED) so "CREATE OR REPLACE FUNCTION" labels as
// CREATE FUNCTION with its precise reason, not a degraded
// "CREATE OR" + generic (review-4 MINOR-1). Multi-word object kinds
// label canonically on every head (review-5 MINOR-2): "DROP FOREIGN
// DATA WRAPPER" labels as DROP FOREIGN DATA WRAPPER, "CREATE USER
// MAPPING" as CREATE USER MAPPING (not a truncated "CREATE USER"
// that would mis-attribute the role-wide reason), and label-only
// phrases outside the guard's target vocabulary (ALTER DEFAULT
// PRIVILEGES, ALTER LARGE OBJECT) still name their full kind.
func statementKindLabel(sql string) string {
	toks := significantTokens(sql)
	if len(toks) == 0 || toks[0].kind != 'w' {
		return strings.TrimSpace(sql)
	}
	first := strings.ToUpper(toks[0].text)
	if len(toks) > 1 && toks[1].kind == 'w' {
		switch toks[0].text {
		case "create":
			i := 1
			for i < len(toks) && toks[i].kind == 'w' {
				switch toks[i].text {
				case "or":
					if isWordAt(toks, i+1, "replace") {
						i += 2
						continue
					}
				case "unique", "temp", "temporary", "unlogged":
					i++
					continue
				}
				break
			}
			if i < len(toks) && toks[i].kind == 'w' {
				if kind, _ := labelKindAt(toks, i); kind != "" {
					return first + " " + displayKindLabel(kind)
				}
				return first + " " + strings.ToUpper(toks[i].text)
			}
		case "alter", "drop":
			if kind, _ := labelKindAt(toks, 1); kind != "" {
				return first + " " + displayKindLabel(kind)
			}
			return first + " " + strings.ToUpper(toks[1].text)
		}
	}
	return first
}

// labelKindAt matches a canonical multi-word object kind for refusal
// labels: every multi-word kind the guard vocabulary defines, plus the
// label-only phrases ALTER spellings take without naming a guardable
// target (DEFAULT PRIVILEGES, LARGE OBJECT). Single-word kinds are the
// caller's verbatim-token fallback.
func labelKindAt(toks []sqlToken, i int) (string, int) {
	for _, words := range [][]string{{"default", "privileges"}, {"large", "object"}} {
		ok := true
		for j, w := range words {
			if !isWordAt(toks, i+j, w) {
				ok = false
				break
			}
		}
		if ok {
			return strings.Join(words, " "), i + len(words)
		}
	}
	return kindAt(toks, i, map[string]bool{})
}

// displayKindLabel renders a canonical kind ("foreign-data-wrapper",
// "default privileges") as its natural uppercase spelling.
func displayKindLabel(kind string) string {
	return strings.ToUpper(strings.ReplaceAll(kind, "-", " "))
}

// allowlistRefusalReason is the precise per-kind reason a known
// out-of-allowlist statement kind is refused (pass-3 findings): each
// named escape route gets its actual cause, unknown kinds get the
// generic allowlist statement.
func allowlistRefusalReason(label string) string {
	switch label {
	case "EXPLAIN":
		return "EXPLAIN ANALYZE executes the statements it plans, so the wrapped statement cannot be target-checked (plain EXPLAIN is refused uniformly for one provable rule)"
	case "PREPARE":
		return "a prepared statement's body runs later, at EXECUTE time, where its targets can no longer be checked"
	case "EXECUTE":
		return "EXECUTE runs a previously prepared statement whose targets are not visible to validation"
	case "DEALLOCATE":
		return "prepared-statement management is outside the migration allowlist"
	case "DO":
		return "procedural bodies are opaque — what a DO block touches cannot be proven from the statement text"
	case "CALL":
		return "procedure bodies are opaque — what a procedure touches cannot be proven from the statement text"
	case "COPY":
		return "COPY FROM STDIN cannot run through this runner's Exec path (it stalls the batch until the timeout) and COPY FROM PROGRAM is arbitrary program execution — both are refused"
	case "CREATE FUNCTION", "CREATE PROCEDURE":
		return "server-side code bodies are opaque — the SQL inside cannot be proven safe from statement text, and invoking them later is refused with them"
	case "CREATE RULE":
		return "rewrite rules redirect DML to different targets at execution time — the guarded target cannot be proven from statement text"
	case "CREATE TRIGGER":
		return "trigger bodies fire on later DML outside the statement that creates them — an opaque execution route"
	case "CREATE DATABASE", "ALTER DATABASE", "DROP DATABASE",
		"CREATE TABLESPACE", "ALTER TABLESPACE", "DROP TABLESPACE",
		"CREATE ROLE", "ALTER ROLE", "DROP ROLE",
		"CREATE USER", "ALTER USER", "DROP USER",
		"CREATE GROUP", "ALTER GROUP", "DROP GROUP":
		return "database-wide and role-wide objects are outside the migration allowlist — migrations may create, alter and drop schema objects only, and no acknowledgement flag widens this"
	case "GRANT", "REVOKE":
		return "privilege changes are outside this alpha's migration allowlist (not emitted by the planner or the corpus)"
	case "COMMENT":
		return "COMMENT ON is outside this alpha's migration allowlist (not emitted by the planner or the corpus)"
	case "VACUUM", "ANALYZE":
		return "maintenance operations are unneeded in migrations — the allowlist covers schema objects and row changes only"
	case "REINDEX":
		return "use CREATE INDEX CONCURRENTLY / DROP INDEX CONCURRENTLY for concurrent index work — REINDEX is outside the allowlist"
	case "LOCK":
		return "the runner owns locking under the migration advisory lock — explicit LOCK is unnecessary"
	case "SAVEPOINT":
		return "the runner owns transaction structure — savepoints are redundant inside it"
	case "BEGIN", "START":
		return "the runner owns transactions — each migration's DDL and history row commit atomically"
	case "COMMIT", "ROLLBACK", "END", "ABORT":
		return "the runner owns transactions — each migration's DDL and history row commit atomically"
	case "SET":
		return "only SET LOCAL is allowed (transaction-scoped); session-level SET persists beyond the migration's transaction"
	case "LISTEN", "NOTIFY":
		return "session-level operations are outside migration SQL"
	}
	return "this statement kind is not in the migration allowlist"
}

// CheckStatementAllowlist refuses one statement whose kind is outside
// the migration allowlist, naming the kind and the reason. Callers run
// it over every statement of every pending migration BEFORE any
// statement executes, so a refused kind fails the whole batch at
// validation time — atomically, with nothing applied.
func CheckStatementAllowlist(sql string) error {
	if !hasExecutableSQL(sql) {
		return nil
	}
	if StatementKind(sql) != "" {
		return nil
	}
	label := statementKindLabel(sql)
	return fmt.Errorf(
		"statement kind %s is refused in migration SQL: %s — migrations may contain only these statement kinds: SELECT (including WITH, with data-modifying CTEs target-guarded), INSERT/UPDATE/DELETE/MERGE, TRUNCATE, CREATE/ALTER/DROP of schema objects, and SET LOCAL; anything else is refused before any statement runs",
		label, allowlistRefusalReason(label))
}

// ---------------------------------------------------------------------------
// Qualified names and destructive targets
// ---------------------------------------------------------------------------

// QualifiedName is a (possibly unqualified) object name from SQL text.
type QualifiedName struct {
	Schema string
	Name   string
}

func (q QualifiedName) String() string {
	if q.Schema != "" {
		return q.Schema + "." + q.Name
	}
	return q.Name
}

// isWordChar matches PostgreSQL bare-identifier characters. Dollar is
// deliberately absent: outside dollar-quoted strings a `$` is punctuation
// (parameters like $1), never part of a name — the guard never matches
// object names through it.
func isWordChar(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// nameAt parses an optionally schema-qualified name at token position i
// (word or quoted identifier parts joined by '.').
func nameAt(toks []sqlToken, i int) (QualifiedName, int, bool) {
	if i >= len(toks) || (toks[i].kind != 'w' && toks[i].kind != 'q') {
		return QualifiedName{}, i, false
	}
	first := toks[i]
	i++
	if i+1 < len(toks) && toks[i].kind == 'p' && toks[i].text == "." &&
		(toks[i+1].kind == 'w' || toks[i+1].kind == 'q') {
		return QualifiedName{Schema: first.text, Name: toks[i+1].text}, i + 2, true
	}
	return QualifiedName{Name: first.text}, i, true
}

// ---------------------------------------------------------------------------
// Guarded targets (protected-object guard vocabulary)
// ---------------------------------------------------------------------------

// Guard forms: how a statement touches the objects it names.
const (
	GuardFormDrop     = "drop"
	GuardFormTruncate = "truncate"
	GuardFormAlter    = "alter"
	GuardFormDML      = "dml-write"
	GuardFormDo       = "do-block"
	GuardFormCreate   = "create"
)

// GuardTarget is one lexically identified object reference the
// protected-object guard must verify: the (possibly schema-qualified,
// case-preserved) object name, the canonical object kind it was spelled
// with, the form of the touch (drop/truncate/alter/row-write/do-block),
// and — for drops — whether the statement spells CASCADE. A do-block
// target carries no name: DO bodies are opaque and refused wholesale.
type GuardTarget struct {
	Name    QualifiedName
	Form    string
	Kind    string
	Cascade bool
}

// GuardTargetClass maps a guard kind to the catalog class its name is
// matched against for extension membership: relations (pg_class), types
// (pg_type), routines (pg_proc) or bare names (prefix rule only).
func GuardTargetClass(kind string) string {
	switch kind {
	case "table", "view", "materialized-view", "index", "sequence", "foreign-table":
		return "relation"
	case "type", "domain":
		return "type"
	case "function", "procedure", "routine", "aggregate":
		return "proc"
	}
	return "name"
}

func isWordAt(toks []sqlToken, i int, w string) bool {
	return i < len(toks) && toks[i].kind == 'w' && toks[i].text == w
}

// multiWordKinds are DROP/ALTER's multi-word object kinds with their
// canonical spellings.
var multiWordKinds = []struct {
	words []string
	kind  string
}{
	{[]string{"materialized", "view"}, "materialized-view"},
	{[]string{"foreign", "data", "wrapper"}, "foreign-data-wrapper"},
	{[]string{"foreign", "table"}, "foreign-table"},
	{[]string{"text", "search", "configuration"}, "text-search-config"},
	{[]string{"text", "search", "dictionary"}, "text-search-dictionary"},
	{[]string{"text", "search", "parser"}, "text-search-parser"},
	{[]string{"text", "search", "template"}, "text-search-template"},
	{[]string{"operator", "class"}, "operator-class"},
	{[]string{"operator", "family"}, "operator-family"},
	{[]string{"event", "trigger"}, "event-trigger"},
	{[]string{"user", "mapping"}, "user-mapping"},
	{[]string{"access", "method"}, "access-method"},
}

// dropKinds is every single-word DROP object kind PostgreSQL defines.
var dropKinds = map[string]bool{
	"table": true, "view": true, "index": true, "sequence": true,
	"type": true, "domain": true, "schema": true, "extension": true,
	"function": true, "procedure": true, "routine": true, "aggregate": true,
	"collation": true, "conversion": true, "policy": true, "trigger": true,
	"rule": true, "statistics": true, "publication": true, "subscription": true,
	"server": true, "language": true, "cast": true, "transform": true,
	"database": true, "tablespace": true, "role": true, "user": true,
	"group": true, "owned": true, "operator": true,
}

// alterKinds is every single-word ALTER object kind that names a guardable
// object. ALTER kinds without a nameable target (DEFAULT PRIVILEGES,
// SYSTEM, LARGE OBJECT) and out-of-vocabulary heads yield no targets.
var alterKinds = map[string]bool{
	"table": true, "view": true, "index": true, "sequence": true,
	"type": true, "domain": true, "schema": true, "extension": true,
	"function": true, "procedure": true, "routine": true,
	"policy": true, "trigger": true, "rule": true, "collation": true,
	"statistics": true, "publication": true, "subscription": true,
	"server": true, "language": true, "operator": true,
	"role": true, "user": true, "group": true,
}

func kindAt(toks []sqlToken, i int, single map[string]bool) (string, int) {
	for _, mw := range multiWordKinds {
		ok := true
		for j, w := range mw.words {
			if !isWordAt(toks, i+j, w) {
				ok = false
				break
			}
		}
		if ok {
			return mw.kind, i + len(mw.words)
		}
	}
	if i < len(toks) && toks[i].kind == 'w' && single[toks[i].text] {
		return toks[i].text, i + 1
	}
	return "", i
}

// GuardStatementTargets extracts every object reference the
// protected-object guard must check (M05 review MAJOR-1/MINOR-2/LOW-1
// widening; pass-2 escalation: comment-proof token stream, data-modifying
// CTEs, DO refusal). The vocabulary covers every DROP form PostgreSQL
// defines (relation kinds: table, view, materialized view, index,
// sequence, foreign table; type/domain; function/procedure/routine/
// aggregate; schema, extension, policy, trigger, rule, collation,
// conversion, statistics, text-search objects, operator class/family,
// event triggers, publications, subscriptions, servers, FDWs, roles,
// users, groups, databases, tablespaces, languages, casts, transforms,
// access methods, user mappings, OWNED BY — with DROP/ALTER POLICY,
// TRIGGER and RULE also yielding their ON table as an alteration
// target), TRUNCATE lists, ALTER
// targets (drop and non-drop alike — metadata must not be modified
// either), and row-write DML targets (INSERT INTO, UPDATE, DELETE FROM,
// MERGE INTO — including those spelled as data-modifying CTEs inside a
// WITH statement; SELECT stays legal). CASCADE is flagged on drops for
// the transitive check. DO statements are refused wholesale (opaque
// bodies). CREATE statements yield their created object as a
// create-form target (review-5 BLOCKER-1: the _neutron_ name prefix is
// reserved, and a same-batch CREATE EXTENSION arms the DROP EXTENSION
// ordering check) — a create target can only be refused, never
// assumed. Classification reads the significant token stream, so
// comments — leading, mid-statement or nested blocks — cannot hide a
// statement from the guard, and string/dollar-quoted bodies can never
// impersonate one. Statements outside the vocabulary return nil. This
// guard-side vocabulary is strictly wider than the M03 ack classifiers
// and leaves them unchanged: the acknowledgement gates data loss on
// legitimately managed objects, it never gates metadata/extension
// protection.
func GuardStatementTargets(sql string) []GuardTarget {
	toks := significantTokens(sql)
	if len(toks) == 0 || toks[0].kind != 'w' {
		return nil
	}
	switch toks[0].text {
	case "drop":
		return dropStatementTargets(toks)
	case "create":
		return createStatementTargets(toks)
	case "truncate":
		i := 1
		if isWordAt(toks, i, "table") {
			i++
		}
		if isWordAt(toks, i, "only") {
			i++
		}
		return nameListTargets(toks, i, "table", GuardFormTruncate, false)
	case "alter":
		return alterStatementTargets(toks)
	case "with":
		return withStatementTargets(toks)
	case "do":
		// DO bodies are opaque procedural code (plpgsql or any installed
		// language, including dynamic SQL): what they touch cannot be
		// proven from the statement text, so DO is refused outright in
		// migration SQL rather than guessed at.
		return []GuardTarget{{Name: QualifiedName{Name: "DO"}, Form: GuardFormDo, Kind: "do-block"}}
	case "insert":
		if isWordAt(toks, 1, "into") {
			if name, _, ok := nameAt(toks, 2); ok {
				return []GuardTarget{{Name: name, Form: GuardFormDML, Kind: "table"}}
			}
		}
	case "update":
		i := 1
		if isWordAt(toks, i, "only") {
			i++
		}
		if name, _, ok := nameAt(toks, i); ok {
			return []GuardTarget{{Name: name, Form: GuardFormDML, Kind: "table"}}
		}
	case "delete":
		if isWordAt(toks, 1, "from") {
			i := 2
			if isWordAt(toks, i, "only") {
				i++
			}
			if name, _, ok := nameAt(toks, i); ok {
				return []GuardTarget{{Name: name, Form: GuardFormDML, Kind: "table"}}
			}
		}
	case "merge":
		if isWordAt(toks, 1, "into") {
			if name, _, ok := nameAt(toks, 2); ok {
				return []GuardTarget{{Name: name, Form: GuardFormDML, Kind: "table"}}
			}
		}
	}
	return nil
}

// withStatementTargets extracts the write targets of data-modifying CTEs
// (M05 review MAJOR-3): every INSERT INTO / UPDATE / DELETE FROM /
// MERGE INTO spelled anywhere inside a WITH statement, at any parenthesis
// depth — the read side (plain CTEs, the trailing SELECT) stays legal.
// Words like update can legally name a CTE; the extractor then reads the
// following non-reserved word (e.g. "as") as the target, which matches no
// protected object — over-extraction is harmless, under-extraction is
// not.
func withStatementTargets(toks []sqlToken) []GuardTarget {
	var targets []GuardTarget
	addTarget := func(name QualifiedName, ok bool) {
		if ok {
			targets = append(targets, GuardTarget{Name: name, Form: GuardFormDML, Kind: "table"})
		}
	}
	for i := 0; i < len(toks); i++ {
		if toks[i].kind != 'w' {
			continue
		}
		switch toks[i].text {
		case "insert", "merge":
			if isWordAt(toks, i+1, "into") {
				name, _, ok := nameAt(toks, i+2)
				addTarget(name, ok)
			}
		case "delete":
			if isWordAt(toks, i+1, "from") {
				name, _, ok := nameAt(toks, i+2)
				addTarget(name, ok)
			}
		case "update":
			name, _, ok := nameAt(toks, i+1)
			addTarget(name, ok)
		}
	}
	return targets
}

func dropStatementTargets(toks []sqlToken) []GuardTarget {
	kind, i := kindAt(toks, 1, dropKinds)
	if kind == "" {
		return nil
	}
	if kind == "owned" {
		// DROP OWNED BY role [, ...] — destroys EVERY object the roles
		// own; the victims are not nameable from the statement.
		if isWordAt(toks, i, "by") {
			i++
		}
		return nameListTargets(toks, i, kind, GuardFormDrop, false)
	}
	if isWordAt(toks, i, "if") && isWordAt(toks, i+1, "exists") {
		i += 2
	}
	if kind == "index" && isWordAt(toks, i, "concurrently") {
		i++
	}
	if kind == "user-mapping" && isWordAt(toks, i, "for") {
		i++
	}
	if kind == "operator" {
		// Operator names are symbol runs (=== , +>> , <! , ...) the
		// tokenizer emits as punctuation — nameAt cannot parse them
		// (review-6 BLOCKER-1 vector 2: the target parsed as the bare
		// schema word and the CASCADE closure never seeded).
		var targets []GuardTarget
		cascade := containsTopLevelWord(toks, i, "cascade")
		for {
			name, next, ok := operatorNameAt(toks, i)
			if !ok {
				break
			}
			targets = append(targets, GuardTarget{Name: name, Form: GuardFormDrop, Kind: kind, Cascade: cascade})
			i = next
			// Skip the signature's balanced parenthesis group so a
			// comma-separated operator list continues correctly.
			if i < len(toks) && toks[i].kind == 'p' && toks[i].text == "(" {
				depth := 0
				for ; i < len(toks); i++ {
					if toks[i].kind == 'p' && toks[i].text == "(" {
						depth++
						continue
					}
					if toks[i].kind == 'p' && toks[i].text == ")" {
						depth--
						i++
						break
					}
				}
			}
			if i < len(toks) && toks[i].kind == 'p' && toks[i].text == "," {
				i++
				continue
			}
			break
		}
		return targets
	}
	return attachedTableTarget(nameListTargets(toks, i, kind, GuardFormDrop, containsTopLevelWord(toks, i, "cascade")), toks, i, kind)
}

// isOperatorChar reports whether a punctuation token is one of
// PostgreSQL's operator-name characters (+ - * / < > = ~ ! @ # % ^ & | `
// ?); operator names are runs of these.
func isOperatorChar(s string) bool {
	if len(s) != 1 {
		return false
	}
	switch s[0] {
	case '+', '-', '*', '/', '<', '>', '=', '~', '!', '@', '#', '%', '^', '&', '|', '`', '?':
		return true
	}
	return false
}

// operatorNameAt parses an optionally schema-qualified operator name at
// token position i: an optional word/quoted schema part joined by '.', then
// one or more operator-character punctuation tokens (vict.=== , ===).
func operatorNameAt(toks []sqlToken, i int) (QualifiedName, int, bool) {
	if i >= len(toks) {
		return QualifiedName{}, i, false
	}
	if toks[i].kind == 'p' && isOperatorChar(toks[i].text) {
		j, b := i, strings.Builder{}
		for j < len(toks) && toks[j].kind == 'p' && isOperatorChar(toks[j].text) {
			b.WriteString(toks[j].text)
			j++
		}
		return QualifiedName{Name: b.String()}, j, true
	}
	if (toks[i].kind == 'w' || toks[i].kind == 'q') &&
		i+2 < len(toks) && toks[i+1].kind == 'p' && toks[i+1].text == "." &&
		toks[i+2].kind == 'p' && isOperatorChar(toks[i+2].text) {
		j, b := i+2, strings.Builder{}
		for j < len(toks) && toks[j].kind == 'p' && isOperatorChar(toks[j].text) {
			b.WriteString(toks[j].text)
			j++
		}
		return QualifiedName{Schema: toks[i].text, Name: b.String()}, j, true
	}
	return QualifiedName{}, i, false
}

// attachedTableTarget appends the ON <table> of a DROP/ALTER POLICY, TRIGGER
// or RULE as an alteration-form table target (review-6 BLOCKER-1): the
// statement mutates that table's definition — it strips the policy, trigger
// or rule attached to it — so the protected-name and extension-membership
// checks must see the table, not just the attached object's own name. The
// scan skips the (single) object name so a trigger literally named "on"
// cannot shadow the real ON.
func attachedTableTarget(targets []GuardTarget, toks []sqlToken, i int, kind string) []GuardTarget {
	switch kind {
	case "policy", "trigger", "rule":
	default:
		return targets
	}
	j := i
	if j < len(toks) && (toks[j].kind == 'w' || toks[j].kind == 'q') {
		j++
	}
	for ; j < len(toks); j++ {
		if toks[j].kind == 'w' && toks[j].text == "on" {
			if name, _, ok := nameAt(toks, j+1); ok {
				targets = append(targets, GuardTarget{Name: name, Form: GuardFormAlter, Kind: "table"})
			}
			break
		}
	}
	return targets
}

func alterStatementTargets(toks []sqlToken) []GuardTarget {
	kind, i := kindAt(toks, 1, alterKinds)
	if kind == "" {
		return nil
	}
	if isWordAt(toks, i, "if") && isWordAt(toks, i+1, "exists") {
		i += 2
	}
	if kind == "user-mapping" && isWordAt(toks, i, "for") {
		i++
	}
	if kind == "operator" {
		if name, _, ok := operatorNameAt(toks, i); ok {
			return []GuardTarget{{Name: name, Form: GuardFormAlter, Kind: kind}}
		}
		return nil
	}
	if name, _, ok := nameAt(toks, i); ok {
		return attachedTableTarget([]GuardTarget{{Name: name, Form: GuardFormAlter, Kind: kind}}, toks, i, kind)
	}
	return nil
}

// createStatementTargets extracts the object a CREATE statement creates
// as a create-form guard target (review-5 BLOCKER-1: migrations must
// not plant _neutron_-prefixed lookalikes, and a same-batch CREATE
// EXTENSION must arm the DROP EXTENSION ordering check). Only the
// allowlisted create kinds yield targets — CREATE FUNCTION and its
// opaque siblings are refused by the allowlist before the guard runs.
// An unnamed CREATE INDEX (server-generated name) yields only its ON
// table; CREATE SCHEMA AUTHORIZATION <role> names no object.
func createStatementTargets(toks []sqlToken) []GuardTarget {
	i := 1
	for i < len(toks) && toks[i].kind == 'w' {
		switch toks[i].text {
		case "or":
			if isWordAt(toks, i+1, "replace") {
				i += 2
				continue
			}
		case "unique", "temp", "temporary", "unlogged":
			i++
			continue
		}
		break
	}
	if i >= len(toks) || toks[i].kind != 'w' {
		return nil
	}
	kind := ""
	if toks[i].text == "materialized" && isWordAt(toks, i+1, "view") {
		kind, i = "materialized-view", i+2
	} else if createAllowKinds[toks[i].text] {
		kind, i = toks[i].text, i+1
	} else {
		return nil
	}
	if kind == "index" && isWordAt(toks, i, "concurrently") {
		i++
	}
	if isWordAt(toks, i, "if") && isWordAt(toks, i+1, "not") && isWordAt(toks, i+2, "exists") {
		i += 3
	}
	if kind == "schema" && isWordAt(toks, i, "authorization") {
		return nil
	}
	if kind == "index" {
		// The created index (when named) and the table it lands on are
		// both create-form targets: either name may be planted into the
		// reserved namespace.
		var targets []GuardTarget
		if !isWordAt(toks, i, "on") {
			if name, next, ok := nameAt(toks, i); ok && isWordAt(toks, next, "on") {
				targets = append(targets, GuardTarget{Name: name, Form: GuardFormCreate, Kind: kind})
				i = next + 1
			}
		} else {
			i++
		}
		if name, _, ok := nameAt(toks, i); ok {
			targets = append(targets, GuardTarget{Name: name, Form: GuardFormCreate, Kind: "table"})
		}
		return targets
	}
	if name, _, ok := nameAt(toks, i); ok {
		return []GuardTarget{{Name: name, Form: GuardFormCreate, Kind: kind}}
	}
	return nil
}

// nameListTargets parses the comma-separated target list starting at i,
// honoring parentheses so commas inside routine signatures do not split
// the list. Anything else (RESTRICT, ON, USING, SERVER, end of statement)
// ends the list.
func nameListTargets(toks []sqlToken, i int, kind, form string, cascade bool) []GuardTarget {
	var targets []GuardTarget
	depth := 0
	expectName := true
	for i < len(toks) {
		t := toks[i]
		if t.kind == 'p' && (t.text == "(" || t.text == "[") {
			depth++
			i++
			continue
		}
		if t.kind == 'p' && (t.text == ")" || t.text == "]") {
			if depth > 0 {
				depth--
			}
			i++
			continue
		}
		if depth > 0 {
			// Inside a signature/argument list: commas and words there
			// never separate or end the top-level target list.
			i++
			continue
		}
		if t.kind == 'p' && t.text == "," {
			expectName = true
			i++
			continue
		}
		if expectName && (t.kind == 'w' || t.kind == 'q') {
			name, next, ok := nameAt(toks, i)
			if !ok {
				break
			}
			targets = append(targets, GuardTarget{Name: name, Form: form, Kind: kind, Cascade: cascade})
			i = next
			expectName = false
			continue
		}
		break
	}
	return targets
}

// containsTopLevelWord reports whether w appears as a bare word at
// parenthesis depth 0 at or after position i (the CASCADE modifier).
func containsTopLevelWord(toks []sqlToken, i int, w string) bool {
	depth := 0
	for ; i < len(toks); i++ {
		t := toks[i]
		if t.kind == 'p' {
			switch t.text {
			case "(", "[":
				depth++
			case ")", "]":
				if depth > 0 {
					depth--
				}
			}
			continue
		}
		if depth == 0 && t.kind == 'w' && t.text == w {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Postconditions
// ---------------------------------------------------------------------------

// Statement postcondition kinds. A postcondition is the durable catalog
// state that proves whether a statement's effect is present.
const (
	PostconditionRelationCreate = "relation-create" // table/view: object exists
	PostconditionRelationDrop   = "relation-drop"   // table/view: object absent
	PostconditionIndexCreate    = "index-create"    // index: exists AND valid
	PostconditionIndexDrop      = "index-drop"      // index: absent
	PostconditionTypeCreate     = "type-create"     // type: exists
	PostconditionTypeDrop       = "type-drop"       // type: absent
	PostconditionSchemaCreate   = "schema-create"   // schema: exists
	PostconditionSchemaDrop     = "schema-drop"     // schema: absent
)

// StatementPostcondition is the checkable durable effect of one statement.
// Table carries the indexed table for index postconditions.
type StatementPostcondition struct {
	Kind   string
	Schema string
	Name   string
	Table  string
}

// IsCreationSide reports whether a satisfied postcondition is positive
// evidence the statement ran (object present), as opposed to drop-side
// satisfaction, which also holds when nothing ever ran.
func (p StatementPostcondition) IsCreationSide() bool {
	switch p.Kind {
	case PostconditionRelationCreate, PostconditionIndexCreate, PostconditionTypeCreate, PostconditionSchemaCreate:
		return true
	}
	return false
}

// StatementPostconditionOf extracts the postcondition of a statement, or
// reports that none is checkable. Unnamed CREATE INDEX (`CREATE INDEX ON
// t(...)`, server-generated name) has no nameable postcondition and is
// unverifiable.
func StatementPostconditionOf(sql string) (StatementPostcondition, bool) {
	toks := significantTokens(sql)
	if len(toks) == 0 || toks[0].kind != 'w' {
		return StatementPostcondition{}, false
	}
	switch toks[0].text {
	case "create":
		// Skip the modifier prefixes: OR REPLACE, UNIQUE, TEMP/TEMPORARY.
		i := 1
		for i < len(toks) && toks[i].kind == 'w' {
			switch toks[i].text {
			case "or":
				if i+1 < len(toks) && toks[i+1].text == "replace" {
					i += 2
					continue
				}
			case "unique", "temp", "temporary":
				i++
				continue
			}
			break
		}
		if i >= len(toks) || toks[i].kind != 'w' {
			return StatementPostcondition{}, false
		}
		// "create materialized view" — consume the prefix so the switch
		// below sees "view".
		if toks[i].text == "materialized" && i+1 < len(toks) && toks[i+1].kind == 'w' && toks[i+1].text == "view" {
			i++
		}
		switch toks[i].text {
		case "table", "view":
			i++
			if i+2 < len(toks) && toks[i].kind == 'w' && toks[i].text == "if" && toks[i+1].kind == 'w' && toks[i+1].text == "not" && toks[i+2].kind == 'w' && toks[i+2].text == "exists" {
				i += 3
			}
			if name, _, ok := nameAt(toks, i); ok {
				return StatementPostcondition{Kind: PostconditionRelationCreate, Schema: name.Schema, Name: name.Name}, true
			}
		case "index":
			i++
			if i < len(toks) && toks[i].kind == 'w' && toks[i].text == "concurrently" {
				i++
			}
			if i+2 < len(toks) && toks[i].kind == 'w' && toks[i].text == "if" && toks[i+1].kind == 'w' && toks[i+1].text == "not" && toks[i+2].kind == 'w' && toks[i+2].text == "exists" {
				i += 3
			}
			// Optional explicit index name followed by ON <table>; an
			// unnamed index gets a server-generated name and is
			// unverifiable.
			if i < len(toks) && toks[i].kind == 'w' && toks[i].text == "on" {
				return StatementPostcondition{}, false
			}
			if name, next, ok := nameAt(toks, i); ok && next < len(toks) && toks[next].kind == 'w' && toks[next].text == "on" {
				if tbl, _, ok2 := nameAt(toks, next+1); ok2 {
					// An index always lives in its table's schema.
					return StatementPostcondition{Kind: PostconditionIndexCreate, Schema: tbl.Schema, Name: name.Name, Table: tbl.Name}, true
				}
			}
		case "type":
			i++
			if name, _, ok := nameAt(toks, i); ok {
				return StatementPostcondition{Kind: PostconditionTypeCreate, Schema: name.Schema, Name: name.Name}, true
			}
		case "schema":
			i++
			if i+2 < len(toks) && toks[i].kind == 'w' && toks[i].text == "if" && toks[i+1].kind == 'w' && toks[i+1].text == "not" && toks[i+2].kind == 'w' && toks[i+2].text == "exists" {
				i += 3
			}
			if i < len(toks) && (toks[i].kind == 'q' || (toks[i].kind == 'w' && !isReservedSchemaWord(toks[i].text))) {
				return StatementPostcondition{Kind: PostconditionSchemaCreate, Name: toks[i].text}, true
			}
		}
		return StatementPostcondition{}, false
	case "drop":
		i := 1
		if i < len(toks) && toks[i].kind == 'w' && toks[i].text == "materialized" && i+1 < len(toks) {
			i++
		}
		if i >= len(toks) || toks[i].kind != 'w' {
			return StatementPostcondition{}, false
		}
		kind := ""
		switch toks[i].text {
		case "table", "view":
			kind = PostconditionRelationDrop
		case "index":
			kind = PostconditionIndexDrop
		case "type":
			kind = PostconditionTypeDrop
		case "schema":
			kind = PostconditionSchemaDrop
		default:
			return StatementPostcondition{}, false
		}
		i++
		if i < len(toks) && toks[i].kind == 'w' && toks[i].text == "concurrently" {
			i++
		}
		if i+1 < len(toks) && toks[i].kind == 'w' && toks[i].text == "if" && toks[i+1].kind == 'w' && toks[i+1].text == "exists" {
			i += 2
		}
		if name, _, ok := nameAt(toks, i); ok {
			return StatementPostcondition{Kind: kind, Schema: name.Schema, Name: name.Name}, true
		}
	}
	return StatementPostcondition{}, false
}

func isReservedSchemaWord(w string) bool {
	switch w {
	case "if", "exists", "authorization", "cascade", "restrict":
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Catalog inspection primitives
// ---------------------------------------------------------------------------

func quoteIdentPart(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// regclassRef renders a name for to_regclass: quoted parts, optionally
// schema-qualified. An empty schema relies on the search path.
func regclassRef(schema, name string) string {
	if schema != "" {
		return quoteIdentPart(schema) + "." + quoteIdentPart(name)
	}
	return quoteIdentPart(name)
}

// RelationExists checks a table/view/index presence via to_regclass, which
// resolves unqualified names through the session search path.
func (c *Client) RelationExists(ctx context.Context, schema, name string) (bool, error) {
	var exists bool
	err := c.pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", regclassRef(schema, name)).Scan(&exists)
	return exists, err
}

// IndexStatus reports whether a named index exists and (when it does)
// whether it is valid. A failed CREATE INDEX CONCURRENTLY leaves an INVALID
// index behind; validity is the difference between "done" and "debris".
// The validity arm aggregates (bool_and) instead of returning a bare scalar
// row: same-name indexes in multiple schemas must never turn a status
// probe into SQLSTATE 21000 — with more than one match the conservative
// aggregate (any invalid ⇒ invalid) decides deterministically (M06 rework,
// review-1 MAJOR-2).
func (c *Client) IndexStatus(ctx context.Context, schema, name string) (exists, valid bool, err error) {
	err = c.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_class ic
			JOIN pg_namespace n ON n.oid = ic.relnamespace
			WHERE ic.relname = $2
			  AND ic.relkind = 'i'
			  AND ($1 = '' OR n.nspname = $1)
		),
		COALESCE((
			SELECT bool_and(i.indisvalid)
			FROM pg_class ic
			JOIN pg_namespace n ON n.oid = ic.relnamespace
			JOIN pg_index i ON i.indexrelid = ic.oid
			WHERE ic.relname = $2
			  AND ic.relkind = 'i'
			  AND ($1 = '' OR n.nspname = $1)
		), false)`, schema, name).Scan(&exists, &valid)
	return exists, valid, err
}

// TypeExists checks a named type's presence.
func (c *Client) TypeExists(ctx context.Context, schema, name string) (bool, error) {
	var exists bool
	err := c.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_type t
			JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE t.typname = $2
			  AND ($1 = '' OR n.nspname = $1)
		)`, schema, name).Scan(&exists)
	return exists, err
}

// SchemaExists checks a namespace's presence.
func (c *Client) SchemaExists(ctx context.Context, name string) (bool, error) {
	var exists bool
	err := c.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", name).Scan(&exists)
	return exists, err
}

// EvaluatePostcondition checks a postcondition against the live catalog.
// Returns "satisfied", "unsatisfied" or "invalid" (a concurrent index that
// exists but is not valid — the debris of a failed build).
func (c *Client) EvaluatePostcondition(ctx context.Context, p StatementPostcondition) (string, error) {
	switch p.Kind {
	case PostconditionRelationCreate, PostconditionRelationDrop:
		exists, err := c.RelationExists(ctx, p.Schema, p.Name)
		if err != nil {
			return "", err
		}
		if p.Kind == PostconditionRelationCreate {
			if exists {
				return "satisfied", nil
			}
			return "unsatisfied", nil
		}
		if exists {
			return "unsatisfied", nil
		}
		return "satisfied", nil
	case PostconditionIndexCreate:
		exists, valid, err := c.IndexStatus(ctx, p.Schema, p.Name)
		if err != nil {
			return "", err
		}
		switch {
		case exists && valid:
			return "satisfied", nil
		case exists && !valid:
			return "invalid", nil
		default:
			return "unsatisfied", nil
		}
	case PostconditionIndexDrop:
		exists, _, err := c.IndexStatus(ctx, p.Schema, p.Name)
		if err != nil {
			return "", err
		}
		if exists {
			return "unsatisfied", nil
		}
		return "satisfied", nil
	case PostconditionTypeCreate, PostconditionTypeDrop:
		exists, err := c.TypeExists(ctx, p.Schema, p.Name)
		if err != nil {
			return "", err
		}
		if (p.Kind == PostconditionTypeCreate) == exists {
			return "satisfied", nil
		}
		return "unsatisfied", nil
	case PostconditionSchemaCreate, PostconditionSchemaDrop:
		exists, err := c.SchemaExists(ctx, p.Name)
		if err != nil {
			return "", err
		}
		if (p.Kind == PostconditionSchemaCreate) == exists {
			return "satisfied", nil
		}
		return "unsatisfied", nil
	}
	return "", fmt.Errorf("unknown postcondition kind %q", p.Kind)
}

// ProtectedTargetReasons returns which guarded targets must be refused and
// why (M05 review MAJOR-1/MINOR-2/LOW-1 widening):
//
//   - the _neutron_ prefix rule, for every form: drops, truncates,
//     alterations, row writes AND creates — neutron-internal metadata is
//     never touched, and the _neutron_ namespace is reserved so no
//     migration can plant a lookalike victim (review-5 BLOCKER-1),
//     whatever flags are passed;
//   - extension membership, class-aware: relation targets against pg_class
//     members, type/domain targets against pg_type members, routine
//     targets against pg_proc members (pg_depend deptype 'e'), and —
//     review-7 MAJOR-1 — name-class kinds (operators, operator classes
//     and families, collations, statistics, policies, triggers, rules,
//     text-search objects) against their own catalog's members, keyed by
//     the cascade seed catalogs. PostgreSQL permits ALTER ... RENAME,
//     SET SCHEMA and OWNER on extension members (it refuses only member
//     drops, 2BP01), so the membership check covers ALTER forms and DROP
//     forms alike;
//   - DROP EXTENSION with live members (the drop destroys every member —
//     CASCADE or not; only an extension with zero members can drop), and
//     DROP EXTENSION of an extension the SAME batch creates: validation
//     runs pre-execution, so members installed by the batch's own CREATE
//     EXTENSION are invisible to the member count — the drop is refused
//     on the ordering alone (review-5 BLOCKER-1);
//   - ALTER EXTENSION (membership re-assignment, UPDATE and SET SCHEMA
//     all mutate extension state that cannot be proven safe);
//   - DROP OWNED BY (destroys every object the named roles own; the
//     victims are not nameable from the statement);
//   - DROP SCHEMA ... CASCADE when the namespace holds protected objects
//     (a non-cascade DROP SCHEMA is refused server-side for non-empty
//     schemas), or when protected objects ANYWHERE — including other
//     schemas — transitively depend on the schema's contents, whatever
//     catalog class those contents belong to (the seed follows the
//     pg_namespace containment edges; review-5/6 BLOCKER-1);
//   - EVERY other allowlisted drop kind spelled CASCADE — relations,
//     types/domains, routines, aggregates, operators, operator classes
//     and families, collations, conversions, statistics, text-search
//     objects, policies, triggers, rules — with the same recursive
//     pg_depend closure, seeded from the kind's own catalog class
//     (review-6 BLOCKER-1: no drop kind sits outside the closure);
//   - metadata ATTACHED to protected relations: a cascade that would
//     strip a constraint, column default, trigger, rule or index from a
//     protected relation is refused even though the relation row itself
//     survives (the protected set includes attaching pg_depend rows;
//     review-6 BLOCKER-1);
//   - DROP/ALTER POLICY/TRIGGER/RULE named ON a protected relation: the
//     statement mutates that relation's definition, so the ON table is
//     a guard target of its own (review-6 BLOCKER-1);
//   - DO blocks, refused outright: their procedural bodies are opaque
//     and cannot be proven safe (pass-2 escalation decision).
//
// Unqualified names match members in any namespace; qualified names match
// the exact schema. The refusal can only err by refusing, never by
// allowing.
func (c *Client) ProtectedTargetReasons(ctx context.Context, targets []GuardTarget) (map[string]string, error) {
	violations := map[string]string{}
	add := func(t GuardTarget, reason string) {
		key := t.Name.String()
		if _, dup := violations[key]; !dup {
			violations[key] = reason
		}
	}

	var relationT, typeT, procT, nameT []GuardTarget
	var extensionDrops, extensionCreates, ownedDrops, schemaCascades, relationCascades, namedCascades []GuardTarget
	for _, t := range targets {
		if t.Form == GuardFormDo {
			add(t, "DO blocks execute opaque procedural code — what a DO body touches cannot be proven from the statement text, so DO is refused outright in migration SQL (split the logic into plain SQL statements)")
			continue
		}
		if t.Form == GuardFormCreate {
			if isProtectedTableName(t.Name.Name) {
				add(t, "the _neutron_ name prefix is reserved for neutron-internal objects — migration SQL cannot create _neutron_-prefixed objects, whatever flags are passed (a planted lookalike would defeat cascade protection)")
			}
			if t.Kind == "extension" {
				extensionCreates = append(extensionCreates, t)
			}
			continue
		}
		if isProtectedTableName(t.Name.Name) {
			add(t, "neutron-internal metadata")
		}
		if t.Form == GuardFormDrop {
			// CASCADE dispatch (review-6 BLOCKER-1): EVERY allowlisted
			// drop kind that spells CASCADE reaches the shared
			// pg_depend closure — relations resolve through pg_class,
			// the schema kind through its namespace-containment seed,
			// extensions through their member rule, and every other
			// kind through its per-class catalog seed. No kind sits
			// outside the closure.
			switch {
			case t.Kind == "extension":
				extensionDrops = append(extensionDrops, t)
			case t.Kind == "owned":
				ownedDrops = append(ownedDrops, t)
			case t.Kind == "schema":
				if t.Cascade {
					schemaCascades = append(schemaCascades, t)
				}
			case !t.Cascade:
			case GuardTargetClass(t.Kind) == "relation":
				relationCascades = append(relationCascades, t)
			default:
				namedCascades = append(namedCascades, t)
			}
		}
		if t.Form == GuardFormAlter && t.Kind == "extension" {
			add(t, "ALTER EXTENSION mutates extension state — membership re-assignment, UPDATE or SET SCHEMA cannot be proven safe from migration SQL")
		}
		switch GuardTargetClass(t.Kind) {
		case "relation":
			relationT = append(relationT, t)
		case "type":
			typeT = append(typeT, t)
		case "proc":
			procT = append(procT, t)
		case "name":
			nameT = append(nameT, t)
		}
	}

	// Class-aware extension membership.
	for _, class := range []struct {
		list  []GuardTarget
		query string
	}{
		{relationT, `
			SELECT n.nspname, c.relname, e.extname
			FROM pg_depend d
			JOIN pg_extension e ON e.oid = d.refobjid
			JOIN pg_class c ON c.oid = d.objid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE d.deptype = 'e' AND d.classid = 'pg_class'::regclass`},
		{typeT, `
			SELECT n.nspname, t.typname, e.extname
			FROM pg_depend d
			JOIN pg_extension e ON e.oid = d.refobjid
			JOIN pg_type t ON t.oid = d.objid
			JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE d.deptype = 'e' AND d.classid = 'pg_type'::regclass`},
		{procT, `
			SELECT n.nspname, p.proname, e.extname
			FROM pg_depend d
			JOIN pg_extension e ON e.oid = d.refobjid
			JOIN pg_proc p ON p.oid = d.objid
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE d.deptype = 'e' AND d.classid = 'pg_proc'::regclass`},
	} {
		if len(class.list) == 0 {
			continue
		}
		rows, err := c.pool.Query(ctx, class.query)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var schema, obj, ext string
			if err := rows.Scan(&schema, &obj, &ext); err != nil {
				rows.Close()
				return nil, err
			}
			for _, t := range class.list {
				if t.Name.Name != obj {
					continue
				}
				if t.Name.Schema != "" && t.Name.Schema != schema {
					continue
				}
				add(t, "owned by extension "+ext)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	// Name-class extension membership (review-7 MAJOR-1): the arms above
	// cover only relation/type/proc targets, so operators, operator
	// classes and families, collations, statistics, policies, triggers,
	// rules and the text-search kinds had NO membership check on any
	// form — and PostgreSQL permits ALTER ... RENAME, SET SCHEMA and
	// OWNER on extension members (it refuses only member drops, 2BP01),
	// which made every allowlisted ALTER of those kinds mutate members
	// with rc 0. One lookup keyed by cascadeSeedCatalogs: the kind's own
	// catalog class decides which membership rows can match, with the
	// same name/namespace semantics as the arms above. DROP forms gain a
	// guard-side refusal ahead of the server's own 2BP01; ALTER forms
	// close the mutation seam.
	nameByKind := make(map[string][]GuardTarget)
	for _, t := range nameT {
		if _, ok := cascadeSeedCatalogs[t.Kind]; ok {
			nameByKind[t.Kind] = append(nameByKind[t.Kind], t)
		}
	}
	nameKinds := make([]string, 0, len(nameByKind))
	for kind := range nameByKind {
		nameKinds = append(nameKinds, kind)
	}
	sort.Strings(nameKinds)
	for _, kind := range nameKinds {
		cat := cascadeSeedCatalogs[kind]
		nsSelect, nsJoin := "'', ", ""
		if cat.nsCol != "" {
			nsSelect = "n.nspname, "
			nsJoin = " JOIN pg_namespace n ON n.oid = o." + cat.nsCol
		}
		rows, err := c.pool.Query(ctx, `SELECT `+nsSelect+`o.`+cat.nameCol+`, e.extname
			FROM pg_depend d
			JOIN pg_extension e ON e.oid = d.refobjid
			JOIN `+cat.class+` o ON o.oid = d.objid`+nsJoin+`
			WHERE d.deptype = 'e' AND d.classid = '`+cat.class+`'::regclass`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var schema, obj, ext string
			if err := rows.Scan(&schema, &obj, &ext); err != nil {
				rows.Close()
				return nil, err
			}
			for _, t := range nameByKind[kind] {
				if t.Name.Name != obj {
					continue
				}
				if t.Name.Schema != "" && t.Name.Schema != schema {
					continue
				}
				add(t, "owned by extension "+ext)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	// DROP EXTENSION with live members: the drop destroys every one of
	// them (CASCADE only additionally pulls in non-member dependents).
	for _, t := range extensionDrops {
		var members int
		if err := c.pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_depend d JOIN pg_extension e ON e.oid = d.refobjid WHERE d.deptype = 'e' AND e.extname = $1`,
			t.Name.Name).Scan(&members); err != nil {
			return nil, err
		}
		if members > 0 {
			add(t, fmt.Sprintf("DROP EXTENSION destroys all %d extension-owned member object(s) — extension members are protected whatever flags are passed", members))
		}
	}

	// Intra-batch ordering (review-5 BLOCKER-1): validation is
	// pre-execution, so members installed by a CREATE EXTENSION in this
	// same batch are invisible to the member count above. A DROP
	// EXTENSION whose extension the batch also creates is refused on
	// the ordering alone.
	createdExtensions := make(map[string]bool, len(extensionCreates))
	for _, t := range extensionCreates {
		createdExtensions[t.Name.Name] = true
	}
	for _, t := range extensionDrops {
		if createdExtensions[t.Name.Name] {
			add(t, fmt.Sprintf("this migration batch also CREATEs extension %s — its members are invisible to the pre-execution member check, so the DROP is refused; drop it in a later run after inspecting its members", t.Name.Name))
		}
	}

	for _, t := range ownedDrops {
		add(t, "DROP OWNED destroys every object the named role(s) own — the victims are not nameable from the statement, so it can never be proven safe")
	}

	for _, t := range schemaCascades {
		protected, err := c.protectedObjectsInSchema(ctx, t.Name.Name)
		if err != nil {
			return nil, err
		}
		hits, err := c.cascadeSchemaDependentsProtected(ctx, t.Name.Name)
		if err != nil {
			return nil, err
		}
		protectedSet := make(map[string]bool, len(protected))
		for _, p := range protected {
			protectedSet[p] = true
		}
		transitive := hits[:0:0]
		for _, h := range hits {
			if !protectedSet[h] {
				transitive = append(transitive, h)
			}
		}
		switch {
		case len(protected) > 0 && len(transitive) > 0:
			add(t, fmt.Sprintf("DROP SCHEMA ... CASCADE would destroy protected object(s) inside: %s; and transitively (across schemas): %s",
				strings.Join(firstNSorted(protected, 5), ", "), strings.Join(firstNSorted(transitive, 5), ", ")))
		case len(protected) > 0:
			add(t, fmt.Sprintf("DROP SCHEMA ... CASCADE would destroy protected object(s) inside: %s", strings.Join(firstNSorted(protected, 5), ", ")))
		case len(transitive) > 0:
			add(t, fmt.Sprintf("DROP SCHEMA ... CASCADE would transitively destroy or alter protected object(s): %s", strings.Join(firstNSorted(transitive, 5), ", ")))
		}
	}

	for _, t := range relationCascades {
		hits, err := c.cascadeDependentsProtected(ctx, t.Name)
		if err != nil {
			return nil, err
		}
		if len(hits) > 0 {
			add(t, fmt.Sprintf("CASCADE would transitively destroy or alter protected object(s): %s", strings.Join(firstNSorted(hits, 5), ", ")))
		}
	}

	// Every remaining allowlisted drop kind (review-6 BLOCKER-1): the
	// closure seeds from the kind's own catalog class, so no spelling of
	// CASCADE escapes the transitive check.
	for _, t := range namedCascades {
		hits, err := c.cascadeNamedKindDependentsProtected(ctx, t.Kind, t.Name)
		if err != nil {
			return nil, err
		}
		if len(hits) > 0 {
			add(t, fmt.Sprintf("CASCADE would transitively destroy or alter protected object(s): %s", strings.Join(firstNSorted(hits, 5), ", ")))
		}
	}

	return violations, nil
}

func firstNSorted(xs []string, n int) []string {
	sort.Strings(xs)
	if len(xs) > n {
		return xs[:n]
	}
	return xs
}

// protectedObjectsInSchema lists extension members (any catalog class) and
// _neutron_-prefixed relations living in the named schema.
func (c *Client) protectedObjectsInSchema(ctx context.Context, schema string) ([]string, error) {
	rows, err := c.pool.Query(ctx, `
		SELECT label FROM (
			SELECT n.nspname || '.' || c.relname AS label
			FROM pg_depend d
			JOIN pg_extension e ON e.oid = d.refobjid
			JOIN pg_class c ON c.oid = d.objid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE d.deptype = 'e' AND d.classid = 'pg_class'::regclass AND n.nspname = $1
			UNION
			SELECT n.nspname || '.' || t.typname
			FROM pg_depend d
			JOIN pg_extension e ON e.oid = d.refobjid
			JOIN pg_type t ON t.oid = d.objid
			JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE d.deptype = 'e' AND d.classid = 'pg_type'::regclass AND n.nspname = $1
			UNION
			SELECT n.nspname || '.' || p.proname
			FROM pg_depend d
			JOIN pg_extension e ON e.oid = d.refobjid
			JOIN pg_proc p ON p.oid = d.objid
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE d.deptype = 'e' AND d.classid = 'pg_proc'::regclass AND n.nspname = $1
			UNION
			SELECT n.nspname || '.' || c.relname
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname LIKE '\_neutron%' AND n.nspname = $1
		) q LIMIT 10`, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			return nil, err
		}
		out = append(out, label)
	}
	return out, rows.Err()
}

// cascadeProtectedQueryHead/Tail form the one recursive pg_depend
// closure shared by EVERY CASCADE check (review-5 BLOCKER-1
// generalization, widened in review-6): the protected set intersected
// with everything that transitively depends on the seed rows. The seed
// is per drop-kind, spliced between the two halves; the recursion
// handles arbitrary classids, including the pg_rewrite hop that maps a
// view's rule to the view's relation. (String concatenation, not
// Sprintf: the body carries a literal % in its LIKE pattern.)
//
// The protected set holds:
//   - extension members of ANY catalog class (deptype 'e');
//   - _neutron_-prefixed relations AND types (planted lookalikes
//     included — the create-arm refuses planting them in migrations,
//     pre-existing ones stay protected here);
//   - objects ATTACHED to a protected relation (review-6 BLOCKER-1
//     seam c): every pg_depend row of an attaching deptype (auto or
//     internal — constraints, column defaults, triggers, rewrite
//     rules, indexes) whose refobjid IS a protected relation. A
//     cascade that reaches such a row strips or mutates the protected
//     relation's definition, so the intersection fires and the label
//     projection below names the owning relation.
const cascadeProtectedQueryHead = `
	WITH RECURSIVE prot(oid, classid) AS (
		SELECT d.objid, d.classid FROM pg_depend d WHERE d.deptype = 'e'
		UNION
		SELECT cl.oid, 'pg_class'::regclass FROM pg_class cl
		JOIN pg_namespace n ON n.oid = cl.relnamespace
		WHERE cl.relname LIKE '\_neutron%'
		UNION
		SELECT ty.oid, 'pg_type'::regclass FROM pg_type ty
		JOIN pg_namespace n ON n.oid = ty.typnamespace
		WHERE ty.typname LIKE '\_neutron%'
		UNION
		SELECT d.objid, d.classid FROM pg_depend d
		WHERE d.refclassid = 'pg_class'::regclass
			AND d.deptype IN ('a', 'i')
			AND d.refobjid IN (
				SELECT cl.oid FROM pg_class cl
				JOIN pg_namespace n ON n.oid = cl.relnamespace
				WHERE cl.relname LIKE '\_neutron%'
				UNION
				SELECT e.objid FROM pg_depend e
				WHERE e.deptype = 'e' AND e.classid = 'pg_class'::regclass
			)
	),
	dep(oid, classid) AS (
	`

const cascadeProtectedQueryTail = `
		UNION
		SELECT y.oid, y.classid FROM dep CROSS JOIN LATERAL (
			SELECT d2.objid AS oid, d2.classid AS classid FROM pg_depend d2
			WHERE d2.refclassid = dep.classid AND d2.refobjid = dep.oid
			UNION ALL
			SELECT r.ev_class, 'pg_class'::regclass FROM pg_rewrite r
			WHERE dep.classid = 'pg_rewrite'::regclass AND r.oid = dep.oid
		) y
	)
	SELECT COALESCE(
		(SELECT 'pg_class ' || n.nspname || '.' || cl.relname FROM pg_class cl JOIN pg_namespace n ON n.oid = cl.relnamespace WHERE cl.oid = p.oid AND p.classid = 'pg_class'::regclass),
		(SELECT 'pg_proc ' || n.nspname || '.' || pr.proname FROM pg_proc pr JOIN pg_namespace n ON n.oid = pr.pronamespace WHERE pr.oid = p.oid AND p.classid = 'pg_proc'::regclass),
		(SELECT 'pg_type ' || n.nspname || '.' || ty.typname FROM pg_type ty JOIN pg_namespace n ON n.oid = ty.typnamespace WHERE ty.oid = p.oid AND p.classid = 'pg_type'::regclass),
		(SELECT 'pg_class ' || n.nspname || '.' || cl.relname || ' (attached constraint)' FROM pg_constraint cn JOIN pg_class cl ON cl.oid = cn.conrelid JOIN pg_namespace n ON n.oid = cl.relnamespace WHERE cn.oid = p.oid AND p.classid = 'pg_constraint'::regclass),
		(SELECT 'pg_class ' || n.nspname || '.' || cl.relname || ' (attached default)' FROM pg_attrdef ad JOIN pg_class cl ON cl.oid = ad.adrelid JOIN pg_namespace n ON n.oid = cl.relnamespace WHERE ad.oid = p.oid AND p.classid = 'pg_attrdef'::regclass),
		(SELECT 'pg_class ' || n.nspname || '.' || cl.relname || ' (attached trigger)' FROM pg_trigger tg JOIN pg_class cl ON cl.oid = tg.tgrelid JOIN pg_namespace n ON n.oid = cl.relnamespace WHERE tg.oid = p.oid AND p.classid = 'pg_trigger'::regclass),
		(SELECT 'pg_class ' || n.nspname || '.' || cl.relname || ' (attached rule)' FROM pg_rewrite rw JOIN pg_class cl ON cl.oid = rw.ev_class JOIN pg_namespace n ON n.oid = cl.relnamespace WHERE rw.oid = p.oid AND p.classid = 'pg_rewrite'::regclass),
		p.classid::regclass::text || ' ' || p.oid::text)
	FROM dep JOIN prot p ON p.oid = dep.oid AND p.classid = dep.classid
	LIMIT 10`

func (c *Client) queryCascadeProtected(ctx context.Context, seedSQL string, args ...any) ([]string, error) {
	rows, err := c.pool.Query(ctx, cascadeProtectedQueryHead+seedSQL+cascadeProtectedQueryTail, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			return nil, err
		}
		out = append(out, label)
	}
	return out, rows.Err()
}

// cascadeDependentsProtected returns the protected objects that a CASCADE
// drop of the named relation would transitively destroy or alter: everything
// that depends on it (recursively, via pg_depend) intersected with the
// protected set — extension members of any class, _neutron_-prefixed
// relations and types, and metadata attached to protected relations.
func (c *Client) cascadeDependentsProtected(ctx context.Context, name QualifiedName) ([]string, error) {
	var oid *uint32
	if err := c.pool.QueryRow(ctx, "SELECT to_regclass($1)::oid", regclassRef(name.Schema, name.Name)).Scan(&oid); err != nil {
		return nil, err
	}
	if oid == nil {
		return nil, nil
	}
	return c.queryCascadeProtected(ctx,
		`SELECT d.objid, d.classid FROM pg_depend d
		WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = $1`, *oid)
}

// cascadeSeedCatalog is the catalog class a non-relation drop kind's named
// object resolves against for the CASCADE seed (review-6 BLOCKER-1 seam a):
// the kind's class, its name column, and — when its objects are
// schema-qualified — its namespace column. Identifiers come from this
// fixed table in code, never from migration SQL.
type cascadeSeedCatalog struct {
	class   string
	nameCol string
	nsCol   string
}

// cascadeSeedCatalogs covers every allowlisted CASCADE-bearing drop kind
// that is neither a relation (pg_class, search-path resolution), a schema
// (namespace-containment seed) nor an extension (member rule): the
// routine family seeds pg_proc by name (prokind-agnostic — refusing a
// DROP whose spelling names the wrong routine kind errs only by
// refusing, matching the type/namespace matching rule), operator
// classes/families, collations, conversions, statistics, the text-search
// classes, and the relation-attached kinds (policy/trigger/rule match by
// name across relations; nothing normally depends on them, but the
// closure runs for them like every other kind — no kind outside).
var cascadeSeedCatalogs = map[string]cascadeSeedCatalog{
	"type":                   {"pg_type", "typname", "typnamespace"},
	"domain":                 {"pg_type", "typname", "typnamespace"},
	"function":               {"pg_proc", "proname", "pronamespace"},
	"procedure":              {"pg_proc", "proname", "pronamespace"},
	"routine":                {"pg_proc", "proname", "pronamespace"},
	"aggregate":              {"pg_proc", "proname", "pronamespace"},
	"operator":               {"pg_operator", "oprname", "oprnamespace"},
	"collation":              {"pg_collation", "collname", "collnamespace"},
	"conversion":             {"pg_conversion", "conname", "connamespace"},
	"operator-class":         {"pg_opclass", "opcname", "opcnamespace"},
	"operator-family":        {"pg_opfamily", "opfname", "opfnamespace"},
	"statistics":             {"pg_statistic_ext", "stxname", "stxnamespace"},
	"text-search-config":     {"pg_ts_config", "cfgname", "cfgnamespace"},
	"text-search-dictionary": {"pg_ts_dict", "dictname", "dictnamespace"},
	"text-search-parser":     {"pg_ts_parser", "prsname", "prsnamespace"},
	"text-search-template":   {"pg_ts_template", "tmplname", "tmplnamespace"},
	"policy":                 {"pg_policy", "polname", ""},
	"trigger":                {"pg_trigger", "tgname", ""},
	"rule":                   {"pg_rewrite", "rulename", ""},
	// Schema membership is live for the name-class membership arm only
	// (ALTER SCHEMA RENAME/OWNER of a member schema — review-7 MAJOR-1's
	// class: PostgreSQL permits it, and ALTER EXTENSION ... ADD SCHEMA
	// constructs members); CASCADE drops of schemas route through the
	// namespace-containment seed above and never read this entry.
	"schema": {"pg_namespace", "nspname", ""},
}

// cascadeNamedKindDependentsProtected is cascadeDependentsProtected for a
// named non-relation drop kind: the seed is every pg_depend row
// referencing the named object of the kind's catalog class. Unqualified
// names match objects in any namespace (safe-direction matching,
// consistent with the extension-membership checks).
func (c *Client) cascadeNamedKindDependentsProtected(ctx context.Context, kind string, name QualifiedName) ([]string, error) {
	cat, ok := cascadeSeedCatalogs[kind]
	if !ok {
		return nil, fmt.Errorf("cascade closure has no seed catalog for drop kind %q", kind)
	}
	join, match := "", ""
	args := []any{name.Name}
	if cat.nsCol != "" {
		join = "JOIN pg_namespace n ON n.oid = o." + cat.nsCol
		match = " AND ($1 = '' OR n.nspname = $1)"
		args = append([]any{name.Schema}, args...)
	}
	seed := `SELECT d.objid, d.classid FROM pg_depend d
		WHERE EXISTS (
			SELECT 1 FROM ` + cat.class + ` o ` + join + `
			WHERE o.` + cat.nameCol + ` = $` + fmt.Sprint(len(args)) + match + `
				AND d.refclassid = '` + cat.class + `'::regclass AND o.oid = d.refobjid
		)`
	return c.queryCascadeProtected(ctx, seed, args...)
}

// cascadeSchemaDependentsProtected is cascadeDependentsProtected for a
// DROP SCHEMA ... CASCADE seed (review-6 BLOCKER-1 seam b): every object
// the namespace contains seeds the closure, through the one containment
// edge PostgreSQL itself uses — each schema-contained object of ANY
// catalog class (pg_class, pg_type, pg_proc, pg_collation, pg_operator,
// pg_opclass, pg_opfamily, pg_conversion, pg_statistic_ext, pg_ts_*) has
// a normal pg_depend row referencing its pg_namespace. Protected
// dependents in OTHER schemas are therefore visible whatever class the
// contained objects belong to, and contained protected objects
// themselves (a collation owned by an extension, a planted _neutron_
// type) enter the closure as seed rows. Protected relations living in
// the schema are protectedObjectsInSchema's business (message listing).
func (c *Client) cascadeSchemaDependentsProtected(ctx context.Context, schema string) ([]string, error) {
	return c.queryCascadeProtected(ctx,
		`SELECT d.objid, d.classid FROM pg_depend d
		WHERE d.refclassid = 'pg_namespace'::regclass
			AND d.refobjid = (SELECT oid FROM pg_namespace WHERE nspname = $1)`, schema)
}

// ---------------------------------------------------------------------------
// Session execution primitives
// ---------------------------------------------------------------------------

// NontransactionalPartialError reports a nontransactional migration that
// failed mid-file: statements before Applied have durable effects that no
// rollback can remove. Recovery is explicit (neutron migrate resolve).
type NontransactionalPartialError struct {
	Applied int
	Total   int
	Err     error
}

func (e *NontransactionalPartialError) Error() string {
	return fmt.Sprintf("statement %d of %d failed after %d statement(s) had already taken effect outside any transaction: %v",
		e.Applied+1, e.Total, e.Applied, e.Err)
}

func (e *NontransactionalPartialError) Unwrap() error { return e.Err }

// ApplyNontransactionalMigration executes each statement as its own implicit
// transaction on the pinned session (required for CREATE INDEX CONCURRENTLY),
// then records the history row once every statement succeeded. The row write
// is the only atomic unit available; a kill window between the last statement
// and the row leaves fully-applied effects with no history — exactly the
// state `neutron migrate resolve --mark-applied` verifies and closes.
func (s *MigrationSession) ApplyNontransactionalMigration(ctx context.Context, mf MigrationFile, stmts []string, onApplied func(index int, stmt string)) error {
	for i, stmt := range stmts {
		if !hasExecutableSQL(stmt) {
			continue
		}
		if _, err := s.conn.Exec(ctx, stmt); err != nil {
			return &NontransactionalPartialError{Applied: i, Total: len(stmts), Err: err}
		}
		if onApplied != nil {
			onApplied(i, stmt)
		}
	}
	return s.RecordAppliedVersion(ctx, mf)
}

// RecordAppliedVersion writes the history row (checksum, owner, format) in
// one transaction without running any SQL. Callers must have verified the
// migration's postconditions first — this records, it never proves.
func (s *MigrationSession) RecordAppliedVersion(ctx context.Context, mf MigrationFile) error {
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx,
		"INSERT INTO _neutron_migrations (version, name, checksum, owner, format) VALUES ($1, $2, $3, $4, $5)",
		mf.Version, mf.Name, MigrationChecksum(mf.SQL), MigrationOwnerCLI, MigrationHistoryFormat,
	); err != nil {
		return fmt.Errorf("record migration %s: %w", mf.Version, err)
	}
	return tx.Commit(ctx)
}

// ApplyStatementsTx executes the statements as ONE transaction on the
// pinned session (the session-level counterpart of Client.ApplyInTransaction,
// so callers holding the advisory lock run their work on the locked
// connection instead of a pool checkout).
func (s *MigrationSession) ApplyStatementsTx(ctx context.Context, statements []string, onApplied func(stmt string)) error {
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	for _, stmt := range statements {
		if !hasExecutableSQL(stmt) {
			if onApplied != nil {
				onApplied(stmt)
			}
			continue
		}
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("apply %q: %w", firstSQLLine(stmt), err)
		}
		if onApplied != nil {
			onApplied(stmt)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
