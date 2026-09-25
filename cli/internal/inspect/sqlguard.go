package inspect

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Read-only enforcement for inspection tools.
//
// PostgreSQL enforces read-only itself: statements run inside BEGIN READ
// ONLY and are rolled back, so a data-modifying CTE, SELECT INTO,
// nextval() or EXPLAIN ANALYZE of DML fails with 25006 before it writes.
// Session-level side effects (advisory locks, dblink connections, settings)
// are removed at the connection level: after every read the session's
// advisory locks are released and the connection is closed instead of
// returned to the pool (db.Client.ReadOnlyTx). The lexical guard there only
// keeps to one statement of a read shape and refuses the built-in functions
// whose effects escape both the rollback and the session (statistics
// resets, WAL messages, replication state, other backends, remote servers,
// the filesystem).
//
// Residual gap: a denylist of names cannot see through a user-defined
// function, view or operator that wraps one of those functions. Run
// inspection reads under a low-privilege role (most of them require
// superuser or explicit grants), which is what actually bounds them.
//
// Nucleus does not apply READ ONLY (capability report
// txn.read_only_rejects_writes: unsupported) and its specialty models are
// written through ordinary SELECTs (SELECT KV_SET(...)). There the guard is
// the only enforcement, so it is strict and fails closed — and best-effort:
// a view or routine that wraps a mutating function is invisible to a name
// check, Nucleus has no READ ONLY or rollback to catch it, and the write
// persists (pinned by TestMCPNucleusWrappedMutatorGapIsDocumented). Its
// roles do not reliably bound this either. Data-modifying
// keywords anywhere, SELECT INTO, row locks, EXPLAIN ANALYZE and every
// function the engine itself classifies as mutating are refused.

// ReadOnlyEnforcement names who enforces a read-only statement.
const (
	EnforcedByEngine  = "engine: BEGIN READ ONLY, then ROLLBACK; the session is then closed (advisory locks released)"
	EnforcedLexically = "lexical guard only: this engine does not apply READ ONLY (capability report txn.read_only_rejects_writes)"
)

// GuardError is a refusal by the read-only guard.
type GuardError struct {
	Reason string
	// unterminated: a literal or comment never ends.
	unterminated bool
}

func (e *GuardError) Error() string { return "read-only guard: " + e.Reason }

func refuse(format string, args ...any) error {
	return &GuardError{Reason: fmt.Sprintf(format, args...)}
}

func unterminated(what string) error {
	return &GuardError{Reason: "unterminated " + what, unterminated: true}
}

// token kinds
const (
	tkWord   = iota // unquoted identifier or keyword, upper-cased
	tkIdent         // quoted identifier ("..." or U&"..."), decoded, upper-cased
	tkString        // string literal
	tkPunct
)

// String literal forms. Only a plain literal's value is known exactly.
const (
	litPlain   = iota // '...' read standard-conforming, or $tag$...$tag$: lit is the value
	litEscaped        // backslash escapes present and not decoded
	litUnicode        // U&'...': escapes not decoded
)

type sqlToken struct {
	kind    int
	text    string // upper-cased name for tkWord/tkIdent, the character for tkPunct
	lit     string // tkString value (exact only for litPlain)
	litForm int
	uraw    string // U&"..." body before decoding
	uident  bool
}

// sqlWhitespace is every ASCII whitespace byte PostgreSQL's scanner
// (space, \t, \n, \r, \f, \v) and sqlparser (char::is_whitespace, which
// Nucleus uses) accept between tokens.
func sqlWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// scanSQL tokenizes SQL, skipping comments. String literals (standard,
// E-prefixed, U&-prefixed and dollar-quoted) become tkString tokens and quoted identifiers
// (including U&"..." with UESCAPE, decoded) become tkIdent tokens, so
// neither is ever read as a keyword, while a quoted or escaped function
// name is still compared by its decoded value.
//
// The scanner errs toward seeing MORE code than the server: a line comment
// ends at \n or \r (both servers), non-ASCII whitespace separates tokens
// (both servers would read it as part of an identifier, so this only ever
// adds matches), ASCII control characters outside literals and non-ASCII
// dollar-quote tags (where PostgreSQL and sqlparser disagree) are refused,
// and unterminated literals and comments are errors: a guard must not
// guess where they end.
//
// backslashStrings lexes '...' the way a server with
// standard_conforming_strings = off does (backslash escapes the next
// character), which moves where such a literal ends.
func scanSQL(sql string, backslashStrings bool) ([]sqlToken, error) {
	var out []sqlToken
	i, n := 0, len(sql)
	for i < n {
		c := sql[i]
		switch {
		case sqlWhitespace(c):
			i++
		case c < 0x20 || c == 0x7f:
			return nil, refuse("control character U+%04X outside a literal", c)
		case c >= 0x80:
			r, size := utf8.DecodeRuneInString(sql[i:])
			if r == utf8.RuneError && size <= 1 {
				return nil, refuse("invalid UTF-8")
			}
			if unicode.IsSpace(r) {
				i += size
				continue
			}
			word, next, err := scanWord(sql, i)
			if err != nil {
				return nil, err
			}
			out = append(out, sqlToken{kind: tkWord, text: strings.ToUpper(word)})
			i = next
		case c == '-' && i+1 < n && sql[i+1] == '-':
			for i < n && sql[i] != '\n' && sql[i] != '\r' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*':
			depth := 0
			for {
				if i+1 >= n {
					return nil, unterminated("block comment")
				}
				if sql[i] == '/' && sql[i+1] == '*' {
					depth++
					i += 2
					continue
				}
				if sql[i] == '*' && sql[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
					continue
				}
				i++
			}
		case (c == 'U' || c == 'u') && i+2 < n && sql[i+1] == '&' && sql[i+2] == '"':
			body, next, err := scanQuotedIdent(sql, i+2)
			if err != nil {
				return nil, err
			}
			out = append(out, sqlToken{kind: tkIdent, uident: true, uraw: body})
			i = next
		case (c == 'U' || c == 'u') && i+2 < n && sql[i+1] == '&' && sql[i+2] == '\'':
			// U&'...' lexes like a standard string (quotes double); its
			// escapes are decoded later by the server.
			val, next, err := scanQuoted(sql, i+2, false)
			if err != nil {
				return nil, err
			}
			out = append(out, sqlToken{kind: tkString, lit: val, litForm: litUnicode})
			i = next
		case (c == 'E' || c == 'e') && i+1 < n && sql[i+1] == '\'':
			val, next, err := scanQuoted(sql, i+1, true)
			if err != nil {
				return nil, err
			}
			form := litPlain
			if strings.Contains(val, `\`) {
				form = litEscaped
			}
			out = append(out, sqlToken{kind: tkString, lit: val, litForm: form})
			i = next
		case c == '\'':
			val, next, err := scanQuoted(sql, i, backslashStrings)
			if err != nil {
				return nil, err
			}
			form := litPlain
			if backslashStrings && strings.Contains(val, `\`) {
				form = litEscaped
			}
			out = append(out, sqlToken{kind: tkString, lit: val, litForm: form})
			i = next
		case c == '"':
			body, next, err := scanQuotedIdent(sql, i)
			if err != nil {
				return nil, err
			}
			out = append(out, sqlToken{kind: tkIdent, text: strings.ToUpper(body)})
			i = next
		case c == '$' && i+1 < n && (sql[i+1] == '$' || isIdentStart(sql[i+1])):
			// Dollar quote $tag$...$tag$ (a positional parameter $1 is not).
			j := i + 1
			for j < n && sql[j] != '$' && isIdentPart(sql[j]) {
				j++
			}
			if j < n && sql[j] == '$' {
				tag := sql[i : j+1]
				for k := 0; k < len(tag); k++ {
					if tag[k] >= 0x80 {
						return nil, refuse("non-ASCII dollar-quote tags are not accepted (PostgreSQL and Nucleus's parser disagree on them)")
					}
				}
				end := strings.Index(sql[j+1:], tag)
				if end < 0 {
					return nil, unterminated("dollar-quoted string")
				}
				out = append(out, sqlToken{kind: tkString, lit: sql[j+1 : j+1+end], litForm: litPlain})
				i = j + 1 + end + len(tag)
				continue
			}
			out = append(out, sqlToken{kind: tkPunct, text: "$"})
			i++
		case isIdentStart(c):
			word, next, err := scanWord(sql, i)
			if err != nil {
				return nil, err
			}
			out = append(out, sqlToken{kind: tkWord, text: strings.ToUpper(word)})
			i = next
		default:
			out = append(out, sqlToken{kind: tkPunct, text: string(c)})
			i++
		}
	}
	return resolveUnicodeIdents(out)
}

// scanWord reads an unquoted identifier starting at i. Non-ASCII
// whitespace ends it (see scanSQL).
func scanWord(sql string, i int) (string, int, error) {
	j := i
	for j < len(sql) {
		b := sql[j]
		if b < 0x80 {
			if !isIdentPart(b) {
				break
			}
			j++
			continue
		}
		r, size := utf8.DecodeRuneInString(sql[j:])
		if r == utf8.RuneError && size <= 1 {
			return "", 0, refuse("invalid UTF-8")
		}
		if unicode.IsSpace(r) {
			break
		}
		j += size
	}
	return sql[i:j], j, nil
}

// scanQuoted reads a '...' literal whose opening quote is at i. Doubled
// quotes are one quote; with backslashes, \x keeps both characters in the
// value and never ends the literal.
func scanQuoted(sql string, i int, backslashes bool) (string, int, error) {
	var b strings.Builder
	i++
	for i < len(sql) {
		switch {
		case backslashes && sql[i] == '\\':
			if i+1 >= len(sql) {
				return "", 0, unterminated("string literal")
			}
			b.WriteString(sql[i : i+2])
			i += 2
		case sql[i] == '\'':
			if i+1 < len(sql) && sql[i+1] == '\'' {
				b.WriteByte('\'')
				i += 2
				continue
			}
			return b.String(), i + 1, nil
		default:
			b.WriteByte(sql[i])
			i++
		}
	}
	return "", 0, unterminated("string literal")
}

// scanQuotedIdent reads a "..." identifier whose opening quote is at i.
func scanQuotedIdent(sql string, i int) (string, int, error) {
	var b strings.Builder
	i++
	for i < len(sql) {
		if sql[i] == '"' {
			if i+1 < len(sql) && sql[i+1] == '"' {
				b.WriteByte('"')
				i += 2
				continue
			}
			return b.String(), i + 1, nil
		}
		b.WriteByte(sql[i])
		i++
	}
	return "", 0, unterminated("quoted identifier")
}

// resolveUnicodeIdents decodes U&"..." identifiers, honouring a following
// UESCAPE 'c' clause, the way PostgreSQL does. An escape the server would
// reject is refused rather than guessed.
func resolveUnicodeIdents(toks []sqlToken) ([]sqlToken, error) {
	for i := range toks {
		if !toks[i].uident {
			continue
		}
		esc := '\\'
		if i+1 < len(toks) && toks[i+1].kind == tkWord && toks[i+1].text == "UESCAPE" {
			if i+2 >= len(toks) || toks[i+2].kind != tkString || toks[i+2].litForm != litPlain || utf8.RuneCountInString(toks[i+2].lit) != 1 {
				return nil, refuse("UESCAPE must be followed by a one-character string literal")
			}
			esc, _ = utf8.DecodeRuneInString(toks[i+2].lit)
			if strings.ContainsRune("0123456789abcdefABCDEF+'\" \t\n\r\f\v", esc) {
				return nil, refuse("invalid UESCAPE character")
			}
		}
		name, err := decodeUnicodeEscapes(toks[i].uraw, esc)
		if err != nil {
			return nil, err
		}
		toks[i].text = strings.ToUpper(name)
	}
	return toks, nil
}

// decodeUnicodeEscapes applies PostgreSQL's U& escapes: esc+XXXX,
// esc++XXXXXX (hex code points, surrogate pairs combined) and esc+esc.
func decodeUnicodeEscapes(body string, esc rune) (string, error) {
	rs := []rune(body)
	var out []rune
	hexAt := func(from, count int) (rune, bool) {
		if from+count > len(rs) {
			return 0, false
		}
		var v rune
		for _, r := range rs[from : from+count] {
			var d rune
			switch {
			case r >= '0' && r <= '9':
				d = r - '0'
			case r >= 'a' && r <= 'f':
				d = r - 'a' + 10
			case r >= 'A' && r <= 'F':
				d = r - 'A' + 10
			default:
				return 0, false
			}
			v = v*16 + d
		}
		return v, true
	}
	var pendingHigh rune
	for i := 0; i < len(rs); {
		if rs[i] != esc {
			if pendingHigh != 0 {
				return "", refuse("invalid Unicode surrogate pair in U& identifier")
			}
			out = append(out, rs[i])
			i++
			continue
		}
		if i+1 < len(rs) && rs[i+1] == esc {
			if pendingHigh != 0 {
				return "", refuse("invalid Unicode surrogate pair in U& identifier")
			}
			out = append(out, esc)
			i += 2
			continue
		}
		var cp rune
		if v, ok := hexAt(i+1, 4); ok {
			cp, i = v, i+5
		} else if i+1 < len(rs) && rs[i+1] == '+' {
			v, ok := hexAt(i+2, 6)
			if !ok {
				return "", refuse("invalid Unicode escape in U& identifier")
			}
			cp, i = v, i+8
		} else {
			return "", refuse("invalid Unicode escape in U& identifier")
		}
		switch {
		case cp >= 0xD800 && cp <= 0xDBFF:
			if pendingHigh != 0 {
				return "", refuse("invalid Unicode surrogate pair in U& identifier")
			}
			pendingHigh = cp
		case cp >= 0xDC00 && cp <= 0xDFFF:
			if pendingHigh == 0 {
				return "", refuse("invalid Unicode surrogate pair in U& identifier")
			}
			out = append(out, 0x10000+(pendingHigh-0xD800)<<10+(cp-0xDC00))
			pendingHigh = 0
		case cp == 0 || cp > 0x10FFFF:
			return "", refuse("invalid Unicode escape value in U& identifier")
		default:
			if pendingHigh != 0 {
				return "", refuse("invalid Unicode surrogate pair in U& identifier")
			}
			out = append(out, cp)
		}
	}
	if pendingHigh != 0 {
		return "", refuse("invalid Unicode surrogate pair in U& identifier")
	}
	return string(out), nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '$'
}

// readLeaders are the statement shapes an inspection tool may run.
var readLeaders = map[string]bool{
	"SELECT": true, "WITH": true, "SHOW": true, "EXPLAIN": true, "VALUES": true, "TABLE": true,
}

// postgresEscapingFns have effects a rolled-back READ ONLY transaction does
// not undo and that closing the session does not reset either: other
// backends, the server process, statistics, WAL and replication state,
// remote servers, the filesystem. (Session-level effects such as advisory
// locks are also released by discarding the connection after every read;
// they stay listed so the refusal is explicit.) pg_notify is not listed:
// PostgreSQL queues notifications until COMMIT and drops them on ROLLBACK.
var postgresEscapingFns = map[string]bool{
	"PG_ADVISORY_LOCK": true, "PG_ADVISORY_LOCK_SHARED": true,
	"PG_TRY_ADVISORY_LOCK": true, "PG_TRY_ADVISORY_LOCK_SHARED": true,
	"PG_TERMINATE_BACKEND": true, "PG_CANCEL_BACKEND": true,
	"PG_RELOAD_CONF": true, "PG_ROTATE_LOGFILE": true, "PG_PROMOTE": true,
	"PG_LOG_BACKEND_MEMORY_CONTEXTS": true, "PG_SWITCH_WAL": true,
	"PG_CREATE_RESTORE_POINT": true, "PG_CREATE_LOGICAL_REPLICATION_SLOT": true,
	"PG_CREATE_PHYSICAL_REPLICATION_SLOT": true, "PG_DROP_REPLICATION_SLOT": true,
	"PG_REPLICATION_SLOT_ADVANCE": true, "PG_SYNC_REPLICATION_SLOTS": true,
	"PG_LOGICAL_EMIT_MESSAGE": true, "PG_LOG_STANDBY_SNAPSHOT": true,
	"PG_BACKUP_START": true, "PG_BACKUP_STOP": true,
	"PG_WAL_REPLAY_PAUSE": true, "PG_WAL_REPLAY_RESUME": true,
	"PG_FILE_WRITE": true, "PG_FILE_UNLINK": true, "PG_FILE_RENAME": true, "PG_FILE_SYNC": true,
	"LO_EXPORT": true, "LO_IMPORT": true,
}

// postgresEscapingPrefixes cover function families: statistics resets
// (pg_stat_reset, pg_stat_reset_shared, ..., pg_stat_statements_reset),
// replication origins, slot copies and consuming slot reads, dblink and
// pg_background (both run statements on another connection).
var postgresEscapingPrefixes = []string{
	"PG_STAT_RESET", "PG_STAT_STATEMENTS_RESET", "PG_REPLICATION_ORIGIN_",
	"PG_COPY_LOGICAL_REPLICATION_SLOT", "PG_COPY_PHYSICAL_REPLICATION_SLOT",
	"PG_LOGICAL_SLOT_GET_", "DBLINK", "PG_BACKGROUND_",
}

func escapesRollback(name string) bool {
	if postgresEscapingFns[name] {
		return true
	}
	for _, p := range postgresEscapingPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// NucleusMutatingFns is the engine's own classification of state-changing
// scalar functions at the measured build: the union of
// scalar_fns::SIDE_EFFECTING_FN_NAMES and admission::MUTATING_SCALAR_FNS(_EXTRA).
// sqlguard_test.go re-reads both engine lists and fails if any name is
// missing here. Those lists are disk-admission lists, not a complete
// read/write classification: GRAPH_QUERY runs arbitrary Cypher (including
// CREATE/DELETE) and is in neither, so it is checked separately.
var NucleusMutatingFns = map[string]bool{
	"BLOB_DELETE": true, "BLOB_STORE": true, "BLOB_TAG": true,
	"COLUMNAR_INSERT": true, "CYPHER": true,
	"DATALOG_ASSERT": true, "DATALOG_CLEAR": true, "DATALOG_IMPORT": true,
	"DATALOG_IMPORT_GRAPH": true, "DATALOG_IMPORT_NODES": true,
	"DATALOG_RETRACT": true, "DATALOG_RULE": true,
	"DB_BRANCH_CREATE": true, "DB_BRANCH_DELETE": true, "DB_BRANCH_MERGE": true,
	"DOC_DELETE": true, "DOC_INSERT": true, "DOC_UPDATE": true,
	"FTS_INDEX": true, "FTS_INDEX_FACETED": true, "FTS_REMOVE": true,
	"GRAPH_ADD_EDGE": true, "GRAPH_ADD_NODE": true, "GRAPH_DELETE_EDGE": true, "GRAPH_DELETE_NODE": true,
	"KV_CDEL": true, "KV_CEXPIRE": true, "KV_DEL": true, "KV_EXPIRE": true, "KV_FLUSHDB": true,
	"KV_HDEL": true, "KV_HSET": true, "KV_INCR": true, "KV_LPOP": true, "KV_LPUSH": true,
	"KV_PFADD": true, "KV_PFMERGE": true, "KV_RPOP": true, "KV_RPUSH": true,
	"KV_SADD": true, "KV_SET": true, "KV_SETNX": true, "KV_SREM": true, "KV_ZADD": true, "KV_ZREM": true,
	"NEXTVAL": true, "SETVAL": true,
	"PROC_DROP": true, "PROC_REGISTER": true,
	"PUBSUB_PUBLISH": true, "RETENTION_SET": true,
	"SPARSE_INSERT": true, "SPARSE_REMOVE": true,
	"STREAM_XACK": true, "STREAM_XADD": true, "STREAM_XGROUP_CREATE": true, "STREAM_XREADGROUP": true,
	"SUBSCRIBE": true, "UNSUBSCRIBE": true,
	"TENSOR_STORE": true, "TS_INSERT": true, "TS_RETENTION": true,
	"VECTOR_DELETE": true, "VECTOR_INSERT": true,
	"VERSION_BRANCH": true, "VERSION_COMMIT": true,
	// PostgreSQL-standard functions with effects the engine does not roll
	// back or confine.
	"SET_CONFIG": true, "PG_NOTIFY": true,
}

// dataModifyingWords make a statement a write wherever they appear.
var dataModifyingWords = map[string]bool{
	"INSERT": true, "UPDATE": true, "DELETE": true, "MERGE": true,
	"TRUNCATE": true, "CREATE": true, "DROP": true, "ALTER": true,
	"GRANT": true, "REVOKE": true, "COPY": true, "CALL": true,
}

// CheckSingleStatement refuses text carrying more than one statement (or
// none). Used by the write tool, which runs exactly what was asked.
func CheckSingleStatement(sql string) error {
	toks, err := scanSQL(sql, false)
	if err != nil {
		return err
	}
	_, err = singleStatement(toks)
	return err
}

func singleStatement(toks []sqlToken) ([]sqlToken, error) {
	for i, t := range toks {
		if t.kind == tkPunct && t.text == ";" {
			for _, rest := range toks[i+1:] {
				if !(rest.kind == tkPunct && rest.text == ";") {
					return nil, refuse("exactly one statement is allowed")
				}
			}
			toks = toks[:i]
			break
		}
	}
	if len(toks) == 0 {
		return nil, refuse("empty statement")
	}
	return toks, nil
}

// CheckReadOnlySQL refuses statements an inspection tool must not run.
// product is the engine product ("postgres" enforces READ ONLY itself;
// anything else gets the strict lexical guard).
//
// Function names are refused wherever they occur as a name, not only when a
// '(' follows: what the server accepts between a name and its argument list
// (whitespace, comments) can then never hide a call.
func CheckReadOnlySQL(sql, product string) error {
	toks, err := scanSQL(sql, false)
	if err != nil {
		return err
	}
	if err := checkReadTokens(toks, product); err != nil {
		return err
	}
	// A server running with standard_conforming_strings = off ends '...'
	// literals differently (backslash escapes). The text must be a read
	// under that reading too. If it does not even lex that way, such a
	// server rejects it as well, so only that outcome is ignored.
	if strings.Contains(sql, `\`) {
		alt, err := scanSQL(sql, true)
		var ge *GuardError
		switch {
		case err == nil:
			if err := checkReadTokens(alt, product); err != nil {
				return err
			}
		case errors.As(err, &ge) && ge.unterminated:
		default:
			return err
		}
	}
	return nil
}

func checkReadTokens(toks []sqlToken, product string) error {
	toks, err := singleStatement(toks)
	if err != nil {
		return err
	}
	lead := toks[0]
	if lead.kind != tkWord || !readLeaders[lead.text] {
		what := lead.text
		if lead.kind == tkString {
			what = "a string literal"
		}
		return refuse("only SELECT, WITH, SHOW, EXPLAIN, VALUES and TABLE statements are inspection reads (got %s)", what)
	}
	strict := product != "postgres"
	for i, t := range toks {
		if t.kind != tkWord && t.kind != tkIdent {
			continue
		}
		name := t.text
		if escapesRollback(name) {
			return refuse("%s() has effects a rolled-back read-only transaction does not undo", strings.ToLower(name))
		}
		if strict && NucleusMutatingFns[name] {
			return refuse("%s() writes to a Nucleus store; use execute_sql (requires --allow-writes)", strings.ToLower(name))
		}
		if strict && name == "GRAPH_QUERY" {
			if err := checkGraphQueryCall(toks, i); err != nil {
				return err
			}
		}
		if !strict || t.kind != tkWord {
			continue
		}
		switch {
		case dataModifyingWords[t.text]:
			return refuse("%s makes this statement a write (this engine does not enforce READ ONLY, so it is refused lexically)", t.text)
		case t.text == "INTO":
			return refuse("SELECT INTO creates a table")
		case t.text == "FOR" && i+1 < len(toks) && toks[i+1].kind == tkWord &&
			(toks[i+1].text == "UPDATE" || toks[i+1].text == "SHARE" || toks[i+1].text == "NO" || toks[i+1].text == "KEY"):
			return refuse("row-locking clauses are not inspection reads")
		case t.text == "ANALYZE" && lead.text == "EXPLAIN":
			return refuse("EXPLAIN ANALYZE executes the statement; this engine does not enforce READ ONLY")
		}
	}
	return nil
}

// checkGraphQueryCall admits GRAPH_QUERY only as GRAPH_QUERY('<cypher>')
// with one plain literal that passes the same check as cypher_query:
// GRAPH_QUERY executes CREATE/DELETE, and a computed argument cannot be
// checked.
func checkGraphQueryCall(toks []sqlToken, i int) error {
	punct := func(k int, p string) bool {
		return k < len(toks) && toks[k].kind == tkPunct && toks[k].text == p
	}
	if !punct(i+1, "(") || i+2 >= len(toks) || toks[i+2].kind != tkString || toks[i+2].litForm != litPlain || !punct(i+3, ")") {
		return refuse("GRAPH_QUERY runs Cypher, which can write: query_sql accepts it only with one plain string literal of read-only Cypher (or use cypher_query)")
	}
	if err := CheckReadOnlyCypher(toks[i+2].lit); err != nil {
		return err
	}
	return nil
}

// cypherWriteClauses are Cypher clauses that change the graph.
var cypherWriteClauses = map[string]bool{
	"CREATE": true, "MERGE": true, "SET": true, "DELETE": true, "DETACH": true,
	"REMOVE": true, "DROP": true, "CALL": true, "LOAD": true, "FOREACH": true,
}

// CheckReadOnlyCypher refuses Cypher that writes. GRAPH_QUERY runs with no
// transaction boundary the tool could roll back, so this lexical check is
// the enforcement.
func CheckReadOnlyCypher(q string) error {
	i, n := 0, len(q)
	for i < n {
		c := q[i]
		switch {
		case c == '/' && i+1 < n && q[i+1] == '/':
			for i < n && q[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && q[i+1] == '*':
			end := strings.Index(q[i+2:], "*/")
			if end < 0 {
				return refuse("unterminated Cypher comment")
			}
			i += end + 4
		case c == '\'' || c == '"' || c == '`':
			quote := c
			i++
			closed := false
			for i < n {
				if q[i] == '\\' {
					i += 2
					continue
				}
				if q[i] == quote {
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return refuse("unterminated Cypher string")
			}
		case isIdentStart(c):
			j := i
			for j < n && isIdentPart(q[j]) {
				j++
			}
			// Property keys and labels (after '.' or ':') are names, not clauses.
			prev := byte(' ')
			for k := i - 1; k >= 0; k-- {
				if q[k] != ' ' && q[k] != '\t' && q[k] != '\n' && q[k] != '\r' {
					prev = q[k]
					break
				}
			}
			word := strings.ToUpper(q[i:j])
			if prev != '.' && prev != ':' && cypherWriteClauses[word] {
				return refuse("Cypher %s changes the graph; use execute_sql with GRAPH_QUERY (requires --allow-writes)", word)
			}
			i = j
		default:
			i++
		}
	}
	return nil
}
