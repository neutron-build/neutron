package inspect

import (
	"fmt"
	"strings"
)

// Read-only enforcement for inspection tools.
//
// PostgreSQL enforces read-only itself: statements run inside BEGIN READ
// ONLY and are rolled back, so a data-modifying CTE, SELECT INTO,
// nextval() or EXPLAIN ANALYZE of DML fails with 25006 before it writes.
// The lexical guard there only keeps to one statement of a read shape and
// refuses functions whose effects escape a rolled-back transaction.
//
// Nucleus does not apply READ ONLY (capability report
// txn.read_only_rejects_writes: unsupported) and its specialty models are
// written through ordinary SELECTs (SELECT KV_SET(...)). There the guard is
// the only enforcement, so it is strict and fails closed: data-modifying
// keywords anywhere, SELECT INTO, row locks, EXPLAIN ANALYZE and every
// function the engine itself classifies as mutating are refused.

// ReadOnlyEnforcement names who enforces a read-only statement.
const (
	EnforcedByEngine  = "engine: BEGIN READ ONLY, then ROLLBACK"
	EnforcedLexically = "lexical guard only: this engine does not apply READ ONLY (capability report txn.read_only_rejects_writes)"
)

// GuardError is a refusal by the read-only guard.
type GuardError struct{ Reason string }

func (e *GuardError) Error() string { return "read-only guard: " + e.Reason }

func refuse(format string, args ...any) error {
	return &GuardError{Reason: fmt.Sprintf(format, args...)}
}

// token kinds
const (
	tkWord = iota
	tkPunct
)

type sqlToken struct {
	kind int
	text string // upper-cased for words
}

// scanSQL tokenizes SQL, skipping comments, string literals (standard, E”
// and dollar-quoted) and quoted identifiers, which are never keywords or
// function names. Unterminated literals and comments are errors: a guard
// must not guess where they end.
func scanSQL(sql string) ([]sqlToken, error) {
	var out []sqlToken
	i, n := 0, len(sql)
	for i < n {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < n && sql[i+1] == '-':
			for i < n && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*':
			depth := 0
			for {
				if i+1 >= n {
					return nil, refuse("unterminated block comment")
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
		case c == '\'' || ((c == 'E' || c == 'e') && i+1 < n && sql[i+1] == '\''):
			escapes := c != '\''
			if escapes {
				i++
			}
			i++
			closed := false
			for i < n {
				if escapes && sql[i] == '\\' {
					i += 2
					continue
				}
				if sql[i] == '\'' {
					if i+1 < n && sql[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return nil, refuse("unterminated string literal")
			}
			out = append(out, sqlToken{kind: tkPunct, text: "'"})
		case c == '"':
			i++
			closed := false
			var ident strings.Builder
			for i < n {
				if sql[i] == '"' {
					if i+1 < n && sql[i+1] == '"' {
						ident.WriteByte('"')
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				ident.WriteByte(sql[i])
				i++
			}
			if !closed {
				return nil, refuse("unterminated quoted identifier")
			}
			// A quoted name is an identifier, never a keyword; keep it
			// (upper-cased, marked) so quoted function names are checked.
			out = append(out, sqlToken{kind: tkWord, text: "\"" + strings.ToUpper(ident.String())})
		case c == '$' && i+1 < n && (sql[i+1] == '$' || isIdentStart(sql[i+1])):
			// Dollar quote $tag$...$tag$ (a positional parameter $1 is not).
			j := i + 1
			for j < n && sql[j] != '$' && isIdentPart(sql[j]) {
				j++
			}
			if j < n && sql[j] == '$' {
				tag := sql[i : j+1]
				end := strings.Index(sql[j+1:], tag)
				if end < 0 {
					return nil, refuse("unterminated dollar-quoted string")
				}
				i = j + 1 + end + len(tag)
				out = append(out, sqlToken{kind: tkPunct, text: "'"})
				continue
			}
			out = append(out, sqlToken{kind: tkPunct, text: "$"})
			i++
		case isIdentStart(c):
			j := i
			for j < n && isIdentPart(sql[j]) {
				j++
			}
			out = append(out, sqlToken{kind: tkWord, text: strings.ToUpper(sql[i:j])})
			i = j
		default:
			out = append(out, sqlToken{kind: tkPunct, text: string(c)})
			i++
		}
	}
	return out, nil
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
// not undo (session-level locks, other backends, the server process,
// remote servers, the filesystem).
var postgresEscapingFns = map[string]bool{
	"PG_ADVISORY_LOCK": true, "PG_ADVISORY_LOCK_SHARED": true,
	"PG_TRY_ADVISORY_LOCK": true, "PG_TRY_ADVISORY_LOCK_SHARED": true,
	"PG_TERMINATE_BACKEND": true, "PG_CANCEL_BACKEND": true,
	"PG_RELOAD_CONF": true, "PG_ROTATE_LOGFILE": true, "PG_PROMOTE": true,
	"PG_LOG_BACKEND_MEMORY_CONTEXTS": true, "PG_SWITCH_WAL": true,
	"PG_CREATE_RESTORE_POINT": true, "PG_CREATE_LOGICAL_REPLICATION_SLOT": true,
	"PG_CREATE_PHYSICAL_REPLICATION_SLOT": true, "PG_DROP_REPLICATION_SLOT": true,
	"PG_FILE_WRITE": true, "PG_FILE_UNLINK": true, "PG_FILE_RENAME": true,
	"LO_EXPORT": true, "LO_IMPORT": true,
	"DBLINK": true, "DBLINK_EXEC": true, "DBLINK_CONNECT": true, "DBLINK_SEND_QUERY": true,
}

// NucleusMutatingFns is the engine's own classification of state-changing
// scalar functions at the measured build: the union of
// scalar_fns::SIDE_EFFECTING_FN_NAMES and admission::MUTATING_SCALAR_FNS(_EXTRA).
// sqlguard_test.go re-reads both engine lists and fails if any name is
// missing here.
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
	toks, err := scanSQL(sql)
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
func CheckReadOnlySQL(sql, product string) error {
	toks, err := scanSQL(sql)
	if err != nil {
		return err
	}
	// One statement: a semicolon may only end the text.
	if toks, err = singleStatement(toks); err != nil {
		return err
	}
	lead := toks[0]
	if lead.kind != tkWord || !readLeaders[lead.text] {
		return refuse("only SELECT, WITH, SHOW, EXPLAIN, VALUES and TABLE statements are inspection reads (got %s)", strings.Trim(lead.text, "\""))
	}
	strict := product != "postgres"
	for i, t := range toks {
		if t.kind != tkWord {
			continue
		}
		name := strings.TrimPrefix(t.text, "\"")
		isCall := i+1 < len(toks) && toks[i+1].kind == tkPunct && toks[i+1].text == "("
		if isCall {
			if postgresEscapingFns[name] {
				return refuse("%s() has effects a rolled-back read-only transaction does not undo", strings.ToLower(name))
			}
			if strict && NucleusMutatingFns[name] {
				return refuse("%s() writes to a Nucleus store; use execute_sql (requires --allow-writes)", strings.ToLower(name))
			}
		}
		if !strict || strings.HasPrefix(t.text, "\"") {
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
