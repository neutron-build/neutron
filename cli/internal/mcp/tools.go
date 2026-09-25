package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/inspect"
	"github.com/neutron-build/neutron/cli/internal/studio"
)

// Access classes. Every tool is read-only unless it is explicitly a write
// tool; write tools exist only when the operator starts the server with
// --allow-writes (the existing authorization), and over HTTP they also
// require NEUTRON_MCP_TOKEN.
const (
	accessRead  = "read-only"
	accessPlan  = "plan-only"
	accessWrite = "write"
)

// toolEnv is what a handler may use: the connection, the identified engine
// and the operator's settings.
type toolEnv struct {
	client        *db.Client
	engine        inspect.Engine
	allowWrites   bool
	redactor      inspect.Redactor
	migrationsDir string
}

// toolOutput is a handler's structured result.
type toolOutput struct {
	Data any
	// Notes explain truncation, enforcement or other qualifications.
	Notes []string
	// Model names the data model whose limits govern the call ("" = none).
	Model string
	// Models lists additional models a write touched.
	Models []string
	// PreRedacted marks data the handler already redacted itself.
	PreRedacted []string
}

// toolHandler executes one tool.
type toolHandler func(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error)

// toolDef is the MCP tool definition (sent to the AI in tools/list).
type toolDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	// Annotations are MCP tool hints (readOnlyHint, destructiveHint, ...).
	Annotations map[string]any `json:"annotations,omitempty"`
}

type toolSpec struct {
	def     toolDef
	handler toolHandler
	access  string
}

func readHints(title string) map[string]any {
	return map[string]any{"title": title, "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false}
}

// toolSpecs is the ordered registry.
func toolSpecs() []toolSpec {
	read := func(name, title, desc string, in map[string]any, h toolHandler) toolSpec {
		return toolSpec{def: toolDef{Name: name, Description: desc, InputSchema: in, Annotations: readHints(title)}, handler: h, access: accessRead}
	}
	return []toolSpec{
		read("list_tables", "List tables",
			"List all SQL tables in the connected database, with column counts and row estimates. Read-only.",
			schema(props{}, nil), handleListTables),
		read("describe_table", "Describe table",
			"Describe the schema of a specific SQL table: column names, data types, nullability, and primary key info. Read-only.",
			schema(props{"table": strProp("Table name to describe")}, []string{"table"}), handleDescribeTable),
		read("list_nucleus_models", "List Nucleus models",
			"List the Nucleus data models and their sizes. Nucleus stores are global and unnamed (one store per model); this reports the counts the engine exposes (KV keys, documents, FTS docs, blobs, graph nodes, CDC events, pub/sub channels). Models without a count function are listed for reference. Read-only.",
			schema(props{}, nil), handleListNucleusModels),
		read("query_sql", "Query SQL (read-only)",
			"Run ONE read-only SQL statement (SELECT, WITH, SHOW, EXPLAIN, VALUES, TABLE). On PostgreSQL it runs inside a READ ONLY transaction that is rolled back, so the server refuses any write; afterwards the connection's advisory locks are released and the connection is closed, so no session state outlives the call. Built-in functions whose effects escape both (statistics resets, WAL messages, replication state, other backends, dblink, server files) are refused by name; a name check cannot see through a user-defined function, view or operator that wraps one, so connect this server as a low-privilege role. On Nucleus, which does not apply READ ONLY, a lexical guard is the only enforcement: it refuses data-modifying keywords, every function the engine classifies as mutating, and GRAPH_QUERY unless its argument is one string literal of read-only Cypher; the engine's own lists are not a complete read/write classification, so a mutating function it adds outside them would pass. This is best-effort: a view or routine that wraps a mutating function is not caught and its write persists, so treat a Nucleus server as trusted-writer only. Values under secret-looking column names are redacted. Writes go through execute_sql, which exists only when the server was started with --allow-writes.",
			schema(props{
				"sql":   strProp("The SQL statement to run"),
				"limit": numProp("Maximum rows to return (default 100, max 1000)"),
			}, []string{"sql"}), handleQuerySQL),
		read("kv_get", "KV get",
			"Get the value for a single key from the Nucleus KV store (a single global keyspace). Read-only; the value is redacted when the key name looks like a secret.",
			schema(props{"key": strProp("Key to retrieve")}, []string{"key"}), handleKVGet),
		read("kv_scan", "KV scan",
			"List keys in the Nucleus KV store, optionally filtered by prefix. Returns a JSON array of matching keys. Read-only.",
			schema(props{"prefix": strProp("Key prefix filter (optional, empty = all keys)")}, nil), handleKVScan),
		read("fts_search", "Full-text search",
			"Full-text search over the Nucleus FTS index using BM25 ranking. Returns a JSON array of {doc_id, score}. Set fuzzy for edit-distance matching. Read-only.",
			schema(props{
				"query":        strProp("Search query text"),
				"fuzzy":        boolProp("Enable fuzzy (edit-distance) matching (default false)"),
				"max_distance": numProp("Max edit distance when fuzzy (default 2)"),
				"limit":        numProp("Maximum results (default 20)"),
			}, []string{"query"}), handleFTSSearch),
		read("vector_search", "Vector search",
			"Nearest-neighbor search over a vector column of a SQL table, ordered by VECTOR_DISTANCE. Provide the query vector as a JSON array of floats, e.g. [0.1, 0.2, 0.3]. Read-only.",
			schema(props{
				"table":     strProp("Table containing the vector column"),
				"column":    strProp("Vector column name"),
				"vector":    strProp("Query vector as JSON array, e.g. [0.1, 0.2, ...]"),
				"id_column": strProp("Identifier column to return (default 'id')"),
				"k":         numProp("Number of nearest neighbors to return (default 10)"),
				"metric":    strProp("Distance metric: cosine, l2, or inner (default cosine)"),
			}, []string{"table", "column", "vector"}), handleVectorSearch),
		read("cypher_query", "Cypher query (read-only)",
			"Run a read-only Cypher query against the Nucleus graph store (a single global graph). Clauses that change the graph (CREATE, MERGE, SET, DELETE, REMOVE, ...) are refused. Returns a JSON object with columns and rows.",
			schema(props{"query": strProp("Cypher query, e.g. MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 25")}, []string{"query"}), handleCypherQuery),
		read("doc_find", "Find documents",
			"Query the Nucleus document store with a JSON filter expression. Returns the matching documents. Read-only; values under secret-looking field names are redacted.",
			schema(props{
				"filter": strProp("JSON filter expression, e.g. {\"status\": \"active\"} (empty = all docs)"),
				"limit":  numProp("Maximum documents to return (default 20)"),
			}, nil), handleDocFind),
		read("ts_range", "Time-series range",
			"Aggregate a Nucleus time series over an epoch-millisecond window. The engine exposes range average and count only (no raw point fetch). Read-only.",
			schema(props{
				"series":   strProp("Time series name"),
				"start_ms": numProp("Range start, epoch milliseconds (default 0)"),
				"end_ms":   numProp("Range end, epoch milliseconds (required)"),
				"agg":      strProp("Aggregation: avg or count (default avg)"),
			}, []string{"series", "end_ms"}), handleTSRange),
		read("geo_distance", "Geo distance",
			"Compute the haversine distance in meters between two latitude/longitude points using the Nucleus GEO_DISTANCE function. Read-only.",
			schema(props{
				"lat1": numProp("First point latitude"),
				"lon1": numProp("First point longitude"),
				"lat2": numProp("Second point latitude"),
				"lon2": numProp("Second point longitude"),
			}, []string{"lat1", "lon1", "lat2", "lon2"}), handleGeoDistance),
		read("blob_list", "List blobs",
			"List keys in the Nucleus blob store, optionally filtered by prefix. Returns a JSON array of key strings. Read-only.",
			schema(props{"prefix": strProp("Key prefix filter (optional, empty = all blobs)")}, nil), handleBlobList),
		read("stream_range", "Stream range",
			"Read entries from a Nucleus stream (append-only log) over an epoch-millisecond window. Returns a JSON array of entries. Read-only (no consumer group is advanced).",
			schema(props{
				"stream":   strProp("Stream name"),
				"start_ms": numProp("Range start, epoch milliseconds (default 0)"),
				"end_ms":   numProp("Range end, epoch milliseconds (required)"),
				"limit":    numProp("Maximum entries to return (default 50)"),
			}, []string{"stream", "end_ms"}), handleStreamRange),
		read("datalog_query", "Datalog query",
			"Evaluate a query against the Nucleus datalog engine. Returns a JSON array of result tuples. Read-only; loading facts and rules (DATALOG_ASSERT/DATALOG_RULE) goes through execute_sql.",
			schema(props{"query": strProp("Datalog query, e.g. ancestor(alice, ?X)")}, []string{"query"}), handleDatalogQuery),
		read("cdc_changes", "CDC changes",
			"Read change data capture (CDC) events from the Nucleus log after a given sequence. Returns a JSON array of {seq, table, change, ts}. Events are metadata only, emitted before COMMIT (rolled-back work appears), and on the measured build only INSERT statements emit them. Read-only.",
			schema(props{
				"after_sequence": numProp("Return events after this sequence number (default 0)"),
				"limit":          numProp("Maximum events to return (default 50)"),
			}, nil), handleCDCChanges),
		read("pubsub_list", "List pub/sub channels",
			"List active pub/sub channels in the Nucleus database. Read-only.",
			schema(props{}, nil), handlePubSubList),
		read("engine_limits", "Engine and model limits",
			"Identify the connected engine and report, for every data model, whether it is available and what its writes actually guarantee: transaction behaviour, durability, atomicity with SQL, hazards, and the measured evidence behind each statement. Read this before relying on a transaction or on data surviving a crash.",
			schema(props{}, nil), handleEngineLimits),
		read("inspect_table", "Inspect table journey",
			"Follow one table across the stack: its structure and foreign keys, the migrations that mention it (with checksum status), a non-executed query plan, a sample of rows, graph nodes bound to its rows, and its change events, each stage stating whether this engine supports it and which model's limits apply. Read-only; secret-looking columns are redacted.",
			schema(props{
				"table":  strProp("Table name"),
				"schema": strProp("Schema name (default public)"),
				"sample": numProp("Rows to sample (default 5, max 50)"),
			}, []string{"table"}), handleInspectTable),
		read("migration_status", "Migration status",
			"Report applied and pending migrations: the _neutron_migrations history (read-only) joined with the files in the server's --migrations directory (the only directory it reads), each with its checksum status (verified, mismatch, unverified, pending, file-missing). PostgreSQL only.",
			schema(props{}, nil), handleMigrationStatus),
		read("explain_sql", "Explain SQL (not executed)",
			"Return PostgreSQL's EXPLAIN (FORMAT JSON) plan for one read-only statement. The statement is planned, never executed (no ANALYZE), inside a rolled-back READ ONLY transaction. PostgreSQL only.",
			schema(props{"sql": strProp("The read-only statement to plan")}, []string{"sql"}), handleExplainSQL),
		{
			def: toolDef{
				Name:        "plan_schema_changes",
				Description: "Plan schema changes without applying them: the CLI's own migration planner diffs the live catalog against the live catalog plus these edits and returns the ordered statements, down statements, destructive/data-loss risk, transaction mode and the equivalent CLI command. Nothing is applied; planning creates and drops session-temporary twin tables to normalize expressions. Apply through Studio's reviewed plan or `neutron db push`. PostgreSQL only.",
				InputSchema: schema(props{
					"changes": {"type": "array", "description": "Designer edits, e.g. [{\"op\":\"add-column\",\"schema\":\"public\",\"table\":\"orders\",\"column\":\"note\",\"type\":\"text\"}]. ops: create-table, drop-table, add-column, drop-column, rename-column, alter-column-type, set-not-null, drop-not-null, set-default, drop-default, add-index, drop-index.", "items": map[string]any{"type": "object"}},
				}, []string{"changes"}),
				Annotations: map[string]any{"title": "Plan schema changes", "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false},
			},
			handler: handlePlanSchemaChanges,
			access:  accessPlan,
		},
		{
			def: toolDef{
				Name:        "execute_sql",
				Description: "WRITE: run ONE SQL statement that may change data or schema, in its own transaction, and commit it. Exists only because the operator started the server with --allow-writes. The result states the engine's actual transaction and durability limits for the touched models (on Nucleus, DDL is not transactional and specialty-model writes are not isolated). Prefer query_sql for reads.",
				InputSchema: schema(props{
					"sql":   strProp("The statement to execute"),
					"limit": numProp("Maximum RETURNING rows to return (default 100, max 1000)"),
				}, []string{"sql"}),
				Annotations: map[string]any{"title": "Execute SQL (writes)", "readOnlyHint": false, "destructiveHint": true, "idempotentHint": false, "openWorldHint": false},
			},
			handler: handleExecuteSQL,
			access:  accessWrite,
		},
		read("search_docs", "Search Neutron docs",
			"Search the Neutron framework documentation by keyword. Returns matching pages with titles, URLs, slugs, and snippets. Use this to answer 'how do I ...' questions about Neutron (routing, loaders, actions, Nucleus, deployment, the SDKs).",
			schema(props{
				"query": strProp("Search terms, e.g. \"loader data\" or \"vector search\"."),
				"limit": numProp("Maximum results to return (default 8)."),
			}, []string{"query"}), legacyText(handleSearchDocs)),
		read("get_doc", "Get a Neutron doc page",
			"Fetch the full markdown of a single Neutron documentation page by slug (e.g. \"routing/app-routes\", \"nucleus/overview\"). Use after search_docs to read a page in full.",
			schema(props{"slug": strProp("Doc slug or path, e.g. \"data/loaders\" or \"/docs/data/loaders\".")}, []string{"slug"}), legacyText(handleGetDoc)),
	}
}

// legacyText adapts a text-returning handler (the docs tools).
func legacyText(h func(ctx context.Context, client *db.Client, args map[string]any) (string, error)) toolHandler {
	return func(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
		text, err := h(ctx, env.client, args)
		if err != nil {
			return nil, err
		}
		return &toolOutput{Data: parseJSONText(text)}, nil
	}
}

// availableSpecs lists the tools this server offers: write tools only with
// --allow-writes.
func availableSpecs(allowWrites bool) []toolSpec {
	var out []toolSpec
	for _, s := range toolSpecs() {
		if s.access == accessWrite && !allowWrites {
			continue
		}
		out = append(out, s)
	}
	return out
}

// toolList returns the tool definitions for tools/list.
func toolList(allowWrites bool) []toolDef {
	specs := availableSpecs(allowWrites)
	out := make([]toolDef, len(specs))
	for i, s := range specs {
		out[i] = s.def
	}
	return out
}

func lookupTool(name string, allowWrites bool) (toolSpec, bool) {
	for _, s := range availableSpecs(allowWrites) {
		if s.def.Name == name {
			return s, true
		}
	}
	return toolSpec{}, false
}

// envelope is a tool's structured result (MCP structuredContent).
type envelope struct {
	Tool        string                `json:"tool"`
	Engine      inspect.Engine        `json:"engine"`
	Access      string                `json:"access"`
	Enforcement string                `json:"enforcement,omitempty"`
	Limits      []inspect.ModelLimits `json:"limits,omitempty"`
	Redacted    []string              `json:"redacted,omitempty"`
	Notes       []string              `json:"notes,omitempty"`
	Data        any                   `json:"data"`
}

// errUnknownTool marks a tool that does not exist on this server.
var errUnknownTool = errors.New("unknown tool")

// callTool runs one tool through the shared gates: availability (write
// tools need --allow-writes), redaction and the limits envelope. Every
// transport goes through it.
func callTool(ctx context.Context, env *toolEnv, name string, args map[string]any) (*envelope, error) {
	spec, ok := lookupTool(name, env.allowWrites)
	if !ok {
		for _, s := range toolSpecs() {
			if s.def.Name == name && s.access == accessWrite {
				return nil, fmt.Errorf("%s is a write tool and this server is read-only; the operator must restart it with --allow-writes", name)
			}
		}
		return nil, fmt.Errorf("%w: %s", errUnknownTool, name)
	}
	return callToolWith(ctx, env, spec, args)
}

func callToolWith(ctx context.Context, env *toolEnv, spec toolSpec, args map[string]any) (*envelope, error) {
	name := spec.def.Name
	out, err := spec.handler(ctx, env, args)
	if err != nil {
		return nil, errors.New(inspect.RedactText(err.Error()))
	}
	env2 := &envelope{Tool: name, Engine: env.engine, Access: spec.access, Notes: out.Notes}
	switch spec.access {
	case accessRead:
		if env.engine.Product == "postgres" {
			env2.Enforcement = inspect.EnforcedByEngine
		} else {
			env2.Enforcement = inspect.EnforcedLexically
		}
	case accessPlan:
		env2.Enforcement = "plans only: nothing is applied; session-temporary twin tables are created and dropped"
	case accessWrite:
		env2.Enforcement = "operator-authorized writes (--allow-writes)"
	}
	models := out.Models
	if out.Model != "" {
		models = append([]string{out.Model}, models...)
	}
	if len(models) > 0 {
		rep := inspect.BuildReport(env.engine, nil)
		seen := map[string]bool{}
		for _, m := range models {
			if seen[m] {
				continue
			}
			seen[m] = true
			if ml, ok := rep.Model(m); ok {
				env2.Limits = append(env2.Limits, ml)
			}
		}
		if !rep.Current && rep.CurrentNote != "" {
			env2.Notes = append(env2.Notes, rep.CurrentNote)
		}
	}
	data, redacted := env.redactor.Value(out.Data)
	env2.Data = data
	env2.Redacted = mergeSorted(out.PreRedacted, redacted)
	return env2, nil
}

func mergeSorted(a, b []string) []string {
	if len(a) == 0 {
		return b
	}
	seen := map[string]bool{}
	var out []string
	for _, s := range append(append([]string{}, a...), b...) {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// parseJSONText decodes engine-returned JSON text losslessly (numbers stay
// json.Number); anything else is returned as the string.
func parseJSONText(text string) any {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" || (trimmed[0] != '{' && trimmed[0] != '[') {
		return text
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(trimmed)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return text
	}
	return v
}

// --- read helpers: every statement runs inside a rolled-back READ ONLY
// transaction ---

func (e *toolEnv) readRows(ctx context.Context, limit int, sql string, args ...any) ([]any, bool, error) {
	var out []any
	truncated := false
	err := inspect.ReadOnly(ctx, e.client, e.engine, func(tx pgx.Tx) error {
		// Unnamed statements: arbitrary SQL must not hit a cached plan whose
		// result shape changed after DDL elsewhere.
		rows, err := tx.Query(ctx, sql, append([]any{pgx.QueryExecModeExec}, args...)...)
		if err != nil {
			return err
		}
		defer rows.Close()
		fields := rows.FieldDescriptions()
		for rows.Next() {
			if limit > 0 && len(out) == limit {
				truncated = true
				break
			}
			vals, err := rows.Values()
			if err != nil {
				return err
			}
			row := make(map[string]any, len(fields))
			for i, f := range fields {
				row[f.Name] = inspect.CellValue(vals[i])
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	if out == nil {
		out = []any{}
	}
	return out, truncated, err
}

// readScalar runs a single-value read; SQL NULL is nil.
func (e *toolEnv) readScalar(ctx context.Context, sql string, args ...any) (*string, error) {
	var v *string
	err := inspect.ReadOnly(ctx, e.client, e.engine, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, sql, args...).Scan(&v)
	})
	return v, err
}

// scalarJSON runs a read returning JSON text and decodes it.
func (e *toolEnv) scalarJSON(ctx context.Context, model, sql string, args ...any) (*toolOutput, error) {
	v, err := e.readScalar(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return &toolOutput{Data: nil, Model: model}, nil
	}
	return &toolOutput{Data: parseJSONText(*v), Model: model}, nil
}

// --- Handlers ---

func handleListTables(ctx context.Context, env *toolEnv, _ map[string]any) (*toolOutput, error) {
	rows, _, err := env.readRows(ctx, 0, `
		SELECT
			t.table_schema,
			t.table_name,
			COUNT(c.column_name) AS column_count,
			pg_class.reltuples::bigint AS row_estimate
		FROM information_schema.tables t
		JOIN information_schema.columns c
			ON t.table_schema = c.table_schema AND t.table_name = c.table_name
		LEFT JOIN pg_class ON pg_class.relname = t.table_name
		WHERE t.table_schema NOT IN ('pg_catalog','information_schema')
		  AND t.table_type = 'BASE TABLE'
		GROUP BY t.table_schema, t.table_name, pg_class.reltuples
		ORDER BY t.table_schema, t.table_name
	`)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	return &toolOutput{Data: rows, Model: "sql"}, nil
}

func handleDescribeTable(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	table, _ := args["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("table argument is required")
	}
	// Nucleus does not implement information_schema.table_constraints /
	// key_column_usage / character_maximum_length; use information_schema.columns
	// and resolve primary keys best-effort from the pg_catalog virtual tables.
	rows, _, err := env.readRows(ctx, 0, `
		SELECT
			c.column_name,
			c.data_type,
			c.udt_name,
			c.is_nullable,
			c.column_default
		FROM information_schema.columns c
		WHERE c.table_name = $1
		  AND c.table_schema NOT IN ('pg_catalog','information_schema')
		ORDER BY c.ordinal_position
	`, table)
	if err != nil {
		return nil, fmt.Errorf("describe table: %w", err)
	}
	pk := mcpPKColumns(ctx, env, table)
	for _, r := range rows {
		m := r.(map[string]any)
		name, _ := m["column_name"].(string)
		m["is_primary_key"] = pk[name]
	}
	return &toolOutput{Data: rows, Model: "sql"}, nil
}

// mcpPKColumns resolves primary-key column names via pg_class -> pg_index ->
// pg_attribute (virtual tables both Nucleus and Postgres implement).
// Best-effort: empty on any error.
func mcpPKColumns(ctx context.Context, env *toolEnv, table string) map[string]bool {
	pk := map[string]bool{}
	_ = inspect.ReadOnly(ctx, env.client, env.engine, func(tx pgx.Tx) error {
		var oid int32
		if err := tx.QueryRow(ctx, `SELECT oid FROM pg_catalog.pg_class WHERE relname = $1`, table).Scan(&oid); err != nil {
			return err
		}
		var indkey string
		if err := tx.QueryRow(ctx, `SELECT indkey FROM pg_catalog.pg_index WHERE indisprimary AND indrelid = $1`, oid).Scan(&indkey); err != nil {
			return err
		}
		positions := map[string]bool{}
		for _, f := range strings.Fields(indkey) {
			positions[f] = true
		}
		rows, err := tx.Query(ctx, `SELECT attname, attnum::text FROM pg_catalog.pg_attribute WHERE attrelid = $1`, oid)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name, num string
			if rows.Scan(&name, &num) == nil && positions[num] {
				pk[name] = true
			}
		}
		return nil
	})
	return pk
}

func handleListNucleusModels(ctx context.Context, env *toolEnv, _ map[string]any) (*toolOutput, error) {
	// Nucleus stores are global and unnamed (one store per model). There is no
	// enumeration function; report each model with the counts the engine exposes.
	type modelResult struct {
		Model string `json:"model"`
		Count *int64 `json:"count,omitempty"`
		Note  string `json:"note,omitempty"`
	}
	scalarCount := func(sql string) *int64 {
		var n int64
		err := inspect.ReadOnly(ctx, env.client, env.engine, func(tx pgx.Tx) error { return tx.QueryRow(ctx, sql).Scan(&n) })
		if err != nil {
			return nil
		}
		return &n
	}
	results := []modelResult{
		{Model: "kv", Count: scalarCount(`SELECT KV_DBSIZE()`)},
		{Model: "document", Count: scalarCount(`SELECT DOC_COUNT()`)},
		{Model: "fts", Count: scalarCount(`SELECT FTS_DOC_COUNT()`)},
		{Model: "blob", Count: scalarCount(`SELECT BLOB_COUNT()`)},
		{Model: "cdc", Count: scalarCount(`SELECT CDC_COUNT()`)},
	}
	nodes := scalarCount(`SELECT GRAPH_NODE_COUNT()`)
	edges := scalarCount(`SELECT GRAPH_EDGE_COUNT()`)
	if nodes != nil || edges != nil {
		results = append(results, modelResult{Model: "graph", Count: nodes, Note: "node count; see GRAPH_EDGE_COUNT() for edges"})
	}
	if channels, err := env.readScalar(ctx, `SELECT PUBSUB_CHANNELS()`); err == nil && channels != nil {
		n := int64(0)
		for _, c := range strings.Split(*channels, ",") {
			if strings.TrimSpace(c) != "" {
				n++
			}
		}
		results = append(results, modelResult{Model: "pubsub", Count: &n})
	}
	for _, m := range []string{"vector", "timeseries", "geo", "columnar", "datalog", "streams"} {
		results = append(results, modelResult{Model: m, Note: "no engine enumeration; access by name via the model tools"})
	}
	return &toolOutput{Data: results, Notes: []string{"engine_limits reports what each model's writes guarantee"}}, nil
}

func handleQuerySQL(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	sql, _ := args["sql"].(string)
	if sql == "" {
		return nil, fmt.Errorf("sql argument is required")
	}
	if err := inspect.CheckReadOnlySQL(sql, env.engine.Product); err != nil {
		if env.allowWrites {
			return nil, fmt.Errorf("%v (query_sql is read-only; use execute_sql for writes)", err)
		}
		return nil, err
	}
	limit := intArg(args, "limit", 100)
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, truncated, err := env.readRows(ctx, limit, sql)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	out := &toolOutput{Data: rows, Model: "sql"}
	if truncated {
		out.Notes = append(out.Notes, fmt.Sprintf("result truncated at %d rows", limit))
	}
	return out, nil
}

func handleKVGet(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	key, _ := args["key"].(string)
	if key == "" {
		return nil, fmt.Errorf("key argument is required")
	}
	v, err := env.readScalar(ctx, "SELECT KV_GET($1)", key)
	if err != nil {
		return nil, err
	}
	out := &toolOutput{Model: "kv"}
	switch {
	case v == nil:
		out.Data = nil
	case env.redactor.Enabled && inspect.SensitiveName(key):
		out.Data = inspect.RedactedValue
		out.PreRedacted = []string{key}
	default:
		out.Data = parseJSONText(*v)
	}
	return out, nil
}

func handleKVScan(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	prefix, _ := args["prefix"].(string)
	// KV_KEYS(pattern) returns a JSON array of matching keys.
	return env.scalarJSON(ctx, "kv", "SELECT KV_KEYS($1)", prefix+"*")
}

func handleFTSSearch(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return nil, fmt.Errorf("query argument is required")
	}
	fuzzy, _ := args["fuzzy"].(bool)
	limit := intArg(args, "limit", 20)
	if fuzzy {
		return env.scalarJSON(ctx, "fts", "SELECT FTS_FUZZY_SEARCH($1, $2, $3)", query, intArg(args, "max_distance", 2), limit)
	}
	return env.scalarJSON(ctx, "fts", "SELECT FTS_SEARCH($1, $2)", query, limit)
}

func handleVectorSearch(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	table, _ := args["table"].(string)
	column, _ := args["column"].(string)
	vector, _ := args["vector"].(string)
	if table == "" || column == "" || vector == "" {
		return nil, fmt.Errorf("table, column, and vector arguments are required")
	}
	if !isSafeIdent(table) || !isSafeIdent(column) {
		return nil, fmt.Errorf("table and column must be simple identifiers")
	}
	idCol, _ := args["id_column"].(string)
	if idCol == "" {
		idCol = "id"
	}
	if !isSafeIdent(idCol) {
		return nil, fmt.Errorf("id_column must be a simple identifier")
	}
	k := intArg(args, "k", 10)
	metric, _ := args["metric"].(string)
	switch metric {
	case "", "cosine":
		metric = "cosine"
	case "l2", "inner":
	default:
		return nil, fmt.Errorf("metric must be one of: cosine, l2, inner")
	}
	sql := fmt.Sprintf(
		"SELECT %s AS id, VECTOR_DISTANCE(%s, VECTOR($1), $2) AS distance FROM %s ORDER BY distance LIMIT $3",
		idCol, column, table)
	rows, _, err := env.readRows(ctx, 0, sql, vector, metric, k)
	if err != nil {
		return nil, err
	}
	return &toolOutput{Data: rows, Model: "vector"}, nil
}

func handleCypherQuery(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return nil, fmt.Errorf("query argument is required")
	}
	if err := inspect.CheckReadOnlyCypher(query); err != nil {
		return nil, err
	}
	return env.scalarJSON(ctx, "graph", "SELECT GRAPH_QUERY($1)", query)
}

func handleDocFind(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	filter, _ := args["filter"].(string)
	if filter == "" {
		filter = "{}"
	}
	limit := intArg(args, "limit", 20)
	type doc struct {
		ID   string `json:"id"`
		Data any    `json:"data"`
	}
	docs := []any{}
	err := inspect.ReadOnly(ctx, env.client, env.engine, func(tx pgx.Tx) error {
		var ids string
		if err := tx.QueryRow(ctx, "SELECT DOC_QUERY($1)", filter).Scan(&ids); err != nil {
			return err
		}
		for _, id := range strings.Split(ids, ",") {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			if len(docs) >= limit {
				break
			}
			var data *string
			if err := tx.QueryRow(ctx, "SELECT DOC_GET($1)", id).Scan(&data); err != nil {
				continue
			}
			d := map[string]any{"id": id, "data": nil}
			if data != nil {
				d["data"] = parseJSONText(*data)
			}
			docs = append(docs, d)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &toolOutput{Data: docs, Model: "document"}, nil
}

func handleTSRange(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	series, _ := args["series"].(string)
	if series == "" {
		return nil, fmt.Errorf("series argument is required")
	}
	startMS := intArg(args, "start_ms", 0)
	endMS := intArg(args, "end_ms", 0)
	if endMS == 0 {
		return nil, fmt.Errorf("end_ms argument is required (epoch milliseconds)")
	}
	agg, _ := args["agg"].(string)
	switch agg {
	case "", "avg":
		return env.scalarJSON(ctx, "timeseries", "SELECT TS_RANGE_AVG($1, $2, $3)::text", series, startMS, endMS)
	case "count":
		return env.scalarJSON(ctx, "timeseries", "SELECT TS_RANGE_COUNT($1, $2, $3)::text", series, startMS, endMS)
	default:
		return nil, fmt.Errorf("agg must be one of: avg, count (the engine exposes no other range aggregate)")
	}
}

func handleGeoDistance(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	lat1, ok1 := args["lat1"].(float64)
	lon1, ok2 := args["lon1"].(float64)
	lat2, ok3 := args["lat2"].(float64)
	lon2, ok4 := args["lon2"].(float64)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return nil, fmt.Errorf("lat1, lon1, lat2, and lon2 arguments are required")
	}
	return env.scalarJSON(ctx, "geo", "SELECT GEO_DISTANCE($1, $2, $3, $4)::text", lat1, lon1, lat2, lon2)
}

func handleBlobList(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	prefix, _ := args["prefix"].(string)
	return env.scalarJSON(ctx, "blob", "SELECT BLOB_LIST($1)", prefix)
}

func handleStreamRange(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	stream, _ := args["stream"].(string)
	if stream == "" {
		return nil, fmt.Errorf("stream argument is required")
	}
	startMS := intArg(args, "start_ms", 0)
	endMS := intArg(args, "end_ms", 0)
	if endMS == 0 {
		return nil, fmt.Errorf("end_ms argument is required (epoch milliseconds)")
	}
	return env.scalarJSON(ctx, "streams", "SELECT STREAM_XRANGE($1, $2, $3, $4)", stream, startMS, endMS, intArg(args, "limit", 50))
}

func handleDatalogQuery(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return nil, fmt.Errorf("query argument is required")
	}
	return env.scalarJSON(ctx, "datalog", "SELECT DATALOG_QUERY($1)", query)
}

func handleCDCChanges(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	return env.scalarJSON(ctx, "cdc", "SELECT CDC_READ($1, $2)", intArg(args, "after_sequence", 0), intArg(args, "limit", 50))
}

func handlePubSubList(ctx context.Context, env *toolEnv, _ map[string]any) (*toolOutput, error) {
	v, err := env.readScalar(ctx, "SELECT PUBSUB_CHANNELS()")
	if err != nil {
		return nil, err
	}
	channels := []string{}
	if v != nil {
		for _, c := range strings.Split(*v, ",") {
			if c = strings.TrimSpace(c); c != "" {
				channels = append(channels, c)
			}
		}
	}
	return &toolOutput{Data: channels, Model: "pubsub"}, nil
}

func handleEngineLimits(ctx context.Context, env *toolEnv, _ map[string]any) (*toolOutput, error) {
	var live *inspect.LiveSettings
	if env.engine.Product == "postgres" {
		live = inspect.ReadLiveSettings(ctx, env.client)
	}
	return &toolOutput{Data: inspect.BuildReport(env.engine, live)}, nil
}

func handleInspectTable(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	table, _ := args["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("table argument is required")
	}
	schemaName, _ := args["schema"].(string)
	if schemaName == "" {
		schemaName = "public"
	}
	j, err := inspect.BuildJourney(ctx, env.client, inspect.JourneyOptions{
		Schema:        schemaName,
		Table:         table,
		MigrationsDir: env.migrationsDir,
		SampleRows:    intArg(args, "sample", 5),
		QueriesReason: "the MCP server keeps no statement record; Studio's journey lists the statements it executed",
	})
	if err != nil {
		return nil, err
	}
	// Row samples are positional; redact by column name here.
	var redacted []string
	if st := j.Stage(inspect.StageRows); st != nil && env.redactor.Enabled {
		if rd, ok := st.Data.(inspect.RowsData); ok {
			for ci, c := range rd.Columns {
				if !inspect.SensitiveName(c) {
					continue
				}
				redacted = append(redacted, c)
				for _, r := range rd.Rows {
					if r[ci] != nil {
						r[ci] = inspect.RedactedValue
					}
				}
			}
			st.Data = rd
		}
	}
	return &toolOutput{Data: j, PreRedacted: redacted}, nil
}

func handleMigrationStatus(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	// The agent may not point the server at another directory: that would
	// enumerate migration-named files anywhere the operator can read.
	if dir, _ := args["dir"].(string); dir != "" && !sameDir(dir, env.migrationsDir) {
		return nil, fmt.Errorf("migration_status reads only the server's --migrations directory (%s); restart the server with --migrations to inspect another", env.migrationsDir)
	}
	st := inspect.MigrationsStage(ctx, env.client, env.engine, env.migrationsDir, "", "")
	if st.Status == inspect.StageUnavailable {
		return nil, errors.New(st.Reason)
	}
	return &toolOutput{Data: st.Data, Model: "sql"}, nil
}

func sameDir(a, b string) bool {
	aa, err1 := filepath.Abs(a)
	bb, err2 := filepath.Abs(b)
	return err1 == nil && err2 == nil && aa == bb
}

func handleExplainSQL(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	sql, _ := args["sql"].(string)
	if sql == "" {
		return nil, fmt.Errorf("sql argument is required")
	}
	if env.engine.Product != "postgres" {
		return nil, fmt.Errorf("EXPLAIN is offered on PostgreSQL only: this engine's plan format and read-only guarantee are not verified (capability report txn.read_only_rejects_writes)")
	}
	raw, err := inspect.ExplainJSON(ctx, env.client, strings.TrimRight(strings.TrimSpace(sql), ";"))
	if err != nil {
		return nil, err
	}
	return &toolOutput{
		Data:  map[string]any{"statement": sql, "plan": parseJSONText(string(raw)), "executed": false},
		Model: "sql",
		Notes: []string{"planned, never executed (no ANALYZE)"},
	}, nil
}

func handlePlanSchemaChanges(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	raw, ok := args["changes"]
	if !ok {
		return nil, fmt.Errorf("changes argument is required")
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var changes []studio.SchemaChange
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&changes); err != nil {
		return nil, fmt.Errorf("changes: %v", err)
	}
	if len(changes) == 0 {
		return nil, fmt.Errorf("changes must list at least one edit")
	}
	plan, err := studio.PlanSchemaChanges(ctx, env.client, changes)
	if err != nil {
		return nil, err
	}
	return &toolOutput{Data: plan, Model: "sql", Notes: []string{"not applied: apply the reviewed plan in Studio or with the CLI equivalent"}}, nil
}

// modelPrefixes maps Nucleus function-name prefixes to their model.
var modelPrefixes = []struct{ prefix, model string }{
	{"KV_", "kv"}, {"DOC_", "document"}, {"GRAPH_", "graph"}, {"CYPHER", "graph"},
	{"TS_", "timeseries"}, {"FTS_", "fts"}, {"BLOB_", "blob"}, {"STREAM_", "streams"},
	{"COLUMNAR_", "columnar"}, {"DATALOG_", "datalog"}, {"PUBSUB_", "pubsub"},
	{"VECTOR_", "vector"}, {"GEO_", "geo"}, {"CDC_", "cdc"},
}

// modelsTouched names the Nucleus models a statement's mutating functions
// write to.
func modelsTouched(sql string) []string {
	upper := strings.ToUpper(sql)
	var out []string
	seen := map[string]bool{}
	for name := range inspect.NucleusMutatingFns {
		if !strings.Contains(upper, name+"(") {
			continue
		}
		for _, p := range modelPrefixes {
			if strings.HasPrefix(name, p.prefix) && !seen[p.model] {
				seen[p.model] = true
				out = append(out, p.model)
			}
		}
	}
	return out
}

func handleExecuteSQL(ctx context.Context, env *toolEnv, args map[string]any) (*toolOutput, error) {
	if !env.allowWrites {
		return nil, fmt.Errorf("execute_sql requires --allow-writes")
	}
	sql, _ := args["sql"].(string)
	if strings.TrimSpace(sql) == "" {
		return nil, fmt.Errorf("sql argument is required")
	}
	if err := inspect.CheckSingleStatement(sql); err != nil {
		return nil, err
	}
	limit := intArg(args, "limit", 100)
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	tx, err := env.client.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck
	rows, err := tx.Query(ctx, sql, pgx.QueryExecModeExec)
	if err != nil {
		return nil, err
	}
	var returned []any
	truncated := false
	fields := rows.FieldDescriptions()
	for rows.Next() {
		if len(returned) == limit {
			truncated = true
			continue
		}
		vals, err := rows.Values()
		if err != nil {
			rows.Close()
			return nil, err
		}
		row := make(map[string]any, len(fields))
		for i, f := range fields {
			row[f.Name] = inspect.CellValue(vals[i])
		}
		returned = append(returned, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	tag := rows.CommandTag()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	if returned == nil {
		returned = []any{}
	}
	out := &toolOutput{
		Data:   map[string]any{"command": tag.String(), "rowsAffected": tag.RowsAffected(), "rows": returned, "committed": true},
		Model:  "sql",
		Models: modelsTouched(sql),
	}
	if truncated {
		out.Notes = append(out.Notes, fmt.Sprintf("returned rows truncated at %d", limit))
	}
	if env.engine.Product != "postgres" {
		out.Notes = append(out.Notes, "committed on "+env.engine.Product+": see limits — the engine's transaction guarantees differ from PostgreSQL's")
	}
	return out, nil
}

// isSafeIdent reports whether s is a simple SQL identifier (letters, digits,
// underscore, not starting with a digit) — used where a name is interpolated
// into SQL because it cannot be a bind parameter.
func isSafeIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// --- Schema export ---

// openAIToolDefs returns tool definitions in OpenAI function-calling format.
// Compatible with OpenAI SDK, Codex CLI, OpenCode, Ollama tool use, LM Studio, etc.
func openAIToolDefs(allowWrites bool) []map[string]any {
	tools := toolList(allowWrites)
	out := make([]map[string]any, len(tools))
	for i, t := range tools {
		out[i] = map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        t.Name,
				"description": t.Description,
				"parameters":  t.InputSchema,
			},
		}
	}
	return out
}

// DumpSchema returns tool definitions serialized in the requested format.
// format: "mcp" | "openai" | "markdown". allowWrites includes the write
// tools a server started with --allow-writes would offer.
func DumpSchema(format string, allowWrites bool) (string, error) {
	switch format {
	case "mcp":
		b, err := json.MarshalIndent(map[string]any{"tools": toolList(allowWrites)}, "", "  ")
		return string(b), err

	case "openai":
		b, err := json.MarshalIndent(openAIToolDefs(allowWrites), "", "  ")
		return string(b), err

	case "markdown":
		var sb strings.Builder
		sb.WriteString("# Nucleus MCP Tools\n\n")
		sb.WriteString("Paste these into any AI system prompt to enable Nucleus tool use.\n\n")
		for _, t := range toolList(allowWrites) {
			sb.WriteString("## `" + t.Name + "`\n\n")
			sb.WriteString(t.Description + "\n\n")
			if props, ok := t.InputSchema["properties"].(map[string]any); ok && len(props) > 0 {
				sb.WriteString("**Arguments:**\n\n")
				for name, def := range props {
					if d, ok := def.(map[string]any); ok {
						sb.WriteString("- `" + name + "` (" + fmt.Sprint(d["type"]) + ") — " + fmt.Sprint(d["description"]) + "\n")
					}
				}
				sb.WriteString("\n")
			}
		}
		return sb.String(), nil

	default:
		return "", fmt.Errorf("unknown format %q — use: mcp, openai, markdown", format)
	}
}

// --- Schema helpers ---

type props map[string]map[string]any

func schema(p props, required []string) map[string]any {
	properties := make(map[string]any, len(p))
	for k, v := range p {
		properties[k] = v
	}
	s := map[string]any{
		"type":       "object",
		"properties": properties,
	}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func strProp(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}

func numProp(desc string) map[string]any {
	return map[string]any{"type": "number", "description": desc}
}

func boolProp(desc string) map[string]any {
	return map[string]any{"type": "boolean", "description": desc}
}

// intArg extracts an integer argument from args with a default fallback.
func intArg(args map[string]any, key string, def int) int {
	v, ok := args[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return int(i)
		}
	}
	return def
}
