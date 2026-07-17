package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/neutron-build/neutron/cli/internal/db"
)

// toolHandler is a function that executes a single MCP tool.
// Returns (resultText, error). If error is non-nil the result is sent as isError=true.
type toolHandler func(ctx context.Context, client *db.Client, args map[string]any) (string, error)

// toolDef is the MCP tool definition (sent to the AI in tools/list).
type toolDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// toolHandlers maps tool name → handler function.
var toolHandlers = map[string]toolHandler{
	"list_tables":         handleListTables,
	"describe_table":      handleDescribeTable,
	"list_nucleus_models": handleListNucleusModels,
	"query_sql":           handleQuerySQL,
	"kv_get":              handleKVGet,
	"kv_scan":             handleKVScan,
	"fts_search":          handleFTSSearch,
	"vector_search":       handleVectorSearch,
	"cypher_query":        handleCypherQuery,
	"doc_find":            handleDocFind,
	"ts_range":            handleTSRange,
	"geo_distance":        handleGeoDistance,
	"blob_list":           handleBlobList,
	"stream_range":        handleStreamRange,
	"datalog_query":       handleDatalogQuery,
	"cdc_changes":         handleCDCChanges,
	"pubsub_list":         handlePubSubList,
	"search_docs":         handleSearchDocs,
	"get_doc":             handleGetDoc,
}

// toolList returns all tool definitions for the tools/list response.
func toolList() []toolDef {
	return []toolDef{
		{
			Name:        "list_tables",
			Description: "List all SQL tables in the connected Nucleus database, with column counts and row estimates.",
			InputSchema: schema(props{}, nil),
		},
		{
			Name:        "describe_table",
			Description: "Describe the schema of a specific SQL table: column names, data types, nullability, and primary key info.",
			InputSchema: schema(props{
				"table": strProp("Table name to describe"),
			}, []string{"table"}),
		},
		{
			Name:        "list_nucleus_models",
			Description: "List the Nucleus data models and their sizes. Nucleus stores are global and unnamed (one store per model); this reports the counts the engine exposes (KV keys, documents, FTS docs, blobs, graph nodes, CDC events, pub/sub channels). Models without a count function are listed for reference.",
			InputSchema: schema(props{}, nil),
		},
		{
			Name:        "query_sql",
			Description: "Execute a SQL query against the Nucleus database. Use this for SELECT queries, aggregations, JOINs, and any relational data access. Also supports Nucleus SQL extensions.",
			InputSchema: schema(props{
				"sql":   strProp("The SQL query to execute"),
				"limit": numProp("Maximum rows to return (default 100, max 1000)"),
			}, []string{"sql"}),
		},
		{
			Name:        "kv_get",
			Description: "Get the value for a single key from the Nucleus KV store (a single global keyspace).",
			InputSchema: schema(props{
				"key": strProp("Key to retrieve"),
			}, []string{"key"}),
		},
		{
			Name:        "kv_scan",
			Description: "List keys in the Nucleus KV store, optionally filtered by prefix. Returns a JSON array of matching keys.",
			InputSchema: schema(props{
				"prefix": strProp("Key prefix filter (optional, empty = all keys)"),
			}, nil),
		},
		{
			Name:        "fts_search",
			Description: "Full-text search over the Nucleus FTS index using BM25 ranking. Returns a JSON array of {doc_id, score}. Set fuzzy for edit-distance matching.",
			InputSchema: schema(props{
				"query":        strProp("Search query text"),
				"fuzzy":        boolProp("Enable fuzzy (edit-distance) matching (default false)"),
				"max_distance": numProp("Max edit distance when fuzzy (default 2)"),
				"limit":        numProp("Maximum results (default 20)"),
			}, []string{"query"}),
		},
		{
			Name:        "vector_search",
			Description: "Nearest-neighbor search over a vector column of a SQL table, ordered by VECTOR_DISTANCE. Provide the query vector as a JSON array of floats, e.g. [0.1, 0.2, 0.3].",
			InputSchema: schema(props{
				"table":     strProp("Table containing the vector column"),
				"column":    strProp("Vector column name"),
				"vector":    strProp("Query vector as JSON array, e.g. [0.1, 0.2, ...]"),
				"id_column": strProp("Identifier column to return (default 'id')"),
				"k":         numProp("Number of nearest neighbors to return (default 10)"),
				"metric":    strProp("Distance metric: cosine, l2, or inner (default cosine)"),
			}, []string{"table", "column", "vector"}),
		},
		{
			Name:        "cypher_query",
			Description: "Run a Cypher query against the Nucleus graph store (a single global graph). Returns a JSON object with columns and rows.",
			InputSchema: schema(props{
				"query": strProp("Cypher query, e.g. MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 25"),
			}, []string{"query"}),
		},
		{
			Name:        "doc_find",
			Description: "Query the Nucleus document store with a JSON filter expression. Returns the matching documents.",
			InputSchema: schema(props{
				"filter": strProp("JSON filter expression, e.g. {\"status\": \"active\"} (empty = all docs)"),
				"limit":  numProp("Maximum documents to return (default 20)"),
			}, nil),
		},
		{
			Name:        "ts_range",
			Description: "Aggregate a Nucleus time series over an epoch-millisecond window. The engine exposes range average and count only (no raw point fetch).",
			InputSchema: schema(props{
				"series":   strProp("Time series name"),
				"start_ms": numProp("Range start, epoch milliseconds (default 0)"),
				"end_ms":   numProp("Range end, epoch milliseconds (required)"),
				"agg":      strProp("Aggregation: avg or count (default avg)"),
			}, []string{"series", "end_ms"}),
		},
		{
			Name:        "geo_distance",
			Description: "Compute the haversine distance in meters between two latitude/longitude points using the Nucleus GEO_DISTANCE function.",
			InputSchema: schema(props{
				"lat1": numProp("First point latitude"),
				"lon1": numProp("First point longitude"),
				"lat2": numProp("Second point latitude"),
				"lon2": numProp("Second point longitude"),
			}, []string{"lat1", "lon1", "lat2", "lon2"}),
		},
		{
			Name:        "blob_list",
			Description: "List keys in the Nucleus blob store, optionally filtered by prefix. Returns a JSON array of key strings.",
			InputSchema: schema(props{
				"prefix": strProp("Key prefix filter (optional, empty = all blobs)"),
			}, nil),
		},
		{
			Name:        "stream_range",
			Description: "Read entries from a Nucleus stream (append-only log) over an epoch-millisecond window. Returns a JSON array of entries.",
			InputSchema: schema(props{
				"stream":   strProp("Stream name"),
				"start_ms": numProp("Range start, epoch milliseconds (default 0)"),
				"end_ms":   numProp("Range end, epoch milliseconds (required)"),
				"limit":    numProp("Maximum entries to return (default 50)"),
			}, []string{"stream", "end_ms"}),
		},
		{
			Name:        "datalog_query",
			Description: "Evaluate a query against the Nucleus datalog engine. Returns a JSON array of result tuples. Use query_sql with DATALOG_ASSERT/DATALOG_RULE to load facts and rules first.",
			InputSchema: schema(props{
				"query": strProp("Datalog query, e.g. ancestor(alice, ?X)"),
			}, []string{"query"}),
		},
		{
			Name:        "cdc_changes",
			Description: "Read change data capture (CDC) events from the Nucleus log after a given sequence. Returns a JSON array of {seq, table, change, ts}.",
			InputSchema: schema(props{
				"after_sequence": numProp("Return events after this sequence number (default 0)"),
				"limit":          numProp("Maximum events to return (default 50)"),
			}, nil),
		},
		{
			Name:        "pubsub_list",
			Description: "List active pub/sub channels in the Nucleus database (comma-separated).",
			InputSchema: schema(props{}, nil),
		},
		{
			Name:        "search_docs",
			Description: "Search the Neutron framework documentation by keyword. Returns matching pages with titles, URLs, slugs, and snippets. Use this to answer 'how do I ...' questions about Neutron (routing, loaders, actions, Nucleus, deployment, the SDKs).",
			InputSchema: schema(props{
				"query": strProp("Search terms, e.g. \"loader data\" or \"vector search\"."),
				"limit": numProp("Maximum results to return (default 8)."),
			}, []string{"query"}),
		},
		{
			Name:        "get_doc",
			Description: "Fetch the full markdown of a single Neutron documentation page by slug (e.g. \"routing/app-routes\", \"nucleus/overview\"). Use after search_docs to read a page in full.",
			InputSchema: schema(props{
				"slug": strProp("Doc slug or path, e.g. \"data/loaders\" or \"/docs/data/loaders\"."),
			}, []string{"slug"}),
		},
	}
}

// AllowWrites controls whether the MCP query_sql tool permits write operations.
// When false (default), only SELECT, EXPLAIN, SHOW, and WITH queries are allowed.
var AllowWrites bool

// isReadOnlySQL checks if a SQL statement is a read-only query.
func isReadOnlySQL(sql string) bool {
	trimmed := strings.TrimSpace(strings.ToUpper(sql))
	return strings.HasPrefix(trimmed, "SELECT") ||
		strings.HasPrefix(trimmed, "EXPLAIN") ||
		strings.HasPrefix(trimmed, "SHOW") ||
		strings.HasPrefix(trimmed, "WITH")
}

// --- Handlers ---

func handleListTables(ctx context.Context, client *db.Client, _ map[string]any) (string, error) {
	rows, err := client.Query(ctx, `
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
		return "", fmt.Errorf("list tables: %w", err)
	}
	return rowsToJSON(rows)
}

func handleDescribeTable(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	table, _ := args["table"].(string)
	if table == "" {
		return "", fmt.Errorf("table argument is required")
	}

	// Nucleus does not implement information_schema.table_constraints /
	// key_column_usage / character_maximum_length; use information_schema.columns
	// and resolve primary keys best-effort from the pg_catalog virtual tables.
	rows, err := client.Query(ctx, `
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
		return "", fmt.Errorf("describe table: %w", err)
	}
	defer rows.Close()

	pk := mcpPKColumns(ctx, client, table)

	type colInfo struct {
		Name         string  `json:"column_name"`
		DataType     string  `json:"data_type"`
		UDTName      string  `json:"udt_name"`
		IsNullable   string  `json:"is_nullable"`
		Default      *string `json:"column_default"`
		IsPrimaryKey bool    `json:"is_primary_key"`
	}
	var cols []colInfo
	for rows.Next() {
		var c colInfo
		if err := rows.Scan(&c.Name, &c.DataType, &c.UDTName, &c.IsNullable, &c.Default); err != nil {
			continue
		}
		c.IsPrimaryKey = pk[c.Name]
		cols = append(cols, c)
	}
	if cols == nil {
		cols = []colInfo{}
	}
	b, err := json.MarshalIndent(cols, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// mcpPKColumns resolves primary-key column names via pg_class -> pg_index ->
// pg_attribute (virtual tables both Nucleus and Postgres implement).
// Best-effort: empty on any error.
func mcpPKColumns(ctx context.Context, client *db.Client, table string) map[string]bool {
	pk := map[string]bool{}

	var oid int32
	if err := client.QueryRow(ctx, `SELECT oid FROM pg_catalog.pg_class WHERE relname = $1`, table).Scan(&oid); err != nil {
		return pk
	}
	var indkey string
	if err := client.QueryRow(ctx, `SELECT indkey FROM pg_catalog.pg_index WHERE indisprimary AND indrelid = $1`, oid).Scan(&indkey); err != nil {
		return pk
	}
	positions := map[int]bool{}
	for _, f := range strings.Fields(indkey) {
		if p, err := strconv.Atoi(f); err == nil {
			positions[p] = true
		}
	}
	if len(positions) == 0 {
		return pk
	}
	rows, err := client.Query(ctx, `SELECT attname, attnum FROM pg_catalog.pg_attribute WHERE attrelid = $1`, oid)
	if err != nil {
		return pk
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var num int
		if rows.Scan(&name, &num) == nil && positions[num] {
			pk[name] = true
		}
	}
	return pk
}

func handleListNucleusModels(ctx context.Context, client *db.Client, _ map[string]any) (string, error) {
	// Nucleus stores are global and unnamed (one store per model). There is no
	// enumeration function; report each model with the counts the engine exposes.
	type modelResult struct {
		Model string `json:"model"`
		Count *int64 `json:"count,omitempty"`
		Note  string `json:"note,omitempty"`
	}

	scalarCount := func(sql string) *int64 {
		var n int64
		if err := client.QueryRow(ctx, sql).Scan(&n); err != nil {
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

	// pubsub channels are enumerable via a comma-separated scalar.
	var channels string
	if err := client.QueryRow(ctx, `SELECT PUBSUB_CHANNELS()`).Scan(&channels); err == nil {
		n := int64(0)
		for _, c := range strings.Split(channels, ",") {
			if strings.TrimSpace(c) != "" {
				n++
			}
		}
		results = append(results, modelResult{Model: "pubsub", Count: &n})
	}

	// Models with no enumeration/count surface in the engine.
	for _, m := range []string{"vector", "timeseries", "geo", "columnar", "datalog", "streams"} {
		results = append(results, modelResult{Model: m, Note: "no engine enumeration; access by name via the model tools"})
	}

	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func handleQuerySQL(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	sql, _ := args["sql"].(string)
	if sql == "" {
		return "", fmt.Errorf("sql argument is required")
	}

	// Reject write operations unless --allow-writes is set
	if !AllowWrites && !isReadOnlySQL(sql) {
		return "", fmt.Errorf("write operations are not allowed — only SELECT, EXPLAIN, SHOW, and WITH queries are permitted. Use --allow-writes to enable mutations")
	}

	limit := intArg(args, "limit", 100)
	if limit > 1000 {
		limit = 1000
	}

	// Wrap in a limit subquery only for SELECT statements
	trimmed := strings.TrimSpace(strings.ToUpper(sql))
	if strings.HasPrefix(trimmed, "SELECT") && !strings.Contains(trimmed, "LIMIT") {
		sql = fmt.Sprintf("SELECT * FROM (%s) __q LIMIT %d", sql, limit)
	}

	rows, err := client.Query(ctx, sql)
	if err != nil {
		return "", fmt.Errorf("query: %w", err)
	}
	return rowsToJSON(rows)
}

// scalarText runs a query returning a single text/JSON scalar and returns it.
// A NULL result is rendered as "null".
func scalarText(ctx context.Context, client *db.Client, sql string, args ...any) (string, error) {
	var v *string
	if err := client.QueryRow(ctx, sql, args...).Scan(&v); err != nil {
		return "", err
	}
	if v == nil {
		return "null", nil
	}
	return *v, nil
}

func handleKVGet(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	key, _ := args["key"].(string)
	if key == "" {
		return "", fmt.Errorf("key argument is required")
	}
	// Nucleus KV is a single global keyspace: KV_GET(key).
	return scalarText(ctx, client, "SELECT KV_GET($1)", key)
}

func handleKVScan(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	prefix, _ := args["prefix"].(string)
	// KV_KEYS(pattern) returns a JSON array of matching keys. A trailing '*' makes
	// the prefix a glob; an empty prefix lists all keys.
	pattern := prefix + "*"
	return scalarText(ctx, client, "SELECT KV_KEYS($1)", pattern)
}

func handleFTSSearch(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return "", fmt.Errorf("query argument is required")
	}
	fuzzy, _ := args["fuzzy"].(bool)
	limit := intArg(args, "limit", 20)

	// FTS is a single global index; results are a JSON array of {doc_id, score}.
	if fuzzy {
		maxDist := intArg(args, "max_distance", 2)
		return scalarText(ctx, client, "SELECT FTS_FUZZY_SEARCH($1, $2, $3)", query, maxDist, limit)
	}
	return scalarText(ctx, client, "SELECT FTS_SEARCH($1, $2)", query, limit)
}

func handleVectorSearch(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	table, _ := args["table"].(string)
	column, _ := args["column"].(string)
	vector, _ := args["vector"].(string)
	if table == "" || column == "" || vector == "" {
		return "", fmt.Errorf("table, column, and vector arguments are required")
	}
	if !isSafeIdent(table) || !isSafeIdent(column) {
		return "", fmt.Errorf("table and column must be simple identifiers")
	}
	idCol, _ := args["id_column"].(string)
	if idCol == "" {
		idCol = "id"
	}
	if !isSafeIdent(idCol) {
		return "", fmt.Errorf("id_column must be a simple identifier")
	}
	k := intArg(args, "k", 10)
	metric, _ := args["metric"].(string)
	switch metric {
	case "", "cosine":
		metric = "cosine"
	case "l2", "inner":
		// accepted
	default:
		return "", fmt.Errorf("metric must be one of: cosine, l2, inner")
	}

	// Nucleus has no vector_search table function; the real pattern is an ORDER BY
	// on VECTOR_DISTANCE with a VECTOR('[...]') literal wrapping the query vector.
	sql := fmt.Sprintf(
		"SELECT %s AS id, VECTOR_DISTANCE(%s, VECTOR($1), $2) AS distance FROM %s ORDER BY distance LIMIT $3",
		idCol, column, table)
	rows, err := client.Query(ctx, sql, vector, metric, k)
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleCypherQuery(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return "", fmt.Errorf("query argument is required")
	}
	// Single global graph store: GRAPH_QUERY(cypher) -> JSON {columns, rows}.
	return scalarText(ctx, client, "SELECT GRAPH_QUERY($1)", query)
}

func handleDocFind(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	filter, _ := args["filter"].(string)
	if filter == "" {
		filter = "{}"
	}
	limit := intArg(args, "limit", 20)

	// DOC_QUERY returns a comma-separated list of matching document IDs; fetch
	// each with DOC_GET (bounded by limit).
	var ids string
	if err := client.QueryRow(ctx, "SELECT DOC_QUERY($1)", filter).Scan(&ids); err != nil {
		return "", err
	}
	type doc struct {
		ID   string          `json:"id"`
		Data json.RawMessage `json:"data"`
	}
	var docs []doc
	for _, id := range strings.Split(ids, ",") {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if len(docs) >= limit {
			break
		}
		var data *string
		if err := client.QueryRow(ctx, "SELECT DOC_GET($1)", id).Scan(&data); err != nil {
			continue
		}
		d := doc{ID: id}
		if data != nil {
			d.Data = json.RawMessage(*data)
		}
		docs = append(docs, d)
	}
	if docs == nil {
		docs = []doc{}
	}
	b, err := json.MarshalIndent(docs, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func handleTSRange(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	series, _ := args["series"].(string)
	if series == "" {
		return "", fmt.Errorf("series argument is required")
	}
	startMS := intArg(args, "start_ms", 0)
	endMS := intArg(args, "end_ms", 0)
	if endMS == 0 {
		return "", fmt.Errorf("end_ms argument is required (epoch milliseconds)")
	}
	agg, _ := args["agg"].(string)
	if agg == "" {
		agg = "avg"
	}

	// The engine exposes range aggregates only (no raw point fetch): TS_RANGE_AVG
	// and TS_RANGE_COUNT over an epoch-millisecond window.
	switch agg {
	case "avg":
		return scalarText(ctx, client, "SELECT TS_RANGE_AVG($1, $2, $3)::text", series, startMS, endMS)
	case "count":
		return scalarText(ctx, client, "SELECT TS_RANGE_COUNT($1, $2, $3)::text", series, startMS, endMS)
	default:
		return "", fmt.Errorf("agg must be one of: avg, count (the engine exposes no other range aggregate)")
	}
}

func handleGeoDistance(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	lat1, ok1 := args["lat1"].(float64)
	lon1, ok2 := args["lon1"].(float64)
	lat2, ok3 := args["lat2"].(float64)
	lon2, ok4 := args["lon2"].(float64)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return "", fmt.Errorf("lat1, lon1, lat2, and lon2 arguments are required")
	}
	// GEO_DISTANCE returns the haversine distance in meters between two points.
	return scalarText(ctx, client, "SELECT GEO_DISTANCE($1, $2, $3, $4)::text", lat1, lon1, lat2, lon2)
}

func handleBlobList(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	prefix, _ := args["prefix"].(string)
	// BLOB_LIST([prefix]) returns a JSON array of key strings.
	return scalarText(ctx, client, "SELECT BLOB_LIST($1)", prefix)
}

func handleStreamRange(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	stream, _ := args["stream"].(string)
	if stream == "" {
		return "", fmt.Errorf("stream argument is required")
	}
	startMS := intArg(args, "start_ms", 0)
	endMS := intArg(args, "end_ms", 0)
	if endMS == 0 {
		return "", fmt.Errorf("end_ms argument is required (epoch milliseconds)")
	}
	count := intArg(args, "limit", 50)
	// STREAM_XRANGE(stream, start_ms, end_ms, count) -> JSON array of entries.
	return scalarText(ctx, client, "SELECT STREAM_XRANGE($1, $2, $3, $4)", stream, startMS, endMS, count)
}

func handleDatalogQuery(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return "", fmt.Errorf("query argument is required")
	}
	// DATALOG_QUERY(text) returns a JSON array of result tuples.
	return scalarText(ctx, client, "SELECT DATALOG_QUERY($1)", query)
}

func handleCDCChanges(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	afterSeq := intArg(args, "after_sequence", 0)
	limit := intArg(args, "limit", 50)
	// CDC_READ(after_sequence, limit) -> JSON array of {seq, table, change, ts}.
	return scalarText(ctx, client, "SELECT CDC_READ($1, $2)", afterSeq, limit)
}

func handlePubSubList(ctx context.Context, client *db.Client, _ map[string]any) (string, error) {
	// PUBSUB_CHANNELS() returns a comma-separated list of active channels.
	return scalarText(ctx, client, "SELECT PUBSUB_CHANNELS()")
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
func openAIToolDefs() []map[string]any {
	tools := toolList()
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
// format: "mcp" | "openai" | "markdown"
func DumpSchema(format string) (string, error) {
	switch format {
	case "mcp":
		b, err := json.MarshalIndent(map[string]any{"tools": toolList()}, "", "  ")
		return string(b), err

	case "openai":
		b, err := json.MarshalIndent(openAIToolDefs(), "", "  ")
		return string(b), err

	case "markdown":
		var sb strings.Builder
		sb.WriteString("# Nucleus MCP Tools\n\n")
		sb.WriteString("Paste these into any AI system prompt to enable Nucleus tool use.\n\n")
		for _, t := range toolList() {
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

// --- Result formatting ---

// rowsToJSON converts pgx.Rows to a compact JSON array of objects.
func rowsToJSON(rows pgx.Rows) (string, error) {
	defer rows.Close()

	cols := rows.FieldDescriptions()
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = string(c.Name)
	}

	var result []map[string]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return "", err
		}
		row := make(map[string]any, len(colNames))
		for i, name := range colNames {
			row[name] = vals[i]
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	if result == nil {
		result = []map[string]any{}
	}

	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
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
	}
	return def
}
