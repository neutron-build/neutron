package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/jackc/pgx/v5"
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
	"geo_radius":          handleGeoRadius,
	"blob_list":           handleBlobList,
	"stream_range":        handleStreamRange,
	"datalog_eval":        handleDatalogEval,
	"cdc_changes":         handleCDCChanges,
	"pubsub_list":         handlePubSubList,
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
			Description: "List all non-SQL Nucleus collections: KV stores, vector indexes, FTS indexes, document stores, graph stores, time series streams, blob stores, and pub/sub channels.",
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
			Description: "Get the value for a single key from a Nucleus KV store.",
			InputSchema: schema(props{
				"store": strProp("KV store name"),
				"key":   strProp("Key to retrieve"),
			}, []string{"store", "key"}),
		},
		{
			Name:        "kv_scan",
			Description: "Scan keys in a Nucleus KV store, optionally filtered by prefix. Returns key, value, and TTL.",
			InputSchema: schema(props{
				"store":  strProp("KV store name"),
				"prefix": strProp("Key prefix filter (optional, empty = all keys)"),
				"limit":  numProp("Maximum keys to return (default 50)"),
			}, []string{"store"}),
		},
		{
			Name:        "fts_search",
			Description: "Full-text search across a Nucleus FTS index using BM25 ranking. Supports fuzzy matching.",
			InputSchema: schema(props{
				"index": strProp("FTS index name"),
				"query": strProp("Search query text"),
				"fuzzy": boolProp("Enable fuzzy matching (default false)"),
				"limit": numProp("Maximum results (default 20)"),
			}, []string{"index", "query"}),
		},
		{
			Name:        "vector_search",
			Description: "Semantic similarity search in a Nucleus vector index. Provide the query vector as a JSON array of floats, e.g. [0.1, 0.2, 0.3].",
			InputSchema: schema(props{
				"index":  strProp("Vector index name"),
				"vector": strProp("Query vector as JSON array, e.g. [0.1, 0.2, ...]"),
				"k":      numProp("Number of nearest neighbors to return (default 10)"),
				"metric": strProp("Distance metric: cosine, l2, or dot (default cosine)"),
			}, []string{"index", "vector"}),
		},
		{
			Name:        "cypher_query",
			Description: "Run a Cypher graph query against a Nucleus graph store. Returns nodes and relationships.",
			InputSchema: schema(props{
				"graph": strProp("Graph store name"),
				"query": strProp("Cypher query, e.g. MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 25"),
			}, []string{"graph", "query"}),
		},
		{
			Name:        "doc_find",
			Description: "Query a Nucleus document store using a JSON filter expression.",
			InputSchema: schema(props{
				"collection": strProp("Document collection name"),
				"filter":     strProp("JSON filter expression, e.g. {\"status\": \"active\"} (empty = all docs)"),
				"limit":      numProp("Maximum documents to return (default 20)"),
			}, []string{"collection"}),
		},
		{
			Name:        "ts_range",
			Description: "Query a Nucleus time series stream over a time range with optional bucketing and aggregation.",
			InputSchema: schema(props{
				"stream": strProp("Time series stream name"),
				"start":  strProp("Start time in ISO 8601 or relative format, e.g. '2024-01-01' or 'now-1h'"),
				"end":    strProp("End time (default: now)"),
				"bucket": strProp("Bucket interval e.g. '1m', '1h', '1d' (optional, raw points if omitted)"),
				"agg":    strProp("Aggregation function: avg, sum, min, max, count (default avg)"),
				"limit":  numProp("Maximum points (default 500)"),
			}, []string{"stream", "start"}),
		},
		{
			Name:        "geo_radius",
			Description: "Find points within a radius of a given latitude/longitude in a Nucleus geo store.",
			InputSchema: schema(props{
				"store":  strProp("Geo store name"),
				"lat":    numProp("Center latitude"),
				"lon":    numProp("Center longitude"),
				"radius": numProp("Search radius in meters"),
				"limit":  numProp("Maximum results (default 20)"),
			}, []string{"store", "lat", "lon", "radius"}),
		},
		{
			Name:        "blob_list",
			Description: "List blobs in a Nucleus blob store with their size, content type, and content hash.",
			InputSchema: schema(props{
				"store": strProp("Blob store name"),
				"limit": numProp("Maximum blobs to return (default 50)"),
			}, []string{"store"}),
		},
		{
			Name:        "stream_range",
			Description: "Read entries from a Nucleus stream (append-only log) between two entry IDs.",
			InputSchema: schema(props{
				"stream": strProp("Stream name"),
				"from":   strProp("Start entry ID (default '0-0' for beginning)"),
				"to":     strProp("End entry ID (default '+' for latest)"),
				"limit":  numProp("Maximum entries to return (default 50)"),
			}, []string{"stream"}),
		},
		{
			Name:        "datalog_eval",
			Description: "Evaluate a Datalog program against the Nucleus datalog engine. Write facts and rules, then query with ?-.",
			InputSchema: schema(props{
				"program": strProp("Datalog program text with facts, rules, and a ?- query"),
			}, []string{"program"}),
		},
		{
			Name:        "cdc_changes",
			Description: "Retrieve recent change data capture (CDC) events from the Nucleus WAL. Filter by table and operation type.",
			InputSchema: schema(props{
				"table":     strProp("Table name filter (optional, empty = all tables)"),
				"operation": strProp("Operation filter: INSERT, UPDATE, DELETE (optional, empty = all)"),
				"limit":     numProp("Maximum events to return (default 50)"),
			}, nil),
		},
		{
			Name:        "pubsub_list",
			Description: "List all active pub/sub channels in the Nucleus database.",
			InputSchema: schema(props{}, nil),
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

	rows, err := client.Query(ctx, `
		SELECT
			c.column_name,
			c.data_type,
			c.character_maximum_length,
			c.is_nullable,
			c.column_default,
			CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key
		FROM information_schema.columns c
		LEFT JOIN (
			SELECT ku.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage ku
				ON tc.constraint_name = ku.constraint_name
			WHERE tc.constraint_type = 'PRIMARY KEY'
			  AND ku.table_name = $1
		) pk ON pk.column_name = c.column_name
		WHERE c.table_name = $1
		  AND c.table_schema NOT IN ('pg_catalog','information_schema')
		ORDER BY c.ordinal_position
	`, table)
	if err != nil {
		return "", fmt.Errorf("describe table: %w", err)
	}
	return rowsToJSON(rows)
}

func handleListNucleusModels(ctx context.Context, client *db.Client, _ map[string]any) (string, error) {
	type modelResult struct {
		Model string `json:"model"`
		Name  string `json:"name"`
	}
	var results []modelResult

	queries := []struct {
		model string
		sql   string
		col   string
	}{
		{"kv", "SELECT store_name FROM kv_stores()", "store_name"},
		{"vector", "SELECT index_name FROM vector_indexes()", "index_name"},
		{"fts", "SELECT index_name FROM fts_indexes()", "index_name"},
		{"document", "SELECT collection_name FROM doc_collections()", "collection_name"},
		{"graph", "SELECT graph_name FROM graph_stores()", "graph_name"},
		{"timeseries", "SELECT stream_name FROM ts_streams()", "stream_name"},
		{"blob", "SELECT store_name FROM blob_stores()", "store_name"},
		{"geo", "SELECT store_name FROM geo_stores()", "store_name"},
		{"streams", "SELECT stream_name FROM stream_list()", "stream_name"},
	}

	for _, q := range queries {
		rows, err := client.Query(ctx, q.sql)
		if err != nil {
			continue // model may not exist; silent skip
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err == nil {
				results = append(results, modelResult{Model: q.model, Name: name})
			}
		}
		rows.Close()
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

func handleKVGet(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	store, _ := args["store"].(string)
	key, _ := args["key"].(string)
	if store == "" || key == "" {
		return "", fmt.Errorf("store and key arguments are required")
	}
	rows, err := client.Query(ctx, "SELECT kv_get($1, $2) AS value", store, key)
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleKVScan(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	store, _ := args["store"].(string)
	if store == "" {
		return "", fmt.Errorf("store argument is required")
	}
	prefix, _ := args["prefix"].(string)
	limit := intArg(args, "limit", 50)

	rows, err := client.Query(ctx, "SELECT key, value, ttl FROM kv_scan($1, $2) LIMIT $3", store, prefix, limit)
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleFTSSearch(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	index, _ := args["index"].(string)
	query, _ := args["query"].(string)
	if index == "" || query == "" {
		return "", fmt.Errorf("index and query arguments are required")
	}
	fuzzy, _ := args["fuzzy"].(bool)
	limit := intArg(args, "limit", 20)

	var rows pgx.Rows
	var err error
	if fuzzy {
		rows, err = client.Query(ctx, "SELECT id, rank, snippet FROM fts_search($1, $2, true) LIMIT $3", index, query, limit)
	} else {
		rows, err = client.Query(ctx, "SELECT id, rank, snippet FROM fts_search($1, $2) LIMIT $3", index, query, limit)
	}
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleVectorSearch(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	index, _ := args["index"].(string)
	vector, _ := args["vector"].(string)
	if index == "" || vector == "" {
		return "", fmt.Errorf("index and vector arguments are required")
	}
	k := intArg(args, "k", 10)
	metric, _ := args["metric"].(string)
	if metric == "" {
		metric = "cosine"
	}

	rows, err := client.Query(ctx,
		"SELECT id, distance FROM vector_search($1, VECTOR($2), $3, $4) LIMIT $3",
		index, vector, k, metric)
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleCypherQuery(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	graph, _ := args["graph"].(string)
	query, _ := args["query"].(string)
	if graph == "" || query == "" {
		return "", fmt.Errorf("graph and query arguments are required")
	}
	rows, err := client.Query(ctx, "SELECT * FROM cypher_query($1, $2)", graph, query)
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleDocFind(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	collection, _ := args["collection"].(string)
	if collection == "" {
		return "", fmt.Errorf("collection argument is required")
	}
	filter, _ := args["filter"].(string)
	if filter == "" {
		filter = "{}"
	}
	limit := intArg(args, "limit", 20)

	rows, err := client.Query(ctx, "SELECT id, data FROM doc_find($1, $2::jsonb) LIMIT $3", collection, filter, limit)
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleTSRange(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	stream, _ := args["stream"].(string)
	start, _ := args["start"].(string)
	if stream == "" || start == "" {
		return "", fmt.Errorf("stream and start arguments are required")
	}
	end, _ := args["end"].(string)
	if end == "" {
		end = "now"
	}
	bucket, _ := args["bucket"].(string)
	agg, _ := args["agg"].(string)
	if agg == "" {
		agg = "avg"
	}
	limit := intArg(args, "limit", 500)

	var rows pgx.Rows
	var err error
	if bucket != "" {
		rows, err = client.Query(ctx,
			"SELECT time_bucket, "+agg+"_value FROM ts_range($1, $2::timestamptz, $3::timestamptz, $4) LIMIT $5",
			stream, start, end, bucket, limit)
	} else {
		rows, err = client.Query(ctx,
			"SELECT ts, value FROM ts_range($1, $2::timestamptz, $3::timestamptz) LIMIT $4",
			stream, start, end, limit)
	}
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleGeoRadius(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	store, _ := args["store"].(string)
	lat, latOK := args["lat"].(float64)
	lon, lonOK := args["lon"].(float64)
	radius, radOK := args["radius"].(float64)
	if store == "" || !latOK || !lonOK || !radOK {
		return "", fmt.Errorf("store, lat, lon, and radius arguments are required")
	}
	limit := intArg(args, "limit", 20)

	rows, err := client.Query(ctx,
		"SELECT id, lat, lon, distance FROM geo_radius($1, $2, $3, $4) LIMIT $5",
		store, lat, lon, radius, limit)
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleBlobList(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	store, _ := args["store"].(string)
	if store == "" {
		return "", fmt.Errorf("store argument is required")
	}
	limit := intArg(args, "limit", 50)

	rows, err := client.Query(ctx,
		"SELECT id, size_bytes, content_type, hash, created_at FROM blob_list($1) LIMIT $2",
		store, limit)
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleStreamRange(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	stream, _ := args["stream"].(string)
	if stream == "" {
		return "", fmt.Errorf("stream argument is required")
	}
	from, _ := args["from"].(string)
	if from == "" {
		from = "0-0"
	}
	to, _ := args["to"].(string)
	if to == "" {
		to = "+"
	}
	limit := intArg(args, "limit", 50)

	rows, err := client.Query(ctx,
		"SELECT id, data FROM stream_range($1, $2, $3) LIMIT $4",
		stream, from, to, limit)
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleDatalogEval(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	program, _ := args["program"].(string)
	if program == "" {
		return "", fmt.Errorf("program argument is required")
	}
	rows, err := client.Query(ctx, "SELECT * FROM datalog_eval($1)", program)
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handleCDCChanges(ctx context.Context, client *db.Client, args map[string]any) (string, error) {
	table, _ := args["table"].(string)
	operation, _ := args["operation"].(string)
	limit := intArg(args, "limit", 50)

	rows, err := client.Query(ctx,
		"SELECT table_name, operation, changed_at, old_data, new_data FROM cdc_changes($1, $2) LIMIT $3",
		table, operation, limit)
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
}

func handlePubSubList(ctx context.Context, client *db.Client, _ map[string]any) (string, error) {
	rows, err := client.Query(ctx, "SELECT channel, subscriber_count FROM pubsub_channels()")
	if err != nil {
		return "", err
	}
	return rowsToJSON(rows)
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
