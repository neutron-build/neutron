package studio

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// Schema represents the full schema view returned to the Studio frontend.
type Schema struct {
	SQL        []SQLTable        `json:"sql"`
	KV         []KVStore         `json:"kv"`
	Vector     []VectorIndex     `json:"vector"`
	TimeSeries []TSMetric        `json:"timeseries"`
	Document   []DocCollection   `json:"document"`
	Graph      []GraphStore      `json:"graph"`
	FTS        []FTSIndex        `json:"fts"`
	Geo        []GeoLayer        `json:"geo"`
	Blob       []BlobStore       `json:"blob"`
	PubSub     []PubSubChannel   `json:"pubsub"`
	Streams    []Stream          `json:"streams"`
	Columnar   []ColumnarTable   `json:"columnar"`
	Datalog    *DatalogStore     `json:"datalog"`
	CDC        bool              `json:"cdc"`
}

type SQLTable struct {
	Schema   string      `json:"schema"`
	Name     string      `json:"name"`
	Columns  []SQLColumn `json:"columns"`
	RowCount *int64      `json:"rowCount,omitempty"`
}

type SQLColumn struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Nullable     bool   `json:"nullable"`
	Default      string `json:"default,omitempty"`
	IsPrimaryKey bool   `json:"isPrimaryKey"`
}

type KVStore        struct { Name string `json:"name"`; KeyCount int64 `json:"keyCount"` }
type VectorIndex    struct { Name string `json:"name"`; Dimensions int `json:"dimensions"`; Metric string `json:"metric"`; Count int64 `json:"count"` }
type TSMetric       struct { Name string `json:"name"`; Count int64 `json:"count"` }
type DocCollection  struct { Name string `json:"name"`; Count int64 `json:"count"` }
type GraphStore     struct { Name string `json:"name"`; NodeCount int64 `json:"nodeCount"`; EdgeCount int64 `json:"edgeCount"` }
type FTSIndex       struct { Name string `json:"name"`; DocCount int64 `json:"docCount"` }
type GeoLayer       struct { Name string `json:"name"`; PointCount int64 `json:"pointCount"` }
type BlobStore      struct { Name string `json:"name"`; BlobCount int64 `json:"blobCount"` }
type PubSubChannel  struct { Name string `json:"name"` }
type Stream         struct { Name string `json:"name"`; Length int64 `json:"length"` }
type ColumnarTable  struct { Name string `json:"name"`; RowCount int64 `json:"rowCount"` }
type DatalogStore   struct { PredicateCount int `json:"predicateCount"`; RuleCount int `json:"ruleCount"` }

// FetchSchema loads schema information from the database.
// For plain PostgreSQL only SQL tables are populated.
// For Nucleus all 14 models are queried via SQL functions.
func FetchSchema(ctx context.Context, client *db.Client, isNucleus bool) (*Schema, error) {
	sc := &Schema{
		SQL:        []SQLTable{},
		KV:         []KVStore{},
		Vector:     []VectorIndex{},
		TimeSeries: []TSMetric{},
		Document:   []DocCollection{},
		Graph:      []GraphStore{},
		FTS:        []FTSIndex{},
		Geo:        []GeoLayer{},
		Blob:       []BlobStore{},
		PubSub:     []PubSubChannel{},
		Streams:    []Stream{},
		Columnar:   []ColumnarTable{},
	}

	if err := fetchSQLTables(ctx, client, sc, isNucleus); err != nil {
		return nil, fmt.Errorf("sql schema: %w", err)
	}

	if isNucleus {
		fetchNucleusModels(ctx, client, sc) // best-effort
	}

	return sc, nil
}

func fetchSQLTables(ctx context.Context, client *db.Client, sc *Schema, isNucleus bool) error {
	// Get tables grouped by schema (exclude system schemas)
	rows, err := client.Query(ctx, `
		SELECT t.table_schema, t.table_name
		FROM information_schema.tables t
		WHERE t.table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
		  AND t.table_type = 'BASE TABLE'
		ORDER BY t.table_schema, t.table_name
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tables := map[string]*SQLTable{}
	var order []string
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			continue
		}
		key := schema + "." + name
		tables[key] = &SQLTable{Schema: schema, Name: name, Columns: []SQLColumn{}}
		order = append(order, key)
	}

	if isNucleus {
		if err := fetchColumnsNucleus(ctx, client, tables); err != nil {
			return err
		}
	} else {
		if err := fetchColumns(ctx, client, tables); err != nil {
			return err
		}
	}

	for _, k := range order {
		sc.SQL = append(sc.SQL, *tables[k])
	}
	return nil
}

func fetchColumns(ctx context.Context, client *db.Client, tables map[string]*SQLTable) error {
	rows, err := client.Query(ctx, `
		SELECT
			c.table_schema,
			c.table_name,
			c.column_name,
			c.udt_name,
			c.is_nullable = 'YES',
			c.column_default,
			EXISTS (
				SELECT 1 FROM information_schema.table_constraints tc
				JOIN information_schema.key_column_usage kcu
				  ON kcu.constraint_name = tc.constraint_name
				  AND kcu.table_schema = tc.table_schema
				WHERE tc.constraint_type = 'PRIMARY KEY'
				  AND tc.table_schema = c.table_schema
				  AND tc.table_name = c.table_name
				  AND kcu.column_name = c.column_name
			) AS is_pk
		FROM information_schema.columns c
		WHERE c.table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
		ORDER BY c.table_schema, c.table_name, c.ordinal_position
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tschema, tname, colName, colType string
		var nullable, isPK bool
		var def *string
		if err := rows.Scan(&tschema, &tname, &colName, &colType, &nullable, &def, &isPK); err != nil {
			continue
		}
		key := tschema + "." + tname
		t, ok := tables[key]
		if !ok {
			continue
		}
		col := SQLColumn{
			Name:         colName,
			Type:         colType,
			Nullable:     nullable,
			IsPrimaryKey: isPK,
		}
		if def != nil {
			col.Default = *def
		}
		t.Columns = append(t.Columns, col)
	}
	return nil
}

// fetchColumnsNucleus loads columns for Nucleus, which does not implement
// information_schema.table_constraints / key_column_usage. Columns come from
// information_schema.columns; primary-key flags are a best-effort enrichment
// via pg_index (indisprimary), so a failure there still yields the columns.
func fetchColumnsNucleus(ctx context.Context, client *db.Client, tables map[string]*SQLTable) error {
	rows, err := client.Query(ctx, `
		SELECT
			c.table_schema,
			c.table_name,
			c.column_name,
			c.udt_name,
			c.is_nullable = 'YES',
			c.column_default,
			c.ordinal_position
		FROM information_schema.columns c
		WHERE c.table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
		ORDER BY c.table_schema, c.table_name, c.ordinal_position
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Track (table key, 1-based ordinal) -> column pointer for PK enrichment.
	type colRef struct {
		table *SQLTable
		idx   int
	}
	byPos := map[string]map[int]colRef{}

	for rows.Next() {
		var tschema, tname, colName, colType string
		var nullable bool
		var def *string
		var ordinal int
		if err := rows.Scan(&tschema, &tname, &colName, &colType, &nullable, &def, &ordinal); err != nil {
			continue
		}
		key := tschema + "." + tname
		t, ok := tables[key]
		if !ok {
			continue
		}
		col := SQLColumn{Name: colName, Type: colType, Nullable: nullable}
		if def != nil {
			col.Default = *def
		}
		t.Columns = append(t.Columns, col)
		if byPos[key] == nil {
			byPos[key] = map[int]colRef{}
		}
		byPos[key][ordinal] = colRef{table: t, idx: len(t.Columns) - 1}
	}
	rows.Close()

	markNucleusPrimaryKeys(ctx, client, tables, func(tableName string, positions []int) {
		// Nucleus tables live in a single schema; match by table name.
		for key, posMap := range byPos {
			if !hasTableName(key, tableName) {
				continue
			}
			for _, p := range positions {
				if ref, ok := posMap[p]; ok {
					ref.table.Columns[ref.idx].IsPrimaryKey = true
				}
			}
		}
	})
	return nil
}

func hasTableName(key, tableName string) bool {
	// key is "schema.table"
	if i := len(key) - len(tableName) - 1; i >= 0 && key[i+1:] == tableName && key[i] == '.' {
		return true
	}
	return key == tableName
}

// markNucleusPrimaryKeys reads primary-key column positions from pg_index and
// pg_class (both virtual tables Nucleus implements) and reports them per table.
// Entirely best-effort: any error leaves columns without PK flags.
func markNucleusPrimaryKeys(ctx context.Context, client *db.Client, _ map[string]*SQLTable, mark func(string, []int)) {
	// pg_class maps oid -> relname; pg_index carries indrelid + indkey + indisprimary.
	classRows, err := client.Query(ctx, `SELECT oid, relname FROM pg_catalog.pg_class`)
	if err != nil {
		return
	}
	oidToName := map[int32]string{}
	for classRows.Next() {
		var oid int32
		var name string
		if err := classRows.Scan(&oid, &name); err == nil {
			oidToName[oid] = name
		}
	}
	classRows.Close()

	idxRows, err := client.Query(ctx, `SELECT indrelid, indkey FROM pg_catalog.pg_index WHERE indisprimary`)
	if err != nil {
		return
	}
	defer idxRows.Close()
	for idxRows.Next() {
		var indrelid int32
		var indkey string
		if err := idxRows.Scan(&indrelid, &indkey); err != nil {
			continue
		}
		name, ok := oidToName[indrelid]
		if !ok {
			continue
		}
		var positions []int
		for _, f := range strings.Fields(indkey) {
			if p, err := strconv.Atoi(f); err == nil && p > 0 {
				positions = append(positions, p)
			}
		}
		if len(positions) > 0 {
			mark(name, positions)
		}
	}
}

// fetchNucleusModels populates each model from the real Nucleus scalar
// functions. Nucleus stores are global and unnamed (one store per model), so
// each populated model surfaces as a single node keyed by the model name.
// Models with no enumeration or count function (vector, timeseries, geo,
// columnar, datalog) have no engine surface to list and stay empty.
// Errors are best-effort: a failing model leaves its list empty.
func fetchNucleusModels(ctx context.Context, client *db.Client, sc *Schema) {
	fetchKV(ctx, client, sc)
	fetchDocument(ctx, client, sc)
	fetchGraph(ctx, client, sc)
	fetchFTS(ctx, client, sc)
	fetchBlob(ctx, client, sc)
	fetchPubSub(ctx, client, sc)
	fetchCDC(ctx, client, sc)
}

// scalarInt64 runs a single-row single-column query returning an int64.
func scalarInt64(ctx context.Context, c *db.Client, sql string) (int64, bool) {
	var n int64
	if err := c.QueryRow(ctx, sql).Scan(&n); err != nil {
		return 0, false
	}
	return n, true
}

func fetchKV(ctx context.Context, c *db.Client, sc *Schema) {
	if n, ok := scalarInt64(ctx, c, `SELECT KV_DBSIZE()`); ok {
		sc.KV = []KVStore{{Name: "keyspace", KeyCount: n}}
	}
}

func fetchDocument(ctx context.Context, c *db.Client, sc *Schema) {
	if n, ok := scalarInt64(ctx, c, `SELECT DOC_COUNT()`); ok {
		sc.Document = []DocCollection{{Name: "documents", Count: n}}
	}
}

func fetchGraph(ctx context.Context, c *db.Client, sc *Schema) {
	nodes, okN := scalarInt64(ctx, c, `SELECT GRAPH_NODE_COUNT()`)
	edges, okE := scalarInt64(ctx, c, `SELECT GRAPH_EDGE_COUNT()`)
	if okN || okE {
		sc.Graph = []GraphStore{{Name: "graph", NodeCount: nodes, EdgeCount: edges}}
	}
}

func fetchFTS(ctx context.Context, c *db.Client, sc *Schema) {
	if n, ok := scalarInt64(ctx, c, `SELECT FTS_DOC_COUNT()`); ok {
		sc.FTS = []FTSIndex{{Name: "fts", DocCount: n}}
	}
}

func fetchBlob(ctx context.Context, c *db.Client, sc *Schema) {
	if n, ok := scalarInt64(ctx, c, `SELECT BLOB_COUNT()`); ok {
		sc.Blob = []BlobStore{{Name: "blobs", BlobCount: n}}
	}
}

func fetchPubSub(ctx context.Context, c *db.Client, sc *Schema) {
	// PUBSUB_CHANNELS() returns a comma-separated list of active channels.
	var raw string
	if err := c.QueryRow(ctx, `SELECT PUBSUB_CHANNELS()`).Scan(&raw); err != nil {
		return
	}
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name != "" {
			sc.PubSub = append(sc.PubSub, PubSubChannel{Name: name})
		}
	}
}

func fetchCDC(ctx context.Context, c *db.Client, sc *Schema) {
	if _, ok := scalarInt64(ctx, c, `SELECT CDC_COUNT()`); ok {
		sc.CDC = true
	}
}
