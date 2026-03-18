package studio

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
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

	if err := fetchSQLTables(ctx, client, sc); err != nil {
		return nil, fmt.Errorf("sql schema: %w", err)
	}

	if isNucleus {
		fetchNucleusModels(ctx, client, sc) // best-effort
	}

	return sc, nil
}

func fetchSQLTables(ctx context.Context, client *db.Client, sc *Schema) error {
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

	if err := fetchColumns(ctx, client, tables); err != nil {
		return err
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

// fetchNucleusModels queries Nucleus SQL functions for each data model.
// All errors are silently ignored — if a model isn't available it stays empty.
func fetchNucleusModels(ctx context.Context, client *db.Client, sc *Schema) {
	fetchKV(ctx, client, sc)
	fetchVector(ctx, client, sc)
	fetchTimeSeries(ctx, client, sc)
	fetchDocument(ctx, client, sc)
	fetchGraph(ctx, client, sc)
	fetchFTS(ctx, client, sc)
	fetchGeo(ctx, client, sc)
	fetchBlob(ctx, client, sc)
	fetchPubSub(ctx, client, sc)
	fetchStreams(ctx, client, sc)
	fetchColumnar(ctx, client, sc)
	fetchDatalog(ctx, client, sc)
}

func queryRows[T any](ctx context.Context, client *db.Client, sql string, scan func(pgx.Rows) (T, error)) []T {
	rows, err := client.Query(ctx, sql)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err == nil {
			out = append(out, v)
		}
	}
	return out
}

func fetchKV(ctx context.Context, c *db.Client, sc *Schema) {
	sc.KV = queryRows(ctx, c, `SELECT name, key_count FROM nucleus_kv_stores()`,
		func(r pgx.Rows) (KVStore, error) {
			var v KVStore
			return v, r.Scan(&v.Name, &v.KeyCount)
		})
	if sc.KV == nil { sc.KV = []KVStore{} }
}

func fetchVector(ctx context.Context, c *db.Client, sc *Schema) {
	sc.Vector = queryRows(ctx, c, `SELECT name, dimensions, metric, count FROM nucleus_vector_indexes()`,
		func(r pgx.Rows) (VectorIndex, error) {
			var v VectorIndex
			return v, r.Scan(&v.Name, &v.Dimensions, &v.Metric, &v.Count)
		})
	if sc.Vector == nil { sc.Vector = []VectorIndex{} }
}

func fetchTimeSeries(ctx context.Context, c *db.Client, sc *Schema) {
	sc.TimeSeries = queryRows(ctx, c, `SELECT name, count FROM nucleus_timeseries_metrics()`,
		func(r pgx.Rows) (TSMetric, error) {
			var v TSMetric
			return v, r.Scan(&v.Name, &v.Count)
		})
	if sc.TimeSeries == nil { sc.TimeSeries = []TSMetric{} }
}

func fetchDocument(ctx context.Context, c *db.Client, sc *Schema) {
	sc.Document = queryRows(ctx, c, `SELECT name, count FROM nucleus_doc_collections()`,
		func(r pgx.Rows) (DocCollection, error) {
			var v DocCollection
			return v, r.Scan(&v.Name, &v.Count)
		})
	if sc.Document == nil { sc.Document = []DocCollection{} }
}

func fetchGraph(ctx context.Context, c *db.Client, sc *Schema) {
	sc.Graph = queryRows(ctx, c, `SELECT name, node_count, edge_count FROM nucleus_graph_stores()`,
		func(r pgx.Rows) (GraphStore, error) {
			var v GraphStore
			return v, r.Scan(&v.Name, &v.NodeCount, &v.EdgeCount)
		})
	if sc.Graph == nil { sc.Graph = []GraphStore{} }
}

func fetchFTS(ctx context.Context, c *db.Client, sc *Schema) {
	sc.FTS = queryRows(ctx, c, `SELECT name, doc_count FROM nucleus_fts_indexes()`,
		func(r pgx.Rows) (FTSIndex, error) {
			var v FTSIndex
			return v, r.Scan(&v.Name, &v.DocCount)
		})
	if sc.FTS == nil { sc.FTS = []FTSIndex{} }
}

func fetchGeo(ctx context.Context, c *db.Client, sc *Schema) {
	sc.Geo = queryRows(ctx, c, `SELECT name, point_count FROM nucleus_geo_layers()`,
		func(r pgx.Rows) (GeoLayer, error) {
			var v GeoLayer
			return v, r.Scan(&v.Name, &v.PointCount)
		})
	if sc.Geo == nil { sc.Geo = []GeoLayer{} }
}

func fetchBlob(ctx context.Context, c *db.Client, sc *Schema) {
	sc.Blob = queryRows(ctx, c, `SELECT name, blob_count FROM nucleus_blob_stores()`,
		func(r pgx.Rows) (BlobStore, error) {
			var v BlobStore
			return v, r.Scan(&v.Name, &v.BlobCount)
		})
	if sc.Blob == nil { sc.Blob = []BlobStore{} }
}

func fetchPubSub(ctx context.Context, c *db.Client, sc *Schema) {
	sc.PubSub = queryRows(ctx, c, `SELECT name FROM nucleus_pubsub_channels()`,
		func(r pgx.Rows) (PubSubChannel, error) {
			var v PubSubChannel
			return v, r.Scan(&v.Name)
		})
	if sc.PubSub == nil { sc.PubSub = []PubSubChannel{} }
}

func fetchStreams(ctx context.Context, c *db.Client, sc *Schema) {
	sc.Streams = queryRows(ctx, c, `SELECT name, length FROM nucleus_streams()`,
		func(r pgx.Rows) (Stream, error) {
			var v Stream
			return v, r.Scan(&v.Name, &v.Length)
		})
	if sc.Streams == nil { sc.Streams = []Stream{} }
}

func fetchColumnar(ctx context.Context, c *db.Client, sc *Schema) {
	sc.Columnar = queryRows(ctx, c, `SELECT name, row_count FROM nucleus_columnar_tables()`,
		func(r pgx.Rows) (ColumnarTable, error) {
			var v ColumnarTable
			return v, r.Scan(&v.Name, &v.RowCount)
		})
	if sc.Columnar == nil { sc.Columnar = []ColumnarTable{} }
}

func fetchDatalog(ctx context.Context, c *db.Client, sc *Schema) {
	var d DatalogStore
	row := c.QueryRow(ctx, `SELECT predicate_count, rule_count FROM nucleus_datalog_stats()`)
	if err := row.Scan(&d.PredicateCount, &d.RuleCount); err == nil {
		sc.Datalog = &d
	}
}
