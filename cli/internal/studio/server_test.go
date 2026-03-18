package studio

import (
	"testing"
)

func TestServerURL(t *testing.T) {
	s := &Server{port: 4983}
	url := s.URL()
	if url != "http://localhost:4983" {
		t.Errorf("URL() = %q, want %q", url, "http://localhost:4983")
	}
}

func TestServerURLDifferentPort(t *testing.T) {
	s := &Server{port: 8080}
	url := s.URL()
	if url != "http://localhost:8080" {
		t.Errorf("URL() = %q, want %q", url, "http://localhost:8080")
	}
}

func TestSchemaTypes(t *testing.T) {
	// Verify all schema types are properly initialized
	sc := Schema{
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

	if sc.SQL == nil {
		t.Error("SQL is nil")
	}
	if sc.KV == nil {
		t.Error("KV is nil")
	}
	if sc.Vector == nil {
		t.Error("Vector is nil")
	}
	if sc.Datalog != nil {
		t.Error("Datalog should be nil when not set")
	}
}

func TestSQLColumnStruct(t *testing.T) {
	col := SQLColumn{
		Name:         "id",
		Type:         "integer",
		Nullable:     false,
		Default:      "nextval('users_id_seq')",
		IsPrimaryKey: true,
	}
	if col.Name != "id" {
		t.Errorf("Name = %q", col.Name)
	}
	if col.Nullable {
		t.Error("Nullable should be false")
	}
	if !col.IsPrimaryKey {
		t.Error("IsPrimaryKey should be true")
	}
}

func TestSQLTableStruct(t *testing.T) {
	table := SQLTable{
		Schema:  "public",
		Name:    "users",
		Columns: []SQLColumn{{Name: "id", Type: "integer"}},
	}
	if table.Schema != "public" {
		t.Errorf("Schema = %q", table.Schema)
	}
	if len(table.Columns) != 1 {
		t.Errorf("Columns len = %d", len(table.Columns))
	}
}

func TestKVStoreStruct(t *testing.T) {
	store := KVStore{Name: "sessions", KeyCount: 100}
	if store.Name != "sessions" {
		t.Errorf("Name = %q", store.Name)
	}
	if store.KeyCount != 100 {
		t.Errorf("KeyCount = %d", store.KeyCount)
	}
}

func TestVectorIndexStruct(t *testing.T) {
	idx := VectorIndex{Name: "embeddings", Dimensions: 1536, Metric: "cosine", Count: 50000}
	if idx.Dimensions != 1536 {
		t.Errorf("Dimensions = %d", idx.Dimensions)
	}
	if idx.Metric != "cosine" {
		t.Errorf("Metric = %q", idx.Metric)
	}
}

func TestGraphStoreStruct(t *testing.T) {
	gs := GraphStore{Name: "social", NodeCount: 1000, EdgeCount: 5000}
	if gs.NodeCount != 1000 {
		t.Errorf("NodeCount = %d", gs.NodeCount)
	}
}

func TestColumnDetailStruct(t *testing.T) {
	cd := ColumnDetail{
		Name:         "email",
		DataType:     "varchar(255)",
		IsNullable:   true,
		IsPrimaryKey: false,
		Ordinal:      3,
	}
	if cd.Ordinal != 3 {
		t.Errorf("Ordinal = %d", cd.Ordinal)
	}
}

func TestIndexDetailStruct(t *testing.T) {
	idx := IndexDetail{
		Name:     "idx_users_email",
		Columns:  []string{"email"},
		IsUnique: true,
	}
	if !idx.IsUnique {
		t.Error("IsUnique should be true")
	}
	if len(idx.Columns) != 1 {
		t.Errorf("Columns len = %d", len(idx.Columns))
	}
}
