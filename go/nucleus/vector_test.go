package nucleus

import (
	"context"
	"strings"
	"testing"
)

func TestDistanceMetricString(t *testing.T) {
	tests := []struct {
		m    DistanceMetric
		want string
	}{
		{Cosine, "cosine"},
		{Euclidean, "l2"},
		{DotProduct, "inner"},
	}
	for _, tt := range tests {
		if got := tt.m.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.m, got, tt.want)
		}
	}
}

func TestFloat32SliceToSQL(t *testing.T) {
	got := float32SliceToSQL([]float32{1.0, 2.5, 3.0})
	want := "[1,2.5,3]"
	if got != want {
		t.Errorf("float32SliceToSQL = %q, want %q", got, want)
	}
}

func TestFloat32SliceToSQLEmpty(t *testing.T) {
	got := float32SliceToSQL([]float32{})
	want := "[]"
	if got != want {
		t.Errorf("float32SliceToSQL = %q, want %q", got, want)
	}
}

func TestDefaultVectorOpts(t *testing.T) {
	o := defaultVectorOpts()
	if o.limit != 10 {
		t.Errorf("default limit = %d, want 10", o.limit)
	}
	if o.metric != Cosine {
		t.Errorf("default metric = %d, want Cosine", o.metric)
	}
}

func TestVectorOptions(t *testing.T) {
	o := defaultVectorOpts()
	WithLimit(50)(&o)
	WithMetric(Euclidean)(&o)

	if o.limit != 50 {
		t.Errorf("limit = %d, want 50", o.limit)
	}
	if o.metric != Euclidean {
		t.Errorf("metric = %d, want Euclidean", o.metric)
	}
}

func TestSearchTypedCompiles(t *testing.T) {
	// Verify the generic function compiles
	type Item struct {
		Name string `json:"name"`
	}
	_ = SearchTyped[Item]
}

func TestVectorSearchResultScore(t *testing.T) {
	r := VectorSearchResult[map[string]any]{
		Item:     map[string]any{"id": "1"},
		Score:    0.95,
		Distance: 0.05,
	}
	if r.Score != 0.95 {
		t.Errorf("Score = %f, want 0.95", r.Score)
	}
	if r.Distance != 0.05 {
		t.Errorf("Distance = %f, want 0.05", r.Distance)
	}
}

func TestWithFilter(t *testing.T) {
	o := defaultVectorOpts()
	f := map[string]any{"category": "tech"}
	WithFilter(f)(&o)
	if o.filter == nil {
		t.Fatal("filter should be set")
	}
	if o.filter["category"] != "tech" {
		t.Errorf("filter = %v", o.filter)
	}
}

func TestDistanceMetricStringDefault(t *testing.T) {
	// An out-of-range DistanceMetric should default to "l2"
	var m DistanceMetric = 99
	if m.String() != "l2" {
		t.Errorf("unknown metric.String() = %q, want l2", m.String())
	}
}

func TestVectorRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	v := &VectorModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Search", func() error {
			_, err := v.Search(context.Background(), "col", []float32{1.0})
			return err
		}},
		{"Insert", func() error {
			return v.Insert(context.Background(), "col", "id1", []float32{1.0}, nil)
		}},
		{"Delete", func() error {
			return v.Delete(context.Background(), "col", "id1")
		}},
		{"CreateCollection", func() error {
			return v.CreateCollection(context.Background(), "col", 128, Cosine)
		}},
		{"Dims", func() error {
			_, err := v.Dims(context.Background(), []float32{1.0})
			return err
		}},
		{"Distance", func() error {
			_, err := v.Distance(context.Background(), []float32{1.0}, []float32{2.0}, Cosine)
			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Fatal("expected error for non-Nucleus database")
			}
		})
	}
}

func TestVectorInvalidIdentifier(t *testing.T) {
	q := &mockCDCQuerier{}
	v := &VectorModel{pool: q, client: nucleusClient()}

	_, err := v.Search(context.Background(), "bad-name", []float32{1.0})
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}
	if !strings.Contains(err.Error(), "invalid collection name") {
		t.Errorf("error = %q", err.Error())
	}

	err = v.Insert(context.Background(), "bad name", "id1", []float32{1.0}, nil)
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}

	err = v.Delete(context.Background(), "123bad", "id1")
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}

	err = v.CreateCollection(context.Background(), "drop;table", 128, Cosine)
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}
}

func TestVectorSearchResultFields(t *testing.T) {
	r := VectorSearchResult[string]{
		Item:     "test",
		Score:    1.5,
		Distance: 0.3,
	}
	if r.Item != "test" {
		t.Errorf("Item = %q", r.Item)
	}
	if r.Score != 1.5 {
		t.Errorf("Score = %f", r.Score)
	}
	if r.Distance != 0.3 {
		t.Errorf("Distance = %f", r.Distance)
	}
}

func TestFloat32SliceToSQLSingle(t *testing.T) {
	got := float32SliceToSQL([]float32{42.0})
	want := "[42]"
	if got != want {
		t.Errorf("float32SliceToSQL = %q, want %q", got, want)
	}
}
