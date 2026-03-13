package nucleus

import (
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
