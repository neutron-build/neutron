package nucleus

import (
	"context"
	"testing"
)

func TestFTSOptionWithFuzzy(t *testing.T) {
	o := ftsOpts{limit: 10}
	WithFuzzy(2)(&o)
	if o.fuzzyDist != 2 {
		t.Errorf("fuzzyDist = %d, want 2", o.fuzzyDist)
	}
}

func TestFTSOptionWithLimit(t *testing.T) {
	var o ftsOpts
	WithFTSLimit(50)(&o)
	if o.limit != 50 {
		t.Errorf("limit = %d, want 50", o.limit)
	}
}

func TestFTSResultStruct(t *testing.T) {
	r := FTSResult{DocID: 42, Score: 0.95, Highlight: map[string]string{"title": "<em>test</em>"}}
	if r.DocID != 42 {
		t.Errorf("DocID = %d", r.DocID)
	}
	if r.Score != 0.95 {
		t.Errorf("Score = %f", r.Score)
	}
	if r.Highlight["title"] != "<em>test</em>" {
		t.Errorf("Highlight = %v", r.Highlight)
	}
}

func TestFTSOptionWithHighlight(t *testing.T) {
	var o ftsOpts
	WithHighlight("title", "body")(&o)
	if len(o.highlight) != 2 {
		t.Errorf("highlight = %v", o.highlight)
	}
}

func TestFTSOptionWithFacets(t *testing.T) {
	var o ftsOpts
	WithFacets("category", "author")(&o)
	if len(o.facets) != 2 {
		t.Errorf("facets = %v", o.facets)
	}
}

func TestFTSRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	f := &FTSModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Index", func() error { _, err := f.Index(context.Background(), 1, "text"); return err }},
		{"Search", func() error { _, err := f.Search(context.Background(), "query"); return err }},
		{"Remove", func() error { _, err := f.Remove(context.Background(), 1); return err }},
		{"DocCount", func() error { _, err := f.DocCount(context.Background()); return err }},
		{"TermCount", func() error { _, err := f.TermCount(context.Background()); return err }},
		{"CreateIndex", func() error { return f.CreateIndex(context.Background(), "idx", nil) }},
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

func TestFTSResultDefaultHighlight(t *testing.T) {
	r := FTSResult{DocID: 1, Score: 0.5}
	if r.Highlight != nil {
		t.Errorf("default Highlight should be nil, got %v", r.Highlight)
	}
}

func TestFTSSchemaIsMap(t *testing.T) {
	var s FTSSchema = map[string]any{"field": "text"}
	if s["field"] != "text" {
		t.Errorf("schema = %v", s)
	}
}
