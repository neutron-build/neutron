package nucleus

import (
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
