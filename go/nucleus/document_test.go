package nucleus

import (
	"context"
	"testing"
)

func TestDocOptionWithSort(t *testing.T) {
	var o docOpts
	WithSort("name", true)(&o)
	if o.sortField != "name" {
		t.Errorf("sortField = %q", o.sortField)
	}
	if !o.sortAsc {
		t.Error("sortAsc should be true")
	}
}

func TestDocOptionWithDocLimit(t *testing.T) {
	var o docOpts
	WithDocLimit(50)(&o)
	if o.limit != 50 {
		t.Errorf("limit = %d, want 50", o.limit)
	}
}

func TestDocOptionWithSkip(t *testing.T) {
	var o docOpts
	WithSkip(10)(&o)
	if o.skip != 10 {
		t.Errorf("skip = %d, want 10", o.skip)
	}
}

func TestDocOptionWithProjection(t *testing.T) {
	var o docOpts
	WithProjection("name", "email")(&o)
	if len(o.fields) != 2 {
		t.Errorf("fields = %v", o.fields)
	}
}

func TestDocGetTypedCompiles(t *testing.T) {
	type User struct {
		Name string `json:"name"`
	}
	_ = DocGetTyped[User]
	_ = FindTyped[User]
	_ = FindOneTyped[User]
}

func TestApplyProjection(t *testing.T) {
	doc := map[string]any{"name": "Alice", "email": "alice@example.com", "age": 30}
	projected := applyProjection(doc, []string{"name", "email"})
	if len(projected) != 2 {
		t.Errorf("projected = %v", projected)
	}
	if projected["name"] != "Alice" {
		t.Errorf("name = %v", projected["name"])
	}
	if _, ok := projected["age"]; ok {
		t.Error("age should not be projected")
	}
}

func TestApplyProjectionEmpty(t *testing.T) {
	doc := map[string]any{"name": "Alice"}
	projected := applyProjection(doc, nil)
	if len(projected) != 1 {
		t.Errorf("projected with no fields should return original doc, got %v", projected)
	}
}

func TestApplyProjectionMissingFields(t *testing.T) {
	doc := map[string]any{"name": "Alice"}
	projected := applyProjection(doc, []string{"email", "phone"})
	if len(projected) != 0 {
		t.Errorf("projected with missing fields should return empty map, got %v", projected)
	}
}

func TestDocRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	d := &DocumentModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Insert", func() error { _, err := d.Insert(context.Background(), "col", nil); return err }},
		{"Get", func() error { _, err := d.Get(context.Background(), 1); return err }},
		{"QueryDocs", func() error { _, err := d.QueryDocs(context.Background(), nil); return err }},
		{"Path", func() error { _, err := d.Path(context.Background(), 1, "key"); return err }},
		{"Count", func() error { _, err := d.Count(context.Background()); return err }},
		{"Update", func() error {
			_, err := d.Update(context.Background(), "col", map[string]any{}, map[string]any{})
			return err
		}},
		{"Delete", func() error {
			_, err := d.Delete(context.Background(), "col", map[string]any{})
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

func TestApplyDocOptsDefaults(t *testing.T) {
	o := applyDocOpts(nil)
	if o.sortField != "" {
		t.Errorf("default sortField = %q", o.sortField)
	}
	if o.limit != 0 {
		t.Errorf("default limit = %d", o.limit)
	}
	if o.skip != 0 {
		t.Errorf("default skip = %d", o.skip)
	}
	if o.fields != nil {
		t.Errorf("default fields = %v", o.fields)
	}
}

func TestDocOptionCombined(t *testing.T) {
	o := applyDocOpts([]DocOption{
		WithSort("name", false),
		WithDocLimit(20),
		WithSkip(5),
		WithProjection("name", "email"),
	})
	if o.sortField != "name" {
		t.Errorf("sortField = %q", o.sortField)
	}
	if o.sortAsc {
		t.Error("sortAsc should be false")
	}
	if o.limit != 20 {
		t.Errorf("limit = %d", o.limit)
	}
	if o.skip != 5 {
		t.Errorf("skip = %d", o.skip)
	}
	if len(o.fields) != 2 {
		t.Errorf("fields = %v", o.fields)
	}
}
