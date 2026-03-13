package nucleus

import (
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
