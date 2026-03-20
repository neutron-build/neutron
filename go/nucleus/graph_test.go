package nucleus

import (
	"context"
	"testing"
)

func TestDirectionString(t *testing.T) {
	tests := []struct {
		d    Direction
		want string
	}{
		{Outgoing, "out"},
		{Incoming, "in"},
		{Both, "both"},
	}
	for _, tt := range tests {
		if got := tt.d.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestNodeStruct(t *testing.T) {
	n := Node{
		ID:         1,
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	}
	if n.ID != 1 {
		t.Errorf("ID = %d", n.ID)
	}
	if len(n.Labels) != 1 || n.Labels[0] != "Person" {
		t.Errorf("Labels = %v", n.Labels)
	}
}

func TestEdgeStruct(t *testing.T) {
	e := Edge{
		ID:     1,
		Type:   "KNOWS",
		FromID: 1,
		ToID:   2,
	}
	if e.Type != "KNOWS" {
		t.Errorf("Type = %q", e.Type)
	}
}

func TestEdgeStructFull(t *testing.T) {
	e := Edge{
		ID:         42,
		Type:       "FOLLOWS",
		FromID:     10,
		ToID:       20,
		Properties: map[string]any{"since": "2024"},
	}
	if e.ID != 42 {
		t.Errorf("ID = %d", e.ID)
	}
	if e.FromID != 10 {
		t.Errorf("FromID = %d", e.FromID)
	}
	if e.ToID != 20 {
		t.Errorf("ToID = %d", e.ToID)
	}
	if e.Properties["since"] != "2024" {
		t.Errorf("Properties = %v", e.Properties)
	}
}

func TestGraphResultStruct(t *testing.T) {
	r := GraphResult{
		Columns: []string{"name", "age"},
		Rows: []map[string]any{
			{"name": "Alice", "age": 30},
		},
	}
	if len(r.Columns) != 2 {
		t.Errorf("Columns = %v", r.Columns)
	}
	if len(r.Rows) != 1 {
		t.Errorf("Rows = %v", r.Rows)
	}
}

func TestDirectionStringDefault(t *testing.T) {
	var d Direction = 99
	if d.String() != "out" {
		t.Errorf("unknown direction.String() = %q, want out", d.String())
	}
}

func TestGraphRequiresNucleus(t *testing.T) {
	q := &mockCDCQuerier{}
	client := plainPGClient()
	g := &GraphModel{pool: q, client: client}

	tests := []struct {
		name string
		fn   func() error
	}{
		{"AddNode", func() error { _, err := g.AddNode(context.Background(), nil, nil); return err }},
		{"AddEdge", func() error { _, err := g.AddEdge(context.Background(), 1, 2, "E", nil); return err }},
		{"DeleteNode", func() error { _, err := g.DeleteNode(context.Background(), 1); return err }},
		{"DeleteEdge", func() error { _, err := g.DeleteEdge(context.Background(), 1); return err }},
		{"Query", func() error { _, err := g.Query(context.Background(), "q", nil); return err }},
		{"Neighbors", func() error { _, err := g.Neighbors(context.Background(), 1, "", Outgoing); return err }},
		{"ShortestPath", func() error { _, err := g.ShortestPath(context.Background(), 1, 2, 0); return err }},
		{"NodeCount", func() error { _, err := g.NodeCount(context.Background()); return err }},
		{"EdgeCount", func() error { _, err := g.EdgeCount(context.Background()); return err }},
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

func TestNodeMultipleLabels(t *testing.T) {
	n := Node{
		ID:     1,
		Labels: []string{"Person", "Employee", "Manager"},
	}
	if len(n.Labels) != 3 {
		t.Errorf("Labels = %v", n.Labels)
	}
}
