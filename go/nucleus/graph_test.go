package nucleus

import (
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
