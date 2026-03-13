package nucleus

import (
	"testing"
)

func TestIsValidIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
	}{
		{"users", true},
		{"my_table", true},
		{"_private", true},
		{"Table123", true},
		{"", false},
		{"123abc", false},
		{"my-table", false},
		{"drop table; --", false},
		{"my table", false},
		{"table.name", false},
	}
	for _, tt := range tests {
		if got := isValidIdentifier(tt.name); got != tt.valid {
			t.Errorf("isValidIdentifier(%q) = %v, want %v", tt.name, got, tt.valid)
		}
	}
}
