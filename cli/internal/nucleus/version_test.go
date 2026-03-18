package nucleus

import (
	"testing"
)

func TestResolveVersionExplicit(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"0.1.0", "0.1.0"},
		{"v0.1.0", "0.1.0"},
		{"1.2.3", "1.2.3"},
		{"v2.0.0", "2.0.0"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ResolveVersion(tt.input)
			if err != nil {
				t.Fatalf("ResolveVersion(%q) error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ResolveVersion(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestResolveVersionStripsVPrefix(t *testing.T) {
	got, err := ResolveVersion("v1.0.0")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got != "1.0.0" {
		t.Errorf("got %q, want 1.0.0", got)
	}
}
