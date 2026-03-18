package studio

import (
	"testing"
)

func TestMaskedURL(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			"full url with password",
			"postgres://user:secret@localhost:5432/db",
			"postgres://user:***@localhost:5432/db",
		},
		{
			"no password",
			"postgres://user@localhost:5432/db",
			"postgres://user@localhost:5432/db",
		},
		{
			"no auth",
			"postgres://localhost:5432/db",
			"postgres://localhost:5432/db",
		},
		{
			"empty url",
			"",
			"",
		},
		{
			"complex password",
			"postgres://admin:p@ssw0rd!@db.example.com:5432/prod",
			"postgres://admin:***@db.example.com:5432/prod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MaskedURL(tt.input)
			if got != tt.want {
				t.Errorf("MaskedURL(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNucleusModels(t *testing.T) {
	t.Run("nucleus", func(t *testing.T) {
		models := nucleusModels(true)
		if len(models) != 14 {
			t.Errorf("nucleusModels(true) returned %d models, want 14", len(models))
		}
		// First should be sql
		if models[0] != "sql" {
			t.Errorf("first model = %q, want %q", models[0], "sql")
		}
	})

	t.Run("postgres", func(t *testing.T) {
		models := nucleusModels(false)
		if len(models) != 1 {
			t.Errorf("nucleusModels(false) returned %d models, want 1", len(models))
		}
		if models[0] != "sql" {
			t.Errorf("model = %q, want %q", models[0], "sql")
		}
	})
}
