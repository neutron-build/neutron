package studio

import (
	"net/http/httptest"
	"strings"
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
		{"malformed path encoding", "postgres://localhost/%zz?password=secret", "[redacted invalid connection URI]"},
		{"invalid authority", "postgres://[invalid/db?password=secret", "[redacted invalid connection URI]"},
		{"query password", "postgres://user@localhost/db?password=secret", "postgres://user@localhost/db?password=%2A%2A%2A"},
		{"encoded query password key", "postgres://localhost/db?%70assword=secret&password=second", "postgres://localhost/db?password=%2A%2A%2A"},
		{"keyword password", "host=localhost user=admin password=secret dbname=app", "host=localhost user=admin password=*** dbname=app"},
		{"quoted keyword password", "host=localhost password='secret with spaces' dbname=app", "host=localhost password=*** dbname=app"},
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

// Exercise the browser-facing response, not only the masking helper.
func TestSavedConnectionResponseRedactsAlternatePasswordForms(t *testing.T) {
	s := &Server{store: &connectionStore{connections: []SavedConnection{
		{ID: "query", Name: "query", URL: "postgres://localhost/db?%70assword=secret_query&password=second_secret"},
		{ID: "invalid-path", Name: "invalid-path", URL: "postgres://localhost/%zz?password=secret_invalid_path"},
		{ID: "invalid-host", Name: "invalid-host", URL: "postgres://[invalid/db?password=secret_invalid_host"},
		{ID: "keyword", Name: "keyword", URL: "host=localhost password='secret keyword' dbname=app"},
	}}}
	recorder := httptest.NewRecorder()
	s.listConnections(recorder, httptest.NewRequest("GET", "/api/connections", nil))
	if recorder.Code != 200 {
		t.Fatalf("status %d", recorder.Code)
	}
	for _, secret := range []string{"secret_query", "second_secret", "secret keyword", "secret_invalid_path", "secret_invalid_host"} {
		if strings.Contains(recorder.Body.String(), secret) {
			t.Fatalf("saved connection response leaked a password")
		}
	}
	if !strings.Contains(recorder.Body.String(), "query") || !strings.Contains(recorder.Body.String(), "keyword") {
		t.Fatal("connection metadata lost")
	}
}
