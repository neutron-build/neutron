package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func source(t *testing.T, text string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "model.go")
	if err := os.WriteFile(p, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDeterministicGeneration(t *testing.T) {
	p := source(t, "package example\ntype Record struct { ID int64 `db:\"id\"`; Note *string `db:\"note,nullable\"` }")
	a, err := generate(p, "Record")
	if err != nil {
		t.Fatal(err)
	}
	b, err := generate(p, "Record")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("generation changed")
	}
	if !bytes.Contains(a, []byte("typed.Optional[*string]")) {
		t.Fatal("nullable write type missing")
	}
}

func TestInvalidModelRejected(t *testing.T) {
	cases := []struct{ name, text, diagnostic string }{
		{"duplicate", "A string `db:\"same\"`; B bool `db:\"same\"`", "duplicate db tag"},
		{"nullable_scalar", "A string `db:\"a,nullable\"`", "pointer/nullability mismatch"},
		{"untagged_pointer", "A *string `db:\"a\"`", "pointer/nullability mismatch"},
		{"unknown_option", "A string `db:\"a,omit\"`", "unknown db option"},
		{"unexported", "a string `db:\"a\"`", "unexported"},
		{"grouped", "A, B string `db:\"a\"`", "grouped"},
		{"embedded", "Other `db:\"other\"`", "embedded"},
		{"unsupported_type", "A []string `db:\"a\"`", "unsupported field type"},
		{"unsafe_identifier", "A string `db:\"a;drop\"`", "unsupported db identifier"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := generate(source(t, "package example\ntype Record struct {"+c.text+"}"), "Record")
			if err == nil || !strings.Contains(err.Error(), c.diagnostic) {
				t.Fatalf("got %v wanted %s", err, c.diagnostic)
			}
		})
	}
}
