package inspect

import (
	"reflect"
	"strings"
	"testing"
)

func TestSensitiveName(t *testing.T) {
	for _, n := range []string{"password", "password_hash", "PasswordHash", "apiKey", "api_key", "API_KEY",
		"refresh_token", "secret", "client_secret", "privateKey", "card_number", "otp", "ssn", "user.password"} {
		if !SensitiveName(n) {
			t.Errorf("%q not treated as sensitive", n)
		}
	}
	for _, n := range []string{"author", "tokenizer_version", "id", "email", "name", "passenger", "key", "session_count", "pinned"} {
		if SensitiveName(n) {
			t.Errorf("%q treated as sensitive", n)
		}
	}
}

func TestRedactorValue(t *testing.T) {
	in := []any{
		map[string]any{"id": float64(1), "email": "a@b", "password_hash": "$2b$...", "profile": map[string]any{"apiKey": "k1", "bio": "hi"}},
		map[string]any{"id": float64(2), "password_hash": nil},
	}
	out, keys := Redactor{Enabled: true}.Value(in)
	rows := out.([]any)
	first := rows[0].(map[string]any)
	if first["password_hash"] != RedactedValue || first["email"] != "a@b" {
		t.Fatalf("row 0: %#v", first)
	}
	if first["profile"].(map[string]any)["apiKey"] != RedactedValue || first["profile"].(map[string]any)["bio"] != "hi" {
		t.Fatalf("nested: %#v", first["profile"])
	}
	// SQL NULL stays NULL: absence of a secret is not a secret.
	if rows[1].(map[string]any)["password_hash"] != nil {
		t.Fatalf("null redacted: %#v", rows[1])
	}
	if !reflect.DeepEqual(keys, []string{"apiKey", "password_hash"}) {
		t.Fatalf("keys %v", keys)
	}
	// The input is untouched.
	if in[0].(map[string]any)["password_hash"] != "$2b$..." {
		t.Fatal("input mutated")
	}
	same, none := Redactor{Enabled: false}.Value(in)
	if !reflect.DeepEqual(same, in) || none != nil {
		t.Fatal("disabled redactor changed data")
	}
}

func TestRedactURL(t *testing.T) {
	cases := map[string]string{
		"postgres://app:s3cret@db:5432/app":            "xxxxx",
		"postgresql://app:p%40ss@db/app?sslmode=off":   "xxxxx",
		"postgres://db/app?password=hunter2&user=app":  "xxxxx",
		"host=db user=app password=hunter2 dbname=app": "password=xxxxx",
		"host=db password='a b c' dbname=app":          "password=xxxxx",
	}
	for in, want := range cases {
		got := RedactURL(in)
		if !strings.Contains(got, want) {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
		for _, secret := range []string{"s3cret", "p%40ss", "hunter2", "a b c"} {
			if strings.Contains(got, secret) {
				t.Errorf("%q -> %q leaks %q", in, got, secret)
			}
		}
	}
	if got := RedactURL("postgres://app@db/app"); got != "postgres://app@db/app" {
		t.Errorf("passwordless URL changed: %q", got)
	}
}

func TestRedactText(t *testing.T) {
	in := `connect to postgres://app:s3cret@db:5432/app failed; retry "postgresql://u:p4ss@h/d" or host=db password=hunter2`
	got := RedactText(in)
	for _, secret := range []string{"s3cret", "p4ss", "hunter2"} {
		if strings.Contains(got, secret) {
			t.Fatalf("%q leaks %q", got, secret)
		}
	}
	if !strings.Contains(got, "connect to postgres://app:xxxxx@db:5432/app failed; retry") {
		t.Fatalf("surrounding text mangled: %q", got)
	}
	if plain := "relation \"orders\" does not exist"; RedactText(plain) != plain {
		t.Fatalf("plain error changed: %q", RedactText(plain))
	}
}
