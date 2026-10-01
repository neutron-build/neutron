package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/neutron-build/neutron/go/neutronauth"
	"github.com/neutron-build/neutron/go/nucleus"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDecimalCanonical(t *testing.T) {
	for input, want := range map[string]string{"0001.2": "1.200000000000000000", "-0.000": "0.000000000000000000", "9999999999999999999999.123456789012345678": "9999999999999999999999.123456789012345678", "-1": "-1.000000000000000000"} {
		got, err := decimal(input)
		if err != nil || got != want {
			t.Fatalf("%q: %q %v", input, got, err)
		}
	}
	for _, input := range []string{"NaN", "Infinity", "1e2", "1.1234567890123456789", "10000000000000000000000", "+1", ".1", "1.", " 1"} {
		if _, err := decimal(input); err == nil {
			t.Fatal("accepted", input)
		}
	}
}
func TestRequiredNullableAndStrictObject(t *testing.T) {
	for _, body := range []string{`{}`, `{"note":1}`, `{"note":null,"note":""}`, `{"note":null} {}`, `{"note":null,"dsn":"private"}`, "{\"note\":\"\xff\"}", `{"note":"\u0000"}`, `{"note":"\ud800"}`, `{"note":"\udc00"}`} {
		r := httptest.NewRequest("POST", "/", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r = r.WithContext(context.WithValue(r.Context(), principalKey{}, principal{tenant: "tenant-a"}))
		m, err := input(r, []string{"note"})
		if err == nil {
			_, err = note(m)
		}
		if err == nil {
			t.Fatal("accepted", body)
		}
	}
	for _, body := range []string{`{"note":null}`, `{"note":""}`} {
		r := httptest.NewRequest("POST", "/", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r = r.WithContext(context.WithValue(r.Context(), principalKey{}, principal{tenant: "tenant-a"}))
		m, err := input(r, []string{"note"})
		if err != nil {
			t.Fatal(err)
		}
		n, err := note(m)
		if err != nil {
			t.Fatal(err)
		}
		if (n == nil) != (body == `{"note":null}`) {
			t.Fatal("null presence lost")
		}
	}
}
func TestApplicationPrincipalAndSecretRedaction(t *testing.T) {
	secret := strings.Repeat("x", 32)
	s := &service{tenants: map[string]*nucleus.Client{"tenant-a": nil}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), timeout: time.Second, secrets: []string{"password", secret}}
	cases := []struct {
		claims neutronauth.Claims
		status int
	}{
		{neutronauth.Claims{"iss": "wrong", "aud": "neutron-data-reference-api", "sub": "demo", "tenant_id": "tenant-a"}, 401},
		{neutronauth.Claims{"iss": "neutron-data-reference", "aud": "wrong", "sub": "demo", "tenant_id": "tenant-a"}, 401},
		{neutronauth.Claims{"iss": "neutron-data-reference", "aud": "neutron-data-reference-api", "sub": "", "tenant_id": "tenant-a"}, 401},
		{neutronauth.Claims{"iss": "neutron-data-reference", "aud": "neutron-data-reference-api", "sub": "demo", "tenant_id": "tenant-c"}, 403},
	}
	for _, tc := range cases {
		token, err := neutronauth.GenerateToken(tc.claims, secret, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("GET", "/api/projects", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.app(secret).Handler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	safe := s.safe(errors.New("postgres://u:password@localhost/db " + secret))
	if strings.Contains(safe, "password") || strings.Contains(safe, secret) || strings.Contains(safe, "postgres://") {
		t.Fatal(safe)
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/documents", nil)
	s.wrap(func(*http.Request) (any, error) { return nil, unknownCommit(errors.New("native failure")) })(w, r)
	var problem map[string]any
	if json.Unmarshal(w.Body.Bytes(), &problem) != nil || w.Code != 503 || !strings.HasSuffix(problem["type"].(string), "unknown-outcome") {
		t.Fatal(w.Body.String())
	}
}

func TestStaticDiscoveryAndWireContract(t *testing.T) {
	s := &service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	var spec struct {
		OpenAPI    string                                `json:"openapi"`
		Paths      map[string]map[string]json.RawMessage `json:"paths"`
		Components struct {
			Schemas map[string]struct {
				Required   []string                   `json:"required"`
				Properties map[string]json.RawMessage `json:"properties"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(apiSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.OpenAPI != "3.1.0" {
		t.Fatal(spec.OpenAPI)
	}
	app := s.app(strings.Repeat("x", 32))
	for _, path := range []string{"/openapi.json", "/docs"} {
		w := httptest.NewRecorder()
		app.Handler().ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 {
			t.Fatal(path, w.Code)
		}
		if path == "/openapi.json" && (w.Header().Get("Content-Type") != "application/json" || w.Body.String() != string(apiSpec)) {
			t.Fatal("discovery differs from embedded spec")
		}
		if path == "/docs" && !strings.Contains(w.Body.String(), "/openapi.json") {
			t.Fatal("docs points elsewhere")
		}
	}
	for _, route := range app.Router().Routes() {
		if strings.HasPrefix(route.Pattern, "/api/") {
			if _, ok := spec.Paths[route.Pattern][strings.ToLower(route.Method)]; !ok {
				t.Fatalf("missing route %s %s", route.Method, route.Pattern)
			}
		}
	}
	samples := map[string]any{"Project": Project{}, "Document": Document{Amount: "9999999999999999999999.123456789012345678", Version: "9007199254740993", Payload: []byte{0, 255}, CreatedAt: "2026-09-30T22:34:56.123456Z"}, "Job": Job{}, "Result": Result{}, "Detail": Detail{}}
	for name, sample := range samples {
		raw, _ := json.Marshal(sample)
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		schema := spec.Components.Schemas[name]
		if len(fields) != len(schema.Properties) {
			t.Fatalf("%s wire field drift", name)
		}
		for _, field := range schema.Required {
			if _, ok := fields[field]; !ok {
				t.Fatalf("%s missing required %s", name, field)
			}
		}
		for field := range fields {
			if _, ok := schema.Properties[field]; !ok {
				t.Fatalf("%s unknown %s", name, field)
			}
		}
		if name == "Document" {
			for _, field := range []string{"amount", "version", "payload", "created_at"} {
				var v string
				if json.Unmarshal(fields[field], &v) != nil {
					t.Fatalf("%s must be string", field)
				}
			}
			if string(fields["note"]) != "null" {
				t.Fatal("note null lost")
			}
		}
	}
	for _, name := range []string{"CreateDocument", "UpdateNote"} {
		schema := spec.Components.Schemas[name]
		found := false
		for _, field := range schema.Required {
			found = found || field == "note"
		}
		if !found {
			t.Fatal("note presence unspecified")
		}
		var value map[string]any
		_ = json.Unmarshal(schema.Properties["note"], &value)
		types := value["type"].([]any)
		if len(types) != 2 || types[1] != "null" {
			t.Fatal("nullable note drift")
		}
	}
}
