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
