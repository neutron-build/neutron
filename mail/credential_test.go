package mail

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCredentialFromRequestReadsHeaders(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(HeaderProvider, "gmail")
	r.Header.Set(HeaderToken, "ya29.token")
	r.Header.Set(HeaderEmail, "user@example.com")

	cred, err := CredentialFromRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	if cred.Provider != ProviderGmail {
		t.Errorf("Provider = %q, want gmail", cred.Provider)
	}
	if cred.AccessToken != "ya29.token" {
		t.Errorf("AccessToken = %q", cred.AccessToken)
	}
	if cred.Email != "user@example.com" {
		t.Errorf("Email = %q", cred.Email)
	}
}

func TestAbsentCredentialIsNotAnError(t *testing.T) {
	// A deployment with statically configured adapters serves requests
	// carrying no credential at all, and that is not a failure.
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	cred, err := CredentialFromRequest(r)
	if err != nil {
		t.Fatalf("a request with no credential errored: %v", err)
	}
	if !cred.Zero() {
		t.Error("expected a zero credential")
	}
}

func TestCredentialValidation(t *testing.T) {
	tests := []struct {
		name string
		cred Credential
		ok   bool
	}{
		{"gmail with token", Credential{Provider: ProviderGmail, AccessToken: "t"}, true},
		{"graph with token", Credential{Provider: ProviderGraph, AccessToken: "t"}, true},
		{"imap with password and host", Credential{Provider: ProviderIMAP, Password: "p", Host: "mail.x.com"}, true},
		{"jmap with token and host", Credential{Provider: ProviderJMAP, AccessToken: "t", Host: "x.com"}, true},

		{"no provider", Credential{AccessToken: "t"}, false},
		{"no secret", Credential{Provider: ProviderGmail}, false},
		{"imap without host", Credential{Provider: ProviderIMAP, Password: "p"}, false},
		{"gmail with only a password", Credential{Provider: ProviderGmail, Password: "p"}, false},
		{"unknown provider", Credential{Provider: "pigeon", AccessToken: "t"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cred.Validate()
			if tt.ok && err != nil {
				t.Errorf("Validate() = %v, want nil", err)
			}
			if !tt.ok && err == nil {
				t.Error("Validate() = nil, want an error")
			}
		})
	}
}

func TestGmailRejectsAPasswordOnlyCredential(t *testing.T) {
	// Gmail retired basic auth for its API entirely. Accepting a password
	// here would produce a confusing failure deep inside the adapter
	// instead of a clear one at the edge.
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(HeaderProvider, "gmail")
	r.Header.Set(HeaderPassword, "app-password")

	if _, err := CredentialFromRequest(r); err == nil {
		t.Error("a password-only Gmail credential was accepted")
	}
}

func TestCredentialRoundTripsThroughHeaders(t *testing.T) {
	original := Credential{
		Provider: ProviderIMAP,
		Email:    "user@example.com",
		Password: "app pass with spaces",
		Host:     "imap.example.com",
		Port:     993,
	}

	out := httptest.NewRequest(http.MethodGet, "/", nil)
	original.Apply(out)

	got, err := CredentialFromRequest(out)
	if err != nil {
		t.Fatal(err)
	}
	if got != original {
		t.Errorf("round trip changed the credential:\n got %+v\nwant %+v", got, original)
	}
}

func TestBadPortIsRejected(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(HeaderProvider, "imap")
	r.Header.Set(HeaderPassword, "p")
	r.Header.Set(HeaderHost, "x.com")
	r.Header.Set(HeaderPort, "not-a-number")

	if _, err := CredentialFromRequest(r); err == nil {
		t.Error("a non-numeric port was accepted")
	}
}

func TestSecretHeadersAreNamedForRedaction(t *testing.T) {
	// Logging middleware in front of this service redacts by header name,
	// so the list has to actually cover the headers that carry secrets.
	secrets := map[string]bool{HeaderToken: true, HeaderPassword: true}
	for _, h := range RedactedHeaders {
		delete(secrets, h)
	}
	if len(secrets) != 0 {
		t.Errorf("secret-carrying headers missing from RedactedHeaders: %v", secrets)
	}
	for _, h := range RedactedHeaders {
		if !strings.HasPrefix(h, "X-Mail-") {
			t.Errorf("RedactedHeaders contains an unexpected entry: %q", h)
		}
	}
}

func TestProviderIsCaseInsensitive(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(HeaderProvider, "GMAIL")
	r.Header.Set(HeaderToken, "t")

	cred, err := CredentialFromRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	if cred.Provider != ProviderGmail {
		t.Errorf("Provider = %q, want it normalised to gmail", cred.Provider)
	}
}

func TestServiceWithoutResolverRefusesProviderCalls(t *testing.T) {
	// Reads against the mirror still work; anything needing the provider
	// reports why rather than panicking on a nil resolver.
	store := newMemStore()
	svc := NewService(store, NewEngine(store, discardLogger()))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/accounts/a1/sync", nil)
	svc.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "resolver") {
		t.Errorf("body = %q, want it to name the missing resolver", rec.Body.String())
	}
}

func TestServiceResolvesPerRequestCredential(t *testing.T) {
	store := newMemStore()
	if err := store.PutAccount(t.Context(), &Account{ID: "a1", Provider: ProviderIMAP}); err != nil {
		t.Fatal(err)
	}

	var seen Credential
	released := false
	svc := NewService(store, NewEngine(store, discardLogger()))
	svc.Resolve = func(_ context.Context, _ AccountID, cred Credential) (Adapter, func(), error) {
		seen = cred
		return &scriptedAdapter{boxes: []Mailbox{{ID: "INBOX"}}}, func() { released = true }, nil
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/accounts/a1/sync", nil)
	req.Header.Set(HeaderProvider, "imap")
	req.Header.Set(HeaderPassword, "secret")
	req.Header.Set(HeaderHost, "imap.example.com")
	svc.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if seen.Password != "secret" {
		t.Errorf("resolver saw %+v, want the request's credential", seen)
	}
	if !released {
		t.Error("the per-request adapter was never released; its connection would leak")
	}
}
