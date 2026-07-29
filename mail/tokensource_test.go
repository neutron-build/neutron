package mail

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var testSecret = []byte("shared-secret-between-engine-and-app")

func TestSignedRequestRoundTrips(t *testing.T) {
	body, _ := json.Marshal(TokenRequest{Account: "acct-1", Timestamp: time.Now().Unix()})
	sig := SignTokenRequest(testSecret, body)

	req, err := VerifyTokenRequest(testSecret, body, sig, time.Minute)
	if err != nil {
		t.Fatalf("a request the engine signed did not verify: %v", err)
	}
	if req.Account != "acct-1" {
		t.Errorf("Account = %q, want acct-1", req.Account)
	}
}

func TestWrongSecretIsRejected(t *testing.T) {
	body, _ := json.Marshal(TokenRequest{Account: "acct-1", Timestamp: time.Now().Unix()})
	sig := SignTokenRequest([]byte("some other secret"), body)

	if _, err := VerifyTokenRequest(testSecret, body, sig, time.Minute); err == nil {
		t.Fatal("a request signed with the wrong secret verified")
	}
}

func TestTamperedBodyIsRejected(t *testing.T) {
	// The signature covers the account, so a captured request cannot be
	// edited to mint a token for somebody else.
	body, _ := json.Marshal(TokenRequest{Account: "victim", Timestamp: time.Now().Unix()})
	sig := SignTokenRequest(testSecret, body)

	tampered, _ := json.Marshal(TokenRequest{Account: "attacker", Timestamp: time.Now().Unix()})
	if _, err := VerifyTokenRequest(testSecret, tampered, sig, time.Minute); err == nil {
		t.Fatal("a request whose account was swapped after signing verified")
	}
}

func TestStaleRequestIsRejected(t *testing.T) {
	// A valid signature alone would let a captured request be replayed
	// forever. Bounding its age makes interception a brief window rather
	// than a permanent key.
	old := time.Now().Add(-10 * time.Minute).Unix()
	body, _ := json.Marshal(TokenRequest{Account: "acct-1", Timestamp: old})
	sig := SignTokenRequest(testSecret, body)

	_, err := VerifyTokenRequest(testSecret, body, sig, 30*time.Second)
	if err == nil {
		t.Fatal("a ten-minute-old request verified inside a thirty-second window")
	}
	if !strings.Contains(err.Error(), "window") {
		t.Errorf("err = %v, want it to name the age window", err)
	}
}

func TestFutureDatedRequestIsRejected(t *testing.T) {
	// Clock skew cuts both ways; a far-future timestamp must not buy an
	// indefinitely valid request.
	future := time.Now().Add(10 * time.Minute).Unix()
	body, _ := json.Marshal(TokenRequest{Account: "acct-1", Timestamp: future})
	sig := SignTokenRequest(testSecret, body)

	if _, err := VerifyTokenRequest(testSecret, body, sig, 30*time.Second); err == nil {
		t.Fatal("a request dated ten minutes in the future verified")
	}
}

func TestRequestWithNoAccountIsRejected(t *testing.T) {
	body, _ := json.Marshal(TokenRequest{Timestamp: time.Now().Unix()})
	sig := SignTokenRequest(testSecret, body)

	if _, err := VerifyTokenRequest(testSecret, body, sig, time.Minute); err == nil {
		t.Fatal("a request naming no account verified")
	}
}

func TestCallbackReturnsACredential(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)

		if _, err := VerifyTokenRequest(testSecret, body, r.Header.Get(HeaderTokenSignature), time.Minute); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(TokenResponse{
			Provider:    ProviderGmail,
			AccessToken: "ya29.fresh",
			Email:       "user@example.com",
		})
	}))
	defer srv.Close()

	ts := NewCallbackTokenSource(srv.URL, testSecret)
	cred, err := ts.Token(context.Background(), "acct-1")
	if err != nil {
		t.Fatal(err)
	}
	if cred.AccessToken != "ya29.fresh" {
		t.Errorf("AccessToken = %q", cred.AccessToken)
	}
	if cred.Provider != ProviderGmail {
		t.Errorf("Provider = %q", cred.Provider)
	}
}

func TestCallbackReportingReauthSurfacesAsReauthRequired(t *testing.T) {
	// The app knowing a grant is dead saves a pointless round trip to the
	// provider, and lets the scheduler stop retrying immediately.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(TokenResponse{NeedsReauth: true})
	}))
	defer srv.Close()

	ts := NewCallbackTokenSource(srv.URL, testSecret)
	_, err := ts.Token(context.Background(), "acct-1")
	if !errors.Is(err, ErrReauthRequired) {
		t.Errorf("err = %v, want ErrReauthRequired", err)
	}
}

func TestRejectedSignatureIsNotReportedAsReauth(t *testing.T) {
	// A misconfigured shared secret is a deployment fault. Reporting it as
	// "this account must reconnect" would march every user to a screen
	// that cannot fix anything.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	ts := NewCallbackTokenSource(srv.URL, testSecret)
	_, err := ts.Token(context.Background(), "acct-1")
	if err == nil {
		t.Fatal("expected an error")
	}
	if errors.Is(err, ErrReauthRequired) {
		t.Error("a rejected signature was reported as a user reauth requirement")
	}
	if !strings.Contains(err.Error(), "signature") {
		t.Errorf("err = %v, want it to name the signature problem", err)
	}
}

func TestCallbackRejectsAnUnusableCredential(t *testing.T) {
	// An IMAP credential with no host cannot connect. Failing here beats
	// failing deep inside the adapter with a worse message.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(TokenResponse{
			Provider:    ProviderIMAP,
			AccessToken: "tok",
		})
	}))
	defer srv.Close()

	ts := NewCallbackTokenSource(srv.URL, testSecret)
	if _, err := ts.Token(context.Background(), "acct-1"); err == nil {
		t.Fatal("an IMAP credential with no host was accepted")
	}
}

func TestNoCallbackConfiguredIsAClearError(t *testing.T) {
	ts := &CallbackTokenSource{}
	if _, err := ts.Token(context.Background(), "acct-1"); err == nil {
		t.Fatal("an unconfigured token source returned a credential")
	}
}

func TestStaticTokenSourceValidates(t *testing.T) {
	good := StaticTokenSource{Cred: Credential{
		Provider: ProviderIMAP, Password: "p", Host: "imap.x.com",
	}}
	if _, err := good.Token(context.Background(), "a"); err != nil {
		t.Errorf("a valid static credential was rejected: %v", err)
	}

	bad := StaticTokenSource{Cred: Credential{Provider: ProviderIMAP}}
	if _, err := bad.Token(context.Background(), "a"); err == nil {
		t.Error("a static credential with no secret was accepted")
	}
}

func TestSchedulerUsesTheTokenSourceForBackgroundSync(t *testing.T) {
	// The whole point of the callback: an account with no statically
	// configured adapter still syncs on a timer.
	store := newMemStore()
	acct := AccountID("oauth-acct")
	if err := store.PutAccount(context.Background(), &Account{
		ID: acct, Provider: ProviderGmail, Email: "u@x.com",
	}); err != nil {
		t.Fatal(err)
	}

	ad := &countingAdapter{scriptedAdapter: scriptedAdapter{boxes: []Mailbox{{ID: "INBOX"}}}}
	released := false

	s := NewScheduler(store, NewEngine(store, discardLogger()), nil, discardLogger())
	s.Jitter = false
	s.Tokens = StaticTokenSource{Cred: Credential{
		Provider: ProviderGmail, AccessToken: "tok",
	}}
	s.Resolve = func(context.Context, AccountID, Credential) (Adapter, func(), error) {
		return ad, func() { released = true }, nil
	}

	s.RunOnce(context.Background())

	if ad.count() == 0 {
		t.Error("the account never synced; the token callback path did not run")
	}
	if !released {
		t.Error("the per-run adapter was never released; its connection would leak")
	}
}

func TestSchedulerWithoutATokenSourceSkipsQuietly(t *testing.T) {
	// Not every account has a credential path, and that is ordinary. It
	// must not be logged as a failure on every single tick.
	store := newMemStore()
	if err := store.PutAccount(context.Background(), &Account{
		ID: "a1", Provider: ProviderGmail, Email: "u@x.com",
	}); err != nil {
		t.Fatal(err)
	}

	s := NewScheduler(store, NewEngine(store, discardLogger()), nil, discardLogger())
	s.Jitter = false
	// No Tokens, no Resolve, no static adapters.
	s.RunOnce(context.Background())

	a, err := store.Account(context.Background(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	if a.NeedsReauth {
		t.Error("an account with no credential path was marked as needing reauthentication")
	}
}
