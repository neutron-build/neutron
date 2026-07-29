package mail

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// A background sync has no request to carry a credential on, which is the one
// hole in per-request credentials: the engine can serve a user who is looking
// at their mail, and can do nothing at all when nobody is.
//
// Rather than reintroduce a credential store here — a second place to secure
// and a second refresh implementation to keep correct — the engine calls back
// to the application that already holds the tokens and asks for a fresh one.
// What it stores is a URL and a shared secret, neither of which reads anyone's
// mail on its own.

// TokenSource mints a credential for an account with no request in flight.
type TokenSource interface {
	Token(ctx context.Context, acct AccountID) (Credential, error)
}

// CallbackTokenSource fetches credentials from an application endpoint.
//
// The application resolves the account to a user and returns a freshly
// refreshed provider token. Refresh therefore stays in exactly one place —
// wherever the app keeps its OAuth state — and this process never holds a
// refresh token or a client secret.
type CallbackTokenSource struct {
	// URL is the application's token endpoint.
	URL string

	// Secret signs each request so the endpoint can tell the engine from
	// anything else that discovers the URL. It is not a bearer token: the
	// signature covers the account and a timestamp, so a captured request
	// cannot be replayed for a different account or indefinitely.
	Secret []byte

	// Skew bounds how old a signed request may be. Defaults to 30s.
	Skew time.Duration

	HTTP *http.Client
}

// NewCallbackTokenSource builds a token source over an application endpoint.
func NewCallbackTokenSource(url string, secret []byte) *CallbackTokenSource {
	return &CallbackTokenSource{
		URL:    url,
		Secret: secret,
		Skew:   30 * time.Second,
		HTTP:   &http.Client{Timeout: 15 * time.Second},
	}
}

// TokenRequest is what the engine sends.
type TokenRequest struct {
	Account   AccountID `json:"account"`
	Timestamp int64     `json:"timestamp"`
}

// TokenResponse is what the application returns.
//
// Deliberately shaped like Credential minus anything long-lived: there is no
// field for a refresh token because the engine has no use for one and no way
// to keep it safe that the application does not already have.
type TokenResponse struct {
	Provider    Provider `json:"provider"`
	AccessToken string   `json:"access_token"`
	Email       string   `json:"email,omitempty"`
	Host        string   `json:"host,omitempty"`
	Port        int      `json:"port,omitempty"`

	// NeedsReauth lets the application say "this user must reconnect"
	// without the engine having to infer it from a failed sync. Acting on
	// it immediately saves a pointless round trip to the provider and, at
	// scale, stops a revoked account being retried forever.
	NeedsReauth bool `json:"needs_reauth,omitempty"`
}

// Token fetches a credential for one account.
func (s *CallbackTokenSource) Token(ctx context.Context, acct AccountID) (Credential, error) {
	if s.URL == "" {
		return Credential{}, fmt.Errorf("mail: no token callback configured")
	}

	body, err := json.Marshal(TokenRequest{
		Account:   acct,
		Timestamp: time.Now().Unix(),
	})
	if err != nil {
		return Credential{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
	if err != nil {
		return Credential{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderTokenSignature, SignTokenRequest(s.Secret, body))

	client := s.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return Credential{}, fmt.Errorf("mail: token callback: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		// The callback rejected the signature. That is a deployment fault,
		// not a user one, and must not be reported as "this account needs
		// to reconnect" — it would send every user to a reauth screen that
		// cannot fix anything.
		return Credential{}, fmt.Errorf("mail: token callback rejected the engine's signature")
	case http.StatusNotFound:
		return Credential{}, fmt.Errorf("mail: %w: account %s at the token callback", ErrNotFound, acct)
	default:
		return Credential{}, fmt.Errorf("mail: token callback: unexpected status %d", resp.StatusCode)
	}

	var out TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Credential{}, fmt.Errorf("mail: decode token callback: %w", err)
	}
	if out.NeedsReauth {
		return Credential{}, fmt.Errorf("%w: account %s", ErrReauthRequired, acct)
	}
	if out.AccessToken == "" {
		return Credential{}, fmt.Errorf("mail: token callback returned no access token for %s", acct)
	}

	cred := Credential{
		Provider:    out.Provider,
		AccessToken: out.AccessToken,
		Email:       out.Email,
		Host:        out.Host,
		Port:        out.Port,
	}
	if err := cred.Validate(); err != nil {
		return Credential{}, err
	}
	return cred, nil
}

// HeaderTokenSignature carries the HMAC over a token-callback request.
const HeaderTokenSignature = "X-Mail-Signature"

// SignTokenRequest returns the hex HMAC-SHA256 of body under secret.
func SignTokenRequest(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyTokenRequest checks a signed token-callback request.
//
// Provided here so both the engine and the applications that answer it agree
// on the scheme byte for byte — a signature verified by a slightly different
// implementation is a signature that fails at the worst possible moment.
//
// Comparison is constant-time: a byte-by-byte compare leaks how much of a
// forged signature was correct, which is enough to reconstruct one.
func VerifyTokenRequest(secret, body []byte, signature string, skew time.Duration) (TokenRequest, error) {
	want := SignTokenRequest(secret, body)
	if !hmac.Equal([]byte(want), []byte(signature)) {
		return TokenRequest{}, fmt.Errorf("mail: bad signature")
	}

	var req TokenRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return TokenRequest{}, fmt.Errorf("mail: malformed token request: %w", err)
	}

	if skew <= 0 {
		skew = 30 * time.Second
	}
	age := time.Since(time.Unix(req.Timestamp, 0))
	if age < 0 {
		age = -age
	}
	// The signature alone would let a captured request be replayed forever.
	// Bounding its age is what makes interception a brief window rather
	// than a permanent key.
	if age > skew {
		return TokenRequest{}, fmt.Errorf("mail: token request is %s old, outside the %s window",
			age.Round(time.Second), skew)
	}
	if req.Account == "" {
		return TokenRequest{}, fmt.Errorf("mail: token request names no account")
	}
	return req, nil
}

// StaticTokenSource returns a fixed credential. For self-hosted deployments
// holding their own app password, and for tests.
type StaticTokenSource struct{ Cred Credential }

func (s StaticTokenSource) Token(context.Context, AccountID) (Credential, error) {
	if err := s.Cred.Validate(); err != nil {
		return Credential{}, err
	}
	return s.Cred, nil
}
