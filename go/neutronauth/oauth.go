package neutronauth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/neutron-build/neutron/go/neutron"
)

// ---------------------------------------------------------------------------
// OAuthUser — normalized user information from any provider
// ---------------------------------------------------------------------------

// OAuthUser represents the normalized user information returned by a
// provider's userinfo endpoint.
type OAuthUser struct {
	// ID is the provider-assigned user identifier (stringified for cross-provider compat).
	ID string `json:"id"`
	// Email is the user's primary email address, if available.
	Email string `json:"email,omitempty"`
	// Name is the display name or username.
	Name string `json:"name,omitempty"`
	// AvatarURL is a link to the user's profile picture.
	AvatarURL string `json:"avatar_url,omitempty"`
	// Provider identifies which OAuth provider supplied this user.
	Provider string `json:"provider"`
	// AccessToken is the bearer token for calling provider APIs on behalf of the user.
	AccessToken string `json:"-"`
	// Raw holds the complete, unprocessed JSON from the userinfo endpoint.
	Raw map[string]any `json:"raw,omitempty"`
}

// ---------------------------------------------------------------------------
// OAuthProvider — configuration for an OAuth2 authorization code flow
// ---------------------------------------------------------------------------

// OAuthProvider holds all configuration for a single OAuth2 provider.
type OAuthProvider struct {
	// ProviderName identifies this provider (e.g. "github", "google").
	ProviderName string
	// ClientID is the application's OAuth client identifier.
	ClientID string
	// ClientSecret is the application's OAuth client secret.
	ClientSecret string
	// RedirectURL is the registered callback URI.
	RedirectURL string
	// AuthURL is the provider's authorization endpoint.
	AuthURL string
	// TokenURL is the provider's token endpoint.
	TokenURL string
	// UserInfoURL is the provider's userinfo endpoint (optional for some OIDC flows).
	UserInfoURL string
	// Scopes are the OAuth scopes to request.
	Scopes []string
	// Secret is the HMAC key for signing the anti-CSRF state cookie (at least 32 bytes).
	Secret []byte
}

// oauthStateCookie is the cookie name used to store OAuth flow state.
const oauthStateCookie = "__oauth_state"

// ---------------------------------------------------------------------------
// Pre-configured providers
// ---------------------------------------------------------------------------

// GitHubProvider returns an OAuthProvider pre-configured for GitHub OAuth.
// Default scopes: read:user, user:email.
func GitHubProvider(clientID, clientSecret, redirectURL string, stateSecret []byte) *OAuthProvider {
	return &OAuthProvider{
		ProviderName: "github",
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURL,
		AuthURL:      "https://github.com/login/oauth/authorize",
		TokenURL:     "https://github.com/login/oauth/access_token",
		UserInfoURL:  "https://api.github.com/user",
		Scopes:       []string{"read:user", "user:email"},
		Secret:       stateSecret,
	}
}

// GoogleProvider returns an OAuthProvider pre-configured for Google OAuth / OIDC.
// Default scopes: openid, profile, email.
func GoogleProvider(clientID, clientSecret, redirectURL string, stateSecret []byte) *OAuthProvider {
	return &OAuthProvider{
		ProviderName: "google",
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURL,
		AuthURL:      "https://accounts.google.com/o/oauth2/v2/auth",
		TokenURL:     "https://oauth2.googleapis.com/token",
		UserInfoURL:  "https://openidconnect.googleapis.com/v1/userinfo",
		Scopes:       []string{"openid", "profile", "email"},
		Secret:       stateSecret,
	}
}

// DiscordProvider returns an OAuthProvider pre-configured for Discord OAuth.
// Default scopes: identify, email.
func DiscordProvider(clientID, clientSecret, redirectURL string, stateSecret []byte) *OAuthProvider {
	return &OAuthProvider{
		ProviderName: "discord",
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURL,
		AuthURL:      "https://discord.com/api/oauth2/authorize",
		TokenURL:     "https://discord.com/api/oauth2/token",
		UserInfoURL:  "https://discord.com/api/users/@me",
		Scopes:       []string{"identify", "email"},
		Secret:       stateSecret,
	}
}

// ---------------------------------------------------------------------------
// PKCE (RFC 7636) — S256 method
// ---------------------------------------------------------------------------

// pkceChallenge holds a PKCE verifier/challenge pair.
type pkceChallenge struct {
	Verifier  string
	Challenge string
}

// newPKCEChallenge generates a random 32-byte verifier and derives the
// S256 challenge (BASE64URL-NoPad(SHA256(verifier))).
func newPKCEChallenge() pkceChallenge {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	verifier := base64.RawURLEncoding.EncodeToString(b)
	return pkceChallenge{
		Verifier:  verifier,
		Challenge: derivePKCEChallenge(verifier),
	}
}

func derivePKCEChallenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

// ---------------------------------------------------------------------------
// State cookie — HMAC-signed, carries state + PKCE verifier + timestamp
// ---------------------------------------------------------------------------

// encodeStateCookie produces a signed cookie value of the form:
//
//	state|verifier|timestamp|hmac(state|verifier|timestamp)
func encodeStateCookie(state, verifier string, secret []byte) string {
	ts := fmt.Sprintf("%d", time.Now().Unix())
	payload := state + "|" + verifier + "|" + ts
	sig := hmacSign(payload, secret)
	return payload + "|" + sig
}

// decodeStateCookie verifies the HMAC and checks the timestamp is within
// maxAge seconds.  Returns (state, verifier, ok).
func decodeStateCookie(cookie string, secret []byte, maxAge time.Duration) (string, string, bool) {
	parts := strings.SplitN(cookie, "|", 4)
	if len(parts) != 4 {
		return "", "", false
	}
	state, verifier, tsStr, sig := parts[0], parts[1], parts[2], parts[3]

	payload := state + "|" + verifier + "|" + tsStr
	expected := hmacSign(payload, secret)
	if subtle.ConstantTimeCompare([]byte(sig), []byte(expected)) != 1 {
		return "", "", false
	}

	// Verify timestamp freshness
	var ts int64
	if _, err := fmt.Sscanf(tsStr, "%d", &ts); err != nil {
		return "", "", false
	}
	if time.Since(time.Unix(ts, 0)) > maxAge {
		return "", "", false
	}

	return state, verifier, true
}

func hmacSign(payload string, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// ---------------------------------------------------------------------------
// Authorization URL builder
// ---------------------------------------------------------------------------

func (p *OAuthProvider) authorizationURL(state, challenge string) string {
	// Parse and rebuild rather than appending an unconditional "?" (GO-19):
	// an authorization endpoint that already carries a query string would
	// otherwise produce a malformed `...?a=b?c=d` URL.
	base, err := url.Parse(p.AuthURL)
	if err != nil {
		// Configuration error; the malformed URL will fail loudly at redirect.
		return p.AuthURL + "?" + state
	}
	v := base.Query()
	v.Set("response_type", "code")
	v.Set("client_id", p.ClientID)
	v.Set("redirect_uri", p.RedirectURL)
	v.Set("scope", strings.Join(p.Scopes, " "))
	v.Set("state", state)
	v.Set("code_challenge", challenge)
	v.Set("code_challenge_method", "S256")
	base.RawQuery = v.Encode()
	return base.String()
}

// ---------------------------------------------------------------------------
// Token exchange
// ---------------------------------------------------------------------------

// tokenResponse is the response from the provider's token endpoint.
type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	IDToken      string `json:"id_token,omitempty"`
	Scope        string `json:"scope,omitempty"`
}

// oauthHTTPClient is the bounded client for all provider calls (GO-19): the
// default client has no timeout, so a slow provider held request resources
// indefinitely. Owned here rather than mutating http.DefaultClient.
var oauthHTTPClient = &http.Client{Timeout: 10 * time.Second}

// validate checks the provider configuration at flow start (GO-18/GO-19): a
// provider with no UserInfoURL cannot establish a verified user identity —
// the removed fallback DERIVED one from an access-token prefix, which
// collided across users whose tokens shared a header prefix. A state-signing
// secret shorter than 32 bytes is rejected too.
func (p *OAuthProvider) validate() error {
	if p.UserInfoURL == "" {
		return fmt.Errorf(
			"provider %q has no UserInfoURL: a verified userinfo endpoint is required "+
				"to establish user identity (token-derived identities are not)",
			p.ProviderName,
		)
	}
	if len(p.Secret) < 32 {
		return fmt.Errorf("provider %q: state-signing Secret must be at least 32 bytes of high-entropy key material", p.ProviderName)
	}
	return nil
}

// exchangeCode sends the authorization code to the token endpoint and returns tokens.
func (p *OAuthProvider) exchangeCode(ctx context.Context, code, codeVerifier string) (*tokenResponse, error) {
	data := url.Values{}
	data.Set("grant_type", "authorization_code")
	data.Set("code", code)
	data.Set("redirect_uri", p.RedirectURL)
	data.Set("client_id", p.ClientID)
	data.Set("client_secret", p.ClientSecret)
	data.Set("code_verifier", codeVerifier)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("neutronauth: build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := oauthHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: token exchange request: %w", err)
	}
	body, err := readProviderBody(resp.Body, 1<<20)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: read token response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("neutronauth: token endpoint returned %d", resp.StatusCode)
	}

	// Some providers (GitHub) may return form-encoded instead of JSON. Trim
	// leading whitespace before classifying (GO-19): the old first-byte test
	// misparsed whitespace-prefixed JSON as form data.
	trimmed := bytes.TrimSpace(body)
	var tok tokenResponse
	if len(trimmed) > 0 && trimmed[0] == '{' {
		if err := json.Unmarshal(trimmed, &tok); err != nil {
			return nil, fmt.Errorf("neutronauth: unmarshal token response: %w", err)
		}
	} else {
		parsed, parseErr := url.ParseQuery(string(trimmed))
		if parseErr != nil {
			return nil, fmt.Errorf("neutronauth: parse form token response: %w", parseErr)
		}
		tok.AccessToken = parsed.Get("access_token")
		tok.TokenType = parsed.Get("token_type")
		tok.RefreshToken = parsed.Get("refresh_token")
		tok.Scope = parsed.Get("scope")
	}
	// An empty access token is a failed exchange, whatever the status said.
	if tok.AccessToken == "" {
		return nil, fmt.Errorf("neutronauth: missing access_token in response")
	}

	return &tok, nil
}

// readProviderBody reads at most limit bytes and rejects truncation rather
// than accepting a silently short body (GO-19).
func readProviderBody(body io.ReadCloser, limit int64) ([]byte, error) {
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("provider response too large")
	}
	return bytes.TrimSpace(data), nil
}

// ---------------------------------------------------------------------------
// Fetch user info
// ---------------------------------------------------------------------------

// fetchUserInfo calls the provider's userinfo endpoint and normalizes the result.
//
// Numeric subject ids keep full precision (GO-18): the JSON decodes with
// UseNumber, so a 64-bit provider id like 9007199254740993 survives instead
// of collapsing onto its float64 neighbor and merging two users.
func (p *OAuthProvider) fetchUserInfo(ctx context.Context, accessToken string) (*OAuthUser, error) {
	// No prefix-derived identities (GO-18): identity without a userinfo
	// endpoint is an explicit configuration error.
	if p.UserInfoURL == "" {
		return nil, fmt.Errorf(
			"neutronauth: provider %q has no userinfo endpoint; a verified subject is required",
			p.ProviderName,
		)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.UserInfoURL, nil)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: build userinfo request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := oauthHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: userinfo request: %w", err)
	}
	body, err := readProviderBody(resp.Body, 1<<20)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: read userinfo response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("neutronauth: userinfo endpoint returned %d", resp.StatusCode)
	}

	var raw map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("neutronauth: unmarshal userinfo: %w", err)
	}

	user := normalizeUser(raw, p.ProviderName, accessToken)
	if user.ID == "" {
		return nil, fmt.Errorf("neutronauth: provider %q returned no stable subject (id/sub)", p.ProviderName)
	}
	return user, nil
}

// normalizeUser maps provider-specific JSON fields to the unified OAuthUser struct.
func normalizeUser(raw map[string]any, provider, accessToken string) *OAuthUser {
	user := &OAuthUser{
		Provider:    provider,
		AccessToken: accessToken,
		Raw:         raw,
	}

	// ID: try "id" then "sub" (OIDC)
	switch v := raw["id"].(type) {
	case string:
		user.ID = v
	case float64:
		user.ID = fmt.Sprintf("%.0f", v)
	case json.Number:
		user.ID = v.String()
	default:
		if sub, ok := raw["sub"].(string); ok {
			user.ID = sub
		}
	}

	// Email
	if email, ok := raw["email"].(string); ok {
		user.Email = email
	}

	// Name: try "name", "login" (GitHub), "username" (Discord)
	for _, key := range []string{"name", "login", "username"} {
		if name, ok := raw[key].(string); ok && name != "" {
			user.Name = name
			break
		}
	}

	// Avatar: try "avatar_url" (GitHub), "picture" (Google OIDC)
	for _, key := range []string{"avatar_url", "picture"} {
		if avatar, ok := raw[key].(string); ok && avatar != "" {
			user.AvatarURL = avatar
			break
		}
	}

	return user
}

// ---------------------------------------------------------------------------
// OAuthRedirectHandler — starts the authorization code flow
// ---------------------------------------------------------------------------

// OAuthRedirectHandler returns an http.Handler that initiates the OAuth2
// authorization code flow with PKCE.
//
// When a browser hits this handler it:
//  1. Generates a PKCE challenge (S256) and a random anti-CSRF state.
//  2. Stores state + PKCE verifier in a signed HttpOnly cookie.
//  3. Redirects the browser to the provider's authorization URL.
func OAuthRedirectHandler(provider *OAuthProvider) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := provider.validate(); err != nil {
			log.Printf("[neutronauth] invalid OAuth provider config: %v", err)
			neutron.WriteError(w, r, neutron.ErrInternal("OAuth provider is misconfigured"))
			return
		}
		pkce := newPKCEChallenge()
		state := generateOAuthState()

		cookieVal := encodeStateCookie(state, pkce.Verifier, provider.Secret)
		authURL := provider.authorizationURL(state, pkce.Challenge)

		http.SetCookie(w, &http.Cookie{
			Name:     oauthStateCookie,
			Value:    cookieVal,
			Path:     "/",
			MaxAge:   600, // 10 minutes to complete the login flow
			HttpOnly: true,
			Secure:   true,
			SameSite: http.SameSiteLaxMode,
		})

		http.Redirect(w, r, authURL, http.StatusFound)
	})
}

// ---------------------------------------------------------------------------
// OAuthCallbackHandler — completes the authorization code flow
// ---------------------------------------------------------------------------

// OAuthCallbackHandler returns an http.Handler that completes the OAuth2
// authorization code flow.
//
// On success, the onSuccess callback is called with the request and the
// normalized OAuthUser.  The callback controls the final response (e.g.
// create a session, set a cookie, redirect to the app).
//
// On failure, an RFC 7807 error is returned.
func OAuthCallbackHandler(provider *OAuthProvider, onSuccess func(w http.ResponseWriter, r *http.Request, user OAuthUser)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 1. Extract code and state from the query string.
		code := r.URL.Query().Get("code")
		if code == "" {
			neutron.WriteError(w, r, neutron.ErrBadRequest("missing authorization code"))
			return
		}
		state := r.URL.Query().Get("state")
		if state == "" {
			neutron.WriteError(w, r, neutron.ErrBadRequest("missing state parameter"))
			return
		}

		// 2. Read and verify the signed state cookie.
		cookie, err := r.Cookie(oauthStateCookie)
		if err != nil {
			neutron.WriteError(w, r, neutron.ErrForbidden("missing OAuth state cookie"))
			return
		}

		storedState, verifier, ok := decodeStateCookie(cookie.Value, provider.Secret, 10*time.Minute)
		if !ok {
			neutron.WriteError(w, r, neutron.ErrForbidden("invalid or expired OAuth state cookie"))
			return
		}

		// 3. Verify anti-CSRF state matches.
		if subtle.ConstantTimeCompare([]byte(state), []byte(storedState)) != 1 {
			neutron.WriteError(w, r, neutron.ErrForbidden("CSRF state mismatch"))
			return
		}

		// 4. Exchange the authorization code for tokens. The outbound call
		// carries the inbound request's context, so a client disconnect
		// cancels it (GO-19).
		tokens, err := provider.exchangeCode(r.Context(), code, verifier)
		if err != nil {
			log.Printf("[neutronauth] token exchange failed: %v", err)
			neutron.WriteError(w, r, neutron.ErrInternal("Authentication failed"))
			return
		}

		// 5. Fetch user information from the provider.
		user, err := provider.fetchUserInfo(r.Context(), tokens.AccessToken)
		if err != nil {
			log.Printf("[neutronauth] userinfo fetch failed: %v", err)
			neutron.WriteError(w, r, neutron.ErrInternal("Authentication failed"))
			return
		}

		// 6. Clear the state cookie.
		http.SetCookie(w, &http.Cookie{
			Name:     oauthStateCookie,
			Value:    "",
			Path:     "/",
			MaxAge:   -1,
			HttpOnly: true,
			Secure:   true,
			SameSite: http.SameSiteLaxMode,
		})

		// 7. Call the success handler.
		onSuccess(w, r, *user)
	})
}

// generateOAuthState returns a cryptographically random 32-byte hex-encoded string.
func generateOAuthState() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
