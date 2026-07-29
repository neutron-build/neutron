package mail

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Credential is how one request reaches a provider.
//
// The engine deliberately stores none of this. Both consuming products run
// Better Auth, which already holds provider access and refresh tokens and
// already refreshes them; a second credential store here would mean a second
// place to secure, a second refresh implementation to keep correct, and a
// second blast radius. The caller fetches a fresh token and passes it in.
//
// The exception is a self-hosted single-mailbox deployment, where there is no
// Better Auth and an app password in the environment is the whole story. That
// path uses a statically configured adapter instead.
type Credential struct {
	Provider Provider
	Email    string

	// AccessToken is an OAuth bearer token, already refreshed by the
	// caller. The engine never refreshes it: it has no refresh token and
	// no client secret, which is the point.
	AccessToken string

	// Password is an app password, for IMAP without OAuth.
	Password string

	// Host and Port address an IMAP or JMAP server. Unused for Gmail and
	// Graph, whose endpoints are fixed.
	Host string
	Port int
}

// Zero reports whether no credential was supplied.
func (c Credential) Zero() bool {
	return c.AccessToken == "" && c.Password == ""
}

// Validate checks that the credential carries enough to connect.
func (c Credential) Validate() error {
	if c.Provider == "" {
		return fmt.Errorf("mail: credential has no provider")
	}
	if c.Zero() {
		return fmt.Errorf("mail: credential for %s carries neither a token nor a password", c.Provider)
	}
	switch c.Provider {
	case ProviderIMAP, ProviderJMAP:
		if c.Host == "" {
			return fmt.Errorf("mail: %s credential has no host", c.Provider)
		}
	case ProviderGmail, ProviderGraph:
		if c.AccessToken == "" {
			return fmt.Errorf("mail: %s requires an OAuth access token", c.Provider)
		}
	default:
		return fmt.Errorf("mail: unknown provider %q", c.Provider)
	}
	return nil
}

// Credential headers.
//
// These carry a live access token, so they must never be logged. Request
// logging middleware in front of this service has to redact them by name.
const (
	HeaderProvider = "X-Mail-Provider"
	HeaderToken    = "X-Mail-Token"
	HeaderPassword = "X-Mail-Password"
	HeaderEmail    = "X-Mail-Email"
	HeaderHost     = "X-Mail-Host"
	HeaderPort     = "X-Mail-Port"
)

// RedactedHeaders lists the headers that carry secrets, for log redaction.
var RedactedHeaders = []string{HeaderToken, HeaderPassword}

// CredentialFromRequest reads a credential from request headers.
//
// Returns a zero Credential and no error when none is present: a deployment
// with statically configured adapters serves requests that carry no
// credential at all, and that is not a failure.
func CredentialFromRequest(r *http.Request) (Credential, error) {
	provider := strings.TrimSpace(r.Header.Get(HeaderProvider))
	token := strings.TrimSpace(r.Header.Get(HeaderToken))
	password := r.Header.Get(HeaderPassword)

	if provider == "" && token == "" && password == "" {
		return Credential{}, nil
	}

	cred := Credential{
		Provider:    Provider(strings.ToLower(provider)),
		Email:       strings.TrimSpace(r.Header.Get(HeaderEmail)),
		AccessToken: token,
		Password:    password,
		Host:        strings.TrimSpace(r.Header.Get(HeaderHost)),
	}
	if p := r.Header.Get(HeaderPort); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil {
			return Credential{}, fmt.Errorf("mail: bad %s header: %w", HeaderPort, err)
		}
		cred.Port = n
	}

	if err := cred.Validate(); err != nil {
		return Credential{}, err
	}
	return cred, nil
}

// Apply writes the credential onto an outbound request, for clients.
func (c Credential) Apply(r *http.Request) {
	r.Header.Set(HeaderProvider, string(c.Provider))
	if c.AccessToken != "" {
		r.Header.Set(HeaderToken, c.AccessToken)
	}
	if c.Password != "" {
		r.Header.Set(HeaderPassword, c.Password)
	}
	if c.Email != "" {
		r.Header.Set(HeaderEmail, c.Email)
	}
	if c.Host != "" {
		r.Header.Set(HeaderHost, c.Host)
	}
	if c.Port != 0 {
		r.Header.Set(HeaderPort, strconv.Itoa(c.Port))
	}
}

// Resolver builds an adapter for one request.
//
// The returned release function is called when the request is done. For a
// per-request adapter that owns a connection this closes it; for a pooled or
// statically configured adapter it does nothing.
type Resolver func(ctx context.Context, acct AccountID, cred Credential) (ad Adapter, release func(), err error)

// StaticResolver adapts a fixed account-to-adapter mapping to the Resolver
// shape, for deployments that configure their adapters up front.
func StaticResolver(lookup func(AccountID) (Adapter, bool)) Resolver {
	return func(_ context.Context, acct AccountID, _ Credential) (Adapter, func(), error) {
		ad, ok := lookup(acct)
		if !ok {
			return nil, nil, fmt.Errorf("mail: no adapter configured for account %s", acct)
		}
		// Statically configured adapters outlive the request.
		return ad, func() {}, nil
	}
}
