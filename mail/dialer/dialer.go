// Package dialer builds provider adapters from a per-request credential.
//
// It lives outside the mail package because every adapter imports mail, so
// mail cannot import them back. This is the one place that knows all four
// providers exist; everything above it works through the Adapter interface.
package dialer

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/neutron-build/neutron/mail"
	"github.com/neutron-build/neutron/mail/gmail"
	"github.com/neutron-build/neutron/mail/graph"
	"github.com/neutron-build/neutron/mail/imap"
	"github.com/neutron-build/neutron/mail/jmap"
	"google.golang.org/api/option"
)

// New returns a Resolver that dials a provider per request.
//
// Each call opens a connection and the returned release closes it. That is
// the correct trade for OAuth accounts: the token arrives with the request
// and authorises that request, so a pooled connection would outlive the
// credential that permitted it.
func New() mail.Resolver { return NewWithGmailRefresh(nil) }

// GmailRefresh renews a rejected access token using the application-owned
// credential store. It must honor ctx and must not create new grants.
type GmailRefresh func(context.Context, mail.AccountID, mail.Credential) (mail.Credential, error)

// NewWithGmailRefresh permits one refresh/retry of a rejected Gmail GET.
// Mutations never use this recovery path.
func NewWithGmailRefresh(refresh GmailRefresh) mail.Resolver {
	return func(ctx context.Context, acct mail.AccountID, cred mail.Credential) (mail.Adapter, func(), error) {
		if cred.Zero() {
			return nil, nil, fmt.Errorf("mail: no credential supplied for account %s", acct)
		}
		if err := cred.Validate(); err != nil {
			return nil, nil, err
		}

		switch cred.Provider {
		case mail.ProviderIMAP:
			return dialIMAP(ctx, cred)
		case mail.ProviderJMAP:
			return dialJMAP(ctx, cred)
		case mail.ProviderGmail:
			return dialGmailAccount(ctx, acct, cred, refresh)
		case mail.ProviderGraph:
			return dialGraph(cred)
		default:
			return nil, nil, fmt.Errorf("mail: unsupported provider %q", cred.Provider)
		}
	}
}

func dialIMAP(ctx context.Context, cred mail.Credential) (mail.Adapter, func(), error) {
	conn, err := imap.Dial(ctx, imap.Config{
		Host:        cred.Host,
		Port:        cred.Port,
		Username:    imapUser(cred),
		Password:    cred.Password,
		AccessToken: cred.AccessToken,
		Timeout:     30 * time.Second,
	})
	if err != nil {
		return nil, nil, err
	}
	ad := imap.New(conn)
	return ad, func() { _ = ad.Close() }, nil
}

func dialJMAP(ctx context.Context, cred mail.Credential) (mail.Adapter, func(), error) {
	sessionURL := fmt.Sprintf("https://%s/.well-known/jmap", cred.Host)
	if cred.Port != 0 {
		sessionURL = fmt.Sprintf("https://%s:%d/.well-known/jmap", cred.Host, cred.Port)
	}

	ad, err := jmap.Dial(ctx, jmap.Config{
		SessionURL: sessionURL,
		Token:      cred.AccessToken,
	})
	if err != nil {
		return nil, nil, err
	}
	return ad, func() { _ = ad.Close() }, nil
}

// The Gmail REST API is served from gmail.googleapis.com, not the
// www.googleapis.com root the older generated clients used. The endpoint and
// the bearer allowlist must name the SAME host: the pinned google.golang.org/api
// Gmail client builds every request against its configured basePath, and a
// transport that only authorizes a different host strips the token from every
// request the SDK makes (audit 4 F01).
const (
	gmailAPIEndpoint = "https://gmail.googleapis.com/"
	gmailAPIHost     = "gmail.googleapis.com"
)

func dialGmail(ctx context.Context, cred mail.Credential) (mail.Adapter, func(), error) {
	return dialGmailAccount(ctx, mail.AccountID(cred.Email), cred, nil)
}

func dialGmailAccount(ctx context.Context, acct mail.AccountID, cred mail.Credential, refresh GmailRefresh) (mail.Adapter, func(), error) {
	limiter := gmailLimiterFor(acct)
	client := bearerClient(cred.AccessToken, gmailAPIHost)
	if refresh != nil {
		client.Transport = &gmailRefreshTransport{acct: acct, cred: cred, refresh: refresh,
			base: http.DefaultTransport, limiter: limiter, gate: make(chan struct{}, 1)}
	}
	ad, err := gmail.NewWithReadLimiter(ctx, limiter,
		option.WithEndpoint(gmailAPIEndpoint), option.WithHTTPClient(client))
	if err != nil {
		return nil, nil, err
	}
	return ad, func() { _ = ad.Close() }, nil
}

func dialGraph(cred mail.Credential) (mail.Adapter, func(), error) {
	ad := graph.New(bearerClient(cred.AccessToken, "graph.microsoft.com"))
	return ad, func() { _ = ad.Close() }, nil
}

// bearerClient returns an HTTP client that attaches a fixed bearer token.
//
// The credential is bound to the explicitly allowed HTTPS hosts: the
// transport attaches the Authorization header only when the request URL is
// https and its host is permitted, so a redirect that leaves the provider
// origin — including an HTTPS-to-HTTP downgrade — receives no token even
// though http.Client replays each redirect hop through this transport
// (audit neutron-16).
func bearerClient(token string, allowedHosts ...string) *http.Client {
	allowed := make(map[string]bool, len(allowedHosts))
	for _, h := range allowedHosts {
		allowed[strings.ToLower(h)] = true
	}
	return &http.Client{
		Timeout:   60 * time.Second,
		Transport: bearerTransport{token: token, hosts: allowed, base: http.DefaultTransport},
	}
}

type bearerTransport struct {
	token string
	hosts map[string]bool
	base  http.RoundTripper
}

func imapUser(cred mail.Credential) string {
	if u := strings.TrimSpace(cred.Username); u != "" {
		return u
	}
	return cred.Email
}

func (t bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Cloned rather than mutated: a RoundTripper must not modify the
	// request it is handed.
	clone := r.Clone(r.Context())
	if r.URL.Scheme == "https" && t.hosts[strings.ToLower(r.URL.Hostname())] {
		clone.Header.Set("Authorization", "Bearer "+t.token)
	} else {
		clone.Header.Del("Authorization")
	}
	return t.base.RoundTrip(clone)
}
