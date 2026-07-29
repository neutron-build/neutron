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
func New() mail.Resolver {
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
			return dialGmail(ctx, cred)
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
		Username:    cred.Email,
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

func dialGmail(ctx context.Context, cred mail.Credential) (mail.Adapter, func(), error) {
	// The token arrives already refreshed by the caller, so a fixed bearer
	// is correct: this adapter must never attempt a refresh, having neither
	// a refresh token nor a client secret.
	ad, err := gmail.New(ctx, option.WithHTTPClient(bearerClient(cred.AccessToken)))
	if err != nil {
		return nil, nil, err
	}
	return ad, func() { _ = ad.Close() }, nil
}

func dialGraph(cred mail.Credential) (mail.Adapter, func(), error) {
	ad := graph.New(bearerClient(cred.AccessToken))
	return ad, func() { _ = ad.Close() }, nil
}

// bearerClient returns an HTTP client that attaches a fixed bearer token.
func bearerClient(token string) *http.Client {
	return &http.Client{
		Timeout:   60 * time.Second,
		Transport: bearerTransport{token: token, base: http.DefaultTransport},
	}
}

type bearerTransport struct {
	token string
	base  http.RoundTripper
}

func (t bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Cloned rather than mutated: a RoundTripper must not modify the
	// request it is handed.
	clone := r.Clone(r.Context())
	clone.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(clone)
}
