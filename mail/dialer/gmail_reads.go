package dialer

import (
	"context"
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"
	"sync"

	"github.com/neutron-build/neutron/mail"
	"github.com/neutron-build/neutron/mail/gmail"
)

// Fixed stripes bound memory without retaining account identifiers, credentials
// or timers after release. All resolvers in this process share pacing for the
// same account. A hash collision can only slow unrelated accounts, never raise
// the local rate. Multiple processes and external clients still share Google's
// quota and must respond to throttling; this is not a quota reservation.
var gmailReadStripes [256]struct {
	once    sync.Once
	limiter *gmail.ReadLimiter
}

func gmailLimiterFor(acct mail.AccountID) *gmail.ReadLimiter {
	h := fnv.New32a()
	_, _ = h.Write([]byte(acct))
	stripe := &gmailReadStripes[h.Sum32()%uint32(len(gmailReadStripes))]
	stripe.once.Do(func() { stripe.limiter = gmail.NewReadLimiter() })
	return stripe.limiter
}

// refreshTransport replays one rejected bodyless GET on its provider host
// after a single credential refresh. Gmail paces the replay through its read
// limiter; Graph has none.
type refreshTransport struct {
	acct     mail.AccountID
	cred     mail.Credential
	refresh  Refresh
	provider mail.Provider
	host     string
	base     http.RoundTripper
	limiter  *gmail.ReadLimiter
	gate     chan struct{}
	mu       sync.Mutex
}

func (t *refreshTransport) current() mail.Credential {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cred
}

func (t *refreshTransport) send(r *http.Request, token string) (*http.Response, error) {
	return (bearerTransport{token: token, hosts: map[string]bool{t.host: true}, base: t.base}).RoundTrip(r)
}

func (t *refreshTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	cred := t.current()
	response, err := t.send(r, cred.AccessToken)
	// Only replay a bodyless GET on the authenticated provider origin. In
	// particular, a 401 from a redirect destination cannot initiate a refresh.
	if err != nil || response.StatusCode != http.StatusUnauthorized || r.Method != http.MethodGet ||
		r.Body != nil && r.Body != http.NoBody || r.URL.Scheme != "https" || !strings.EqualFold(r.URL.Hostname(), t.host) || (r.URL.Port() != "" && r.URL.Port() != "443") {
		return response, err
	}
	response.Body.Close()
	ctx := r.Context()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case t.gate <- struct{}{}:
	}
	fresh, refreshErr := t.refreshOnce(ctx, cred)
	<-t.gate
	if refreshErr != nil {
		return nil, refreshErr
	}
	if t.limiter != nil {
		if err := t.limiter.Wait(ctx); err != nil {
			return nil, err
		}
	}
	// Exactly one replay. A second 401 reaches the adapter's permanent failure
	// classification; no mutation, callback recursion or refresh loop occurs.
	return t.send(r, fresh.AccessToken)
}

func (t *refreshTransport) refreshOnce(ctx context.Context, rejected mail.Credential) (mail.Credential, error) {
	if err := ctx.Err(); err != nil {
		return mail.Credential{}, err
	}
	fresh := t.current()
	if fresh.AccessToken != rejected.AccessToken {
		return fresh, nil
	}
	var err error
	fresh, err = t.refresh(ctx, t.acct, rejected)
	if err != nil {
		return mail.Credential{}, err
	}
	if fresh.Provider != t.provider || fresh.AccessToken == "" {
		return mail.Credential{}, fmt.Errorf("%s: invalid refreshed credential", t.provider)
	}
	t.mu.Lock()
	t.cred = fresh
	t.mu.Unlock()
	return fresh, nil
}
