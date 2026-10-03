package dialer

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/mail"
	"github.com/neutron-build/neutron/mail/gmail"
)

func refreshFixture(t *testing.T, method, url string, refreshErr error) (*http.Response, error, int, int) {
	t.Helper()
	calls, refreshes := 0, 0
	tr := &gmailRefreshTransport{acct: "synthetic", cred: mail.Credential{Provider: mail.ProviderGmail, AccessToken: "old"}, gate: make(chan struct{}, 1), limiter: gmail.NewReadLimiter()}
	tr.base = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		allowed := r.URL.Scheme == "https" && r.URL.Hostname() == gmailAPIHost
		if allowed && r.Header.Get("Authorization") == "" || !allowed && r.Header.Get("Authorization") != "" {
			t.Fatalf("bearer-origin restriction violated at %s", r.URL)
		}
		return &http.Response{StatusCode: 401, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("rejected"))}, nil
	})
	tr.refresh = func(context.Context, mail.AccountID, mail.Credential) (mail.Credential, error) {
		refreshes++
		return mail.Credential{Provider: mail.ProviderGmail, AccessToken: "fresh"}, refreshErr
	}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := tr.RoundTrip(req)
	return response, err, calls, refreshes
}

func TestGmailRefreshRetriesGetExactlyOnce(t *testing.T) {
	response, err, calls, refreshes := refreshFixture(t, http.MethodGet, gmailAPIEndpoint+"gmail/v1/users/me/labels", nil)
	if err != nil || response.StatusCode != 401 || calls != 2 || refreshes != 1 {
		t.Fatalf("status=%v err=%v calls=%d refreshes=%d", response, err, calls, refreshes)
	}
	response.Body.Close()
}
func TestGmailRefreshNeverReplaysMutationsOrForeignOrigin(t *testing.T) {
	for _, tc := range []struct{ method, url string }{{http.MethodPost, gmailAPIEndpoint + "gmail/v1/users/me/messages/send"}, {http.MethodPost, gmailAPIEndpoint + "gmail/v1/users/me/messages/batchModify"}, {http.MethodGet, "https://foreign.example.invalid/"}, {http.MethodGet, "http://gmail.googleapis.com/"}, {http.MethodGet, "https://gmail.googleapis.com:444/"}} {
		t.Run(tc.method+tc.url, func(t *testing.T) {
			response, err, calls, refreshes := refreshFixture(t, tc.method, tc.url, nil)
			if err != nil || response.StatusCode != 401 || calls != 1 || refreshes != 0 {
				t.Fatalf("err=%v calls=%d refreshes=%d", err, calls, refreshes)
			}
			response.Body.Close()
		})
	}
}
func TestGmailRefreshFailureStopsBeforeReadReplay(t *testing.T) {
	for _, want := range []error{mail.ErrReauthRequired, context.Canceled, context.DeadlineExceeded, errors.New("temporary OAuth failure")} {
		t.Run(want.Error(), func(t *testing.T) {
			response, err, calls, refreshes := refreshFixture(t, http.MethodGet, gmailAPIEndpoint+"gmail/v1/users/me/labels", want)
			if response != nil || !errors.Is(err, want) || calls != 1 || refreshes != 1 {
				t.Fatalf("response=%v err=%v calls=%d refreshes=%d", response, err, calls, refreshes)
			}
		})
	}
}
func TestGmailReadLimiterSharedAndBounded(t *testing.T) {
	first := gmailLimiterFor("same-account")
	if gmailLimiterFor("same-account") != first {
		t.Fatal("same account did not share limiter")
	}
	unique := map[*gmail.ReadLimiter]bool{}
	for i := 0; i < 4096; i++ {
		unique[gmailLimiterFor(mail.AccountID(strings.Repeat("a", i)))] = true
	}
	if len(unique) > 256 {
		t.Fatalf("unbounded account limiter retention: %d", len(unique))
	}
}
