package jmap

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

type policyTransportFunc func(*http.Request) (*http.Response, error)

func (f policyTransportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func policyResponse(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}
}
func TestDialRejectsSelfAuthorizedOriginsBeforeBearerDispatch(t *testing.T) {
	for _, endpoint := range []string{"https://foreign.test/api", "http://mail.test/api", "https://mail.test:444/api", "https://user@mail.test/api", "https://mail.test/api#fragment"} {
		t.Run(endpoint, func(t *testing.T) {
			calls := 0
			hc := &http.Client{Transport: policyTransportFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.Host != "mail.test" {
					t.Fatal("token reached foreign transport")
				}
				raw, _ := json.Marshal(session{APIURL: endpoint, DownloadURL: "https://mail.test/download/{blobId}", PrimaryAccounts: map[string]string{capMail: "a"}})
				return policyResponse(r, 200, string(raw)), nil
			})}
			_, err := Dial(context.Background(), Config{SessionURL: "https://mail.test/session", Token: "secret", HTTPClient: hc})
			if err == nil || calls != 1 {
				t.Fatal("unapproved metadata accepted", err, calls)
			}
		})
	}
}
func TestDiscoveryRedirectPolicyCannotBeBypassedByCustomCallback(t *testing.T) {
	for _, location := range []string{"https://foreign.test/session", "http://mail.test/session", "https://mail.test:444/session"} {
		calls := 0
		customCalls := 0
		hc := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { customCalls++; return nil }, Transport: policyTransportFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			resp := policyResponse(r, 302, "")
			resp.Header.Set("Location", location)
			return resp, nil
		})}
		if _, err := Dial(context.Background(), Config{SessionURL: "https://mail.test/session", Token: "secret", HTTPClient: hc}); err == nil {
			t.Fatal("redirect bypassed policy")
		}
		if calls != 1 || customCalls != 0 {
			t.Fatal("mandatory policy did not run first", calls, customCalls)
		}
	}
	// A custom callback mutating a permitted redirect is checked again at transport.
	calls := 0
	hc := &http.Client{CheckRedirect: func(r *http.Request, _ []*http.Request) error {
		r.URL, _ = url.Parse("https://foreign.test/stolen")
		return nil
	}, Transport: policyTransportFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		resp := policyResponse(r, 302, "")
		resp.Header.Set("Location", "/next")
		return resp, nil
	})}
	if _, err := Dial(context.Background(), Config{SessionURL: "https://mail.test/session", Token: "secret", HTTPClient: hc}); err == nil || calls != 1 {
		t.Fatal("mutating custom redirect leaked request", err, calls)
	}
}
func TestConfiguredSecondaryOriginAndExpandedDownload(t *testing.T) {
	calls := 0
	hc := &http.Client{Transport: policyTransportFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatal("missing token")
		}
		if calls == 1 {
			raw, _ := json.Marshal(session{APIURL: "https://api.test:443/api", DownloadURL: "https://download.test/{blobId}", PrimaryAccounts: map[string]string{capMail: "a"}})
			return policyResponse(r, 200, string(raw)), nil
		}
		if r.URL.Hostname() != "download.test" {
			t.Fatal("wrong origin")
		}
		return policyResponse(r, 200, "bytes"), nil
	})}
	a, err := Dial(context.Background(), Config{SessionURL: "https://mail.test/session", Token: "secret", HTTPClient: hc, AllowedOrigins: []string{"https://api.test", "https://download.test:443"}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := a.download(context.Background(), "blob/with?reserved", "name", "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	body.Close()
	if calls != 2 {
		t.Fatal(calls)
	}
	a.downloadURL = "https://unapproved.test/{blobId}"
	if _, err := a.download(context.Background(), "blob", "name", "text/plain"); err == nil || calls != 2 {
		t.Fatal("expanded destination bypassed transport policy", err, calls)
	}
}
