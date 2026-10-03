package dialer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
)

// captureTransport records the Authorization header of every hop so a test
// can follow redirects through the real http.Client loop.
type captureTransport struct {
	hops []string
}

func (c *captureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.hops = append(c.hops, r.URL.String()+" | "+r.Header.Get("Authorization"))
	rec := httptest.NewRecorder()
	return rec.Result(), nil
}

// The bearer must reach only the explicitly permitted HTTPS origin: a
// cross-origin redirect or an HTTPS-to-HTTP downgrade must proceed without
// the credential (audit neutron-16).
func TestBearerClientAttachesTokenOnlyToAllowedOrigin(t *testing.T) {
	capture := &captureTransport{}
	client := &http.Client{
		Transport: bearerTransport{
			token: "secret-token",
			hosts: map[string]bool{"api.example.com": true},
			base:  capture,
		},
	}

	hop := func(url string) string {
		n := len(capture.hops)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer should-be-replaced")
		resp, err := client.Transport.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if len(capture.hops) != n+1 {
			t.Fatalf("transport did not record the hop for %s", url)
		}
		return capture.hops[n]
	}

	if got := hop("https://api.example.com/v1/mail"); got != "https://api.example.com/v1/mail | Bearer secret-token" {
		t.Errorf("allowed origin hop = %q, want the bearer attached", got)
	}
	if got := hop("https://evil.example.net/v1/mail"); got != "https://evil.example.net/v1/mail | " {
		t.Errorf("cross-origin hop = %q, want no credential", got)
	}
	if got := hop("http://api.example.com/v1/mail"); got != "http://api.example.com/v1/mail | " {
		t.Errorf("downgraded hop = %q, want no credential on plain HTTP", got)
	}
}

// The full client loop must keep the credential on a same-origin redirect
// and drop it when a redirect leaves the origin, driving the real
// http.Client redirect machinery through a synthetic base transport.
func TestBearerClientRedirectHandling(t *testing.T) {
	var hops []string

	respond := func(r *http.Request) (*http.Response, error) {
		hops = append(hops, r.URL.String()+" | "+r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/same-start":
			return redirectResponse("https://api.example.com/landing"), nil
		case "/cross-start":
			return redirectResponse("https://evil.example.net/sink"), nil
		case "/downgrade-start":
			return redirectResponse("http://api.example.com/plain"), nil
		default:
			return textResponse(), nil
		}
	}

	client := bearerClient("secret-token", "api.example.com")
	client.Transport = bearerTransport{
		token: "secret-token",
		hosts: map[string]bool{"api.example.com": true},
		base:  roundTripFunc(respond),
	}

	for _, start := range []string{"same-start", "cross-start", "downgrade-start"} {
		t.Run(start, func(t *testing.T) {
			hops = nil
			resp, err := client.Get("https://api.example.com/" + start)
			if err != nil {
				t.Fatalf("redirect chain failed: %v", err)
			}
			resp.Body.Close()

			for _, hop := range hops {
				url, auth, _ := strings.Cut(hop, " | ")
				host := strings.TrimPrefix(strings.TrimPrefix(url, "https://"), "http://")
				host, _, _ = strings.Cut(host, "/")
				allowed := strings.HasPrefix(url, "https://") && host == "api.example.com"
				if allowed && auth != "Bearer secret-token" {
					t.Errorf("allowed hop %q lost the credential (%q)", url, auth)
				}
				if !allowed && auth != "" {
					t.Errorf("hop %q carried the credential outside the allowed origin (%q)", url, auth)
				}
			}
			if len(hops) < 2 {
				t.Errorf("redirect was not followed; hops = %v", hops)
			}
		})
	}
}

// The Gmail SDK dials the host named by its configured endpoint, and the
// bearer transport only authorizes its allowlisted hosts. The two must agree:
// pinning the endpoint while allowing a different host (the www.googleapis.com
// root the allowlist used to name) strips the token from every SDK request
// (audit 4 F01).
func TestGmailEndpointMatchesBearerAllowlist(t *testing.T) {
	endpoint, err := url.Parse(gmailAPIEndpoint)
	if err != nil {
		t.Fatalf("gmail endpoint does not parse: %v", err)
	}
	if endpoint.Scheme != "https" {
		t.Fatalf("gmail endpoint scheme = %q, want https", endpoint.Scheme)
	}

	transport, ok := bearerClient("secret-token", gmailAPIHost).Transport.(bearerTransport)
	if !ok {
		t.Fatal("bearerClient transport is not a bearerTransport")
	}
	if !transport.hosts[endpoint.Hostname()] {
		t.Fatalf("allowlist %v omits the endpoint host %q", transport.hosts, endpoint.Hostname())
	}
	if len(transport.hosts) != 1 {
		t.Fatalf("allowlist = %v, want exactly the Gmail API host", transport.hosts)
	}
	// The old defect, pinned: the www root must NOT be the authorized host.
	if transport.hosts["www.googleapis.com"] {
		t.Fatal("allowlist authorizes www.googleapis.com; the Gmail API is served from gmail.googleapis.com")
	}

	// The real pinned SDK must actually dial the configured endpoint.
	svc, err := gmail.NewService(context.Background(),
		option.WithEndpoint(gmailAPIEndpoint),
		option.WithHTTPClient(bearerClient("secret-token", gmailAPIHost)))
	if err != nil {
		t.Fatalf("gmail service construction failed: %v", err)
	}
	if svc.BasePath != gmailAPIEndpoint {
		t.Fatalf("service BasePath = %q, want %q", svc.BasePath, gmailAPIEndpoint)
	}

	// And the transport authorizes exactly that host, not the www root.
	capture := &captureTransport{}
	client := &http.Client{Transport: bearerTransport{token: "secret-token", hosts: transport.hosts, base: capture}}
	for _, target := range []string{
		gmailAPIEndpoint + "gmail/v1/users/me/labels",
		"https://www.googleapis.com/gmail/v1/users/me/labels",
	} {
		req, err := http.NewRequest(http.MethodGet, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Transport.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if len(capture.hops) == 0 {
			t.Fatalf("no recorded hop for %s", target)
		}
		hop := capture.hops[len(capture.hops)-1]
		got := hop[strings.LastIndex(hop, "| ")+2:]
		if target == gmailAPIEndpoint+"gmail/v1/users/me/labels" && got != "Bearer secret-token" {
			t.Errorf("Gmail API request carried %q, want the bearer token", got)
		}
		if target != gmailAPIEndpoint+"gmail/v1/users/me/labels" && got != "" {
			t.Errorf("www root request carried %q, want no credential", got)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func redirectResponse(location string) *http.Response {
	rec := httptest.NewRecorder()
	rec.Header().Set("Location", location)
	rec.WriteHeader(http.StatusFound)
	return rec.Result()
}

func textResponse() *http.Response {
	rec := httptest.NewRecorder()
	rec.WriteHeader(http.StatusOK)
	return rec.Result()
}
