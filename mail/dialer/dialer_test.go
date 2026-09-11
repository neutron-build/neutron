package dialer

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
