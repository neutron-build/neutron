package neutrontest

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/neutron-dev/neutron-go/neutron"
)

// NewTestApp creates a test Neutron app and httptest.Server.
// The server is automatically closed when the test finishes.
func NewTestApp(t *testing.T, opts ...neutron.Option) (*neutron.App, *httptest.Server) {
	t.Helper()
	app := neutron.New(opts...)
	srv := httptest.NewServer(app.Handler())
	t.Cleanup(srv.Close)
	return app, srv
}

// Request performs an HTTP request against the test server.
// If body is non-nil, it is JSON-encoded.
func Request(t *testing.T, server *httptest.Server, method, path string, body any) *http.Response {
	t.Helper()

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("neutrontest: marshal body: %v", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, server.URL+path, bodyReader)
	if err != nil {
		t.Fatalf("neutrontest: create request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("neutrontest: do request: %v", err)
	}
	return resp
}

// AssertStatus checks that the response has the expected HTTP status code.
func AssertStatus(t *testing.T, resp *http.Response, status int) {
	t.Helper()
	if resp.StatusCode != status {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("expected status %d, got %d; body: %s", status, resp.StatusCode, string(body))
	}
}

// ParseBody reads the response body and JSON-decodes it into T.
func ParseBody[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var result T
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("neutrontest: read body: %v", err)
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("neutrontest: unmarshal body: %v (body: %s)", err, string(data))
	}
	return result
}
