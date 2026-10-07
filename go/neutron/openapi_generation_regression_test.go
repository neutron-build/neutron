package neutron

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func echo(_ context.Context, _ Empty) (Empty, error) { return Empty{}, nil }

// NA-05 regressions. Two defects shared one symptom — a stale /openapi.json:
//
//  1. Group() built its Router with a fresh value-type generation counter, so
//     a grouped registration bumped a counter nothing watched. Once the root
//     spec had been generated once, grouped routes were absent from it forever.
//  2. Build() installed OpenAPIJSON(a.OpenAPI()), which marshaled the spec
//     into bytes at registration time. Even a root-level route added after
//     Build() could never change what the endpoint served.

func specPaths(t *testing.T, spec *OpenAPISpec) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for p := range spec.Paths {
		out[p] = true
	}
	return out
}

func fetchOpenAPI(t *testing.T, h http.Handler) map[string]any {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET /openapi.json = %d", w.Code)
	}
	var doc map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode openapi json: %v", err)
	}
	return doc
}

// A grouped registration must invalidate the root's cached spec the same way
// a root registration does — including a nested group, and including after
// the spec has already been generated once (the stale-cache half of NA-05).
func TestGroupRegistrationInvalidatesOpenAPICache(t *testing.T) {
	app := New(WithOpenAPIInfo("t", "1"))
	Get[Empty, Empty](app.Router(), "/root", echo)

	// Warm the cache first: the defect only appeared after the first build.
	if got := specPaths(t, app.OpenAPI()); !got["/root"] {
		t.Fatalf("warm spec missing /root: %v", got)
	}

	g := app.Router().Group("/api")
	Get[Empty, Empty](g, "/grouped", echo)
	if got := specPaths(t, app.OpenAPI()); !got["/api/grouped"] {
		t.Fatalf("spec missing group route after registration: %v", got)
	}

	nested := g.Group("/v1")
	Get[Empty, Empty](nested, "/deep", echo)
	if got := specPaths(t, app.OpenAPI()); !got["/api/v1/deep"] {
		t.Fatalf("spec missing nested-group route after registration: %v", got)
	}
}

// The default endpoint must resolve the current spec per request: routes
// added after Build() — root or grouped — change what /openapi.json serves
// (the captured-bytes half of NA-05).
func TestOpenAPIEndpointServesPostBuildRegistrations(t *testing.T) {
	app := New(WithOpenAPIInfo("t", "1"))
	Get[Empty, Empty](app.Router(), "/before", echo)

	h := app.Handler() // calls Build()
	before := fetchOpenAPI(t, h)
	if _, ok := before["paths"].(map[string]any)["/before"]; !ok {
		t.Fatalf("endpoint missing pre-Build route: %v", before)
	}

	Get[Empty, Empty](app.Router(), "/after", echo)
	Get[Empty, Empty](app.Router().Group("/api"), "/grouped", echo)

	after := fetchOpenAPI(t, h)
	paths := after["paths"].(map[string]any)
	if _, ok := paths["/after"]; !ok {
		t.Error("route registered after Build() is absent from /openapi.json (stale captured bytes)")
	}
	if _, ok := paths["/api/grouped"]; !ok {
		t.Error("grouped route registered after Build() is absent from /openapi.json")
	}
}

// An application that claims /openapi.json itself keeps ownership: the
// framework endpoint must step aside, as it always has.
func TestOpenAPIEndpointYieldsToUserRegistration(t *testing.T) {
	app := New()
	app.Router().HandleFunc("GET /openapi.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"custom":true}`))
	})
	w := httptest.NewRecorder()
	app.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if strings.TrimSpace(w.Body.String()) != `{"custom":true}` {
		t.Errorf("user-owned /openapi.json was overridden: %q", w.Body.String())
	}
}

// The spec cache is now reachable from concurrent requests on the default
// endpoint; run them to catch an unsynchronized cache under -race.
func TestOpenAPIConcurrentReadsAreRaceFree(t *testing.T) {
	app := New(WithOpenAPIInfo("t", "1"))
	Get[Empty, Empty](app.Router(), "/r", echo)
	Get[Empty, Empty](app.Router().Group("/g"), "/x", echo)
	h := app.Handler()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%4 == 0 {
				Get[Empty, Empty](app.Router(), "/more-"+strconv.Itoa(i), echo)
			}
			doc := fetchOpenAPI(t, h)
			if doc == nil {
				t.Error("nil document")
			}
		}(i)
	}
	wg.Wait()
}
