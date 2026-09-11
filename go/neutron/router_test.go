package neutron

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func TestRouterBasicRoute(t *testing.T) {
	r := newRouter()

	type Resp struct {
		Message string `json:"message"`
	}

	Get[Empty, Resp](r, "/hello", func(ctx context.Context, _ Empty) (Resp, error) {
		return Resp{Message: "world"}, nil
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/hello", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	var resp Resp
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Message != "world" {
		t.Errorf("message = %q", resp.Message)
	}
}

func TestRouterPostWithBody(t *testing.T) {
	r := newRouter()

	type Input struct {
		Name string `json:"name" validate:"required"`
	}
	type Resp struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	Post[Input, Resp](r, "/users", func(ctx context.Context, input Input) (Resp, error) {
		return Resp{ID: 1, Name: input.Name}, nil
	})

	body := `{"name": "Alice"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/users", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", w.Code)
	}

	var resp Resp
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Name != "Alice" {
		t.Errorf("name = %q", resp.Name)
	}
}

func TestRouterValidationError(t *testing.T) {
	r := newRouter()

	type Input struct {
		Name string `json:"name" validate:"required"`
	}
	type Resp struct {
		ID int `json:"id"`
	}

	Post[Input, Resp](r, "/users", func(ctx context.Context, input Input) (Resp, error) {
		return Resp{ID: 1}, nil
	})

	body := `{}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/users", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", w.Code)
	}
}

func TestRouterPathParams(t *testing.T) {
	r := newRouter()

	type Input struct {
		ID int64 `path:"id"`
	}
	type Resp struct {
		ID int64 `json:"id"`
	}

	Get[Input, Resp](r, "/users/{id}", func(ctx context.Context, input Input) (Resp, error) {
		return Resp{ID: input.ID}, nil
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/users/42", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	var resp Resp
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ID != 42 {
		t.Errorf("id = %d, want 42", resp.ID)
	}
}

func TestRouterQueryParams(t *testing.T) {
	r := newRouter()

	type Input struct {
		Page int    `query:"page"`
		Sort string `query:"sort"`
	}
	type Resp struct {
		Page int    `json:"page"`
		Sort string `json:"sort"`
	}

	Get[Input, Resp](r, "/items", func(ctx context.Context, input Input) (Resp, error) {
		return Resp{Page: input.Page, Sort: input.Sort}, nil
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/items?page=3&sort=name", nil)
	r.ServeHTTP(w, req)

	var resp Resp
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Page != 3 {
		t.Errorf("page = %d, want 3", resp.Page)
	}
	if resp.Sort != "name" {
		t.Errorf("sort = %q, want name", resp.Sort)
	}
}

func TestRouterGroup(t *testing.T) {
	r := newRouter()
	api := r.Group("/api")

	type Resp struct {
		OK bool `json:"ok"`
	}

	Get[Empty, Resp](api, "/health", func(ctx context.Context, _ Empty) (Resp, error) {
		return Resp{OK: true}, nil
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/health", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestRouterGroupMiddleware(t *testing.T) {
	r := newRouter()

	var middlewareCalled bool
	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			middlewareCalled = true
			next.ServeHTTP(w, r)
		})
	}

	api := r.Group("/api", mw)

	type Resp struct {
		OK bool `json:"ok"`
	}

	Get[Empty, Resp](api, "/test", func(ctx context.Context, _ Empty) (Resp, error) {
		return Resp{OK: true}, nil
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/test", nil)
	r.ServeHTTP(w, req)

	if !middlewareCalled {
		t.Error("group middleware was not called")
	}
}

func TestRouterHandlerError(t *testing.T) {
	r := newRouter()

	Get[Empty, Empty](r, "/fail", func(ctx context.Context, _ Empty) (Empty, error) {
		return Empty{}, ErrNotFound("resource not found")
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/fail", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestRouterInvalidJSON(t *testing.T) {
	r := newRouter()

	type Input struct {
		Name string `json:"name"`
	}
	type Resp struct{}

	Post[Input, Resp](r, "/test", func(ctx context.Context, input Input) (Resp, error) {
		return Resp{}, nil
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/test", strings.NewReader("{invalid"))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

// P0.3: unmatched routes (404) and method mismatches (405) render as RFC 7807
// application/problem+json, not the std plain-text replies; 405 carries Allow.
func TestNotFoundAndMethodNotAllowedAreProblemJSON(t *testing.T) {
	r := newRouter()
	Get[Empty, map[string]string](r, "/users", func(ctx context.Context, _ Empty) (map[string]string, error) {
		return map[string]string{"ok": "yes"}, nil
	})

	t.Run("404", func(t *testing.T) {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/nope", nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
		if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "application/problem+json") {
			t.Errorf("content-type = %q, want problem+json", ct)
		}
		var pd map[string]any
		_ = json.NewDecoder(w.Body).Decode(&pd)
		if pd["status"] != float64(404) {
			t.Errorf("body status = %v, want 404", pd["status"])
		}
		if pd["instance"] != "/nope" {
			t.Errorf("instance = %v, want /nope", pd["instance"])
		}
	})

	t.Run("405", func(t *testing.T) {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("DELETE", "/users", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want 405", w.Code)
		}
		if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "application/problem+json") {
			t.Errorf("content-type = %q, want problem+json", ct)
		}
		if allow := w.Header().Get("Allow"); !strings.Contains(allow, "GET") {
			t.Errorf("Allow = %q, want GET", allow)
		}
	})
}

// Group prefixes have to be spliced between the method and the path. Pasting
// them onto the front produced "/apiGET /x", which the mux rejects with
// `invalid method "/apiGET"` — a startup panic that only the untyped
// registration path could reach, and nothing tested it.
func TestGroupWithMethodQualifiedPattern(t *testing.T) {
	app := New()
	g := app.Router().Group("/api")
	g.HandleFunc("GET /widgets", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	req := httptest.NewRequest(http.MethodGet, "/api/widgets", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("grouped HandleFunc route not reachable: got %d", rec.Code)
	}
}

// Routes() only ever recorded the typed helpers, so an app registering through
// HandleFunc — which is most of them — got an empty route table with nothing
// reporting why. There is then nothing to assert on in a test, which is the
// gap that let a route collision reach production.
func TestRoutesIncludesUntypedRegistrations(t *testing.T) {
	app := New()
	r := app.Router()
	r.HandleFunc("GET /alpha", func(w http.ResponseWriter, r *http.Request) {})
	r.Group("/api").HandleFunc("POST /beta", func(w http.ResponseWriter, r *http.Request) {})

	got := map[string]string{}
	for _, ri := range r.Routes() {
		got[ri.Pattern] = ri.Method
	}
	if got["/alpha"] != "GET" {
		t.Fatalf("untyped route missing from Routes(): %+v", got)
	}
	if got["/api/beta"] != "POST" {
		t.Fatalf("grouped untyped route missing or wrong prefix: %+v", got)
	}
}

// Mount/Static/StaticFS used to register raw handlers, so middleware
// attached to a group silently did not apply to them — any guard on the
// group (auth, rate limit) was bypassed by mounted or static routes.
func TestGroupMiddlewareAppliesToMountStaticStaticFS(t *testing.T) {
	reject := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
		})
	}

	setup := func() (*Router, string) {
		r := newRouter()
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "file.txt"), []byte("asset"), 0644)
		g := r.Group("/g", reject)
		g.Mount("/m", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("mounted"))
		}))
		g.Static("/s/", dir)
		g.StaticFS("/f/", http.FS(fstest.MapFS{"file.txt": &fstest.MapFile{Data: []byte("embed")}}))
		return r, dir
	}

	for _, path := range []string{"/g/m/x", "/g/s/file.txt", "/g/f/file.txt"} {
		t.Run(path, func(t *testing.T) {
			r, _ := setup()
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
			if w.Code != http.StatusForbidden {
				t.Errorf("status = %d, want 403: group middleware was not applied", w.Code)
			}
		})
	}
}

// Allowed requests must reach the handler with the guard applied exactly
// once — not zero times (bypass) and not twice (double wrap).
func TestGroupMiddlewareRunsExactlyOnceOnMountStaticStaticFS(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
	}{
		{"mount", "/g/m/x"},
		{"mount-exact", "/g/m"},
		{"static", "/g/s/file.txt"},
		{"staticfs", "/g/f/file.txt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			pass := func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					next.ServeHTTP(w, r)
				})
			}

			r := newRouter()
			dir := t.TempDir()
			os.WriteFile(filepath.Join(dir, "file.txt"), []byte("asset"), 0644)
			g := r.Group("/g", pass)
			g.Mount("/m", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("mounted:" + r.URL.Path))
			}))
			g.Static("/s/", dir)
			g.StaticFS("/f/", http.FS(fstest.MapFS{"file.txt": &fstest.MapFile{Data: []byte("embed")}}))

			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", w.Code)
			}
			if calls != 1 {
				t.Errorf("guard ran %d times, want exactly 1", calls)
			}
		})
	}
}

// The exact mount root and the slash-suffixed mount must expose ONE path
// namespace to the subhandler: both /service and /service/ resolve to the
// "/" subrequest, and subpaths resolve relative to the prefix. Query
// strings survive (audit neutron-22).
func TestMountExactRootMatchesSlashSuffixedRoot(t *testing.T) {
	app := New()
	sub := http.NewServeMux()
	sub.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("root:" + r.URL.RequestURI()))
	})
	sub.HandleFunc("GET /child", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("child:" + r.URL.RequestURI()))
	})
	app.Router().Mount("/service", sub)

	for _, tc := range []struct {
		path     string
		wantBody string
	}{
		{"/service", "root:/"},
		{"/service/", "root:/"},
		{"/service/child?q=1", "child:/child?q=1"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			app.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 for %s", w.Code, tc.path)
			}
			if got := w.Body.String(); got != tc.wantBody {
				t.Errorf("body = %q, want %q", got, tc.wantBody)
			}
		})
	}

	// An unknown subtree path still 404s through the subrouter.
	w := httptest.NewRecorder()
	app.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/service/nope", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("unknown subtree path status = %d, want 404", w.Code)
	}
}

// A matched application handler's own 404/405 must reach the client
// untouched — status, content type, and body. Only genuine routing misses
// get the problem+json treatment (audit neutron-23).
func TestApplication404PassesThroughUntouched(t *testing.T) {
	r := newRouter()
	r.HandleFunc("GET /missing/{id}", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error":"no such widget","code":"W-42"}`)
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/missing/7", nil))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want the application's 404", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want the application's own", ct)
	}
	if got := strings.TrimSpace(w.Body.String()); got != `{"error":"no such widget","code":"W-42"}` {
		t.Errorf("body = %q, want the application's error payload", got)
	}
	if strings.Contains(w.Header().Get("Content-Type"), "application/problem+json") {
		t.Error("a matched handler's 404 was rewritten as a router error")
	}
}

func TestApplication405AndHtml404PassThrough(t *testing.T) {
	r := newRouter()
	// Registered without a method qualifier: the handler itself decides to
	// answer 405 for the wrong verb. A method-qualified pattern would never
	// match the GET, which is a genuine routing 405 and IS rewritten.
	r.HandleFunc("/only-post", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			fmt.Fprint(w, "use POST")
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	r.HandleFunc("GET /page", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "<html><body>gone fishing</body></html>")
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/only-post", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want the application's 405", w.Code)
	}
	if got := strings.TrimSpace(w.Body.String()); got != "use POST" {
		t.Errorf("body = %q, want the application's own body", got)
	}

	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/page", nil))
	if w2.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want the application's 404", w2.Code)
	}
	if !strings.Contains(w2.Body.String(), "gone fishing") {
		t.Errorf("body = %q, want the application's HTML page", w2.Body.String())
	}
}
