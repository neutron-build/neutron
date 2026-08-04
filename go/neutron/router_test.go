package neutron

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
		Page  int    `query:"page"`
		Sort  string `query:"sort"`
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
