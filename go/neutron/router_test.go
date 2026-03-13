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
