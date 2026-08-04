package neutron

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewApp(t *testing.T) {
	app := New()
	if app == nil {
		t.Fatal("New returned nil")
	}
	if app.Router() == nil {
		t.Error("Router is nil")
	}
}

func TestAppHandler(t *testing.T) {
	app := New()

	type Resp struct {
		Msg string `json:"msg"`
	}
	Get[Empty, Resp](app.Router(), "/test", func(ctx context.Context, _ Empty) (Resp, error) {
		return Resp{Msg: "hello"}, nil
	})

	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/test")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var body Resp
	json.NewDecoder(resp.Body).Decode(&body)
	if body.Msg != "hello" {
		t.Errorf("msg = %q", body.Msg)
	}
}

func TestAppWithMiddleware(t *testing.T) {
	var called bool
	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			next.ServeHTTP(w, r)
		})
	}

	app := New(WithMiddleware(mw))

	type Resp struct {
		OK bool `json:"ok"`
	}
	Get[Empty, Resp](app.Router(), "/test", func(ctx context.Context, _ Empty) (Resp, error) {
		return Resp{OK: true}, nil
	})

	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	http.Get(srv.URL + "/test")

	if !called {
		t.Error("middleware was not called")
	}
}

func TestAppOpenAPI(t *testing.T) {
	app := New(WithOpenAPIInfo("My API", "2.0.0"))

	type Resp struct {
		ID int `json:"id"`
	}
	Get[Empty, Resp](app.Router(), "/items", func(ctx context.Context, _ Empty) (Resp, error) {
		return Resp{ID: 1}, nil
	})

	spec := app.OpenAPI()
	if spec.Info.Title != "My API" {
		t.Errorf("title = %q", spec.Info.Title)
	}
	if spec.Info.Version != "2.0.0" {
		t.Errorf("version = %q", spec.Info.Version)
	}
	if _, ok := spec.Paths["/items"]; !ok {
		t.Error("missing /items path")
	}
}

type mockNucleusChecker struct {
	isNucleus bool
}

func (m *mockNucleusChecker) IsNucleus() bool {
	return m.isNucleus
}

func TestHealthCheckWithNucleus(t *testing.T) {
	app := New(
		WithNucleusChecker(&mockNucleusChecker{isNucleus: true}),
		WithOpenAPIInfo("Test", "1.0.0"),
	)
	app.registerHealthCheck()

	srv := httptest.NewServer(app.Router())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)

	if body["status"] != "ok" {
		t.Errorf("status = %v", body["status"])
	}
	if body["nucleus"] != "connected" {
		t.Errorf("nucleus = %v, want \"connected\"", body["nucleus"])
	}
}

func TestHealthCheckWithoutNucleus(t *testing.T) {
	app := New(WithOpenAPIInfo("Test", "1.0.0"))
	app.registerHealthCheck()

	srv := httptest.NewServer(app.Router())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)

	if body["nucleus"] != "unconfigured" {
		t.Errorf("nucleus should be \"unconfigured\", got %v", body["nucleus"])
	}
}

func TestAppLifecycle(t *testing.T) {
	var started, stopped bool

	hook := LifecycleHook{
		Name: "test",
		OnStart: func(ctx context.Context) error {
			started = true
			return nil
		},
		OnStop: func(ctx context.Context) error {
			stopped = true
			return nil
		},
	}

	app := New(WithLifecycle(hook))

	// Manually test lifecycle
	ctx := context.Background()
	if err := app.lifecycle.start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !started {
		t.Error("OnStart not called")
	}

	if err := app.lifecycle.stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if !stopped {
		t.Error("OnStop not called")
	}
}

// The default routes used to be registered inside Run(), so a test driving
// Handler() exercised a different route set than production — and /health,
// /docs and /openapi.json, the routes most likely to be hit by a load balancer
// or uptime check, were precisely the ones no test could reach.
func TestHandlerServesDefaultRoutes(t *testing.T) {
	app := New()

	for _, path := range []string{"/health", "/openapi.json", "/docs"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		app.Handler().ServeHTTP(rec, req)
		if rec.Code == http.StatusNotFound {
			t.Errorf("%s is 404 through Handler(); tests cannot see what Run() serves", path)
		}
	}
}

// An application defining its own /health is ordinary. The framework default
// must step aside rather than overwrite it or panic on startup.
func TestApplicationHealthRouteWins(t *testing.T) {
	app := New()
	app.Router().HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want %d — the framework default replaced the app's own /health",
			rec.Code, http.StatusTeapot)
	}
}

func TestWithoutDefaultRoutes(t *testing.T) {
	app := New(WithoutDefaultRoutes())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 with default routes disabled", rec.Code)
	}
}

// Build must be idempotent: Handler() calls it, and Run() calls it again.
func TestBuildIsIdempotent(t *testing.T) {
	app := New()
	app.Build()
	app.Build() // must not panic on a duplicate registration

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

// A collision panic has to name the two application lines in conflict. Every
// registration funnels through one line in router.go, so the std ServeMux panic
// points there twice and at neither of the files you need to open.
func TestDuplicateRoutePanicNamesBothSites(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("registering the same route twice did not panic")
		}
		msg, _ := r.(string)
		if !strings.Contains(msg, "app_test.go") {
			t.Errorf("panic does not name the registering file, so it points at the "+
				"framework instead of the conflict: %s", msg)
		}
		if strings.Count(msg, "app_test.go") < 2 {
			t.Errorf("panic names only one of the two conflicting sites: %s", msg)
		}
	}()

	app := New()
	app.Router().HandleFunc("GET /dup", func(w http.ResponseWriter, r *http.Request) {})
	app.Router().HandleFunc("GET /dup", func(w http.ResponseWriter, r *http.Request) {})
}
