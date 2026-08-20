package neutron

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// NucleusChecker is an interface for checking Nucleus status in health checks.
type NucleusChecker interface {
	IsNucleus() bool
}

// App is the Neutron application. It ties together routing, middleware,
// lifecycle management, configuration, and OpenAPI generation.
type App struct {
	router         *Router
	middleware     []Middleware
	lifecycle      *lifecycle
	config         *Config
	logger         *slog.Logger
	openapi        *OpenAPISpec
	oaInfo         OpenAPIInfo
	nucleusChecker NucleusChecker
	// built records that Build() has already registered the default routes, so
	// Run() and Handler() can both call it without registering them twice.
	built bool
	// disableDefaultRoutes suppresses every framework-supplied route
	// (/openapi.json, /docs, /health) rather than just the docs pair.
	disableDefaultRoutes bool
	// disableDefaultDocs suppresses the built-in GET /docs (Swagger UI) route.
	// Callers can mount Swagger UI themselves at a different path.
	disableDefaultDocs bool
}

// Option configures the App.
type Option func(*App)

// WithConfig sets the application configuration.
func WithConfig(cfg *Config) Option {
	return func(a *App) { a.config = cfg }
}

// WithoutDefaultRoutes suppresses every framework-supplied route
// (/openapi.json, /docs, /docs/, /health).
//
// Defining any of them yourself already takes precedence without this — it is
// for the case where the route should not exist at all, such as an internal
// service that must not expose its schema.
func WithoutDefaultRoutes() Option {
	return func(a *App) { a.disableDefaultRoutes = true }
}

// WithMiddleware adds global middleware applied to all routes.
func WithMiddleware(mw ...Middleware) Option {
	return func(a *App) { a.middleware = append(a.middleware, mw...) }
}

// WithLifecycle adds lifecycle hooks for startup/shutdown.
func WithLifecycle(hooks ...LifecycleHook) Option {
	return func(a *App) { a.lifecycle.add(hooks...) }
}

// WithLogger sets the slog logger for the application.
func WithLogger(logger *slog.Logger) Option {
	return func(a *App) {
		a.logger = logger
		a.lifecycle.logger = logger
	}
}

// WithOpenAPIInfo sets the OpenAPI spec info.
func WithOpenAPIInfo(title, version string) Option {
	return func(a *App) {
		a.oaInfo = OpenAPIInfo{Title: title, Version: version}
	}
}

// WithNucleusChecker registers a NucleusChecker for the health endpoint.
func WithNucleusChecker(nc NucleusChecker) Option {
	return func(a *App) {
		a.nucleusChecker = nc
	}
}

// DisableDefaultDocs suppresses the auto-registered Swagger UI at /docs.
// /openapi.json is still served. Useful when an SPA wants to own /docs for
// its own in-product documentation page.
func DisableDefaultDocs() Option {
	return func(a *App) { a.disableDefaultDocs = true }
}

// New creates a new Neutron application.
func New(opts ...Option) *App {
	logger := slog.Default()
	a := &App{
		router:    newRouter(),
		lifecycle: newLifecycle(logger),
		logger:    logger,
		config:    &Config{Server: ServerConfig{Addr: ":8080", ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, ShutdownTimeout: 30 * time.Second}},
		oaInfo:    OpenAPIInfo{Title: "Neutron API", Version: "1.0.0"},
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Router returns the application router.
func (a *App) Router() *Router {
	return a.router
}

// OpenAPI returns the auto-generated OpenAPI 3.1 specification.
// The spec is built lazily on first access from registered routes.
func (a *App) OpenAPI() *OpenAPISpec {
	if a.openapi == nil {
		var routes []routeRecord
		if a.router.routes != nil {
			routes = *a.router.routes
		}
		a.openapi = generateOpenAPI(routes, a.oaInfo)
	}
	return a.openapi
}

// Build registers the framework's default routes (/openapi.json, /docs,
// /health). It is idempotent, and both Run and Handler call it.
//
// These used to be registered inside Run, which meant the handler a test
// exercised through Handler() served a different set of routes than the one
// production served through Run() — the four routes most likely to be probed by
// a load balancer or an uptime check were exactly the ones no test could see.
//
// A route the application already registered is left alone rather than
// overwritten or treated as a collision: defining your own /health is normal,
// and a framework default should yield to it silently rather than panic on
// startup.
func (a *App) Build() {
	if a.built || a.disableDefaultRoutes {
		return
	}
	a.built = true

	a.router.handleIfAbsent("GET /openapi.json", OpenAPIJSON(a.OpenAPI()))
	if !a.disableDefaultDocs {
		a.router.handleIfAbsent("GET /docs", SwaggerUI(a.OpenAPI()))
		a.router.handleIfAbsent("GET /docs/", SwaggerUI(a.OpenAPI()))
	}
	a.registerHealthCheck()
}

// Handler returns the root http.Handler with all global middleware applied,
// including the framework's default routes — so a test drives the same routes
// production does.
func (a *App) Handler() http.Handler {
	a.Build()
	var h http.Handler = a.router
	for i := len(a.middleware) - 1; i >= 0; i-- {
		h = a.middleware[i](h)
	}
	return h
}

// Run starts the HTTP server with graceful shutdown on SIGTERM/SIGINT.
func (a *App) Run(addr string) error {
	if addr == "" {
		addr = a.config.Server.Addr
	}

	a.Build()

	// Start lifecycle hooks
	ctx := context.Background()
	if err := a.lifecycle.start(ctx); err != nil {
		return err
	}

	srv := &http.Server{
		Addr:         addr,
		Handler:      a.Handler(),
		ReadTimeout:  a.config.Server.ReadTimeout,
		WriteTimeout: a.config.Server.WriteTimeout,
	}

	// Graceful shutdown
	errCh := make(chan error, 1)
	go func() {
		a.logger.Info("server starting", "addr", addr)
		errCh <- srv.ListenAndServe()
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		a.logger.Info("shutdown signal received", "signal", sig.String())
	case err := <-errCh:
		if err != http.ErrServerClosed {
			return err
		}
	}

	// Drain with timeout
	shutdownCtx, cancel := context.WithTimeout(ctx, a.config.Server.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		a.logger.Error("server shutdown error", "error", err)
	}

	// Stop lifecycle hooks in reverse order
	if err := a.lifecycle.stop(shutdownCtx); err != nil {
		a.logger.Error("lifecycle shutdown error", "error", err)
	}

	a.logger.Info("server stopped")
	return nil
}

func (a *App) registerHealthCheck() {
	a.router.handleIfAbsent("GET /health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"status":  "ok",
			"version": a.oaInfo.Version,
		}
		// Contract §7: nucleus reflects the HEALTH of the nucleus dependency.
		// No checker → "unconfigured"; checker present → "connected" when the
		// nucleus connection is detected, else "disconnected".
		if a.nucleusChecker == nil {
			resp["nucleus"] = "unconfigured"
		} else if a.nucleusChecker.IsNucleus() {
			resp["nucleus"] = "connected"
		} else {
			resp["nucleus"] = "disconnected"
		}
		JSON(w, http.StatusOK, resp)
	}))
}
