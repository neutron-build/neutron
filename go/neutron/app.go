package neutron

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	router     *Router
	middleware []Middleware
	lifecycle  *lifecycle
	config     *Config
	logger     *slog.Logger
	openapi    *OpenAPISpec
	// openapiGen is the router generation the cached spec was built at
	// (GO-13).
	openapiGen     uint64
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
// The spec is built lazily on first access from registered routes and
// re-generated when routes were registered after the last build (GO-13):
// a spec cached before later registrations was silently stale.
func (a *App) OpenAPI() *OpenAPISpec {
	gen := a.router.Generation()
	if a.openapi == nil || a.openapiGen != gen {
		var routes []routeRecord
		if a.router.routes != nil {
			routes = *a.router.routes
		}
		a.openapi = generateOpenAPI(routes, a.oaInfo)
		a.openapiGen = gen
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
//
// A non-empty addr is used as given. An empty addr derives the listen address
// from NEUTRON_HOST and NEUTRON_PORT (contract §6), each falling back to the
// corresponding part of the configured Server.Addr (default ":8080" — all
// interfaces, port 8080). An invalid NEUTRON_PORT is an error.
func (a *App) Run(addr string) error {
	addr, err := a.listenAddr(addr)
	if err != nil {
		return err
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
			// GO-11: a bind/listen failure must still stop the hooks that
			// started successfully, or their resources leak on startup exit.
			if stopErr := a.lifecycle.stopWithBudget(ctx); stopErr != nil {
				a.logger.Error("lifecycle shutdown error", "error", stopErr)
				return errors.Join(err, stopErr)
			}
			return err
		}
	}

	// Drain with timeout
	shutdownCtx, cancel := context.WithTimeout(ctx, a.config.Server.ShutdownTimeout)
	defer cancel()

	// GO-11: a shutdown timeout must not strand stop hooks with an expired
	// context. Force-close the connections Server.Shutdown was waiting on,
	// then run the hooks with a guaranteed-fresh bounded budget; hook errors
	// propagate instead of being logged and swallowed.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		a.logger.Error("server shutdown error", "error", err)
		srv.Close()
	}

	// Stop lifecycle hooks in reverse order
	if err := a.lifecycle.stopWithBudget(shutdownCtx); err != nil {
		a.logger.Error("lifecycle shutdown error", "error", err)
		return fmt.Errorf("neutron: lifecycle shutdown: %w", err)
	}

	a.logger.Info("server stopped")
	return nil
}

// listenAddr resolves the address Run listens on. An explicit addr always
// wins; otherwise NEUTRON_HOST / NEUTRON_PORT override the host and port of
// the configured Server.Addr independently.
func (a *App) listenAddr(addr string) (string, error) {
	if addr != "" {
		return addr, nil
	}
	host, port := "", "8080"
	if cfg := a.config.Server.Addr; cfg != "" {
		h, p, err := net.SplitHostPort(cfg)
		if err != nil {
			return "", fmt.Errorf("neutron: invalid Server.Addr %q: %w", cfg, err)
		}
		host = h
		if p != "" {
			port = p
		}
	}
	if v, ok := os.LookupEnv("NEUTRON_HOST"); ok && v != "" {
		host = strings.TrimSuffix(strings.TrimPrefix(v, "["), "]")
	}
	if v, ok := os.LookupEnv("NEUTRON_PORT"); ok && v != "" {
		n, err := strconv.ParseUint(v, 10, 16)
		if err != nil || n < 1 {
			return "", fmt.Errorf("neutron: invalid NEUTRON_PORT %q: must be an integer between 1 and 65535", v)
		}
		port = strconv.FormatUint(n, 10)
	}
	return net.JoinHostPort(host, port), nil
}

func (a *App) registerHealthCheck() {
	a.router.handleIfAbsent("GET /health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"status":  "ok",
			"version": a.oaInfo.Version,
		}
		// Contract §7: nucleus reflects the HEALTH of the nucleus dependency.
		// No checker → "unconfigured". With a checker, prefer a LIVE probe:
		// `IsNucleus` reports features detected earlier by a version query —
		// a previously identified database can be down while identity stays
		// true (GO-12). Checkers that implement Ping get a real bounded
		// connectivity check; a failed probe degrades the response.
		if a.nucleusChecker == nil {
			resp["nucleus"] = "unconfigured"
		} else if pinger, ok := a.nucleusChecker.(interface {
			Ping(context.Context) error
		}); ok {
			probeCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := pinger.Ping(probeCtx); err != nil {
				resp["status"] = "degraded"
				resp["nucleus"] = "disconnected"
				resp["error"] = "nucleus dependency unreachable"
				JSON(w, http.StatusServiceUnavailable, resp)
				return
			}
			resp["nucleus"] = "connected"
		} else if a.nucleusChecker.IsNucleus() {
			resp["nucleus"] = "connected"
		} else {
			resp["nucleus"] = "disconnected"
		}
		JSON(w, http.StatusOK, resp)
	}))
}
