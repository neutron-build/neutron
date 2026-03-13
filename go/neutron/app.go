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
}

// Option configures the App.
type Option func(*App)

// WithConfig sets the application configuration.
func WithConfig(cfg *Config) Option {
	return func(a *App) { a.config = cfg }
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
		a.openapi = generateOpenAPI(a.router.routes, a.oaInfo)
	}
	return a.openapi
}

// Handler returns the root http.Handler with all global middleware applied.
func (a *App) Handler() http.Handler {
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

	// Register default routes
	a.router.mux.Handle("GET /openapi.json", OpenAPIJSON(a.OpenAPI()))
	a.router.mux.Handle("GET /docs", SwaggerUI(a.OpenAPI()))
	a.router.mux.Handle("GET /docs/", SwaggerUI(a.OpenAPI()))
	a.registerHealthCheck()

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
	a.router.mux.Handle("GET /health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"status":  "ok",
			"version": a.oaInfo.Version,
		}
		if a.nucleusChecker != nil {
			resp["nucleus"] = a.nucleusChecker.IsNucleus()
		} else {
			resp["nucleus"] = false
		}
		JSON(w, http.StatusOK, resp)
	}))
}
