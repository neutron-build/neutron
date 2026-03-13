package neutron

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
)

// Router wraps Go 1.22+ net/http.ServeMux with composable route groups and
// middleware support.
type Router struct {
	mux        *http.ServeMux
	prefix     string
	middleware []Middleware
	routes     []routeRecord // tracked for OpenAPI generation
}

// routeRecord stores metadata about a registered route for OpenAPI.
type routeRecord struct {
	Method  string
	Pattern string
	InType  reflect.Type
	OutType reflect.Type
	Options routeOptions
}

// RouteOption customizes per-route metadata (used for OpenAPI).
type RouteOption func(*routeOptions)

type routeOptions struct {
	Summary     string
	Description string
	Tags        []string
	Deprecated  bool
	OperationID string
}

func WithSummary(s string) RouteOption {
	return func(o *routeOptions) { o.Summary = s }
}

func WithDescription(s string) RouteOption {
	return func(o *routeOptions) { o.Description = s }
}

func WithTags(tags ...string) RouteOption {
	return func(o *routeOptions) { o.Tags = tags }
}

func WithDeprecated(d bool) RouteOption {
	return func(o *routeOptions) { o.Deprecated = d }
}

func WithOperationID(id string) RouteOption {
	return func(o *routeOptions) { o.OperationID = id }
}

// newRouter creates a root router.
func newRouter() *Router {
	return &Router{
		mux: http.NewServeMux(),
	}
}

// Group creates a sub-router with a prefix and optional middleware.
// Routes registered on the group inherit the prefix and middleware.
func (r *Router) Group(prefix string, mw ...Middleware) *Router {
	return &Router{
		mux:        r.mux,
		prefix:     r.prefix + prefix,
		middleware:  append(r.middleware[:len(r.middleware):len(r.middleware)], mw...),
		routes:     r.routes, // share route list (pointer to same backing)
	}
}

// Mount attaches an http.Handler under a prefix. Useful for mounting external
// handlers or sub-routers.
func (r *Router) Mount(prefix string, handler http.Handler) {
	fullPrefix := r.prefix + prefix
	// Strip prefix before passing to the handler
	r.mux.Handle(fullPrefix+"/", http.StripPrefix(fullPrefix, handler))
	// Also handle exact prefix match
	r.mux.Handle(fullPrefix, handler)
}

// Handle registers a raw http.Handler for the given pattern.
func (r *Router) Handle(pattern string, handler http.Handler) {
	fullPattern := r.prefix + pattern
	wrapped := applyMiddleware(handler, r.middleware)
	r.mux.Handle(fullPattern, wrapped)
}

// HandleFunc registers a raw http.HandlerFunc for the given pattern.
func (r *Router) HandleFunc(pattern string, handler http.HandlerFunc) {
	r.Handle(pattern, handler)
}

// register adds a route with full metadata tracking.
func (r *Router) register(method, pattern string, handler http.Handler, inType, outType reflect.Type, opts routeOptions) {
	fullPattern := method + " " + r.prefix + pattern
	wrapped := applyMiddleware(handler, r.middleware)
	r.mux.Handle(fullPattern, wrapped)

	r.routes = append(r.routes, routeRecord{
		Method:  method,
		Pattern: r.prefix + pattern,
		InType:  inType,
		OutType: outType,
		Options: opts,
	})
}

// ServeHTTP implements http.Handler.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}

func applyMiddleware(h http.Handler, mw []Middleware) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

// Static serves files from a directory on disk under the given URL prefix.
// For example, r.Static("/assets/", "./public") serves files from ./public
// when requests hit /assets/*.
func (r *Router) Static(prefix, dir string) {
	fullPrefix := r.prefix + prefix
	fs := http.FileServer(http.Dir(dir))
	r.mux.Handle(fullPrefix, http.StripPrefix(fullPrefix, fs))
}

// StaticFS serves files from an http.FileSystem (e.g. embed.FS) under the
// given URL prefix.
func (r *Router) StaticFS(prefix string, fs http.FileSystem) {
	fullPrefix := r.prefix + prefix
	fileServer := http.FileServer(fs)
	r.mux.Handle(fullPrefix, http.StripPrefix(fullPrefix, fileServer))
}

// RouteInfo describes a registered route for debugging/inspection.
type RouteInfo struct {
	Method  string
	Pattern string
	Summary string
	Tags    []string
}

// Routes returns a list of all registered routes for debugging/inspection.
func (r *Router) Routes() []RouteInfo {
	infos := make([]RouteInfo, 0, len(r.routes))
	for _, rec := range r.routes {
		infos = append(infos, RouteInfo{
			Method:  rec.Method,
			Pattern: rec.Pattern,
			Summary: rec.Options.Summary,
			Tags:    rec.Options.Tags,
		})
	}
	return infos
}

// PrintRoutes prints all registered routes to stdout in a formatted table.
func (r *Router) PrintRoutes() {
	routes := r.Routes()
	if len(routes) == 0 {
		fmt.Println("No routes registered.")
		return
	}

	// Determine column widths
	mw, pw, sw := len("METHOD"), len("PATTERN"), len("SUMMARY")
	for _, ri := range routes {
		if len(ri.Method) > mw {
			mw = len(ri.Method)
		}
		if len(ri.Pattern) > pw {
			pw = len(ri.Pattern)
		}
		if len(ri.Summary) > sw {
			sw = len(ri.Summary)
		}
	}

	fmtStr := fmt.Sprintf("%%-%ds  %%-%ds  %%-%ds  %%s\n", mw, pw, sw)
	fmt.Printf(fmtStr, "METHOD", "PATTERN", "SUMMARY", "TAGS")
	fmt.Printf(fmtStr,
		strings.Repeat("-", mw),
		strings.Repeat("-", pw),
		strings.Repeat("-", sw),
		strings.Repeat("-", 4))
	for _, ri := range routes {
		tags := ""
		if len(ri.Tags) > 0 {
			tags = strings.Join(ri.Tags, ", ")
		}
		fmt.Printf(fmtStr, ri.Method, ri.Pattern, ri.Summary, tags)
	}
}

// extractPathParams returns path parameter names from a pattern like /users/{id}.
func extractPathParams(pattern string) []string {
	var params []string
	for _, part := range strings.Split(pattern, "/") {
		if strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}") {
			params = append(params, part[1:len(part)-1])
		}
	}
	return params
}
