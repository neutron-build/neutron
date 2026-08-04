package neutron

import (
	"bufio"
	"fmt"
	"net"
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
	// routes is shared across a root router and its Group() descendants via
	// pointer so OpenAPI sees every registered endpoint regardless of where it
	// was registered.
	routes *[]routeRecord
}

// routeRecord stores metadata about a registered route for OpenAPI.
type routeRecord struct {
	Method  string
	Pattern string
	InType  reflect.Type
	OutType reflect.Type
	Options routeOptions
	// Untyped marks a route registered through Handle/HandleFunc/Mount rather
	// than the typed helpers. It appears in Routes() but is withheld from
	// OpenAPI, which has no schema for it.
	Untyped bool
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
	var routes []routeRecord
	return &Router{
		mux:    http.NewServeMux(),
		routes: &routes,
	}
}

// Group creates a sub-router with a prefix and optional middleware.
// Routes registered on the group inherit the prefix and middleware.
func (r *Router) Group(prefix string, mw ...Middleware) *Router {
	return &Router{
		mux:        r.mux,
		prefix:     r.prefix + prefix,
		middleware: append(r.middleware[:len(r.middleware):len(r.middleware)], mw...),
		routes:     r.routes, // pointer-shared across the whole tree
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
//
// The pattern may carry a method, as `net/http` allows ("GET /x"). A group
// prefix has to be spliced between the method and the path, not pasted onto the
// front: `Group("/api").HandleFunc("GET /x", h)` used to build "/apiGET /x" and
// panic with `invalid method "/apiGET"`. Only the typed Get/Post helpers got
// this right, and nothing tested the untyped path.
func (r *Router) Handle(pattern string, handler http.Handler) {
	fullPattern := joinPattern(r.prefix, pattern)
	wrapped := applyMiddleware(handler, r.middleware)
	r.mux.Handle(fullPattern, wrapped)

	// Record it. Previously only the typed path did, so an app registering
	// through Handle/HandleFunc got an empty Routes() and an empty
	// /openapi.json with nothing reporting why.
	if r.routes != nil {
		method, path := splitPattern(pattern)
		*r.routes = append(*r.routes, routeRecord{
			Method:  method,
			Pattern: r.prefix + path,
			Untyped: true,
		})
	}
}

// splitPattern separates an optional leading method from the path.
func splitPattern(pattern string) (method, path string) {
	if m, p, found := strings.Cut(pattern, " "); found {
		return m, p
	}
	return "", pattern
}

// joinPattern applies a group prefix to a possibly method-qualified pattern.
func joinPattern(prefix, pattern string) string {
	method, path := splitPattern(pattern)
	if method == "" {
		return prefix + path
	}
	return method + " " + prefix + path
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

	if r.routes != nil {
		*r.routes = append(*r.routes, routeRecord{
			Method:  method,
			Pattern: r.prefix + pattern,
			InType:  inType,
			OutType: outType,
			Options: opts,
		})
	}
}

// ServeHTTP implements http.Handler.
//
// P0.3: the std ServeMux replies to unmatched routes with plain text, violating
// the framework's own RFC 7807 contract. We render 404 and 405 as
// application/problem+json instead. Go 1.22's mux returns an empty pattern when
// no route matches the path (genuine 404) and a non-empty pattern when the path
// matches but the method does not (405) — so we distinguish them via Handler().
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Serve via the mux (it populates req.PathValue) wrapped in an interceptor
	// that rewrites the mux's built-in plain-text 404/405 as problem+json. The
	// interceptor forwards Flush/Hijack/Unwrap so SSE/WebSocket are unaffected.
	r.mux.ServeHTTP(&errInterceptor{ResponseWriter: w, req: req}, req)
}

// errInterceptor rewrites the std ServeMux's built-in plain-text 404/405 replies
// as RFC 7807 problem+json. It only rewrites when the response content-type is
// not already problem+json — so handlers that produce their own errors (via
// WriteError) pass through untouched. Go 1.22's mux returns an empty pattern for
// both genuine 404s and method-mismatch 405s, so the status code + the mux-set
// Allow header are the reliable signals, not the pattern.
type errInterceptor struct {
	http.ResponseWriter
	req       *http.Request
	rewritten bool
}

func (w *errInterceptor) WriteHeader(code int) {
	if (code == http.StatusNotFound || code == http.StatusMethodNotAllowed) && !w.rewritten {
		ct := w.ResponseWriter.Header().Get("Content-Type")
		if !strings.Contains(ct, "application/problem+json") {
			w.rewritten = true
			// The mux sets the Allow header before WriteHeader; WriteError keeps it.
			if code == http.StatusMethodNotAllowed {
				WriteError(w.ResponseWriter, w.req,
					ErrMethodNotAllowed("The request method is not supported for this resource."))
			} else {
				WriteError(w.ResponseWriter, w.req,
					ErrNotFound("No route matches the requested path."))
			}
			return
		}
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *errInterceptor) Write(b []byte) (int, error) {
	if w.rewritten {
		return len(b), nil // swallow the std plain-text body
	}
	return w.ResponseWriter.Write(b)
}

func (w *errInterceptor) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *errInterceptor) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *errInterceptor) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("neutron: underlying ResponseWriter does not support Hijack")
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
	if r.routes == nil {
		return nil
	}
	records := *r.routes
	infos := make([]RouteInfo, 0, len(records))
	for _, rec := range records {
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
