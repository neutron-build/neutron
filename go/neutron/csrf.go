package neutron

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"
)

// CSRFOptions configures the CSRF middleware.
type CSRFOptions struct {
	// CookieName is the name of the CSRF cookie. Default: "__csrf".
	CookieName string
	// HeaderName is the header checked for the token. Default: "X-CSRF-Token".
	HeaderName string
	// FormField is the form field checked for the token. Default: "_csrf".
	FormField string
	// Secure sets the Secure flag on the cookie. Default: true.
	Secure bool
	// Path sets the cookie path. Default: "/".
	Path string
	// SkipPaths is a list of path prefixes that bypass CSRF validation.
	SkipPaths []string
	// TrustedOrigins is a list of origins allowed for cross-origin requests.
	// When set, the middleware also validates the Origin/Referer header on
	// unsafe methods.
	TrustedOrigins []string
}

type ctxKeyCSRF struct{}

// CSRFTokenFromContext returns the current CSRF token from the request context.
// Use this in server-rendered templates to embed the token in a hidden form field.
func CSRFTokenFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyCSRF{}).(string); ok {
		return v
	}
	return ""
}

// CSRF returns middleware implementing the double-submit cookie pattern.
//
// A 32-byte random token is set in a SameSite=Strict cookie.  The cookie is
// intentionally NOT HttpOnly so that JavaScript SPAs can read it and echo it
// back via the X-CSRF-Token header.  For server-rendered forms, the token is
// also stored in the request context and can be retrieved with
// CSRFTokenFromContext.
//
// For unsafe methods (POST, PUT, PATCH, DELETE) the token must be echoed back
// via the X-CSRF-Token header or the _csrf form field.  Comparison uses
// crypto/subtle.ConstantTimeCompare.
//
// When TrustedOrigins is set, the middleware additionally validates the
// Origin (or Referer) header against the allow list to defend against
// cross-origin attacks even when cookies leak.
func CSRF(opts CSRFOptions) Middleware {
	if opts.CookieName == "" {
		opts.CookieName = "__csrf"
	}
	if opts.HeaderName == "" {
		opts.HeaderName = "X-CSRF-Token"
	}
	if opts.FormField == "" {
		opts.FormField = "_csrf"
	}
	if opts.Path == "" {
		opts.Path = "/"
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Check if this path should skip CSRF validation.
			for _, prefix := range opts.SkipPaths {
				if strings.HasPrefix(r.URL.Path, prefix) {
					next.ServeHTTP(w, r)
					return
				}
			}

			// Read existing token from cookie, or generate a new one.
			cookieToken := ""
			if c, err := r.Cookie(opts.CookieName); err == nil {
				cookieToken = c.Value
			}
			if cookieToken == "" {
				cookieToken = generateCSRFToken()
			}

			// Set the cookie.  NOT HttpOnly — JavaScript SPAs need to read it
			// to echo it back via X-CSRF-Token.  SameSite=Strict prevents the
			// cookie from being sent on cross-origin requests.
			http.SetCookie(w, &http.Cookie{
				Name:     opts.CookieName,
				Value:    cookieToken,
				Path:     opts.Path,
				HttpOnly: false,
				Secure:   opts.Secure,
				SameSite: http.SameSiteStrictMode,
			})

			// Store the token in context for server-rendered templates.
			ctx := context.WithValue(r.Context(), ctxKeyCSRF{}, cookieToken)
			r = r.WithContext(ctx)

			// For unsafe methods, validate the token.
			if isUnsafeMethod(r.Method) {
				// Origin validation (when TrustedOrigins is configured).
				if len(opts.TrustedOrigins) > 0 {
					origin := r.Header.Get("Origin")
					if origin == "" {
						// Fall back to Referer header.
						origin = r.Header.Get("Referer")
					}
					if origin != "" && !originInList(origin, opts.TrustedOrigins) {
						WriteError(w, r, newAppError(
							http.StatusForbidden,
							"csrf-origin",
							"CSRF Validation Failed",
							"Untrusted origin",
						))
						return
					}
				}

				// Try header first, then form field.
				submitted := r.Header.Get(opts.HeaderName)
				if submitted == "" {
					submitted = r.FormValue(opts.FormField)
				}
				if submitted == "" || !tokensMatch(cookieToken, submitted) {
					WriteError(w, r, newAppError(
						http.StatusForbidden,
						"csrf-invalid",
						"CSRF Validation Failed",
						"Missing or invalid CSRF token",
					))
					return
				}
			}

			next.ServeHTTP(w, r)
		})
	}
}

// generateCSRFToken returns a 32-byte hex-encoded random token.
func generateCSRFToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// tokensMatch compares two token strings in constant time.
func tokensMatch(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// isUnsafeMethod returns true for HTTP methods that mutate state.
func isUnsafeMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

// originInList checks whether the given origin (or referer URL) matches any
// entry in the trusted origins list.  Comparison is case-insensitive and
// matches on the scheme+host prefix.
func originInList(origin string, trusted []string) bool {
	origin = strings.ToLower(strings.TrimRight(origin, "/"))
	for _, t := range trusted {
		t = strings.ToLower(strings.TrimRight(t, "/"))
		if origin == t || strings.HasPrefix(origin, t+"/") {
			return true
		}
	}
	return false
}
