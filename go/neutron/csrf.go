package neutron

import (
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
}

// CSRF returns middleware implementing the double-submit cookie pattern.
//
// On every request a 32-byte random token is set in an HttpOnly, SameSite=Strict
// cookie. For unsafe methods (POST, PUT, PATCH, DELETE) the token must be echoed
// back via the X-CSRF-Token header or the _csrf form field.
// Comparison uses crypto/subtle.ConstantTimeCompare.
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

			// Always set/refresh the cookie so the client has a token.
			http.SetCookie(w, &http.Cookie{
				Name:     opts.CookieName,
				Value:    cookieToken,
				Path:     opts.Path,
				HttpOnly: true,
				Secure:   opts.Secure,
				SameSite: http.SameSiteStrictMode,
			})

			// For unsafe methods, validate the token.
			if isUnsafeMethod(r.Method) {
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
