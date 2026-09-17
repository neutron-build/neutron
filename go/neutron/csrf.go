package neutron

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
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
	// To explicitly disable, set AllowInsecure to true and Secure to false.
	Secure bool
	// AllowInsecure must be set to true to disable the Secure cookie flag.
	// This prevents accidental insecure defaults from Go's zero value.
	AllowInsecure bool
	// Path sets the cookie path. Default: "/".
	Path string
	// SkipPaths is a list of path prefixes that bypass CSRF validation.
	SkipPaths []string
	// TrustedOrigins is a list of origins allowed for cross-origin requests.
	// When set, the middleware also validates the Origin/Referer header on
	// unsafe methods.
	TrustedOrigins []string
	// Secret, when set, hardens the double-submit cookie against same-site
	// sibling injection (GO-22): the cookie carries nonce.MAC where MAC is
	// an HMAC over the random nonce under this server-only key. An attacker
	// who can inject a cookie (a hostile sibling subdomain) can mint a
	// nonce but not a valid MAC, so their injected pair is rejected.
	// Minimum 32 bytes of high-entropy key material; shorter values panic
	// at construction.
	Secret []byte
}

// csrfMinSecretLen is the minimum HMAC key size for the signed-token mode.
const csrfMinSecretLen = 32

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

	// Default Secure to true unless explicitly disabled via AllowInsecure
	cookieSecure := true
	if !opts.Secure && opts.AllowInsecure {
		cookieSecure = false
	}

	// A short HMAC key is no HMAC key (GO-22) — fail loudly at assembly.
	if len(opts.Secret) > 0 && len(opts.Secret) < csrfMinSecretLen {
		panic(fmt.Sprintf("neutron: CSRF Secret must be at least %d bytes of high-entropy key material", csrfMinSecretLen))
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

			// In signed mode the cookie value is nonce.MAC and the client
			// must echo the exact same value: the MAC is verified, so a
			// cookie injected by a same-site sibling (which cannot compute
			// MACs) fails even when attacker-supplied cookie and header
			// match (GO-22).
			if len(opts.Secret) > 0 && !validSignedCSRFToken(cookieToken, opts.Secret) {
				cookieToken = newSignedCSRFToken(opts.Secret)
			}

			// Set the cookie.  NOT HttpOnly — JavaScript SPAs need to read it
			// to echo it back via X-CSRF-Token.  SameSite=Strict prevents the
			// cookie from being sent on cross-origin requests.
			http.SetCookie(w, &http.Cookie{
				Name:     opts.CookieName,
				Value:    cookieToken,
				Path:     opts.Path,
				HttpOnly: false,
				Secure:   cookieSecure,
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
					if origin == "" {
						// No origin info — reject for safety
						WriteError(w, r, newAppError(
							http.StatusForbidden,
							"csrf-origin",
							"CSRF Validation Failed",
							"Missing Origin and Referer headers",
						))
						return
					}
					if !originInList(origin, opts.TrustedOrigins) {
						WriteError(w, r, newAppError(
							http.StatusForbidden,
							"csrf-origin",
							"CSRF Validation Failed",
							"Untrusted origin",
						))
						return
					}
				}

				// Try header first, then the FORM body only (GO-22): the old
				// r.FormValue fallback also read the URL query, so a token
				// could ride on the query string of a crafted link.
				submitted := r.Header.Get(opts.HeaderName)
				if submitted == "" {
					submitted = r.PostFormValue(opts.FormField)
				}
				// In signed mode the echoed value must carry a valid MAC.
				if len(opts.Secret) > 0 && !validSignedCSRFToken(submitted, opts.Secret) {
					submitted = ""
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

// newSignedCSRFToken mints nonce.MAC (both base64url) under the server key.
func newSignedCSRFToken(secret []byte) string {
	nonce := make([]byte, 32)
	_, _ = rand.Read(nonce)
	return signedCSRFToken(nonce, csrfMAC(secret, nonce))
}

func signedCSRFToken(nonce, mac []byte) string {
	return base64.RawURLEncoding.EncodeToString(nonce) + "." +
		base64.RawURLEncoding.EncodeToString(mac)
}

// csrfMAC computes the length-prefixed HMAC binding the nonce to the key.
func csrfMAC(secret, nonce []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%d:%s", len(nonce), base64.RawURLEncoding.EncodeToString(nonce))
	return mac.Sum(nil)
}

// validSignedCSRFToken verifies a nonce.MAC token under the server key.
func validSignedCSRFToken(token string, secret []byte) bool {
	nonceB64, macB64, ok := strings.Cut(token, ".")
	if !ok {
		return false
	}
	nonce, err := base64.RawURLEncoding.DecodeString(nonceB64)
	if err != nil || len(nonce) != 32 {
		return false
	}
	mac, err := base64.RawURLEncoding.DecodeString(macB64)
	if err != nil {
		return false
	}
	return hmac.Equal(mac, csrfMAC(secret, nonce))
}

// tokensMatch compares two token strings in constant time.
func tokensMatch(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// isUnsafeMethod returns true for any method that can mutate state (GO-22):
// the old closed list of four missed custom verbs, which then bypassed CSRF
// entirely.
func isUnsafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return false
	default:
		return true
	}
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
