package neutron

import (
	"net/http"
)

// SecureHeadersOptions configures the security headers middleware.
// Zero-value fields use sensible defaults; set a field to "-" to omit that
// header entirely.
type SecureHeadersOptions struct {
	// ContentTypeOptions sets X-Content-Type-Options. Default: "nosniff".
	ContentTypeOptions string
	// FrameOptions sets X-Frame-Options. Default: "DENY".
	FrameOptions string
	// StrictTransportSecurity sets Strict-Transport-Security.
	// Default: "max-age=63072000; includeSubDomains".
	StrictTransportSecurity string
	// ReferrerPolicy sets Referrer-Policy. Default: "strict-origin-when-cross-origin".
	ReferrerPolicy string
	// XSSProtection sets X-XSS-Protection. Default: "0" (modern best practice).
	XSSProtection string
	// ContentSecurityPolicy sets Content-Security-Policy. Default: "" (not set).
	ContentSecurityPolicy string
	// PermissionsPolicy sets Permissions-Policy. Default: "" (not set).
	PermissionsPolicy string
}

// SecureHeaders returns middleware that sets security-related HTTP headers.
// It should be the FIRST middleware in the chain so headers are set before
// any response body is written.
func SecureHeaders(opts SecureHeadersOptions) Middleware {
	// Apply defaults for zero-value fields.
	if opts.ContentTypeOptions == "" {
		opts.ContentTypeOptions = "nosniff"
	}
	if opts.FrameOptions == "" {
		opts.FrameOptions = "DENY"
	}
	if opts.StrictTransportSecurity == "" {
		opts.StrictTransportSecurity = "max-age=63072000; includeSubDomains"
	}
	if opts.ReferrerPolicy == "" {
		opts.ReferrerPolicy = "strict-origin-when-cross-origin"
	}
	if opts.XSSProtection == "" {
		opts.XSSProtection = "0"
	}

	// Build the static header list once so the per-request work is a simple loop.
	type header struct{ key, value string }
	var headers []header
	add := func(key, value string) {
		if value != "" && value != "-" {
			headers = append(headers, header{key, value})
		}
	}
	add("X-Content-Type-Options", opts.ContentTypeOptions)
	add("X-Frame-Options", opts.FrameOptions)
	add("Strict-Transport-Security", opts.StrictTransportSecurity)
	add("Referrer-Policy", opts.ReferrerPolicy)
	add("X-XSS-Protection", opts.XSSProtection)
	add("Content-Security-Policy", opts.ContentSecurityPolicy)
	add("Permissions-Policy", opts.PermissionsPolicy)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for _, h := range headers {
				w.Header().Set(h.key, h.value)
			}
			next.ServeHTTP(w, r)
		})
	}
}
