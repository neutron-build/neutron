package neutron

import (
	"encoding/json"
	"errors"
	"net/http"
)

// JSON writes a JSON response with the given status code.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

// WriteError writes an error response in RFC 7807 format.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	var appErr *AppError
	if errors.As(err, &appErr) {
		pd := appErr.ToProblemDetail(r.URL.Path)
		w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
		w.WriteHeader(appErr.Status)
		_ = json.NewEncoder(w).Encode(pd)
		return
	}
	// Unknown errors become 500
	pd := ProblemDetail{
		Type:   errBaseURL + "internal",
		Title:  "Internal Server Error",
		Status: http.StatusInternalServerError,
		Detail: "An unexpected error occurred",
	}
	w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
	w.WriteHeader(http.StatusInternalServerError)
	_ = json.NewEncoder(w).Encode(pd)
}
