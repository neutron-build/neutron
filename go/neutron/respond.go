package neutron

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
)

// JSON writes a JSON response with the given status code.
//
// The value is marshalled BEFORE headers are committed (GO-04): streaming
// into the writer after WriteHeader meant an encoding failure (NaN, cycles,
// channels) left a committed 200 with an empty or partial body. Marshal
// failures log server-side and answer a clean 500; transport write errors
// after commitment are logged, as nothing can be sent in-band anymore.
func JSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		log.Printf("[neutron] JSON encoding failed: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	body = append(body, '\n')
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		log.Printf("[neutron] response write failed: %v", err)
	}
}

// WriteError writes an error response in RFC 7807 format.
//
// Same marshal-before-commit rule as JSON: the problem document is encoded
// first so a failure to encode it cannot produce a committed status with no
// body. The fallback for an unencodable problem is a bare status line.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	var appErr *AppError
	if errors.As(err, &appErr) {
		writeProblem(w, appErr.Status, appErr.ToProblemDetail(r.URL.Path))
		return
	}
	// Unknown errors become 500
	writeProblem(w, http.StatusInternalServerError, ProblemDetail{
		Type:   errBaseURL + "internal",
		Title:  "Internal Server Error",
		Status: http.StatusInternalServerError,
		Detail: "An unexpected error occurred",
	})
}

func writeProblem(w http.ResponseWriter, status int, pd ProblemDetail) {
	body, err := json.Marshal(pd)
	if err != nil {
		log.Printf("[neutron] problem-detail encoding failed: %v", err)
		w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
		w.WriteHeader(status)
		return
	}
	body = append(body, '\n')
	w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		log.Printf("[neutron] response write failed: %v", err)
	}
}
