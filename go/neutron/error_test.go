package neutron

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAppErrorError(t *testing.T) {
	err := ErrNotFound("user 42 not found")
	got := err.Error()
	if got != "Not Found: user 42 not found" {
		t.Errorf("Error() = %q, want %q", got, "Not Found: user 42 not found")
	}
}

func TestAppErrorToProblemDetail(t *testing.T) {
	err := ErrNotFound("user 42 not found")
	pd := err.ToProblemDetail("/api/users/42")

	if pd.Status != http.StatusNotFound {
		t.Errorf("Status = %d, want %d", pd.Status, http.StatusNotFound)
	}
	if pd.Type != "https://neutron.dev/errors/not-found" {
		t.Errorf("Type = %q", pd.Type)
	}
	if pd.Instance != "/api/users/42" {
		t.Errorf("Instance = %q", pd.Instance)
	}
}

func TestErrValidationProblemDetail(t *testing.T) {
	verrs := []ValidationError{
		{Field: "email", Message: "must be a valid email address", Value: "bad"},
	}
	err := ErrValidation("Request body failed validation", verrs)
	pd := err.ToProblemDetail("/api/users")

	if pd.Status != http.StatusUnprocessableEntity {
		t.Errorf("Status = %d, want 422", pd.Status)
	}
	if len(pd.Errors) != 1 {
		t.Fatalf("expected 1 validation error, got %d", len(pd.Errors))
	}
	if pd.Errors[0].Field != "email" {
		t.Errorf("field = %q, want email", pd.Errors[0].Field)
	}
}

func TestWriteErrorAppError(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/users/42", nil)

	WriteError(w, r, ErrNotFound("user not found"))

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if ct != "application/problem+json; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}

	var pd ProblemDetail
	if err := json.NewDecoder(w.Body).Decode(&pd); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if pd.Status != 404 {
		t.Errorf("pd.Status = %d", pd.Status)
	}
}

func TestWriteErrorGenericError(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)

	WriteError(w, r, http.ErrAbortHandler)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

func TestAllErrorConstructors(t *testing.T) {
	tests := []struct {
		name   string
		err    *AppError
		status int
	}{
		{"BadRequest", ErrBadRequest("bad"), http.StatusBadRequest},
		{"Unauthorized", ErrUnauthorized("auth"), http.StatusUnauthorized},
		{"Forbidden", ErrForbidden("forbid"), http.StatusForbidden},
		{"NotFound", ErrNotFound("missing"), http.StatusNotFound},
		{"Conflict", ErrConflict("dup"), http.StatusConflict},
		{"RateLimited", ErrRateLimited("slow"), http.StatusTooManyRequests},
		{"Internal", ErrInternal("oops"), http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Status != tt.status {
				t.Errorf("Status = %d, want %d", tt.err.Status, tt.status)
			}
		})
	}
}
