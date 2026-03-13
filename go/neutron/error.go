package neutron

import (
	"fmt"
	"net/http"
)

// AppError is an application error that carries HTTP status and RFC 7807 problem details.
type AppError struct {
	Status int            `json:"-"`
	Code   string         `json:"type"`
	Title  string         `json:"title"`
	Detail string         `json:"detail"`
	Meta   map[string]any `json:"extensions,omitempty"`
}

func (e *AppError) Error() string {
	return fmt.Sprintf("%s: %s", e.Title, e.Detail)
}

// ProblemDetail is the RFC 7807 JSON representation.
type ProblemDetail struct {
	Type     string             `json:"type"`
	Title    string             `json:"title"`
	Status   int                `json:"status"`
	Detail   string             `json:"detail"`
	Instance string             `json:"instance,omitempty"`
	Errors   []ValidationError  `json:"errors,omitempty"`
	Meta     map[string]any     `json:"extensions,omitempty"`
}

// ValidationError represents a single field validation failure.
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Value   any    `json:"value,omitempty"`
}

const errBaseURL = "https://neutron.dev/errors/"

func newAppError(status int, code, title, detail string) *AppError {
	return &AppError{
		Status: status,
		Code:   errBaseURL + code,
		Title:  title,
		Detail: detail,
	}
}

func ErrBadRequest(detail string) *AppError {
	return newAppError(http.StatusBadRequest, "bad-request", "Bad Request", detail)
}

func ErrUnauthorized(detail string) *AppError {
	return newAppError(http.StatusUnauthorized, "unauthorized", "Unauthorized", detail)
}

func ErrForbidden(detail string) *AppError {
	return newAppError(http.StatusForbidden, "forbidden", "Forbidden", detail)
}

func ErrNotFound(detail string) *AppError {
	return newAppError(http.StatusNotFound, "not-found", "Not Found", detail)
}

func ErrConflict(detail string) *AppError {
	return newAppError(http.StatusConflict, "conflict", "Conflict", detail)
}

func ErrValidation(detail string, errs []ValidationError) *AppError {
	e := newAppError(http.StatusUnprocessableEntity, "validation", "Validation Failed", detail)
	e.Meta = map[string]any{"errors": errs}
	return e
}

func ErrRateLimited(detail string) *AppError {
	return newAppError(http.StatusTooManyRequests, "rate-limited", "Rate Limited", detail)
}

func ErrInternal(detail string) *AppError {
	return newAppError(http.StatusInternalServerError, "internal", "Internal Server Error", detail)
}

// ToProblemDetail converts an AppError to the RFC 7807 wire format.
func (e *AppError) ToProblemDetail(instance string) ProblemDetail {
	pd := ProblemDetail{
		Type:     e.Code,
		Title:    e.Title,
		Status:   e.Status,
		Detail:   e.Detail,
		Instance: instance,
	}
	if errs, ok := e.Meta["errors"]; ok {
		if ve, ok := errs.([]ValidationError); ok {
			pd.Errors = ve
		}
	}
	return pd
}
