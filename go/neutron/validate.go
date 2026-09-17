package neutron

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
)

// validate is a package-level cached validator instance. Struct analysis is
// cached on first validation so subsequent calls for the same type are ~1μs.
var validate = validator.New(validator.WithRequiredStructEnabled())

func init() {
	// Use the json tag as the field name in error messages.
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := fld.Tag.Get("json")
		if name == "" || name == "-" {
			return fld.Name
		}
		if idx := strings.Index(name, ","); idx != -1 {
			name = name[:idx]
		}
		return name
	})
}

// Validate checks struct fields against their `validate` tags.
// Returns nil if validation passes, or a slice of ValidationError.
//
// Field errors deliberately do NOT carry the rejected raw value (GO-05): the
// value could be a password, token, or other secret, and echoing it into a
// response body or log leaks it. Callers needing the value for their own
// handling can re-read it from their input.
func Validate(v any) []ValidationError {
	errs, _ := validateInput(v)
	return errs
}

// validateInput separates field violations from validator configuration
// errors (GO-05). An *validator.InvalidValidationError — an unsupported
// target or a malformed tag — used to be swallowed as "no validation
// errors", hiding the mistake behind a 422-or-pass; it now surfaces as a
// distinct error the handler maps to a 500.
func validateInput(v any) ([]ValidationError, error) {
	err := validate.Struct(v)
	if err == nil {
		return nil, nil
	}

	var invalid *validator.InvalidValidationError
	if errors.As(err, &invalid) {
		return nil, fmt.Errorf("invalid validation target: %w", err)
	}

	var validationErrors validator.ValidationErrors
	if !errors.As(err, &validationErrors) {
		return nil, err
	}

	out := make([]ValidationError, 0, len(validationErrors))
	for _, fe := range validationErrors {
		out = append(out, ValidationError{
			Field:   fe.Field(),
			Message: messageForTag(fe),
		})
	}
	return out, nil
}

// messageForTag converts a validator.FieldError into a human-readable message.
func messageForTag(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required":
		return "is required"
	case "min":
		return "must be at least " + fe.Param()
	case "max":
		return "must be at most " + fe.Param()
	case "gte":
		return "must be greater than or equal to " + fe.Param()
	case "lte":
		return "must be less than or equal to " + fe.Param()
	case "email":
		return "must be a valid email address"
	case "oneof":
		return "must be one of: " + strings.ReplaceAll(fe.Param(), " ", ", ")
	default:
		return "failed on " + fe.Tag() + " validation"
	}
}
