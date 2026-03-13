package neutron

import (
	"github.com/go-playground/validator/v10"
	"reflect"
	"strings"
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
func Validate(v any) []ValidationError {
	err := validate.Struct(v)
	if err == nil {
		return nil
	}

	validationErrors, ok := err.(validator.ValidationErrors)
	if !ok {
		// Non-struct or other error — match previous behaviour of returning nil
		return nil
	}

	out := make([]ValidationError, 0, len(validationErrors))
	for _, fe := range validationErrors {
		out = append(out, ValidationError{
			Field:   fe.Field(),
			Message: messageForTag(fe),
			Value:   fe.Value(),
		})
	}
	return out
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
