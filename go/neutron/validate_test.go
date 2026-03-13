package neutron

import (
	"testing"
)

func TestValidateRequired(t *testing.T) {
	type Input struct {
		Name string `json:"name" validate:"required"`
	}
	errs := Validate(Input{})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
	if errs[0].Field != "name" {
		t.Errorf("field = %q", errs[0].Field)
	}
	if errs[0].Message != "is required" {
		t.Errorf("message = %q", errs[0].Message)
	}
}

func TestValidateRequiredPasses(t *testing.T) {
	type Input struct {
		Name string `json:"name" validate:"required"`
	}
	errs := Validate(Input{Name: "Alice"})
	if len(errs) != 0 {
		t.Errorf("expected no errors, got %v", errs)
	}
}

func TestValidateMinMax(t *testing.T) {
	type Input struct {
		Name string `json:"name" validate:"min=3,max=10"`
	}
	errs := Validate(Input{Name: "ab"})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if errs[0].Field != "name" {
		t.Errorf("field = %q", errs[0].Field)
	}
}

func TestValidateMinMaxPass(t *testing.T) {
	type Input struct {
		Name string `json:"name" validate:"min=3,max=10"`
	}
	errs := Validate(Input{Name: "Alice"})
	if len(errs) != 0 {
		t.Errorf("expected no errors, got %v", errs)
	}
}

func TestValidateEmail(t *testing.T) {
	type Input struct {
		Email string `json:"email" validate:"email"`
	}

	errs := Validate(Input{Email: "not-an-email"})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	errs = Validate(Input{Email: "user@example.com"})
	if len(errs) != 0 {
		t.Errorf("expected no errors for valid email, got %v", errs)
	}
}

func TestValidateOneof(t *testing.T) {
	type Input struct {
		Role string `json:"role" validate:"oneof=admin user moderator"`
	}

	errs := Validate(Input{Role: "hacker"})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	errs = Validate(Input{Role: "admin"})
	if len(errs) != 0 {
		t.Errorf("expected no errors, got %v", errs)
	}
}

func TestValidateOmitempty(t *testing.T) {
	type Input struct {
		Age int `json:"age" validate:"omitempty,gte=0,lte=150"`
	}
	// Zero value with omitempty should pass
	errs := Validate(Input{Age: 0})
	if len(errs) != 0 {
		t.Errorf("expected no errors for omitempty zero value, got %v", errs)
	}

	// Non-zero out of range should fail
	errs = Validate(Input{Age: 200})
	if len(errs) == 0 {
		t.Error("expected error for age=200")
	}
}

func TestValidateGteLte(t *testing.T) {
	type Input struct {
		Score int `json:"score" validate:"gte=1,lte=100"`
	}

	errs := Validate(Input{Score: 0})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error for score=0, got %d", len(errs))
	}

	errs = Validate(Input{Score: 50})
	if len(errs) != 0 {
		t.Errorf("expected no errors for score=50, got %v", errs)
	}
}

func TestValidateMultipleErrors(t *testing.T) {
	type Input struct {
		Name  string `json:"name" validate:"required"`
		Email string `json:"email" validate:"required,email"`
	}
	errs := Validate(Input{})
	if len(errs) < 2 {
		t.Errorf("expected at least 2 errors, got %d: %v", len(errs), errs)
	}
}

func TestValidateNonStruct(t *testing.T) {
	errs := Validate("hello")
	if errs != nil {
		t.Errorf("expected nil for non-struct, got %v", errs)
	}
}
