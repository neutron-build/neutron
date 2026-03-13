package neutron

import (
	"context"
	"encoding/json"
	"testing"
)

func TestOpenAPIGeneration(t *testing.T) {
	r := newRouter()

	type CreateUserInput struct {
		Name  string `json:"name" validate:"required,min=1,max=100"`
		Email string `json:"email" validate:"required,email"`
	}
	type UserResponse struct {
		ID    int64  `json:"id"`
		Name  string `json:"name"`
		Email string `json:"email"`
	}

	Post[CreateUserInput, UserResponse](r, "/users", func(ctx context.Context, input CreateUserInput) (UserResponse, error) {
		return UserResponse{ID: 1, Name: input.Name, Email: input.Email}, nil
	}, WithSummary("Create user"), WithTags("users"))

	Get[Empty, []UserResponse](r, "/users", func(ctx context.Context, _ Empty) ([]UserResponse, error) {
		return nil, nil
	}, WithSummary("List users"), WithTags("users"))

	spec := generateOpenAPI(r.routes, OpenAPIInfo{Title: "Test API", Version: "1.0.0"})

	if spec.OpenAPI != "3.1.0" {
		t.Errorf("openapi = %q", spec.OpenAPI)
	}
	if spec.Info.Title != "Test API" {
		t.Errorf("title = %q", spec.Info.Title)
	}

	// Check /users path exists
	pathItem, ok := spec.Paths["/users"]
	if !ok {
		t.Fatal("missing /users path")
	}

	// POST operation
	postOp := pathItem["post"]
	if postOp == nil {
		t.Fatal("missing POST /users operation")
	}
	if postOp.Summary != "Create user" {
		t.Errorf("summary = %q", postOp.Summary)
	}
	if postOp.RequestBody == nil {
		t.Fatal("missing request body")
	}

	// GET operation
	getOp := pathItem["get"]
	if getOp == nil {
		t.Fatal("missing GET /users operation")
	}
	if getOp.Summary != "List users" {
		t.Errorf("summary = %q", getOp.Summary)
	}

	// Check component schemas
	if _, ok := spec.Components.Schemas["CreateUserInput"]; !ok {
		t.Error("missing CreateUserInput schema")
	}
	if _, ok := spec.Components.Schemas["UserResponse"]; !ok {
		t.Error("missing UserResponse schema")
	}

	// Verify JSON serialization works
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(data) == 0 {
		t.Error("empty JSON output")
	}
}

func TestOpenAPIPathParams(t *testing.T) {
	r := newRouter()

	type GetUserInput struct {
		ID int64 `path:"id"`
	}
	type UserResponse struct {
		ID int64 `json:"id"`
	}

	Get[GetUserInput, UserResponse](r, "/users/{id}", func(ctx context.Context, input GetUserInput) (UserResponse, error) {
		return UserResponse{ID: input.ID}, nil
	})

	spec := generateOpenAPI(r.routes, OpenAPIInfo{Title: "Test", Version: "1.0.0"})

	pathItem, ok := spec.Paths["/users/{id}"]
	if !ok {
		t.Fatal("missing /users/{id} path")
	}

	getOp := pathItem["get"]
	if getOp == nil {
		t.Fatal("missing GET operation")
	}

	if len(getOp.Parameters) != 1 {
		t.Fatalf("expected 1 parameter, got %d", len(getOp.Parameters))
	}
	param := getOp.Parameters[0]
	if param.Name != "id" {
		t.Errorf("param name = %q", param.Name)
	}
	if param.In != "path" {
		t.Errorf("param in = %q", param.In)
	}
	if !param.Required {
		t.Error("path param should be required")
	}
}

func TestOpenAPIValidationConstraints(t *testing.T) {
	r := newRouter()

	type Input struct {
		Name string `json:"name" validate:"required,min=1,max=100"`
		Role string `json:"role" validate:"oneof=admin user"`
	}
	type Resp struct{}

	Post[Input, Resp](r, "/test", func(ctx context.Context, input Input) (Resp, error) {
		return Resp{}, nil
	})

	spec := generateOpenAPI(r.routes, OpenAPIInfo{Title: "Test", Version: "1.0.0"})

	schema, ok := spec.Components.Schemas["Input"]
	if !ok {
		t.Fatal("missing Input schema")
	}

	nameProp, ok := schema.Properties["name"]
	if !ok {
		t.Fatal("missing name property")
	}
	if nameProp.MinLength == nil || *nameProp.MinLength != 1 {
		t.Errorf("minLength = %v", nameProp.MinLength)
	}
	if nameProp.MaxLength == nil || *nameProp.MaxLength != 100 {
		t.Errorf("maxLength = %v", nameProp.MaxLength)
	}

	roleProp, ok := schema.Properties["role"]
	if !ok {
		t.Fatal("missing role property")
	}
	if len(roleProp.Enum) != 2 {
		t.Errorf("enum = %v", roleProp.Enum)
	}
}
