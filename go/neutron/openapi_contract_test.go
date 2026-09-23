package neutron

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type oaAddress struct {
	City string `json:"city"`
	Zip  string `json:"zip,omitempty"`
}

type oaBase struct {
	CreatedAt time.Time `json:"created_at"`
}

type oaAudit struct {
	By string `json:"by"`
}

type oaUser struct {
	oaBase
	*oaAudit
	ID       int64          `json:"id"`
	Name     string         `json:"name" validate:"required"`
	Nick     string         `json:"nick,omitempty"`
	Bio      *string        `json:"bio"`
	Avatar   *string        `json:"avatar,omitempty"`
	Tags     []string       `json:"tags"`
	Labels   []string       `json:"labels,omitzero"`
	Attrs    map[string]int `json:"attrs"`
	Extra    any            `json:"extra"`
	Home     oaAddress      `json:"home"`
	Work     *oaAddress     `json:"work"`
	Blob     []byte         `json:"blob"`
	Secret   string         `json:"-"`
	Dash     string         `json:"-,"`
	Untagged bool
	private  string //nolint:unused
}

type oaNode struct {
	Name     string   `json:"name"`
	Children []oaNode `json:"children"`
}

func oaSpecJSON(t *testing.T, routes []routeRecord) map[string]any {
	t.Helper()
	data, err := json.Marshal(generateOpenAPI(routes, OpenAPIInfo{Title: "T", Version: "1"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

func oaDig(t *testing.T, v any, path ...string) any {
	t.Helper()
	for _, p := range path {
		m, ok := v.(map[string]any)
		if !ok {
			t.Fatalf("at %q: not an object: %#v", p, v)
		}
		if v, ok = m[p]; !ok {
			t.Fatalf("missing %q in %v", p, keys(m))
		}
	}
	return v
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func strs(v any) []string {
	var out []string
	for _, s := range v.([]any) {
		out = append(out, s.(string))
	}
	sort.Strings(out)
	return out
}

// Response schemas: required iff encoding/json always emits the field;
// nullable iff it can be emitted as null.
func TestOpenAPIResponseRequiredAndNullable(t *testing.T) {
	r := newRouter()
	Get[Empty, oaUser](r, "/user", func(context.Context, Empty) (oaUser, error) { return oaUser{}, nil })
	spec := oaSpecJSON(t, *r.routes)
	user := oaDig(t, spec, "components", "schemas", "oaUser").(map[string]any)

	wantRequired := []string{"-", "Untagged", "attrs", "bio", "blob", "created_at", "extra", "home", "id", "name", "tags", "work"}
	if got := strs(user["required"]); !reflect.DeepEqual(got, wantRequired) {
		t.Errorf("required = %v\n want %v", got, wantRequired)
	}

	props := user["properties"].(map[string]any)
	for _, absent := range []string{"Secret", "private", "oaBase", "oaAudit"} {
		if _, ok := props[absent]; ok {
			t.Errorf("property %q must not be documented", absent)
		}
	}

	typeOf := func(name string) any { return oaDig(t, props, name, "type") }
	cases := map[string]any{
		"id":         "integer",
		"name":       "string",
		"nick":       "string",
		"bio":        []any{"string", "null"},
		"avatar":     "string", // omitempty pointer: absent, never null
		"tags":       []any{"array", "null"},
		"labels":     "array", // omitzero slice: absent, never null
		"attrs":      []any{"object", "null"},
		"home":       "object",
		"work":       []any{"object", "null"},
		"blob":       []any{"string", "null"},
		"created_at": "string",
		"by":         "string",
		"Untagged":   "boolean",
	}
	for name, want := range cases {
		if got := typeOf(name); !reflect.DeepEqual(got, want) {
			t.Errorf("%s type = %v, want %v", name, got, want)
		}
	}
	if _, ok := props["extra"].(map[string]any)["type"]; ok {
		t.Errorf("interface field must be unconstrained, got %v", props["extra"])
	}
	if got := oaDig(t, props, "created_at", "format"); got != "date-time" {
		t.Errorf("created_at format = %v", got)
	}
	if got := oaDig(t, props, "attrs", "additionalProperties", "type"); got != "integer" {
		t.Errorf("attrs additionalProperties = %v", got)
	}

	// Nested struct: same response rules applied inline.
	home := props["home"].(map[string]any)
	if got := strs(home["required"]); !reflect.DeepEqual(got, []string{"city"}) {
		t.Errorf("home.required = %v, want [city]", got)
	}
}

// Request bodies keep validate-derived required; a type used in both
// directions gets a separate request component.
func TestOpenAPIDirectionSpecificComponents(t *testing.T) {
	r := newRouter()
	Post[oaUser, oaUser](r, "/user", func(_ context.Context, u oaUser) (oaUser, error) { return u, nil })
	spec := oaSpecJSON(t, *r.routes)
	schemas := oaDig(t, spec, "components", "schemas").(map[string]any)

	in := schemas["oaUserInput"].(map[string]any)
	if got := strs(in["required"]); !reflect.DeepEqual(got, []string{"name"}) {
		t.Errorf("request required = %v, want [name]", got)
	}
	if got := oaDig(t, in, "properties", "bio", "type"); got != "string" {
		t.Errorf("request bio type = %v, want plain string", got)
	}
	out := schemas["oaUser"].(map[string]any)
	if len(out["required"].([]any)) < 10 {
		t.Errorf("response required = %v", out["required"])
	}

	post := oaDig(t, spec, "paths", "/user", "post").(map[string]any)
	if got := oaDig(t, post, "requestBody", "content", "application/json", "schema", "$ref"); got != "#/components/schemas/oaUserInput" {
		t.Errorf("request ref = %v", got)
	}
	if got := oaDig(t, post, "responses", "201", "content", "application/json", "schema", "$ref"); got != "#/components/schemas/oaUser" {
		t.Errorf("response ref = %v", got)
	}
}

// A type used only as a request keeps its plain name and old semantics.
func TestOpenAPIRequestOnlyComponentName(t *testing.T) {
	r := newRouter()
	Post[oaUser, Empty](r, "/user", func(context.Context, oaUser) (Empty, error) { return Empty{}, nil })
	spec := oaSpecJSON(t, *r.routes)
	u := oaDig(t, spec, "components", "schemas", "oaUser").(map[string]any)
	if got := strs(u["required"]); !reflect.DeepEqual(got, []string{"name"}) {
		t.Errorf("required = %v, want [name]", got)
	}
}

func TestOpenAPITopLevelNullability(t *testing.T) {
	r := newRouter()
	Get[Empty, *oaAddress](r, "/ptr", func(context.Context, Empty) (*oaAddress, error) { return nil, nil })
	Get[Empty, []oaAddress](r, "/list", func(context.Context, Empty) ([]oaAddress, error) { return nil, nil })
	Get[Empty, []*oaAddress](r, "/ptrs", func(context.Context, Empty) ([]*oaAddress, error) { return nil, nil })
	Get[Empty, oaAddress](r, "/val", func(context.Context, Empty) (oaAddress, error) { return oaAddress{}, nil })
	spec := oaSpecJSON(t, *r.routes)

	schema := func(path string) any {
		return oaDig(t, spec, "paths", path, "get", "responses", "200", "content", "application/json", "schema")
	}
	ref := map[string]any{"$ref": "#/components/schemas/oaAddress"}
	nullRef := map[string]any{"oneOf": []any{ref, map[string]any{"type": "null"}}}
	if got := schema("/ptr"); !reflect.DeepEqual(got, nullRef) {
		t.Errorf("*T = %v", got)
	}
	if got := schema("/val"); !reflect.DeepEqual(got, ref) {
		t.Errorf("T = %v", got)
	}
	if got := schema("/list"); !reflect.DeepEqual(got, map[string]any{"type": []any{"array", "null"}, "items": ref}) {
		t.Errorf("[]T = %v", got)
	}
	if got := schema("/ptrs"); !reflect.DeepEqual(got, map[string]any{"type": []any{"array", "null"}, "items": nullRef}) {
		t.Errorf("[]*T = %v", got)
	}
}

func TestOpenAPIRecursiveTypeTerminates(t *testing.T) {
	r := newRouter()
	Get[Empty, oaNode](r, "/tree", func(context.Context, Empty) (oaNode, error) { return oaNode{}, nil })
	spec := oaSpecJSON(t, *r.routes)
	oaDig(t, spec, "components", "schemas", "oaNode", "properties", "children", "items")
}

func TestOpenAPINullableEnumAdmitsNull(t *testing.T) {
	s := &OpenAPISchema{Type: "string", Enum: []string{"a", "b"}, Nullable: true}
	data, _ := json.Marshal(s)
	if string(data) != `{"type":["string","null"],"enum":["a","b",null]}` {
		t.Errorf("got %s", data)
	}
}

// 404 on every typed operation, 422 only where validation can fail, and a
// ProblemDetail schema that matches the wire (errors[] of {field, message}).
func TestOpenAPIErrorResponses(t *testing.T) {
	type Create struct {
		Name string `json:"name" validate:"required"`
	}
	type Plain struct {
		Name string `json:"name"`
	}
	type Search struct {
		Q string `query:"q" validate:"max=10"`
	}
	r := newRouter()
	Post[Create, Plain](r, "/validated", func(context.Context, Create) (Plain, error) { return Plain{}, nil })
	Post[Plain, Plain](r, "/unvalidated", func(context.Context, Plain) (Plain, error) { return Plain{}, nil })
	Get[Empty, Plain](r, "/empty", func(context.Context, Empty) (Plain, error) { return Plain{}, nil })
	Get[Search, Plain](r, "/search", func(context.Context, Search) (Plain, error) { return Plain{}, nil })
	spec := oaSpecJSON(t, *r.routes)

	codes := func(path, method string) []string {
		return keys(oaDig(t, spec, "paths", path, method, "responses").(map[string]any))
	}
	for path, want := range map[string][]string{
		"/validated":   {"201", "400", "404", "422", "500"},
		"/unvalidated": {"201", "400", "404", "500"},
		"/empty":       {"200", "400", "404", "500"},
		"/search":      {"200", "400", "404", "422", "500"},
	} {
		method := "post"
		if path == "/empty" || path == "/search" {
			method = "get"
		}
		if got := codes(path, method); !reflect.DeepEqual(got, want) {
			t.Errorf("%s responses = %v, want %v", path, got, want)
		}
	}
	for _, code := range []string{"404", "422"} {
		got := oaDig(t, spec, "paths", "/validated", "post", "responses", code, "content", "application/problem+json", "schema", "$ref")
		if got != "#/components/schemas/ProblemDetail" {
			t.Errorf("%s schema = %v", code, got)
		}
	}

	pd := oaDig(t, spec, "components", "schemas", "ProblemDetail").(map[string]any)
	if got := strs(pd["required"]); !reflect.DeepEqual(got, []string{"detail", "status", "title", "type"}) {
		t.Errorf("ProblemDetail required = %v", got)
	}
	item := oaDig(t, pd, "properties", "errors", "items").(map[string]any)
	if got := strs(item["required"]); !reflect.DeepEqual(got, []string{"field", "message"}) {
		t.Errorf("errors item required = %v", got)
	}
	if got := keys(item["properties"].(map[string]any)); !reflect.DeepEqual(got, []string{"field", "message", "value"}) {
		t.Errorf("errors item properties = %v", got)
	}
}

// The documented ProblemDetail must describe what a real 422 sends.
func TestValidationWireMatchesProblemSchema(t *testing.T) {
	type Create struct {
		Name string `json:"name" validate:"required"`
	}
	app := New()
	Post[Create, Create](app.Router(), "/c", func(_ context.Context, c Create) (Create, error) { return c, nil })
	req := httptest.NewRequest("POST", "/c", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != 422 {
		t.Fatalf("status = %d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	documented := map[string]bool{"type": true, "title": true, "status": true, "detail": true, "instance": true, "errors": true}
	for k := range body {
		if !documented[k] {
			t.Errorf("wire member %q is not in the ProblemDetail schema", k)
		}
	}
	errs := body["errors"].([]any)
	first := errs[0].(map[string]any)
	if first["field"] != "name" || first["message"] != "is required" {
		t.Errorf("errors[0] = %v", first)
	}
}
