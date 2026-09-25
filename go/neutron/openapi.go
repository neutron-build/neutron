package neutron

import (
	"encoding"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"
)

// OpenAPISpec represents an OpenAPI 3.1 specification.
type OpenAPISpec struct {
	OpenAPI    string                     `json:"openapi"`
	Info       OpenAPIInfo                `json:"info"`
	Paths      map[string]OpenAPIPathItem `json:"paths"`
	Components *OpenAPIComponents         `json:"components,omitempty"`
	Security   []SecurityRequirement      `json:"security,omitempty"`
}

type OpenAPIInfo struct {
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
	Version     string `json:"version"`
}

type OpenAPIPathItem map[string]*OpenAPIOperation

type OpenAPIOperation struct {
	Summary     string                     `json:"summary,omitempty"`
	Description string                     `json:"description,omitempty"`
	OperationID string                     `json:"operationId,omitempty"`
	Tags        []string                   `json:"tags,omitempty"`
	Deprecated  bool                       `json:"deprecated,omitempty"`
	Parameters  []OpenAPIParameter         `json:"parameters,omitempty"`
	RequestBody *OpenAPIRequestBody        `json:"requestBody,omitempty"`
	Responses   map[string]OpenAPIResponse `json:"responses"`
}

type OpenAPIParameter struct {
	Name     string         `json:"name"`
	In       string         `json:"in"` // path, query, header
	Required bool           `json:"required,omitempty"`
	Schema   *OpenAPISchema `json:"schema"`
}

type OpenAPIRequestBody struct {
	Required bool                        `json:"required,omitempty"`
	Content  map[string]OpenAPIMediaType `json:"content"`
}

type OpenAPIMediaType struct {
	Schema *OpenAPISchema `json:"schema"`
}

type OpenAPIResponse struct {
	Description string                      `json:"description"`
	Content     map[string]OpenAPIMediaType `json:"content,omitempty"`
}

type OpenAPIComponents struct {
	Schemas         map[string]*OpenAPISchema  `json:"schemas,omitempty"`
	SecuritySchemes map[string]*SecurityScheme `json:"securitySchemes,omitempty"`
}

// SecurityScheme describes an OpenAPI 3.1 security scheme.
type SecurityScheme struct {
	Type             string      `json:"type"` // apiKey, http, oauth2, openIdConnect
	Description      string      `json:"description,omitempty"`
	Name             string      `json:"name,omitempty"`             // required for apiKey
	In               string      `json:"in,omitempty"`               // required for apiKey: query, header, cookie
	Scheme           string      `json:"scheme,omitempty"`           // required for http: bearer, basic, etc.
	BearerFormat     string      `json:"bearerFormat,omitempty"`     // optional hint for http/bearer
	Flows            *OAuthFlows `json:"flows,omitempty"`            // required for oauth2
	OpenIDConnectURL string      `json:"openIdConnectUrl,omitempty"` // required for openIdConnect
}

// OAuthFlows describes the available OAuth2 flows.
type OAuthFlows struct {
	Implicit          *OAuthFlow `json:"implicit,omitempty"`
	Password          *OAuthFlow `json:"password,omitempty"`
	ClientCredentials *OAuthFlow `json:"clientCredentials,omitempty"`
	AuthorizationCode *OAuthFlow `json:"authorizationCode,omitempty"`
}

// OAuthFlow describes a single OAuth2 flow.
type OAuthFlow struct {
	AuthorizationURL string            `json:"authorizationUrl,omitempty"`
	TokenURL         string            `json:"tokenUrl,omitempty"`
	RefreshURL       string            `json:"refreshUrl,omitempty"`
	Scopes           map[string]string `json:"scopes"`
}

// SecurityRequirement maps scheme names to required scopes.
// For schemes that don't use scopes (e.g. bearer), use an empty slice.
type SecurityRequirement map[string][]string

// AddSecurityScheme registers a named security scheme in the spec's components.
func (s *OpenAPISpec) AddSecurityScheme(name string, scheme SecurityScheme) {
	if s.Components == nil {
		s.Components = &OpenAPIComponents{}
	}
	if s.Components.SecuritySchemes == nil {
		s.Components.SecuritySchemes = make(map[string]*SecurityScheme)
	}
	s.Components.SecuritySchemes[name] = &scheme
}

// AddGlobalSecurity appends one or more security requirements to the spec's
// top-level security field. Each requirement is applied globally to all
// operations unless overridden at the operation level.
func (s *OpenAPISpec) AddGlobalSecurity(requirements ...SecurityRequirement) {
	s.Security = append(s.Security, requirements...)
}

// BearerAuthScheme returns a SecurityScheme for HTTP Bearer token authentication.
func BearerAuthScheme() SecurityScheme {
	return SecurityScheme{
		Type:         "http",
		Scheme:       "bearer",
		BearerFormat: "JWT",
		Description:  "Bearer token authentication (JWT)",
	}
}

// APIKeyScheme returns a SecurityScheme for API key authentication.
// name is the header/query/cookie parameter name (e.g. "X-API-Key").
// in is the location: "header", "query", or "cookie".
func APIKeyScheme(name, in string) SecurityScheme {
	return SecurityScheme{
		Type:        "apiKey",
		Name:        name,
		In:          in,
		Description: "API key via " + in + " parameter \"" + name + "\"",
	}
}

// OAuth2Scheme returns a SecurityScheme for OAuth 2.0 authentication.
func OAuth2Scheme(flows OAuthFlows) SecurityScheme {
	return SecurityScheme{
		Type:        "oauth2",
		Flows:       &flows,
		Description: "OAuth 2.0 authentication",
	}
}

type OpenAPISchema struct {
	Type                 string                    `json:"type,omitempty"`
	Format               string                    `json:"format,omitempty"`
	Properties           map[string]*OpenAPISchema `json:"properties,omitempty"`
	AdditionalProperties *OpenAPISchema            `json:"additionalProperties,omitempty"`
	Required             []string                  `json:"required,omitempty"`
	Items                *OpenAPISchema            `json:"items,omitempty"`
	Ref                  string                    `json:"$ref,omitempty"`
	Description          string                    `json:"description,omitempty"`
	Enum                 []string                  `json:"enum,omitempty"`
	Minimum              *float64                  `json:"minimum,omitempty"`
	Maximum              *float64                  `json:"maximum,omitempty"`
	MinLength            *int                      `json:"minLength,omitempty"`
	MaxLength            *int                      `json:"maxLength,omitempty"`
	// Nullable marks a value that may also be JSON null. OpenAPI 3.1 has no
	// `nullable` keyword: it serializes as a type array (`["string","null"]`)
	// or, for a $ref, as `oneOf: [{$ref}, {type: "null"}]`.
	Nullable bool `json:"-"`
}

// MarshalJSON renders Nullable in OpenAPI 3.1 form.
func (s *OpenAPISchema) MarshalJSON() ([]byte, error) {
	type plain OpenAPISchema
	if !s.Nullable || (s.Type == "" && s.Ref == "") {
		// An untyped schema already admits null.
		return json.Marshal((*plain)(s))
	}
	if s.Ref != "" {
		return json.Marshal(struct {
			OneOf       []map[string]string `json:"oneOf"`
			Description string              `json:"description,omitempty"`
		}{
			OneOf:       []map[string]string{{"$ref": s.Ref}, {"type": "null"}},
			Description: s.Description,
		})
	}
	out := struct {
		*plain
		Type []string `json:"type"`
		Enum []any    `json:"enum,omitempty"`
	}{plain: (*plain)(s), Type: []string{s.Type, "null"}}
	if len(s.Enum) > 0 {
		for _, e := range s.Enum {
			out.Enum = append(out.Enum, e)
		}
		out.Enum = append(out.Enum, nil)
	}
	return json.Marshal(out)
}

// generateOpenAPI builds the OpenAPI spec from registered routes.
func generateOpenAPI(routes []routeRecord, info OpenAPIInfo) *OpenAPISpec {
	spec := &OpenAPISpec{
		OpenAPI: "3.1.0",
		Info:    info,
		Paths:   make(map[string]OpenAPIPathItem),
		Components: &OpenAPIComponents{
			Schemas: make(map[string]*OpenAPISchema),
		},
	}

	// The standard problem detail schema, matching what WriteError emits:
	// instance and errors are omitted when empty, and a validation error's
	// value is omitted when unset.
	spec.Components.Schemas["ProblemDetail"] = &OpenAPISchema{
		Type: "object",
		Properties: map[string]*OpenAPISchema{
			"type":     {Type: "string"},
			"title":    {Type: "string"},
			"status":   {Type: "integer", Format: "int32"},
			"detail":   {Type: "string"},
			"instance": {Type: "string"},
			"errors": {
				Type: "array",
				Items: &OpenAPISchema{
					Type: "object",
					Properties: map[string]*OpenAPISchema{
						"field":   {Type: "string"},
						"message": {Type: "string"},
						"value":   {},
					},
					Required: []string{"field", "message"},
				},
			},
		},
		Required: []string{"type", "title", "status", "detail"},
	}

	emptyType := reflect.TypeOf(Empty{})
	isEmpty := func(t reflect.Type) bool {
		return t == nil || t == emptyType || (t.Kind() == reflect.Ptr && t.Elem() == emptyType)
	}
	hasRequestBody := func(route routeRecord) bool {
		return !isEmpty(route.InType) && route.InType.Kind() == reflect.Struct && hasBody(route.Method)
	}

	b := newSchemaBuilder(spec.Components.Schemas)
	// A named type used both as a request body and as a response gets one
	// component per direction.
	inBodies := make(map[reflect.Type]bool)
	for _, route := range routes {
		if !route.Untyped && hasRequestBody(route) {
			if t := componentType(route.InType); t != nil {
				inBodies[t] = true
			}
		}
	}
	for _, route := range routes {
		if !route.Untyped && !isEmpty(route.OutType) {
			if t := componentType(route.OutType); t != nil && inBodies[t] {
				b.both[t] = true
			}
		}
	}

	problem := func(desc string) OpenAPIResponse {
		return OpenAPIResponse{
			Description: desc,
			Content: map[string]OpenAPIMediaType{
				"application/problem+json": {
					Schema: &OpenAPISchema{Ref: "#/components/schemas/ProblemDetail"},
				},
			},
		}
	}

	for _, route := range routes {
		// Untyped routes (Handle/HandleFunc/Mount) carry no request or response
		// schema, so they cannot be described. They are recorded for Routes()
		// and route-table inspection, but publishing them here would fill the
		// spec with entries that document nothing.
		if route.Untyped {
			continue
		}
		method := strings.ToLower(route.Method)
		pattern := route.Pattern

		if _, ok := spec.Paths[pattern]; !ok {
			spec.Paths[pattern] = make(OpenAPIPathItem)
		}

		op := &OpenAPIOperation{
			Summary:     route.Options.Summary,
			Description: route.Options.Description,
			Tags:        route.Options.Tags,
			Deprecated:  route.Options.Deprecated,
			OperationID: route.Options.OperationID,
			Responses:   make(map[string]OpenAPIResponse),
		}

		// Parameters from path, query, header tags
		if !isEmpty(route.InType) && route.InType.Kind() == reflect.Struct {
			for i := 0; i < route.InType.NumField(); i++ {
				f := route.InType.Field(i)
				if pathKey := f.Tag.Get("path"); pathKey != "" {
					op.Parameters = append(op.Parameters, OpenAPIParameter{
						Name:     pathKey,
						In:       "path",
						Required: true,
						Schema:   b.typeSchema(f.Type, dirRequest, false),
					})
				}
				if queryKey := f.Tag.Get("query"); queryKey != "" {
					op.Parameters = append(op.Parameters, OpenAPIParameter{
						Name:   queryKey,
						In:     "query",
						Schema: b.typeSchema(f.Type, dirRequest, false),
					})
				}
				if headerKey := f.Tag.Get("header"); headerKey != "" {
					op.Parameters = append(op.Parameters, OpenAPIParameter{
						Name:   headerKey,
						In:     "header",
						Schema: b.typeSchema(f.Type, dirRequest, false),
					})
				}
			}

			// Request body for methods that accept a body
			if hasRequestBody(route) {
				op.RequestBody = &OpenAPIRequestBody{
					Required: true,
					Content: map[string]OpenAPIMediaType{
						"application/json": {Schema: b.bodySchema(route.InType, dirRequest)},
					},
				}
			}
		}

		// Response
		if !isEmpty(route.OutType) {
			schema := b.bodySchema(route.OutType, dirResponse)
			// POST operations use 201 Created, everything else uses 200 OK
			statusCode := "200"
			statusDesc := "Successful response"
			if route.Method == "POST" {
				statusCode = "201"
				statusDesc = "Created"
			}
			op.Responses[statusCode] = OpenAPIResponse{
				Description: statusDesc,
				Content: map[string]OpenAPIMediaType{
					"application/json": {Schema: schema},
				},
			}
		} else {
			op.Responses["204"] = OpenAPIResponse{Description: "No content"}
		}

		// Error responses. 422 is only reachable when the input carries
		// validate rules; the handler validates every non-Empty input.
		op.Responses["400"] = problem("Bad Request")
		op.Responses["404"] = problem("Not Found")
		if !isEmpty(route.InType) && hasValidation(route.InType, map[reflect.Type]bool{}) {
			op.Responses["422"] = problem("Validation Failed")
		}
		op.Responses["500"] = problem("Internal Server Error")

		spec.Paths[pattern][method] = op
	}

	return spec
}

// schemaDirection distinguishes request bodies from responses. The same Go
// type means different things in each: a request field is required when
// validation demands it; a response field is required when encoding/json
// always emits it.
type schemaDirection int

const (
	dirRequest schemaDirection = iota
	dirResponse
)

type componentKey struct {
	t   reflect.Type
	dir schemaDirection
}

// schemaBuilder generates component and inline schemas for one spec.
type schemaBuilder struct {
	schemas map[string]*OpenAPISchema
	names   map[componentKey]string
	taken   map[string]bool
	// both holds named types used as a request body AND a response; their
	// request component is suffixed "Input" so neither direction weakens the
	// other.
	both     map[reflect.Type]bool
	visiting map[reflect.Type]bool
}

func newSchemaBuilder(schemas map[string]*OpenAPISchema) *schemaBuilder {
	b := &schemaBuilder{
		schemas:  schemas,
		names:    make(map[componentKey]string),
		taken:    make(map[string]bool),
		both:     make(map[reflect.Type]bool),
		visiting: make(map[reflect.Type]bool),
	}
	for name := range schemas {
		b.taken[name] = true
	}
	return b
}

var (
	timeType          = reflect.TypeOf(time.Time{})
	rawMessageType    = reflect.TypeOf(json.RawMessage(nil))
	jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
)

// componentType returns the named struct type that would become a component
// for a top-level body type (T, *T, []T, []*T), or nil.
func componentType(t reflect.Type) reflect.Type {
	if t == nil {
		return nil
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		t = t.Elem()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}
	if t.Kind() == reflect.Struct && t.Name() != "" && !isScalarStruct(t) {
		return t
	}
	return nil
}

// isScalarStruct reports struct types that encode as a JSON scalar.
func isScalarStruct(t reflect.Type) bool {
	return t == timeType || (t.Implements(textMarshalerType) && !t.Implements(jsonMarshalerType))
}

func (b *schemaBuilder) componentName(t reflect.Type, dir schemaDirection) string {
	key := componentKey{t, dir}
	if n, ok := b.names[key]; ok {
		return n
	}
	base := t.Name()
	if dir == dirRequest && b.both[t] {
		base += "Input"
	}
	name := base
	for i := 2; b.taken[name]; i++ {
		name = fmt.Sprintf("%s%d", base, i)
	}
	b.taken[name] = true
	b.names[key] = name
	return name
}

// ref returns a $ref to t's component for dir, generating it on first use.
func (b *schemaBuilder) ref(t reflect.Type, dir schemaDirection) *OpenAPISchema {
	name := b.componentName(t, dir)
	if _, exists := b.schemas[name]; !exists {
		b.schemas[name] = b.structSchema(t, dir)
	}
	return &OpenAPISchema{Ref: "#/components/schemas/" + name}
}

// bodySchema describes a top-level request or response body. Named structs
// (and slices of them) become component references; everything else inline.
func (b *schemaBuilder) bodySchema(t reflect.Type, dir schemaDirection) *OpenAPISchema {
	nullable := false
	if t.Kind() == reflect.Ptr {
		nullable = dir == dirResponse
		t = t.Elem()
	}
	var s *OpenAPISchema
	switch {
	case componentType(t) == t:
		s = b.ref(t, dir)
	case t.Kind() == reflect.Slice && componentType(t) != nil:
		items := b.ref(componentType(t), dir)
		items.Nullable = dir == dirResponse && t.Elem().Kind() == reflect.Ptr
		s = &OpenAPISchema{Type: "array", Items: items, Nullable: dir == dirResponse}
	default:
		s = b.typeSchema(t, dir, false)
	}
	if nullable {
		s.Nullable = true
	}
	return s
}

// typeSchema maps a Go type to the schema of its encoding/json form. In the
// response direction, kinds that encode nil as JSON null (pointer, slice,
// map) are nullable; interfaces get the unconstrained schema. viaPtr reports
// that the value is reached through a pointer, which makes pointer-receiver
// marshal methods apply.
func (b *schemaBuilder) typeSchema(t reflect.Type, dir schemaDirection, viaPtr bool) *OpenAPISchema {
	nullable := false
	for t.Kind() == reflect.Ptr {
		nullable = true
		viaPtr = true
		t = t.Elem()
	}
	s := b.valueSchema(t, dir, viaPtr)
	if nullable && dir == dirResponse {
		s.Nullable = true
	}
	return s
}

func (b *schemaBuilder) valueSchema(t reflect.Type, dir schemaDirection, viaPtr bool) *OpenAPISchema {
	implements := func(iface reflect.Type) bool {
		return t.Implements(iface) || (viaPtr && reflect.PointerTo(t).Implements(iface))
	}
	switch {
	case t == timeType:
		return &OpenAPISchema{Type: "string", Format: "date-time"}
	case t == rawMessageType:
		return &OpenAPISchema{}
	case !implements(jsonMarshalerType) && implements(textMarshalerType):
		return &OpenAPISchema{Type: "string"}
	}

	switch t.Kind() {
	case reflect.String:
		return &OpenAPISchema{Type: "string"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return &OpenAPISchema{Type: "integer", Format: "int32"}
	case reflect.Int64:
		return &OpenAPISchema{Type: "integer", Format: "int64"}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return &OpenAPISchema{Type: "integer", Format: "int32"}
	case reflect.Uint64:
		return &OpenAPISchema{Type: "integer", Format: "int64"}
	case reflect.Float32:
		return &OpenAPISchema{Type: "number", Format: "float"}
	case reflect.Float64:
		return &OpenAPISchema{Type: "number", Format: "double"}
	case reflect.Bool:
		return &OpenAPISchema{Type: "boolean"}
	case reflect.Slice:
		s := &OpenAPISchema{Type: "array", Nullable: dir == dirResponse}
		if t.Elem().Kind() == reflect.Uint8 {
			// encoding/json writes []byte as a base64 string.
			s = &OpenAPISchema{Type: "string", Format: "byte", Nullable: dir == dirResponse}
		} else {
			s.Items = b.typeSchema(t.Elem(), dir, false)
		}
		return s
	case reflect.Array:
		return &OpenAPISchema{Type: "array", Items: b.typeSchema(t.Elem(), dir, false)}
	case reflect.Map:
		return &OpenAPISchema{
			Type:                 "object",
			AdditionalProperties: b.typeSchema(t.Elem(), dir, false),
			Nullable:             dir == dirResponse,
		}
	case reflect.Interface:
		return &OpenAPISchema{}
	case reflect.Struct:
		return b.structSchema(t, dir)
	default:
		return &OpenAPISchema{Type: "string"}
	}
}

// structSchema builds an inline object schema for t in direction dir.
func (b *schemaBuilder) structSchema(t reflect.Type, dir schemaDirection) *OpenAPISchema {
	if b.visiting[t] {
		// Recursive type: stop expanding rather than recurse forever.
		return &OpenAPISchema{Type: "object"}
	}
	b.visiting[t] = true
	defer delete(b.visiting, t)

	s := &OpenAPISchema{Type: "object", Properties: make(map[string]*OpenAPISchema)}
	b.addFields(s, t, dir, false)
	return s
}

// addFields adds t's JSON-visible fields to s, promoting the fields of
// untagged embedded structs the way encoding/json does (a shallower field
// wins a name). viaNilPtr marks fields promoted through an embedded pointer,
// which vanish from the output when that pointer is nil.
func (b *schemaBuilder) addFields(s *OpenAPISchema, t reflect.Type, dir schemaDirection, viaNilPtr bool) {
	type embed struct {
		t   reflect.Type
		ptr bool
	}
	var embedded []embed
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, opts, _ := strings.Cut(tag, ",")
		if f.Anonymous && name == "" {
			ft := f.Type
			ptr := ft.Kind() == reflect.Ptr
			if ptr {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				if b.visiting[ft] {
					continue
				}
				embedded = append(embedded, embed{ft, ptr})
				continue
			}
		}
		if !f.IsExported() {
			continue
		}
		if name == "" {
			name = f.Name
		}
		if _, dup := s.Properties[name]; dup {
			continue
		}

		prop := b.typeSchema(f.Type, dir, false)
		addValidationConstraints(prop, f)
		omitted := hasJSONOption(opts, "omitempty") || hasJSONOption(opts, "omitzero")
		if dir == dirResponse {
			// An omitted-when-empty field is absent rather than null.
			if omitted {
				prop.Nullable = false
			} else if !viaNilPtr {
				s.Required = append(s.Required, name)
			}
		} else if isRequired(f) {
			s.Required = append(s.Required, name)
		}
		s.Properties[name] = prop
	}
	for _, e := range embedded {
		b.visiting[e.t] = true
		b.addFields(s, e.t, dir, viaNilPtr || e.ptr)
		delete(b.visiting, e.t)
	}
}

func hasJSONOption(opts, want string) bool {
	for opts != "" {
		var o string
		o, opts, _ = strings.Cut(opts, ",")
		if o == want {
			return true
		}
	}
	return false
}

// hasValidation reports whether validating a value of t can fail, i.e. t
// (or a struct it contains) carries validate rules.
func hasValidation(t reflect.Type, seen map[reflect.Type]bool) bool {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array || t.Kind() == reflect.Map {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || seen[t] {
		return false
	}
	seen[t] = true
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if v := f.Tag.Get("validate"); v != "" && v != "-" {
			return true
		}
		if hasValidation(f.Type, seen) {
			return true
		}
	}
	return false
}

func isRequired(f reflect.StructField) bool {
	tag := f.Tag.Get("validate")
	if tag == "" {
		return false
	}
	for _, rule := range strings.Split(tag, ",") {
		if strings.TrimSpace(rule) == "required" {
			return true
		}
	}
	return false
}

func addValidationConstraints(s *OpenAPISchema, f reflect.StructField) {
	tag := f.Tag.Get("validate")
	if tag == "" {
		return
	}
	kind := f.Type.Kind()
	if kind == reflect.Ptr {
		kind = f.Type.Elem().Kind()
	}
	for _, rule := range strings.Split(tag, ",") {
		rule = strings.TrimSpace(rule)
		if strings.HasPrefix(rule, "min=") {
			// For strings, this is minLength; for numbers, minimum
			if kind == reflect.String {
				if v := parseConstraint(rule); v != nil {
					n := int(*v)
					s.MinLength = &n
				}
			} else {
				s.Minimum = parseConstraint(rule)
			}
		}
		if strings.HasPrefix(rule, "max=") {
			if kind == reflect.String {
				if v := parseConstraint(rule); v != nil {
					n := int(*v)
					s.MaxLength = &n
				}
			} else {
				s.Maximum = parseConstraint(rule)
			}
		}
		if strings.HasPrefix(rule, "oneof=") {
			s.Enum = strings.Fields(strings.TrimPrefix(rule, "oneof="))
		}
		if rule == "email" {
			s.Format = "email"
		}
	}
}

func parseConstraint(rule string) *float64 {
	parts := strings.SplitN(rule, "=", 2)
	if len(parts) != 2 {
		return nil
	}
	var v float64
	if _, err := json.Number(parts[1]).Float64(); err == nil {
		v, _ = json.Number(parts[1]).Float64()
		return &v
	}
	return nil
}

// OpenAPIJSON returns an http.Handler that serves the OpenAPI spec as JSON.
func OpenAPIJSON(spec *OpenAPISpec) http.Handler {
	data, _ := json.MarshalIndent(spec, "", "  ")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
}

// SwaggerUI returns an http.Handler that serves a simple Swagger UI.
func SwaggerUI(spec *OpenAPISpec) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(swaggerHTML))
	})
}

const swaggerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>API Documentation</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
SwaggerUIBundle({ url: '/openapi.json', dom_id: '#swagger-ui' });
</script>
</body>
</html>`
