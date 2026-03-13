package neutron

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
)

// OpenAPISpec represents an OpenAPI 3.1 specification.
type OpenAPISpec struct {
	OpenAPI    string                    `json:"openapi"`
	Info       OpenAPIInfo               `json:"info"`
	Paths      map[string]OpenAPIPathItem `json:"paths"`
	Components *OpenAPIComponents        `json:"components,omitempty"`
	Security   []SecurityRequirement     `json:"security,omitempty"`
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
	Name     string        `json:"name"`
	In       string        `json:"in"` // path, query, header
	Required bool          `json:"required,omitempty"`
	Schema   *OpenAPISchema `json:"schema"`
}

type OpenAPIRequestBody struct {
	Required bool                       `json:"required,omitempty"`
	Content  map[string]OpenAPIMediaType `json:"content"`
}

type OpenAPIMediaType struct {
	Schema *OpenAPISchema `json:"schema"`
}

type OpenAPIResponse struct {
	Description string                     `json:"description"`
	Content     map[string]OpenAPIMediaType `json:"content,omitempty"`
}

type OpenAPIComponents struct {
	Schemas         map[string]*OpenAPISchema   `json:"schemas,omitempty"`
	SecuritySchemes map[string]*SecurityScheme   `json:"securitySchemes,omitempty"`
}

// SecurityScheme describes an OpenAPI 3.1 security scheme.
type SecurityScheme struct {
	Type             string      `json:"type"`                       // apiKey, http, oauth2, openIdConnect
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
	Type        string                    `json:"type,omitempty"`
	Format      string                    `json:"format,omitempty"`
	Properties  map[string]*OpenAPISchema `json:"properties,omitempty"`
	Required    []string                  `json:"required,omitempty"`
	Items       *OpenAPISchema            `json:"items,omitempty"`
	Ref         string                    `json:"$ref,omitempty"`
	Description string                    `json:"description,omitempty"`
	Enum        []string                  `json:"enum,omitempty"`
	Minimum     *float64                  `json:"minimum,omitempty"`
	Maximum     *float64                  `json:"maximum,omitempty"`
	MinLength   *int                      `json:"minLength,omitempty"`
	MaxLength   *int                      `json:"maxLength,omitempty"`
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

	// Add the standard problem detail schema
	spec.Components.Schemas["ProblemDetail"] = &OpenAPISchema{
		Type: "object",
		Properties: map[string]*OpenAPISchema{
			"type":     {Type: "string"},
			"title":    {Type: "string"},
			"status":   {Type: "integer", Format: "int32"},
			"detail":   {Type: "string"},
			"instance": {Type: "string"},
		},
		Required: []string{"type", "title", "status", "detail"},
	}

	for _, route := range routes {
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

		emptyType := reflect.TypeOf(Empty{})

		// Parameters from path, query, header tags
		if route.InType != nil && route.InType != emptyType && route.InType.Kind() == reflect.Struct {
			for i := 0; i < route.InType.NumField(); i++ {
				f := route.InType.Field(i)
				if pathKey := f.Tag.Get("path"); pathKey != "" {
					op.Parameters = append(op.Parameters, OpenAPIParameter{
						Name:     pathKey,
						In:       "path",
						Required: true,
						Schema:   schemaForType(f.Type),
					})
				}
				if queryKey := f.Tag.Get("query"); queryKey != "" {
					op.Parameters = append(op.Parameters, OpenAPIParameter{
						Name:   queryKey,
						In:     "query",
						Schema: schemaForType(f.Type),
					})
				}
				if headerKey := f.Tag.Get("header"); headerKey != "" {
					op.Parameters = append(op.Parameters, OpenAPIParameter{
						Name:   headerKey,
						In:     "header",
						Schema: schemaForType(f.Type),
					})
				}
			}

			// Request body for methods that accept a body
			if hasBody(route.Method) {
				schema := schemaForStructType(route.InType, spec.Components.Schemas)
				op.RequestBody = &OpenAPIRequestBody{
					Required: true,
					Content: map[string]OpenAPIMediaType{
						"application/json": {Schema: schema},
					},
				}
			}
		}

		// Response
		if route.OutType != nil && route.OutType != emptyType {
			schema := schemaForResponseType(route.OutType, spec.Components.Schemas)
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

		// Error responses
		op.Responses["400"] = OpenAPIResponse{
			Description: "Bad Request",
			Content: map[string]OpenAPIMediaType{
				"application/problem+json": {
					Schema: &OpenAPISchema{Ref: "#/components/schemas/ProblemDetail"},
				},
			},
		}
		op.Responses["500"] = OpenAPIResponse{
			Description: "Internal Server Error",
			Content: map[string]OpenAPIMediaType{
				"application/problem+json": {
					Schema: &OpenAPISchema{Ref: "#/components/schemas/ProblemDetail"},
				},
			},
		}

		spec.Paths[pattern][method] = op
	}

	return spec
}

func schemaForType(t reflect.Type) *OpenAPISchema {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
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
		return &OpenAPISchema{Type: "array", Items: schemaForType(t.Elem())}
	case reflect.Map:
		return &OpenAPISchema{Type: "object"}
	case reflect.Struct:
		return schemaForStructInline(t)
	default:
		return &OpenAPISchema{Type: "string"}
	}
}

func schemaForStructInline(t reflect.Type) *OpenAPISchema {
	s := &OpenAPISchema{
		Type:       "object",
		Properties: make(map[string]*OpenAPISchema),
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name := jsonFieldName(f)
		if name == "-" {
			continue
		}
		propSchema := schemaForType(f.Type)
		addValidationConstraints(propSchema, f)
		s.Properties[name] = propSchema

		if isRequired(f) {
			s.Required = append(s.Required, name)
		}
	}
	return s
}

func schemaForStructType(t reflect.Type, schemas map[string]*OpenAPISchema) *OpenAPISchema {
	name := t.Name()
	if name == "" {
		return schemaForStructInline(t)
	}
	if _, exists := schemas[name]; !exists {
		schemas[name] = schemaForStructInline(t)
	}
	return &OpenAPISchema{Ref: "#/components/schemas/" + name}
}

func schemaForResponseType(t reflect.Type, schemas map[string]*OpenAPISchema) *OpenAPISchema {
	if t.Kind() == reflect.Slice {
		elem := t.Elem()
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		if elem.Kind() == reflect.Struct {
			return &OpenAPISchema{
				Type:  "array",
				Items: schemaForStructType(elem, schemas),
			}
		}
		return &OpenAPISchema{Type: "array", Items: schemaForType(elem)}
	}
	if t.Kind() == reflect.Struct {
		return schemaForStructType(t, schemas)
	}
	return schemaForType(t)
}

func jsonFieldName(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if tag == "" {
		return f.Name
	}
	parts := strings.SplitN(tag, ",", 2)
	if parts[0] == "" {
		return f.Name
	}
	return parts[0]
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
	for _, rule := range strings.Split(tag, ",") {
		rule = strings.TrimSpace(rule)
		if strings.HasPrefix(rule, "min=") {
			// For strings, this is minLength; for numbers, minimum
			if f.Type.Kind() == reflect.String {
				if v := parseConstraint(rule); v != nil {
					n := int(*v)
					s.MinLength = &n
				}
			} else {
				s.Minimum = parseConstraint(rule)
			}
		}
		if strings.HasPrefix(rule, "max=") {
			if f.Type.Kind() == reflect.String {
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
