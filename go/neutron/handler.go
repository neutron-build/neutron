package neutron

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"mime/multipart"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

// Empty is a sentinel type for handlers that take no input.
type Empty struct{}

// HandlerFunc is the core handler type. Input is extracted from the request
// (body, path, query, headers) and output is serialized to JSON.
type HandlerFunc[In, Out any] func(ctx context.Context, input In) (Out, error)

// Register registers a typed handler on the router.
func Register[In, Out any](r *Router, method, pattern string, h HandlerFunc[In, Out], opts ...RouteOption) {
	var options routeOptions
	for _, o := range opts {
		o(&options)
	}

	var in In
	var out Out
	inType := reflect.TypeOf(in)
	outType := reflect.TypeOf(out)

	// Unwrap pointer types for reflection
	if inType != nil && inType.Kind() == reflect.Ptr {
		inType = inType.Elem()
	}
	if outType != nil && outType.Kind() == reflect.Ptr {
		outType = outType.Elem()
	}

	emptyType := reflect.TypeOf(Empty{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var input In

		// Decode input unless it's Empty
		if inType != nil && inType != emptyType {
			rv := reflect.New(inType).Elem()
			inputPtr := rv.Addr().Interface()

			if hasBody(method) && req.Body != nil && req.ContentLength != 0 {
				ct := req.Header.Get("Content-Type")
				mediaType, _, _ := mime.ParseMediaType(ct)

				switch mediaType {
				case "multipart/form-data":
					if err := req.ParseMultipartForm(32 << 20); err != nil {
						WriteError(w, req, ErrBadRequest("Invalid multipart form: "+err.Error()))
						return
					}
					populateFromForm(rv, req.MultipartForm)
				case "application/x-www-form-urlencoded":
					if err := req.ParseForm(); err != nil {
						WriteError(w, req, ErrBadRequest("Invalid form data: "+err.Error()))
						return
					}
					populateFromURLValues(rv, req.Form)
				default:
					// Default: JSON binding
					if err := json.NewDecoder(req.Body).Decode(inputPtr); err != nil {
						WriteError(w, req, ErrBadRequest("Invalid JSON: "+err.Error()))
						return
					}
				}
			}

			// Extract path, query, header, and form params
			populateFromRequest(rv, req)

			input = rv.Interface().(In)

			// Validate
			if errs := Validate(input); len(errs) > 0 {
				WriteError(w, req, ErrValidation("Request body failed validation", errs))
				return
			}
		}

		output, err := h(req.Context(), input)
		if err != nil {
			WriteError(w, req, err)
			return
		}

		// Determine status code: 201 for POST, 200 otherwise
		status := http.StatusOK
		if method == http.MethodPost {
			status = http.StatusCreated
		}
		// Check if output is the zero value of an empty struct
		outVal := reflect.ValueOf(output)
		if !outVal.IsValid() || (outVal.Kind() == reflect.Struct && outVal.Type() == emptyType) {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		JSON(w, status, output)
	})

	r.register(method, pattern, handler, inType, outType, options)
}

// Convenience functions for common HTTP methods.

func Get[In, Out any](r *Router, pattern string, h HandlerFunc[In, Out], opts ...RouteOption) {
	Register(r, http.MethodGet, pattern, h, opts...)
}

func Post[In, Out any](r *Router, pattern string, h HandlerFunc[In, Out], opts ...RouteOption) {
	Register(r, http.MethodPost, pattern, h, opts...)
}

func Put[In, Out any](r *Router, pattern string, h HandlerFunc[In, Out], opts ...RouteOption) {
	Register(r, http.MethodPut, pattern, h, opts...)
}

func Patch[In, Out any](r *Router, pattern string, h HandlerFunc[In, Out], opts ...RouteOption) {
	Register(r, http.MethodPatch, pattern, h, opts...)
}

func Delete[In, Out any](r *Router, pattern string, h HandlerFunc[In, Out], opts ...RouteOption) {
	Register(r, http.MethodDelete, pattern, h, opts...)
}

func hasBody(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch
}

// populateFromRequest fills struct fields from path, query, and header parameters.
func populateFromRequest(rv reflect.Value, r *http.Request) {
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()
	if rt.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		fieldVal := rv.Field(i)

		if !fieldVal.CanSet() {
			continue
		}

		if pathKey := field.Tag.Get("path"); pathKey != "" {
			val := r.PathValue(pathKey)
			if val != "" {
				setFieldValue(fieldVal, val)
			}
		}

		if queryKey := field.Tag.Get("query"); queryKey != "" {
			val := r.URL.Query().Get(queryKey)
			if val != "" {
				setFieldValue(fieldVal, val)
			}
		}

		if headerKey := field.Tag.Get("header"); headerKey != "" {
			val := r.Header.Get(headerKey)
			if val != "" {
				setFieldValue(fieldVal, val)
			}
		}

		// form tag — populated from URL-encoded or multipart form data
		if formKey := field.Tag.Get("form"); formKey != "" {
			// For *multipart.FileHeader fields, extract from multipart files
			if field.Type == reflect.TypeOf((*multipart.FileHeader)(nil)) {
				if r.MultipartForm != nil && r.MultipartForm.File != nil {
					if files, ok := r.MultipartForm.File[formKey]; ok && len(files) > 0 {
						fieldVal.Set(reflect.ValueOf(files[0]))
					}
				}
				continue
			}
			// For regular fields, check form values
			if r.Form != nil {
				if val := r.Form.Get(formKey); val != "" {
					setFieldValue(fieldVal, val)
				}
			} else if r.MultipartForm != nil && r.MultipartForm.Value != nil {
				if vals, ok := r.MultipartForm.Value[formKey]; ok && len(vals) > 0 {
					setFieldValue(fieldVal, vals[0])
				}
			}
		}
	}
}

func setFieldValue(v reflect.Value, s string) {
	switch v.Kind() {
	case reflect.String:
		v.SetString(s)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			v.SetInt(n)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if n, err := strconv.ParseUint(s, 10, 64); err == nil {
			v.SetUint(n)
		}
	case reflect.Float32, reflect.Float64:
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			v.SetFloat(f)
		}
	case reflect.Bool:
		if b, err := strconv.ParseBool(s); err == nil {
			v.SetBool(b)
		}
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.String {
			parts := strings.Split(s, ",")
			v.Set(reflect.ValueOf(parts))
		}
	}
}

// populateFromForm fills struct fields from a parsed multipart form.
// Fields are matched via the `form` struct tag. *multipart.FileHeader fields
// are populated from the file map; all other fields use the value map.
func populateFromForm(rv reflect.Value, mf *multipart.Form) {
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()
	if rt.Kind() != reflect.Struct || mf == nil {
		return
	}

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		fieldVal := rv.Field(i)
		if !fieldVal.CanSet() {
			continue
		}
		formKey := field.Tag.Get("form")
		if formKey == "" {
			continue
		}

		// File upload field
		if field.Type == reflect.TypeOf((*multipart.FileHeader)(nil)) {
			if mf.File != nil {
				if files, ok := mf.File[formKey]; ok && len(files) > 0 {
					fieldVal.Set(reflect.ValueOf(files[0]))
				}
			}
			continue
		}

		// Regular value field
		if mf.Value != nil {
			if vals, ok := mf.Value[formKey]; ok && len(vals) > 0 {
				setFieldValue(fieldVal, vals[0])
			}
		}
	}
}

// populateFromURLValues fills struct fields from url.Values using `form` tags.
func populateFromURLValues(rv reflect.Value, values map[string][]string) {
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()
	if rt.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		fieldVal := rv.Field(i)
		if !fieldVal.CanSet() {
			continue
		}
		formKey := field.Tag.Get("form")
		if formKey == "" {
			continue
		}
		if vals, ok := values[formKey]; ok && len(vals) > 0 {
			setFieldValue(fieldVal, vals[0])
		}
	}
}

// typeNameForSchema returns the type name suitable for OpenAPI schema references.
func typeNameForSchema(t reflect.Type) string {
	if t == nil {
		return ""
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		elem := t.Elem()
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		return fmt.Sprintf("ArrayOf%s", elem.Name())
	}
	return t.Name()
}
