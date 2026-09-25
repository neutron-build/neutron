package neutron

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
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
	// outType keeps its pointer: a nil *T handler result is sent as JSON
	// null, and the OpenAPI response schema must say so.

	emptyType := reflect.TypeOf(Empty{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var input In

		// Bind through the actual generic input variable (GO-01): the old
		// code reconstructed the input from a separately allocated value and
		// asserted it back to In, which panicked for pointer inputs (`In =
		// *T`) because the assertion ran from T to *T.
		rv := reflect.ValueOf(&input).Elem()
		isPointerInput := rv.Kind() == reflect.Ptr
		if isPointerInput {
			// Allocate the target even without a body so query/path/header
			// binding has somewhere to land.
			rv.Set(reflect.New(rv.Type().Elem()))
			rv = rv.Elem()
		}

		// Decode input unless it's Empty
		if inType != nil && inType != emptyType {
			if hasBody(method) && req.Body != nil && req.ContentLength != 0 {
				ct := req.Header.Get("Content-Type")
				mediaType, _, _ := mime.ParseMediaType(ct)

				switch mediaType {
				case "multipart/form-data":
					if err := req.ParseMultipartForm(32 << 20); err != nil {
						WriteError(w, req, ErrBadRequest("Invalid multipart form: "+err.Error()))
						return
					}
					if err := populateFromForm(rv, req.MultipartForm); err != nil {
						WriteError(w, req, ErrBadRequest("Invalid multipart form: "+err.Error()))
						return
					}
				case "application/x-www-form-urlencoded":
					if err := decodeURLEncodedBody(w, req, maxFormBodyBytes); err != nil {
						WriteError(w, req, formDecodeError(w, req, err))
						return
					}
					if err := populateFromURLValues(rv, req.PostForm); err != nil {
						WriteError(w, req, ErrBadRequest("Invalid form data: "+err.Error()))
						return
					}
				default:
					// Default: JSON binding, decoded through &input so both T
					// and *T work (GO-01). Exactly one JSON value; trailing
					// garbage or a second document is a 400 (GO-03).
					if err := decodeOneJSON(req.Body, &input); err != nil {
						WriteError(w, req, ErrBadRequest("Invalid JSON: "+err.Error()))
						return
					}
					// Re-derive after the decode (it may have replaced a
					// pointer input) and reject JSON `null` for it.
					rv = reflect.ValueOf(&input).Elem()
					if rv.Kind() == reflect.Ptr {
						if rv.IsNil() {
							WriteError(w, req, ErrBadRequest("Request body must not be null"))
							return
						}
						rv = rv.Elem()
					}
				}
			}

			// Extract path, query, header, and form params. Parse failures
			// are field errors now (GO-02): the old binder discarded every
			// error, parsed integers at 64 bits, and silently wrapped
			// out-of-range values into narrow fields.
			if err := populateFromRequest(rv, req); err != nil {
				WriteError(w, req, ErrBadRequest(err.Error()))
				return
			}

			// Validate. A validator configuration problem (unsupported
			// target, bad tags) is a 500, not a silent pass (GO-05); field
			// violations remain 422 and no longer echo the rejected raw
			// value, which could reflect a password or token into the
			// response.
			errs, verr := validateInput(input)
			if verr != nil {
				WriteError(w, req, ErrInternal("Validation configuration error"))
				return
			}
			if len(errs) > 0 {
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
	// DELETE can carry a typed body (GO-03): a typed Delete handler binds
	// its request body like POST/PUT/PATCH.
	return method == http.MethodPost ||
		method == http.MethodPut ||
		method == http.MethodPatch ||
		method == http.MethodDelete
}

// decodeOneJSON decodes exactly one JSON value and requires EOF afterwards
// (GO-03): a bare Decode accepted `{}{}` and trailing garbage, so a valid
// first document plus junk sailed through.
func decodeOneJSON(r io.Reader, dst any) error {
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain a single JSON value")
		}
		return errors.New("trailing data after JSON value")
	}
	return nil
}

// Upper bound on a URL-encoded request body read for form binding (GO-28).
const maxFormBodyBytes = 10 << 20 // 10 MiB, matching ParseMultipartForm's memory threshold scale

// decodeURLEncodedBody reads and parses an `application/x-www-form-urlencoded`
// BODY explicitly for every body-bearing method (GO-28): `Request.ParseForm`
// only populates PostForm from the body for POST/PUT/PATCH — Go's net/http
// ignores a DELETE body — so a typed DELETE handler silently received empty
// form fields. The body is read through MaxBytesReader (overflow maps to 413,
// not 400) and PostForm becomes the body-derived values; `form`-tagged fields
// bind from the BODY, while `query` tags keep binding from the URL.
func decodeURLEncodedBody(w http.ResponseWriter, r *http.Request, limit int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return err
	}
	r.PostForm = values
	// Rebuild the combined r.Form with BODY values taking Get() precedence
	// over query values (the same precedence ParseForm documents).
	combined := make(url.Values, len(values))
	for key, items := range values {
		combined[key] = append([]string(nil), items...)
	}
	for key, items := range r.URL.Query() {
		combined[key] = append(combined[key], items...)
	}
	r.Form = combined
	return nil
}

// formDecodeError classifies a form-body decode failure (GO-28): a
// MaxBytesReader overflow is a 413, everything else is a malformed 400.
func formDecodeError(_ http.ResponseWriter, _ *http.Request, err error) *AppError {
	var maxBytes *http.MaxBytesError
	if errors.As(err, &maxBytes) {
		return newAppError(
			http.StatusRequestEntityTooLarge,
			"payload-too-large",
			"Payload Too Large",
			"Form body exceeds the size limit",
		)
	}
	return newAppError(
		http.StatusBadRequest,
		"bad-request",
		"Invalid form data",
		err.Error(),
	)
}

// populateFromRequest fills struct fields from path, query, and header parameters.
// Binding failures return an error naming the parameter (GO-02).
func populateFromRequest(rv reflect.Value, r *http.Request) error {
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()
	if rt.Kind() != reflect.Struct {
		return nil
	}

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		fieldVal := rv.Field(i)

		if !fieldVal.CanSet() {
			continue
		}

		if pathKey := field.Tag.Get("path"); pathKey != "" {
			if val := r.PathValue(pathKey); val != "" {
				if err := bindText(fieldVal, val); err != nil {
					return fmt.Errorf("invalid path parameter %s: %w", pathKey, err)
				}
			}
		}

		if queryKey := field.Tag.Get("query"); queryKey != "" {
			if val := r.URL.Query().Get(queryKey); val != "" {
				if err := bindText(fieldVal, val); err != nil {
					return fmt.Errorf("invalid query parameter %s: %w", queryKey, err)
				}
			}
		}

		if headerKey := field.Tag.Get("header"); headerKey != "" {
			if val := r.Header.Get(headerKey); val != "" {
				if err := bindText(fieldVal, val); err != nil {
					return fmt.Errorf("invalid header %s: %w", headerKey, err)
				}
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
					if err := bindText(fieldVal, val); err != nil {
						return fmt.Errorf("invalid form field %s: %w", formKey, err)
					}
				}
			} else if r.MultipartForm != nil && r.MultipartForm.Value != nil {
				if vals, ok := r.MultipartForm.Value[formKey]; ok && len(vals) > 0 {
					if err := bindText(fieldVal, vals[0]); err != nil {
						return fmt.Errorf("invalid form field %s: %w", formKey, err)
					}
				}
			}
		}
	}
	return nil
}

// bindText sets v (a settable struct field) from a raw string with
// destination-width parsing and error reporting (GO-02). Integers parse at
// the destination's width, so int8("128") is a 400 rather than a silent -128;
// floats reject NaN/Infinity; named string element types are honored.
func bindText(v reflect.Value, raw string) error {
	if !v.CanSet() {
		return errors.New("field is not settable")
	}
	if v.Kind() == reflect.Ptr {
		n := reflect.New(v.Type().Elem())
		if err := bindText(n.Elem(), raw); err != nil {
			return err
		}
		v.Set(n)
		return nil
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(raw)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(raw, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(raw, v.Type().Bits())
		if err != nil {
			return err
		}
		if math.IsInf(n, 0) || math.IsNaN(n) {
			return errors.New("non-finite number")
		}
		v.SetFloat(n)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		v.SetBool(b)
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			return fmt.Errorf("unsupported slice element type %s", v.Type().Elem())
		}
		parts := strings.Split(raw, ",")
		n := reflect.MakeSlice(v.Type(), len(parts), len(parts))
		for i, s := range parts {
			n.Index(i).SetString(s)
		}
		v.Set(n)
	default:
		return fmt.Errorf("unsupported field type %s", v.Type())
	}
	return nil
}

// populateFromForm fills struct fields from a parsed multipart form.
// Fields are matched via the `form` struct tag. *multipart.FileHeader fields
// are populated from the file map; all other fields use the value map.
func populateFromForm(rv reflect.Value, mf *multipart.Form) error {
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()
	if rt.Kind() != reflect.Struct || mf == nil {
		return nil
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
				if err := bindText(fieldVal, vals[0]); err != nil {
					return fmt.Errorf("invalid form field %s: %w", formKey, err)
				}
			}
		}
	}
	return nil
}

// populateFromURLValues fills struct fields from url.Values using `form` tags.
func populateFromURLValues(rv reflect.Value, values map[string][]string) error {
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()
	if rt.Kind() != reflect.Struct {
		return nil
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
			if err := bindText(fieldVal, vals[0]); err != nil {
				return fmt.Errorf("invalid form field %s: %w", formKey, err)
			}
		}
	}
	return nil
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
