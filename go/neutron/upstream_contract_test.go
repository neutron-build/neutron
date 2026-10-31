package neutron

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestTypedBodyAdmissionUpstream(t *testing.T) {
	type input struct {
		Value string `json:"value" form:"value"`
	}
	for _, tc := range []struct {
		name, body, contentType string
		limit                   int64
		status                  int
	}{
		{"exact JSON", `{"value":"x"}`, "application/json", 13, 204},
		{"first JSON overflow", `{"value":"x"}`, "application/json", 12, 413},
		{"trailing whitespace overflow", `{"value":"x"} `, "application/json", 13, 413},
		{"second document", `{"value":"x"}{}`, "application/json", 100, 400},
		{"malformed tail", `{"value":"x"}x`, "application/json", 100, 400},
		{"exact form", "value=x", "application/x-www-form-urlencoded", 7, 204},
		{"form overflow", "value=x ", "application/x-www-form-urlencoded", 7, 413},
	} {
		for _, unknown := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unknown=%v", tc.name, unknown), func(t *testing.T) {
				called := false
				r := newRouter()
				Post(r, "/", func(context.Context, input) (Empty, error) { called = true; return Empty{}, nil }, WithBodyLimit(tc.limit))
				req := httptest.NewRequest("POST", "/", strings.NewReader(tc.body))
				if unknown {
					req.ContentLength = -1
				}
				req.Header.Set("Content-Type", tc.contentType)
				rec := httptest.NewRecorder()
				r.ServeHTTP(rec, req)
				if rec.Code != tc.status || called != (tc.status == 204) {
					t.Fatalf("status=%d called=%v body=%s", rec.Code, called, rec.Body.String())
				}
			})
		}
	}
}

func TestTypedDefaultBodyLimitAndMiddlewareCeiling(t *testing.T) {
	type input struct {
		Value string `json:"value"`
	}
	for _, middlewareLimit := range []int64{0, 16} {
		called := false
		r := newRouter()
		Post(r, "/", func(context.Context, input) (Empty, error) { called = true; return Empty{}, nil })
		var h http.Handler = r
		if middlewareLimit > 0 {
			h = BodyLimit(middlewareLimit)(h)
		}
		body := `{"value":"` + strings.Repeat("x", int(DefaultBodyLimit)) + `"}`
		req := httptest.NewRequest("POST", "/", strings.NewReader(body))
		req.ContentLength = -1
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 413 || called {
			t.Fatalf("status=%d called=%v", rec.Code, called)
		}
	}
}

func TestTypedMultipartAdmissionAndCleanup(t *testing.T) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	part, err := mw.CreateFormFile("file", "fixture.txt")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(part, strings.Repeat("x", 128))
	_ = mw.Close()
	type jsonOnly struct {
		Value string `json:"value"`
	}
	type upload struct {
		File *multipart.FileHeader `form:"file"`
	}
	for _, tc := range []struct {
		name    string
		allowed bool
		limit   int64
		status  int
	}{
		{"JSON-only refuses before parse", false, 1024, 415},
		{"multipart total overflow", true, 64, 413},
		{"multipart accepted", true, 1024, 204},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			r := newRouter()
			if tc.allowed {
				Post(r, "/", func(_ context.Context, in upload) (Empty, error) {
					called = true
					f, err := in.File.Open()
					if err != nil {
						t.Fatal(err)
					}
					defer f.Close()
					data, err := io.ReadAll(f)
					if err != nil || len(data) != 128 {
						t.Fatalf("file size=%d error=%v", len(data), err)
					}
					return Empty{}, nil
				}, WithBodyLimit(tc.limit))
			} else {
				Post(r, "/", func(context.Context, jsonOnly) (Empty, error) { called = true; return Empty{}, nil }, WithBodyLimit(tc.limit))
			}
			req := httptest.NewRequest("POST", "/", bytes.NewReader(body.Bytes()))
			req.ContentLength = -1
			req.Header.Set("Content-Type", mw.FormDataContentType())
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != tc.status || called != (tc.status == 204) {
				t.Fatalf("status=%d called=%v", rec.Code, called)
			}
			if !tc.allowed && req.MultipartForm != nil {
				t.Fatal("irrelevant multipart was parsed")
			}
		})
	}
	// Exercise cleanup ownership when middleware already spooled a file.
	req := httptest.NewRequest("POST", "/", bytes.NewReader(body.Bytes()))
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if err := req.ParseMultipartForm(1); err != nil {
		t.Fatal(err)
	}
	f, err := req.MultipartForm.File["file"][0].Open()
	if err != nil {
		t.Fatal(err)
	}
	disk, ok := f.(*os.File)
	if !ok {
		t.Fatal("fixture did not spool")
	}
	name := disk.Name()
	_ = f.Close()
	t.Cleanup(func() { _ = req.MultipartForm.RemoveAll() })
	r := newRouter()
	Post(r, "/", func(context.Context, upload) (Empty, error) { return Empty{}, ErrBadRequest("refused") }, WithBodyLimit(64))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != 413 {
		t.Fatalf("preparsed overflow status=%d", rec.Code)
	}
	if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary file survived: %v", err)
	}
}

func TestLifecycleMixedPrefixOwnsRollbackErrors(t *testing.T) {
	startErr, stopErr := errors.New("start"), errors.New("stop")
	var order []string
	key := struct{}{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key, "trace"))
	cancel()
	lc := newLifecycle(slog.New(slog.NewTextHandler(io.Discard, nil)))
	stop := func(name string, err error) func(context.Context) error {
		return func(ctx context.Context) error {
			if ctx.Err() != nil {
				t.Fatal("cleanup inherited cancelled startup")
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("cleanup lacks deadline")
			}
			if ctx.Value(key) != "trace" {
				t.Fatal("cleanup lost context value")
			}
			order = append(order, name)
			return err
		}
	}
	lc.add(
		LifecycleHook{Name: "a", OnStart: func(context.Context) error { return nil }, OnStop: stop("a", nil)},
		LifecycleHook{Name: "stop-only", OnStop: stop("stop-only", stopErr)},
		LifecycleHook{Name: "b", OnStart: func(context.Context) error { return nil }, OnStop: stop("b", nil)},
		LifecycleHook{Name: "fail", OnStart: func(context.Context) error { return startErr }, OnStop: stop("fail", nil)},
		LifecycleHook{Name: "unreached", OnStop: stop("unreached", nil)},
	)
	err := lc.start(ctx)
	if !errors.Is(err, startErr) || !errors.Is(err, stopErr) || !reflect.DeepEqual(order, []string{"b", "stop-only", "a"}) {
		t.Fatalf("order=%v err=%v", order, err)
	}
}

func TestProblemExtensionsSurviveWireConversion(t *testing.T) {
	for _, status := range []int{429, 504} {
		appErr := &AppError{Status: status, Code: "refused", Title: "Refused", Detail: "budget", Meta: map[string]any{"refusal_code": "budget", "errors": []ValidationError{{Field: "query", Message: "refused"}}}}
		rec := httptest.NewRecorder()
		WriteError(rec, httptest.NewRequest("GET", "/", nil), fmt.Errorf("wrapped: %w", appErr))
		var pd ProblemDetail
		if err := json.Unmarshal(rec.Body.Bytes(), &pd); err != nil {
			t.Fatal(err)
		}
		if rec.Code != status || pd.Meta["refusal_code"] != "budget" || len(pd.Errors) != 1 {
			t.Fatalf("status=%d problem=%+v", rec.Code, pd)
		}
		if _, duplicate := pd.Meta["errors"]; duplicate {
			t.Fatal("validation errors duplicated")
		}
		copy := appErr.ToProblemDetail("/")
		copy.Meta["refusal_code"] = "changed"
		if appErr.Meta["refusal_code"] != "budget" {
			t.Fatal("extension map aliased")
		}
	}
	if pd := ErrBadRequest("bad").ToProblemDetail("/"); pd.Meta != nil {
		t.Fatal("nil meta changed")
	}
}

func TestDynamicOpenAPIMatchesRuntime(t *testing.T) {
	r := newRouter()
	Get(r, "/dynamic", func(context.Context, Empty) (any, error) { return map[string]any{"value": 1}, nil })
	Post(r, "/dynamic", func(context.Context, Empty) (any, error) { return []string{"x"}, nil })
	Get(r, "/nil", func(context.Context, Empty) (any, error) { return nil, nil })
	spec := generateOpenAPI(r.snapshotRoutes(), OpenAPIInfo{})
	for _, tc := range []struct{ method, path, status string }{{"GET", "/dynamic", "200"}, {"POST", "/dynamic", "201"}, {"GET", "/nil", "204"}} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
		op := spec.Paths[tc.path][strings.ToLower(tc.method)]
		if fmt.Sprint(rec.Code) != tc.status {
			t.Fatalf("runtime=%d", rec.Code)
		}
		if _, ok := op.Responses[tc.status]; !ok {
			t.Fatalf("missing response %s: %+v", tc.status, op)
		}
		if tc.status != "204" {
			schema := op.Responses[tc.status].Content["application/json"].Schema
			if schema == nil || schema.Type != "" || schema.Ref != "" {
				t.Fatalf("dynamic schema=%+v", schema)
			}
		}
	}
}

func TestRawBodyLimitRetainsReadErrorControl(t *testing.T) {
	called := false
	raw := BodyLimit(4)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		_, err := io.ReadAll(r.Body)
		if err != nil {
			WriteError(w, r, bodyDecodeError("raw", err))
			return
		}
		w.WriteHeader(204)
	}))
	req := httptest.NewRequest("POST", "/", strings.NewReader("12345"))
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	raw.ServeHTTP(rec, req)
	if rec.Code != 413 || !called {
		t.Fatalf("raw status=%d called=%v", rec.Code, called)
	}
}
