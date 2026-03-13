package neutronrealtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSSEHandler(t *testing.T) {
	handler := SSEHandler(func(ctx interface{ Done() <-chan struct{} }, send func(string, []byte) error) error {
		if err := send("message", []byte(`{"hello":"world"}`)); err != nil {
			return err
		}
		return nil
	})

	w := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest("GET", "/events", nil).WithContext(ctx)
	handler.ServeHTTP(w, r)

	body := w.Body.String()
	if !strings.Contains(body, "event: message") {
		t.Errorf("body missing event line: %q", body)
	}
	if !strings.Contains(body, `data: {"hello":"world"}`) {
		t.Errorf("body missing data line: %q", body)
	}

	ct := w.Header().Get("Content-Type")
	if ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}
