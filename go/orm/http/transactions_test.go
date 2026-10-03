package ormhttp

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestResponseBoundsSnapshotBeforeCommit(t *testing.T) {
	body := []byte("ok")
	header := http.Header{"X-Value": []string{"one"}}
	response, err := snapshotResponse(Response{Body: body, Header: header}, 2)
	if err != nil {
		t.Fatal(err)
	}
	body[0] = 'x'
	header["X-Value"][0] = "changed"
	if string(response.Body) != "ok" || response.Header.Get("X-Value") != "one" || response.Status != 200 {
		t.Fatal("mutable response snapshot")
	}
	for _, response := range []Response{{Body: []byte("long")}, {Status: 199}, {Status: 204, Body: []byte("x")}, {Header: http.Header{"Bad\rKey": []string{"x"}}}, {Header: http.Header{"X-Value": []string{"secret\nheader"}}}} {
		if _, err := snapshotResponse(response, 2); !errors.Is(err, ErrResponse) {
			t.Fatal("response refusal", err)
		}
	}
}
func TestRequestShutdownStopsAdmissionCancelsAndRetainsBudget(t *testing.T) {
	lifetime, err := NewTransactions(&pgxpool.Pool{}, Options{MaxResponseBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	request, release, ok := lifetime.acquire(context.Background())
	if !ok {
		t.Fatal("admission")
	}
	budget, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := lifetime.Shutdown(budget); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(request.Err(), context.Canceled) {
		t.Fatal("bounded noncooperative shutdown", err)
	}
	if _, _, ok := lifetime.acquire(context.Background()); ok {
		t.Fatal("new request admitted after shutdown")
	}
	release()
	if err := lifetime.Shutdown(context.Background()); err != nil {
		t.Fatal("settled shutdown retry", err)
	}
	if err := lifetime.Shutdown(context.Background()); err != nil {
		t.Fatal("idempotent shutdown", err)
	}
}
