package studio

// X04: blob download range serving. The /api/blob/{id}/data handler serves
// through http.ServeContent, so HTTP Range requests are byte-exact — the
// Studio blob journey exposes partial reads, matching the client's
// getRange semantics. Skipped unless NEUTRON_E2E_DATABASE_URL is set;
// NEUTRON_LIVE_REQUIRED=1 turns a missing URL into a failure. Uses one
// uniquely-named x04_* database, dropped afterwards.

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

func TestBlobDataServesRangesE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; blob range e2e skipped")
	}

	dbName := fmt.Sprintf("x04_%d_%d", os.Getpid(), time.Now().Unix())
	dbURL := deriveStudioDatabaseURL(t, base, dbName)

	admin, err := db.Connect(context.Background(), base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Exec(context.Background(), fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatalf("create database %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := admin.Exec(ctx, fmt.Sprintf(
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()`, dbName,
		)); err != nil {
			t.Errorf("terminate backends: %v", err)
		}
		if !strings.HasPrefix(dbName, "x04_") {
			t.Errorf("refusing to drop unexpected database %q", dbName)
			return
		}
		if err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})

	fixture, err := db.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	defer fixture.Close()
	for _, stmt := range []string{
		`CREATE TABLE x04blobs (id text PRIMARY KEY, data bytea NOT NULL, content_type text, size int, hash text, created_at timestamptz)`,
	} {
		if err := fixture.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Deterministic 64-byte payload: byte i = (i*37+11)%256.
	payload := make([]byte, 64)
	for i := range payload {
		payload[i] = byte((i*37 + 11) % 256)
	}
	const hexID = "x04range01"
	if err := fixture.Exec(context.Background(),
		`INSERT INTO x04blobs (id, data, content_type, size, hash, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		hexID, payload, "application/x-x04", len(payload), hexID, time.Now(),
	); err != nil {
		t.Fatalf("insert blob: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	connStore, err := newConnectionStore()
	if err != nil {
		t.Fatalf("connection store: %v", err)
	}
	savedStore, err := newSavedQueryStore()
	if err != nil {
		t.Fatalf("saved query store: %v", err)
	}
	s := &Server{
		port: port, sessionToken: fmt.Sprintf("x04-token-%d", port),
		store: connStore, saved: savedStore, epochs: map[string]string{},
		clients: map[string]*db.Client{"e2e": fixture},
	}
	mux, err := s.routes()
	if err != nil {
		t.Fatalf("routes: %v", err)
	}
	ts := httptest.NewUnstartedServer(s.corsMiddleware(mux))
	ts.Listener = ln
	ts.Start()
	t.Cleanup(ts.Close)

	path := fmt.Sprintf("/api/blob/%s/data?connectionId=e2e&store=x04blobs", hexID)

	get := func(rangeHeader string) (*httptest.ResponseRecorder, []byte) {
		req := httptest.NewRequest("GET", path, nil)
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec, rec.Body.Bytes()
	}

	// Whole file.
	rec, body := get("")
	if rec.Code != 200 {
		t.Fatalf("plain GET: status %d", rec.Code)
	}
	if string(body) != string(payload) {
		t.Fatalf("plain GET: bytes differ")
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-x04" {
		t.Fatalf("plain GET: content-type %q", ct)
	}

	// First four bytes.
	rec, body = get("bytes=0-3")
	if rec.Code != 206 {
		t.Fatalf("bytes=0-3: status %d", rec.Code)
	}
	if string(body) != string(payload[:4]) {
		t.Fatalf("bytes=0-3: got %v want %v", body, payload[:4])
	}
	if cr := rec.Header().Get("Content-Range"); cr != "bytes 0-3/64" {
		t.Fatalf("bytes=0-3: content-range %q", cr)
	}

	// Suffix range: last five bytes.
	rec, body = get("bytes=-5")
	if rec.Code != 206 {
		t.Fatalf("bytes=-5: status %d", rec.Code)
	}
	if string(body) != string(payload[59:]) {
		t.Fatalf("bytes=-5: got %v want %v", body, payload[59:])
	}

	// EOF-adjacent open range: clamps to the end.
	rec, body = get("bytes=60-")
	if rec.Code != 206 {
		t.Fatalf("bytes=60-: status %d", rec.Code)
	}
	if string(body) != string(payload[60:]) {
		t.Fatalf("bytes=60-: got %v want %v", body, payload[60:])
	}

	// Unsatisfiable range.
	rec, _ = get("bytes=99999-")
	if rec.Code != 416 {
		t.Fatalf("bytes=99999-: status %d, want 416", rec.Code)
	}

	// Missing blob.
	req := httptest.NewRequest("GET", "/api/blob/nope/data?connectionId=e2e&store=x04blobs", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Fatalf("missing blob: status %d, want 404", rec.Code)
	}
}
