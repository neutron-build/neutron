package ormhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/neutron"
	"github.com/neutron-build/neutron/go/orm"
)

func TestPostgresNeutronRequestScopesRollbackAndShutdown(t *testing.T) {
	url := os.Getenv("NEUTRON_ORM_TEST_DATABASE_URL")
	if url == "" {
		if os.Getenv("NEUTRON_ORM_REQUIRE_LIVE") == "1" {
			t.Fatal("native URL required")
		}
		t.Skip("disposable native PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal("invalid native configuration")
	}
	marker := fmt.Sprintf("orm_http_%d", time.Now().UnixNano())
	config.MaxConns = 1
	config.ConnConfig.RuntimeParams["application_name"] = marker
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	table := `"` + marker + `".records`
	if _, err := admin.Exec(ctx, `CREATE SCHEMA "`+marker+`"; CREATE TABLE `+table+`(id bigint PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		clean, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := admin.Exec(clean, `DROP SCHEMA "`+marker+`" CASCADE`); err != nil {
			t.Error(err)
		}
	}()
	lifetime, err := NewTransactions(pool, Options{MaxResponseBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	retained := make(chan *orm.Scope, 4)
	handler, err := lifetime.Handler(func(ctx context.Context, session RequestSession, r *http.Request) (Response, error) {
		retained <- session.Scope
		id := int64(1)
		switch r.URL.Path {
		case "/fail":
			id = 2
		case "/oversize":
			id = 3
		case "/block":
			id = 4
		}
		if _, err := session.Executor.Exec(ctx, "INSERT INTO "+table+" VALUES($1)", id); err != nil {
			return Response{}, err
		}
		if r.URL.Path == "/fail" {
			return Response{}, errors.New("native_secret_canary")
		}
		if r.URL.Path == "/oversize" {
			return Response{Body: []byte("oversized")}, nil
		}
		if r.URL.Path == "/block" {
			rows, err := session.Executor.Query(ctx, "SELECT pg_catalog.pg_sleep(30)")
			if err != nil {
				return Response{}, err
			}
			defer rows.Close()
			for rows.Next() {
			}
			return Response{}, rows.Err()
		}
		if session.Metrics.Snapshot().Calls != 1 {
			return Response{}, errors.New("request counters leaked")
		}
		return Response{Body: []byte("ok")}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	app := neutron.New(neutron.WithoutDefaultRoutes())
	app.Router().Handle("/", handler)
	server := httptest.NewServer(app.Handler())
	defer server.Close()
	client := server.Client()
	for _, path := range []string{"/ok", "/fail", "/oversize"} {
		response, err := client.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		expected := http.StatusServiceUnavailable
		if path == "/ok" {
			expected = http.StatusOK
		}
		if response.StatusCode != expected || strings.Contains(string(body), "native_secret_canary") {
			t.Fatal("response boundary")
		}
		previous := <-retained
		if _, err := previous.Exec(ctx, "SELECT 1"); !errors.Is(err, orm.ErrScopeClosed) {
			t.Fatal("retained request scope usable", err)
		}
	}
	finished := make(chan error, 1)
	go func() {
		response, err := client.Get(server.URL + "/block")
		if err == nil {
			_, err = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
		finished <- err
	}()
	blocked := false
	for !blocked {
		var count int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE application_name=$1 AND state='active' AND query LIKE '%pg_sleep(30)%'`, marker).Scan(&count); err != nil {
			t.Fatal(err)
		}
		blocked = count == 1
		if !blocked {
			select {
			case err := <-finished:
				t.Fatal("blocked request ended early", err)
			case <-ctx.Done():
				t.Fatal("native blocked backend missing")
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	if err := lifetime.Shutdown(shutdown); err != nil {
		t.Fatal("native bounded shutdown", err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-shutdown.Done():
		t.Fatal("HTTP request did not settle")
	}
	previous := <-retained
	if _, err := previous.Exec(ctx, "SELECT 1"); !errors.Is(err, orm.ErrScopeClosed) {
		t.Fatal("canceled retained scope", err)
	}
	var nativeIDs []int64
	rows, err := admin.Query(ctx, "SELECT id FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		nativeIDs = append(nativeIDs, id)
	}
	rows.Close()
	if rows.Err() != nil || len(nativeIDs) != 1 || nativeIDs[0] != 1 {
		t.Fatal("native request rollback oracle", nativeIDs)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal("adapter closed borrowed pool or leaked operation", err)
	}
	rejected, err := client.Get(server.URL + "/ok")
	if err != nil {
		t.Fatal(err)
	}
	rejected.Body.Close()
	if rejected.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("shutdown admitted request")
	}
	pool.Close()
	var backendCount int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE application_name=$1`, marker).Scan(&backendCount); err != nil || backendCount != 0 {
		t.Fatal("owned pool close left native backends", err)
	}
}
