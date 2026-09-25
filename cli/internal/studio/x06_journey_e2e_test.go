package studio

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/inspect"
)

// TestStudioX06JourneyPostgresE2E is the PostgreSQL leg of V18 for X06: one
// application journey — a migration creates the schema, the app queries it,
// Studio inspects schema -> migrations -> queries -> plan -> rows -> models
// -> change events — through the REAL route table against a REAL disposable
// database, with psql-level oracle reads on a separate connection. Every
// stage names the model whose limits govern it and those limits are the
// engine's actual ones (live fsync/synchronous_commit read).
//
// Skipped unless NEUTRON_E2E_DATABASE_URL is set; NEUTRON_LIVE_REQUIRED=1
// makes a missing URL a failure. One x06_* database, dropped afterwards.
func TestStudioX06JourneyPostgresE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; X06 journey e2e skipped")
	}
	ctx := context.Background()
	dbName := fmt.Sprintf("x06_%d_%d", os.Getpid(), time.Now().UnixNano())
	dbURL := deriveStudioDatabaseURL(t, base, dbName)
	admin, err := db.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if !strings.HasPrefix(dbName, "x06_") {
			t.Errorf("refusing to drop %q", dbName)
			return
		}
		if err := admin.Exec(c, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName)); err != nil {
			t.Errorf("drop database: %v", err)
		}
	})

	oracle, err := db.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect oracle: %v", err)
	}
	defer oracle.Close()
	studioClient, err := db.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect studio: %v", err)
	}
	defer studioClient.Close()

	// The application's migrations directory: 001 creates the journey's
	// tables and is applied; 002 names only orders_archive and stays pending.
	migDir := t.TempDir()
	write := func(name, sql string) {
		if err := os.WriteFile(filepath.Join(migDir, name), []byte(sql), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("001_orders.up.sql", `CREATE TABLE customers (id bigserial PRIMARY KEY, email text NOT NULL, password_hash text);
CREATE TABLE orders (
  id bigint PRIMARY KEY,
  customer_id bigint NOT NULL REFERENCES customers(id),
  total numeric(12,2) NOT NULL
);
CREATE INDEX orders_customer_idx ON orders (customer_id);
`)
	write("002_archive.up.sql", "CREATE TABLE orders_archive (id bigint PRIMARY KEY);\n")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	store, err := newConnectionStore()
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{
		port: port, sessionToken: "x06-token", store: store,
		clients: map[string]*db.Client{"e2e": studioClient},
		saved:   &savedQueryStore{path: t.TempDir() + "/saved.json"},
	}
	s.SetMigrationsDir(migDir)
	mux, err := s.routes()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewUnstartedServer(s.corsMiddleware(mux))
	ts.Listener = ln
	ts.Start()
	defer ts.Close()

	getJourney := func(t *testing.T, table string) inspect.Journey {
		t.Helper()
		res, err := http.Get(ts.URL + "/api/inspect/journey?connectionId=e2e&schema=public&table=" + table)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("journey status %d", res.StatusCode)
		}
		var raw struct {
			inspect.Journey
			Stages []struct {
				Stage  string          `json:"stage"`
				Model  string          `json:"model"`
				Status string          `json:"status"`
				Reason string          `json:"reason"`
				Data   json.RawMessage `json:"data"`
			} `json:"stages"`
		}
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			t.Fatal(err)
		}
		j := raw.Journey
		j.Stages = nil
		for _, st := range raw.Stages {
			j.Stages = append(j.Stages, inspect.Stage{Stage: st.Stage, Model: st.Model, Status: st.Status, Reason: st.Reason, Data: st.Data})
		}
		return j
	}
	stageData := func(t *testing.T, j inspect.Journey, name string, dst any) inspect.Stage {
		t.Helper()
		st := j.Stage(name)
		if st == nil {
			t.Fatalf("stage %s missing", name)
		}
		if dst != nil && st.Status == inspect.StageUnavailable {
			t.Fatalf("stage %s unavailable: %s", name, st.Reason)
		}
		if dst != nil && st.Data != nil {
			if err := json.Unmarshal(st.Data.(json.RawMessage), dst); err != nil {
				t.Fatalf("decode %s: %v", name, err)
			}
		}
		return *st
	}
	count := func(q string) int64 {
		var n int64
		if err := oracle.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatalf("oracle %q: %v", q, err)
		}
		return n
	}

	t.Run("before any migration the journey reads history without creating it", func(t *testing.T) {
		if err := oracle.Exec(ctx, `CREATE TABLE orders (id bigint PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		j := getJourney(t, "orders")
		var mig inspect.MigrationsData
		st := stageData(t, j, inspect.StageMigrations, &mig)
		if st.Status == inspect.StageUnavailable || mig.History != db.HistoryAbsent.String() {
			t.Fatalf("migrations stage %s history %q: %s", st.Status, mig.History, st.Reason)
		}
		if n := count(`SELECT count(*) FROM information_schema.tables WHERE table_name = '_neutron_migrations'`); n != 0 {
			t.Fatalf("journey created the history table (%d)", n)
		}
		if err := oracle.Exec(ctx, `DROP TABLE orders`); err != nil {
			t.Fatal(err)
		}
	})

	// Apply 001 through the CLI's migration machinery (lock, history row
	// and DDL in one transaction).
	sess, err := oracle.LockMigrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.EnsureMigrationTableV2(ctx); err != nil {
		t.Fatal(err)
	}
	files, err := db.ReadMigrationFiles(migDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.ApplyMigration(ctx, files[0]); err != nil {
		t.Fatal(err)
	}
	sess.Release()
	for _, stmt := range []string{
		`INSERT INTO customers (email, password_hash) VALUES ('a@x.test', 'h1'), ('b@x.test', 'h2')`,
		`INSERT INTO orders VALUES (1, 1, 9.50), (2, 2, 12.00), (3, 1, 4.25)`,
	} {
		if err := oracle.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	// The application's query runs through Studio's editor endpoint and is
	// recorded; a query on another table must not show up in the journey.
	for _, sql := range []string{"SELECT id, total FROM orders WHERE total > 5", "SELECT count(*) FROM customers"} {
		body, _ := json.Marshal(map[string]any{"sql": sql, "connectionId": "e2e"})
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/query", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Studio-Session", "x06-token")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("query %q: %d", sql, res.StatusCode)
		}
	}

	t.Run("limits are PostgreSQL's actual ones", func(t *testing.T) {
		res, err := http.Get(ts.URL + "/api/inspect/limits?connectionId=e2e")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var rep inspect.Report
		if err := json.NewDecoder(res.Body).Decode(&rep); err != nil {
			t.Fatal(err)
		}
		var fsync, syncCommit string
		if err := oracle.QueryRow(ctx, "SHOW fsync").Scan(&fsync); err != nil {
			t.Fatal(err)
		}
		if err := oracle.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommit); err != nil {
			t.Fatal(err)
		}
		if rep.Engine.Product != "postgres" || rep.Live == nil || rep.Live.Fsync != fsync || rep.Live.SynchronousCommit != syncCommit {
			t.Fatalf("engine %+v live %+v (oracle fsync=%s synchronous_commit=%s)", rep.Engine, rep.Live, fsync, syncCommit)
		}
		sql, _ := rep.Model("sql")
		if sql.Transaction != inspect.TxAtomic || sql.Availability != inspect.Supported {
			t.Fatalf("sql limits %+v", sql)
		}
		for _, m := range rep.Models[1:] {
			if m.Availability != inspect.Unsupported {
				t.Errorf("%s availability %s on PostgreSQL", m.Model, m.Availability)
			}
		}
	})

	t.Run("journey across every stage", func(t *testing.T) {
		before := count(`SELECT count(*) FROM orders`)
		j := getJourney(t, "orders")
		if j.Engine.Product != "postgres" || !j.Limits.Current {
			t.Fatalf("engine %+v current %v", j.Engine, j.Limits.Current)
		}
		order := []string{}
		for _, st := range j.Stages {
			order = append(order, st.Stage)
			if _, ok := j.Limits.Model(st.Model); !ok {
				t.Errorf("stage %s names model %q absent from the limits", st.Stage, st.Model)
			}
		}
		if strings.Join(order, ",") != "schema,migrations,queries,plan,rows,models,change-events" {
			t.Fatalf("stage order %v", order)
		}

		var schema inspect.SchemaData
		stageData(t, j, inspect.StageSchema, &schema)
		var oracleCols int64 = count(`SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='orders'`)
		if int64(len(schema.Columns)) != oracleCols || strings.Join(schema.KeyColumns, ",") != "id" {
			t.Fatalf("schema columns %d (oracle %d) key %v", len(schema.Columns), oracleCols, schema.KeyColumns)
		}
		if len(schema.References) != 1 || schema.References[0].Name != "customers" || schema.References[0].Columns[0] != "customer_id" {
			t.Fatalf("references %+v", schema.References)
		}

		var mig inspect.MigrationsData
		stageData(t, j, inspect.StageMigrations, &mig)
		if len(mig.Entries) != 2 || mig.Mentioning != 1 {
			t.Fatalf("migrations %+v", mig)
		}
		e1, e2 := mig.Entries[0], mig.Entries[1]
		if e1.Version != "001" || !e1.Applied || e1.Checksum != "verified" || !e1.Mentions || !strings.Contains(strings.Join(e1.Lines, "\n"), "CREATE TABLE orders") {
			t.Fatalf("001 %+v", e1)
		}
		// orders_archive is a different identifier: no mention.
		if e2.Version != "002" || e2.Applied || e2.Checksum != "pending" || e2.Mentions {
			t.Fatalf("002 %+v", e2)
		}

		var queries inspect.QueriesData
		stageData(t, j, inspect.StageQueries, &queries)
		if len(queries.Entries) != 1 || queries.Entries[0].SQL != "SELECT id, total FROM orders WHERE total > 5" {
			t.Fatalf("queries %+v", queries.Entries)
		}

		var plan struct {
			Statement string `json:"statement"`
			Plan      []struct {
				Plan map[string]any `json:"Plan"`
			} `json:"plan"`
			Executed bool `json:"executed"`
		}
		stageData(t, j, inspect.StagePlan, &plan)
		if plan.Executed || len(plan.Plan) != 1 || plan.Plan[0].Plan["Node Type"] != "Limit" {
			t.Fatalf("plan %+v", plan)
		}
		if _, has := plan.Plan[0].Plan["Actual Rows"]; has {
			t.Fatal("plan carries ANALYZE output: the statement was executed")
		}

		var rows inspect.RowsData
		stageData(t, j, inspect.StageRows, &rows)
		if len(rows.Rows) != 3 || rows.IDColumn != "id" || strings.Join(rows.Columns, ",") != "id,customer_id,total" {
			t.Fatalf("rows %+v", rows)
		}
		// Lossless: numeric(12,2) comes back as its exact text.
		if rows.Rows[0][2] != "9.50" {
			t.Fatalf("numeric cell %#v", rows.Rows[0][2])
		}

		for _, name := range []string{inspect.StageModels, inspect.StageChangeEvents} {
			st := stageData(t, j, name, nil)
			if st.Status != inspect.StageUnavailable || st.Reason == "" {
				t.Fatalf("%s on PostgreSQL: %+v", name, st)
			}
		}
		if after := count(`SELECT count(*) FROM orders`); after != before {
			t.Fatalf("journey changed rows: %d -> %d", before, after)
		}
	})

	t.Run("a tampered applied migration shows a checksum mismatch", func(t *testing.T) {
		write("001_orders.up.sql", "-- edited after apply\n"+files[0].SQL)
		j := getJourney(t, "orders")
		var mig inspect.MigrationsData
		stageData(t, j, inspect.StageMigrations, &mig)
		if mig.Entries[0].Checksum != "mismatch" {
			t.Fatalf("tampered 001: %+v", mig.Entries[0])
		}
	})

	t.Run("a missing table is reported, not invented", func(t *testing.T) {
		j := getJourney(t, "no_such_table")
		if st := j.Stage(inspect.StageSchema); st.Status != inspect.StageUnavailable {
			t.Fatalf("schema of a missing table: %+v", st)
		}
		if st := j.Stage(inspect.StageRows); st.Status != inspect.StageUnavailable {
			t.Fatalf("rows of a missing table: %+v", st)
		}
	})

	t.Run("journey endpoints are GET-only and need a connection", func(t *testing.T) {
		res, err := http.Get(ts.URL + "/api/inspect/journey?connectionId=nope&schema=public&table=orders")
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("unknown connection: %d", res.StatusCode)
		}
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/inspect/limits?connectionId=e2e", nil)
		req.Header.Set("X-Studio-Session", "x06-token")
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("POST limits: %d", res.StatusCode)
		}
	})
}
