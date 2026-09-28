package studio

// S07 Studio regressions against REAL disposable s07_* Postgres databases,
// through the production route table: rule-bearing tables refuse edits
// with a 4xx, an invalid primary index is not a key, the retired legacy
// row endpoints are gone, a dotted rename target maps correctly, and the
// migration-managed refusal gives the review's --rename flags. Skipped
// unless NEUTRON_E2E_DATABASE_URL is set; NEUTRON_LIVE_REQUIRED=1 turns a
// missing URL into a failure.

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/neutron-build/neutron/cli/internal/db"
)

func newS07StudioDB(t *testing.T, label string) (*db.Client, string) {
	t.Helper()
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_DATABASE_URL is not set; failing instead of skipping")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set; studio S07 e2e skipped")
	}
	dbName := fmt.Sprintf("s07_%s_%d_%d", label, os.Getpid(), time.Now().UnixNano()%1_000_000)
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
		if !strings.HasPrefix(dbName, "s07_") {
			t.Errorf("refusing to drop unexpected database %q", dbName)
			return
		}
		if err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName)); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})
	dbURL := deriveStudioDatabaseURL(t, base, dbName)
	fixture, err := db.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect fixture: %v", err)
	}
	t.Cleanup(fixture.Close)
	return fixture, dbURL
}

func s07Exec(t *testing.T, client *db.Client, stmts ...string) {
	t.Helper()
	for _, stmt := range stmts {
		if err := client.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("%q: %v", stmt, err)
		}
	}
}

func s07Text(t *testing.T, client *db.Client, query string) string {
	t.Helper()
	var out string
	if err := client.QueryRow(context.Background(), query).Scan(&out); err != nil {
		t.Fatalf("%q: %v", query, err)
	}
	return out
}

// TestStudioS07RuleTablesRefuseEditsWith4xx: a table with a DO ALSO or DO
// INSTEAD rule on a write refuses that write with 400 and a message naming
// the rule, never a 502 from the rewritten statement. Writes no rule
// covers still apply.
func TestStudioS07RuleTablesRefuseEditsWith4xx(t *testing.T) {
	fixture, _ := newS07StudioDB(t, "rules")
	s07Exec(t, fixture,
		`CREATE TABLE audit (id int, note text)`,
		`CREATE TABLE ruled_also (id int PRIMARY KEY, v text)`,
		`CREATE TABLE ruled_instead (id int PRIMARY KEY, v text)`,
		`INSERT INTO ruled_also VALUES (1, 'a')`,
		`INSERT INTO ruled_instead VALUES (1, 'i')`,
		`CREATE RULE r_also AS ON UPDATE TO ruled_also DO ALSO INSERT INTO audit VALUES (NEW.id, NEW.v)`,
		`CREATE RULE r_instead AS ON DELETE TO ruled_instead DO INSTEAD UPDATE ruled_instead SET v = 'deleted' WHERE id = OLD.id`,
		`CREATE RULE r_nothing AS ON INSERT TO ruled_instead DO INSTEAD NOTHING`,
	)
	ts, token := s06Server(t, map[string]*db.Client{"e2e": fixture})
	defer ts.Close()
	auth := map[string]string{sessionHeader: token, "Content-Type": "application/json"}
	read := func(table string) (string, string) {
		t.Helper()
		code, body := s06Do(t, ts, http.MethodGet, "/api/table?connectionId=e2e&schema=public&table="+table, "", auth)
		if versions, _ := body["versions"].([]any); code != http.StatusOK || body["readOnly"] != false || len(versions) != 1 {
			t.Fatalf("read %s: %d %v", table, code, body)
		}
		return body["binding"].(string), body["versions"].([]any)[0].(string)
	}
	commit := func(opID, op string) (int, map[string]any) {
		t.Helper()
		return s06Do(t, ts, http.MethodPost, "/api/table/v2/commit",
			fmt.Sprintf(`{"connectionId":"e2e","operationId":%q,"operations":[%s]}`, opID, op), auth)
	}
	wantRefused := func(t *testing.T, code int, body map[string]any, verb string) {
		t.Helper()
		msg := fmt.Sprint(body["error"])
		if code != http.StatusBadRequest || !strings.Contains(msg, "has a rule on "+verb) || !strings.Contains(msg, "nothing was applied") {
			t.Fatalf("%s on a rule table = %d %v, want 400 naming the rule", verb, code, body)
		}
	}

	t.Run("DO ALSO update", func(t *testing.T) {
		binding, version := read("ruled_also")
		code, body := commit("s07-also", fmt.Sprintf(`{"op":"update","schema":"public","table":"ruled_also","binding":%q,"key":[{"column":"id","value":1}],"version":%q,"column":"v","value":"b"}`, binding, version))
		wantRefused(t, code, body, "UPDATE")
		code, body = s06Do(t, ts, http.MethodPost, "/api/table/v2/update", fmt.Sprintf(`{"connectionId":"e2e","binding":%q,"schema":"public","table":"ruled_also","key":[{"column":"id","value":1}],"version":%q,"column":"v","value":"b"}`, binding, version), auth)
		wantRefused(t, code, body, "UPDATE")
		if got := s07Text(t, fixture, `SELECT v || '/' || (SELECT count(*) FROM audit) FROM ruled_also`); got != "a/0" {
			t.Fatalf("a refused update changed state: %s", got)
		}
	})
	t.Run("DO INSTEAD delete and insert", func(t *testing.T) {
		binding, version := read("ruled_instead")
		code, body := commit("s07-instead-del", fmt.Sprintf(`{"op":"delete","schema":"public","table":"ruled_instead","binding":%q,"key":[{"column":"id","value":1}],"version":%q}`, binding, version))
		wantRefused(t, code, body, "DELETE")
		code, body = commit("s07-instead-ins", fmt.Sprintf(`{"op":"insert","schema":"public","table":"ruled_instead","binding":%q,"values":{"id":2,"v":"n"}}`, binding))
		wantRefused(t, code, body, "INSERT")
		code, body = s06Do(t, ts, http.MethodPost, "/api/table/v2/delete", fmt.Sprintf(`{"connectionId":"e2e","binding":%q,"schema":"public","table":"ruled_instead","key":[{"column":"id","value":1}],"version":%q}`, binding, version), auth)
		wantRefused(t, code, body, "DELETE")
		code, body = s06Do(t, ts, http.MethodPost, "/api/table/v2/insert", fmt.Sprintf(`{"connectionId":"e2e","binding":%q,"schema":"public","table":"ruled_instead","values":{"id":2,"v":"n"}}`, binding), auth)
		wantRefused(t, code, body, "INSERT")
		// No rule covers UPDATE on this table: it applies.
		code, body = commit("s07-instead-upd", fmt.Sprintf(`{"op":"update","schema":"public","table":"ruled_instead","binding":%q,"key":[{"column":"id","value":1}],"version":%q,"column":"v","value":"j"}`, binding, version))
		if code != http.StatusOK {
			t.Fatalf("update with no rule on UPDATE = %d %v", code, body)
		}
		if got := s07Text(t, fixture, `SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM ruled_instead`); got != "1:j" {
			t.Fatalf("ruled_instead = %s", got)
		}
	})
}

// TestStudioS07InvalidPrimaryIndexIsNoKey: a primary key whose index is
// invalid (ALTER TABLE ONLY on a partitioned table) enforces no
// uniqueness, so it is not a row identity: the table reads as having no
// usable key and edits are refused.
func TestStudioS07InvalidPrimaryIndexIsNoKey(t *testing.T) {
	fixture, _ := newS07StudioDB(t, "invalidpk")
	s07Exec(t, fixture,
		`CREATE TABLE pq (id int NOT NULL, v text) PARTITION BY RANGE (id)`,
		`CREATE TABLE pq1 PARTITION OF pq FOR VALUES FROM (0) TO (100)`,
		`ALTER TABLE ONLY pq ADD PRIMARY KEY (id)`,
		`INSERT INTO pq VALUES (1, 'a'), (1, 'b')`,
	)
	if got := s07Text(t, fixture, `SELECT i.indisvalid::text FROM pg_index i WHERE i.indrelid = 'pq'::regclass AND i.indisprimary`); got != "false" {
		t.Fatalf("fixture must hold an invalid primary index, indisvalid=%s", got)
	}
	ts, token := s06Server(t, map[string]*db.Client{"e2e": fixture})
	defer ts.Close()
	auth := map[string]string{sessionHeader: token, "Content-Type": "application/json"}
	code, body := s06Do(t, ts, http.MethodGet, "/api/table?connectionId=e2e&schema=public&table=pq", "", auth)
	if code != http.StatusOK {
		t.Fatalf("read pq: %d %v", code, body)
	}
	if keys, _ := body["keyColumns"].([]any); len(keys) != 0 || body["readOnly"] != true || !strings.Contains(fmt.Sprint(body["readOnlyReason"]), "no primary key") {
		t.Fatalf("an invalid primary index must not be the key: keyColumns %v readOnly %v reason %v", body["keyColumns"], body["readOnly"], body["readOnlyReason"])
	}
	binding, _ := body["binding"].(string)
	code, body = s06Do(t, ts, http.MethodPost, "/api/table/v2/insert", fmt.Sprintf(`{"connectionId":"e2e","binding":%q,"schema":"public","table":"pq","values":{"id":2,"v":"c"}}`, binding), auth)
	if code != http.StatusBadRequest || !strings.Contains(fmt.Sprint(body["error"]), "no primary key") {
		t.Fatalf("insert on pq = %d %v, want 400 no primary key", code, body)
	}
}

// TestStudioS07LegacyRowEndpointsRetired: the pre-S01 /api/table/update and
// /api/table/delete endpoints (refusals as 200 with an error field) are
// gone; nothing in the SPA calls them.
func TestStudioS07LegacyRowEndpointsRetired(t *testing.T) {
	fixture, _ := newS07StudioDB(t, "legacy")
	s07Exec(t, fixture, `CREATE TABLE memo (id int PRIMARY KEY, v text)`, `INSERT INTO memo VALUES (1, 'a')`)
	ts, token := s06Server(t, map[string]*db.Client{"e2e": fixture})
	defer ts.Close()
	auth := map[string]string{sessionHeader: token, "Content-Type": "application/json"}
	for path, body := range map[string]string{
		"/api/table/update": `{"connectionId":"e2e","schema":"public","table":"memo","pkColumn":"id","pkValue":1,"column":"v","value":"legacy"}`,
		"/api/table/delete": `{"connectionId":"e2e","schema":"public","table":"memo","pkColumn":"id","pkValue":1}`,
	} {
		if code, res := s06Do(t, ts, http.MethodPost, path, body, auth); code != http.StatusNotFound || !strings.Contains(fmt.Sprint(res["error"]), "no such Studio API endpoint") {
			t.Errorf("%s = %d %v, want 404 (retired)", path, code, res)
		}
	}
	if got := s07Text(t, fixture, `SELECT string_agg(id || ':' || v, ',') FROM memo`); got != "1:a" {
		t.Fatalf("memo = %s", got)
	}
}

// TestStudioS07DottedRenameTarget: a designer rename whose new name
// contains a dot maps the whole new name (not the text after its last
// dot): no spurious churn, the down file reverts cleanly, and the CLI
// flag names the table and both columns.
func TestStudioS07DottedRenameTarget(t *testing.T) {
	fixture, dbURL := newS07StudioDB(t, "dotted")
	s07Exec(t, fixture,
		`CREATE SCHEMA app`,
		`CREATE TABLE app.t (id int PRIMARY KEY, net numeric NOT NULL, CONSTRAINT t_u UNIQUE (net))`,
		`CREATE INDEX t_n ON app.t (net)`,
		`INSERT INTO app.t VALUES (1, 5)`,
	)
	ctx := context.Background()
	catalog := func() string { t.Helper(); return s07Text(t, fixture, q11StudioCatalog) }
	before := catalog()
	plan, err := PlanSchemaChanges(ctx, fixture, []SchemaChange{{Op: "rename-column", Schema: "app", Table: "t", From: "net", To: "a.b"}})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(plan.Up) != 1 || plan.Up[0] != `alter table "app"."t" rename column "net" to "a.b"` {
		t.Fatalf("a rename alone plans only the rename, got up:\n%s", strings.Join(plan.Up, "\n"))
	}
	if want := "app.t.net>app.t.a.b"; len(plan.RenameFlags) != 1 || plan.RenameFlags[0] != want {
		t.Fatalf("rename flags = %v, want [%s]", plan.RenameFlags, want)
	}
	apply := func(stmts []string) error {
		conn, err := pgx.Connect(ctx, dbURL)
		if err != nil {
			return err
		}
		defer conn.Close(ctx)
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		for _, s := range stmts {
			if !db.HasExecutableSQL(s) {
				continue
			}
			if _, err := tx.Exec(ctx, s); err != nil {
				_ = tx.Rollback(ctx)
				return fmt.Errorf("%s: %w", s, err)
			}
		}
		return tx.Commit(ctx)
	}
	if err := apply(plan.Up); err != nil {
		t.Fatalf("up: %v", err)
	}
	down := make([]string, 0, len(plan.Down))
	for i := len(plan.Down) - 1; i >= 0; i-- {
		down = append(down, plan.Down[i])
	}
	if err := apply(down); err != nil {
		t.Fatalf("down: %v\ndown:\n%s", err, strings.Join(down, "\n"))
	}
	if got := catalog(); got != before {
		t.Fatalf("after the down, the catalog must equal the original:\n got: %s\nwant: %s", got, before)
	}
}

// TestStudioS07MigrationManagedRefusalNamesRenames: on a database with
// migration history, the apply refusal's migrate generate command carries
// the review's --rename flags, so following it plans a rename (not an
// added column).
func TestStudioS07MigrationManagedRefusalNamesRenames(t *testing.T) {
	fixture, _ := newS07StudioDB(t, "managed")
	s07Exec(t, fixture,
		`CREATE TABLE orders (id int PRIMARY KEY, note text)`,
		`CREATE TABLE _neutron_migrations (version text PRIMARY KEY)`,
		`INSERT INTO _neutron_migrations VALUES ('001')`,
	)
	ts, token := s06Server(t, map[string]*db.Client{"e2e": fixture})
	defer ts.Close()
	auth := map[string]string{sessionHeader: token, "Content-Type": "application/json"}
	changes := `[{"op":"rename-column","schema":"public","table":"orders","from":"note","to":"remark"}]`
	code, plan := s06Do(t, ts, http.MethodPost, "/api/schema/plan", `{"connectionId":"e2e","changes":`+changes+`}`, auth)
	if code != http.StatusOK {
		t.Fatalf("plan: %d %v", code, plan)
	}
	code, body := s06Do(t, ts, http.MethodPost, "/api/schema/apply",
		fmt.Sprintf(`{"connectionId":"e2e","changes":%s,"planId":%q}`, changes, plan["planId"]), auth)
	msg := fmt.Sprint(body["error"])
	if code != http.StatusConflict || body["state"] != "migration-managed" {
		t.Fatalf("apply = %d %v, want 409 migration-managed", code, body)
	}
	if want := "neutron migrate generate --schema target.schema.json --name <name> --rename 'public.orders.note>public.orders.remark'"; !strings.Contains(msg, want) {
		t.Fatalf("the refusal must give the review's flags:\n got: %s\nwant: %s", msg, want)
	}
	if got := s07Text(t, fixture, `SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 'orders'`); got != "id,note" {
		t.Fatalf("nothing is applied: orders columns %s", got)
	}
}
