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
