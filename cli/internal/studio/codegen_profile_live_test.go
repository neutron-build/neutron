package studio

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// This profile-specific test only touches its own v10_values_* schema.
func TestLosslessCodegenPostgresHTTP(t *testing.T) {
	dsn := os.Getenv("NEUTRON_VALUES_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("NEUTRON_VALUES_REQUIRED") == "1" {
			t.Fatal("NEUTRON_VALUES_DATABASE_URL required")
		}
		t.Skip("profile PostgreSQL control URL absent")
	}
	ctx := context.Background()
	c, err := db.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	schema := "v10_values_codegen_http"
	if err = c.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE; CREATE SCHEMA "+schema+"; CREATE TABLE "+schema+".samples(id bigint NOT NULL, amount numeric, data bytea, s smallint, i integer, b boolean, t text, v varchar(20), c char(3), u uuid)"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	s := &Server{clients: map[string]*db.Client{"profile": c}}
	mux, err := s.routes()
	if err != nil {
		t.Fatal(err)
	}
	call := func(table, lang, profile string) (int, string) {
		q := url.Values{"connectionId": {"profile"}, "schema": {schema}, "table": {table}, "lang": {lang}, "profile": {profile}}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/codegen?"+q.Encode(), nil))
		return w.Code, w.Body.String()
	}
	for _, lang := range []string{"ts", "python", "go"} {
		status, body := call("samples", lang, LosslessReadProfile)
		if status != 200 {
			t.Fatal(status, body)
		}
		var result struct {
			Code string `json:"code"`
		}
		if err := json.Unmarshal([]byte(body), &result); err != nil {
			t.Fatal(err)
		}
		cols, err := FetchColsForProfile(ctx, c, schema, "samples", LosslessReadProfile)
		if err != nil {
			t.Fatal(err)
		}
		want, err := GenerateCodeProfile(LosslessReadProfile, lang, "samples", cols)
		if err != nil || result.Code != want {
			t.Fatal("HTTP/shared generator mismatch", err)
		}
	}
	for _, p := range []struct{ lang, profile string }{{"rust", LosslessReadProfile}, {"ts", "unknown"}} {
		status, _ := call("samples", p.lang, p.profile)
		if status != 400 {
			t.Fatal(status)
		}
	}
	for _, typ := range []string{"timestamp", "numeric[]", "jsonb", "real", "interval"} {
		if err := c.Exec(ctx, "ALTER TABLE "+schema+".samples ADD COLUMN unsupported "+typ); err != nil {
			t.Fatal(err)
		}
		status, body := call("samples", "ts", LosslessReadProfile)
		if status != 400 || !strings.Contains(body, "unsupported") || !strings.Contains(body, "samples") {
			t.Fatal(status, body)
		}
		if err := c.Exec(ctx, "ALTER TABLE "+schema+".samples DROP COLUMN unsupported"); err != nil {
			t.Fatal(err)
		}
	}
}
