package studio

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/neutron-build/neutron/cli/internal/db"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Production handlers and PostgreSQL, including forged apply and a file
// changed after review. SQL assertions independently check rows and columns.
func TestStudioX14OwnershipE2E(t *testing.T) {
	base := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if base == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("live database URL required")
		}
		t.Skip("NEUTRON_E2E_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := db.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("x14studio_%d_%d", os.Getpid(), time.Now().UnixNano()%1000000)
	if err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, name)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE %q WITH (FORCE)`, name)); err != nil {
			t.Error(err)
		}
	}()
	client, err := db.Connect(ctx, deriveStudioDatabaseURL(t, base, name))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, sql := range []string{`CREATE TABLE external (id integer PRIMARY KEY, note text)`, `CREATE TABLE owned (id integer PRIMARY KEY)`, `INSERT INTO external VALUES (1, 'keep me')`} {
		if err := client.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	model, err := db.ModelFromRoot(mustIntrospect(t, client).Root)
	if err != nil {
		t.Fatal(err)
	}
	model.Table(db.V2Identity{Schema: "public", Name: "external"}).Managed = false
	model.Tables = append(model.Tables, db.V2Table{Identity: db.V2Identity{Schema: "public", Name: "absent"}, Managed: false, Columns: []db.V2Column{{Name: "id", Type: int4()}}, Constraints: []db.V2Constraint{}, Indexes: []db.V2Index{}})
	ownership, err := documentFromModel(model)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "schema.json")
	if err := os.WriteFile(path, ownership.Canonical, 0600); err != nil {
		t.Fatal(err)
	}
	s := &Server{sessionToken: "x14-token", clients: map[string]*db.Client{"live": client}}
	if err := s.SetSchemaSource(path); err != nil {
		t.Fatal(err)
	}
	post := func(apply bool, ch SchemaChange, planID string) *httptest.ResponseRecorder {
		payload, _ := json.Marshal(schemaPlanRequest{ConnectionID: "live", Changes: []SchemaChange{ch}, PlanID: planID, AllowDestructive: apply})
		req := httptest.NewRequest(http.MethodPost, "/api/schema/plan", strings.NewReader(string(payload)))
		req.Header.Set("X-Studio-Session", "x14-token")
		req.Header.Set("Origin", "http://localhost:0")
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		if apply {
			s.handleSchemaApply(w, req)
		} else {
			s.handleSchemaPlan(w, req)
		}
		return w
	}
	for _, ch := range []SchemaChange{{Op: "drop-table", Schema: "public", Table: "external"}, {Op: "add-column", Schema: "public", Table: "external", Column: "extra", Type: "text"}, {Op: "create-table", Schema: "public", Table: "absent"}} {
		for _, apply := range []bool{false, true} {
			id := ""
			if apply {
				id = "forged"
			}
			w := post(apply, ch, id)
			if w.Code < 400 || !strings.Contains(w.Body.String(), "unmanaged") {
				t.Fatalf("unmanaged apply=%v: %d %s", apply, w.Code, w.Body)
			}
		}
	}
	change := SchemaChange{Op: "add-column", Schema: "public", Table: "owned", Column: "extra", Type: "text"}
	w := post(false, change, "")
	if w.Code != 200 {
		t.Fatalf("owned preview: %d %s", w.Code, w.Body)
	}
	var reviewed planResponse
	if err := json.Unmarshal(w.Body.Bytes(), &reviewed); err != nil {
		t.Fatal(err)
	}
	model.Table(db.V2Identity{Schema: "public", Name: "owned"}).Managed = false
	ownership, err = documentFromModel(model)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, ownership.Canonical, 0600); err != nil {
		t.Fatal(err)
	}
	w = post(true, change, reviewed.PlanID)
	if w.Code < 400 || !strings.Contains(w.Body.String(), "unmanaged") {
		t.Fatalf("ownership changed after preview: %d %s", w.Code, w.Body)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	w = post(false, change, "")
	if w.Code < 400 || !strings.Contains(w.Body.String(), "ownership") {
		t.Fatalf("lost ownership file: %d %s", w.Code, w.Body)
	}
	var note string
	if err := client.QueryRow(ctx, `SELECT note FROM external WHERE id=1`).Scan(&note); err != nil || note != "keep me" {
		t.Fatalf("external data: %q %v", note, err)
	}
	var columns int
	if err := client.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name IN ('external','owned') AND column_name='extra'`).Scan(&columns); err != nil || columns != 0 {
		t.Fatalf("unrequested changes: %d %v", columns, err)
	}
	var absent bool
	if err := client.QueryRow(ctx, `SELECT to_regclass('public.absent') IS NULL`).Scan(&absent); err != nil || !absent {
		t.Fatalf("unmanaged creation: %v %v", absent, err)
	}
}
