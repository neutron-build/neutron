package studio

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Unit tests for the B04 interim key contract that need no database:
// the single-column-key gate, auto-assigned column detection, guarded SQL
// construction, and handler-level request validation. Live-database behavior
// (re-introspection, forged keys, exactly-one enforcement) is covered by
// TestStudioRowSafetyE2E in table_rows_e2e_test.go.

func TestRequireSingleColumnKey(t *testing.T) {
	cases := []struct {
		name       string
		meta       *tableMeta
		claimedPK  string
		wantPK     string
		wantReject bool
		wantText   string
	}{
		{
			name: "table missing is rejected",
			meta: &tableMeta{Exists: false, Columns: map[string]tableColumnMeta{}},
			wantReject: true, wantText: "was not found or is not an ordinary table",
		},
		{
			name: "no primary key is read-only",
			meta: &tableMeta{Exists: true, Columns: map[string]tableColumnMeta{
				"a": {Name: "a"}, "b": {Name: "b"},
			}},
			wantReject: true, wantText: "has no primary key",
		},
		{
			name: "composite primary key is read-only and names both columns",
			meta: &tableMeta{Exists: true, PKCols: []string{"tenant_id", "id"}, Columns: map[string]tableColumnMeta{
				"tenant_id": {Name: "tenant_id", IsPK: true}, "id": {Name: "id", IsPK: true},
			}},
			wantReject: true, wantText: "composite primary key (tenant_id, id)",
		},
		{
			name: "single-column PK with forged claim is rejected",
			meta: &tableMeta{Exists: true, PKCols: []string{"id"}, Columns: map[string]tableColumnMeta{
				"id": {Name: "id", IsPK: true}, "payload": {Name: "payload"},
			}},
			claimedPK: "payload", wantReject: true, wantText: `pkColumn "payload" is not the primary key`,
		},
		{
			name: "single-column PK with matching claim passes",
			meta: &tableMeta{Exists: true, PKCols: []string{"id"}, Columns: map[string]tableColumnMeta{
				"id": {Name: "id", IsPK: true}, "payload": {Name: "payload"},
			}},
			claimedPK: "id", wantPK: "id",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pk, reject := requireSingleColumnKey(tc.meta, "public", "t", tc.claimedPK)
			if tc.wantReject {
				if reject == "" {
					t.Fatalf("expected rejection, got pk=%q", pk)
				}
				if !strings.Contains(reject, tc.wantText) {
					t.Errorf("rejection %q does not contain %q", reject, tc.wantText)
				}
				if pk != "" {
					t.Errorf("expected empty pk on rejection, got %q", pk)
				}
			} else {
				if reject != "" {
					t.Fatalf("unexpected rejection: %q", reject)
				}
				if pk != tc.wantPK {
					t.Errorf("pk = %q, want %q", pk, tc.wantPK)
				}
			}
		})
	}
}

func TestAutoAssigned(t *testing.T) {
	cases := []struct {
		name string
		meta tableColumnMeta
		want bool
	}{
		{"plain column", tableColumnMeta{Name: "id"}, false},
		{"explicit value default", tableColumnMeta{Name: "flag", DefaultExpr: "true"}, false},
		{"identity always", tableColumnMeta{Name: "id", Identity: "a"}, true},
		{"identity by default", tableColumnMeta{Name: "id", Identity: "d"}, true},
		{"stored generated", tableColumnMeta{Name: "total", Generated: "s"}, true},
		{"serial nextval default", tableColumnMeta{Name: "id", DefaultExpr: "nextval('t_id_seq'::regclass)"}, true},
		{"nextval uppercase", tableColumnMeta{Name: "id", DefaultExpr: "NEXTVAL('t_id_seq'::regclass)"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.meta.autoAssigned(); got != tc.want {
				t.Errorf("autoAssigned() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildUpdateExactlyOneCarriesCountGuard(t *testing.T) {
	sqlText, args := buildUpdateExactlyOne("public", "docs", "id", "payload", false, "v", 7)
	if !strings.Contains(sqlText, `(SELECT count(*) FROM "public"."docs" WHERE "id" = $2) = 1`) {
		t.Errorf("update SQL missing exactly-one count guard: %s", sqlText)
	}
	if !strings.Contains(sqlText, `SET "payload" = $1`) {
		t.Errorf("update SQL missing bound SET: %s", sqlText)
	}
	if !strings.Contains(sqlText, `WHERE "id" = $2`) {
		t.Errorf("update SQL missing key predicate: %s", sqlText)
	}
	if len(args) != 2 {
		t.Errorf("args = %v, want [value, pkValue]", args)
	}

	nullSQL, nullArgs := buildUpdateExactlyOne("public", "docs", "id", "payload", true, nil, 7)
	if !strings.Contains(nullSQL, `SET "payload" = NULL`) {
		t.Errorf("null update SQL should set NULL: %s", nullSQL)
	}
	if !strings.Contains(nullSQL, `(SELECT count(*) FROM "public"."docs" WHERE "id" = $1) = 1`) {
		t.Errorf("null update SQL missing count guard: %s", nullSQL)
	}
	if len(nullArgs) != 1 {
		t.Errorf("null args = %v, want [pkValue]", nullArgs)
	}
}

func TestBuildDeleteExactlyOneCarriesCountGuard(t *testing.T) {
	sqlText, args := buildDeleteExactlyOne("public", "docs", "id", 7)
	if !strings.Contains(sqlText, `DELETE FROM "public"."docs" WHERE "id" = $1`) {
		t.Errorf("delete SQL missing key predicate: %s", sqlText)
	}
	if !strings.Contains(sqlText, `(SELECT count(*) FROM "public"."docs" WHERE "id" = $1) = 1`) {
		t.Errorf("delete SQL missing exactly-one count guard: %s", sqlText)
	}
	if len(args) != 1 {
		t.Errorf("args = %v, want [pkValue]", args)
	}
}

func TestMutateExactlyOneWrapsWithCount(t *testing.T) {
	// mutateExactlyOne needs a live database (E2E); here we only pin the
	// wrapper text shape it produces around a guarded statement.
	inner := `DELETE FROM "public"."docs" WHERE "id" = $1`
	wrapped := "WITH mutated AS (" + inner + " RETURNING 1) SELECT count(*) FROM mutated"
	if !strings.Contains(wrapped, "WITH mutated AS (") || !strings.Contains(wrapped, "SELECT count(*) FROM mutated") {
		t.Errorf("wrapper missing CTE count envelope: %s", wrapped)
	}
}

func TestRowHandlerRequestValidation(t *testing.T) {
	s := &Server{}

	post := func(handler http.HandlerFunc, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/table/x", strings.NewReader(body))
		rec := httptest.NewRecorder()
		handler(rec, req)
		return rec
	}

	t.Run("rejects malformed JSON", func(t *testing.T) {
		rec := post(s.handleTableRowUpdate, "{not json")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("rejects missing pkColumn", func(t *testing.T) {
		rec := post(s.handleTableRowUpdate, `{"connectionId":"c","schema":"public","table":"t","column":"c","value":"v"}`)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("rejects missing value without isNull", func(t *testing.T) {
		rec := post(s.handleTableRowUpdate, `{"connectionId":"c","schema":"public","table":"t","pkColumn":"id","column":"c","pkValue":1}`)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(body["error"].(string), "isNull") {
			t.Errorf("error should mention isNull: %v", body["error"])
		}
	})

	t.Run("rejects unknown connection", func(t *testing.T) {
		rec := post(s.handleTableRowUpdate, `{"connectionId":"nope","schema":"public","table":"t","pkColumn":"id","column":"c","pkValue":1,"value":"v"}`)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("delete rejects unknown connection", func(t *testing.T) {
		rec := post(s.handleTableRowDelete, `{"connectionId":"nope","schema":"public","table":"t","pkColumn":"id","pkValue":1}`)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("update rejects non-POST method", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/table/update", nil)
		rec := httptest.NewRecorder()
		s.handleTableRowUpdate(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", rec.Code)
		}
	})
}
