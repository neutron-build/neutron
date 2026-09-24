package studio

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// Offline tests for the S01 v2 protocol's pure validation layers. Database
// behavior (introspection binding, stale conflicts, tagged keys against
// real columns) lives in rows_v2_e2e_test.go.

func metaFixture() *tableMeta {
	cols := []tableColumnMeta{
		{Name: "tenant_id", IsPK: true, KeyPos: 1, TypeName: "int4", TypeOID: 23, TypType: "b", NotNull: true, CanUpdate: true, CanInsert: true},
		{Name: "id", IsPK: true, KeyPos: 2, TypeName: "int8", TypeOID: 20, TypType: "b", NotNull: true, CanUpdate: true, CanInsert: true},
		{Name: "payload", TypeName: "text", TypeOID: 25, TypType: "b", CanUpdate: true, CanInsert: true},
		{Name: "total", Generated: "s", TypeName: "int4", TypeOID: 23, TypType: "b", CanUpdate: true, CanInsert: true},
		{Name: "seq", Identity: "d", TypeName: "int4", TypeOID: 23, TypType: "b", CanUpdate: true, CanInsert: true},
		{Name: "doc", TypeName: "jsonb", TypeOID: 3802, TypType: "b", CanUpdate: true, CanInsert: true},
	}
	m := &tableMeta{Exists: true, RelOID: 16384, PKCols: []string{"tenant_id", "id"}, Columns: map[string]tableColumnMeta{}, Order: cols, CanDelete: true}
	for _, c := range cols {
		m.Columns[c.Name] = c
	}
	return m
}

func TestValidateKeyTuple(t *testing.T) {
	meta := metaFixture()

	t.Run("full tuple passes and preserves order", func(t *testing.T) {
		args, cols, err := validateKeyTuple(meta, []keyCell{
			{Column: "id", Value: map[string]any{"t": "int8", "v": "7"}},
			{Column: "tenant_id", Value: json.Number("3")},
		})
		if err != nil {
			t.Fatalf("unexpected rejection: %v", err)
		}
		if cols[0] != "id" || cols[1] != "tenant_id" {
			t.Errorf("column order = %v", cols)
		}
		if args[0] != int64(7) {
			t.Errorf("tagged key decoded to %v (%T), want int64(7)", args[0], args[0])
		}
		// A plain JSON number for an int4 key is passed on as its exact
		// literal text (PostgreSQL parses it into the column type).
		if args[1] != "3" {
			t.Errorf("int4 key decoded to %v (%T), want the literal \"3\"", args[1], args[1])
		}
	})

	t.Run("bare JSON number for an int8 key is refused (may be client-rounded)", func(t *testing.T) {
		_, _, err := validateKeyTuple(meta, []keyCell{
			{Column: "tenant_id", Value: json.Number("1")},
			{Column: "id", Value: json.Number("9007199254740993")},
		})
		if err == nil || !strings.Contains(err.Error(), "wire cell") {
			t.Errorf("untagged int8 key = %v, want rejection", err)
		}
	})

	t.Run("mismatched tag is refused", func(t *testing.T) {
		_, _, err := validateKeyTuple(meta, []keyCell{
			{Column: "tenant_id", Value: map[string]any{"t": "int8", "v": "1"}},
			{Column: "id", Value: map[string]any{"t": "int8", "v": "1"}},
		})
		if err == nil || !strings.Contains(err.Error(), "does not match column type") {
			t.Errorf("tagged cell on int4 key = %v, want rejection", err)
		}
		_, _, err = validateKeyTuple(meta, []keyCell{
			{Column: "tenant_id", Value: json.Number("1")},
			{Column: "id", Value: map[string]any{"t": "numeric", "v": "1"}},
		})
		if err == nil || !strings.Contains(err.Error(), "does not match column type") {
			t.Errorf("numeric tag on int8 key = %v, want rejection", err)
		}
	})

	t.Run("object and array key values are refused", func(t *testing.T) {
		for _, v := range []any{map[string]any{"a": "b"}, []any{json.Number("1")}} {
			_, _, err := validateKeyTuple(meta, []keyCell{
				{Column: "tenant_id", Value: v},
				{Column: "id", Value: map[string]any{"t": "int8", "v": "1"}},
			})
			if err == nil {
				t.Errorf("key value %v accepted", v)
			}
		}
	})

	t.Run("subset of a composite key is rejected", func(t *testing.T) {
		_, _, err := validateKeyTuple(meta, []keyCell{{Column: "tenant_id", Value: 1}})
		if err == nil || !strings.Contains(err.Error(), "full primary key (tenant_id, id)") {
			t.Errorf("subset rejection = %v", err)
		}
	})

	t.Run("extra key column is rejected", func(t *testing.T) {
		_, _, err := validateKeyTuple(meta, []keyCell{
			{Column: "tenant_id", Value: json.Number("1")}, {Column: "payload", Value: "x"},
		})
		if err == nil || !strings.Contains(err.Error(), "is not part of the primary key") {
			t.Errorf("extra column rejection = %v", err)
		}
	})

	t.Run("duplicate key column is rejected", func(t *testing.T) {
		_, _, err := validateKeyTuple(meta, []keyCell{
			{Column: "tenant_id", Value: json.Number("1")}, {Column: "tenant_id", Value: json.Number("2")},
		})
		if err == nil || !strings.Contains(err.Error(), "appears twice") {
			t.Errorf("duplicate rejection = %v", err)
		}
	})

	t.Run("NULL key value is rejected before SQL", func(t *testing.T) {
		_, _, err := validateKeyTuple(meta, []keyCell{
			{Column: "tenant_id", Value: nil}, {Column: "id", Value: json.Number("1")},
		})
		if err == nil || !strings.Contains(err.Error(), "NULL can never match") {
			t.Errorf("null key rejection = %v", err)
		}
	})

	t.Run("malformed tagged key is rejected", func(t *testing.T) {
		_, _, err := validateKeyTuple(meta, []keyCell{
			{Column: "tenant_id", Value: json.Number("1")},
			{Column: "id", Value: map[string]any{"t": "int8", "v": "not-a-number"}},
		})
		if err == nil || !strings.Contains(err.Error(), "invalid payload") {
			t.Errorf("malformed tag rejection = %v", err)
		}
	})
}

func TestEditableReason(t *testing.T) {
	cases := []struct {
		col   tableColumnMeta
		want  string
		match string
	}{
		{tableColumnMeta{Name: "plain", CanUpdate: true}, "", ""},
		{tableColumnMeta{Name: "d", DefaultExpr: "'x'", CanUpdate: true}, "", ""}, // plain default stays writable
		{tableColumnMeta{Name: "g", Generated: "s", CanUpdate: true}, "generated", "generated column"},
		{tableColumnMeta{Name: "i", Identity: "a", CanUpdate: true}, "identity", "identity column"},
		{tableColumnMeta{Name: "k", IsPK: true, CanUpdate: true}, "key", "addresses the row"},
		{tableColumnMeta{Name: "p"}, "privilege", "no UPDATE privilege"},
	}
	for _, tc := range cases {
		got := editableReason(tc.col)
		if tc.want == "" && got != "" {
			t.Errorf("editableReason(%q) = %q, want editable", tc.col.Name, got)
		}
		if tc.want != "" && !strings.Contains(got, tc.match) {
			t.Errorf("editableReason(%q) = %q, want it to mention %q", tc.col.Name, got, tc.match)
		}
	}
}

func TestInsertableReason(t *testing.T) {
	// Plain PK columns are insertable (that is how keyed rows are created);
	// generated, identity and serial columns are not.
	if r := insertableReason(tableColumnMeta{Name: "id", IsPK: true, CanInsert: true}); r != "" {
		t.Errorf("plain PK must be insertable, got %q", r)
	}
	if r := insertableReason(tableColumnMeta{Name: "x"}); !strings.Contains(r, "INSERT privilege") {
		t.Errorf("column without INSERT privilege must not be insertable, got %q", r)
	}
	if r := insertableReason(tableColumnMeta{Name: "g", Generated: "s"}); r == "" {
		t.Error("generated column must not be insertable")
	}
	if r := insertableReason(tableColumnMeta{Name: "i", Identity: "d"}); r == "" {
		t.Error("identity column must not be insertable")
	}
	if r := insertableReason(tableColumnMeta{Name: "s", DefaultExpr: "nextval('x'::regclass)"}); r == "" {
		t.Error("serial column must not be insertable (omit for default)")
	}
}

func TestTableReadOnlyState(t *testing.T) {
	keyed := metaFixture()
	keyless := &tableMeta{Exists: true, Columns: map[string]tableColumnMeta{"a": {Name: "a"}}}
	missing := &tableMeta{Exists: false, Columns: map[string]tableColumnMeta{}}

	if st := tableReadOnlyState(keyed, true); st.readOnly || !st.versioned {
		t.Errorf("keyed+versioned must be editable, got %+v", st)
	}
	if st := tableReadOnlyState(keyed, false); !st.readOnly || st.versioned {
		t.Errorf("keyed but unversioned must be read-only (no xmin), got %+v", st)
	}
	if st := tableReadOnlyState(keyless, true); !st.readOnly || st.reason == "" {
		t.Errorf("keyless table must be read-only with a reason, got %+v", st)
	}
	if st := tableReadOnlyState(keyless, false); !st.readOnly {
		t.Errorf("keyless unversioned must be read-only, got %+v", st)
	}
	if st := tableReadOnlyState(missing, true); !st.readOnly || st.reason == "" {
		t.Errorf("missing table must be read-only with a reason, got %+v", st)
	}

	// Key types without an exact round-trip make the table read-only.
	floatKey := metaFixture()
	c := floatKey.Columns["tenant_id"]
	c.TypeOID, c.TypeName = 701, "float8"
	floatKey.Columns["tenant_id"] = c
	if st := tableReadOnlyState(floatKey, true); !st.readOnly || !strings.Contains(st.reason, "float8") {
		t.Errorf("float8 key must be read-only naming the type, got %+v", st)
	}
	enumKey := metaFixture()
	c = enumKey.Columns["tenant_id"]
	c.TypeOID, c.TypeName, c.TypType = 99999, "mood", "e"
	enumKey.Columns["tenant_id"] = c
	if st := tableReadOnlyState(enumKey, true); st.readOnly {
		t.Errorf("enum key must be editable, got %+v", st)
	}
}

func TestBuildTableMetaV2(t *testing.T) {
	resp := buildTableMetaV2(metaFixture(), "ep:16384", true, "public", "docs")
	if resp.ReadOnly || resp.Binding != "ep:16384" || !resp.CanDelete {
		t.Fatalf("editable table meta = %+v", resp)
	}
	if len(resp.KeyColumns) != 2 || resp.KeyColumns[0] != "tenant_id" || resp.KeyColumns[1] != "id" {
		t.Errorf("keyColumns = %v (must keep constraint order)", resp.KeyColumns)
	}
	byName := map[string]metaColumnV2{}
	for _, c := range resp.Columns {
		byName[c.Name] = c
	}
	if byName["payload"].Editable != true || byName["payload"].Tag != "" {
		t.Errorf("payload = %+v", byName["payload"])
	}
	if byName["id"].Editable || byName["id"].Tag != "int8" || !byName["id"].IsKey {
		t.Errorf("int8 key = %+v", byName["id"])
	}
	if byName["total"].Editable || byName["total"].Insertable {
		t.Errorf("generated column = %+v", byName["total"])
	}

	// Read-only table: every column non-editable with the table's reason.
	keyless := metaFixture()
	keyless.PKCols = nil
	resp = buildTableMetaV2(keyless, "ep:1", true, "public", "keyless")
	if !resp.ReadOnly || !strings.Contains(resp.ReadOnlyReason, "no primary key") || resp.CanDelete {
		t.Fatalf("keyless meta = %+v", resp)
	}
	for _, c := range resp.Columns {
		if c.Editable || c.Insertable || c.ReadOnlyReason != resp.ReadOnlyReason {
			t.Errorf("keyless column %q offered editing: %+v", c.Name, c)
		}
	}
}

func TestBindingFor(t *testing.T) {
	if got := bindingFor("abc", 16384); got != "abc:16384" {
		t.Errorf("bindingFor = %q", got)
	}
	if bindingFor("abc", 1) == bindingFor("abd", 1) || bindingFor("abc", 1) == bindingFor("abc", 2) {
		t.Error("binding must change with either the epoch or the relation")
	}
}

func TestConnectionEpochChangesPerClient(t *testing.T) {
	s := &Server{clients: map[string]*db.Client{}, epochs: map[string]string{}}
	if got := s.connectionEpoch("c"); got != "0" {
		t.Errorf("epoch without a client = %q, want the test default", got)
	}
	s.setClient("c", nil)
	first := s.connectionEpoch("c")
	s.mu.Lock()
	delete(s.clients, "c") // setClient would Close the (nil) old client
	s.mu.Unlock()
	s.setClient("c", nil)
	second := s.connectionEpoch("c")
	if first == "0" || second == "0" || first == second {
		t.Errorf("reconnect must mint a fresh epoch: %q then %q", first, second)
	}
}

func TestBuildGuardedMutationV2(t *testing.T) {
	mut := func(keyWhere string, valueParam int) string {
		return `UPDATE "public"."docs" SET "payload" = $` + itoa(valueParam)
	}
	sqlText, args := buildGuardedMutationV2("public", "docs", mut,
		[]any{json.Number("1"), json.Number("2")}, []string{"tenant_id", "id"}, "123", "v")

	wantPieces := []string{
		`UPDATE "public"."docs" SET "payload" = $4`,
		`WHERE ("tenant_id" = $1 AND "id" = $2 AND "public"."docs".xmin::text = $3)`,
		`(SELECT count(*) FROM "public"."docs" WHERE "tenant_id" = $1 AND "id" = $2) = 1`,
	}
	for _, piece := range wantPieces {
		if !strings.Contains(sqlText, piece) {
			t.Errorf("guarded SQL missing %q:\n%s", piece, sqlText)
		}
	}
	if len(args) != 4 || args[0] != json.Number("1") || args[1] != json.Number("2") || args[2] != "123" || args[3] != "v" {
		t.Errorf("args = %v, want [1 2 123 v]", args)
	}

	// DELETE takes no value parameter.
	delMut := func(keyWhere string, valueParam int) string {
		return `DELETE FROM "public"."docs"`
	}
	sqlText, args = buildGuardedMutationV2("public", "docs", delMut,
		[]any{json.Number("9")}, []string{"id"}, "77")
	if !strings.Contains(sqlText, `"id" = $1`) || !strings.Contains(sqlText, `xmin::text = $2`) {
		t.Errorf("delete guard missing key/version predicates: %s", sqlText)
	}
	if len(args) != 2 {
		t.Errorf("delete args = %v, want [9 77]", args)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func TestV2HandlerAuthAndValidationOrder(t *testing.T) {
	s := newAuthTestServer(t)

	post := func(handler http.HandlerFunc, payload string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/table/v2/x", strings.NewReader(payload))
		authed(s, req)
		rec := httptest.NewRecorder()
		handler(rec, req)
		return rec
	}

	// Auth fires before body parsing (403, not 400, for unauthenticated).
	req := httptest.NewRequest(http.MethodPost, "/api/table/v2/update", strings.NewReader(`{`))
	rec := httptest.NewRecorder()
	s.handleTableRowUpdateV2(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("unauthenticated v2 update = %d, want 403", rec.Code)
	}

	if rec := post(s.handleTableRowUpdateV2, `{not json`); rec.Code != http.StatusBadRequest {
		t.Errorf("malformed JSON = %d, want 400", rec.Code)
	}
	full := `"connectionId":"nope","binding":"0:1","schema":"s","table":"t","key":[{"column":"id","value":1}],"version":"7"`
	cases := []struct {
		name    string
		handler http.HandlerFunc
		payload string
		want    string
	}{
		{"missing version", s.handleTableRowUpdateV2, `{"connectionId":"c","binding":"0:1","schema":"s","table":"t","key":[{"column":"id","value":1}],"column":"c","value":"v"}`, "required"},
		{"missing binding", s.handleTableRowUpdateV2, `{"connectionId":"c","schema":"s","table":"t","key":[{"column":"id","value":1}],"version":"7","column":"c","value":"v"}`, "required"},
		{"empty key", s.handleTableRowDeleteV2, `{"connectionId":"c","binding":"0:1","schema":"s","table":"t","key":[],"version":"7"}`, "key is required"},
		{"non-numeric version", s.handleTableRowDeleteV2, `{"connectionId":"c","binding":"0:1","schema":"s","table":"t","version":"abc","key":[{"column":"id","value":1}]}`, "version"},
		{"version beyond 32 bits", s.handleTableRowDeleteV2, `{"connectionId":"c","binding":"0:1","schema":"s","table":"t","version":"4294967296","key":[{"column":"id","value":1}]}`, "version"},
		{"forged pkColumn field", s.handleTableRowUpdateV2, `{` + full + `,"pkColumn":"tenant_id","column":"c","value":"v"}`, "unknown field"},
		{"forged where field", s.handleTableRowDeleteV2, `{` + full + `,"where":"true"}`, "unknown field"},
		{"delete carrying a column", s.handleTableRowDeleteV2, `{` + full + `,"column":"c"}`, "unknown field"},
		{"trailing data", s.handleTableRowDeleteV2, `{` + full + `}{"x":1}`, "trailing"},
		{"isNull with a value", s.handleTableRowUpdateV2, `{` + full + `,"column":"c","value":"v","isNull":true}`, "mutually exclusive"},
		{"value missing", s.handleTableRowUpdateV2, `{` + full + `,"column":"c"}`, "isNull"},
		{"missing insert values", s.handleTableRowInsertV2, `{"connectionId":"c","binding":"0:1","schema":"s","table":"t"}`, "values is required"},
		{"insert without binding", s.handleTableRowInsertV2, `{"connectionId":"c","schema":"s","table":"t","values":{}}`, "required"},
		{"unknown connection", s.handleTableRowInsertV2, `{"connectionId":"nope","binding":"0:1","schema":"s","table":"t","values":{"a":1}}`, "not connected"},
		{"unknown connection (update)", s.handleTableRowUpdateV2, `{` + full + `,"column":"c","value":"v"}`, "not connected"},
	}
	for _, tc := range cases {
		rec := post(tc.handler, tc.payload)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s = %d %s, want 400", tc.name, rec.Code, rec.Body.String())
			continue
		}
		if !strings.Contains(rec.Body.String(), tc.want) {
			t.Errorf("%s body = %s, want it to mention %q", tc.name, rec.Body.String(), tc.want)
		}
	}

	// Oversized bodies are refused before any database work.
	big := `{"connectionId":"c","binding":"0:1","schema":"s","table":"t","values":{"a":"` + strings.Repeat("x", maxMutationBody) + `"}}`
	if rec := post(s.handleTableRowInsertV2, big); rec.Code != http.StatusBadRequest {
		t.Errorf("oversized body = %d, want 400", rec.Code)
	}
}

func TestParseMatch(t *testing.T) {
	meta := metaFixture()
	conds, args, err := parseMatch(`[{"column":"tenant_id","value":1},{"column":"id","value":{"t":"int8","v":"9007199254740993"}}]`, meta, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(conds) != 2 || conds[0] != `"tenant_id" = $2` || conds[1] != `"id" = $3` {
		t.Errorf("conds = %v (placeholders must follow the filter's)", conds)
	}
	if args[0] != "1" || args[1] != int64(9007199254740993) {
		t.Errorf("args = %#v, want exact literal and exact int64", args)
	}
	for _, bad := range []string{
		`[]`,
		`{"column":"id"}`,
		`[{"column":"nope","value":1}]`,
		`[{"column":"id","value":null}]`,
		`[{"column":"id","value":1},{"column":"id","value":2}]`,
		`[{"column":"id","value":{"a":1}}]`,
		`[{"column":"id","value":1,"op":"<>"}]`,
		`[{"column":"id","value":{"t":"int8","v":"x"}}]`,
	} {
		if _, _, err := parseMatch(bad, meta, true, 0); err == nil {
			t.Errorf("parseMatch accepted %s", bad)
		}
	}
	// Unknown catalog (engine without introspection): columns are quoted,
	// not validated.
	if _, _, err := parseMatch(`[{"column":"x\"y","value":"v"}]`, nil, false, 0); err != nil {
		t.Errorf("catalog-less match rejected: %v", err)
	}
}
