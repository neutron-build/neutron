package studio

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ResponseController delegates to these methods on a real connection writer.
// Recording them verifies the handler releases its socket lease on early return.
type pageDeadlineWriter struct {
	*httptest.ResponseRecorder
	reads, writes []time.Time
}

func (w *pageDeadlineWriter) SetReadDeadline(d time.Time) error {
	w.reads = append(w.reads, d)
	return nil
}
func (w *pageDeadlineWriter) SetWriteDeadline(d time.Time) error {
	w.writes = append(w.writes, d)
	return nil
}

func TestPageRequestAdmission(t *testing.T) {
	valid := `{"connectionId":"c","schema":"public","table":"t","profile":"postgres-direct","limit":2}`
	if _, err := parsePageRequest([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		strings.Replace(valid, `"limit":2`, `"limit":0`, 1), strings.Replace(valid, `"limit":2`, `"limit":1001`, 1), strings.Replace(valid, `"limit":2`, `"limit":1.5`, 1),
		strings.Replace(valid, `"profile":"postgres-direct"`, `"profile":"nucleus"`, 1),
		strings.Replace(valid, `"limit":2`, `"Limit":2`, 1), strings.Replace(valid, `"limit":2`, `"limit":2,"limit":3`, 1),
		strings.Replace(valid, `"limit":2`, `"limit":2,"filters":[]`, 1), strings.Replace(valid, `"limit":2`, `"limit":2,"sorts":[]`, 1), strings.Replace(valid, `"limit":2`, `"limit":2,"match":[]`, 1),
		strings.Replace(valid, `"limit":2`, `"limit":2,"offset":10`, 1), valid + ` {}`, `[]`,
	} {
		if _, err := parsePageRequest([]byte(body)); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
	s := &Server{sessionToken: "session"}
	deadlineRequest := httptest.NewRequest(http.MethodPost, "/api/table/v2/page", strings.NewReader(valid))
	deadlineRequest.Header.Set(sessionHeader, "session")
	deadlineWriter := &pageDeadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	s.handleTablePageV2(deadlineWriter, deadlineRequest)
	for _, history := range [][]time.Time{deadlineWriter.reads, deadlineWriter.writes} {
		if len(history) != 2 || history[0].IsZero() || !history[1].IsZero() {
			t.Fatal("request socket deadline leaked into keep-alive reuse")
		}
	}

	wrongOrigin := httptest.NewRequest(http.MethodPost, "/api/table/v2/page", strings.NewReader(valid))
	wrongOrigin.Header.Set(sessionHeader, "session")
	wrongOrigin.Header.Set("Origin", "https://hostile.example")
	forbidden := httptest.NewRecorder()
	s.handleTablePageV2(forbidden, wrongOrigin)
	if forbidden.Code != 403 {
		t.Fatal("wrong origin admitted")
	}
	for _, tc := range []struct {
		body, token string
		status      int
	}{{valid, "", 403}, {strings.Repeat(" ", pageBodyBytes+1), "session", 400}, {valid, "session", 400}} {
		r := httptest.NewRequest(http.MethodPost, "/api/table/v2/page", strings.NewReader(tc.body))
		r.Header.Set(sessionHeader, tc.token)
		w := httptest.NewRecorder()
		s.handleTablePageV2(w, r)
		if w.Code != tc.status {
			t.Fatalf("status %d want%d: %s", w.Code, tc.status, w.Body.String())
		}
	}
}

func TestPageCursorFences(t *testing.T) {
	now := time.Unix(2000000000, 0)
	key := []byte(strings.Repeat("k", 32))
	p := pageRequest{ConnectionID: "c", Schema: "s", Table: "t", Profile: "postgres-direct", Limit: 2}
	c := pageCursor{Version: 1, Expires: now.Add(pageTTL).Unix(), Launch: "launch", ConnectionID: "c", Epoch: "epoch", Schema: "s", Table: "t", Definition: "definition", Role: "reader", Limit: 2, After: "9223372036854775807"}
	token, err := signPageCursor(c, key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := verifyPageCursor(token, key, now)
	if err != nil || got != c {
		t.Fatalf("roundtrip %#v %v", got, err)
	}
	if !pageCursorMatches(got, p, "epoch", "launch", "definition", "reader") {
		t.Fatal("matching cursor refused")
	}
	for _, mutate := range []func(*pageCursor){func(c *pageCursor) { c.Epoch = "new" }, func(c *pageCursor) { c.Launch = "new" }, func(c *pageCursor) { c.Definition = "new" }, func(c *pageCursor) { c.Role = "new" }, func(c *pageCursor) { c.Limit = 3 }, func(c *pageCursor) { c.Table = "other" }, func(c *pageCursor) { c.ConnectionID = "other" }} {
		changed := got
		mutate(&changed)
		if pageCursorMatches(changed, p, "epoch", "launch", "definition", "reader") {
			t.Fatal("changed cursor admitted")
		}
	}
	for _, bad := range []string{token + "x", token[:len(token)-2] + "AA", strings.Repeat("x", pageTokenBytes+1), "garbage"} {
		if _, err = verifyPageCursor(bad, key, now); err == nil {
			t.Fatal("tamper accepted")
		}
	}
	if _, err = verifyPageCursor(token, []byte(strings.Repeat("z", 32)), now); err == nil {
		t.Fatal("other process secret admitted")
	}
	if _, err = verifyPageCursor(token, key, now.Add(pageTTL)); err == nil {
		t.Fatal("expired admitted")
	}
	for _, after := range []string{"9223372036854775808", "-9223372036854775809", "01", "1.0", "-0"} {
		changed := c
		changed.After = after
		bad, _ := signPageCursor(changed, key)
		if _, err = verifyPageCursor(bad, key, now); err == nil {
			t.Fatalf("bad exact key %q admitted", after)
		}
	}
	changed := c
	changed.After = "-9223372036854775808"
	min, _ := signPageCursor(changed, key)
	if _, err = verifyPageCursor(min, key, now); err != nil {
		t.Fatal("int64 minimum refused")
	}
	// Cursor never embeds a database URL, SQL statement or raw session token.
	b, _ := json.Marshal(c)
	if strings.Contains(string(b), "postgres://") {
		t.Fatal("unexpected secret")
	}
}

func TestPageScalarProfileAndReadOnly(t *testing.T) {
	for _, oid := range []uint32{16, 17, 20, 21, 23, 25, 1042, 1043, 1700, 1082, 1114, 1184, 2950} {
		if !pageColumnSupported(tableColumnMeta{TypeOID: oid, TypType: "b"}) {
			t.Fatalf("builtin exact scalar OID %d refused", oid)
		}
	}
	for _, col := range []tableColumnMeta{{TypeOID: 114, TypType: "b"}, {TypeOID: 3802, TypType: "b"}, {TypeOID: 1016, TypType: "b"}, {TypeOID: 701, TypType: "b"}, {TypeOID: 20, TypType: "d"}, {TypeOID: 999999, TypType: "e"}, {TypeOID: 999999, TypType: "c"}} {
		if pageColumnSupported(col) {
			t.Fatalf("uncertified wire family admitted: %#v", col)
		}
	}
	meta := &tableMeta{Exists: true, PKCols: []string{"id"}, Columns: map[string]tableColumnMeta{"id": {Name: "id", TypeOID: 20, TypType: "b", IsPK: true}}, Order: []tableColumnMeta{{Name: "id", TypeOID: 20, TypType: "b", IsPK: true}, {Name: "body", TypeOID: 25, TypType: "b"}}}
	if got := pageReadOnlyState(meta); !got.readOnly || got.reason == "" || !got.versioned {
		t.Fatal("SELECT-only table did not retain read-only reason")
	}
	meta.CanDelete = true
	if pageReadOnlyState(meta).readOnly {
		t.Fatal("DELETE privilege refused")
	}
	meta.RuleEvents = "4"
	if !pageReadOnlyState(meta).readOnly {
		t.Fatal("refused DELETE rule falsely admitted")
	}
	meta.Order[1].CanUpdate = true
	if pageReadOnlyState(meta).readOnly {
		t.Fatal("admitted UPDATE privilege refused")
	}
	meta.ForeignDescendant = true
	if got := pageReadOnlyState(meta); !got.readOnly || got.reason != foreignDescendantReason {
		t.Fatal("existing guarded-state reason lost")
	}
	for _, v := range []string{"PostgreSQL 17.3", "postgresql 17.3"} {
		if !pageEngineSupported(v) {
			t.Fatal("native marker refused")
		}
	}
	for _, v := range []string{"Nucleus 1", "PostgreSQL 17 NuClEuS", "PostgreSQL 17 CockroachDB", "PostgreSQL 17 Yugabyte", "PostgreSQL 17 Redshift", "PostgreSQL 17 Greenplum", "PostgreSQL 17 Materialize", "PostgreSQL 17 QuestDB", "PostgreSQL 17 CrateDB"} {
		if pageEngineSupported(v) {
			t.Fatalf("unsupported marker accepted %s", v)
		}
	}
}

func TestPageUnsupportedCodeBoundaries(t *testing.T) {
	w := httptest.NewRecorder()
	writePageUnsupported(w, "exact profile refusal")
	var refusal map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &refusal); err != nil || w.Code != 400 || refusal["state"] != "unsupported-profile" {
		t.Fatal("profile capability code missing")
	}
	s := &Server{sessionToken: "session"}
	r := httptest.NewRequest(http.MethodPost, "/api/table/v2/page", strings.NewReader(`{"profile":"postgres-direct","limit":0}`))
	r.Header.Set(sessionHeader, "session")
	w = httptest.NewRecorder()
	s.handleTablePageV2(w, r)
	var malformed map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &malformed); err != nil || w.Code != 400 || malformed["state"] != nil {
		t.Fatal("malformed request was marked unsupported")
	}
}
