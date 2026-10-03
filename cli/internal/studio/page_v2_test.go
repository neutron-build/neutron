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
