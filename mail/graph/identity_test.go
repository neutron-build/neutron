package graph

// Identity-stability machinery tests: the verified mailbox key
// (GraphMailboxKey), the batched Exchange-ID translator
// (TranslateExchangeIds), and the opt-in immutable-ID preference's
// built-not-enabled guarantee (docs/identity-stability.md §3 in the
// consumer repo is the driving plan).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/mail"
)

func TestGraphMailboxKeyReturnsTheMeID(t *testing.T) {
	var gotPath, gotSelect, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotSelect = r.Method, r.URL.Path, r.URL.Query().Get("$select")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"e8626f0c-1eaf-4b3f-b0a6-2d6f14c95b41"}`))
	}))
	t.Cleanup(srv.Close)
	useBase(t, srv.URL)
	a := New(srv.Client())

	key, err := a.GraphMailboxKey(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// The raw /me id IS the verified mailbox key — not an email-derived
	// stand-in.
	if key != "e8626f0c-1eaf-4b3f-b0a6-2d6f14c95b41" {
		t.Errorf("mailbox key = %q, want the raw /me id", key)
	}
	if gotMethod != http.MethodGet || gotPath != "/me" || gotSelect != "id" {
		t.Errorf("verification request = %s %s?$select=%s, want GET /me?$select=id",
			gotMethod, gotPath, gotSelect)
	}
}

func TestGraphMailboxKeyRefusesAnEmptyID(t *testing.T) {
	// Binding refuses without a verified key, so a provider answer that
	// carries no id must be an error, never an empty success.
	for _, body := range []string{`{}`, `{"id":""}`} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(srv.Close)
		useBase(t, srv.URL)
		a := New(srv.Client())
		if key, err := a.GraphMailboxKey(context.Background()); err == nil || key != "" {
			t.Errorf("body %s: key = %q, err = %v; want a refusal", body, key, err)
		}
	}
}

// translateCall is one POST recorded by the translate fake, carrying the
// decoded request body so batch splitting is assertable from the request
// log rather than inferred from the result.
type translateCall struct {
	Method      string
	Path        string
	ContentType string
	Body        struct {
		InputIDs     []string `json:"inputIds"`
		SourceIDType string   `json:"sourceIdType"`
		TargetIDType string   `json:"targetIdType"`
	}
}

// translateFake answers every translateExchangeIds POST by converting each
// input id to "immutable:"+id, recording the call.
func translateFake(t *testing.T, calls *[]translateCall, override func(input []string) string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call translateCall
		call.Method, call.Path, call.ContentType = r.Method, r.URL.Path, r.Header.Get("Content-Type")
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading translate request: %v", err)
		}
		if err := json.Unmarshal(raw, &call.Body); err != nil {
			t.Errorf("translate request body did not decode: %v (%s)", err, raw)
		}
		*calls = append(*calls, call)

		body := `{"value":[]}`
		if override != nil {
			body = override(call.Body.InputIDs)
		} else {
			type pair struct {
				SourceID string `json:"sourceId"`
				TargetID string `json:"targetId"`
			}
			value := make([]pair, 0, len(call.Body.InputIDs))
			for _, id := range call.Body.InputIDs {
				value = append(value, pair{id, "immutable:" + id})
			}
			resp, err := json.Marshal(map[string]any{"value": value})
			if err != nil {
				t.Errorf("marshaling translate response: %v", err)
			}
			body = string(resp)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
}

func TestTranslateExchangeIdsMapsOneBatch(t *testing.T) {
	var calls []translateCall
	srv := translateFake(t, &calls, nil)
	t.Cleanup(srv.Close)
	useBase(t, srv.URL)
	a := New(srv.Client())

	got, err := a.TranslateExchangeIds(context.Background(), []string{"AAMk-one", "AAMk-two"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"AAMk-one": "immutable:AAMk-one", "AAMk-two": "immutable:AAMk-two"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("translations = %v, want %v", got, want)
	}
	if len(calls) != 1 {
		t.Fatalf("translate requests = %d, want a single batch", len(calls))
	}
	call := calls[0]
	if call.Method != http.MethodPost || call.Path != "/me/translateExchangeIds" {
		t.Errorf("request = %s %s, want POST /me/translateExchangeIds", call.Method, call.Path)
	}
	if call.ContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", call.ContentType)
	}
	if !reflect.DeepEqual(call.Body.InputIDs, []string{"AAMk-one", "AAMk-two"}) {
		t.Errorf("inputIds = %v, want both ids in order", call.Body.InputIDs)
	}
	// The documented translation: mutable Graph rest ids in, immutable
	// rest ids out.
	if call.Body.SourceIDType != "restId" || call.Body.TargetIDType != "restImmutableEntryId" {
		t.Errorf("id types = %s -> %s, want restId -> restImmutableEntryId",
			call.Body.SourceIDType, call.Body.TargetIDType)
	}
}

func TestTranslateExchangeIdsSplitsAtTheDocumentedLimit(t *testing.T) {
	// 2501 ids must split at Microsoft's documented 1,000-id request
	// ceiling: 1000 + 1000 + 501, asserted from the fake's request log.
	var calls []translateCall
	srv := translateFake(t, &calls, nil)
	t.Cleanup(srv.Close)
	useBase(t, srv.URL)
	a := New(srv.Client())

	ids := make([]string, 2501)
	for i := range ids {
		ids[i] = fmt.Sprintf("AAMk-%04d", i)
	}
	got, err := a.TranslateExchangeIds(context.Background(), ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(ids) {
		t.Fatalf("translated %d of %d ids", len(got), len(ids))
	}
	if got["AAMk-0000"] != "immutable:AAMk-0000" || got["AAMk-2500"] != "immutable:AAMk-2500" {
		t.Errorf("boundary translations wrong: %q, %q", got["AAMk-0000"], got["AAMk-2500"])
	}

	wantSizes := []int{translateBatchLimit, translateBatchLimit, 501}
	if len(calls) != len(wantSizes) {
		t.Fatalf("batch count = %d, want %d", len(calls), len(wantSizes))
	}
	for i, size := range wantSizes {
		if len(calls[i].Body.InputIDs) != size {
			t.Errorf("batch %d carried %d ids, want %d", i, len(calls[i].Body.InputIDs), size)
		}
	}
	if calls[0].Body.InputIDs[0] != "AAMk-0000" || calls[2].Body.InputIDs[0] != "AAMk-2000" {
		t.Error("batches did not carry the ids in order")
	}
}

func TestTranslateExchangeIdsFailsNamingARejectedID(t *testing.T) {
	// A conversion Microsoft reports as failed (convertIdResult carries
	// errorDetails and no targetId) must surface as an error naming the
	// id — a silently dropped translation would strand every retained
	// product reference to it.
	rejected := `{"value":[
		{"sourceId":"AAMk-good","targetId":"immutable:AAMk-good"},
		{"sourceId":"AAMk-bad","errorDetails":{"code":"ErrorCannotFindItem","message":"cannot find item"}}
	]}`
	var calls []translateCall
	srv := translateFake(t, &calls, func([]string) string { return rejected })
	t.Cleanup(srv.Close)
	useBase(t, srv.URL)
	a := New(srv.Client())

	_, err := a.TranslateExchangeIds(context.Background(), []string{"AAMk-good", "AAMk-bad"})
	if err == nil {
		t.Fatal("a Microsoft-reported failed conversion came back as success")
	}
	if !strings.Contains(err.Error(), "AAMk-bad") {
		t.Errorf("error does not name the rejected id: %v", err)
	}
}

func TestTranslateExchangeIdsFailsNamingAnOmittedID(t *testing.T) {
	// An id the response simply never mentions is as unusable as a
	// rejected one; it must also refuse rather than pass back a partial
	// map that looks complete.
	partial := `{"value":[{"sourceId":"AAMk-good","targetId":"immutable:AAMk-good"}]}`
	var calls []translateCall
	srv := translateFake(t, &calls, func([]string) string { return partial })
	t.Cleanup(srv.Close)
	useBase(t, srv.URL)
	a := New(srv.Client())

	_, err := a.TranslateExchangeIds(context.Background(), []string{"AAMk-good", "AAMk-gone"})
	if err == nil {
		t.Fatal("an id the response omitted came back as success")
	}
	if !strings.Contains(err.Error(), "AAMk-gone") {
		t.Errorf("error does not name the omitted id: %v", err)
	}
}

func TestTranslateExchangeIdsEmptyInputMakesNoCall(t *testing.T) {
	var calls []translateCall
	srv := translateFake(t, &calls, nil)
	t.Cleanup(srv.Close)
	useBase(t, srv.URL)
	a := New(srv.Client())

	for _, in := range [][]string{nil, {}} {
		got, err := a.TranslateExchangeIds(context.Background(), in)
		if err != nil {
			t.Fatalf("input %v: %v", in, err)
		}
		if len(got) != 0 {
			t.Errorf("input %v: translations = %v, want empty", in, got)
		}
	}
	if len(calls) != 0 {
		t.Errorf("empty input issued %d HTTP calls, want none", len(calls))
	}
}

// driveGetPaths exercises every GET path the adapter has — folder listing,
// delta sync, message refetch, body plus attachment metadata — so a header
// assertion sees the whole GET surface, not one endpoint.
func driveGetPaths(t *testing.T, a *Adapter) error {
	t.Helper()
	ctx := context.Background()
	if _, err := a.Sync(ctx, "inbox", ""); err != nil {
		return err
	}
	if _, err := a.Mailboxes(ctx); err != nil {
		return err
	}
	if _, err := a.Envelopes(ctx, []mail.MessageID{mail.NativeMessageID(mail.ProviderGraph, "m1")}); err != nil {
		return err
	}
	_, err := a.Body(ctx, mail.NativeMessageID(mail.ProviderGraph, "m1"))
	return err
}

// THE BUILT-NOT-ENABLED GUARANTEE: with PreferImmutableIds at its zero
// value the wire bytes of every existing request are unchanged. The header
// set a GET carries is exactly the one the adapter always set — nothing
// more, nothing less.
func TestPreferImmutableIdsIsInertByDefault(t *testing.T) {
	var requests []*http.Request
	a := New(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests = append(requests, r.Clone(r.Context()))
		return respondJSON(`{"value":[],"@odata.deltaLink":"https://next"}`)
	})})

	if err := driveGetPaths(t, a); err != nil {
		t.Fatal(err)
	}
	if len(requests) == 0 {
		t.Fatal("no GET requests captured")
	}
	for _, r := range requests {
		if got := r.Header.Get("Prefer"); got != "" {
			t.Errorf("GET %s sent Prefer: %q with the option off", r.URL.Path, got)
		}
		want := http.Header{}
		want.Set("Accept", "application/json")
		if !reflect.DeepEqual(r.Header, want) {
			t.Errorf("GET %s headers = %v, want exactly %v", r.URL.Path, r.Header, want)
		}
	}
}

func TestPreferImmutableIdsAddsTheHeaderWhenOptedIn(t *testing.T) {
	var requests []*http.Request
	a := New(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests = append(requests, r.Clone(r.Context()))
		return respondJSON(`{"value":[],"@odata.deltaLink":"https://next"}`)
	})})
	a.PreferImmutableIds = true

	if err := driveGetPaths(t, a); err != nil {
		t.Fatal(err)
	}
	// Mutations stay byte-identical even opted in: the header is a GET
	// preference, and send() must not grow one.
	if err := a.Apply(context.Background(), mail.Operation{
		Kind: mail.OpAddKeyword, Keyword: "seen",
		IDs: []mail.MessageID{mail.NativeMessageID(mail.ProviderGraph, "m1")},
	}); err != nil {
		t.Fatal(err)
	}

	var gets, mutations int
	for _, r := range requests {
		if r.Method == http.MethodGet {
			gets++
			if got := r.Header.Get("Prefer"); got != `IdType="ImmutableId"` {
				t.Errorf("GET %s carried Prefer %q, want IdType=%q", r.URL.Path, got, "ImmutableId")
			}
			continue
		}
		mutations++
		if got := r.Header.Get("Prefer"); got != "" {
			t.Errorf("%s %s carried Prefer %q, want none", r.Method, r.URL.Path, got)
		}
		want := http.Header{}
		want.Set("Content-Type", "application/json")
		if !reflect.DeepEqual(r.Header, want) {
			t.Errorf("%s %s headers = %v, want exactly %v", r.Method, r.URL.Path, r.Header, want)
		}
	}
	if gets == 0 || mutations == 0 {
		t.Fatalf("captured %d GETs and %d mutations, want both", gets, mutations)
	}
}
