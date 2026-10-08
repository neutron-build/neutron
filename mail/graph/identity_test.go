package graph

import (
	"context"
	"encoding/json"
	"github.com/neutron-build/neutron/mail"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestTranslationRequestAndIncompleteResponseRefusal(t *testing.T) {
	for _, response := range []string{
		`{"value":[{"sourceId":"CaseA","targetId":"ImmutableA"},{"sourceId":"caseB","targetId":"ImmutableB"}]}`,
		`{"value":[{"sourceId":"CaseA","targetId":"ImmutableA"}]}`,
		`{"value":[{"sourceId":"CaseA","targetId":"ImmutableA"},{"sourceId":"caseB","targetId":"ImmutableA"}]}`,
		`{"value":[{"sourceId":"CaseA","targetId":"ImmutableA"},{"sourceId":"caseB","errorDetails":{"code":"bad"}}]}`,
	} {
		hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != "POST" || r.URL.Path != "/v1.0/me/translateExchangeIds" {
				t.Fatal("incorrect endpoint", r.URL)
			}
			var body struct {
				InputIDs []string `json:"inputIds"`
				Source   string   `json:"sourceIdType"`
				Target   string   `json:"targetIdType"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Source != "restId" || body.Target != "restImmutableEntryId" || len(body.InputIDs) != 2 || body.InputIDs[0] != "CaseA" || body.InputIDs[1] != "caseB" {
				t.Fatal("translation contract", body)
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response)), Header: make(http.Header)}, nil
		})}
		pairs, err := New(hc).TranslateGraphIDs(context.Background(), []mail.MessageID{mail.NativeMessageID(mail.ProviderGraph, "CaseA"), mail.NativeMessageID(mail.ProviderGraph, "caseB")})
		if strings.Contains(response, "ImmutableB") {
			if err != nil || len(pairs) != 2 {
				t.Fatal(pairs, err)
			}
		} else if err == nil {
			t.Fatal("invalid translation accepted", pairs)
		}
	}
}
func TestAccountHeaderPolicyCoversReadRawAttachmentMutation(t *testing.T) {
	for _, format := range []string{mail.GraphLegacyIDs, mail.GraphImmutableIDs} {
		calls := 0
		hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			has := strings.Contains(r.Header.Get("Prefer"), `IdType="ImmutableId"`)
			if has != (format == mail.GraphImmutableIDs) {
				t.Fatal("incorrect identity preference", r.Header)
			}
			body := `{"id":"A"}`
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		})}
		a, err := NewWithIdentityFormat(hc, format)
		if err != nil {
			t.Fatal(err)
		}
		var out any
		if err := a.get(context.Background(), "/me/messages/A", &out); err != nil {
			t.Fatal(err)
		}
		raw, err := a.Raw(context.Background(), mail.NativeMessageID(mail.ProviderGraph, "A"))
		if err != nil {
			t.Fatal(err)
		}
		raw.Close()
		part, err := a.Attachment(context.Background(), mail.NativeMessageID(mail.ProviderGraph, "A"), "p")
		if err != nil {
			t.Fatal(err)
		}
		part.Close()
		if err := a.Apply(context.Background(), mail.Operation{Kind: mail.OpMove, IDs: []mail.MessageID{mail.NativeMessageID(mail.ProviderGraph, "A")}, Target: "archive"}); err != nil {
			t.Fatal(err)
		}
		if calls != 4 {
			t.Fatal(calls)
		}
	}
}
func TestIdentityPreferenceMergesAndRejectsUploadOrigin(t *testing.T) {
	calls := 0
	transport, err := WithIdentityPreference(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Header.Get("Prefer") != `outlook.body-content-type="text", IdType="ImmutableId"` {
			t.Fatal(r.Header)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	}), mail.GraphImmutableIDs)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("GET", baseURL+"/me/messages/A", nil)
	req.Header.Set("Prefer", `outlook.body-content-type="text", IdType="RestId"`)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if req.Header.Get("Prefer") != `outlook.body-content-type="text", IdType="RestId"` {
		t.Fatal("transport mutated original")
	}
	req, _ = http.NewRequest("PUT", "https://outlook.office.com/preauthenticated-upload", nil)
	if _, err := transport.RoundTrip(req); err == nil || calls != 1 {
		t.Fatal("upload escaped Graph restriction", err)
	}
}
