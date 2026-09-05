package graph

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/mail"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func serve(t *testing.T, handler http.HandlerFunc) *Adapter {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return New(srv.Client())
}

func TestExpiredDeltaTokenBecomesAReset(t *testing.T) {
	// Graph reports an aged-out delta token as 410 Gone. It has to reach
	// the engine as a reset, not an error, so it joins the one recovery
	// path shared with IMAP's UIDVALIDITY change and JMAP's
	// cannotCalculateChanges.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGone)
	}))
	defer srv.Close()

	a := New(srv.Client())
	changes, err := a.Sync(context.Background(), "inbox", mail.Cursor(srv.URL+"/stale"))
	if err != nil {
		t.Fatalf("expected a reset, got error: %v", err)
	}
	if !changes.Reset {
		t.Error("410 Gone did not produce a reset")
	}
}

func TestRemovedAnnotationBecomesADestroy(t *testing.T) {
	body := `{"value":[
		{"id":"AAA","subject":"live"},
		{"id":"BBB","@removed":{"reason":"deleted"}}
	],"@odata.deltaLink":"https://next"}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	a := New(srv.Client())
	changes, err := a.Sync(context.Background(), "inbox", mail.Cursor(srv.URL+"/delta"))
	if err != nil {
		t.Fatal(err)
	}

	var destroyed, updated int
	for _, c := range changes.Changes {
		switch c.Kind {
		case mail.ChangeDestroyed:
			destroyed++
			if c.ID != mail.NativeMessageID(mail.ProviderGraph, "BBB") {
				t.Errorf("destroyed id = %s, want BBB", c.ID)
			}
		case mail.ChangeUpdated:
			updated++
		}
	}
	if destroyed != 1 || updated != 1 {
		t.Errorf("got %d destroyed and %d updated, want 1 each", destroyed, updated)
	}
}

func TestDeltaContinuationIsNeverComplete(t *testing.T) {
	// Only an initial enumeration can be authoritative. A delta
	// continuation reports changes, not contents — marking it complete
	// would make the engine sweep every message that simply had not
	// changed.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"value":[],"@odata.deltaLink":"https://next"}`))
	}))
	defer srv.Close()

	a := New(srv.Client())
	changes, err := a.Sync(context.Background(), "inbox", mail.Cursor(srv.URL+"/delta"))
	if err != nil {
		t.Fatal(err)
	}
	if changes.Complete {
		t.Error("a delta continuation was marked as a complete listing")
	}
	if changes.More {
		t.Error("a page with a deltaLink and no nextLink reported more pages")
	}
	if changes.Next != "https://next" {
		t.Errorf("Next = %q, want the deltaLink", changes.Next)
	}
}

func TestNextLinkPagesBeforeDeltaLink(t *testing.T) {
	// While a nextLink is present the run is still paging, and the cursor
	// must follow it rather than jumping to a deltaLink.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"value":[],"@odata.nextLink":"https://page2"}`))
	}))
	defer srv.Close()

	a := New(srv.Client())
	changes, err := a.Sync(context.Background(), "inbox", mail.Cursor(srv.URL+"/delta"))
	if err != nil {
		t.Fatal(err)
	}
	if !changes.More {
		t.Error("a page with a nextLink did not report more pages")
	}
	if changes.Next != "https://page2" {
		t.Errorf("Next = %q, want the nextLink", changes.Next)
	}
}

func TestMailboxesFollowsEveryNextLink(t *testing.T) {
	var paths []string
	a := New(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.String())
		body := `{"value":[{"id":"inbox","displayName":"Inbox","wellKnownName":"inbox"}],"@odata.nextLink":"https://graph.test/page2"}`
		if len(paths) == 2 {
			body = `{"value":[{"id":"archive","displayName":"Archive","wellKnownName":"archive"}]}`
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})})

	boxes, err := a.Mailboxes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(boxes) != 2 || boxes[0].ID != "inbox" || boxes[1].ID != "archive" {
		t.Fatalf("mailboxes = %+v, want both pages", boxes)
	}
	if len(paths) != 2 || paths[1] != "https://graph.test/page2" {
		t.Fatalf("requests = %v, want nextLink page", paths)
	}
}

func TestStatusMapping(t *testing.T) {
	tests := []struct {
		status int
		want   error
	}{
		{http.StatusUnauthorized, mail.ErrReauthRequired},
		{http.StatusForbidden, mail.ErrReauthRequired},
		{http.StatusNotFound, mail.ErrNotFound},
		{http.StatusTooManyRequests, mail.ErrRateLimited},
		{http.StatusServiceUnavailable, mail.ErrRateLimited},
		{http.StatusGone, mail.ErrCursorInvalid},
	}
	for _, tt := range tests {
		resp := &http.Response{
			StatusCode: tt.status,
			Body:       http.NoBody,
		}
		err := statusError(resp)
		if !errors.Is(err, tt.want) {
			t.Errorf("status %d mapped to %v, want %v", tt.status, err, tt.want)
		}
	}
}

func TestSuccessStatusesAreNotErrors(t *testing.T) {
	for _, code := range []int{200, 201, 202, 204} {
		resp := &http.Response{StatusCode: code, Body: http.NoBody}
		if err := statusError(resp); err != nil {
			t.Errorf("status %d = %v, want nil", code, err)
		}
	}
}

func TestRoleFromWellKnownName(t *testing.T) {
	tests := map[string]mail.Role{
		"inbox":        mail.RoleInbox,
		"sentitems":    mail.RoleSent,
		"drafts":       mail.RoleDrafts,
		"deleteditems": mail.RoleTrash,
		"junkemail":    mail.RoleJunk,
		"archive":      mail.RoleArchive,
		"":             mail.RoleNone,
		"somefolder":   mail.RoleNone,
	}
	for in, want := range tests {
		if got := roleFrom(in); got != want {
			t.Errorf("roleFrom(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestToEnvelopeNormalisesFlags(t *testing.T) {
	m := graphMessage{
		ID:                "AAA",
		ConversationID:    "conv-1",
		Subject:           "Hello",
		IsRead:            true,
		IsDraft:           false,
		InternetMessageID: "<abc@example.com>",
		ParentFolderID:    "inbox-id",
	}
	m.Flag = &struct {
		FlagStatus string `json:"flagStatus"`
	}{FlagStatus: "flagged"}
	m.From = &graphRecipient{}
	m.From.EmailAddress.Name = "Alice"
	m.From.EmailAddress.Address = "alice@example.com"

	env := m.toEnvelope()

	if !env.Keywords.Seen {
		t.Error("isRead did not map to Seen")
	}
	if !env.Keywords.Flagged {
		t.Error("flagStatus flagged did not map to Flagged")
	}
	if env.Keywords.Draft {
		t.Error("Draft set despite isDraft false")
	}
	if len(env.From) != 1 || env.From[0].Email != "alice@example.com" {
		t.Errorf("From = %+v, want alice@example.com", env.From)
	}
	if env.From[0].Name != "Alice" {
		t.Errorf("From name = %q, want Alice", env.From[0].Name)
	}
	if env.ThreadID != "conv-1" {
		t.Errorf("ThreadID = %q, want conv-1", env.ThreadID)
	}
	if len(env.MailboxIDs) != 1 || env.MailboxIDs[0] != "inbox-id" {
		t.Errorf("MailboxIDs = %v, want [inbox-id]", env.MailboxIDs)
	}
	if env.Fingerprint == "" {
		t.Error("fingerprint was not computed")
	}
}

func TestKeywordPatchInvertsCorrectly(t *testing.T) {
	seen := keywordPatch(mail.Operation{Kind: mail.OpAddKeyword, Keyword: "seen"})
	if seen["isRead"] != true {
		t.Errorf("add seen = %v, want isRead true", seen)
	}
	unseen := keywordPatch(mail.Operation{Kind: mail.OpRemoveKeyword, Keyword: "seen"})
	if unseen["isRead"] != false {
		t.Errorf("remove seen = %v, want isRead false", unseen)
	}

	flagged := keywordPatch(mail.Operation{Kind: mail.OpAddKeyword, Keyword: "flagged"})
	flag, ok := flagged["flag"].(map[string]any)
	if !ok || flag["flagStatus"] != "flagged" {
		t.Errorf("add flagged = %v, want flagStatus flagged", flagged)
	}
}

func TestNativeIDStripsThePrefix(t *testing.T) {
	id := mail.NativeMessageID(mail.ProviderGraph, "AAMkAGI2")
	if got := nativeID(id); got != "AAMkAGI2" {
		t.Errorf("nativeID = %q, want AAMkAGI2", got)
	}
	// An identity from another provider must not be silently mangled.
	other := mail.NativeMessageID(mail.ProviderGmail, "xyz")
	if got := nativeID(other); !strings.Contains(got, "gmail") {
		t.Errorf("nativeID stripped a foreign provider prefix: %q", got)
	}
}
