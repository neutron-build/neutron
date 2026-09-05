package jmap

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/neutron-build/neutron/mail"
)

func adapterFor(t *testing.T, handler http.HandlerFunc) *Adapter {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &Adapter{http: srv.Client(), apiURL: srv.URL, accountID: "acct", token: "tok"}
}

func TestCannotCalculateChangesBecomesAReset(t *testing.T) {
	// JMAP's name for "your cursor is unusable". It must reach the engine
	// as a reset so it shares the recovery path with IMAP's UIDVALIDITY
	// change and Graph's expired delta token.
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"methodResponses":[["error",{"type":"cannotCalculateChanges"},"0"]]}`))
	})

	changes, err := a.Sync(context.Background(), "box", "old-state")
	if err != nil {
		t.Fatalf("expected a reset, got error: %v", err)
	}
	if !changes.Reset {
		t.Error("cannotCalculateChanges did not produce a reset")
	}
}

func TestUnauthorizedBecomesReauthRequired(t *testing.T) {
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})

	_, err := a.Sync(context.Background(), "box", "state")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !isErr(err, mail.ErrReauthRequired) {
		t.Errorf("err = %v, want ErrReauthRequired", err)
	}
}

func TestTooManyRequestsBecomesRateLimited(t *testing.T) {
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	})

	_, err := a.Sync(context.Background(), "box", "state")
	if !isErr(err, mail.ErrRateLimited) {
		t.Errorf("err = %v, want ErrRateLimited", err)
	}
}

func isErr(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestChangesMapToTheThreeKinds(t *testing.T) {
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"methodResponses":[
			["Email/changes",{
				"newState":"s2","hasMoreChanges":false,
				"created":["m1"],"updated":["m2"],"destroyed":["m3"]
			},"0"]
		]}`))
	})

	changes, err := a.Sync(context.Background(), "box", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if changes.Next != "s2" {
		t.Errorf("Next = %q, want s2", changes.Next)
	}

	// created and updated need a follow-up Email/get, which this handler
	// does not answer, so only the destroy survives — which is the point
	// worth asserting: a destroy never requires a second round trip.
	var destroyed int
	for _, c := range changes.Changes {
		if c.Kind == mail.ChangeDestroyed {
			destroyed++
			if c.ID != mail.NativeMessageID(mail.ProviderJMAP, "m3") {
				t.Errorf("destroyed id = %s, want m3", c.ID)
			}
		}
	}
	if destroyed != 1 {
		t.Errorf("got %d destroys, want 1", destroyed)
	}
}

func TestRoleMapping(t *testing.T) {
	tests := map[string]mail.Role{
		"inbox":   mail.RoleInbox,
		"archive": mail.RoleArchive,
		"sent":    mail.RoleSent,
		"drafts":  mail.RoleDrafts,
		"trash":   mail.RoleTrash,
		"junk":    mail.RoleJunk,
		"spam":    mail.RoleJunk,
		"all":     mail.RoleAll,
		"":        mail.RoleNone,
		"custom":  mail.RoleNone,
	}
	for in, want := range tests {
		if got := roleFrom(in); got != want {
			t.Errorf("roleFrom(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestKeywordsFromJMAPFlags(t *testing.T) {
	kw := keywordsFrom(map[string]bool{
		"$seen":     true,
		"$flagged":  true,
		"$draft":    false,
		"$answered": true,
		"custom":    true,
	})

	if !kw.Seen || !kw.Flagged || !kw.Answered {
		t.Errorf("standard keywords not mapped: %+v", kw)
	}
	if kw.Draft {
		t.Error("a keyword set to false was treated as present")
	}
	if len(kw.Custom) != 1 || kw.Custom[0] != "custom" {
		t.Errorf("Custom = %v, want [custom]", kw.Custom)
	}
}

func TestJMAPKeywordNamesGetTheDollarPrefix(t *testing.T) {
	if got := jmapKeyword("seen"); got != "$seen" {
		t.Errorf("jmapKeyword(seen) = %q, want $seen", got)
	}
	if got := jmapKeyword("Flagged"); got != "$flagged" {
		t.Errorf("jmapKeyword(Flagged) = %q, want $flagged", got)
	}
	// A user-defined keyword keeps its own name.
	if got := jmapKeyword("project-x"); got != "project-x" {
		t.Errorf("jmapKeyword(project-x) = %q, want it unchanged", got)
	}
}

func TestNativeIDRoundTrip(t *testing.T) {
	id := mail.NativeMessageID(mail.ProviderJMAP, "Mdeadbeef")
	if got := nativeID(id); got != "Mdeadbeef" {
		t.Errorf("nativeID = %q, want Mdeadbeef", got)
	}
}

func TestDecodeEmailsBuildsCanonicalEnvelopes(t *testing.T) {
	raw := []byte(`{"list":[{
		"id":"m1","threadId":"t1",
		"mailboxIds":{"box1":true,"box2":false},
		"keywords":{"$seen":true},
		"size":1234,
		"subject":"Hello",
		"preview":"Hi there",
		"hasAttachment":true,
		"from":[{"name":"Alice","email":"alice@example.com"}],
		"to":[{"name":"Bob","email":"bob@example.com"}],
		"messageId":["<abc@example.com>"],
		"references":["<root@example.com>"]
	}]}`)

	envs, err := decodeEmails(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != 1 {
		t.Fatalf("got %d envelopes, want 1", len(envs))
	}
	e := envs[0]

	if e.ID != mail.NativeMessageID(mail.ProviderJMAP, "m1") {
		t.Errorf("ID = %s", e.ID)
	}
	if e.ThreadID != "t1" {
		t.Errorf("ThreadID = %q, want t1", e.ThreadID)
	}
	// A mailbox mapped to false is not a membership.
	if len(e.MailboxIDs) != 1 || e.MailboxIDs[0] != "box1" {
		t.Errorf("MailboxIDs = %v, want [box1]", e.MailboxIDs)
	}
	if !e.Keywords.Seen {
		t.Error("$seen did not map to Seen")
	}
	if e.MessageIDHeader != "<abc@example.com>" {
		t.Errorf("MessageIDHeader = %q", e.MessageIDHeader)
	}
	if e.Fingerprint == "" {
		t.Error("fingerprint was not computed")
	}
	if len(e.From) != 1 || e.From[0].Email != "alice@example.com" {
		t.Errorf("From = %+v", e.From)
	}
}

// downloadAdapter wires both the API and the download template at one server,
// so a test can answer Email/get and serve blob bytes from the same handler.
func downloadAdapter(t *testing.T, handler http.HandlerFunc) *Adapter {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &Adapter{
		http:        srv.Client(),
		apiURL:      srv.URL,
		downloadURL: srv.URL + "/dl/{accountId}/{blobId}/{name}?type={type}",
		accountID:   "acct",
		token:       "tok",
	}
}

func TestExpandDownloadEscapesEveryPlaceholder(t *testing.T) {
	// A blob id is server-chosen and a filename comes from the sender, so
	// neither may be pasted into a URL raw. A '/' that survives would move
	// the request to a different path entirely.
	a := &Adapter{
		accountID:   "acct",
		downloadURL: "https://h/dl/{accountId}/{blobId}/{name}?type={type}",
	}
	got, err := a.expandDownload("b/1", "text/plain", "q&a report.txt")
	if err != nil {
		t.Fatalf("expandDownload: %v", err)
	}
	// '&' is escaped too: the template may put a filename in the query, and a
	// sender-chosen name must not be able to start a new parameter there.
	want := "https://h/dl/acct/b%2F1/q%26a%20report.txt?type=text%2Fplain"
	if got != want {
		t.Errorf("expandDownload =\n  %s\nwant\n  %s", got, want)
	}
}

func TestExpandDownloadWithoutATemplateIsAnError(t *testing.T) {
	a := &Adapter{accountID: "acct"}
	if _, err := a.expandDownload("blob", "text/plain", "f.txt"); err == nil {
		t.Error("expected an error when the session advertised no downloadUrl")
	}
}

func TestRawDownloadsTheMessageBlob(t *testing.T) {
	a := downloadAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"methodResponses":[["Email/get",{"list":[{"blobId":"blob-9"}]},"0"]]}`))
			return
		}
		if r.URL.Path != "/dl/acct/blob-9/m1.eml" {
			t.Errorf("download path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization = %q", got)
		}
		_, _ = w.Write([]byte("From: a@b\r\n\r\nhi"))
	})

	rc, err := a.Raw(context.Background(), "m1")
	if err != nil {
		t.Fatalf("Raw: %v", err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if string(body) != "From: a@b\r\n\r\nhi" {
		t.Errorf("body = %q", body)
	}
}

func TestAttachmentResolvesPartToBlob(t *testing.T) {
	a := downloadAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"methodResponses":[["Email/get",{"list":[{"attachments":[
				{"partId":"2","blobId":"blob-2","type":"application/pdf","name":"invoice.pdf"}]}]},"0"]]}`))
			return
		}
		if r.URL.Path != "/dl/acct/blob-2/invoice.pdf" {
			t.Errorf("download path = %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("type"); got != "application/pdf" {
			t.Errorf("type = %q", got)
		}
		_, _ = w.Write([]byte("%PDF-1.4"))
	})

	rc, err := a.Attachment(context.Background(), "m1", "2")
	if err != nil {
		t.Fatalf("Attachment: %v", err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if string(body) != "%PDF-1.4" {
		t.Errorf("body = %q", body)
	}
}

func TestAttachmentAcceptsABlobIDAsThePart(t *testing.T) {
	// A caller holding a blob id from an earlier Body should not have to
	// re-fetch the message just to name the part.
	a := downloadAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"methodResponses":[["Email/get",{"list":[{"attachments":[
				{"partId":"2","blobId":"blob-2","type":"text/plain","name":"a.txt"}]}]},"0"]]}`))
			return
		}
		_, _ = w.Write([]byte("ok"))
	})

	rc, err := a.Attachment(context.Background(), "m1", "blob-2")
	if err != nil {
		t.Fatalf("Attachment by blob id: %v", err)
	}
	rc.Close()
}

func TestAttachmentUnknownPartIsNotFound(t *testing.T) {
	a := downloadAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"methodResponses":[["Email/get",{"list":[{"attachments":[]}]},"0"]]}`))
	})

	_, err := a.Attachment(context.Background(), "m1", "nope")
	if !isErr(err, mail.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestDownloadMapsStatusesToSharedErrors(t *testing.T) {
	// The download endpoint is a separate HTTP surface from the API, so its
	// failures have to reach the engine as the same errors the API's do.
	for _, tc := range []struct {
		name   string
		status int
		want   error
	}{
		{"unauthorized", http.StatusUnauthorized, mail.ErrReauthRequired},
		{"forbidden", http.StatusForbidden, mail.ErrReauthRequired},
		{"missing blob", http.StatusNotFound, mail.ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := downloadAdapter(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					_, _ = w.Write([]byte(`{"methodResponses":[["Email/get",{"list":[{"blobId":"b"}]},"0"]]}`))
					return
				}
				w.WriteHeader(tc.status)
			})
			if _, err := a.Raw(context.Background(), "m1"); !isErr(err, tc.want) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
		})
	}
}
