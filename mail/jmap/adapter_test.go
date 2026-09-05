package jmap

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/mail"
)

func adapterFor(t *testing.T, handler http.HandlerFunc) *Adapter {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &Adapter{http: srv.Client(), apiURL: srv.URL, downloadURL: srv.URL + "/download/{accountId}/{blobId}/{name}?type={type}", accountID: "acct", token: "tok"}
}

func TestInitialSyncPaginatesPastServerLimit(t *testing.T) {
	page := 0
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		page++
		if page == 1 {
			_, _ = w.Write([]byte(`{"methodResponses":[["Email/query",{"ids":["m1"],"position":0,"total":2},"0"],["Email/get",{"state":"s1","list":[{"id":"m1","threadId":"t","mailboxIds":{"box":true},"keywords":{}}]},"1"]]}`))
			return
		}
		_, _ = w.Write([]byte(`{"methodResponses":[["Email/query",{"ids":["m2"],"position":1,"total":2},"0"],["Email/get",{"state":"s2","list":[{"id":"m2","threadId":"t","mailboxIds":{"box":true},"keywords":{}}]},"1"]]}`))
	})

	first, err := a.Sync(context.Background(), "box", "")
	if err != nil {
		t.Fatal(err)
	}
	if !first.More || first.Next != "jmap-initial:1" || len(first.Changes) != 1 {
		t.Fatalf("first page = %+v", first)
	}
	second, err := a.Sync(context.Background(), "box", first.Next)
	if err != nil {
		t.Fatal(err)
	}
	if second.More || second.Next != "s2" || len(second.Changes) != 1 {
		t.Fatalf("second page = %+v", second)
	}
}

func TestRawAndAttachmentDownloadAdvertisedBlobs(t *testing.T) {
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			if r.Header.Get("Authorization") != "Bearer tok" {
				t.Error("download omitted bearer token")
			}
			_, _ = w.Write([]byte("blob:" + r.URL.EscapedPath()))
			return
		}
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"properties":["blobId"]`) {
			_, _ = w.Write([]byte(`{"methodResponses":[["Email/get",{"list":[{"blobId":"raw/id"}]},"0"]]}`))
			return
		}
		_, _ = w.Write([]byte(`{"methodResponses":[["Email/get",{"list":[{"attachments":[{"partId":"2","blobId":"part/id","type":"application/pdf","name":"a file.pdf"}]}]},"0"]]}`))
	})

	raw, err := a.Raw(context.Background(), mail.NativeMessageID(mail.ProviderJMAP, "m1"))
	if err != nil {
		t.Fatal(err)
	}
	rawBytes, _ := io.ReadAll(raw)
	raw.Close()
	if !strings.Contains(string(rawBytes), "/raw%2Fid/message.eml") {
		t.Errorf("raw URL was not template-expanded safely: %s", rawBytes)
	}

	part, err := a.Attachment(context.Background(), mail.NativeMessageID(mail.ProviderJMAP, "m1"), "2")
	if err != nil {
		t.Fatal(err)
	}
	partBytes, _ := io.ReadAll(part)
	part.Close()
	if !strings.Contains(string(partBytes), "/part%2Fid/a%20file.pdf") {
		t.Errorf("attachment URL was not template-expanded safely: %s", partBytes)
	}
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

func TestEscapeTemplateValueIsSafeInPathAndQuery(t *testing.T) {
	// The template decides where a value lands, so it has to be safe in both
	// positions. url.PathEscape is not: it leaves '&', '=' and '+' intact,
	// and an attachment filename comes from whoever sent the mail.
	for _, tc := range []struct{ in, want string }{
		{"raw/id", "raw%2Fid"},
		{"a&x=y.txt", "a%26x%3Dy.txt"},
		{"q a+b.txt", "q%20a%2Bb.txt"},
		{"plain-name_1.txt", "plain-name_1.txt"},
	} {
		if got := escapeTemplateValue(tc.in); got != tc.want {
			t.Errorf("escapeTemplateValue(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestDownloadMapsStatusesToSharedErrors(t *testing.T) {
	// Downloads are a second HTTP surface from the API. Its failures have to
	// reach the engine as the same errors, or the recovery and backoff paths
	// are skipped for arriving on the wrong socket.
	for _, tc := range []struct {
		name   string
		status int
		want   error
	}{
		{"unauthorized", http.StatusUnauthorized, mail.ErrReauthRequired},
		{"forbidden", http.StatusForbidden, mail.ErrReauthRequired},
		{"throttled", http.StatusTooManyRequests, mail.ErrRateLimited},
		{"missing blob", http.StatusNotFound, mail.ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					w.WriteHeader(tc.status)
					return
				}
				_, _ = w.Write([]byte(`{"methodResponses":[["Email/get",{"list":[{"blobId":"b"}]},"0"]]}`))
			})
			_, err := a.Raw(context.Background(), mail.NativeMessageID(mail.ProviderJMAP, "m1"))
			if !isErr(err, tc.want) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestAttachmentUnknownPartIsNotFound(t *testing.T) {
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"methodResponses":[["Email/get",{"list":[{"attachments":[]}]},"0"]]}`))
	})

	_, err := a.Attachment(context.Background(), mail.NativeMessageID(mail.ProviderJMAP, "m1"), "nope")
	if !isErr(err, mail.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}
