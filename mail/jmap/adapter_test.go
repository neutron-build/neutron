package jmap

import (
	"context"
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
