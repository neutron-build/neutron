package jmap

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

func adapterFor(t *testing.T, handler http.HandlerFunc) *Adapter {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &Adapter{http: srv.Client(), apiURL: srv.URL, downloadURL: srv.URL + "/download/{accountId}/{blobId}/{name}?type={type}", accountID: "acct", token: "tok"}
}

func TestInitialSyncPaginatesPastServerLimit(t *testing.T) {
	request := 0
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		request++
		switch request {
		case 1:
			// First page: baseline Email/get + query + page get.
			_, _ = w.Write([]byte(`{"methodResponses":[
				["Email/get",{"state":"s0","list":[]},"b"],
				["Email/query",{"ids":["m1"],"position":0,"total":2,"queryState":"q1"},"q"],
				["Email/get",{"state":"s1","list":[{"id":"m1","threadId":"t","mailboxIds":{"box":true},"keywords":{}}]},"g"]
			]}`))
		case 2:
			// Final page: query + get.
			_, _ = w.Write([]byte(`{"methodResponses":[
				["Email/query",{"ids":["m2"],"position":1,"total":2,"queryState":"q1"},"q"],
				["Email/get",{"state":"s2","list":[{"id":"m2","threadId":"t","mailboxIds":{"box":true},"keywords":{}}]},"g"]
			]}`))
		case 3:
			// Baseline catch-up: m1 was modified and m3 destroyed while the
			// enumeration paginated.
			_, _ = w.Write([]byte(`{"methodResponses":[
				["Email/changes",{"newState":"s9","hasMoreChanges":false,"created":[],"updated":["m1"],"destroyed":["m3"]},"0"]
			]}`))
		default:
			// Catch-up envelope refetch for m1.
			_, _ = w.Write([]byte(`{"methodResponses":[
				["Email/get",{"state":"s9","list":[{"id":"m1","threadId":"t","mailboxIds":{"box":true},"keywords":{"$seen":true}}]},"0"]
			]}`))
		}
	})

	first, err := a.Sync(context.Background(), "box", "")
	if err != nil {
		t.Fatal(err)
	}
	if !first.More || len(first.Changes) != 1 {
		t.Fatalf("first page = %+v", first)
	}
	state, isInitial, wellFormed := decodeInitialCursor(first.Next)
	if !isInitial || !wellFormed || state.Position != 1 || state.Baseline != "s0" || state.QueryState != "q1" {
		t.Fatalf("first cursor = %+v initial=%v wellFormed=%v, want position 1 baseline s0 queryState q1", state, isInitial, wellFormed)
	}
	second, err := a.Sync(context.Background(), "box", first.Next)
	if err != nil {
		t.Fatal(err)
	}
	if second.More || second.Next != "s9" || !second.Complete {
		t.Fatalf("second page = %+v", second)
	}
	// The page's own message, the catch-up update, and the destroy all
	// survive: the final cursor is past every change it never observed
	// (audit JMAP-03).
	var updated, destroyed int
	for _, c := range second.Changes {
		switch c.Kind {
		case mail.ChangeCreated:
		case mail.ChangeUpdated:
			updated++
		case mail.ChangeDestroyed:
			destroyed++
			if c.ID != mail.NativeMessageID(mail.ProviderJMAP, "m3") {
				t.Errorf("destroyed id = %s, want m3", c.ID)
			}
		}
	}
	if len(second.Changes) != 3 || updated != 1 || destroyed != 1 {
		t.Fatalf("second page changes = %+v", second.Changes)
	}
}

// A 200 response with an empty, short, or out-of-order methodResponses
// array must produce an error, never an index panic (audit JMAP-02).
func TestCallRejectsMalformedResponses(t *testing.T) {
	cases := []string{
		`{"methodResponses":[]}`,
		`{}`,
		`{"methodResponses":[["Email/get",{"list":[]},"1"]]}`,                               // wrong call entirely
		`{"methodResponses":[["error",{"type":"serverFail"},"0"]]}`,                         // error tuple
		`{"methodResponses":[["Email/get",{"list":[]},"0"],["Email/get",{"list":[]},"0"]]}`, // duplicate
	}
	for _, body := range cases {
		a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(body))
		})
		if _, err := a.call(context.Background(), [3]any{"Email/get", map[string]any{"ids": []string{"x"}}, "0"}); err == nil {
			t.Errorf("malformed response %s produced no error", body)
		}
	}
}

// A method-level 200 whose Email/set payload reports per-message failures
// is a failed mutation, not a success (audit JMAP-01).
func TestApplyReportsSetFailures(t *testing.T) {
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"methodResponses":[
			["Email/set",{"oldState":"s1","newState":"s2","updated":{"m1":null},"notUpdated":{"m2":{"type":"invalidPatch"}}},"0"]
		]}`))
	})
	err := a.Apply(context.Background(), mail.Operation{
		Kind:    mail.OpAddKeyword,
		Keyword: "seen",
		IDs:     []mail.MessageID{mail.NativeMessageID(mail.ProviderJMAP, "m1"), mail.NativeMessageID(mail.ProviderJMAP, "m2")},
	})
	if err == nil || !strings.Contains(err.Error(), "m2") {
		t.Fatalf("notUpdated was swallowed: %v", err)
	}

	a = adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"methodResponses":[
			["Email/set",{"notDestroyed":{"m9":{"type":"notFound"}}},"0"]
		]}`))
	})
	err = a.Apply(context.Background(), mail.Operation{
		Kind: mail.OpDelete,
		IDs:  []mail.MessageID{mail.NativeMessageID(mail.ProviderJMAP, "m9")},
	})
	if err == nil || !strings.Contains(err.Error(), "m9") {
		t.Fatalf("notDestroyed was swallowed: %v", err)
	}
}

// Body values the provider flags as truncated must not be cached as
// complete content (audit JMAP-04).
func TestBodyRefusesTruncatedBodyValues(t *testing.T) {
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"methodResponses":[
			["Email/get",{"list":[{
				"id":"m1",
				"bodyValues":{"p1":{"value":"half a mes","isTruncated":true}},
				"textBody":[{"partId":"p1","type":"text/plain"}],
				"attachments":[]
			}]},"0"]
		]}`))
	})
	if _, err := a.Body(context.Background(), mail.NativeMessageID(mail.ProviderJMAP, "m1")); err == nil {
		t.Fatal("a truncated body value was accepted as complete")
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

// A changed queryState mid-enumeration means the positional contract is
// broken: unchanged objects were skipped, and Email/changes cannot report
// them. The safe answer is a typed cursor reset — never a prune around
// the hole (audit 5 SYNC-04).
func TestQueryStateShiftInvalidatesTheEnumeration(t *testing.T) {
	request := 0
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		request++
		switch request {
		case 1:
			_, _ = w.Write([]byte(`{"methodResponses":[
				["Email/get",{"state":"s0","list":[]},"b"],
				["Email/query",{"ids":["m1"],"position":0,"total":2,"queryState":"q1"},"q"],
				["Email/get",{"state":"s1","list":[{"id":"m1","threadId":"t","mailboxIds":{"box":true},"keywords":{}}]},"g"]
			]}`))
		case 2:
			// The result set shifted under the pagination.
			_, _ = w.Write([]byte(`{"methodResponses":[
				["Email/query",{"ids":["m2"],"position":1,"total":2,"queryState":"q2"},"q"],
				["Email/get",{"state":"s2","list":[]},"g"]
			]}`))
		}
	})
	first, err := a.Sync(context.Background(), "box", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Sync(context.Background(), "box", first.Next)
	if err == nil || !errors.Is(err, mail.ErrCursorInvalid) {
		t.Fatalf("err = %v, want ErrCursorInvalid on a shifted query state", err)
	}
}

// A server answering the wrong position echo is unusable for positional
// pagination (audit 5 SYNC-04).
func TestQueryPositionEchoMismatchIsCursorInvalid(t *testing.T) {
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"methodResponses":[
			["Email/query",{"ids":["m9"],"position":7,"total":9,"queryState":"q1"},"q"],
			["Email/get",{"state":"s1","list":[]},"g"]
		]}`))
	})
	if _, err := a.Sync(context.Background(), "box", encodeInitialCursor(3, "s0", "q1")); err == nil || !errors.Is(err, mail.ErrCursorInvalid) {
		t.Fatalf("err = %v, want ErrCursorInvalid on a position echo mismatch", err)
	}
}

// The terminal catch-up drains hasMoreChanges pages: a window busier than
// one 500-change page must not have its tail silently adopted past
// (audit 5 SYNC-04).
func TestTerminalCatchUpDrainsHasMoreChanges(t *testing.T) {
	request := 0
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		request++
		switch request {
		case 1:
			_, _ = w.Write([]byte(`{"methodResponses":[
				["Email/get",{"state":"s0","list":[]},"b"],
				["Email/query",{"ids":["m1"],"position":0,"total":1,"queryState":"q1"},"q"],
				["Email/get",{"state":"s1","list":[{"id":"m1","threadId":"t","mailboxIds":{"box":true},"keywords":{}}]},"g"]
			]}`))
		case 2:
			_, _ = w.Write([]byte(`{"methodResponses":[
				["Email/changes",{"newState":"s5","hasMoreChanges":true,"created":["m2"],"updated":[],"destroyed":[]},"0"]
			]}`))
		case 3:
			// Envelope refetch for m2.
			_, _ = w.Write([]byte(`{"methodResponses":[
				["Email/get",{"state":"s5","list":[{"id":"m2","threadId":"t","mailboxIds":{"box":true},"keywords":{}}]},"0"]
			]}`))
		case 4:
			_, _ = w.Write([]byte(`{"methodResponses":[
				["Email/changes",{"newState":"s9","hasMoreChanges":false,"created":[],"updated":[],"destroyed":["m1"]},"0"]
			]}`))
		}
	})
	changes, err := a.Sync(context.Background(), "box", "")
	if err != nil {
		t.Fatal(err)
	}
	if !changes.Complete || changes.Next != "s9" {
		t.Fatalf("terminal page = complete=%v next=%q; want drained completion at s9", changes.Complete, changes.Next)
	}
	var created, destroyed int
	for _, c := range changes.Changes {
		switch c.Kind {
		case mail.ChangeCreated:
			created++
		case mail.ChangeDestroyed:
			destroyed++
		}
	}
	if created != 2 || destroyed != 1 {
		t.Fatalf("changes = created %d destroyed %d; want both pages' evidence (2 created, 1 destroyed)", created, destroyed)
	}
}

// A referenced body part with NO bodyValues entry is as incomplete as a
// truncated one — never cached as complete content (audit 5 JMAP-01). A
// message with no parts at all is legitimately empty.
func TestBodyRefusesMissingBodyValues(t *testing.T) {
	a := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"methodResponses":[
			["Email/get",{"list":[{
				"id":"m1",
				"textBody":[{"partId":"p1","type":"text/plain"}],
				"htmlBody":[{"partId":"p2","type":"text/html"}],
				"bodyValues":{}
			}]},"0"]
		]}`))
	})
	if _, err := a.Body(context.Background(), mail.NativeMessageID(mail.ProviderJMAP, "m1")); err == nil || !errors.Is(err, errIncompleteBody) {
		t.Fatalf("err = %v, want errIncompleteBody for missing bodyValues", err)
	}

	empty := adapterFor(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"methodResponses":[["Email/get",{"list":[{"id":"m1","textBody":[],"htmlBody":[],"bodyValues":{}}]},"0"]]}`))
	})
	body, err := empty.Body(context.Background(), mail.NativeMessageID(mail.ProviderJMAP, "m1"))
	if err != nil || body.Text != "" || body.HTML != "" {
		t.Fatalf("genuinely empty body = %+v, %v; want an empty success", body, err)
	}
}

// Credential-bearing JMAP endpoints must be HTTPS (loopback HTTP for
// local fakes), with no userinfo/query/fragment — a poisoned session
// document must not redirect the bearer token (audit 5 JMAP-02).
func TestCredentialEndpointPolicy(t *testing.T) {
	good := []string{"https://api.example.com/jmap", "http://127.0.0.1:9000/api"}
	bad := []string{
		"http://api.example.com/jmap",
		"https://user:pw@api.example.com/jmap",
		"https://api.example.com/jmap#frag",
		"https://api.example.com/jmap?x=1",
		"not-a-url",
	}
	for _, raw := range good {
		if _, err := credentialEndpoint(raw, "apiUrl"); err != nil {
			t.Errorf("%s rejected: %v", raw, err)
		}
	}
	for _, raw := range bad {
		if _, err := credentialEndpoint(raw, "apiUrl"); err == nil {
			t.Errorf("%s accepted", raw)
		}
	}
}

// Dial refuses a session whose discovered apiUrl or downloadUrl would
// carry the token over plain HTTP to a remote host.
func TestDialRejectsInsecureDiscoveredEndpoints(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"apiUrl":"http://evil.example.com/api",
			"downloadUrl":"https://ok.example.com/dl/{accountId}/{blobId}/{name}",
			"primaryAccounts":{"urn:ietf:params:jmap:mail":"acct"}
		}`))
	}))
	t.Cleanup(srv.Close)
	if _, err := Dial(context.Background(), Config{SessionURL: srv.URL, Token: "t"}); err == nil {
		t.Fatal("an HTTP discovered apiUrl was accepted")
	}
}
