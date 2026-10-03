package imap

import (
	"context"
	"fmt"
	"net/smtp"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/mail"
)

// The differential oracle: sync a mailbox, change it from outside, resync,
// and compare what the mirror holds against what the server holds.
//
// Everything else in this package tests parsing in isolation. This is the
// only test that proves the adapter reads a real server correctly — that its
// FETCH round trip, its identity derivation, and its deletion detection agree
// with a server that was never told what the client expected.
//
// Run it against the Dovecot fixture (mail/testdata/dovecot), which also
// seeds the mailbox the Seeded tests below assert on:
//
//	mail/scripts/live-imap.sh up
//	eval "$(mail/scripts/live-imap.sh env)"
//	go test -race ./imap/...
//
// or against any other disposable IMAP server, for example GreenMail (the
// Seeded tests then skip, since they need the fixture's known contents):
//
//	docker run -d -p 13143:3143 -p 13025:3025 \
//	  -e GREENMAIL_OPTS='-Dgreenmail.setup.test.all -Dgreenmail.hostname=0.0.0.0 -Dgreenmail.auth.disabled' \
//	  greenmail/standalone:latest
//
//	NEUTRON_MAIL_TEST_IMAP=127.0.0.1:13143 \
//	NEUTRON_MAIL_TEST_SMTP=127.0.0.1:13025 go test ./imap/...
type liveEnv struct {
	imapHost string
	imapPort int
	smtpAddr string
	user     string
	pass     string
}

func liveConfig(t *testing.T) liveEnv {
	t.Helper()

	addr := os.Getenv("NEUTRON_MAIL_TEST_IMAP")
	if addr == "" {
		t.Skip("set NEUTRON_MAIL_TEST_IMAP=host:port to run the live IMAP oracle")
	}
	host, portStr, found := strings.Cut(addr, ":")
	if !found {
		t.Fatalf("NEUTRON_MAIL_TEST_IMAP=%q is not host:port", addr)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("bad port in %q: %v", addr, err)
	}

	return liveEnv{
		imapHost: host,
		imapPort: port,
		smtpAddr: os.Getenv("NEUTRON_MAIL_TEST_SMTP"),
		user:     "oracle@test.local",
		pass:     "oraclepass",
	}
}

func dialLive(t *testing.T, env liveEnv) *Adapter {
	t.Helper()
	conn, err := Dial(context.Background(), Config{
		Host:      env.imapHost,
		Port:      env.imapPort,
		Username:  env.user,
		Password:  env.pass,
		Plaintext: true,
		Timeout:   15 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return New(conn)
}

// deliver injects a message out of band, over SMTP, so the adapter learns
// about it the way it would in production rather than by being told.
func deliver(t *testing.T, env liveEnv, subject, messageID, body string) {
	t.Helper()
	if env.smtpAddr == "" {
		t.Skip("set NEUTRON_MAIL_TEST_SMTP=host:port to deliver test mail")
	}

	msg := fmt.Sprintf(
		"From: sender@test.local\r\nTo: %s\r\nSubject: %s\r\nMessage-ID: %s\r\nDate: %s\r\n\r\n%s\r\n",
		env.user, subject, messageID, time.Now().Format(time.RFC1123Z), body)

	err := smtp.SendMail(env.smtpAddr, nil, "sender@test.local", []string{env.user}, []byte(msg))
	if err != nil {
		t.Fatalf("deliver %s: %v", subject, err)
	}
	// Delivery is asynchronous; give the server a moment to file it.
	time.Sleep(300 * time.Millisecond)
}

// providerState reads what the server holds, independently of the mirror.
func providerState(t *testing.T, ad *Adapter, box mail.MailboxID) map[mail.MessageID]mail.Keywords {
	t.Helper()
	changes, err := ad.Sync(context.Background(), box, "")
	if err != nil {
		t.Fatalf("provider read: %v", err)
	}
	out := map[mail.MessageID]mail.Keywords{}
	for _, c := range changes.Changes {
		if c.Envelope != nil {
			out[c.ID] = c.Envelope.Keywords
		}
	}
	return out
}

func ids(m map[mail.MessageID]mail.Keywords) []string {
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, string(id))
	}
	sort.Strings(out)
	return out
}

func TestLiveInitialSyncMatchesTheServer(t *testing.T) {
	env := liveConfig(t)
	ad := dialLive(t, env)

	stamp := time.Now().UnixNano()
	deliver(t, env, "oracle one", fmt.Sprintf("<o1-%d@test.local>", stamp), "first")
	deliver(t, env, "oracle two", fmt.Sprintf("<o2-%d@test.local>", stamp), "second")

	changes, err := ad.Sync(context.Background(), "INBOX", "")
	if err != nil {
		t.Fatalf("sync: %v", err)
	}

	if len(changes.Changes) < 2 {
		t.Fatalf("initial sync returned %d messages, want at least the 2 delivered", len(changes.Changes))
	}
	if !changes.Complete {
		t.Error("an initial enumeration was not marked complete; deletions would never be detected")
	}
	if changes.Next == "" {
		t.Error("initial sync produced no cursor to resume from")
	}

	// Identity must come from the Message-ID header, not the UID, or the
	// next UIDVALIDITY change invalidates everything.
	for _, c := range changes.Changes {
		if mail.IsPositional(c.ID) {
			t.Errorf("message %s got a positional identity despite carrying a Message-ID header", c.ID)
		}
		if c.Envelope == nil {
			t.Errorf("message %s arrived without an envelope", c.ID)
			continue
		}
		if c.Envelope.Subject == "" {
			t.Errorf("message %s has no subject; ENVELOPE parsing failed", c.ID)
		}
		if len(c.Envelope.From) == 0 {
			t.Errorf("message %s has no sender; address parsing failed", c.ID)
		}
	}
}

func TestLiveIdentitySurvivesAResync(t *testing.T) {
	// The core claim: the same message keeps the same identity across
	// independent reads. If this fails, every resync duplicates the mailbox.
	env := liveConfig(t)
	ad := dialLive(t, env)

	deliver(t, env, "stable identity", fmt.Sprintf("<stable-%d@test.local>", time.Now().UnixNano()), "body")

	first := providerState(t, ad, "INBOX")
	second := providerState(t, ad, "INBOX")

	a, b := ids(first), ids(second)
	if len(a) != len(b) {
		t.Fatalf("two reads disagreed on message count: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("identity churned between reads: %s vs %s", a[i], b[i])
		}
	}
}

func TestLiveFlagChangeIsObserved(t *testing.T) {
	env := liveConfig(t)
	ad := dialLive(t, env)

	// Flag the message this test delivered, never "some unflagged message":
	// the mailbox may hold other tests' (or the fixture's) mail, and
	// changing it would leak state between tests.
	stamp := time.Now().UnixNano()
	msgID := fmt.Sprintf("<flag-%d@test.local>", stamp)
	deliver(t, env, fmt.Sprintf("flag test %d", stamp), msgID, "body")

	before := providerState(t, ad, "INBOX")
	target := mail.HeaderMessageID(msgID)
	kwBefore, ok := before[target]
	if !ok {
		t.Fatalf("delivered message %s was not synced; got %v", target, ids(before))
	}
	if kwBefore.Flagged {
		t.Fatalf("a freshly delivered message %s arrived already flagged", target)
	}

	if err := ad.Apply(context.Background(), mail.Operation{
		Kind: mail.OpAddKeyword, IDs: []mail.MessageID{target}, Keyword: "flagged",
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	after := providerState(t, ad, "INBOX")
	kw, ok := after[target]
	if !ok {
		t.Fatalf("message %s vanished after a flag change", target)
	}
	if !kw.Flagged {
		t.Error("the server did not report the flag the adapter set")
	}
	if len(after) != len(before) {
		t.Errorf("message count changed from %d to %d across a flag change", len(before), len(after))
	}
}

func TestLiveDeletionIsDetected(t *testing.T) {
	// Every read here starts from an empty cursor, so this exercises the
	// complete-listing path — the one servers without QRESYNC (GreenMail)
	// depend on for deletions. The VANISHED path on a QRESYNC server is
	// TestLiveIncrementalSyncReportsChangesAndDeletions.
	env := liveConfig(t)
	ad := dialLive(t, env)

	msgID := fmt.Sprintf("<doomed-%d@test.local>", time.Now().UnixNano())
	deliver(t, env, "doomed message", msgID, "body")

	before := providerState(t, ad, "INBOX")
	target := mail.HeaderMessageID(msgID)
	if _, ok := before[target]; !ok {
		t.Fatalf("delivered message %s was not synced; got %v", target, ids(before))
	}

	if err := ad.Apply(context.Background(), mail.Operation{
		Kind: mail.OpDelete, IDs: []mail.MessageID{target},
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	after := providerState(t, ad, "INBOX")
	if _, ok := after[target]; ok {
		t.Error("a deleted message was still reported by the server read")
	}
	if len(after) >= len(before) {
		t.Errorf("message count did not drop after a delete: %d then %d", len(before), len(after))
	}
}

func TestLiveIncrementalSyncReportsChangesAndDeletions(t *testing.T) {
	// The QRESYNC path: resume from a cursor, let a different connection
	// create, flag and expunge messages, and require the resumed sync to
	// report exactly those — deletions via VANISHED rather than a sweep.
	env := liveConfig(t)
	ad := dialLive(t, env)
	if !ad.conn.Supports("QRESYNC") || !ad.conn.Supports("CONDSTORE") {
		t.Skip("server does not advertise QRESYNC and CONDSTORE")
	}

	stamp := time.Now().UnixNano()
	doomedID := fmt.Sprintf("<inc-doomed-%d@test.local>", stamp)
	flaggedID := fmt.Sprintf("<inc-flagged-%d@test.local>", stamp)
	deliver(t, env, "incremental doomed", doomedID, "body")
	deliver(t, env, "incremental flagged", flaggedID, "body")

	first, err := ad.Sync(context.Background(), "INBOX", "")
	if err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	doomed, flagged := mail.HeaderMessageID(doomedID), mail.HeaderMessageID(flaggedID)

	// A second client changes the mailbox behind the first one's back.
	other := dialLive(t, env)
	if _, err := other.Sync(context.Background(), "INBOX", ""); err != nil {
		t.Fatalf("second client sync: %v", err)
	}
	freshID := fmt.Sprintf("<inc-fresh-%d@test.local>", stamp)
	deliver(t, env, "incremental fresh", freshID, "body")
	if err := other.Apply(context.Background(), mail.Operation{
		Kind: mail.OpAddKeyword, IDs: []mail.MessageID{flagged}, Keyword: "flagged",
	}); err != nil {
		t.Fatalf("flag: %v", err)
	}
	if err := other.Apply(context.Background(), mail.Operation{
		Kind: mail.OpDelete, IDs: []mail.MessageID{doomed},
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	second, err := ad.Sync(context.Background(), "INBOX", first.Next)
	if err != nil {
		t.Fatalf("incremental sync: %v", err)
	}
	if second.Reset {
		t.Fatal("an unchanged UIDVALIDITY produced a reset")
	}
	if second.EnumerationStart || second.Complete {
		t.Error("an incremental sync claimed to be a full enumeration; the engine would sweep on it")
	}

	got := map[mail.MessageID]mail.ChangeKind{}
	for _, c := range second.Changes {
		got[c.ID] = c.Kind
		if c.ID == flagged && (c.Envelope == nil || !c.Envelope.Keywords.Flagged) {
			t.Errorf("update for %s does not carry the new flag", c.ID)
		}
	}
	if k := got[doomed]; k != mail.ChangeDestroyed {
		t.Errorf("expunged message %s reported as kind %d, want %d (ChangeDestroyed) (changes: %v)", doomed, k, mail.ChangeDestroyed, got)
	}
	if k := got[flagged]; k != mail.ChangeUpdated {
		t.Errorf("flagged message %s reported as kind %d, want %d (ChangeUpdated) (changes: %v)", flagged, k, mail.ChangeUpdated, got)
	}
	fresh := mail.HeaderMessageID(freshID)
	if _, ok := got[fresh]; !ok {
		t.Errorf("new message %s missing from the incremental sync (changes: %v)", fresh, got)
	}
	for id, k := range got {
		if id != doomed && id != flagged && id != fresh {
			t.Errorf("unrelated message %s reported as kind %d", id, k)
		}
	}
}

func TestLiveMailboxListingHasRoles(t *testing.T) {
	env := liveConfig(t)
	ad := dialLive(t, env)

	boxes, err := ad.Mailboxes(context.Background())
	if err != nil {
		t.Fatalf("mailboxes: %v", err)
	}
	if len(boxes) == 0 {
		t.Fatal("no mailboxes returned")
	}

	var foundInbox bool
	for _, b := range boxes {
		if b.Role == mail.RoleInbox {
			foundInbox = true
		}
		if b.Native == "" {
			t.Errorf("mailbox %s has no native handle; commands against it would fail", b.ID)
		}
	}
	if !foundInbox {
		t.Error("INBOX was not recognised; it is the one name the protocol reserves")
	}
}

func TestLiveBodyFetchDoesNotMarkSeen(t *testing.T) {
	// Reading a message in the mirror must not change its state at the
	// provider. That is why every fetch uses BODY.PEEK.
	env := liveConfig(t)
	ad := dialLive(t, env)

	msgID := fmt.Sprintf("<peek-%d@test.local>", time.Now().UnixNano())
	deliver(t, env, "peek test", msgID, "the body text")

	target := mail.HeaderMessageID(msgID)
	before := providerState(t, ad, "INBOX")
	kwBefore, ok := before[target]
	if !ok {
		t.Fatalf("delivered message not found; got %v", ids(before))
	}
	if kwBefore.Seen {
		t.Skip("message already seen; cannot test peek semantics")
	}

	body, err := ad.Body(context.Background(), target)
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	if !strings.Contains(body.Text, "the body text") {
		t.Errorf("body = %q, want it to contain the delivered text", body.Text)
	}

	after := providerState(t, ad, "INBOX")
	if after[target].Seen {
		t.Error("fetching a body marked the message seen at the provider")
	}
}

// The Seeded tests assert on the fixed mailbox mail/testdata/dovecot/seed.sh
// builds. NEUTRON_MAIL_TEST_IMAP_SEEDED=1 (printed by live-imap.sh env) says
// the server is that fixture; against any other server they skip.
func requireSeeded(t *testing.T) {
	t.Helper()
	if os.Getenv("NEUTRON_MAIL_TEST_IMAP_SEEDED") != "1" {
		t.Skip("set NEUTRON_MAIL_TEST_IMAP_SEEDED=1 against the Dovecot fixture to assert on its seeded mailbox")
	}
}

func TestLiveSeededSpecialUseRoles(t *testing.T) {
	// Roles must come from the server's RFC 6154 SPECIAL-USE attributes,
	// not from guessing at folder names: the fixture's folders carry
	// attributes and the names are conventional, so a name-based guess and a
	// correct one agree here — the test pins that the attribute path works
	// and that nothing is mislabelled.
	requireSeeded(t)
	ad := dialLive(t, liveConfig(t))

	boxes, err := ad.Mailboxes(context.Background())
	if err != nil {
		t.Fatalf("mailboxes: %v", err)
	}
	got := map[string]mail.Role{}
	for _, b := range boxes {
		got[b.Name] = b.Role
	}
	want := map[string]mail.Role{
		"INBOX":   mail.RoleInbox,
		"Drafts":  mail.RoleDrafts,
		"Sent":    mail.RoleSent,
		"Trash":   mail.RoleTrash,
		"Junk":    mail.RoleJunk,
		"Archive": mail.RoleArchive,
	}
	for name, role := range want {
		if got[name] != role {
			t.Errorf("mailbox %q has role %q, want %q (all: %v)", name, got[name], role, got)
		}
	}
}

func TestLiveSeededKeywordsAreMirrored(t *testing.T) {
	requireSeeded(t)
	ad := dialLive(t, liveConfig(t))

	state := providerState(t, ad, "INBOX")
	// seed.sh INBOX, in order: unread x3, read, flagged unread, flagged read.
	want := map[int]mail.Keywords{
		1: {},
		2: {},
		3: {},
		4: {Seen: true},
		5: {Flagged: true},
		6: {Seen: true, Flagged: true},
	}
	for n, kw := range want {
		id := mail.HeaderMessageID(fmt.Sprintf("<seed-%d@test.local>", n))
		got, ok := state[id]
		if !ok {
			t.Errorf("seed message %d (%s) missing from INBOX; have %v", n, id, ids(state))
			continue
		}
		if got.Seen != kw.Seen || got.Flagged != kw.Flagged || got.Draft || got.Answered {
			t.Errorf("seed message %d keywords = %+v, want seen=%v flagged=%v and nothing else", n, got, kw.Seen, kw.Flagged)
		}
	}
}

func TestLiveSeededDraftsAndTrashHoldTheirMessages(t *testing.T) {
	requireSeeded(t)
	ad := dialLive(t, liveConfig(t))

	boxes, err := ad.Mailboxes(context.Background())
	if err != nil {
		t.Fatalf("mailboxes: %v", err)
	}
	byRole := map[mail.Role]mail.MailboxID{}
	for _, b := range boxes {
		byRole[b.Role] = b.ID
	}

	// seed.sh numbers its messages 7 (Drafts), 8 (Sent), 9 (Trash).
	cases := []struct {
		role  mail.Role
		n     int
		draft bool
	}{
		{mail.RoleDrafts, 7, true},
		{mail.RoleSent, 8, false},
		{mail.RoleTrash, 9, false},
	}
	for _, c := range cases {
		box, ok := byRole[c.role]
		if !ok {
			t.Errorf("no mailbox with role %q", c.role)
			continue
		}
		state := providerState(t, ad, box)
		id := mail.HeaderMessageID(fmt.Sprintf("<seed-%d@test.local>", c.n))
		kw, found := state[id]
		if !found {
			t.Errorf("%s: seed message %d missing; have %v", c.role, c.n, ids(state))
			continue
		}
		if kw.Draft != c.draft {
			t.Errorf("%s: seed message %d draft = %v, want %v", c.role, c.n, kw.Draft, c.draft)
		}
		if !kw.Seen {
			t.Errorf("%s: seed message %d should be seen", c.role, c.n)
		}
	}
}
