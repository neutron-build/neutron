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
// Run it against a disposable IMAP server:
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

	subject := fmt.Sprintf("flag test %d", time.Now().UnixNano())
	deliver(t, env, subject, fmt.Sprintf("<flag-%d@test.local>", time.Now().UnixNano()), "body")

	before := providerState(t, ad, "INBOX")
	var target mail.MessageID
	for id, kw := range before {
		if !kw.Flagged {
			target = id
			break
		}
	}
	if target == "" {
		t.Skip("no unflagged message available to flag")
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
	// GreenMail advertises neither QRESYNC nor CONDSTORE, which makes this
	// the complete-listing sweep path — the one most IMAP servers use and
	// the only way a deletion is observed there.
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
