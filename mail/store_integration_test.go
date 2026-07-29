package mail

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// These tests exercise PgStore — the only implementation the service ever
// runs — against a live engine. The engine tests in sync_test.go use a
// double, and a double is exactly what cannot tell you whether the SQL is
// right. Everything below runs against real Nucleus or not at all.
//
// Point NEUTRON_MAIL_TEST_DATABASE_URL at a scratch database:
//
//	NEUTRON_MAIL_TEST_DATABASE_URL=postgres://user:pass@localhost:5432/mailtest go test ./...
//
// The tables are dropped and recreated on every run, so never aim this at
// anything you care about.
func testStore(t *testing.T) *PgStore {
	t.Helper()

	url := os.Getenv("NEUTRON_MAIL_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set NEUTRON_MAIL_TEST_DATABASE_URL to run store integration tests")
	}

	ctx := context.Background()
	store, err := Open(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(store.Close)

	if err := store.Drop(ctx); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return store
}

func seedAccount(t *testing.T, s *PgStore) AccountID {
	t.Helper()
	acct := AccountID("it-acct")
	err := s.PutAccount(context.Background(), &Account{
		ID: acct, Provider: ProviderIMAP, Email: "it@example.com", Name: "Integration",
	})
	if err != nil {
		t.Fatalf("put account: %v", err)
	}
	return acct
}

func TestIntegrationEnvelopeRoundTrip(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	sent := time.Date(2026, 7, 28, 9, 30, 0, 0, time.UTC)
	original := Envelope{
		ID:              HeaderMessageID("<roundtrip@example.com>"),
		ThreadID:        "thread-1",
		MailboxIDs:      []MailboxID{"INBOX", "Archive"},
		From:            []Address{{Name: "Alice", Email: "alice@example.com"}},
		To:              []Address{{Email: "bob@example.com"}, {Email: "carol@example.com"}},
		Cc:              []Address{{Email: "dave@example.com"}},
		Subject:         "Quarterly numbers",
		SentAt:          sent,
		ReceivedAt:      sent.Add(time.Minute),
		Keywords:        Keywords{Seen: true, Flagged: true, Custom: []string{"project-x"}},
		HasAttachment:   true,
		Size:            4096,
		Preview:         "Here are the figures",
		MessageIDHeader: "<roundtrip@example.com>",
		References:      []string{"root@example.com"},
	}

	if err := s.PutEnvelopes(ctx, acct, []Envelope{original}); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := s.Envelope(ctx, acct, original.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Subject != original.Subject {
		t.Errorf("Subject = %q, want %q", got.Subject, original.Subject)
	}
	if len(got.To) != 2 || got.To[0].Email != "bob@example.com" {
		t.Errorf("To = %+v", got.To)
	}
	if got.From[0].Name != "Alice" {
		t.Errorf("From name = %q, want Alice", got.From[0].Name)
	}
	if !got.Keywords.Seen || !got.Keywords.Flagged {
		t.Errorf("Keywords = %+v", got.Keywords)
	}
	if len(got.Keywords.Custom) != 1 || got.Keywords.Custom[0] != "project-x" {
		t.Errorf("custom keywords = %v", got.Keywords.Custom)
	}
	if len(got.MailboxIDs) != 2 {
		t.Errorf("MailboxIDs = %v, want two", got.MailboxIDs)
	}
	if !got.SentAt.Equal(sent) {
		t.Errorf("SentAt = %v, want %v", got.SentAt, sent)
	}
	if got.Fingerprint == "" {
		t.Error("fingerprint was not persisted")
	}
}

func TestIntegrationUpsertReplacesMailboxMembership(t *testing.T) {
	// A moved message must not keep its old membership row, or it stays
	// visible in a folder it has left.
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	id := HeaderMessageID("<moved@example.com>")
	first := Envelope{ID: id, MailboxIDs: []MailboxID{"INBOX"}, Subject: "before"}
	if err := s.PutEnvelopes(ctx, acct, []Envelope{first}); err != nil {
		t.Fatal(err)
	}

	second := Envelope{ID: id, MailboxIDs: []MailboxID{"Archive"}, Subject: "after"}
	if err := s.PutEnvelopes(ctx, acct, []Envelope{second}); err != nil {
		t.Fatal(err)
	}

	got, err := s.Envelope(ctx, acct, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.MailboxIDs) != 1 || got.MailboxIDs[0] != "Archive" {
		t.Errorf("MailboxIDs = %v, want [Archive] only", got.MailboxIDs)
	}
	if got.Subject != "after" {
		t.Errorf("Subject = %q, want the updated value", got.Subject)
	}
}

func TestIntegrationRemoveFromLastMailboxDeletes(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	id := HeaderMessageID("<two-boxes@example.com>")
	env := Envelope{ID: id, MailboxIDs: []MailboxID{"INBOX", "Archive"}}
	if err := s.PutEnvelopes(ctx, acct, []Envelope{env}); err != nil {
		t.Fatal(err)
	}

	// Leaving one of two mailboxes keeps the message.
	if err := s.RemoveFromMailbox(ctx, acct, "INBOX", []MessageID{id}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Envelope(ctx, acct, id); err != nil {
		t.Fatalf("message deleted while still filed in Archive: %v", err)
	}

	// Leaving the last one deletes it.
	if err := s.RemoveFromMailbox(ctx, acct, "Archive", []MessageID{id}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Envelope(ctx, acct, id); !errors.Is(err, ErrNoStore) {
		t.Error("message survived leaving its last mailbox")
	}
}

func TestIntegrationCursorRoundTrip(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	// An unsynced mailbox has no cursor, and that is not an error.
	cur, err := s.Cursor(ctx, acct, "INBOX")
	if err != nil {
		t.Fatalf("cursor on an unsynced mailbox errored: %v", err)
	}
	if cur != "" {
		t.Errorf("cursor = %q, want empty", cur)
	}

	// Cursors are opaque and may contain JSON, which must survive the round
	// trip byte for byte.
	opaque := Cursor(`{"uidvalidity":123,"modseq":90060128194045007}`)
	if err := s.PutCursor(ctx, acct, "INBOX", opaque); err != nil {
		t.Fatal(err)
	}
	got, err := s.Cursor(ctx, acct, "INBOX")
	if err != nil {
		t.Fatal(err)
	}
	if got != opaque {
		t.Errorf("cursor = %q, want %q", got, opaque)
	}
}

func TestIntegrationResetMailboxClearsBothMessagesAndCursor(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	envs := []Envelope{
		{ID: HeaderMessageID("<r1@example.com>"), MailboxIDs: []MailboxID{"INBOX"}},
		{ID: HeaderMessageID("<r2@example.com>"), MailboxIDs: []MailboxID{"INBOX"}},
	}
	if err := s.PutEnvelopes(ctx, acct, envs); err != nil {
		t.Fatal(err)
	}
	if err := s.PutCursor(ctx, acct, "INBOX", "some-cursor"); err != nil {
		t.Fatal(err)
	}

	if err := s.ResetMailbox(ctx, acct, "INBOX"); err != nil {
		t.Fatal(err)
	}

	ids, err := s.EnvelopeIDs(ctx, acct, "INBOX")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Errorf("mailbox still holds %d messages after a reset", len(ids))
	}
	cur, err := s.Cursor(ctx, acct, "INBOX")
	if err != nil {
		t.Fatal(err)
	}
	if cur != "" {
		t.Errorf("cursor = %q, want it cleared so the next sync starts over", cur)
	}
}

func TestIntegrationRebuildFromZero(t *testing.T) {
	// The invariant the whole design rests on: the mirror is derived, so
	// dropping it must cost nothing but a resync. If this fails, the local
	// copy has become authoritative for something.
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	env := Envelope{
		ID:         HeaderMessageID("<rebuild@example.com>"),
		MailboxIDs: []MailboxID{"INBOX"},
		Subject:    "before the drop",
	}
	if err := s.PutEnvelopes(ctx, acct, []Envelope{env}); err != nil {
		t.Fatal(err)
	}

	if err := s.Drop(ctx); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, err := s.Account(ctx, acct); !errors.Is(err, ErrNoStore) {
		t.Error("account survived a drop; the store is holding state it cannot rebuild")
	}

	// Everything replays from the provider's data with no special casing.
	seedAccount(t, s)
	if err := s.PutEnvelopes(ctx, acct, []Envelope{env}); err != nil {
		t.Fatalf("replay after rebuild: %v", err)
	}
	got, err := s.Envelope(ctx, acct, env.ID)
	if err != nil {
		t.Fatalf("message did not come back after a rebuild: %v", err)
	}
	if got.Subject != env.Subject {
		t.Errorf("Subject = %q, want %q", got.Subject, env.Subject)
	}
}

func TestIntegrationSearchAndThread(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	envs := []Envelope{
		{
			ID: HeaderMessageID("<s1@example.com>"), ThreadID: "t1",
			MailboxIDs: []MailboxID{"INBOX"}, Subject: "Quarterly numbers",
			From: []Address{{Email: "alice@example.com"}}, ReceivedAt: time.Now().UTC(),
		},
		{
			ID: HeaderMessageID("<s2@example.com>"), ThreadID: "t1",
			MailboxIDs: []MailboxID{"INBOX"}, Subject: "Re: Quarterly numbers",
			From: []Address{{Email: "bob@example.com"}}, ReceivedAt: time.Now().UTC(),
		},
		{
			ID: HeaderMessageID("<s3@example.com>"), ThreadID: "t2",
			MailboxIDs: []MailboxID{"INBOX"}, Subject: "Lunch",
			From: []Address{{Email: "carol@example.com"}}, ReceivedAt: time.Now().UTC(),
		},
	}
	if err := s.PutEnvelopes(ctx, acct, envs); err != nil {
		t.Fatal(err)
	}

	hits, err := s.Search(ctx, acct, "Quarterly", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 {
		t.Errorf("search returned %d, want 2", len(hits))
	}

	thread, err := s.Thread(ctx, acct, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(thread) != 2 {
		t.Errorf("thread returned %d, want 2", len(thread))
	}
	for _, m := range thread {
		if len(m.MailboxIDs) == 0 {
			t.Error("thread results lost their mailbox membership")
		}
	}
}

func TestIntegrationBodyLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	id := HeaderMessageID("<body@example.com>")
	if err := s.PutEnvelopes(ctx, acct, []Envelope{{ID: id, MailboxIDs: []MailboxID{"INBOX"}}}); err != nil {
		t.Fatal(err)
	}

	// An envelope with no body yet is a cache miss, not an error condition.
	if _, err := s.Body(ctx, acct, id); !errors.Is(err, ErrNoStore) {
		t.Errorf("missing body = %v, want ErrNoStore", err)
	}

	body := &Body{
		MessageID: id,
		Text:      "plain text",
		HTML:      "<p>rich text</p>",
		Parts:     []BodyPart{{PartID: "1", Type: "application/pdf", Filename: "a.pdf", Disposition: "attachment"}},
	}
	if err := s.PutBody(ctx, acct, body); err != nil {
		t.Fatal(err)
	}

	got, err := s.Body(ctx, acct, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Text != body.Text || got.HTML != body.HTML {
		t.Errorf("body = %+v", got)
	}
	if len(got.Attachments()) != 1 {
		t.Errorf("attachments = %+v, want one", got.Attachments())
	}

	// Deleting the message takes its body with it.
	if err := s.DeleteMessages(ctx, acct, []MessageID{id}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Body(ctx, acct, id); !errors.Is(err, ErrNoStore) {
		t.Error("body outlived its message")
	}
}

func TestIntegrationMalformedIdentityIsRejected(t *testing.T) {
	// An identity that does not round-trip is indistinguishable from data
	// loss later, so the store refuses it on write.
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	err := s.PutEnvelopes(ctx, acct, []Envelope{{ID: "not-a-valid-identity"}})
	if err == nil {
		t.Fatal("store accepted a malformed message id")
	}
}
