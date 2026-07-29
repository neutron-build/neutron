package mail

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
)

// discardLogger keeps expected-failure paths from spraying the test output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// The store used here is a test double, and it validates the engine's logic
// only — never PgStore, which is the sole production implementation and is
// covered separately by store_integration_test.go against a real engine.
//
// The distinction matters: a well-tested double standing in for the path
// production actually runs is how silent breakage gets in. Everything below
// asserts engine behaviour that cannot be provoked against a real provider on
// demand — a UIDVALIDITY reset, a revoked grant, a crash between two writes.

type memStore struct {
	accounts  map[AccountID]*Account
	mailboxes map[AccountID][]Mailbox
	messages  map[AccountID]map[MessageID]*Envelope
	members   map[AccountID]map[MessageID]map[MailboxID]bool
	bodies    map[AccountID]map[MessageID]*Body
	cursors   map[AccountID]map[MailboxID]Cursor

	// failPutEnvelopes makes the next PutEnvelopes fail, to simulate a
	// crash between storing data and advancing the cursor.
	failPutEnvelopes bool
}

func newMemStore() *memStore {
	return &memStore{
		accounts:  map[AccountID]*Account{},
		mailboxes: map[AccountID][]Mailbox{},
		messages:  map[AccountID]map[MessageID]*Envelope{},
		members:   map[AccountID]map[MessageID]map[MailboxID]bool{},
		bodies:    map[AccountID]map[MessageID]*Body{},
		cursors:   map[AccountID]map[MailboxID]Cursor{},
	}
}

func (m *memStore) Migrate(context.Context) error { return nil }
func (m *memStore) Close()                        {}

func (m *memStore) PutAccount(_ context.Context, a *Account) error {
	cp := *a
	m.accounts[a.ID] = &cp
	return nil
}

func (m *memStore) Account(_ context.Context, id AccountID) (*Account, error) {
	a, ok := m.accounts[id]
	if !ok {
		return nil, ErrNoStore
	}
	cp := *a
	return &cp, nil
}

func (m *memStore) Accounts(context.Context) ([]Account, error) {
	var out []Account
	for _, a := range m.accounts {
		out = append(out, *a)
	}
	return out, nil
}

func (m *memStore) SetNeedsReauth(_ context.Context, id AccountID, needs bool) error {
	if a, ok := m.accounts[id]; ok {
		a.NeedsReauth = needs
	}
	return nil
}

func (m *memStore) PutMailboxes(_ context.Context, acct AccountID, boxes []Mailbox) error {
	m.mailboxes[acct] = boxes
	return nil
}

func (m *memStore) Mailboxes(_ context.Context, acct AccountID) ([]Mailbox, error) {
	return m.mailboxes[acct], nil
}

func (m *memStore) PutEnvelopes(_ context.Context, acct AccountID, envs []Envelope) error {
	if m.failPutEnvelopes {
		m.failPutEnvelopes = false
		return errors.New("simulated store failure")
	}
	if m.messages[acct] == nil {
		m.messages[acct] = map[MessageID]*Envelope{}
		m.members[acct] = map[MessageID]map[MailboxID]bool{}
	}
	for i := range envs {
		e := envs[i]
		if err := e.ID.Validate(); err != nil {
			return err
		}
		if e.Fingerprint == "" {
			e.Fingerprint = ComputeFingerprint(&e)
		}
		m.messages[acct][e.ID] = &e
		m.members[acct][e.ID] = map[MailboxID]bool{}
		for _, b := range e.MailboxIDs {
			m.members[acct][e.ID][b] = true
		}
	}
	return nil
}

func (m *memStore) Envelope(_ context.Context, acct AccountID, id MessageID) (*Envelope, error) {
	e, ok := m.messages[acct][id]
	if !ok {
		return nil, ErrNoStore
	}
	return e, nil
}

func (m *memStore) EnvelopeIDs(_ context.Context, acct AccountID, box MailboxID) ([]MessageID, error) {
	var out []MessageID
	for id, boxes := range m.members[acct] {
		if boxes[box] {
			out = append(out, id)
		}
	}
	return out, nil
}

func (m *memStore) DeleteMessages(_ context.Context, acct AccountID, ids []MessageID) error {
	for _, id := range ids {
		delete(m.messages[acct], id)
		delete(m.members[acct], id)
		delete(m.bodies[acct], id)
	}
	return nil
}

func (m *memStore) RemoveFromMailbox(_ context.Context, acct AccountID, box MailboxID, ids []MessageID) error {
	for _, id := range ids {
		if m.members[acct][id] == nil {
			continue
		}
		delete(m.members[acct][id], box)
		if len(m.members[acct][id]) == 0 {
			delete(m.messages[acct], id)
			delete(m.members[acct], id)
		}
	}
	return nil
}

func (m *memStore) PutBody(_ context.Context, acct AccountID, b *Body) error {
	if m.bodies[acct] == nil {
		m.bodies[acct] = map[MessageID]*Body{}
	}
	m.bodies[acct][b.MessageID] = b
	return nil
}

func (m *memStore) Body(_ context.Context, acct AccountID, id MessageID) (*Body, error) {
	b, ok := m.bodies[acct][id]
	if !ok {
		return nil, ErrNoStore
	}
	return b, nil
}

func (m *memStore) Cursor(_ context.Context, acct AccountID, box MailboxID) (Cursor, error) {
	return m.cursors[acct][box], nil
}

func (m *memStore) PutCursor(_ context.Context, acct AccountID, box MailboxID, cur Cursor) error {
	if m.cursors[acct] == nil {
		m.cursors[acct] = map[MailboxID]Cursor{}
	}
	m.cursors[acct][box] = cur
	return nil
}

func (m *memStore) ResetMailbox(ctx context.Context, acct AccountID, box MailboxID) error {
	ids, _ := m.EnvelopeIDs(ctx, acct, box)
	if m.cursors[acct] != nil {
		delete(m.cursors[acct], box)
	}
	return m.RemoveFromMailbox(ctx, acct, box, ids)
}

func (m *memStore) Search(context.Context, AccountID, string, int) ([]Envelope, error) {
	return nil, nil
}

func (m *memStore) Thread(context.Context, AccountID, ThreadID) ([]Envelope, error) {
	return nil, nil
}

func (m *memStore) count(acct AccountID) int { return len(m.messages[acct]) }

var _ Store = (*memStore)(nil)

// ---------------------------------------------------------------------------
// A scripted adapter: each Sync call returns the next queued page.
// ---------------------------------------------------------------------------

type scriptedAdapter struct {
	boxes []Mailbox
	pages []*Changes
	call  int

	envelopes  map[MessageID]Envelope
	syncErr    error
	mailboxErr error

	// seenCursors records what the engine passed in, so tests can assert
	// the cursor actually round-trips rather than being recomputed.
	seenCursors []Cursor
}

func (a *scriptedAdapter) Provider() Provider { return ProviderIMAP }

func (a *scriptedAdapter) Mailboxes(context.Context) ([]Mailbox, error) {
	if a.mailboxErr != nil {
		return nil, a.mailboxErr
	}
	return a.boxes, nil
}

func (a *scriptedAdapter) Sync(_ context.Context, _ MailboxID, cur Cursor) (*Changes, error) {
	a.seenCursors = append(a.seenCursors, cur)
	if a.syncErr != nil {
		return nil, a.syncErr
	}
	if a.call >= len(a.pages) {
		return &Changes{Next: cur}, nil
	}
	page := a.pages[a.call]
	a.call++
	return page, nil
}

func (a *scriptedAdapter) Envelopes(_ context.Context, ids []MessageID) ([]Envelope, error) {
	var out []Envelope
	for _, id := range ids {
		if e, ok := a.envelopes[id]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

func (a *scriptedAdapter) Body(_ context.Context, id MessageID) (*Body, error) {
	return &Body{MessageID: id, Text: "body of " + string(id)}, nil
}

func (a *scriptedAdapter) Raw(context.Context, MessageID) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (a *scriptedAdapter) Attachment(context.Context, MessageID, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (a *scriptedAdapter) Apply(context.Context, Operation) error { return nil }
func (a *scriptedAdapter) Close() error                           { return nil }

var _ Adapter = (*scriptedAdapter)(nil)

func envelope(id string, box MailboxID) Envelope {
	return Envelope{
		ID:              NativeMessageID(ProviderIMAP, id),
		MailboxIDs:      []MailboxID{box},
		Subject:         "subject " + id,
		MessageIDHeader: fmt.Sprintf("<%s@example.com>", id),
	}
}

func created(e Envelope) Change {
	return Change{Kind: ChangeCreated, ID: e.ID, Envelope: &e}
}

func setup(t *testing.T) (*Engine, *memStore, AccountID) {
	t.Helper()
	store := newMemStore()
	acct := AccountID("acct-1")
	if err := store.PutAccount(context.Background(), &Account{
		ID: acct, Provider: ProviderIMAP, Email: "user@example.com",
	}); err != nil {
		t.Fatal(err)
	}
	return NewEngine(store, discardLogger()), store, acct
}

// ---------------------------------------------------------------------------

func TestInitialSyncStoresEverything(t *testing.T) {
	eng, store, acct := setup(t)
	ad := &scriptedAdapter{
		boxes: []Mailbox{{ID: "INBOX", Name: "INBOX", Role: RoleInbox}},
		pages: []*Changes{{
			Changes: []Change{created(envelope("1", "INBOX")), created(envelope("2", "INBOX"))},
			Next:    "cursor-1",
		}},
	}

	rep, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Created != 2 {
		t.Errorf("Created = %d, want 2", rep.Created)
	}
	if store.count(acct) != 2 {
		t.Errorf("stored %d messages, want 2", store.count(acct))
	}
	if got, _ := store.Cursor(context.Background(), acct, "INBOX"); got != "cursor-1" {
		t.Errorf("cursor = %q, want cursor-1", got)
	}
}

func TestCursorRoundTripsToTheAdapter(t *testing.T) {
	// The cursor is opaque: the engine must hand back exactly what the
	// adapter produced, never a value it derived.
	eng, _, acct := setup(t)
	ad := &scriptedAdapter{
		pages: []*Changes{
			{Next: "opaque-A", More: true},
			{Next: "opaque-B"},
		},
	}

	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	want := []Cursor{"", "opaque-A"}
	if len(ad.seenCursors) != len(want) {
		t.Fatalf("adapter saw %v, want %v", ad.seenCursors, want)
	}
	for i := range want {
		if ad.seenCursors[i] != want[i] {
			t.Errorf("cursor %d = %q, want %q", i, ad.seenCursors[i], want[i])
		}
	}
}

func TestProviderResetRefetchesWithoutDuplicating(t *testing.T) {
	// A UIDVALIDITY change, a Gmail historyId aged out, an expired Graph
	// delta token: all four providers arrive here.
	eng, store, acct := setup(t)
	ad := &scriptedAdapter{
		pages: []*Changes{
			{Changes: []Change{created(envelope("1", "INBOX")), created(envelope("2", "INBOX"))}, Next: "c1"},
		},
	}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if store.count(acct) != 2 {
		t.Fatalf("setup stored %d, want 2", store.count(acct))
	}

	// The provider now rejects the cursor and offers a fresh start.
	ad.call = 0
	ad.pages = []*Changes{
		{Reset: true, Next: ""},
		{Changes: []Change{created(envelope("1", "INBOX")), created(envelope("3", "INBOX"))}, Next: "c2"},
	}

	rep, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Reset {
		t.Error("report did not record the reset")
	}
	if store.count(acct) != 2 {
		t.Errorf("after reset stored %d messages, want 2 (message 2 is gone, 3 is new)", store.count(acct))
	}
	if _, err := store.Envelope(context.Background(), acct, NativeMessageID(ProviderIMAP, "2")); !errors.Is(err, ErrNoStore) {
		t.Error("message 2 vanished at the provider but survived the reset")
	}
	if _, err := store.Envelope(context.Background(), acct, NativeMessageID(ProviderIMAP, "3")); err != nil {
		t.Error("message 3 was not picked up after the reset")
	}
}

func TestRepeatedResetIsRefusedRatherThanLooping(t *testing.T) {
	// A provider that always rejects its own cursor would otherwise spin
	// forever, refetching the whole mailbox on each pass.
	eng, _, acct := setup(t)
	ad := &scriptedAdapter{
		pages: []*Changes{
			{Reset: true, Next: ""},
			{Reset: true, Next: ""},
		},
	}

	_, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad)
	if err == nil {
		t.Fatal("expected an error on a second reset in one sync")
	}
	if !strings.Contains(err.Error(), "reset twice") {
		t.Errorf("error = %v, want it to name the repeated reset", err)
	}
}

func TestCursorIsNotAdvancedWhenStoringFails(t *testing.T) {
	// Writing the cursor before the data would lose messages on a crash
	// between the two: the next sync would resume past data never written.
	eng, store, acct := setup(t)
	store.failPutEnvelopes = true
	ad := &scriptedAdapter{
		pages: []*Changes{{Changes: []Change{created(envelope("1", "INBOX"))}, Next: "advanced"}},
	}

	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err == nil {
		t.Fatal("expected the store failure to surface")
	}
	if got, _ := store.Cursor(context.Background(), acct, "INBOX"); got != "" {
		t.Errorf("cursor advanced to %q despite the write failing", got)
	}
}

func TestPositionalIdentityIsUpgradedOnIngest(t *testing.T) {
	eng, store, acct := setup(t)

	pos := PositionalMessageID("INBOX", 1, 7)
	env := Envelope{
		ID:              pos,
		MailboxIDs:      []MailboxID{"INBOX"},
		MessageIDHeader: "<real@example.com>",
	}
	ad := &scriptedAdapter{
		pages: []*Changes{{Changes: []Change{{Kind: ChangeCreated, ID: pos, Envelope: &env}}, Next: "c1"}},
	}

	rep, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Upgraded != 1 {
		t.Errorf("Upgraded = %d, want 1", rep.Upgraded)
	}
	if _, err := store.Envelope(context.Background(), acct, pos); !errors.Is(err, ErrNoStore) {
		t.Error("superseded positional row survived the upgrade")
	}
	if _, err := store.Envelope(context.Background(), acct, HeaderMessageID("<real@example.com>")); err != nil {
		t.Error("upgraded identity was not stored")
	}
}

func TestMessageInTwoMailboxesSurvivesRemovalFromOne(t *testing.T) {
	// Gmail labels and JMAP mailboxes are many-to-many. Removing a message
	// from one must not delete a copy that is still filed elsewhere.
	eng, store, acct := setup(t)

	e := envelope("1", "INBOX")
	e.MailboxIDs = []MailboxID{"INBOX", "Archive"}
	ad := &scriptedAdapter{
		pages: []*Changes{{Changes: []Change{created(e)}, Next: "c1"}},
	}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}

	ad.call = 0
	ad.pages = []*Changes{{Changes: []Change{{Kind: ChangeDestroyed, ID: e.ID}}, Next: "c2"}}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Envelope(context.Background(), acct, e.ID); err != nil {
		t.Error("message was deleted despite still being filed in Archive")
	}
}

func TestMessageIsDeletedWhenItLeavesItsLastMailbox(t *testing.T) {
	eng, store, acct := setup(t)
	e := envelope("1", "INBOX")
	ad := &scriptedAdapter{
		pages: []*Changes{{Changes: []Change{created(e)}, Next: "c1"}},
	}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}

	ad.call = 0
	ad.pages = []*Changes{{Changes: []Change{{Kind: ChangeDestroyed, ID: e.ID}}, Next: "c2"}}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Envelope(context.Background(), acct, e.ID); !errors.Is(err, ErrNoStore) {
		t.Error("message survived leaving its last mailbox")
	}
}

func TestBareIDDeltasAreBackfilled(t *testing.T) {
	// Gmail's history API returns message IDs without envelopes.
	eng, store, acct := setup(t)
	e := envelope("1", "INBOX")
	ad := &scriptedAdapter{
		pages:     []*Changes{{Changes: []Change{{Kind: ChangeCreated, ID: e.ID}}, Next: "c1"}},
		envelopes: map[MessageID]Envelope{e.ID: e},
	}

	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Envelope(context.Background(), acct, e.ID); err != nil {
		t.Error("bare-ID delta was not backfilled from Envelopes()")
	}
}

func TestPaginationWalksEveryPage(t *testing.T) {
	eng, store, acct := setup(t)
	ad := &scriptedAdapter{
		pages: []*Changes{
			{Changes: []Change{created(envelope("1", "INBOX"))}, Next: "p1", More: true},
			{Changes: []Change{created(envelope("2", "INBOX"))}, Next: "p2", More: true},
			{Changes: []Change{created(envelope("3", "INBOX"))}, Next: "p3"},
		},
	}

	rep, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Pages != 3 {
		t.Errorf("Pages = %d, want 3", rep.Pages)
	}
	if store.count(acct) != 3 {
		t.Errorf("stored %d messages, want 3", store.count(acct))
	}
}

func TestMaxPagesBoundsOneSyncWithoutLosingProgress(t *testing.T) {
	eng, store, acct := setup(t)
	eng.MaxPages = 2
	ad := &scriptedAdapter{
		pages: []*Changes{
			{Changes: []Change{created(envelope("1", "INBOX"))}, Next: "p1", More: true},
			{Changes: []Change{created(envelope("2", "INBOX"))}, Next: "p2", More: true},
			{Changes: []Change{created(envelope("3", "INBOX"))}, Next: "p3"},
		},
	}

	rep, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Pages != 2 {
		t.Errorf("Pages = %d, want the bound of 2", rep.Pages)
	}
	// Progress must be durable: the stored cursor lets the next call resume.
	if got, _ := store.Cursor(context.Background(), acct, "INBOX"); got != "p2" {
		t.Errorf("cursor = %q, want p2 so the next sync resumes", got)
	}
}

func TestCompleteListingSweepsMessagesThatVanished(t *testing.T) {
	// Most IMAP servers advertise neither QRESYNC nor CONDSTORE, so they
	// report what exists and never what stopped existing. The sweep against
	// a complete enumeration is the only way a deletion is observed there.
	eng, store, acct := setup(t)
	ad := &scriptedAdapter{
		pages: []*Changes{{
			Changes:  []Change{created(envelope("1", "INBOX")), created(envelope("2", "INBOX"))},
			Next:     "c1",
			Complete: true,
		}},
	}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if store.count(acct) != 2 {
		t.Fatalf("setup stored %d, want 2", store.count(acct))
	}

	// Message 2 is gone at the provider; the next enumeration omits it.
	ad.call = 0
	ad.pages = []*Changes{{
		Changes:  []Change{created(envelope("1", "INBOX"))},
		Next:     "c2",
		Complete: true,
	}}

	rep, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Deleted != 1 {
		t.Errorf("Deleted = %d, want 1", rep.Deleted)
	}
	if _, err := store.Envelope(context.Background(), acct, NativeMessageID(ProviderIMAP, "2")); !errors.Is(err, ErrNoStore) {
		t.Error("message absent from a complete listing was not swept")
	}
	if _, err := store.Envelope(context.Background(), acct, NativeMessageID(ProviderIMAP, "1")); err != nil {
		t.Error("message present in the listing was swept")
	}
}

func TestIncompleteListingNeverSweeps(t *testing.T) {
	// A CONDSTORE delta reports only what changed. Sweeping against it
	// would delete every message that simply had not been modified.
	eng, store, acct := setup(t)
	ad := &scriptedAdapter{
		pages: []*Changes{{
			Changes:  []Change{created(envelope("1", "INBOX")), created(envelope("2", "INBOX"))},
			Next:     "c1",
			Complete: true,
		}},
	}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}

	ad.call = 0
	ad.pages = []*Changes{{
		Changes:  []Change{{Kind: ChangeUpdated, ID: NativeMessageID(ProviderIMAP, "1"), Envelope: ptr(envelope("1", "INBOX"))}},
		Next:     "c2",
		Complete: false,
	}}

	rep, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Deleted != 0 {
		t.Errorf("Deleted = %d, want 0 on a partial listing", rep.Deleted)
	}
	if store.count(acct) != 2 {
		t.Errorf("stored %d messages, want 2; an unchanged message was swept", store.count(acct))
	}
}

func TestTruncatedEnumerationDoesNotSweep(t *testing.T) {
	// A complete listing cut short by MaxPages is no longer complete.
	// Sweeping on it would delete every message on the pages never read.
	eng, store, acct := setup(t)
	ad := &scriptedAdapter{
		pages: []*Changes{{
			Changes:  []Change{created(envelope("1", "INBOX")), created(envelope("2", "INBOX"))},
			Next:     "c1",
			Complete: true,
		}},
	}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}

	eng.MaxPages = 1
	ad.call = 0
	ad.pages = []*Changes{
		{Changes: []Change{created(envelope("1", "INBOX"))}, Next: "p1", More: true, Complete: true},
		{Changes: []Change{created(envelope("2", "INBOX"))}, Next: "p2", Complete: true},
	}

	rep, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Deleted != 0 {
		t.Errorf("Deleted = %d, want 0 when the enumeration was truncated", rep.Deleted)
	}
	if store.count(acct) != 2 {
		t.Errorf("stored %d messages, want 2; a truncated run swept live mail", store.count(acct))
	}
}

func ptr(e Envelope) *Envelope { return &e }

func TestReauthErrorMarksTheAccountAndStops(t *testing.T) {
	eng, store, acct := setup(t)
	ad := &scriptedAdapter{syncErr: fmt.Errorf("imap: %w", ErrReauthRequired)}

	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); !errors.Is(err, ErrReauthRequired) {
		t.Fatalf("err = %v, want ErrReauthRequired", err)
	}
	a, err := store.Account(context.Background(), acct)
	if err != nil {
		t.Fatal(err)
	}
	if !a.NeedsReauth {
		t.Error("account was not marked as needing reauthentication")
	}
}

func TestSyncAccountRefusesAnAccountNeedingReauth(t *testing.T) {
	eng, store, acct := setup(t)
	if err := store.SetNeedsReauth(context.Background(), acct, true); err != nil {
		t.Fatal(err)
	}
	ad := &scriptedAdapter{boxes: []Mailbox{{ID: "INBOX"}}}

	if _, err := eng.SyncAccount(context.Background(), acct, ad); !errors.Is(err, ErrReauthRequired) {
		t.Errorf("err = %v, want ErrReauthRequired before any provider call", err)
	}
	if len(ad.seenCursors) != 0 {
		t.Error("engine contacted the provider despite the account needing reauth")
	}
}

func TestSyncAccountContinuesPastOneUnreadableMailbox(t *testing.T) {
	// A folder the credential cannot read is common; it must not abandon
	// the rest of the account.
	eng, store, acct := setup(t)
	ad := &failingBoxAdapter{
		scriptedAdapter: scriptedAdapter{
			boxes: []Mailbox{{ID: "INBOX"}, {ID: "Restricted"}, {ID: "Archive"}},
			envelopes: map[MessageID]Envelope{
				NativeMessageID(ProviderIMAP, "1"): envelope("1", "INBOX"),
			},
		},
		fail: "Restricted",
	}

	reports, err := eng.SyncAccount(context.Background(), acct, ad)
	if err != nil {
		t.Fatalf("account sync aborted: %v", err)
	}
	if len(reports) != 2 {
		t.Errorf("got %d reports, want 2 (INBOX and Archive)", len(reports))
	}
	if len(store.mailboxes[acct]) != 3 {
		t.Error("mailbox list should record every folder, including unreadable ones")
	}
}

type failingBoxAdapter struct {
	scriptedAdapter
	fail MailboxID
}

func (a *failingBoxAdapter) Sync(ctx context.Context, box MailboxID, cur Cursor) (*Changes, error) {
	if box == a.fail {
		return nil, errors.New("permission denied")
	}
	return a.scriptedAdapter.Sync(ctx, box, cur)
}

func TestBodyIsFetchedLazilyAndThenCached(t *testing.T) {
	eng, store, acct := setup(t)
	e := envelope("1", "INBOX")
	ad := &scriptedAdapter{
		pages: []*Changes{{Changes: []Change{created(e)}, Next: "c1"}},
	}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}

	// Syncing envelopes must not have pulled the body.
	if _, err := store.Body(context.Background(), acct, e.ID); !errors.Is(err, ErrNoStore) {
		t.Error("body was fetched eagerly; the mirror would be unbounded")
	}

	body, err := eng.Body(context.Background(), acct, e.ID, ad)
	if err != nil {
		t.Fatal(err)
	}
	if body.Text == "" {
		t.Error("lazy body fetch returned nothing")
	}
	if _, err := store.Body(context.Background(), acct, e.ID); err != nil {
		t.Error("lazily fetched body was not cached")
	}
}

func TestApplyPushesToProviderBeforeTouchingTheMirror(t *testing.T) {
	// The provider is authoritative. A rejected mutation must leave no
	// local trace.
	eng, store, acct := setup(t)
	e := envelope("1", "INBOX")
	ad := &rejectingAdapter{scriptedAdapter: scriptedAdapter{
		pages:     []*Changes{{Changes: []Change{created(e)}, Next: "c1"}},
		envelopes: map[MessageID]Envelope{e.ID: e},
	}}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}

	err := eng.Apply(context.Background(), acct, Operation{
		Kind: OpAddKeyword, IDs: []MessageID{e.ID}, Keyword: "flagged",
	}, ad)
	if err == nil {
		t.Fatal("expected the provider rejection to surface")
	}

	stored, err := store.Envelope(context.Background(), acct, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Keywords.Flagged {
		t.Error("local state changed despite the provider rejecting the mutation")
	}
}

type rejectingAdapter struct{ scriptedAdapter }

func (a *rejectingAdapter) Apply(context.Context, Operation) error {
	return errors.New("provider rejected the operation")
}
