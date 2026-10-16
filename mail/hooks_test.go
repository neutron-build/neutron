package mail

import (
	"context"
	"errors"
	"testing"
)

// The identity-capability half of the test double (see sync_test.go for
// the rest). It mirrors PgStore's ReplaceIdentities and
// RemoveFromMailboxReferenced semantics exactly — one unit, hooks inside
// it, all-or-nothing on failure — using an account snapshot in place of
// a transaction, because the double has no engine behind it.

var _ IdentityReplaceStore = (*memStore)(nil)
var _ ReferenceAwareStore = (*memStore)(nil)

// memHookTx is the HookTx the double hands to hooks: any callback that
// tries to run SQL gets a clear refusal instead of a silent nil, so a
// test hook written against the real store cannot quietly pass here.
type memHookTx struct{}

var errDoubleHasNoSQL = errors.New("mail: the test double has no SQL to run hooks against")

func (memHookTx) Exec(context.Context, string, ...any) error { return errDoubleHasNoSQL }

func (memHookTx) QueryRow(context.Context, string, ...any) HookRow { return memHookRow{} }

type memHookRow struct{}

func (memHookRow) Scan(...any) error { return errDoubleHasNoSQL }

func (memHookTx) Query(context.Context, string, ...any) (HookRows, error) {
	return nil, errDoubleHasNoSQL
}

// accountSnapshot is the double's rollback medium: the maps of one
// account, copied before a unit whose hooks may fail.
type accountSnapshot struct {
	messages map[MessageID]*Envelope
	members  map[MessageID]map[MailboxID]bool
	bodies   map[MessageID]*Body
}

func (m *memStore) snapshotLocked(acct AccountID) accountSnapshot {
	snap := accountSnapshot{
		messages: map[MessageID]*Envelope{},
		members:  map[MessageID]map[MailboxID]bool{},
		bodies:   map[MessageID]*Body{},
	}
	for id, e := range m.messages[acct] {
		snap.messages[id] = e
	}
	for id, boxes := range m.members[acct] {
		cp := make(map[MailboxID]bool, len(boxes))
		for b := range boxes {
			cp[b] = true
		}
		snap.members[id] = cp
	}
	for id, b := range m.bodies[acct] {
		snap.bodies[id] = b
	}
	return snap
}

func (m *memStore) restoreLocked(acct AccountID, snap accountSnapshot) {
	m.messages[acct] = snap.messages
	m.members[acct] = snap.members
	m.bodies[acct] = snap.bodies
}

func (m *memStore) ReplaceIdentities(ctx context.Context, acct AccountID, reps []IdentityReplacement, hooks IdentityHooks) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replaceCalls++
	snap := m.snapshotLocked(acct)
	rollback := func(err error) error {
		m.restoreLocked(acct, snap)
		return err
	}

	if hooks != nil {
		if err := hooks.AdmitIdentityWrite(ctx, memHookTx{}, acct); err != nil {
			return rollback(err)
		}
	}

	if m.messages[acct] == nil {
		m.messages[acct] = map[MessageID]*Envelope{}
		m.members[acct] = map[MessageID]map[MailboxID]bool{}
	}
	var pairs []IdentityPair
	for i := range reps {
		e := reps[i].Envelope
		if err := e.ID.Validate(); err != nil {
			return rollback(err)
		}
		if e.Fingerprint == "" {
			e.Fingerprint = ComputeFingerprint(&e)
		}
		m.messages[acct][e.ID] = &e
		if e.MailboxIDsComplete || m.members[acct][e.ID] == nil {
			m.members[acct][e.ID] = map[MailboxID]bool{}
		}
		for _, b := range e.MailboxIDs {
			m.members[acct][e.ID][b] = true
		}
		if reps[i].OldID != "" && reps[i].OldID != e.ID {
			pairs = append(pairs, IdentityPair{OldID: reps[i].OldID, NewID: e.ID})
		}
	}
	for i := range reps {
		if reps[i].Body == nil {
			continue
		}
		if m.bodies[acct] == nil {
			m.bodies[acct] = map[MessageID]*Body{}
		}
		m.bodies[acct][reps[i].Body.MessageID] = reps[i].Body
	}
	if hooks != nil && len(pairs) > 0 {
		if err := hooks.RemapIdentity(ctx, memHookTx{}, acct, pairs); err != nil {
			return rollback(err)
		}
	}
	for i := range reps {
		if reps[i].OldID == "" {
			continue
		}
		delete(m.messages[acct], reps[i].OldID)
		delete(m.members[acct], reps[i].OldID)
		delete(m.bodies[acct], reps[i].OldID)
	}
	return nil
}

func (m *memStore) RemoveFromMailboxReferenced(ctx context.Context, acct AccountID, box MailboxID, ids []MessageID, hooks IdentityHooks) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := m.snapshotLocked(acct)
	rollback := func(err error) error {
		m.restoreLocked(acct, snap)
		return err
	}

	var orphaned []MessageID
	for _, id := range ids {
		if m.members[acct][id] == nil {
			continue
		}
		delete(m.members[acct][id], box)
		if len(m.members[acct][id]) == 0 {
			orphaned = append(orphaned, id)
		}
	}
	if hooks != nil && len(orphaned) > 0 {
		if err := hooks.ReferencesDropped(ctx, memHookTx{}, acct, orphaned); err != nil {
			return rollback(err)
		}
	}
	for _, id := range orphaned {
		delete(m.messages[acct], id)
		delete(m.members[acct], id)
		delete(m.bodies[acct], id)
	}
	return nil
}

// recordingHooks records every callback so tests can assert which engine
// path ran and what the product saw.
type recordingHooks struct {
	admit   int
	remap   int
	dropped int

	acct        AccountID
	pairs       []IdentityPair
	droppedIDs  []MessageID
	failAdmit   error
	failRemap   error
	failDropped error
}

func (h *recordingHooks) AdmitIdentityWrite(context.Context, HookTx, AccountID) error {
	h.admit++
	return h.failAdmit
}

func (h *recordingHooks) RemapIdentity(_ context.Context, _ HookTx, acct AccountID, pairs []IdentityPair) error {
	h.remap++
	h.acct = acct
	h.pairs = append(h.pairs, pairs...)
	return h.failRemap
}

func (h *recordingHooks) ReferencesDropped(_ context.Context, _ HookTx, acct AccountID, ids []MessageID) error {
	h.dropped++
	h.acct = acct
	h.droppedIDs = append(h.droppedIDs, ids...)
	return h.failDropped
}

var _ IdentityHooks = (*recordingHooks)(nil)

// ---------------------------------------------------------------------------
// Engine wiring
// ---------------------------------------------------------------------------

// A positional identity promoting on ingest takes the paired replacement
// path when hooks are supplied: the product's remap runs inside the same
// unit that writes the new identity and retires the old one.
func TestIdentityHooksRunAtomicReplacementOnPromotion(t *testing.T) {
	eng, store, acct := setup(t)
	eng.Hooks = &recordingHooks{}
	ctx := context.Background()

	pos := PositionalMessageID("INBOX", 1, 7)
	env := Envelope{
		ID:              pos,
		MailboxIDs:      []MailboxID{"INBOX"},
		MessageIDHeader: "<real@example.com>",
	}
	ad := &scriptedAdapter{pages: []*Changes{{Changes: []Change{{Kind: ChangeCreated, ID: pos, Envelope: &env}}, Next: "c1"}}}
	hooks := eng.Hooks.(*recordingHooks)

	rep, err := eng.SyncMailbox(ctx, acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Upgraded != 1 {
		t.Errorf("Upgraded = %d, want 1", rep.Upgraded)
	}
	if store.replaceCalls != 1 {
		t.Fatalf("ReplaceIdentities called %d times, want the paired path once", store.replaceCalls)
	}
	if hooks.admit != 1 || hooks.remap != 1 {
		t.Errorf("admit = %d remap = %d, want one of each", hooks.admit, hooks.remap)
	}
	upgraded := HeaderMessageID("<real@example.com>")
	want := []IdentityPair{{OldID: pos, NewID: upgraded}}
	if len(hooks.pairs) != 1 || hooks.pairs[0] != want[0] {
		t.Errorf("remap pairs = %v, want %v", hooks.pairs, want)
	}
	if _, err := store.Envelope(ctx, acct, pos); !errors.Is(err, ErrNoStore) {
		t.Error("old identity survived the replacement")
	}
	if _, err := store.Envelope(ctx, acct, upgraded); err != nil {
		t.Errorf("new identity missing after replacement: %v", err)
	}
}

// The same promotion through the staged-scan path: the retirement after
// the final page is the tail of one atomic replacement, not a bare
// delete.
func TestIdentityHooksRunAtomicReplacementOnScanCompletion(t *testing.T) {
	eng, store, acct := setup(t)
	eng.Hooks = &recordingHooks{}
	ctx := context.Background()

	pos := PositionalMessageID("INBOX", 2, 9)
	env := Envelope{
		ID:              pos,
		MailboxIDs:      []MailboxID{"INBOX"},
		MessageIDHeader: "<scanned@example.com>",
	}
	ad := &scriptedAdapter{pages: []*Changes{{
		Changes:          []Change{{Kind: ChangeCreated, ID: pos, Envelope: &env}},
		Next:             "terminal",
		EnumerationStart: true,
		Complete:         true,
	}}}
	hooks := eng.Hooks.(*recordingHooks)

	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if store.replaceCalls != 1 {
		t.Fatalf("ReplaceIdentities called %d times, want the paired retirement once", store.replaceCalls)
	}
	if hooks.admit != 1 || hooks.remap != 1 {
		t.Errorf("admit = %d remap = %d, want one of each", hooks.admit, hooks.remap)
	}
	upgraded := HeaderMessageID("<scanned@example.com>")
	if len(hooks.pairs) != 1 || hooks.pairs[0] != (IdentityPair{OldID: pos, NewID: upgraded}) {
		t.Errorf("remap pairs = %v, want [%s→%s]", hooks.pairs, pos, upgraded)
	}
	if _, err := store.Envelope(ctx, acct, pos); !errors.Is(err, ErrNoStore) {
		t.Error("old identity survived the scan-path replacement")
	}
	if _, err := store.Envelope(ctx, acct, upgraded); err != nil {
		t.Errorf("new identity missing after scan-path replacement: %v", err)
	}
}

// A mid-unit hook failure rolls the whole replacement back: the old
// identity stays readable and the next sync retries the same unit to
// convergence — the engine-level statement of the contract's atomicity.
func TestIdentityHooksReplacementFailureRollsBackAndRetries(t *testing.T) {
	eng, store, acct := setup(t)
	eng.Hooks = &recordingHooks{failRemap: errors.New("product remap failed")}
	ctx := context.Background()

	pos := PositionalMessageID("INBOX", 3, 11)
	env := Envelope{
		ID:              pos,
		MailboxIDs:      []MailboxID{"INBOX"},
		MessageIDHeader: "<retry@example.com>",
	}
	pages := []*Changes{{Changes: []Change{{Kind: ChangeCreated, ID: pos, Envelope: &env}}, Next: "c1"}}
	ad := &scriptedAdapter{pages: pages}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err == nil {
		t.Fatal("sync hid the injected hook failure")
	}
	// The page's ordinary envelope did not land either — the unit is
	// all-or-nothing — and nothing was retired.
	if _, err := store.Envelope(ctx, acct, HeaderMessageID("<retry@example.com>")); !errors.Is(err, ErrNoStore) {
		t.Error("partially-applied replacement left the new identity behind")
	}

	hooks := eng.Hooks.(*recordingHooks)
	hooks.failRemap = nil
	ad.call = 0
	ad.pages = pages
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Envelope(ctx, acct, pos); !errors.Is(err, ErrNoStore) {
		t.Error("old identity survived the retried replacement")
	}
	if _, err := store.Envelope(ctx, acct, HeaderMessageID("<retry@example.com>")); err != nil {
		t.Fatalf("new identity missing after retry: %v", err)
	}
}

// Destroyed messages that lose their LAST membership are reported to the
// product inside the deleting unit; messages still filed elsewhere are
// not — exactly the last-membership set.
func TestIdentityHooksReportDestroyedLastMembershipOnly(t *testing.T) {
	eng, store, acct := setup(t)
	eng.Hooks = &recordingHooks{}
	ctx := context.Background()

	shared := envelope("1", "INBOX")
	shared.MailboxIDs = []MailboxID{"INBOX", "Archive"}
	solo := envelope("2", "INBOX")
	ad := &scriptedAdapter{pages: []*Changes{{Changes: []Change{created(shared), created(solo)}, Next: "c1"}}}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	hooks := eng.Hooks.(*recordingHooks)
	if hooks.dropped != 0 {
		t.Fatalf("ReferencesDropped fired during ingest: %v", hooks.droppedIDs)
	}

	// Both leave INBOX; only solo loses its last membership.
	ad.call = 0
	ad.pages = []*Changes{{Changes: []Change{
		{Kind: ChangeDestroyed, ID: shared.ID},
		{Kind: ChangeDestroyed, ID: solo.ID},
	}, Next: "c2"}}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if hooks.dropped != 1 || len(hooks.droppedIDs) != 1 || hooks.droppedIDs[0] != solo.ID {
		t.Errorf("ReferencesDropped ids = %v, want exactly [%s]", hooks.droppedIDs, solo.ID)
	}
	if _, err := store.Envelope(ctx, acct, shared.ID); err != nil {
		t.Fatalf("message still filed in Archive was deleted: %v", err)
	}
	if _, err := store.Envelope(ctx, acct, solo.ID); !errors.Is(err, ErrNoStore) {
		t.Error("last-membership message survived the destroy")
	}
}

// A ReferencesDropped failure aborts the deletion: the message keeps its
// body and its membership, and a later sync retries.
func TestIdentityHooksDroppedFailureKeepsTheMessage(t *testing.T) {
	eng, store, acct := setup(t)
	eng.Hooks = &recordingHooks{failDropped: errors.New("product snapshot failed")}
	ctx := context.Background()

	solo := envelope("3", "INBOX")
	ad := &scriptedAdapter{pages: []*Changes{{Changes: []Change{created(solo)}, Next: "c1"}}}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}

	ad.call = 0
	ad.pages = []*Changes{{Changes: []Change{{Kind: ChangeDestroyed, ID: solo.ID}}, Next: "c2"}}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err == nil {
		t.Fatal("sync hid the injected snapshot failure")
	}
	if _, err := store.Envelope(ctx, acct, solo.ID); err != nil {
		t.Fatalf("hook failure deleted the message anyway: %v", err)
	}
	boxes := store.members[acct][solo.ID]
	if len(boxes) != 1 || !boxes["INBOX"] {
		t.Errorf("memberships = %v, want INBOX restored", boxes)
	}

	hooks := eng.Hooks.(*recordingHooks)
	hooks.failDropped = nil
	ad.call = 0
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Envelope(ctx, acct, solo.ID); !errors.Is(err, ErrNoStore) {
		t.Error("retry after the snapshot failure did not delete the message")
	}
}

// bareStore hides every optional capability of the store it wraps: only
// the Store interface survives. The engine must behave exactly as it did
// before hooks existed — the probes are the only new branches.
type bareStore struct{ Store }

// With the capabilities hidden, hooks-on must fall back to today's
// paths: no callback fires, and the end state is identical.
func TestIdentityHooksAbsentCapabilitiesPreserveBehavior(t *testing.T) {
	ctx := context.Background()

	// The replacement path falls back to PutEnvelopes + DeleteMessages.
	store := newMemStore()
	acct := AccountID("acct-1")
	if err := store.PutAccount(ctx, &Account{ID: acct, Provider: ProviderIMAP, Email: "user@example.com"}); err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(bareStore{store}, discardLogger())
	hooks := &recordingHooks{}
	eng.Hooks = hooks

	pos := PositionalMessageID("INBOX", 4, 13)
	env := Envelope{
		ID:              pos,
		MailboxIDs:      []MailboxID{"INBOX"},
		MessageIDHeader: "<plain@example.com>",
	}
	ad := &scriptedAdapter{pages: []*Changes{{Changes: []Change{{Kind: ChangeCreated, ID: pos, Envelope: &env}}, Next: "c1"}}}
	rep, err := eng.SyncMailbox(ctx, acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Upgraded != 1 {
		t.Errorf("Upgraded = %d, want 1 (identity still promotes)", rep.Upgraded)
	}
	if store.replaceCalls != 0 || hooks.admit != 0 || hooks.remap != 0 {
		t.Errorf("paired path ran without the capability: replaceCalls=%d admit=%d remap=%d",
			store.replaceCalls, hooks.admit, hooks.remap)
	}
	if _, err := store.Envelope(ctx, acct, pos); !errors.Is(err, ErrNoStore) {
		t.Error("old identity survived the plain promotion path")
	}
	if _, err := store.Envelope(ctx, acct, HeaderMessageID("<plain@example.com>")); err != nil {
		t.Errorf("new identity missing after plain promotion: %v", err)
	}

	// The destroy path falls back to RemoveFromMailbox: no callback, the
	// same deletion semantics.
	solo := envelope("5", "INBOX")
	ad.call = 0
	ad.pages = []*Changes{{Changes: []Change{created(solo)}, Next: "c2"}}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	ad.call = 0
	ad.pages = []*Changes{{Changes: []Change{{Kind: ChangeDestroyed, ID: solo.ID}}, Next: "c3"}}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if hooks.dropped != 0 {
		t.Errorf("ReferencesDropped fired without the capability: %v", hooks.droppedIDs)
	}
	if _, err := store.Envelope(ctx, acct, solo.ID); !errors.Is(err, ErrNoStore) {
		t.Error("plain RemoveFromMailbox did not delete the last-membership message")
	}
}

// Nil hooks — the library default — must be a no-op even on a capable
// store: the probes are gated on the host having supplied callbacks.
func TestNilHooksKeepThePlainPathsOnACapableStore(t *testing.T) {
	eng, store, acct := setup(t)
	ctx := context.Background()

	pos := PositionalMessageID("INBOX", 5, 17)
	env := Envelope{
		ID:              pos,
		MailboxIDs:      []MailboxID{"INBOX"},
		MessageIDHeader: "<nilhooks@example.com>",
	}
	ad := &scriptedAdapter{pages: []*Changes{{Changes: []Change{{Kind: ChangeCreated, ID: pos, Envelope: &env}}, Next: "c1"}}}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if store.replaceCalls != 0 {
		t.Errorf("ReplaceIdentities ran with nil hooks: %d calls", store.replaceCalls)
	}
	if _, err := store.Envelope(ctx, acct, pos); !errors.Is(err, ErrNoStore) {
		t.Error("plain promotion path changed under nil hooks")
	}
	if _, err := store.Envelope(ctx, acct, HeaderMessageID("<nilhooks@example.com>")); err != nil {
		t.Errorf("new identity missing under nil hooks: %v", err)
	}
}
