package mail

import (
	"context"
	"errors"
	"testing"
)

// Real-PostgreSQL coverage for the identity-hook capabilities (see
// hooks.go). Same skip contract as store_integration_test.go: set
// NEUTRON_MAIL_TEST_DATABASE_URL.

// pgHooks is a product-side callback set that records everything it saw,
// including the transaction it saw it on, and can inject a failure at
// any callback — the contract's mid-transaction interruption.
type pgHooks struct {
	failAdmit   error
	failRemap   error
	failDropped error

	admitCalls   int
	remapCalls   int
	droppedCalls int
	admitTxids   []uint64
	remapTxids   []uint64
	droppedTxids []uint64

	acct       AccountID
	pairs      []IdentityPair
	droppedIDs []MessageID

	// rowsVisibleAtDrop counts how many reported ids were still readable
	// in mail_messages inside ReferencesDropped — the before-deletion
	// witness.
	rowsVisibleAtDrop int
}

func (h *pgHooks) txid(ctx context.Context, tx HookTx) uint64 {
	var id uint64
	if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&id); err != nil {
		return 0
	}
	return id
}

func (h *pgHooks) AdmitIdentityWrite(ctx context.Context, tx HookTx, acct AccountID) error {
	h.admitCalls++
	h.acct = acct
	h.admitTxids = append(h.admitTxids, h.txid(ctx, tx))
	return h.failAdmit
}

func (h *pgHooks) RemapIdentity(ctx context.Context, tx HookTx, acct AccountID, pairs []IdentityPair) error {
	h.remapCalls++
	h.acct = acct
	h.pairs = append(h.pairs, pairs...)
	h.remapTxids = append(h.remapTxids, h.txid(ctx, tx))
	return h.failRemap
}

func (h *pgHooks) ReferencesDropped(ctx context.Context, tx HookTx, acct AccountID, ids []MessageID) error {
	h.droppedCalls++
	h.acct = acct
	h.droppedIDs = append(h.droppedIDs, ids...)
	h.droppedTxids = append(h.droppedTxids, h.txid(ctx, tx))
	for _, id := range ids {
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM mail_messages WHERE account_id = $1 AND id = $2)`,
			string(acct), string(id)).Scan(&exists); err != nil {
			return err
		}
		if exists {
			h.rowsVisibleAtDrop++
		}
	}
	return h.failDropped
}

var _ IdentityHooks = (*pgHooks)(nil)

// txidSupported reports whether the backend can witness transaction
// identity (PostgreSQL can; Nucleus may not). The single-transaction
// assertions run only where the witness exists.
func txidSupported(t *testing.T, s *PgStore) bool {
	t.Helper()
	var id uint64
	if err := s.pool.QueryRow(context.Background(), `SELECT txid_current()`).Scan(&id); err != nil {
		return false
	}
	return true
}

func assertOneTransaction(t *testing.T, name string, txids ...uint64) {
	t.Helper()
	var seen uint64
	n := 0
	for _, id := range txids {
		if id == 0 {
			continue
		}
		if n == 0 {
			seen = id
		} else if id != seen {
			t.Errorf("%s ran on transactions %d and %d; the unit must be one transaction", name, seen, id)
			return
		}
		n++
	}
	if n < len(txids) {
		t.Errorf("%s: %d of %d callbacks produced a txid witness", name, n, len(txids))
	}
}

// The contract's replacement obligations in one: a mid-transaction hook
// failure leaves the old record readable and the unit retryable, the
// retry's body copy lands atomically, and every hook in the unit saw the
// SAME transaction. Fail-before: on the two-transaction seam this
// replaces (write new, commit, retire old, commit), a remap failure
// after the second commit leaves the old identity gone while the product
// still references it — unrecoverable without this test's ordering.
func TestIntegrationReplaceIdentitiesHookFailureRollbackAndRetry(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	oldID := PositionalMessageID("INBOX", 12, 345)
	if err := s.PutEnvelopes(ctx, acct, []Envelope{
		{ID: oldID, MailboxIDs: []MailboxID{"INBOX"}, Subject: "before promotion"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutBody(ctx, acct, &Body{MessageID: oldID, Text: "the body"}); err != nil {
		t.Fatal(err)
	}

	newID := HeaderMessageID("<promoted@example.com>")
	replacement := IdentityReplacement{
		OldID: oldID,
		Envelope: Envelope{
			ID: newID, MailboxIDs: []MailboxID{"INBOX"},
			Subject: "after promotion", MessageIDHeader: "<promoted@example.com>",
		},
		Body: &Body{MessageID: newID, Text: "the body"}, // copied inside the transaction
	}

	// Interruption after the writes, before the retire: the remap fails.
	failed := &pgHooks{failRemap: errors.New("product remap failed")}
	if err := s.ReplaceIdentities(ctx, acct, []IdentityReplacement{replacement}, failed); err == nil {
		t.Fatal("replacement ignored the injected remap failure")
	}
	if _, err := s.Envelope(ctx, acct, oldID); err != nil {
		t.Fatalf("OLD RECORD LOST to a rolled-back replacement: %v", err)
	}
	if _, err := s.Body(ctx, acct, oldID); err != nil {
		t.Fatalf("old body lost to a rolled-back replacement: %v", err)
	}
	if _, err := s.Envelope(ctx, acct, newID); !errors.Is(err, ErrNoStore) {
		t.Error("rolled-back replacement left the new identity behind")
	}
	if _, err := s.Body(ctx, acct, newID); !errors.Is(err, ErrNoStore) {
		t.Error("rolled-back replacement left the copied body behind")
	}

	// Interruption before any write: the admit fails, nothing changes.
	failed = &pgHooks{failAdmit: errors.New("product admit failed")}
	if err := s.ReplaceIdentities(ctx, acct, []IdentityReplacement{replacement}, failed); err == nil {
		t.Fatal("replacement ignored the injected admit failure")
	}
	if _, err := s.Envelope(ctx, acct, oldID); err != nil {
		t.Fatalf("old record lost to an admitted-then-aborted replacement: %v", err)
	}
	if _, err := s.Envelope(ctx, acct, newID); !errors.Is(err, ErrNoStore) {
		t.Error("aborted replacement wrote the new identity anyway")
	}

	// The retry — the same unit, hooks healthy — converges.
	hooks := &pgHooks{}
	if err := s.ReplaceIdentities(ctx, acct, []IdentityReplacement{replacement}, hooks); err != nil {
		t.Fatal(err)
	}
	got, err := s.Envelope(ctx, acct, newID)
	if err != nil {
		t.Fatalf("replacement envelope missing after retry: %v", err)
	}
	if got.Subject != "after promotion" {
		t.Errorf("Subject = %q, want the replacement's", got.Subject)
	}
	boxes, err := s.MessageMailboxes(ctx, acct, newID)
	if err != nil || len(boxes) != 1 || boxes[0] != "INBOX" {
		t.Errorf("memberships = %v err %v, want [INBOX]", boxes, err)
	}
	body, err := s.Body(ctx, acct, newID)
	if err != nil || body.Text != "the body" {
		t.Errorf("copied body = %+v err %v, want the body copied inside the transaction", body, err)
	}
	if _, err := s.Envelope(ctx, acct, oldID); !errors.Is(err, ErrNoStore) {
		t.Error("old identity survived a committed replacement")
	}
	if _, err := s.Body(ctx, acct, oldID); !errors.Is(err, ErrNoStore) {
		t.Error("old body survived a committed replacement")
	}

	if want := []IdentityPair{{OldID: oldID, NewID: newID}}; len(hooks.pairs) != 1 || hooks.pairs[0] != want[0] {
		t.Errorf("remap pairs = %v, want %v", hooks.pairs, want)
	}
	if txidSupported(t, s) {
		assertOneTransaction(t, "the successful replacement", hooks.admitTxids...)
		assertOneTransaction(t, "the successful replacement", hooks.remapTxids...)
		assertOneTransaction(t, "the successful replacement", append(append([]uint64{}, hooks.admitTxids...), hooks.remapTxids...)...)
	}
}

// The unit is idempotent: replaying a committed replacement changes
// nothing and fails nothing.
func TestIntegrationReplaceIdentitiesIsIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	oldID := PositionalMessageID("INBOX", 13, 346)
	newID := HeaderMessageID("<idempotent@example.com>")
	if err := s.PutEnvelopes(ctx, acct, []Envelope{
		{ID: oldID, MailboxIDs: []MailboxID{"INBOX"}, Subject: "original"},
	}); err != nil {
		t.Fatal(err)
	}
	replacement := IdentityReplacement{
		OldID:    oldID,
		Envelope: Envelope{ID: newID, MailboxIDs: []MailboxID{"INBOX"}, Subject: "replaced", MessageIDHeader: "<idempotent@example.com>"},
	}
	hooks := &pgHooks{}
	for i := 0; i < 2; i++ {
		if err := s.ReplaceIdentities(ctx, acct, []IdentityReplacement{replacement}, hooks); err != nil {
			t.Fatalf("replay %d failed: %v", i+1, err)
		}
	}
	if n := countMessages(t, s, acct); n != 1 {
		t.Errorf("after replay: %d messages, want 1", n)
	}
	if _, err := s.Envelope(ctx, acct, newID); err != nil {
		t.Fatalf("replacement envelope missing after replay: %v", err)
	}
	if hooks.admitCalls != 2 || hooks.remapCalls != 2 {
		t.Errorf("admit = %d remap = %d, want one of each per replay", hooks.admitCalls, hooks.remapCalls)
	}
}

// ReferencesDropped fires exactly for last-membership ids, inside the
// deleting transaction, with the rows still readable; the deletion
// semantics are otherwise RemoveFromMailbox's.
func TestIntegrationRemoveFromMailboxReferencedFiresForLastMembershipOnly(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	shared := HeaderMessageID("<ref-shared@example.com>")
	solo := HeaderMessageID("<ref-solo@example.com>")
	if err := s.PutEnvelopes(ctx, acct, []Envelope{
		{ID: shared, MailboxIDs: []MailboxID{"INBOX", "Archive"}, Subject: "shared"},
		{ID: solo, MailboxIDs: []MailboxID{"INBOX"}, Subject: "solo"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutBody(ctx, acct, &Body{MessageID: solo, Text: "solo body"}); err != nil {
		t.Fatal(err)
	}

	hooks := &pgHooks{}
	if err := s.RemoveFromMailboxReferenced(ctx, acct, "INBOX", []MessageID{shared, solo}, hooks); err != nil {
		t.Fatal(err)
	}
	if len(hooks.droppedIDs) != 1 || hooks.droppedIDs[0] != solo {
		t.Errorf("ReferencesDropped ids = %v, want exactly [%s]", hooks.droppedIDs, solo)
	}
	if hooks.rowsVisibleAtDrop != 1 {
		t.Errorf("rows visible to the hook = %d, want the reported id still present before deletion", hooks.rowsVisibleAtDrop)
	}
	if _, err := s.Envelope(ctx, acct, shared); err != nil {
		t.Fatalf("message still filed in Archive was deleted: %v", err)
	}
	boxes, err := s.MessageMailboxes(ctx, acct, shared)
	if err != nil || len(boxes) != 1 || boxes[0] != "Archive" {
		t.Errorf("shared memberships = %v err %v, want [Archive]", boxes, err)
	}
	if _, err := s.Envelope(ctx, acct, solo); !errors.Is(err, ErrNoStore) {
		t.Error("last-membership message survived")
	}
	if _, err := s.Body(ctx, acct, solo); !errors.Is(err, ErrNoStore) {
		t.Error("body of the last-membership message survived")
	}
	if txidSupported(t, s) {
		assertOneTransaction(t, "the referenced removal", hooks.droppedTxids...)
	}
}

// A ReferencesDropped failure aborts the whole unit: the message, its
// body, and the mailbox membership all survive, and a retry completes.
// Fail-before: with the callback outside the transaction (or the delete
// before the callback), the rows would already be gone when the product
// refuses.
func TestIntegrationRemoveFromMailboxReferencedHookErrorAbortsDeletion(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	solo := HeaderMessageID("<ref-abort@example.com>")
	if err := s.PutEnvelopes(ctx, acct, []Envelope{
		{ID: solo, MailboxIDs: []MailboxID{"INBOX"}, Subject: "solo"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutBody(ctx, acct, &Body{MessageID: solo, Text: "body"}); err != nil {
		t.Fatal(err)
	}

	hooks := &pgHooks{failDropped: errors.New("product snapshot failed")}
	if err := s.RemoveFromMailboxReferenced(ctx, acct, "INBOX", []MessageID{solo}, hooks); err == nil {
		t.Fatal("removal ignored the injected snapshot failure")
	}
	if _, err := s.Envelope(ctx, acct, solo); err != nil {
		t.Fatalf("hook failure deleted the message anyway: %v", err)
	}
	if _, err := s.Body(ctx, acct, solo); err != nil {
		t.Fatalf("hook failure deleted the body anyway: %v", err)
	}
	boxes, err := s.MessageMailboxes(ctx, acct, solo)
	if err != nil || len(boxes) != 1 || boxes[0] != "INBOX" {
		t.Errorf("memberships = %v err %v, want the aborted unlink rolled back to [INBOX]", boxes, err)
	}

	hooks.failDropped = nil
	if err := s.RemoveFromMailboxReferenced(ctx, acct, "INBOX", []MessageID{solo}, hooks); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Envelope(ctx, acct, solo); !errors.Is(err, ErrNoStore) {
		t.Error("retry after the snapshot failure did not delete the message")
	}
}

// End to end over the real store: the engine, given hooks, drives a
// positional promotion through ReplaceIdentities and a destroy through
// RemoveFromMailboxReferenced — no store primitives called directly.
func TestIntegrationEngineHooksDriveRealStore(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)

	eng := NewEngine(s, discardLogger())
	hooks := &pgHooks{}
	eng.Hooks = hooks

	pos := PositionalMessageID("INBOX", 20, 400)
	promoted := Envelope{
		ID: pos, MailboxIDs: []MailboxID{"INBOX"},
		Subject: "promote me", MessageIDHeader: "<engine@example.com>",
	}
	solo := Envelope{
		ID: HeaderMessageID("<engine-solo@example.com>"), MailboxIDs: []MailboxID{"INBOX"},
		Subject: "destroy me",
	}
	ad := &scriptedAdapter{pages: []*Changes{{
		Changes: []Change{{Kind: ChangeCreated, ID: pos, Envelope: &promoted}, created(solo)},
		Next:    "c1",
	}}}
	rep, err := eng.SyncMailbox(ctx, acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Upgraded != 1 {
		t.Errorf("Upgraded = %d, want 1", rep.Upgraded)
	}
	if hooks.admitCalls != 1 || hooks.remapCalls != 1 {
		t.Errorf("admit = %d remap = %d, want one of each", hooks.admitCalls, hooks.remapCalls)
	}
	newID := HeaderMessageID("<engine@example.com>")
	if _, err := s.Envelope(ctx, acct, newID); err != nil {
		t.Fatalf("promoted identity missing: %v", err)
	}
	if _, err := s.Envelope(ctx, acct, pos); !errors.Is(err, ErrNoStore) {
		t.Error("positional identity survived the engine-driven replacement")
	}

	ad.call = 0
	ad.pages = []*Changes{{Changes: []Change{{Kind: ChangeDestroyed, ID: solo.ID}}, Next: "c2"}}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if len(hooks.droppedIDs) != 1 || hooks.droppedIDs[0] != solo.ID {
		t.Errorf("ReferencesDropped ids = %v, want [%s]", hooks.droppedIDs, solo.ID)
	}
	if _, err := s.Envelope(ctx, acct, solo.ID); !errors.Is(err, ErrNoStore) {
		t.Error("destroyed message survived")
	}
}
