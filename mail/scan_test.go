package mail

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The ScanStore half of the test double. It mirrors PgStore's
// mirror_scans/mirror_scan_seen semantics exactly — staged pages are
// atomic (envelopes + seen + continuation), and nothing is pruned outside
// FinishScan.

func (m *memStore) BeginScan(ctx context.Context, acct AccountID, box MailboxID) (*Scan, error) {
	return m.BeginScanGeneration(ctx, acct, box, 0)
}

func (m *memStore) BeginScanGeneration(_ context.Context, acct AccountID, box MailboxID, generation int64) (*Scan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := ScanID(fmt.Sprintf("scan-%d", atomic.AddInt64(&m.scanSeq, 1)))
	if old, ok := m.scans[acct][box]; ok {
		delete(m.scanSeen, old.ID)
		delete(m.scans[acct], box)
	}
	if m.scans[acct] == nil {
		m.scans[acct] = map[MailboxID]*Scan{}
	}
	scan := &Scan{ID: id, Account: acct, Mailbox: box, StartedAt: time.Now().UTC(), Generation: generation}
	m.scans[acct][box] = scan
	m.scanSeen[id] = map[MessageID]bool{}
	return scan, nil
}

func (m *memStore) ScanDone(_ context.Context, acct AccountID, box MailboxID, generation int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.scanDone[acct][generation][box], nil
}

func (m *memStore) PruneScanDone(_ context.Context, acct AccountID, keepGeneration int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for gen := range m.scanDone[acct] {
		if gen != keepGeneration {
			delete(m.scanDone[acct], gen)
		}
	}
	return nil
}

func (m *memStore) RunningScan(_ context.Context, acct AccountID, box MailboxID) (*Scan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	scan, ok := m.scans[acct][box]
	if !ok {
		return nil, ErrNoStore
	}
	cp := *scan
	return &cp, nil
}

func (m *memStore) RunningScans(_ context.Context, acct AccountID) ([]Scan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Scan
	for _, scan := range m.scans[acct] {
		out = append(out, *scan)
	}
	return out, nil
}

func (m *memStore) ApplyScanPage(ctx context.Context, scan ScanID, envs []Envelope, seen, destroyed []MessageID, next Cursor) error {
	_, err := m.applyScanPage(scan, envs, seen, destroyed, next, false)
	return err
}

func (m *memStore) ApplyFinalScanPage(ctx context.Context, scan ScanID, envs []Envelope, seen, destroyed []MessageID, terminal Cursor) (int, error) {
	return m.applyScanPage(scan, envs, seen, destroyed, terminal, true)
}

func (m *memStore) applyScanPage(scan ScanID, envs []Envelope, seen, destroyed []MessageID, next Cursor, final bool) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var acct AccountID
	found := false
	for _, byMailbox := range m.scans {
		for _, s := range byMailbox {
			if s.ID == scan {
				acct = s.Account
				found = true
			}
		}
	}
	if !found {
		return 0, ErrNoStore
	}
	// Validate the complete write set before touching the test mirror, just
	// as a PostgreSQL transaction rolls back a later invalid envelope.
	for _, e := range envs {
		if err := e.ID.Validate(); err != nil {
			return 0, err
		}
	}
	for i := range envs {
		e := envs[i]
		if e.Fingerprint == "" {
			e.Fingerprint = ComputeFingerprint(&e)
		}
		if m.messages[acct] == nil {
			m.messages[acct] = map[MessageID]*Envelope{}
			m.members[acct] = map[MessageID]map[MailboxID]bool{}
		}
		m.messages[acct][e.ID] = &e
		if e.MailboxIDsComplete || m.members[acct][e.ID] == nil {
			m.members[acct][e.ID] = map[MailboxID]bool{}
		}
		for _, b := range e.MailboxIDs {
			m.members[acct][e.ID][b] = true
		}
	}
	for _, id := range seen {
		m.scanSeen[scan][id] = true
	}
	var scanAcct AccountID
	var scanBox MailboxID
	for _, byMailbox := range m.scans {
		for _, s := range byMailbox {
			if s.ID == scan {
				scanAcct = s.Account
				scanBox = s.Mailbox
			}
		}
	}
	for _, id := range destroyed {
		delete(m.scanSeen[scan], id)
		if m.members[scanAcct] != nil && m.members[scanAcct][id] != nil {
			delete(m.members[scanAcct][id], scanBox)
			if len(m.members[scanAcct][id]) == 0 {
				delete(m.messages[scanAcct], id)
				delete(m.members[scanAcct], id)
				delete(m.bodies[scanAcct], id)
			}
		}
	}
	for _, byMailbox := range m.scans {
		for _, s := range byMailbox {
			if s.ID == scan {
				s.Continuation = next
			}
		}
	}
	if final {
		return m.finishScanLocked(scanAcct, scanBox, scan, next)
	}
	return 0, nil
}

func (m *memStore) FinishScan(_ context.Context, acct AccountID, box MailboxID, scan ScanID, terminal Cursor) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.finishScanLocked(acct, box, scan, terminal)
}

func (m *memStore) finishScanLocked(acct AccountID, box MailboxID, scan ScanID, terminal Cursor) (int, error) {
	s, ok := m.scans[acct][box]
	if !ok || s.ID != scan {
		return 0, ErrNoStore
	}
	seen := m.scanSeen[scan]
	pruned := 0
	for id, boxes := range m.members[acct] {
		if !boxes[box] {
			continue
		}
		if seen[id] {
			continue
		}
		pruned++
		delete(boxes, box)
		if len(boxes) == 0 {
			delete(m.messages[acct], id)
			delete(m.members[acct], id)
			delete(m.bodies[acct], id)
		}
	}
	if m.cursors[acct] == nil {
		m.cursors[acct] = map[MailboxID]Cursor{}
	}
	m.cursors[acct][box] = terminal
	if m.scanDone[acct] == nil {
		m.scanDone[acct] = map[int64]map[MailboxID]bool{}
	}
	if m.scanDone[acct][s.Generation] == nil {
		m.scanDone[acct][s.Generation] = map[MailboxID]bool{}
	}
	m.scanDone[acct][s.Generation][box] = true
	delete(m.scanSeen, scan)
	delete(m.scans[acct], box)
	return pruned, nil
}

var _ ScanStore = (*memStore)(nil)
var _ FinalScanPageStore = (*memStore)(nil)

// ---------------------------------------------------------------------------
// Staged reconciliation tests (audit SYNC-03/SYNC-02)
// ---------------------------------------------------------------------------

// stagedScenario is the SYNC-03 chain: a mailbox mirrored with two
// messages and a healthy cursor; the provider then invalidates the cursor
// and the replacement enumeration differs (message 2 is gone, message 3
// is new).
func stagedScenario(t *testing.T) (*Engine, *memStore, AccountID, *scriptedAdapter) {
	t.Helper()
	eng, store, acct := setup(t)
	ad := &scriptedAdapter{
		pages: []*Changes{{
			Changes: []Change{created(envelope("1", "INBOX")), created(envelope("2", "INBOX"))},
			Next:    "c1",
		}},
	}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if store.count(acct) != 2 {
		t.Fatalf("setup stored %d, want 2", store.count(acct))
	}
	return eng, store, acct, ad
}

func resetPages(ad *scriptedAdapter, pages ...*Changes) {
	ad.call = 0
	ad.pages = pages
}

// THE REGISTER'S HEADLINE CASE: the reset-then-fail window. The old path
// deleted local state when the cursor died and lost it forever when the
// rebuild failed; the staged scan must leave every pre-existing message
// readable, and a later retry must complete the rebuild.
func TestStagedResetSurvivesFailedRebuild(t *testing.T) {
	eng, store, acct, ad := stagedScenario(t)
	ctx := context.Background()

	// The provider invalidates the cursor, then the first replacement
	// page fails.
	failing := &killAfterNAdapter{scriptedAdapter{pages: []*Changes{{Reset: true, Next: ""}}}, 1, nil}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", failing); err == nil {
		t.Fatal("expected the rebuild failure to surface")
	}

	// Nothing was pruned: the failed rebuild left the mirror intact.
	if store.count(acct) != 2 {
		t.Fatalf("after failed rebuild stored %d messages, want 2 (no destructive reset)", store.count(acct))
	}
	for _, id := range []string{"1", "2"} {
		if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, id)); err != nil {
			t.Errorf("message %s vanished during a failed rebuild: %v", id, err)
		}
	}
	scans, err := store.RunningScans(ctx, acct)
	if err != nil || len(scans) != 1 {
		t.Fatalf("running scans = %v (err %v), want the staged scan to survive", scans, err)
	}

	// The retry enumerates successfully: 1 stays, 2 is gone, 3 is new.
	// (Reset was recorded on the invalidated call; the resume call is the
	// same durable recovery continuing.)
	resetPages(ad,
		&Changes{Changes: []Change{created(envelope("1", "INBOX"))}, Next: "p1", More: true, EnumerationStart: true},
		&Changes{Changes: []Change{created(envelope("3", "INBOX"))}, Next: "p2", Complete: true},
	)
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if store.count(acct) != 2 {
		t.Errorf("after completed rebuild stored %d messages, want 2 (1 and 3)", store.count(acct))
	}
	if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, "2")); !errors.Is(err, ErrNoStore) {
		t.Error("message 2 vanished at the provider but survived the completed scan")
	}
	if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, "3")); err != nil {
		t.Error("message 3 was not restored by the completed scan")
	}
	if got, _ := store.Cursor(ctx, acct, "INBOX"); got != "p2" {
		t.Errorf("terminal cursor = %q, want p2 published at completion", got)
	}
	if scans, _ := store.RunningScans(ctx, acct); len(scans) != 0 {
		t.Errorf("scan rows survived completion: %v", scans)
	}
}

// The kill-between-pages matrix: interruption after EVERY page count must
// prune nothing, and each retry must complete the same scan. This is the
// regression the register asks for verbatim.
func TestStagedScanInterruptionAtEveryPhaseNeverPrunes(t *testing.T) {
	// The provider's post-reset enumeration: three pages, then complete.
	enumeration := func() []*Changes {
		return []*Changes{
			{Changes: []Change{created(envelope("1", "INBOX"))}, Next: "p1", More: true, EnumerationStart: true},
			{Changes: []Change{created(envelope("3", "INBOX"))}, Next: "p2", More: true},
			{Changes: []Change{created(envelope("4", "INBOX"))}, Next: "p3", Complete: true},
		}
	}

	for killAfter := 0; killAfter <= 3; killAfter++ {
		t.Run(fmt.Sprintf("kill-after-%d-pages", killAfter), func(t *testing.T) {
			eng, store, acct, _ := stagedScenario(t)
			ctx := context.Background()

			// The cursor dies; `killAfter` enumeration pages land; then the
			// process "dies" mid-rebuild. With all three pages staged the
			// final Complete page has already committed the reconciliation,
			// so that variant asserts completion instead of interruption.
			pages := []*Changes{{Reset: true, Next: ""}}
			pages = append(pages, enumeration()[:killAfter]...)
			killed := &killAfterNAdapter{scriptedAdapter{pages: pages}, len(pages), nil}
			rep, err := eng.SyncMailbox(ctx, acct, "INBOX", killed)

			if killAfter < 3 {
				if err == nil {
					t.Fatal("expected the interruption to surface as a sync error")
				}
				// NOTHING was pruned at any interruption point, and the
				// filing state (the mirror rows product decisions key on)
				// stayed readable — the exact SYNC-03 chain. Staged pages
				// may have ADDED messages; removal is the forbidden thing.
				for _, id := range []string{"1", "2"} {
					if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, id)); err != nil {
						t.Errorf("kill at page %d: message %s was lost to pruning before completion", killAfter, id)
					}
				}
				if rep != nil && rep.Deleted != 0 {
					t.Fatalf("kill at page %d: Deleted = %d before completion, want 0", killAfter, rep.Deleted)
				}
				scans, _ := store.RunningScans(ctx, acct)
				if len(scans) != 1 {
					t.Fatalf("kill at page %d: %d running scans, want the durable staged scan", killAfter, len(scans))
				}
			} else if err != nil {
				t.Fatalf("full enumeration killed after its final page still errored: %v", err)
			}

			// A fresh engine (a restart) completes the same scan and
			// reconciles exactly: 1, 3, 4 present; 2 — absent from the
			// complete listing — pruned now and only now. (For
			// kill-after-3 the killed run already committed its own final
			// page, so the restart reconciles nothing further.)
			eng2 := NewEngine(store, discardLogger())
			ad := &scriptedAdapter{pages: enumeration()}
			rep, err = eng2.SyncMailbox(ctx, acct, "INBOX", ad)
			if err != nil {
				t.Fatal(err)
			}
			wantDeleted := 1
			if killAfter == 3 {
				wantDeleted = 0
			}
			if rep.Deleted != wantDeleted {
				t.Errorf("kill at %d: Deleted = %d after completion, want %d (message 2)", killAfter, rep.Deleted, wantDeleted)
			}
			if store.count(acct) != 3 {
				t.Errorf("kill at %d: stored %d after completion, want 3 (messages 1, 3, 4)", killAfter, store.count(acct))
			}
			if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, "2")); !errors.Is(err, ErrNoStore) {
				t.Errorf("kill at %d: message 2 was not reconciled at completion", killAfter)
			}
			if scans, _ := store.RunningScans(ctx, acct); len(scans) != 0 {
				t.Errorf("kill at %d: scan rows survived completion", killAfter)
			}
		})
	}
}

// killAfterNAdapter answers exactly n Sync calls and then fails — the
// process-kill injection for the interruption matrix.
type killAfterNAdapter struct {
	scriptedAdapter
	n   int
	err error
}

func (a *killAfterNAdapter) Sync(ctx context.Context, box MailboxID, cur Cursor) (*Changes, error) {
	if len(a.seenCursors) >= a.n {
		if a.err == nil {
			a.err = errors.New("process killed mid-rebuild")
		}
		return nil, a.err
	}
	return a.scriptedAdapter.Sync(ctx, box, cur)
}

func TestStagedScanPruneKeepsOtherMailboxMembership(t *testing.T) {
	eng, store, acct, _ := stagedScenario(t)
	ctx := context.Background()

	// Message 1 is also filed in Archive; message 2 only in INBOX. The
	// provider's INBOX enumeration now omits message 1 entirely (it left
	// INBOX but still lives in Archive) and carries 3.
	e1 := envelope("1", "INBOX")
	e1.MailboxIDs = []MailboxID{"INBOX", "Archive"}
	if err := store.PutEnvelopes(ctx, acct, []Envelope{e1}); err != nil {
		t.Fatal(err)
	}

	ad := &scriptedAdapter{pages: []*Changes{{
		Changes:          []Change{created(envelope("3", "INBOX"))},
		Next:             "final",
		EnumerationStart: true,
		Complete:         true,
	}}}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Envelope(ctx, acct, e1.ID); err != nil {
		t.Fatalf("message filed in Archive was deleted by the INBOX scan: %v", err)
	}
	boxes := store.members[acct][e1.ID]
	if !boxes["Archive"] || boxes["INBOX"] {
		t.Errorf("memberships = %v, want Archive kept and INBOX pruned", boxes)
	}
	if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, "2")); !errors.Is(err, ErrNoStore) {
		t.Error("message absent from the complete listing was not pruned")
	}
	if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, "3")); err != nil {
		t.Error("message 3 from the listing was not staged")
	}
}

// MaxPages cutting an enumeration mid-run must leave a durable, resumable
// scan whose earlier pages are already staged — the SYNC-02 half.
func TestMaxPagesCutLeavesDurableResumableScan(t *testing.T) {
	eng, store, acct, _ := stagedScenario(t)
	eng.MaxPages = 1
	ctx := context.Background()

	ad := &scriptedAdapter{pages: []*Changes{
		{Changes: []Change{created(envelope("1", "INBOX"))}, Next: "p1", More: true, EnumerationStart: true},
		{Changes: []Change{created(envelope("3", "INBOX"))}, Next: "p2", Complete: true},
	}}
	rep, err := eng.SyncMailbox(ctx, acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Deleted != 0 {
		t.Fatalf("Deleted = %d on a truncated scan, want 0 (never prune before completion)", rep.Deleted)
	}
	scans, _ := store.RunningScans(ctx, acct)
	if len(scans) != 1 || scans[0].Continuation != "p1" {
		t.Fatalf("scan after page budget = %+v, want continuation p1", scans)
	}
	// The cut staged page 1 durably (message 1 re-staged); page 2 has not
	// run, so message 3 is legitimately absent — and crucially messages 1
	// and 2 were not pruned by the truncated enumeration.
	for _, id := range []string{"1", "2"} {
		if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, id)); err != nil {
			t.Fatalf("truncated scan pruned message %s: %v", id, err)
		}
	}
	if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, "3")); err == nil {
		t.Fatal("message 3 appeared before its page ran")
	}

	// A NEW engine instance resumes the same scan — restart resumption.
	eng2 := NewEngine(store, discardLogger())
	eng2.MaxPages = 10
	resetPages(ad, &Changes{Changes: []Change{
		created(envelope("1", "INBOX")), created(envelope("3", "INBOX")),
	}, Next: "final", Complete: true})
	rep, err = eng2.SyncMailbox(ctx, acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Reset {
		t.Error("resuming a staged scan reported a reset")
	}
	if rep.Deleted != 1 {
		t.Errorf("Deleted = %d after completion, want 1 (message 2)", rep.Deleted)
	}
	if store.count(acct) != 2 {
		t.Errorf("stored %d, want 2", store.count(acct))
	}
	if got, _ := store.Cursor(ctx, acct, "INBOX"); got != "final" {
		t.Errorf("cursor = %q, want final", got)
	}
}

// A running scan takes precedence over the delta path: while a scan is in
// progress, ordinary cursor state must not interleave pages into it.
func TestRunningScanTakesPrecedenceOverDelta(t *testing.T) {
	eng, store, acct, _ := stagedScenario(t)
	ctx := context.Background()

	scan, err := store.BeginScan(ctx, acct, "INBOX")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyScanPage(ctx, scan.ID,
		[]Envelope{envelope("9", "INBOX")},
		[]MessageID{NativeMessageID(ProviderIMAP, "9")}, nil, "scan-page-1"); err != nil {
		t.Fatal(err)
	}

	// The delta adapter answers with a destroyed change — irrelevant
	// while the scan owns the mailbox; the scan page is what must land.
	ad := &scriptedAdapter{pages: []*Changes{{
		Changes: []Change{{Kind: ChangeDestroyed, ID: NativeMessageID(ProviderIMAP, "1")}},
		Next:    "scan-final", Complete: true,
	}}}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	// The scan enumerated only message 9: everything else in INBOX was
	// pruned at completion — including 1 and 2 — but NOT via the delta
	// destroy. What matters for precedence: the adapter was driven by the
	// scan's continuation, and message 9 survived.
	if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, "9")); err != nil {
		t.Error("scanned message 9 did not survive completion")
	}
	if got := ad.seenCursors[0]; got != "scan-page-1" {
		t.Errorf("adapter saw cursor %q, want the scan continuation scan-page-1", got)
	}
}

// RequestRescan marks every mailbox; the next sync completes the scans
// and restores messages an incremental feed would never re-report (the
// SYNC-05 engine primitive).
func TestRequestRescanDrivesFullRestoration(t *testing.T) {
	eng, store, acct, _ := stagedScenario(t)
	ctx := context.Background()

	ad := &scriptedAdapter{
		boxes: []Mailbox{{ID: "INBOX"}},
		pages: []*Changes{{
			Changes: []Change{
				created(envelope("1", "INBOX")),
				created(envelope("2", "INBOX")), // previously pruned locally
				created(envelope("3", "INBOX")), // never mirrored
			},
			Next: "fresh", EnumerationStart: true, Complete: true,
		}},
	}
	if err := eng.RequestRescan(ctx, acct, ad); err != nil {
		t.Fatal(err)
	}
	scans, _ := store.RunningScans(ctx, acct)
	if len(scans) != 1 {
		t.Fatalf("RequestRescan staged %d scans, want 1", len(scans))
	}

	// A delta-shaped page set must NOT be used: the running scan forces
	// enumeration semantics. Give the same adapter an enumeration final
	// page and complete.
	resetPages(ad, &Changes{Changes: []Change{
		created(envelope("1", "INBOX")),
		created(envelope("2", "INBOX")),
		created(envelope("3", "INBOX")),
	}, Next: "fresh", Complete: true})
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if store.count(acct) != 3 {
		t.Errorf("stored %d messages after rescan, want 3 restored", store.count(acct))
	}
}

func TestFinishScanWithoutScanIsErrNoStore(t *testing.T) {
	_, store, acct, _ := stagedScenario(t)
	if _, err := store.FinishScan(context.Background(), acct, "INBOX", "scan-none", "x"); !errors.Is(err, ErrNoStore) {
		t.Fatalf("err = %v, want ErrNoStore", err)
	}
}

// A provider that rejects even a FRESH recovery enumeration surfaces the
// bounded refusal instead of looping: one replacement per call, then an
// honest error (audit 5 SYNC-03). The first reset still restarts the scan
// non-destructively — that is the new capability — so it takes three
// rejections to be refused.
func TestResetDuringRecoveryScanIsRefused(t *testing.T) {
	eng, store, acct, _ := stagedScenario(t)
	ad := &scriptedAdapter{pages: []*Changes{
		{Reset: true, Next: ""},
		{Reset: true, Next: ""},
		{Reset: true, Next: ""},
	}}
	_, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad)
	if err == nil || !strings.Contains(err.Error(), "rejected a fresh recovery enumeration") {
		t.Fatalf("err = %v, want the fresh-enumeration refusal", err)
	}
	// The mirror survived the refused recovery.
	if store.count(acct) != 2 {
		t.Errorf("stored %d, want 2 intact", store.count(acct))
	}
}

// ---------------------------------------------------------------------------
// Bounded recovery restart + destroyed evidence + generations (audit 5
// SYNC-03/SYNC-04/SYNC-05)
// ---------------------------------------------------------------------------

// rejectCursorAdapter fails any Sync called with the poisoned continuation
// and delegates everything else — the provider that expired the stored
// scan continuation but serves a fresh enumeration happily.
type rejectCursorAdapter struct {
	scriptedAdapter
	reject Cursor
}

func (a *rejectCursorAdapter) Sync(ctx context.Context, box MailboxID, cur Cursor) (*Changes, error) {
	if cur == a.reject {
		return nil, fmt.Errorf("imap: session state expired: %w", ErrCursorInvalid)
	}
	return a.scriptedAdapter.Sync(ctx, box, cur)
}

// A stored scan continuation the provider rejects is REPLACED, not retried
// forever: the scan restarts from empty, keeps every live message, and the
// replacement enumeration completes in the same call (audit 5 SYNC-03).
func TestRejectedScanContinuationIsReplacedNotWedged(t *testing.T) {
	eng, store, acct, _ := stagedScenario(t)
	ctx := context.Background()

	// A scan is mid-flight with a stored continuation the provider now
	// rejects; the fresh enumeration (page at cursor "") reports message 3
	// new, message 2 gone, and completes.
	scan, err := store.BeginScan(ctx, acct, "INBOX")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyScanPage(ctx, scan.ID, nil, nil, nil, "poisoned"); err != nil {
		t.Fatal(err)
	}
	ad := &rejectCursorAdapter{reject: "poisoned"}
	ad.boxes = []Mailbox{{ID: "INBOX", Name: "INBOX"}}
	ad.pages = []*Changes{{
		Changes:  []Change{created(envelope("1", "INBOX")), created(envelope("3", "INBOX"))},
		Next:     "fresh-terminal",
		Complete: true,
	}}
	rep, err := eng.SyncMailbox(ctx, acct, "INBOX", ad)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Reset {
		t.Error("report did not record the replacement")
	}
	if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, "2")); !errors.Is(err, ErrNoStore) {
		t.Error("message 2 (absent from the complete replacement enumeration) survived")
	}
	if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, "3")); err != nil {
		t.Error("message 3 (present in the replacement) was not stored")
	}
}

// Destroyed evidence from an enumeration's own catch-up removes the
// message from the staged seen set AND the live membership — a message
// deleted at the provider mid-enumeration must not ride the seen set into
// survival (audit 5 SYNC-04).
func TestStagedScanAppliesDestroyedEvidence(t *testing.T) {
	_, store, acct, _ := stagedScenario(t)
	ctx := context.Background()

	// Page one stages the mailbox as [1, 2, 3]; the provider then
	// destroys message 1 before the terminal page completes. Without the
	// destroyed-evidence path, 1's staged presence would ride the seen
	// set into survival.
	scan, err := store.BeginScan(ctx, acct, "INBOX")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyScanPage(ctx, scan.ID,
		[]Envelope{envelope("1", "INBOX"), envelope("3", "INBOX")},
		[]MessageID{NativeMessageID(ProviderIMAP, "1"), NativeMessageID(ProviderIMAP, "2"), NativeMessageID(ProviderIMAP, "3")},
		nil, "page-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyScanPage(ctx, scan.ID, nil, nil,
		[]MessageID{NativeMessageID(ProviderIMAP, "1")}, "terminal"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishScan(ctx, acct, "INBOX", scan.ID, "terminal"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, "1")); !errors.Is(err, ErrNoStore) {
		t.Error("destroyed message 1 survived the scan")
	}
	for _, id := range []string{"2", "3"} {
		if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, id)); err != nil {
			t.Errorf("live message %s did not survive: %v", id, err)
		}
	}
}

// A reconciliation retry must RESUME its generation's staged progress and
// SKIP mailboxes that generation already completed — never re-request page
// one of anything (audit 5 SYNC-05).
func TestRequestRescanVersionPreservesProgressAcrossRetries(t *testing.T) {
	eng, store, acct := setup(t)
	ctx := context.Background()

	ad := &scriptedAdapter{
		boxes: []Mailbox{{ID: "INBOX", Name: "INBOX"}, {ID: "Archive", Name: "Archive"}},
	}
	// Archive completes on the first pass; INBOX is still mid-scan.
	archivePages := []*Changes{{
		Changes:  []Change{created(envelope("a1", "Archive"))},
		Next:     "archive-terminal",
		Complete: true,
	}}
	inboxPage := &Changes{
		Changes: []Change{created(envelope("1", "INBOX"))},
		Next:    "inbox-page-1",
	}

	// RequestRescanVersion for generation 7, then sync: Archive completes,
	// INBOX stages one page.
	if err := eng.RequestRescanVersion(ctx, acct, ad, 7); err != nil {
		t.Fatal(err)
	}
	ad.pages = archivePages
	if _, err := eng.SyncMailbox(ctx, acct, "Archive", ad); err != nil {
		t.Fatal(err)
	}
	ad.call = 0
	ad.pages = []*Changes{inboxPage}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}

	// The retry: same generation, new RequestRescanVersion call — the
	// destructive old behavior restarted BOTH mailboxes from page one.
	if err := eng.RequestRescanVersion(ctx, acct, ad, 7); err != nil {
		t.Fatal(err)
	}
	if scans, _ := store.RunningScans(ctx, acct); len(scans) != 1 {
		t.Fatalf("retry restarted scans: %d running, want only INBOX's", len(scans))
	}
	scan, err := store.RunningScan(ctx, acct, "INBOX")
	if err != nil {
		t.Fatal(err)
	}
	if scan.Generation != 7 {
		t.Fatalf("INBOX scan generation = %d, want 7", scan.Generation)
	}
	if scan.Continuation != "inbox-page-1" {
		t.Fatalf("INBOX staged continuation = %q, want the preserved inbox-page-1", scan.Continuation)
	}
	done, err := store.ScanDone(ctx, acct, "Archive", 7)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Error("Archive's completion marker was lost with its scan rows")
	}

	// The resumed INBOX scan finishes from its stored continuation — page
	// one is NOT requested again.
	ad.call = 0
	ad.seenCursors = nil
	ad.pages = []*Changes{{
		Changes:  []Change{created(envelope("2", "INBOX"))},
		Next:     "inbox-terminal",
		Complete: true,
	}}
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	for _, c := range ad.seenCursors {
		if c == "" {
			t.Fatal("the retry re-requested page one of INBOX instead of resuming")
		}
	}
	if scans, _ := store.RunningScans(ctx, acct); len(scans) != 0 {
		t.Fatalf("scans still running after completion: %d", len(scans))
	}
}
