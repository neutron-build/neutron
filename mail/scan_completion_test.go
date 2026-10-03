package mail

import (
	"context"
	"errors"
	"testing"
)

type failTerminalBodyAdapter struct {
	scriptedAdapter
	bodyErr error
}

func (a *failTerminalBodyAdapter) Body(ctx context.Context, id MessageID) (*Body, error) {
	if a.bodyErr != nil {
		err := a.bodyErr
		a.bodyErr = nil
		return nil, err
	}
	return a.scriptedAdapter.Body(ctx, id)
}

// The terminal provider cursor is already incremental. A retry's no-op
// delta does not repeat Complete, so finalization must commit with the last
// enumeration page rather than depending on optional body-prefetch success.
func TestTerminalScanCompletionSurvivesPrefetchError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"rate-limit", ErrRateLimited},
		{"reauth", ErrReauthRequired},
		{"cancel", context.Canceled},
		{"deadline", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, store, acct, _ := stagedScenario(t)
			ctx := context.Background()
			eng.FetchBodies = true
			if _, err := store.BeginScanGeneration(ctx, acct, "INBOX", 7); err != nil {
				t.Fatal(err)
			}
			ad := &failTerminalBodyAdapter{bodyErr: tc.err, scriptedAdapter: scriptedAdapter{
				boxes: []Mailbox{{ID: "INBOX"}},
				pages: []*Changes{{
					Changes: []Change{created(envelope("1", "INBOX"))},
					Next:    "incremental-terminal", EnumerationStart: true, Complete: true,
				}},
			}}
			if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); !errors.Is(err, tc.err) {
				t.Fatalf("body error = %v, want %v", err, tc.err)
			}
			if len(ad.seenCursors) != 1 {
				t.Fatalf("terminal scan fetched extra provider pages: %v", ad.seenCursors)
			}
			assertCompletedScan(t, store, acct, 7, "incremental-terminal")
			if _, err := store.Envelope(ctx, acct, NativeMessageID(ProviderIMAP, "2")); !errors.Is(err, ErrNoStore) {
				t.Fatalf("absent message survived terminal page: %v", err)
			}

			// A new engine and a retried policy job must skip enumeration.
			restarted := NewEngine(store, discardLogger())
			if err := restarted.RequestRescanVersion(ctx, acct, ad, 7); err != nil {
				t.Fatal(err)
			}
			assertCompletedScan(t, store, acct, 7, "incremental-terminal")
			if _, err := restarted.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
				t.Fatal(err)
			}
			if len(ad.seenCursors) != 2 || ad.seenCursors[1] != "incremental-terminal" {
				t.Fatalf("retry enumerated instead of using terminal delta: %v", ad.seenCursors)
			}
			// Missing bodies remain available through the ordinary lazy path.
			if _, err := restarted.Body(ctx, acct, NativeMessageID(ProviderIMAP, "1"), ad); err != nil {
				t.Fatalf("lazy body recovery failed: %v", err)
			}
		})
	}
}

func assertCompletedScan(t *testing.T, store ScanStore, acct AccountID, generation int64, terminal Cursor) {
	t.Helper()
	ctx := context.Background()
	if scans, err := store.RunningScans(ctx, acct); err != nil || len(scans) != 0 {
		t.Fatalf("running scans = %+v, err %v, want none", scans, err)
	}
	if done, err := store.ScanDone(ctx, acct, "INBOX", generation); err != nil || !done {
		t.Fatalf("generation %d completion = %v, err %v", generation, done, err)
	}
	if cur, err := store.(Store).Cursor(ctx, acct, "INBOX"); err != nil || cur != terminal {
		t.Fatalf("cursor = %q, err %v, want %q", cur, err, terminal)
	}
}

type failingFinalScanStore struct {
	*memStore
	fail        error
	afterCommit bool
}

func (s *failingFinalScanStore) ApplyFinalScanPage(ctx context.Context, scan ScanID, envs []Envelope, seen, destroyed []MessageID, terminal Cursor) (int, error) {
	err := s.fail
	s.fail = nil
	if err != nil && !s.afterCommit {
		return 0, err
	}
	pruned, commitErr := s.memStore.ApplyFinalScanPage(ctx, scan, envs, seen, destroyed, terminal)
	if commitErr != nil {
		return 0, commitErr
	}
	return pruned, err
}

func TestTerminalScanTransactionFailureAndLostAcknowledgment(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		name := "rollback"
		if afterCommit {
			name = "lost-acknowledgment"
		}
		t.Run(name, func(t *testing.T) {
			_, store, acct, _ := stagedScenario(t)
			ctx := context.Background()
			scan, err := store.BeginScanGeneration(ctx, acct, "INBOX", 7)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.ApplyScanPage(ctx, scan.ID, nil, nil, nil, "last-enumeration-page"); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("terminal transaction interrupted")
			wrapped := &failingFinalScanStore{memStore: store, fail: failure, afterCommit: afterCommit}
			eng := NewEngine(wrapped, discardLogger())
			terminalPage := &Changes{
				Changes: []Change{created(envelope("1", "INBOX"))},
				Next:    "incremental-terminal", Complete: true,
			}
			ad := &scriptedAdapter{pages: []*Changes{terminalPage}}
			if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); !errors.Is(err, failure) {
				t.Fatalf("sync error = %v, want injected error", err)
			}
			if afterCommit {
				assertCompletedScan(t, store, acct, 7, "incremental-terminal")
				ad.pages = nil // only ordinary no-op incremental responses remain
			} else {
				running, err := store.RunningScan(ctx, acct, "INBOX")
				if err != nil || running.ID != scan.ID || running.Continuation != "last-enumeration-page" || running.Generation != 7 {
					t.Fatalf("rollback changed progress: %+v, err %v", running, err)
				}
				if cur, _ := store.Cursor(ctx, acct, "INBOX"); cur != "c1" {
					t.Fatalf("rollback published terminal cursor: %q", cur)
				}
				if store.count(acct) != 2 {
					t.Fatal("rollback pruned live messages")
				}
				ad.call = 0 // retry the same final page
			}
			restarted := NewEngine(wrapped, discardLogger())
			if _, err := restarted.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
				t.Fatal(err)
			}
			wantCursor := Cursor("last-enumeration-page")
			if afterCommit {
				wantCursor = "incremental-terminal"
			}
			if len(ad.seenCursors) != 2 || ad.seenCursors[1] != wantCursor {
				t.Fatalf("retry cursors = %v, want second cursor %q", ad.seenCursors, wantCursor)
			}
			assertCompletedScan(t, store, acct, 7, "incremental-terminal")
		})
	}
}

func TestCursorResetPreservesPolicyGeneration(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "invalid-cursor-error"
		if reset {
			name = "explicit-reset"
		}
		t.Run(name, func(t *testing.T) {
			eng, store, acct := setup(t)
			ctx := context.Background()
			eng.MaxPages = 2
			scan, err := store.BeginScanGeneration(ctx, acct, "INBOX", 7)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.ApplyScanPage(ctx, scan.ID, nil, nil, nil, "poisoned"); err != nil {
				t.Fatal(err)
			}
			pages := []*Changes{{Changes: []Change{created(envelope("1", "INBOX"))}, Next: "page-one", More: true, EnumerationStart: true}}
			base := &scriptedAdapter{boxes: []Mailbox{{ID: "INBOX"}}, pages: pages}
			var ad Adapter = base
			if reset {
				base.pages = append([]*Changes{{Reset: true}}, pages...)
			} else {
				ad = &rejectCursorAdapter{reject: "poisoned", scriptedAdapter: *base}
			}
			if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
				t.Fatal(err)
			}
			running, err := store.RunningScan(ctx, acct, "INBOX")
			if err != nil || running.Generation != 7 || running.ID == scan.ID || running.Continuation != "page-one" {
				t.Fatalf("replacement scan = %+v, err %v, want same generation and new progress", running, err)
			}
			if err := eng.RequestRescanVersion(ctx, acct, ad, 7); err != nil {
				t.Fatal(err)
			}
			retry, err := store.RunningScan(ctx, acct, "INBOX")
			if err != nil || retry.ID != running.ID || retry.Continuation != "page-one" {
				t.Fatalf("policy retry discarded replacement progress: %+v, err %v", retry, err)
			}
			terminal := &scriptedAdapter{pages: []*Changes{{Next: "terminal", Complete: true}}}
			if _, err := eng.SyncMailbox(ctx, acct, "INBOX", terminal); err != nil {
				t.Fatal(err)
			}
			assertCompletedScan(t, store, acct, 7, "terminal")
			if done, _ := store.ScanDone(ctx, acct, "INBOX", 0); done {
				t.Fatal("replacement completed the wrong policy generation")
			}
		})
	}
}

// The extension must not turn existing ScanStore implementations into the
// destructive legacy-store fallback merely because they lack the new method.
type legacyScanStore struct {
	Store
	ScanStore
}

func TestTerminalScanCompatibilityFinishesBeforePrefetch(t *testing.T) {
	_, store, acct, _ := stagedScenario(t)
	wrapped := &legacyScanStore{Store: store, ScanStore: store}
	if _, ok := any(wrapped).(FinalScanPageStore); ok {
		t.Fatal("test wrapper unexpectedly exposes atomic final-page staging")
	}
	eng := NewEngine(wrapped, discardLogger())
	eng.FetchBodies = true
	ad := &failTerminalBodyAdapter{bodyErr: ErrRateLimited, scriptedAdapter: scriptedAdapter{
		pages: []*Changes{{
			Changes: []Change{created(envelope("1", "INBOX"))},
			Next:    "terminal", EnumerationStart: true, Complete: true,
		}},
	}}
	if _, err := eng.SyncMailbox(context.Background(), acct, "INBOX", ad); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("body error = %v, want ErrRateLimited", err)
	}
	assertCompletedScan(t, store, acct, 0, "terminal")
}
