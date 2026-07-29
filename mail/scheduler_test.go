package mail

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// countingAdapter records how many times it was asked to sync.
type countingAdapter struct {
	scriptedAdapter
	mu    sync.Mutex
	syncs int
	err   error
}

func (a *countingAdapter) Sync(ctx context.Context, box MailboxID, cur Cursor) (*Changes, error) {
	a.mu.Lock()
	a.syncs++
	err := a.err
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &Changes{Next: "c1"}, nil
}

func (a *countingAdapter) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.syncs
}

func schedulerFixture(t *testing.T, ad Adapter, accounts ...Account) (*Scheduler, *memStore) {
	t.Helper()
	store := newMemStore()
	for i := range accounts {
		if err := store.PutAccount(context.Background(), &accounts[i]); err != nil {
			t.Fatal(err)
		}
	}
	eng := NewEngine(store, discardLogger())
	s := NewScheduler(store, eng, func(AccountID) (Adapter, bool) { return ad, true }, discardLogger())
	s.Jitter = false // determinism; jitter is covered separately
	return s, store
}

func TestSchedulerSyncsEveryEligibleAccount(t *testing.T) {
	ad := &countingAdapter{scriptedAdapter: scriptedAdapter{boxes: []Mailbox{{ID: "INBOX"}}}}
	s, _ := schedulerFixture(t, ad,
		Account{ID: "a1", Provider: ProviderIMAP, Email: "a1@x.com"},
		Account{ID: "a2", Provider: ProviderIMAP, Email: "a2@x.com"},
	)

	s.RunOnce(context.Background())

	if got := ad.count(); got != 2 {
		t.Errorf("synced %d mailboxes, want 2 (one per account)", got)
	}
}

func TestSchedulerSkipsAccountsNeedingReauth(t *testing.T) {
	// A revoked credential never recovers on its own. Retrying it every
	// interval burns quota and, at scale, looks like an attack.
	ad := &countingAdapter{scriptedAdapter: scriptedAdapter{boxes: []Mailbox{{ID: "INBOX"}}}}
	s, _ := schedulerFixture(t, ad,
		Account{ID: "good", Provider: ProviderIMAP, Email: "g@x.com"},
		Account{ID: "revoked", Provider: ProviderIMAP, Email: "r@x.com", NeedsReauth: true},
	)

	s.RunOnce(context.Background())

	if got := ad.count(); got != 1 {
		t.Errorf("synced %d times, want 1; the revoked account should have been skipped", got)
	}
}

func TestSchedulerBacksOffAfterRateLimiting(t *testing.T) {
	ad := &countingAdapter{
		scriptedAdapter: scriptedAdapter{boxes: []Mailbox{{ID: "INBOX"}}},
		err:             fmt.Errorf("provider said no: %w", ErrRateLimited),
	}
	s, _ := schedulerFixture(t, ad, Account{ID: "a1", Provider: ProviderIMAP, Email: "a@x.com"})
	s.Interval = time.Hour

	s.RunOnce(context.Background())
	first := ad.count()
	if first == 0 {
		t.Fatal("the first pass did not attempt a sync")
	}

	// A second pass inside the backoff window must not touch the provider.
	s.RunOnce(context.Background())
	if ad.count() != first {
		t.Errorf("synced again during backoff: %d then %d", first, ad.count())
	}
}

func TestSchedulerRetriesAfterTheBackoffExpires(t *testing.T) {
	ad := &countingAdapter{
		scriptedAdapter: scriptedAdapter{boxes: []Mailbox{{ID: "INBOX"}}},
		err:             fmt.Errorf("slow down: %w", ErrRateLimited),
	}
	s, _ := schedulerFixture(t, ad, Account{ID: "a1", Provider: ProviderIMAP, Email: "a@x.com"})
	s.Interval = time.Millisecond

	s.RunOnce(context.Background())
	first := ad.count()

	time.Sleep(5 * time.Millisecond)
	s.RunOnce(context.Background())

	if ad.count() <= first {
		t.Error("the account was never retried after its backoff expired")
	}
}

func TestSchedulerRecoversAfterASuccessfulSync(t *testing.T) {
	ad := &countingAdapter{
		scriptedAdapter: scriptedAdapter{boxes: []Mailbox{{ID: "INBOX"}}},
		err:             fmt.Errorf("throttled: %w", ErrRateLimited),
	}
	s, _ := schedulerFixture(t, ad, Account{ID: "a1", Provider: ProviderIMAP, Email: "a@x.com"})
	s.Interval = time.Millisecond

	s.RunOnce(context.Background())

	// The provider recovers.
	ad.mu.Lock()
	ad.err = nil
	ad.mu.Unlock()
	time.Sleep(5 * time.Millisecond)
	s.RunOnce(context.Background())

	before := ad.count()
	s.RunOnce(context.Background())
	if ad.count() <= before {
		t.Error("backoff was not cleared after a successful sync")
	}
}

func TestSchedulerRunStopsOnContextCancellation(t *testing.T) {
	ad := &countingAdapter{scriptedAdapter: scriptedAdapter{boxes: []Mailbox{{ID: "INBOX"}}}}
	s, _ := schedulerFixture(t, ad, Account{ID: "a1", Provider: ProviderIMAP, Email: "a@x.com"})
	s.Interval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// Run does an immediate first pass, so the account syncs before the
	// first tick would ever fire.
	time.Sleep(20 * time.Millisecond)
	if ad.count() == 0 {
		t.Error("Run did not sync immediately on start; the mirror would be stale for a full interval")
	}

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("Run returned nil on cancellation, want the context error")
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestSchedulerRespectsTheConcurrencyBound(t *testing.T) {
	var (
		mu      sync.Mutex
		active  int
		maxSeen int
	)

	ad := &blockingAdapter{
		onSync: func() {
			mu.Lock()
			active++
			if active > maxSeen {
				maxSeen = active
			}
			mu.Unlock()

			time.Sleep(20 * time.Millisecond)

			mu.Lock()
			active--
			mu.Unlock()
		},
	}

	accounts := make([]Account, 8)
	for i := range accounts {
		accounts[i] = Account{
			ID:       AccountID(fmt.Sprintf("a%d", i)),
			Provider: ProviderIMAP,
			Email:    fmt.Sprintf("a%d@x.com", i),
		}
	}

	s, _ := schedulerFixture(t, ad, accounts...)
	s.Concurrency = 2
	s.RunOnce(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if maxSeen > 2 {
		t.Errorf("as many as %d accounts synced at once, want at most 2", maxSeen)
	}
	if maxSeen == 0 {
		t.Error("no account synced at all")
	}
}

type blockingAdapter struct {
	scriptedAdapter
	onSync func()
}

func (a *blockingAdapter) Mailboxes(context.Context) ([]Mailbox, error) {
	return []Mailbox{{ID: "INBOX"}}, nil
}

func (a *blockingAdapter) Sync(ctx context.Context, box MailboxID, cur Cursor) (*Changes, error) {
	a.onSync()
	return &Changes{Next: "c1"}, nil
}
