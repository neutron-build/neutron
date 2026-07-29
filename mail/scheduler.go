package mail

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// Scheduler keeps connected accounts in sync without anyone asking.
//
// Without this the mirror only advances when something calls the sync
// endpoint, which makes it a cache that is always stale by exactly as long as
// nobody looked.
type Scheduler struct {
	store    Store
	eng      *Engine
	adapters func(AccountID) (Adapter, bool)
	log      *slog.Logger

	// Tokens mints a credential for an account when no request is in
	// flight. Without it the scheduler can only drive accounts whose
	// adapters were configured up front — a self-hosted mailbox with an app
	// password — because there is nothing to authenticate an OAuth account
	// with between requests.
	//
	// With it, and a Resolver, the scheduler asks the application for a
	// fresh token and builds an adapter per run. Refresh still happens in
	// exactly one place; this process holds no refresh token.
	Tokens  TokenSource
	Resolve Resolver

	// Interval is how often each account is synced. Defaults to 5 minutes.
	//
	// Polling rather than IMAP IDLE is deliberate for the general case:
	// IDLE holds one connection per mailbox per account, and providers cap
	// concurrent connections — Gmail at around fifteen. A poll costs one
	// short connection per account per interval and scales to many accounts
	// on a budget IDLE cannot match. IDLE is worth adding for a small number
	// of hot mailboxes, not as the default for all of them.
	Interval time.Duration

	// Concurrency bounds how many accounts sync at once. Defaults to 4.
	Concurrency int

	// Jitter spreads the first sync of each account across the interval so
	// a restart does not stampede every provider at once. Defaults to true.
	Jitter bool

	mu      sync.Mutex
	backoff map[AccountID]time.Time
	rng     *rand.Rand
}

// NewScheduler builds a scheduler over an engine.
func NewScheduler(store Store, eng *Engine, adapters func(AccountID) (Adapter, bool), log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{
		store:       store,
		eng:         eng,
		adapters:    adapters,
		log:         log,
		Interval:    5 * time.Minute,
		Concurrency: 4,
		Jitter:      true,
		backoff:     map[AccountID]time.Time{},
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Run syncs every account on a loop until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	if s.Interval <= 0 {
		s.Interval = 5 * time.Minute
	}
	if s.Concurrency <= 0 {
		s.Concurrency = 4
	}

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	s.log.InfoContext(ctx, "sync scheduler started",
		"interval", s.Interval, "concurrency", s.Concurrency)

	// A first pass runs immediately so a restart does not leave the mirror
	// stale for a whole interval.
	s.RunOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			s.log.InfoContext(ctx, "sync scheduler stopped")
			return ctx.Err()
		case <-ticker.C:
			s.RunOnce(ctx)
		}
	}
}

// RunOnce syncs every eligible account once, respecting the concurrency bound.
func (s *Scheduler) RunOnce(ctx context.Context) {
	accounts, err := s.store.Accounts(ctx)
	if err != nil {
		s.log.ErrorContext(ctx, "could not list accounts", "err", err)
		return
	}

	sem := make(chan struct{}, s.Concurrency)
	var wg sync.WaitGroup

	for _, a := range accounts {
		acct := a
		if !s.eligible(acct) {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			s.syncOne(ctx, acct)
		}()
	}
	wg.Wait()
}

// eligible reports whether an account should be synced now.
func (s *Scheduler) eligible(a Account) bool {
	// A revoked credential will never start working on its own. Retrying it
	// every interval burns provider quota and, at scale, looks like an
	// attack on the account.
	if a.NeedsReauth {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	until, backingOff := s.backoff[a.ID]
	return !backingOff || time.Now().After(until)
}

func (s *Scheduler) syncOne(ctx context.Context, a Account) {
	ad, release, err := s.adapterFor(ctx, a.ID)
	if err != nil {
		s.handleFailure(ctx, a.ID, err)
		return
	}
	if ad == nil {
		return
	}
	defer release()

	if s.Jitter {
		// Spread the load so many accounts on one provider do not arrive
		// in the same instant.
		s.mu.Lock()
		d := time.Duration(s.rng.Int63n(int64(s.Interval / 10)))
		s.mu.Unlock()
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return
		}
	}

	reports, err := s.eng.SyncAccount(ctx, a.ID, ad)
	if err != nil {
		s.handleFailure(ctx, a.ID, err)
		return
	}

	s.mu.Lock()
	delete(s.backoff, a.ID)
	s.mu.Unlock()

	var created, updated, deleted int
	for _, r := range reports {
		created += r.Created
		updated += r.Updated
		deleted += r.Deleted
	}
	if created+updated+deleted > 0 {
		s.log.InfoContext(ctx, "account synced",
			"account", a.ID, "created", created, "updated", updated, "deleted", deleted)
	}
}

// adapterFor obtains an adapter for a background run.
//
// Two paths, and a deployment may use either or both. A statically configured
// adapter is returned as-is and outlives the run. Otherwise a token is
// requested from the application and an adapter built for this run only, so
// the connection never outlives the credential that authorised it.
//
// A nil adapter with a nil error means "this account is not syncable here",
// which is an ordinary state — not every account has a credential path — and
// must not be logged as a failure on every tick.
func (s *Scheduler) adapterFor(ctx context.Context, acct AccountID) (Adapter, func(), error) {
	if s.adapters != nil {
		if ad, ok := s.adapters(acct); ok {
			return ad, func() {}, nil
		}
	}
	if s.Tokens == nil || s.Resolve == nil {
		return nil, nil, nil
	}

	cred, err := s.Tokens.Token(ctx, acct)
	if err != nil {
		return nil, nil, err
	}
	return s.Resolve(ctx, acct, cred)
}

// handleFailure decides whether and when to try the account again.
func (s *Scheduler) handleFailure(ctx context.Context, acct AccountID, err error) {
	switch {
	case errors.Is(err, ErrReauthRequired):
		// The engine has already marked the account; eligible() will skip
		// it from here until a human reconnects it.
		s.log.WarnContext(ctx, "account needs reauthentication, pausing sync", "account", acct)

	case errors.Is(err, ErrRateLimited):
		// Backing off a full interval is the polite response, and matters
		// because the provider is counting.
		until := time.Now().Add(s.Interval)
		s.mu.Lock()
		s.backoff[acct] = until
		s.mu.Unlock()
		s.log.WarnContext(ctx, "provider rate limited, backing off",
			"account", acct, "until", until)

	default:
		s.log.WarnContext(ctx, "sync failed", "account", acct, "err", err)
	}
}
