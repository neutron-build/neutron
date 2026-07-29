package mail

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Engine drives adapters against the store.
//
// One engine handles many accounts. Calls are serialised per account, because
// no adapter is required to be safe for concurrent use and because two syncs
// racing on one mailbox would interleave cursor writes.
type Engine struct {
	store Store
	log   *slog.Logger

	// FetchBodies makes the engine fetch and store a body for every message
	// it sees. Off by default: bodies are what make a mirror unbounded, and
	// the useful default is to fetch them on demand.
	FetchBodies bool

	// MaxPages bounds how many pages one Sync call will walk before
	// returning. A mailbox with more changes than this resumes from its
	// stored cursor on the next call, so progress is never lost — the bound
	// exists so one enormous mailbox cannot starve every other account.
	MaxPages int
}

// NewEngine builds an engine over a store.
func NewEngine(store Store, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.Default()
	}
	return &Engine{store: store, log: log, MaxPages: 100}
}

// SyncReport records what one sync did. It is returned for observability and
// consumed directly by the differential oracle, which compares it against
// what the provider actually holds.
type SyncReport struct {
	Account  AccountID
	Mailbox  MailboxID
	Created  int
	Updated  int
	Deleted  int
	Upgraded int
	Pages    int

	// Reset records that the provider invalidated the cursor and the
	// mailbox was refetched from empty.
	Reset bool

	Duration time.Duration
}

// SyncAccount syncs every mailbox in an account.
//
// Mailbox discovery runs first, so a folder created since the last sync is
// picked up in the same pass rather than on the one after.
func (e *Engine) SyncAccount(ctx context.Context, acct AccountID, ad Adapter) ([]SyncReport, error) {
	a, err := e.store.Account(ctx, acct)
	if err != nil {
		return nil, err
	}
	if a.NeedsReauth {
		return nil, fmt.Errorf("%w: account %s", ErrReauthRequired, acct)
	}

	boxes, err := ad.Mailboxes(ctx)
	if err != nil {
		return nil, e.classify(ctx, acct, err)
	}
	if err := e.store.PutMailboxes(ctx, acct, boxes); err != nil {
		return nil, err
	}

	reports := make([]SyncReport, 0, len(boxes))
	for _, box := range boxes {
		rep, err := e.SyncMailbox(ctx, acct, box.ID, ad)
		if err != nil {
			// One unreadable mailbox should not abandon the rest of the
			// account; a permission-scoped folder is common and normal.
			if errors.Is(err, ErrReauthRequired) {
				return reports, err
			}
			e.log.WarnContext(ctx, "mailbox sync failed",
				"account", acct, "mailbox", box.ID, "err", err)
			continue
		}
		reports = append(reports, *rep)
	}
	return reports, nil
}

// SyncMailbox brings one mailbox up to date.
func (e *Engine) SyncMailbox(ctx context.Context, acct AccountID, box MailboxID, ad Adapter) (*SyncReport, error) {
	start := time.Now()
	rep := &SyncReport{Account: acct, Mailbox: box}

	cur, err := e.store.Cursor(ctx, acct, box)
	if err != nil {
		return nil, err
	}

	// A reset is permitted once per call. Looping on it would mean a
	// provider that always rejects its own cursor could spin forever,
	// refetching the mailbox on every pass.
	resetUsed := false

	// A provider without a change feed reports what exists but never what
	// stopped existing, so deletions are inferred by sweeping the store
	// against a complete enumeration. seen accumulates across pages; the
	// sweep runs only if the run finished, since a truncated run's listing
	// is partial and sweeping on it would delete live mail.
	var (
		seen            = map[MessageID]bool{}
		enumerating     bool
		ranToCompletion bool
	)

	for page := 0; page < e.MaxPages; page++ {
		changes, err := ad.Sync(ctx, box, cur)
		if err != nil {
			if errors.Is(err, ErrCursorInvalid) && !resetUsed {
				resetUsed = true
				rep.Reset = true
				if err := e.store.ResetMailbox(ctx, acct, box); err != nil {
					return nil, err
				}
				cur = ""
				continue
			}
			return nil, e.classify(ctx, acct, err)
		}

		if changes.Reset {
			if resetUsed {
				return nil, fmt.Errorf("mail: %s/%s reset twice in one sync; provider cursor is unusable", acct, box)
			}
			resetUsed = true
			rep.Reset = true
			e.log.InfoContext(ctx, "provider invalidated cursor, refetching mailbox",
				"account", acct, "mailbox", box)
			if err := e.store.ResetMailbox(ctx, acct, box); err != nil {
				return nil, err
			}
			cur = changes.Next
			continue
		}

		rep.Pages++
		if changes.Complete {
			enumerating = true
		}
		if err := e.apply(ctx, acct, box, ad, changes, rep, seen); err != nil {
			return nil, err
		}

		// The cursor is written only after the page's changes are stored.
		// Writing it first would lose changes on a crash between the two:
		// the mailbox would resume past data it never wrote.
		cur = changes.Next
		if err := e.store.PutCursor(ctx, acct, box, cur); err != nil {
			return nil, err
		}

		if !changes.More {
			ranToCompletion = true
			break
		}
	}

	if enumerating && ranToCompletion {
		swept, err := e.sweepAbsent(ctx, acct, box, seen)
		if err != nil {
			return nil, err
		}
		rep.Deleted += swept
	}

	rep.Duration = time.Since(start)
	return rep, nil
}

// sweepAbsent deletes stored messages that a complete enumeration omitted.
//
// This is only ever called after a run that both enumerated the mailbox in
// full and finished. Running it on a partial listing would delete live mail,
// which is why the two conditions are tracked separately.
func (e *Engine) sweepAbsent(ctx context.Context, acct AccountID, box MailboxID, seen map[MessageID]bool) (int, error) {
	stored, err := e.store.EnvelopeIDs(ctx, acct, box)
	if err != nil {
		return 0, err
	}

	var gone []MessageID
	for _, id := range stored {
		if !seen[id] {
			gone = append(gone, id)
		}
	}
	if len(gone) == 0 {
		return 0, nil
	}

	e.log.InfoContext(ctx, "sweeping messages absent from a complete listing",
		"account", acct, "mailbox", box, "count", len(gone))
	if err := e.store.RemoveFromMailbox(ctx, acct, box, gone); err != nil {
		return 0, err
	}
	return len(gone), nil
}

// apply writes one page of deltas.
func (e *Engine) apply(ctx context.Context, acct AccountID, box MailboxID, ad Adapter, changes *Changes, rep *SyncReport, seen map[MessageID]bool) error {
	var (
		upsert  []Envelope
		fetch   []MessageID
		destroy []MessageID
	)

	for _, c := range changes.Changes {
		switch c.Kind {
		case ChangeDestroyed:
			destroy = append(destroy, c.ID)
		case ChangeCreated, ChangeUpdated:
			if c.Envelope != nil {
				upsert = append(upsert, *c.Envelope)
			} else {
				fetch = append(fetch, c.ID)
			}
		}
	}

	// Providers that report deltas as bare IDs — Gmail's history API among
	// them — need a second round trip for the envelopes.
	if len(fetch) > 0 {
		envs, err := ad.Envelopes(ctx, fetch)
		if err != nil {
			return e.classify(ctx, acct, err)
		}
		upsert = append(upsert, envs...)
	}

	for i := range upsert {
		env := &upsert[i]

		// A message first seen without its Message-ID header carries a
		// positional identity, which the next UIDVALIDITY change would
		// invalidate. Promoting it now, while the header is in hand, is
		// what keeps that message from reappearing as a duplicate later.
		if upgraded, ok := UpgradeIdentity(env.ID, env.MessageIDHeader); ok {
			if err := e.store.DeleteMessages(ctx, acct, []MessageID{env.ID}); err != nil {
				return err
			}
			env.ID = upgraded
			rep.Upgraded++
		}

		if len(env.MailboxIDs) == 0 {
			env.MailboxIDs = []MailboxID{box}
		}
		if env.ThreadID == "" {
			env.ThreadID = ThreadID(ThreadKey(env))
		}

		// Recorded after any identity upgrade, so the sweep compares
		// against the identities actually written to the store.
		seen[env.ID] = true
	}

	if err := e.store.PutEnvelopes(ctx, acct, upsert); err != nil {
		return err
	}

	// Destroyed means "gone from this mailbox", which for a multi-mailbox
	// provider is not the same as deleted. RemoveFromMailbox deletes only
	// once the last membership is gone.
	if err := e.store.RemoveFromMailbox(ctx, acct, box, destroy); err != nil {
		return err
	}

	for _, c := range changes.Changes {
		switch c.Kind {
		case ChangeCreated:
			rep.Created++
		case ChangeUpdated:
			rep.Updated++
		case ChangeDestroyed:
			rep.Deleted++
		}
	}

	if e.FetchBodies {
		for i := range upsert {
			if err := e.fetchBody(ctx, acct, ad, upsert[i].ID); err != nil {
				e.log.WarnContext(ctx, "body fetch failed",
					"account", acct, "message", upsert[i].ID, "err", err)
			}
		}
	}
	return nil
}

func (e *Engine) fetchBody(ctx context.Context, acct AccountID, ad Adapter, id MessageID) error {
	body, err := ad.Body(ctx, id)
	if err != nil {
		return err
	}
	return e.store.PutBody(ctx, acct, body)
}

// Body returns a message body, fetching and caching it if absent.
//
// This is the lazy path that keeps the mirror bounded: envelopes are always
// present, bodies arrive when something actually asks for one.
func (e *Engine) Body(ctx context.Context, acct AccountID, id MessageID, ad Adapter) (*Body, error) {
	body, err := e.store.Body(ctx, acct, id)
	if err == nil {
		return body, nil
	}
	if !errors.Is(err, ErrNoStore) {
		return nil, err
	}

	body, err = ad.Body(ctx, id)
	if err != nil {
		return nil, e.classify(ctx, acct, err)
	}
	if err := e.store.PutBody(ctx, acct, body); err != nil {
		return nil, err
	}
	return body, nil
}

// Apply pushes a mutation to the provider and then refreshes the affected
// messages locally.
//
// The order matters and is not an implementation detail: the provider is
// authoritative, so a local write that has not been accepted upstream would
// make the mirror briefly the source of truth. If the provider rejects the
// operation, nothing local changed.
func (e *Engine) Apply(ctx context.Context, acct AccountID, op Operation, ad Adapter) error {
	if err := ad.Apply(ctx, op); err != nil {
		return e.classify(ctx, acct, err)
	}

	envs, err := ad.Envelopes(ctx, op.IDs)
	if err != nil {
		// The mutation succeeded; only the refresh failed. The next sync
		// will pick the change up, so this is not worth failing the call.
		e.log.WarnContext(ctx, "mutation applied but refresh failed",
			"account", acct, "err", err)
		return nil
	}
	return e.store.PutEnvelopes(ctx, acct, envs)
}

// classify turns an adapter error into engine-level state.
//
// A permanently rejected credential is recorded on the account so that
// scheduled syncs stop retrying it — a revoked OAuth grant will never start
// working again, and hammering it wastes quota and looks like an attack.
func (e *Engine) classify(ctx context.Context, acct AccountID, err error) error {
	if errors.Is(err, ErrReauthRequired) {
		if serr := e.store.SetNeedsReauth(ctx, acct, true); serr != nil {
			e.log.ErrorContext(ctx, "could not record reauth requirement",
				"account", acct, "err", serr)
		}
		e.log.WarnContext(ctx, "account needs reauthentication", "account", acct)
	}
	return err
}
