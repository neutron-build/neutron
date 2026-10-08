package mail

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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
	// it sees, so opening one is a database read rather than a live round trip
	// to the provider.
	//
	// The zero value is off, because an unbounded mirror is the wrong default
	// for a library: bodies are what make one unbounded. The SERVER turns it on
	// (see mail.go), because a single-operator install holds a few hundred
	// messages and the alternative is that every message is slow exactly once,
	// which is every message a person has not read yet.
	FetchBodies bool

	// MaxPages bounds how many pages one Sync call will walk before
	// returning. A mailbox with more changes than this resumes from its
	// stored cursor on the next call, so progress is never lost — the bound
	// exists so one enormous mailbox cannot starve every other account.
	// A staged scan cut short by it keeps its staged pages and resumes from
	// the scan's stored continuation (audit SYNC-02).
	MaxPages int

	accountLocks sync.Map // AccountID -> *sync.Mutex
}

func (e *Engine) lockAccount(acct AccountID) func() {
	value, _ := e.accountLocks.LoadOrStore(acct, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
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
	// mailbox is being refetched. With a staged-capable store the refetch
	// is a non-destructive scan: the old contents stay readable until the
	// replacement enumeration completes (audit SYNC-03).
	Reset bool

	Duration time.Duration
}

// SyncAccount syncs every mailbox in an account.
//
// Mailbox discovery runs first, so a folder created since the last sync is
// picked up in the same pass rather than on the one after.
func (e *Engine) SyncAccount(ctx context.Context, acct AccountID, ad Adapter) ([]SyncReport, error) {
	if err := e.checkGraphFormat(ctx, acct, ad); err != nil {
		return nil, err
	}
	unlock := e.lockAccount(acct)
	defer unlock()
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
	var folderErrors []error
	for _, box := range boxes {
		rep, err := e.SyncMailbox(ctx, acct, box.ID, ad)
		if err != nil {
			// One unreadable mailbox should not abandon the rest of the
			// account; a permission-scoped folder is common and normal.
			// The failure is still a failure: it is accumulated and
			// returned so the caller cannot mistake a partial sync for a
			// healthy one (which would clear the account's error state).
			//
			// Two conditions are account-wide rather than per-mailbox and
			// must propagate: a rejected credential, and throttling.
			// Swallowing a rate limit here would mean the caller sees a
			// clean result and immediately hammers the next mailbox, which
			// is precisely what the provider asked us to stop doing.
			if errors.Is(err, ErrReauthRequired) || errors.Is(err, ErrRateLimited) {
				return reports, err
			}
			e.log.WarnContext(ctx, "mailbox sync failed",
				"account", acct, "mailbox", box.ID, "err", err)
			folderErrors = append(folderErrors, fmt.Errorf("mailbox %s: %w", box.ID, err))
			if ctx.Err() != nil {
				break
			}
			continue
		}
		reports = append(reports, *rep)
	}
	return reports, errors.Join(folderErrors...)
}

// SyncMailbox brings one mailbox up to date.
//
// A staged scan in progress takes precedence over the delta path: the scan
// IS the recovery, and its pages are the authoritative enumeration. Without
// one, the stored cursor drives ordinary deltas; a provider-invalidated
// cursor or the first page of a full enumeration starts (or resumes) a
// staged scan when the store supports one, so interruption at any phase
// costs resumption, never the existing mirror (audit SYNC-03/SYNC-02).
func (e *Engine) SyncMailbox(ctx context.Context, acct AccountID, box MailboxID, ad Adapter) (*SyncReport, error) {
	if err := e.checkGraphFormat(ctx, acct, ad); err != nil {
		return nil, err
	}
	start := time.Now()
	rep := &SyncReport{Account: acct, Mailbox: box}
	defer func() { rep.Duration = time.Since(start) }()

	scans, scanCapable := e.store.(ScanStore)

	if scanCapable {
		scan, err := scans.RunningScan(ctx, acct, box)
		if err != nil && !errors.Is(err, ErrNoStore) {
			return nil, err
		}
		if scan != nil {
			return e.resumeScan(ctx, acct, box, ad, scan, rep, nil)
		}
	}

	cur, err := e.store.Cursor(ctx, acct, box)
	if err != nil {
		return nil, err
	}

	// A reset is permitted once per call. Looping on it would mean a
	// provider that always rejects its own cursor could spin forever,
	// refetching the mailbox on every pass.
	resetUsed := false

	// Stores without staged scans keep the in-memory enumeration sweep:
	// seen accumulates across pages and the sweep runs only if the run
	// finished, since a truncated run's listing is partial and sweeping
	// on it would delete live mail.
	var (
		seen            = map[MessageID]bool{}
		enumerating     bool
		ranToCompletion bool
		finalComplete   bool
	)

	for page := 0; page < e.MaxPages; page++ {
		changes, err := ad.Sync(ctx, box, cur)
		if err != nil {
			if errors.Is(err, ErrCursorInvalid) && !resetUsed {
				resetUsed = true
				rep.Reset = true
				if scanCapable {
					return e.beginScanAndRun(ctx, acct, box, ad, rep)
				}
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
			if scanCapable {
				return e.beginScanAndRun(ctx, acct, box, ad, rep)
			}
			if err := e.store.ResetMailbox(ctx, acct, box); err != nil {
				return nil, err
			}
			cur = changes.Next
			continue
		}

		// The first page of a full enumeration enters a staged scan when
		// the store supports one: the seen set, the staged envelopes, and
		// the continuation become durable together, so MaxPages and
		// restarts resume the enumeration instead of losing its
		// bookkeeping (audit SYNC-02).
		if scanCapable && changes.EnumerationStart {
			scan, err := scans.BeginScan(ctx, acct, box)
			if err != nil {
				return nil, err
			}
			return e.resumeScan(ctx, acct, box, ad, scan, rep, changes)
		}

		rep.Pages++
		if changes.EnumerationStart {
			enumerating = true
		}
		finalComplete = changes.Complete
		if err := e.apply(ctx, acct, box, ad, changes, rep, seen); err != nil {
			return nil, err
		}

		// The cursor is written only after the page's changes are stored.
		// Writing it first would lose changes on a crash between the two:
		// the mailbox would resume past data it never wrote.
		cur = changes.Next
		if _, atomic := e.store.(IdentityPageStore); !atomic {
			if err := e.store.PutCursor(ctx, acct, box, cur); err != nil {
				return nil, err
			}
		}

		if !changes.More {
			ranToCompletion = true
			break
		}
	}

	if enumerating && ranToCompletion && finalComplete {
		swept, err := e.sweepAbsent(ctx, acct, box, seen)
		if err != nil {
			return nil, err
		}
		rep.Deleted += swept
	}

	return rep, nil
}

// RequestRescan durably marks every mailbox of an account for a full,
// non-destructive rescan (audit SYNC-05/DATA-09: widening the retention
// window). The next SyncAccount calls resume the scans; pruning happens
// only as each scan completes, and messages the provider still holds —
// including older ones an incremental feed would never re-report — are
// staged back into the mirror by the enumeration itself.
func (e *Engine) RequestRescan(ctx context.Context, acct AccountID, ad Adapter) error {
	return e.RequestRescanVersion(ctx, acct, ad, 0)
}

// RequestRescanVersion is the generation-aware rescan (audit 5 SYNC-05).
// A retry used to re-BeginScan every mailbox, discarding the previous
// attempt's durable staged progress and restarting mailboxes that had
// already completed. With the policy version as the scan generation:
//
//   - a mailbox whose scan for THIS generation is already running keeps
//     its staged pages and resumes;
//   - a mailbox whose scan for this generation already completed (its
//     mail_scan_done marker) is not restarted at all;
//   - a mailbox with a stale-generation scan (a superseding policy) has
//     it replaced, which is correct — the old enumeration answers a
//     question nobody is asking anymore.
func (e *Engine) RequestRescanVersion(ctx context.Context, acct AccountID, ad Adapter, generation int64) error {
	unlock := e.lockAccount(acct)
	defer unlock()
	scans, ok := e.store.(ScanStore)
	if !ok {
		return fmt.Errorf("mail: store %T cannot stage reconciliation scans", e.store)
	}
	boxes, err := ad.Mailboxes(ctx)
	if err != nil {
		return e.classify(ctx, acct, err)
	}
	if err := e.store.PutMailboxes(ctx, acct, boxes); err != nil {
		return err
	}
	// Done markers of other generations are stale the moment this one
	// starts; drop them so the table stays bounded by the live generation.
	if generation != 0 {
		if err := scans.PruneScanDone(ctx, acct, generation); err != nil {
			return err
		}
	}
	for _, box := range boxes {
		scan, err := scans.RunningScan(ctx, acct, box.ID)
		if err != nil && !errors.Is(err, ErrNoStore) {
			return err
		}
		if scan != nil {
			if scan.Generation == generation {
				continue // resume this generation's staged progress
			}
			// A superseding policy replaces the stale enumeration.
			if _, err := scans.BeginScanGeneration(ctx, acct, box.ID, generation); err != nil {
				return err
			}
			continue
		}
		done, err := scans.ScanDone(ctx, acct, box.ID, generation)
		if err != nil {
			return err
		}
		if done {
			continue // this generation already finished this mailbox
		}
		if _, err := scans.BeginScanGeneration(ctx, acct, box.ID, generation); err != nil {
			return err
		}
	}
	return nil
}

// beginScanAndRun is the reset path for staged-capable stores: a durable
// scan begins from empty and runs immediately. The live mirror is not
// touched until the scan completes.
func (e *Engine) beginScanAndRun(ctx context.Context, acct AccountID, box MailboxID, ad Adapter, rep *SyncReport) (*SyncReport, error) {
	scan, err := e.store.(ScanStore).BeginScan(ctx, acct, box)
	if err != nil {
		return nil, err
	}
	return e.resumeScan(ctx, acct, box, ad, scan, rep, nil)
}

// resumeScan walks a staged scan's remaining pages. Every page is staged
// in one transaction (envelopes + seen + continuation). With PgStore the
// terminal page and completion share that transaction, so pruning absent
// messages, publishing the cursor, and recording the generation cannot be
// interrupted between commits. A failure or page-budget exhaustion between
// pages leaves the scan durable and resumable without pruning absences.
//
// A provider that rejects the scan's stored continuation (typed
// ErrCursorInvalid, or an explicit Reset) gets ONE bounded replacement:
// the invalid scan bookkeeping is discarded and a fresh scan begins from
// empty, retaining all live mail. The old behavior — return an error and
// leave the same rejected continuation stored — retried that continuation
// forever across calls (audit 5 SYNC-03). A provider that rejects even a
// fresh empty cursor is an error, not a loop.
func (e *Engine) resumeScan(ctx context.Context, acct AccountID, box MailboxID, ad Adapter, scan *Scan, rep *SyncReport, first *Changes) (*SyncReport, error) {
	scans := e.store.(ScanStore)
	restarted := false

	for page := 0; page < e.MaxPages; page++ {
		var changes *Changes
		if first != nil {
			changes, first = first, nil
		} else {
			var err error
			changes, err = ad.Sync(ctx, box, scan.Continuation)
			if err != nil {
				if errors.Is(err, ErrCursorInvalid) && !restarted {
					restarted = true
					rep.Reset = true
					fresh, berr := scans.BeginScanGeneration(ctx, acct, box, scan.Generation)
					if berr != nil {
						return nil, berr
					}
					scan = fresh
					first = nil
					continue
				}
				return nil, e.classify(ctx, acct, err)
			}
			if changes.Reset {
				if restarted {
					return nil, fmt.Errorf("mail: %s/%s provider rejected a fresh recovery enumeration; retrying later", acct, box)
				}
				restarted = true
				rep.Reset = true
				fresh, berr := scans.BeginScanGeneration(ctx, acct, box, scan.Generation)
				if berr != nil {
					return nil, berr
				}
				scan = fresh
				first = nil
				continue
			}
		}

		rep.Pages++
		for _, c := range changes.Changes {
			switch c.Kind {
			case ChangeCreated:
				rep.Created++
			case ChangeUpdated:
				rep.Updated++
			}
			// Destroyed inside a scan is NEGATIVE evidence, not noise: a
			// message seen on an earlier page and destroyed at the
			// provider before completion must not ride the seen set into
			// survival — it is removed from the staged set and the live
			// membership by ApplyScanPage (audit 5 SYNC-04).
		}

		prepared, err := e.prepareUpserts(ctx, acct, box, ad, changes)
		if err != nil {
			return nil, err
		}
		if atomic, ok := e.store.(IdentityPageStore); ok {
			pruned, err := atomic.ApplyIdentityScanPage(ctx, scan.ID, prepared.upsert, prepared.promoted, prepared.seen, prepared.destroy, changes.Next, changes.Complete)
			if err != nil {
				return nil, err
			}
			rep.Deleted += pruned
		} else if len(prepared.promoted) > 0 {
			return nil, ErrIdentityTransactionRequired
		} else if finalizer, ok := scans.(FinalScanPageStore); changes.Complete && ok {
			// Persist completion with the terminal page. Neither a process
			// interruption nor optional body-prefetch failure may strand a
			// running scan at an incremental cursor that never says Complete.
			pruned, err := finalizer.ApplyFinalScanPage(ctx, scan.ID, prepared.upsert, prepared.seen, prepared.destroy, changes.Next)
			if err != nil {
				return nil, err
			}
			rep.Deleted += pruned
		} else {
			if err := scans.ApplyScanPage(ctx, scan.ID, prepared.upsert, prepared.seen, prepared.destroy, changes.Next); err != nil {
				return nil, err
			}
			if changes.Complete {
				// Compatibility for older ScanStore implementations. Finish
				// before ancillary work, but only FinalScanPageStore can close
				// the crash window between these two store transactions.
				pruned, err := scans.FinishScan(ctx, acct, box, scan.ID, changes.Next)
				if err != nil {
					return nil, err
				}
				rep.Deleted += pruned
			}
		}

		rep.Upgraded += len(prepared.promoted)

		if err := e.prefetchBodies(ctx, acct, ad, prepared.upsert); err != nil {
			return nil, err
		}
		scan.Continuation = changes.Next

		if changes.Complete {
			return rep, nil
		}
	}

	// Page budget exhausted mid-scan. Progress is staged; the next
	// SyncMailbox call resumes from the stored continuation. This is not
	// an error, exactly like a bounded delta run.
	e.log.InfoContext(ctx, "staged scan reached the page budget; resuming on the next sync",
		"account", acct, "mailbox", box)
	return rep, nil
}

// sweepAbsent deletes stored messages that a complete enumeration omitted.
//
// This is only ever called after a run that both enumerated the mailbox in
// full and finished, and only on stores without staged scans. Running it on
// a partial listing would delete live mail, which is why the two conditions
// are tracked separately.
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

// pageUpserts is the prepared write set of one page: envelopes to upsert
// (bare-ID deltas backfilled), their post-promotion identities to record
// as present, and explicit superseded/replacement pairs for atomic migration.
type pageUpserts struct {
	upsert   []Envelope
	seen     []MessageID
	promoted []IdentityPair
	destroy  []MessageID
}

// prepareUpserts resolves one page of changes into its write set. It is
// shared by the delta path and staged scans so identity promotion,
// mailbox defaults, and thread keys behave identically in both.
func (e *Engine) prepareUpserts(ctx context.Context, acct AccountID, box MailboxID, ad Adapter, changes *Changes) (*pageUpserts, error) {
	out := &pageUpserts{}
	var fetch []MessageID

	for _, c := range changes.Changes {
		switch c.Kind {
		case ChangeDestroyed:
			out.destroy = append(out.destroy, c.ID)
		case ChangeCreated, ChangeUpdated:
			if c.Envelope != nil {
				out.upsert = append(out.upsert, *c.Envelope)
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
			return nil, e.classify(ctx, acct, err)
		}
		out.upsert = append(out.upsert, envs...)
	}

	for i := range out.upsert {
		env := &out.upsert[i]

		// Promotion is a transactional migration, with explicit old/new pairs.
		// A store without that contract refuses; no split delete/upsert fallback.
		if upgraded, ok := UpgradeIdentity(env.ID, env.MessageIDHeader); ok {
			out.promoted = append(out.promoted, IdentityPair{OldID: env.ID, NewID: upgraded})
			env.ID = upgraded
		}

		if len(env.MailboxIDs) == 0 {
			env.MailboxIDs = []MailboxID{box}
		}
		if env.ThreadID == "" {
			env.ThreadID = ThreadID(ThreadKey(env))
		}

		// Recorded after any identity upgrade, so a scan's seen set (and
		// the legacy sweep) compare against the identities actually
		// written to the store.
		out.seen = append(out.seen, env.ID)
	}
	return out, nil
}

// apply writes one page of deltas.
func (e *Engine) apply(ctx context.Context, acct AccountID, box MailboxID, ad Adapter, changes *Changes, rep *SyncReport, seen map[MessageID]bool) error {
	prepared, err := e.prepareUpserts(ctx, acct, box, ad, changes)
	if err != nil {
		return err
	}

	if atomic, ok := e.store.(IdentityPageStore); ok {
		if err := atomic.ApplyIdentityPage(ctx, acct, box, prepared.upsert, prepared.promoted, prepared.destroy, changes.Next); err != nil {
			return err
		}
	} else {
		if len(prepared.promoted) > 0 {
			return ErrIdentityTransactionRequired
		}
		if err := e.store.PutEnvelopes(ctx, acct, prepared.upsert); err != nil {
			return err
		}
		if err := e.store.RemoveFromMailbox(ctx, acct, box, prepared.destroy); err != nil {
			return err
		}
	}
	rep.Upgraded += len(prepared.promoted)

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

	for _, id := range prepared.seen {
		seen[id] = true
	}

	return e.prefetchBodies(ctx, acct, ad, prepared.upsert)
}

// prefetchBodies caches bodies for a staged page of envelopes when the
// server enabled prefetch. Message-local failures are best-effort;
// account-wide conditions are returned so the caller halts the sync.
func (e *Engine) prefetchBodies(ctx context.Context, acct AccountID, ad Adapter, upsert []Envelope) error {
	if !e.FetchBodies {
		return nil
	}
	for i := range upsert {
		// A body never changes, and an envelope is upserted again for
		// every flag change — a read receipt, a star, a move. Without this
		// check the first sync after someone reads their mail re-downloads
		// each message they touched, which is most of the cost of having
		// prefetch on at all.
		if _, err := e.store.Body(ctx, acct, upsert[i].ID); err == nil {
			continue
		}
		if err := e.fetchBody(ctx, acct, ad, upsert[i].ID); err != nil {
			// Account-wide conditions stop the whole prefetch loop, not
			// just this message: a provider already throttling must not
			// receive one more request per remaining body, and a dead
			// context means nobody is listening anyway. A message-local
			// failure stays best-effort — one unreadable message must not
			// stop the sync carrying every other one. With mirror foreign
			// keys in force (audit SYNC-04) this is also the path where a
			// body whose parent expired under a concurrent retention sweep
			// is refused instead of becoming an orphan.
			if stopPrefetch(err) {
				return err
			}
			e.log.WarnContext(ctx, "body fetch failed",
				"account", acct, "message", upsert[i].ID, "err", err)
		}
	}
	return nil
}

// stopPrefetch reports whether a body-fetch failure is account-wide: the
// provider rejected the credential, is throttling, or the caller went away.
// These must halt the batch instead of being retried per message.
func stopPrefetch(err error) bool {
	return errors.Is(err, ErrReauthRequired) ||
		errors.Is(err, ErrRateLimited) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

func (e *Engine) fetchBody(ctx context.Context, acct AccountID, ad Adapter, id MessageID) error {
	id, err := e.resolveIdentity(ctx, acct, id)
	if err != nil {
		return err
	}
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
	id, err := e.resolveIdentity(ctx, acct, id)
	if err != nil {
		return nil, err
	}
	if err := e.checkGraphFormat(ctx, acct, ad); err != nil {
		return nil, err
	}
	body, err := e.store.Body(ctx, acct, id)
	if err == nil {
		return body, nil
	}
	if !errors.Is(err, ErrNoStore) {
		return nil, err
	}

	if err := e.Locate(ctx, acct, id, ad); err != nil {
		return nil, e.classify(ctx, acct, err)
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

// Locate prepares an adapter to read one message when its protocol needs a
// mailbox selected first (see MailboxSelector). It is a no-op for adapters
// that do not care, and for stores that cannot say where a message lives.
// Body calls it; callers going straight to Raw or Attachment should too.
func (e *Engine) Locate(ctx context.Context, acct AccountID, id MessageID, ad Adapter) error {
	sel, ok := ad.(MailboxSelector)
	if !ok {
		return nil
	}
	loc, ok := e.store.(MessageLocator)
	if !ok {
		return nil
	}
	boxes, err := loc.MessageMailboxes(ctx, acct, id)
	if errors.Is(err, ErrNoStore) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(boxes) == 0 {
		return fmt.Errorf("mail: message %s is not filed in any mailbox", id)
	}
	return sel.SelectMailbox(ctx, boxes[0])
}

// Apply pushes a mutation to the provider and then refreshes the affected
// messages locally.
//
// The order matters and is not an implementation detail: the provider is
// authoritative, so a local write that has not been accepted upstream would
// make the mirror briefly the source of truth. If the provider rejects the
// operation, nothing local changed.
func (e *Engine) Apply(ctx context.Context, acct AccountID, op Operation, ad Adapter) error {
	op.IDs = append([]MessageID(nil), op.IDs...)
	for i, id := range op.IDs {
		resolved, err := e.resolveIdentity(ctx, acct, id)
		if err != nil {
			return err
		}
		op.IDs[i] = resolved
	}
	if err := e.checkGraphFormat(ctx, acct, ad); err != nil {
		return err
	}
	unlock := e.lockAccount(acct)
	defer unlock()
	// Protocols that need a selected mailbox get one before mutating: a
	// freshly resolved adapter has not selected anything, and resolution
	// of identities inside Apply is only meaningful in the message's own
	// mailbox (audit IMAP-03). No-op for adapters that do not care.
	if len(op.IDs) > 0 {
		if err := e.Locate(ctx, acct, op.IDs[0], ad); err != nil {
			return err
		}
	}
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
