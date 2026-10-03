package mail

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"hash/fnv"
	"time"
)

// Staged reconciliation scans (audit SYNC-03/SYNC-02, pass 6/7).
//
// The old recovery path for an invalidated cursor deleted local mailbox
// state first and refetched second. If the refetch failed, the mirror was
// already missing rows, and the product's orphan cleanup could read that
// absence as authoritative deletions of user filing state. A rebuildable
// provider mirror is not the same thing as rebuildable user decisions.
//
// A scan is the replacement: a full-mailbox enumeration that stages every
// page durably (envelopes, seen IDs, continuation cursor in one
// transaction) and prunes NOTHING until the final page has been staged.
// At authoritative completion, PgStore atomically stages the final page,
// prunes memberships absent from the seen set, publishes the terminal
// cursor, and drops the scan rows. A crash leaves either the previous
// continuation retryable or the entire scan completed, never a running
// scan whose terminal cursor has lost its completion evidence.

// ScanID identifies one staged scan.
type ScanID string

// Scan is a durable, resumable full-mailbox enumeration.
type Scan struct {
	ID           ScanID
	Account      AccountID
	Mailbox      MailboxID
	Continuation Cursor
	StartedAt    time.Time
	// Generation is the policy generation this scan enumerates for: 0 for
	// a policy-agnostic recovery scan, the product's policy_version for a
	// reconciliation rescan (audit 5 SYNC-05).
	Generation int64
}

// ScanStore is the staged-reconciliation surface of a store (audit
// SYNC-03). It is a separate interface so Store implementations outside
// this module keep compiling; the engine falls back to the legacy
// destructive reset only for stores that do not implement it. PgStore,
// the only implementation production runs, always does.
type ScanStore interface {
	// BeginScan starts a staged scan for one mailbox, discarding any
	// previous staged progress for it. Nothing live is touched: the
	// current mirror contents and cursor stay exactly as they are.
	BeginScan(ctx context.Context, acct AccountID, box MailboxID) (*Scan, error)

	// BeginScanGeneration is BeginScan tagged with a policy generation;
	// the generation rides the scan rows so retries can distinguish
	// "resume this generation's progress" from "a newer policy
	// supersedes it" (audit 5 SYNC-05).
	BeginScanGeneration(ctx context.Context, acct AccountID, box MailboxID, generation int64) (*Scan, error)

	// ScanDone reports whether this generation's scan of the mailbox
	// already completed (its completion outlives the dropped scan rows).
	ScanDone(ctx context.Context, acct AccountID, box MailboxID, generation int64) (bool, error)

	// PruneScanDone drops completion markers for every generation but the
	// kept one — called when a new generation starts, so the marker table
	// stays bounded by the live generation.
	PruneScanDone(ctx context.Context, acct AccountID, keepGeneration int64) error

	// RunningScan returns the staged scan for a mailbox, or ErrNoStore
	// when none is running.
	RunningScan(ctx context.Context, acct AccountID, box MailboxID) (*Scan, error)

	// RunningScans lists every staged scan for an account. Callers use it
	// to decide whether a full-account reconciliation has finished.
	RunningScans(ctx context.Context, acct AccountID) ([]Scan, error)

	// ApplyScanPage stages one page atomically: the envelopes are
	// upserted, their (post-promotion) IDs recorded as seen, destroyed
	// IDs are removed from both the staged seen set and this mailbox's
	// live membership, and the continuation advanced — one transaction.
	// A failure stages nothing. The destroyed set exists so a message
	// seen on an earlier page and deleted at the provider before the
	// enumeration finished cannot survive the prune as a false positive
	// (audit 5 SYNC-04).
	ApplyScanPage(ctx context.Context, scan ScanID, envs []Envelope, seen, destroyed []MessageID, next Cursor) error

	// FinishScan completes the scan authoritatively under one
	// transaction: memberships of this mailbox absent from the seen set
	// are pruned, messages left with no membership anywhere are deleted,
	// the terminal cursor is published, the scan's generation-completion
	// marker is recorded, and the scan rows are dropped. It returns the
	// number of pruned memberships. Nothing is pruned before this call,
	// ever.
	FinishScan(ctx context.Context, acct AccountID, box MailboxID, scan ScanID, terminal Cursor) (int, error)
}

// FinalScanPageStore atomically stages the terminal page and completes its
// scan. Committing a terminal cursor with ApplyScanPage and then calling
// FinishScan separately leaves a crash window: the provider's incremental
// response at that cursor need not repeat the enumeration's Complete flag.
// PgStore implements this extension so a failed final transaction leaves the
// previous continuation retryable, while a committed one has already pruned
// absences, published the cursor, and recorded the generation's completion.
//
// This is additive to ScanStore so existing external implementations keep
// their non-destructive staged path. They must implement this extension to
// get atomic terminal-page completion as well.
type FinalScanPageStore interface {
	ApplyFinalScanPage(ctx context.Context, scan ScanID, envs []Envelope, seen, destroyed []MessageID, terminal Cursor) (int, error)
}

// NewScanID mints a scan identifier. It is generated client-side so the
// store stays portable across PostgreSQL and Nucleus, neither of which
// shares a UUID function.
func NewScanID() ScanID {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ScanID("scan-" + hex.EncodeToString([]byte(time.Now().String()))[:16])
	}
	// Marking the version nibble keeps the hex unambiguous in logs.
	b[6] = (b[6] & 0x0f) | 0x40
	return ScanID("scan-" + hex.EncodeToString(b[:]))
}

// AccountLockKey derives the advisory-lock key for one account's mirror
// maintenance (audit SYNC-04). Every transaction that writes or deletes
// mirror rows — engine sync writeback, scan staging, product retention,
// account deletion — takes pg_advisory_xact_lock(AccountLockKey(acct))
// as its first statement. One lock per transaction, always acquired
// first: that is the entire lock order, and it is why the order cannot
// deadlock. The engine pool and the product pool compute the same key,
// which is what serializes writers the in-process gates cannot see.
func AccountLockKey(acct AccountID) int64 {
	h := fnv.New64a()
	h.Write([]byte("lullmail-account-maintenance:"))
	h.Write([]byte(acct))
	return int64(h.Sum64())
}
