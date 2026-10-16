package mail

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Identity hooks: the engine-side half of the identity contract.
//
// A host product keeps rows that reference mirror identities — threading,
// filing decisions, reply parents. When the engine replaces one identity
// with another (an identity upgrade) or destroys one outright (a message
// leaving its last mailbox), those product rows must move or snapshot in
// the SAME unit of work, or the product is left pointing at an identity
// the mirror no longer holds. The engine owns the connection, so the
// product's callbacks run on the engine's transaction through HookTx.
// The product imports the engine; never the reverse.
//
// Both capabilities below are optional store interfaces (the
// FinalScanPageStore pattern, scan.go), never additions to Store —
// consumers implement Store by design and must keep compiling. PgStore,
// the only implementation production runs, implements both; the engine
// probes for them and falls back to the plain paths when absent or when
// no hooks are supplied.
//
// Prohibitions on any callback running on a HookTx (enforced by review;
// see the identity contract): a second connection self-deadlocks on the
// account advisory lock the engine already holds. Inside a callback:
//
//   - no new connections, no engine-store calls;
//   - no additional advisory locks; no DDL;
//   - only the caller's own (product) tables.

// HookRow is the single-row result of a hook query.
type HookRow interface{ Scan(dest ...any) error }

// HookRows is the multi-row result of a hook query.
type HookRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// HookTx executes caller SQL on THIS transaction's connection. Given to
// product callbacks so their writes commit with the engine's unit of work.
type HookTx interface {
	Exec(ctx context.Context, query string, args ...any) error
	QueryRow(ctx context.Context, query string, args ...any) HookRow
	Query(ctx context.Context, query string, args ...any) (HookRows, error)
}

// IdentityPair is one old→new identity replacement.
type IdentityPair struct{ OldID, NewID MessageID }

// IdentityHooks are product callbacks run inside engine transactions,
// after the account advisory lock, before any old identity is retired.
type IdentityHooks interface {
	// Admit fences the unit on the owner rows (row locks are legal here;
	// the advisory lock is already held — never take it again).
	AdmitIdentityWrite(ctx context.Context, tx HookTx, acct AccountID) error

	// Remap moves retained product references for every pair.
	RemapIdentity(ctx context.Context, tx HookTx, acct AccountID, pairs []IdentityPair) error

	// ReferencesDropped reports identities about to be deleted with no
	// known replacement. The product snapshots; it must not resurrect.
	ReferencesDropped(ctx context.Context, tx HookTx, acct AccountID, ids []MessageID) error
}

// IdentityReplacement is one atomic identity replacement.
type IdentityReplacement struct {
	OldID MessageID

	// Envelope is the new identity; written FIRST.
	Envelope Envelope

	// Body, when non-nil, is copied inside the transaction.
	Body *Body

	// An empty OldID writes the envelope and retires nothing: the sync
	// engine uses that form to carry a page's ordinary (unpaired) upserts
	// into the same transaction as its promotions. A self-replacement
	// (OldID equal to the envelope's own identity) degrades the same way
	// to a plain upsert. OldID is otherwise deleted with its body once
	// the new identity and any remap have landed; the delete is a no-op
	// when the old row is already absent, which is what makes a retried
	// unit safe.
}

// IdentityReplaceStore is the optional atomic-replacement capability.
type IdentityReplaceStore interface {
	// ReplaceIdentities, in ONE transaction (advisory lock first):
	// AdmitIdentityWrite → write every replacement envelope + membership
	// (+ body copy) → RemapIdentity(all pairs) → retire old identities.
	// Any failure rolls back everything; an interrupted replacement must
	// leave the old record readable and the unit retryable (idempotent:
	// envelope upserts, old-id deletes are no-ops when absent).
	ReplaceIdentities(ctx context.Context, acct AccountID, reps []IdentityReplacement, hooks IdentityHooks) error
}

// ReferenceAwareStore is the optional destroy-with-references capability.
type ReferenceAwareStore interface {
	// RemoveFromMailboxReferenced behaves as RemoveFromMailbox but calls
	// ReferencesDropped inside the transaction (advisory first), before
	// deletion, with every id that will lose its LAST membership and be
	// deleted with its body. Callers fall back to RemoveFromMailbox when
	// the capability is absent.
	RemoveFromMailboxReferenced(ctx context.Context, acct AccountID, box MailboxID, ids []MessageID, hooks IdentityHooks) error
}

// pgxHookTx adapts a pgx transaction to HookTx. pgx.Row and pgx.Rows
// already satisfy HookRow and HookRows, so the adapter only narrows the
// return types (Exec drops its command tag). It is the single bridge
// between the engine's transaction and product callbacks, and it exposes
// nothing but the caller's own SQL on the already-open connection.
type pgxHookTx struct{ tx pgx.Tx }

func (h pgxHookTx) Exec(ctx context.Context, query string, args ...any) error {
	_, err := h.tx.Exec(ctx, query, args...)
	return err
}

func (h pgxHookTx) QueryRow(ctx context.Context, query string, args ...any) HookRow {
	return h.tx.QueryRow(ctx, query, args...)
}

func (h pgxHookTx) Query(ctx context.Context, query string, args ...any) (HookRows, error) {
	return h.tx.Query(ctx, query, args...)
}
