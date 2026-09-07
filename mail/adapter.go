package mail

import (
	"context"
	"errors"
	"io"
)

// Cursor is an opaque, provider-defined sync position.
//
// The sync engine stores it and hands it back verbatim; only the adapter that
// produced it may interpret it. Behind it sits whatever that provider offers:
// a Gmail historyId, a Graph deltaLink, a JMAP state string, or an IMAP
// UIDVALIDITY/MODSEQ pair. Keeping it opaque is what lets one sync engine
// drive four protocols that agree on nothing else.
type Cursor string

// ChangeKind distinguishes the three things that can happen to a message.
type ChangeKind int

const (
	// ChangeCreated is a message newly visible in this mailbox. It may be
	// newly delivered, or moved in from elsewhere — providers rarely
	// distinguish the two, and for a mirror it does not matter.
	ChangeCreated ChangeKind = iota

	// ChangeUpdated is a flag or mailbox-membership change.
	ChangeUpdated

	// ChangeDestroyed is a message no longer visible in this mailbox.
	// Deleted and moved-away are indistinguishable at this layer for the
	// same reason as ChangeCreated.
	ChangeDestroyed
)

// Change is one delta reported by a provider.
type Change struct {
	Kind ChangeKind
	ID   MessageID

	// Envelope is populated for ChangeCreated and ChangeUpdated when the
	// provider returned it as part of the delta. When nil the sync engine
	// fetches it separately — Gmail's history API, for example, returns
	// bare message IDs.
	Envelope *Envelope
}

// Changes is one page of deltas.
type Changes struct {
	Changes []Change
	Next    Cursor

	// EnumerationStart marks the first page of an authoritative full
	// listing. Complete marks its final page. Keeping both boundaries lets
	// the engine avoid sweeping when MaxPages splits an enumeration across
	// two scheduled runs and the earlier pages' seen-set is no longer in
	// memory.
	EnumerationStart bool

	// More reports that the provider has further pages. The caller should
	// sync again with Next rather than waiting for the next poll.
	More bool

	// Complete reports that this is the final page of a full enumeration, so
	// anything the store holds for it and this listing omits has been
	// deleted at the provider.
	//
	// This is how a deletion is observed at all on the many IMAP servers
	// that advertise neither QRESYNC nor CONDSTORE: they will report which
	// messages exist but never which stopped existing. Providers with a
	// real change feed leave this false and report deletions directly.
	//
	// It applies across a paginated run: only the final page's Complete
	// closes the enumeration, because a mid-run listing is by definition
	// partial.
	Complete bool

	// Reset reports that the cursor is no longer usable and the mailbox
	// must be resynced from empty.
	//
	// Every provider has this failure mode under a different name — an IMAP
	// UIDVALIDITY change, a Gmail historyId aged out of the retention
	// window, an expired Graph delta token, a JMAP cannotCalculateChanges
	// error. Normalising them into one flag is what keeps the recovery path
	// single and therefore testable. When Reset is set, Changes is empty and
	// Next is the cursor to begin a full resync from.
	Reset bool
}

// OpKind is a mutation to apply at the provider.
type OpKind int

const (
	OpAddKeyword OpKind = iota
	OpRemoveKeyword
	OpMove
	OpDelete
)

// Operation is a change to push back to the provider.
//
// Mutations go to the provider first and are reflected locally only once it
// accepts them. The alternative — optimistic local writes — would make the
// local copy authoritative for a moment, which is exactly the property this
// design refuses.
type Operation struct {
	Kind    OpKind
	IDs     []MessageID
	Keyword string    // for OpAddKeyword / OpRemoveKeyword
	Target  MailboxID // for OpMove
}

// Adapter is one provider's implementation of the mailbox model.
//
// Implementations are not required to be safe for concurrent use; the sync
// engine serialises calls per account.
type Adapter interface {
	// Provider names the protocol this adapter speaks.
	Provider() Provider

	// Mailboxes lists every mailbox in the account.
	Mailboxes(ctx context.Context) ([]Mailbox, error)

	// Sync returns changes since cur. An empty cur means "from the
	// beginning" and yields the whole mailbox as ChangeCreated.
	Sync(ctx context.Context, mailbox MailboxID, cur Cursor) (*Changes, error)

	// Envelopes fetches envelopes by ID, for deltas that carried only IDs.
	Envelopes(ctx context.Context, ids []MessageID) ([]Envelope, error)

	// Body fetches and parses a message's renderable content.
	Body(ctx context.Context, id MessageID) (*Body, error)

	// Raw returns the full RFC 5322 message. Callers must close it.
	Raw(ctx context.Context, id MessageID) (io.ReadCloser, error)

	// Attachment streams one part's decoded content. Callers must close it.
	Attachment(ctx context.Context, id MessageID, partID string) (io.ReadCloser, error)

	// Apply pushes a mutation to the provider.
	Apply(ctx context.Context, op Operation) error

	// Close releases connections held by the adapter.
	Close() error
}

// MailboxSelector is implemented by adapters whose message reads only work
// against a currently selected mailbox (IMAP). Mid-sync one is selected; on a
// freshly dialed connection nothing is, and strict servers answer UID FETCH
// with "command not valid in this state". The engine calls SelectMailbox with
// the mailbox the store says holds the message before Body, and consumers
// reaching Raw or Attachment directly should call Engine.Locate first.
// Adapters with account-global message IDs (Gmail, Graph, JMAP) do not
// implement it and are never asked.
type MailboxSelector interface {
	SelectMailbox(ctx context.Context, box MailboxID) error
}

// Errors that the sync engine treats specially. Adapters should wrap these
// rather than inventing equivalents, because the engine's recovery path
// branches on them.
var (
	// ErrReauthRequired means the stored credential is permanently rejected
	// and no retry will succeed. The engine marks the account and stops.
	ErrReauthRequired = errors.New("mail: account must re-authenticate")

	// ErrCursorInvalid means the provider rejected the cursor. Adapters
	// should normally report this as Changes.Reset instead; the error form
	// exists for providers that surface it outside a sync response.
	ErrCursorInvalid = errors.New("mail: sync cursor no longer valid")

	// ErrRateLimited means the provider is throttling. The engine backs off
	// rather than treating it as a failure.
	ErrRateLimited = errors.New("mail: provider rate limited")

	// ErrNotFound means the message or part is gone at the provider.
	ErrNotFound = errors.New("mail: not found")
)
