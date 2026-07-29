// Package mail is a provider-independent model of a user's mailbox.
//
// The engine never receives mail. Messages live at the user's existing
// provider — Gmail, Microsoft 365, Fastmail, any IMAP host — and this package
// connects to that account and mirrors it, the way a desktop mail client does.
// Nothing here provisions addresses or accepts inbound SMTP.
//
// The model is deliberately JMAP-shaped. Of the four protocols the adapters
// speak, JMAP is the only one with stable message identity, so it is the only
// one whose model the others can be projected onto without loss. IMAP in
// particular has no stable identity — UIDs are per-mailbox, a move is a
// delete-and-append, and a UIDVALIDITY change invalidates every UID at once —
// so modelling on IMAP would bake that damage into every other provider.
//
// The local copy is derived, never authoritative. The provider is the source
// of truth and the store must be droppable and rebuildable from zero; that
// invariant is testable in a way that "stateless" is not.
package mail

import "time"

// Provider names the protocol an account is reached over.
type Provider string

const (
	ProviderIMAP  Provider = "imap"
	ProviderJMAP  Provider = "jmap"
	ProviderGmail Provider = "gmail"
	ProviderGraph Provider = "graph"
)

// AccountID identifies a connected account within this engine.
type AccountID string

// MailboxID identifies a mailbox within one account.
type MailboxID string

// MessageID is a stable identity for a message within one account.
//
// Stability is the whole point: it must survive a move between mailboxes, a
// flag change, and a UIDVALIDITY reset. See ResolveMessageID for how each
// provider's identity is derived.
type MessageID string

// ThreadID groups messages into a conversation within one account.
type ThreadID string

// Fingerprint is a content identity, stable across accounts.
//
// Distinct from MessageID: the same message delivered to two connected
// accounts is two MessageIDs (each account mirrors it separately) but one
// Fingerprint, which is what a unified inbox needs in order to show it once.
type Fingerprint string

// Role is a mailbox's well-known purpose, normalised across providers.
//
// Providers disagree on how to express this — IMAP uses SPECIAL-USE
// attributes, Gmail uses reserved label IDs, JMAP has a role property — so it
// is resolved by the adapter rather than inferred from the name, which is
// localised and unreliable.
type Role string

const (
	RoleNone    Role = ""
	RoleInbox   Role = "inbox"
	RoleArchive Role = "archive"
	RoleSent    Role = "sent"
	RoleDrafts  Role = "drafts"
	RoleTrash   Role = "trash"
	RoleJunk    Role = "junk"
	RoleAll     Role = "all"
)

// Mailbox is a folder or label.
type Mailbox struct {
	ID       MailboxID
	Name     string
	Role     Role
	ParentID MailboxID

	// Native is the provider's own handle for this mailbox: an IMAP folder
	// path, a Gmail label ID, a JMAP mailbox ID. Adapters need it to issue
	// commands; nothing above the adapter layer should interpret it.
	Native string
}

// Address is one RFC 5322 mailbox — a display name and an addr-spec.
type Address struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email"`
}

// Keywords are a message's flags.
//
// The four booleans are the ones every provider models. Custom carries
// anything else — IMAP keywords, Gmail labels that are not mailboxes, JMAP
// keywords — so provider-specific state survives a round trip.
type Keywords struct {
	Seen     bool     `json:"seen"`
	Flagged  bool     `json:"flagged"`
	Draft    bool     `json:"draft"`
	Answered bool     `json:"answered"`
	Custom   []string `json:"custom,omitempty"`
}

// Envelope is everything about a message except its body.
//
// Envelopes sync eagerly and bodies do not: envelope-only for a 100k-message
// account is tens of megabytes, while bodies and attachments are what make a
// local mirror unbounded.
type Envelope struct {
	ID         MessageID
	ThreadID   ThreadID
	MailboxIDs []MailboxID

	From    []Address
	To      []Address
	Cc      []Address
	Bcc     []Address
	ReplyTo []Address

	Subject    string
	SentAt     time.Time
	ReceivedAt time.Time

	Keywords      Keywords
	HasAttachment bool
	Size          int64
	Preview       string

	// RFC 5322 threading headers, retained because they are the identity
	// fallback for providers that expose no stable ID, and the only way to
	// reconstruct threads on providers that do not thread server-side.
	MessageIDHeader string
	InReplyTo       []string
	References      []string

	// Fingerprint is the cross-account content identity. See Fingerprint.
	Fingerprint Fingerprint
}

// BodyPart describes one MIME part without carrying its content.
type BodyPart struct {
	PartID      string `json:"part_id"`
	Type        string `json:"type"`
	Charset     string `json:"charset,omitempty"`
	Disposition string `json:"disposition,omitempty"`
	Filename    string `json:"filename,omitempty"`
	Size        int64  `json:"size"`
	ContentID   string `json:"content_id,omitempty"`
}

// IsAttachment reports whether a part should be surfaced as an attachment
// rather than rendered inline.
//
// A part counts as an attachment when it says so, or when it has a filename
// and is not referenced inline by a Content-ID — the common shape for mail
// that omits Content-Disposition entirely.
func (p BodyPart) IsAttachment() bool {
	switch p.Disposition {
	case "attachment":
		return true
	case "inline":
		return false
	}
	return p.Filename != "" && p.ContentID == ""
}

// Body is a message's rendered content and part tree.
type Body struct {
	MessageID MessageID
	Text      string
	HTML      string
	Parts     []BodyPart
}

// Attachments returns the parts that should be surfaced to a user.
func (b *Body) Attachments() []BodyPart {
	var out []BodyPart
	for _, p := range b.Parts {
		if p.IsAttachment() {
			out = append(out, p)
		}
	}
	return out
}

// Account is a connected mailbox and the credentials that reach it.
type Account struct {
	ID       AccountID
	Provider Provider
	Email    string
	Name     string

	// NeedsReauth is set when the provider has rejected the stored
	// credential permanently — a revoked OAuth grant, a changed password.
	// Syncing is pointless until the user reconnects, so the engine stops
	// rather than retrying a credential that can never succeed.
	NeedsReauth bool
}
