package mail

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// Message identity is the hardest correctness problem in this package, so the
// rules are stated once, here.
//
// A MessageID must survive three things: a move between mailboxes, a flag
// change, and a UIDVALIDITY reset. Providers vary in how much help they give:
//
//	Gmail    X-GM-MSGID / message id   stable across labels        native
//	Graph    message id                stable across folders       native
//	JMAP     emailId                   stable by specification     native
//	IMAP     OBJECTID (RFC 8474)       stable when advertised      native
//	IMAP     Message-ID header         stable, usually present     header
//	IMAP     UIDVALIDITY + UID         unstable by construction    positional
//
// Each identity carries a one-letter prefix naming its source. That keeps the
// three schemes from colliding, and lets the sync engine notice when a
// message's identity can be upgraded to a better source — a positional ID
// becomes a header ID as soon as the header is fetched.

const (
	sourceNative     = "n"
	sourcePositional = "p"
	sourceHeader     = "h"
)

// digest returns a 128-bit base64url digest of parts, joined with a separator
// that cannot appear in the inputs unescaped.
func digest(parts ...string) string {
	h := sha256.New()
	for i, p := range parts {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write([]byte(p))
	}
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil)[:16])
}

// NativeMessageID wraps a provider's own stable identifier.
//
// Use this whenever the provider has one. It is the only identity that
// survives without heuristics.
func NativeMessageID(p Provider, native string) MessageID {
	return MessageID(sourceNative + ":" + string(p) + ":" + native)
}

// HeaderMessageID derives an identity from the RFC 5322 Message-ID header.
//
// This is the fallback for IMAP servers that advertise neither OBJECTID nor
// Gmail's extensions, which is most of them. It is stable across mailboxes
// and across UIDVALIDITY resets because it travels with the message.
//
// It is not perfect: a message may carry no Message-ID, and a broken sender
// may reuse one across genuinely different messages. Both are rare, and both
// degrade to a duplicate or a merge rather than to data loss.
func HeaderMessageID(header string) MessageID {
	return MessageID(sourceHeader + ":" + digest(NormalizeMessageIDHeader(header)))
}

// PositionalMessageID is the last-resort identity, derived from an IMAP
// message's position rather than its content.
//
// It is unstable by construction: a UIDVALIDITY change invalidates it, and a
// move to another mailbox produces a different one for the same message. It
// exists only so that a message with no Message-ID header on a server with no
// object IDs can still be stored. Callers should upgrade away from it via
// UpgradeIdentity as soon as a better source is known.
func PositionalMessageID(mailbox MailboxID, uidValidity, uid uint32) MessageID {
	return MessageID(sourcePositional + ":" + digest(
		string(mailbox),
		strconv.FormatUint(uint64(uidValidity), 10),
		strconv.FormatUint(uint64(uid), 10),
	))
}

// IsPositional reports whether id is the unstable last-resort form.
//
// Positional identities are the only ones that a UIDVALIDITY reset can
// invalidate, so the resync path checks this to decide what it may keep.
func IsPositional(id MessageID) bool {
	return strings.HasPrefix(string(id), sourcePositional+":")
}

// IsNative reports whether id came from a provider's own stable identifier.
func IsNative(id MessageID) bool {
	return strings.HasPrefix(string(id), sourceNative+":")
}

// UpgradeIdentity returns the best identity available for a message.
//
// Identity sources are ordered native > header > positional. A message first
// seen positionally — because only its UID was known — becomes header-identified
// once its Message-ID has been fetched. Returning the stronger identity lets
// the store migrate the row instead of accumulating a duplicate after the
// next UIDVALIDITY reset.
//
// ok reports whether the identity changed.
func UpgradeIdentity(current MessageID, header string) (id MessageID, ok bool) {
	if IsNative(current) {
		return current, false
	}
	if NormalizeMessageIDHeader(header) == "" {
		return current, false
	}
	upgraded := HeaderMessageID(header)
	if upgraded == current {
		return current, false
	}
	return upgraded, true
}

// NormalizeMessageIDHeader canonicalises a Message-ID header value.
//
// It trims surrounding whitespace and the angle brackets that RFC 5322
// requires but that some parsers strip and others do not. Case is left alone
// deliberately: the addr-spec local part is case-sensitive per the RFC, and
// generators do emit case-significant identifiers, so lowercasing would merge
// messages that are genuinely distinct.
//
// It returns "" for a header that is absent or contains nothing usable.
func NormalizeMessageIDHeader(header string) string {
	s := strings.TrimSpace(header)
	s = strings.TrimPrefix(s, "<")
	s = strings.TrimSuffix(s, ">")
	return strings.TrimSpace(s)
}

// ComputeFingerprint derives a content identity that is stable across
// accounts, for deduplicating a unified inbox.
//
// Distinct from MessageID, which is per-account: one message delivered to two
// connected accounts has two MessageIDs and one Fingerprint. A unified inbox
// that keyed on MessageID would show it twice.
//
// When a Message-ID header exists it is the fingerprint, since that is
// precisely what it is for. Otherwise this falls back to a hash of the fields
// a mail system does not rewrite in transit — send time, sender, subject, and
// size. That is a heuristic; it can merge two genuinely different messages
// sent in the same second by the same sender with the same subject and size.
// The failure mode is a missing duplicate in a unified view, never data loss,
// because each account still holds its own copy under its own MessageID.
func ComputeFingerprint(env *Envelope) Fingerprint {
	if h := NormalizeMessageIDHeader(env.MessageIDHeader); h != "" {
		return Fingerprint("m:" + digest(h))
	}

	var from string
	if len(env.From) > 0 {
		from = strings.ToLower(env.From[0].Email)
	}
	return Fingerprint("c:" + digest(
		strconv.FormatInt(env.SentAt.UTC().Unix(), 10),
		from,
		env.Subject,
		strconv.FormatInt(env.Size, 10),
	))
}

// ParseReferences splits a References or In-Reply-To header into normalised
// message identifiers.
//
// Both headers are defined as a sequence of msg-ids, but real mail separates
// them inconsistently — with spaces, commas, or nothing at all — so this scans
// for bracketed groups rather than splitting on a delimiter. A header with no
// brackets at all is treated as a single identifier, which is what broken
// senders that omit them intend.
func ParseReferences(header string) []string {
	var out []string
	rest := header
	for {
		start := strings.Index(rest, "<")
		if start < 0 {
			break
		}
		end := strings.Index(rest[start:], ">")
		if end < 0 {
			break
		}
		if id := NormalizeMessageIDHeader(rest[start : start+end+1]); id != "" {
			out = append(out, id)
		}
		rest = rest[start+end+1:]
	}

	if len(out) == 0 {
		if id := NormalizeMessageIDHeader(header); id != "" {
			out = append(out, id)
		}
	}
	return out
}

// ThreadKey returns the identifier a message should thread under when the
// provider does not thread server-side.
//
// This is the root of the References chain, which is the JWZ algorithm's
// starting point and is correct for every well-formed reply. Mail that
// carries no threading headers threads under its own identity, forming a
// thread of one.
func ThreadKey(env *Envelope) string {
	if len(env.References) > 0 {
		return env.References[0]
	}
	if len(env.InReplyTo) > 0 {
		return env.InReplyTo[0]
	}
	if h := NormalizeMessageIDHeader(env.MessageIDHeader); h != "" {
		return h
	}
	return string(env.ID)
}

// String renders an identity for logs and errors.
func (id MessageID) String() string { return string(id) }

// Short renders a truncated identity for log lines where the full value is
// noise.
func (id MessageID) Short() string {
	s := string(id)
	if len(s) <= 12 {
		return s
	}
	return s[:12] + "…"
}

// Validate reports whether id is well-formed — carrying a known source
// prefix and a non-empty body.
//
// The store rejects malformed identities on write, because an identity that
// does not round-trip is indistinguishable from data loss later.
func (id MessageID) Validate() error {
	s := string(id)
	prefix, rest, found := strings.Cut(s, ":")
	if !found || rest == "" {
		return fmt.Errorf("mail: malformed message id %q", s)
	}
	switch prefix {
	case sourceNative, sourceHeader, sourcePositional:
		return nil
	default:
		return fmt.Errorf("mail: unknown identity source %q in %q", prefix, s)
	}
}
