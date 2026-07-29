package imap

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	netmail "net/mail"
	"strconv"
	"strings"
	"time"

	"github.com/jhillyerd/enmime/v2"
	"github.com/neutron-build/neutron/mail"
)

// Adapter implements mail.Adapter over an IMAP session.
type Adapter struct {
	conn  *Conn
	boxes map[mail.MailboxID]string // canonical id -> native folder name
}

// New wraps an authenticated connection.
func New(conn *Conn) *Adapter {
	return &Adapter{conn: conn, boxes: map[mail.MailboxID]string{}}
}

func (a *Adapter) Provider() mail.Provider { return mail.ProviderIMAP }
func (a *Adapter) Close() error            { return a.conn.Close() }

// cursor is the IMAP sync position.
//
// UIDValidity is carried because it is the invalidation signal: when the
// server reports a different value, every UID the mirror holds is
// meaningless and the mailbox has to be refetched.
type cursor struct {
	UIDValidity uint32 `json:"uidvalidity"`
	ModSeq      uint64 `json:"modseq,omitempty"`
	UIDNext     uint32 `json:"uidnext,omitempty"`
}

func (c cursor) encode() mail.Cursor {
	b, _ := json.Marshal(c)
	return mail.Cursor(b)
}

func decodeCursor(c mail.Cursor) (cursor, bool) {
	if c == "" {
		return cursor{}, false
	}
	var out cursor
	if err := json.Unmarshal([]byte(c), &out); err != nil {
		return cursor{}, false
	}
	return out, true
}

// Mailboxes lists selectable mailboxes.
func (a *Adapter) Mailboxes(ctx context.Context) ([]mail.Mailbox, error) {
	entries, err := a.conn.List(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]mail.Mailbox, 0, len(entries))
	for _, e := range entries {
		if !e.Selectable() {
			continue
		}
		id := mail.MailboxID(e.Name)
		a.boxes[id] = e.Name
		out = append(out, mail.Mailbox{
			ID:     id,
			Name:   e.Name,
			Role:   e.Role(),
			Native: e.Name,
		})
	}
	return out, nil
}

func (a *Adapter) native(box mail.MailboxID) string {
	if n, ok := a.boxes[box]; ok {
		return n
	}
	// A mailbox the adapter has not listed this session still has a usable
	// name: the canonical id is the folder path.
	return string(box)
}

// Sync returns changes since cur.
//
// Three paths, in descending order of what the server supports:
//
//	QRESYNC    changed messages plus VANISHED — the only path that reports
//	           deletions directly
//	CONDSTORE  changed messages via CHANGEDSINCE; deletions need the
//	           complete-listing fallback
//	neither    a full enumeration every time
//
// The fallback is not a corner case. Most IMAP servers in the wild advertise
// neither extension, so the expensive path is the common one and is written
// to be correct rather than fast.
func (a *Adapter) Sync(ctx context.Context, box mail.MailboxID, cur mail.Cursor) (*mail.Changes, error) {
	uidValidity, highestMod, err := a.conn.Select(ctx, a.native(box), true)
	if err != nil {
		return nil, err
	}

	prev, hadCursor := decodeCursor(cur)
	next := cursor{UIDValidity: uidValidity, ModSeq: highestMod}

	// The invalidation signal. Everything keyed on a UID from the previous
	// generation is now meaningless.
	if hadCursor && prev.UIDValidity != uidValidity {
		return &mail.Changes{Reset: true, Next: next.encode()}, nil
	}

	if !hadCursor {
		changes, err := a.fullScan(ctx, box)
		if err != nil {
			return nil, err
		}
		changes.Next = next.encode()
		return changes, nil
	}

	if a.conn.Supports("CONDSTORE") && prev.ModSeq > 0 {
		changes, err := a.incremental(ctx, box, prev)
		if err != nil {
			return nil, err
		}
		changes.Next = next.encode()
		return changes, nil
	}

	changes, err := a.fullScan(ctx, box)
	if err != nil {
		return nil, err
	}
	changes.Next = next.encode()
	return changes, nil
}

// fullScan enumerates the mailbox.
//
// Complete is set so the engine knows the listing is authoritative and can
// delete anything it holds that is absent here — the only way to observe a
// deletion on a server with no QRESYNC.
func (a *Adapter) fullScan(ctx context.Context, box mail.MailboxID) (*mail.Changes, error) {
	envs, err := a.fetchRange(ctx, box, "1:*", "")
	if err != nil {
		return nil, err
	}

	changes := &mail.Changes{Complete: true}
	for i := range envs {
		e := envs[i]
		changes.Changes = append(changes.Changes, mail.Change{
			Kind: mail.ChangeCreated, ID: e.ID, Envelope: &e,
		})
	}
	return changes, nil
}

// incremental fetches only messages modified since the stored MODSEQ.
func (a *Adapter) incremental(ctx context.Context, box mail.MailboxID, prev cursor) (*mail.Changes, error) {
	modifier := fmt.Sprintf(" (CHANGEDSINCE %d", prev.ModSeq)
	if a.conn.Supports("QRESYNC") {
		modifier += " VANISHED"
	}
	modifier += ")"

	envs, vanished, err := a.fetchRangeWithVanished(ctx, box, "1:*", modifier)
	if err != nil {
		return nil, err
	}

	changes := &mail.Changes{}
	for i := range envs {
		e := envs[i]
		changes.Changes = append(changes.Changes, mail.Change{
			Kind: mail.ChangeUpdated, ID: e.ID, Envelope: &e,
		})
	}
	for _, id := range vanished {
		changes.Changes = append(changes.Changes, mail.Change{
			Kind: mail.ChangeDestroyed, ID: id,
		})
	}

	// Without QRESYNC, CHANGEDSINCE reports modifications but never
	// deletions, so this page cannot be treated as authoritative.
	changes.Complete = false
	return changes, nil
}

func (a *Adapter) fetchRange(ctx context.Context, box mail.MailboxID, set, modifier string) ([]mail.Envelope, error) {
	envs, _, err := a.fetchRangeWithVanished(ctx, box, set, modifier)
	return envs, err
}

const fetchItems = "(UID FLAGS INTERNALDATE RFC822.SIZE ENVELOPE BODYSTRUCTURE)"

func (a *Adapter) fetchRangeWithVanished(ctx context.Context, box mail.MailboxID, set, modifier string) ([]mail.Envelope, []mail.MessageID, error) {
	resp, err := a.conn.exec(ctx, "UID FETCH %s %s%s", set, fetchItems, modifier)
	if err != nil {
		return nil, nil, err
	}

	var (
		envs     []mail.Envelope
		vanished []mail.MessageID
	)
	for _, line := range resp {
		if len(line) < 2 {
			continue
		}

		// * VANISHED (EARLIER) 41,43:116
		if line[1].atomEq("VANISHED") {
			for _, t := range line[2:] {
				if t.kind != tokenAtom {
					continue
				}
				for _, uid := range expandUIDSet(t.text) {
					vanished = append(vanished, positionalID(box, a.conn.uidValidity, uid))
				}
			}
			continue
		}

		if len(line) < 4 || !line[2].atomEq("FETCH") {
			continue
		}
		env, ok := a.parseFetch(box, line[3])
		if ok {
			envs = append(envs, env)
		}
	}
	return envs, vanished, nil
}

// parseFetch turns one FETCH item list into an envelope.
func (a *Adapter) parseFetch(box mail.MailboxID, items token) (mail.Envelope, bool) {
	uidTok, ok := items.find("UID")
	if !ok {
		return mail.Envelope{}, false
	}
	uid64, ok := uidTok.int()
	if !ok {
		return mail.Envelope{}, false
	}
	uid := uint32(uid64)

	var env mail.Envelope
	env.MailboxIDs = []mail.MailboxID{box}

	if sz, ok := items.find("RFC822.SIZE"); ok {
		if n, ok := sz.int(); ok {
			env.Size = n
		}
	}
	if flags, ok := items.find("FLAGS"); ok {
		env.Keywords = parseFlags(flags)
	}
	if internal, ok := items.find("INTERNALDATE"); ok {
		if t, err := time.Parse("02-Jan-2006 15:04:05 -0700", internal.text); err == nil {
			env.ReceivedAt = t
		}
	}
	if bs, ok := items.find("BODYSTRUCTURE"); ok {
		env.HasAttachment = bodyStructureHasAttachment(bs)
	}

	if e, ok := items.find("ENVELOPE"); ok {
		applyEnvelope(&env, e)
	}

	// Identity is chosen by what the server actually gave us. A
	// Message-ID header yields an identity that survives a UIDVALIDITY
	// reset; without one there is nothing stable to key on and the
	// positional form is the honest fallback.
	if env.MessageIDHeader != "" {
		env.ID = mail.HeaderMessageID(env.MessageIDHeader)
	} else {
		env.ID = positionalID(box, a.conn.uidValidity, uid)
	}
	env.Fingerprint = mail.ComputeFingerprint(&env)
	env.ThreadID = mail.ThreadID(mail.ThreadKey(&env))

	return env, true
}

func positionalID(box mail.MailboxID, uidValidity, uid uint32) mail.MessageID {
	return mail.PositionalMessageID(box, uidValidity, uid)
}

// applyEnvelope reads IMAP's ENVELOPE structure, whose field order is fixed
// by the protocol: date, subject, from, sender, reply-to, to, cc, bcc,
// in-reply-to, message-id.
func applyEnvelope(env *mail.Envelope, e token) {
	if e.kind != tokenList || len(e.list) < 10 {
		return
	}
	if !e.list[0].isNil() {
		if t, err := netmail.ParseDate(e.list[0].text); err == nil {
			env.SentAt = t
		}
	}
	if !e.list[1].isNil() {
		env.Subject = decodeWord(e.list[1].text)
	}
	env.From = parseAddressList(e.list[2])
	env.ReplyTo = parseAddressList(e.list[4])
	env.To = parseAddressList(e.list[5])
	env.Cc = parseAddressList(e.list[6])
	env.Bcc = parseAddressList(e.list[7])
	if !e.list[8].isNil() {
		env.InReplyTo = mail.ParseReferences(e.list[8].text)
	}
	if !e.list[9].isNil() {
		env.MessageIDHeader = e.list[9].text
		// ENVELOPE carries no References header. In-Reply-To is the best
		// available parent link, and threading falls back to it.
		if len(env.References) == 0 && len(env.InReplyTo) > 0 {
			env.References = env.InReplyTo
		}
	}
}

// parseAddressList reads IMAP's address structure: a list of
// (name adl mailbox host) quadruples.
func parseAddressList(t token) []mail.Address {
	if t.kind != tokenList {
		return nil
	}
	var out []mail.Address
	for _, a := range t.list {
		if a.kind != tokenList || len(a.list) < 4 {
			continue
		}
		var addr mail.Address
		if !a.list[0].isNil() {
			addr.Name = decodeWord(a.list[0].text)
		}
		local, host := a.list[2], a.list[3]
		if local.isNil() || host.isNil() {
			continue
		}
		addr.Email = local.text + "@" + host.text
		out = append(out, addr)
	}
	return out
}

// decodeWord decodes RFC 2047 encoded-words, which is how non-ASCII subjects
// and display names travel.
func decodeWord(s string) string {
	dec := new(mime.WordDecoder)
	if out, err := dec.DecodeHeader(s); err == nil {
		return out
	}
	return s
}

func parseFlags(t token) mail.Keywords {
	var kw mail.Keywords
	for _, f := range t.list {
		switch strings.ToLower(f.text) {
		case `\seen`:
			kw.Seen = true
		case `\flagged`:
			kw.Flagged = true
		case `\draft`:
			kw.Draft = true
		case `\answered`:
			kw.Answered = true
		case `\deleted`, `\recent`:
			// \Deleted is IMAP's two-phase delete marker and \Recent is
			// session state; neither is a user-visible keyword.
		default:
			if f.text != "" {
				kw.Custom = append(kw.Custom, f.text)
			}
		}
	}
	return kw
}

// bodyStructureHasAttachment walks a BODYSTRUCTURE looking for a part with an
// attachment disposition.
func bodyStructureHasAttachment(t token) bool {
	if t.kind != tokenList {
		return false
	}
	for _, child := range t.list {
		if child.kind == tokenString || child.kind == tokenAtom {
			if strings.EqualFold(child.text, "attachment") {
				return true
			}
		}
		if child.kind == tokenList && bodyStructureHasAttachment(child) {
			return true
		}
	}
	return false
}

// expandUIDSet expands an IMAP sequence set such as "41,43:116".
func expandUIDSet(s string) []uint32 {
	var out []uint32
	for _, part := range strings.Split(s, ",") {
		lo, hi, isRange := strings.Cut(part, ":")
		start, err := strconv.ParseUint(lo, 10, 32)
		if err != nil {
			continue
		}
		if !isRange {
			out = append(out, uint32(start))
			continue
		}
		end, err := strconv.ParseUint(hi, 10, 32)
		if err != nil {
			continue
		}
		// A range may be written in either direction.
		if end < start {
			start, end = end, start
		}
		// Guard against a server reporting an absurd range; expanding
		// "1:*" literally would allocate until the process dies.
		if end-start > 1_000_000 {
			end = start + 1_000_000
		}
		for u := start; u <= end; u++ {
			out = append(out, uint32(u))
		}
	}
	return out
}

// Envelopes refetches envelopes by identity.
//
// IMAP addresses messages by UID, not by the canonical identity, so this
// searches by Message-ID header for header-derived identities. Servers
// without SEARCH support for HEADER fall back to a mailbox scan.
func (a *Adapter) Envelopes(ctx context.Context, ids []mail.MessageID) ([]mail.Envelope, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	want := make(map[mail.MessageID]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}

	envs, err := a.fetchRange(ctx, mail.MailboxID(a.conn.selected), "1:*", "")
	if err != nil {
		return nil, err
	}

	out := make([]mail.Envelope, 0, len(ids))
	for i := range envs {
		if want[envs[i].ID] {
			out = append(out, envs[i])
		}
	}
	return out, nil
}

// Body fetches and parses a message.
func (a *Adapter) Body(ctx context.Context, id mail.MessageID) (*mail.Body, error) {
	rc, err := a.Raw(ctx, id)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	env, err := enmime.ReadEnvelope(rc)
	if err != nil {
		return nil, fmt.Errorf("imap: parse message: %w", err)
	}

	body := &mail.Body{MessageID: id, Text: env.Text, HTML: env.HTML}
	for _, p := range env.Attachments {
		body.Parts = append(body.Parts, mail.BodyPart{
			PartID:      p.PartID,
			Type:        p.ContentType,
			Filename:    p.FileName,
			Disposition: "attachment",
			Size:        int64(len(p.Content)),
			ContentID:   p.ContentID,
		})
	}
	for _, p := range env.Inlines {
		body.Parts = append(body.Parts, mail.BodyPart{
			PartID:      p.PartID,
			Type:        p.ContentType,
			Filename:    p.FileName,
			Disposition: "inline",
			Size:        int64(len(p.Content)),
			ContentID:   p.ContentID,
		})
	}
	return body, nil
}

// Raw returns the full RFC 5322 message.
func (a *Adapter) Raw(ctx context.Context, id mail.MessageID) (io.ReadCloser, error) {
	uid, err := a.uidFor(ctx, id)
	if err != nil {
		return nil, err
	}

	// BODY.PEEK avoids setting \Seen: reading a message in the mirror must
	// not change its state at the provider.
	resp, err := a.conn.exec(ctx, "UID FETCH %d (BODY.PEEK[])", uid)
	if err != nil {
		return nil, err
	}
	for _, line := range resp {
		if len(line) < 4 || !line[2].atomEq("FETCH") {
			continue
		}
		if v, ok := line[3].findPrefix("BODY["); ok {
			return io.NopCloser(bytes.NewReader([]byte(v.text))), nil
		}
	}
	return nil, fmt.Errorf("imap: %w: message %s", mail.ErrNotFound, id)
}

// Attachment streams one decoded part.
func (a *Adapter) Attachment(ctx context.Context, id mail.MessageID, partID string) (io.ReadCloser, error) {
	rc, err := a.Raw(ctx, id)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	env, err := enmime.ReadEnvelope(rc)
	if err != nil {
		return nil, fmt.Errorf("imap: parse message: %w", err)
	}
	for _, p := range append(env.Attachments, env.Inlines...) {
		if p.PartID == partID {
			return io.NopCloser(bytes.NewReader(p.Content)), nil
		}
	}
	return nil, fmt.Errorf("imap: %w: part %s of %s", mail.ErrNotFound, partID, id)
}

// uidFor resolves a canonical identity back to a UID in the selected mailbox.
func (a *Adapter) uidFor(ctx context.Context, id mail.MessageID) (uint32, error) {
	envs, err := a.fetchRange(ctx, mail.MailboxID(a.conn.selected), "1:*", "")
	if err != nil {
		return 0, err
	}
	for i := range envs {
		if envs[i].ID == id {
			// The identity is derived from the same FETCH, so re-deriving
			// the UID means finding which one produced it.
			if uid, ok := a.uidOf(ctx, envs[i]); ok {
				return uid, nil
			}
		}
	}
	return 0, fmt.Errorf("imap: %w: message %s", mail.ErrNotFound, id)
}

// uidOf finds the UID whose envelope matches, by searching on the Message-ID
// header when one exists.
func (a *Adapter) uidOf(ctx context.Context, env mail.Envelope) (uint32, bool) {
	if env.MessageIDHeader == "" {
		return 0, false
	}
	resp, err := a.conn.exec(ctx, `UID SEARCH HEADER Message-ID %s`, quote(env.MessageIDHeader))
	if err != nil {
		return 0, false
	}
	for _, line := range resp {
		if len(line) < 2 || !line[1].atomEq("SEARCH") {
			continue
		}
		for _, t := range line[2:] {
			if n, ok := t.int(); ok {
				return uint32(n), true
			}
		}
	}
	return 0, false
}

// Apply pushes a mutation.
//
// The mailbox is re-selected read-write first. Syncing selects with EXAMINE
// so that reading never sets \Seen as a side effect, and a server refuses
// STORE, COPY, and EXPUNGE against a read-only selection — without this every
// mutation fails with "mailbox selected read only".
func (a *Adapter) Apply(ctx context.Context, op mail.Operation) error {
	if err := a.selectWritable(ctx); err != nil {
		return err
	}

	uids := make([]string, 0, len(op.IDs))
	for _, id := range op.IDs {
		uid, err := a.uidFor(ctx, id)
		if err != nil {
			return err
		}
		uids = append(uids, strconv.FormatUint(uint64(uid), 10))
	}
	if len(uids) == 0 {
		return nil
	}
	set := strings.Join(uids, ",")

	switch op.Kind {
	case mail.OpAddKeyword:
		_, err := a.conn.exec(ctx, "UID STORE %s +FLAGS (%s)", set, imapFlag(op.Keyword))
		return err
	case mail.OpRemoveKeyword:
		_, err := a.conn.exec(ctx, "UID STORE %s -FLAGS (%s)", set, imapFlag(op.Keyword))
		return err
	case mail.OpMove:
		if a.conn.Supports("MOVE") {
			_, err := a.conn.exec(ctx, "UID MOVE %s %s", set, quote(a.native(op.Target)))
			return err
		}
		// Without the MOVE extension a move is copy, mark deleted, expunge.
		if _, err := a.conn.exec(ctx, "UID COPY %s %s", set, quote(a.native(op.Target))); err != nil {
			return err
		}
		if _, err := a.conn.exec(ctx, `UID STORE %s +FLAGS (\Deleted)`, set); err != nil {
			return err
		}
		_, err := a.conn.exec(ctx, "UID EXPUNGE %s", set)
		return err
	case mail.OpDelete:
		if _, err := a.conn.exec(ctx, `UID STORE %s +FLAGS (\Deleted)`, set); err != nil {
			return err
		}
		_, err := a.conn.exec(ctx, "UID EXPUNGE %s", set)
		return err
	default:
		return fmt.Errorf("imap: unsupported operation %d", op.Kind)
	}
}

// selectWritable re-opens the current mailbox read-write if it was opened
// read-only, leaving it selected for the caller's commands.
func (a *Adapter) selectWritable(ctx context.Context) error {
	if a.conn.selected == "" {
		return fmt.Errorf("imap: no mailbox selected")
	}
	if !a.conn.selectedReadOnly {
		return nil
	}
	_, _, err := a.conn.Select(ctx, a.conn.selected, false)
	return err
}

func imapFlag(keyword string) string {
	switch strings.ToLower(keyword) {
	case "seen":
		return `\Seen`
	case "flagged":
		return `\Flagged`
	case "answered":
		return `\Answered`
	case "draft":
		return `\Draft`
	default:
		return keyword
	}
}

// encodeXOAuth2 builds the SASL XOAUTH2 initial response.
func encodeXOAuth2(user, token string) string {
	s := "user=" + user + "\x01auth=Bearer " + token + "\x01\x01"
	return base64.StdEncoding.EncodeToString([]byte(s))
}

var _ mail.Adapter = (*Adapter)(nil)
