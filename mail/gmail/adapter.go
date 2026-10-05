// Package gmail implements the mail.Adapter interface over the Gmail API.
//
// The Gmail API rather than IMAP, because it is the only path with a real
// change feed: historyId gives incremental sync, message IDs are stable
// across labels, and threadId means threading needs no reconstruction. IMAP
// against Gmail has none of that.
//
// Scope note: every useful Gmail scope is restricted, which means the OAuth
// client needs verification and an annual CASA assessment before serving
// users beyond the testing cohort.
package gmail

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	netmail "net/mail"
	"strconv"
	"strings"
	"time"

	"github.com/neutron-build/neutron/mail"
	"golang.org/x/text/encoding/htmlindex"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// Adapter is a Gmail client bound to one account.
type Adapter struct {
	svc         *gmail.Service
	reads       *ReadLimiter
	retryJitter func() time.Duration

	// user is always "me" in practice; the API keys off the token.
	user string
}

// New wraps an authenticated Gmail service.
//
// The caller supplies the token source, so refresh, storage, and revocation
// stay outside this package — x/oauth2 already handles refresh correctly and
// reimplementing it here would only add a second thing to get wrong.
func New(ctx context.Context, opts ...option.ClientOption) (*Adapter, error) {
	return NewWithReadLimiter(ctx, NewReadLimiter(), opts...)
}

// NewWithReadLimiter shares pacing across adapters without sharing credentials.
func NewWithReadLimiter(ctx context.Context, limiter *ReadLimiter, opts ...option.ClientOption) (*Adapter, error) {
	if limiter == nil {
		limiter = NewReadLimiter()
	}
	svc, err := gmail.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gmail: new service: %w", err)
	}
	return &Adapter{svc: svc, user: "me", reads: limiter, retryJitter: readJitter}, nil
}

func (a *Adapter) Provider() mail.Provider { return mail.ProviderGmail }
func (a *Adapter) Close() error            { return nil }

// classify maps Google API errors onto the engine's typed errors.
func classify(err error) error {
	if err == nil {
		return nil
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		switch gerr.Code {
		case 401, 403:
			// 403 is ambiguous at Google — it covers both a revoked grant
			// and a quota exhaustion. The reason field disambiguates, and
			// getting this wrong means either retrying forever or asking a
			// user to reconnect an account that was merely throttled.
			for _, e := range gerr.Errors {
				switch e.Reason {
				case "rateLimitExceeded", "userRateLimitExceeded", "quotaExceeded":
					return fmt.Errorf("gmail: %s: %w", e.Reason, mail.ErrRateLimited)
				}
			}
			return fmt.Errorf("gmail: %s: %w", gerr.Message, mail.ErrReauthRequired)
		case 404:
			return fmt.Errorf("gmail: %s: %w", gerr.Message, mail.ErrNotFound)
		case 429:
			return fmt.Errorf("gmail: %s: %w", gerr.Message, mail.ErrRateLimited)
		}
	}
	return err
}

// Mailboxes lists labels. Gmail models folders as labels, and a message can
// carry several at once.
func (a *Adapter) Mailboxes(ctx context.Context) ([]mail.Mailbox, error) {
	res, err := readCall(ctx, a, func() (*gmail.ListLabelsResponse, error) { return a.svc.Users.Labels.List(a.user).Context(ctx).Do() })
	if err != nil {
		return nil, classify(err)
	}

	boxes := make([]mail.Mailbox, 0, len(res.Labels))
	for _, l := range res.Labels {
		boxes = append(boxes, mail.Mailbox{
			ID:     mail.MailboxID(l.Id),
			Name:   l.Name,
			Role:   roleFrom(l.Id),
			Native: l.Id,
		})
	}
	return boxes, nil
}

// roleFrom maps Gmail's reserved label IDs onto the canonical role. The IDs
// are stable and locale-independent; the display names are neither.
func roleFrom(labelID string) mail.Role {
	switch labelID {
	case "INBOX":
		return mail.RoleInbox
	case "SENT":
		return mail.RoleSent
	case "DRAFT":
		return mail.RoleDrafts
	case "TRASH":
		return mail.RoleTrash
	case "SPAM":
		return mail.RoleJunk
	default:
		return mail.RoleNone
	}
}

// Sync returns changes since cur.
//
// The cursor is a historyId. Google retains history for a limited window, so
// a cursor older than that window is rejected with 404 — reported here as a
// reset, which is the same recovery path an IMAP UIDVALIDITY change takes.
func (a *Adapter) Sync(ctx context.Context, box mail.MailboxID, cur mail.Cursor) (*mail.Changes, error) {
	if cur == "" {
		profile, err := readCall(ctx, a, func() (*gmail.Profile, error) { return a.svc.Users.GetProfile(a.user).Context(ctx).Do() })
		if err != nil {
			return nil, classify(err)
		}
		return a.initialSync(ctx, box, "", profile.HistoryId)
	}
	if strings.HasPrefix(string(cur), "gmail-initial:") {
		pageToken, historyID, err := decodeInitialCursor(cur)
		if err != nil {
			return &mail.Changes{Reset: true}, nil
		}
		return a.initialSync(ctx, box, pageToken, historyID)
	}

	var pageToken string
	start, err := strconv.ParseUint(string(cur), 10, 64)
	if strings.HasPrefix(string(cur), "gmail-history:") {
		pageToken, start, err = decodeHistoryCursor(cur)
	}
	if err != nil {
		return &mail.Changes{Reset: true}, nil
	}

	call := a.svc.Users.History.List(a.user).
		StartHistoryId(start).
		LabelId(string(box)).
		MaxResults(500)
	if pageToken != "" {
		call = call.PageToken(pageToken)
	}

	res, err := readCall(ctx, a, func() (*gmail.ListHistoryResponse, error) { return call.Context(ctx).Do() })
	if err != nil {
		if errors.Is(classify(err), mail.ErrNotFound) {
			// The history window has moved past this cursor.
			return &mail.Changes{Reset: true}, nil
		}
		return nil, classify(err)
	}

	changes := &mail.Changes{More: res.NextPageToken != ""}
	if changes.More {
		changes.Next = encodeHistoryCursor(res.NextPageToken, start)
	} else {
		changes.Next = mail.Cursor(strconv.FormatUint(res.HistoryId, 10))
	}

	// History records carry message IDs, not envelopes. Deduplicating here
	// matters: one message touched several times in a window appears in
	// every record, and refetching it once per appearance burns quota that
	// Gmail counts per user per second.
	added := map[string]bool{}
	removed := map[string]bool{}

	for _, h := range res.History {
		for _, m := range h.MessagesAdded {
			added[m.Message.Id] = true
			delete(removed, m.Message.Id)
		}
		for _, m := range h.MessagesDeleted {
			removed[m.Message.Id] = true
			delete(added, m.Message.Id)
		}
		for _, l := range h.LabelsAdded {
			if !removed[l.Message.Id] {
				added[l.Message.Id] = true
			}
		}
		for _, l := range h.LabelsRemoved {
			if !removed[l.Message.Id] {
				added[l.Message.Id] = true
			}
		}
	}

	for id := range added {
		changes.Changes = append(changes.Changes, mail.Change{
			Kind: mail.ChangeUpdated,
			ID:   mail.NativeMessageID(mail.ProviderGmail, id),
		})
	}
	for id := range removed {
		changes.Changes = append(changes.Changes, mail.Change{
			Kind: mail.ChangeDestroyed,
			ID:   mail.NativeMessageID(mail.ProviderGmail, id),
		})
	}
	return changes, nil
}

// initialSync enumerates one label page. historyID is captured before the
// first list request, so changes arriving while a large import paginates are
// replayed rather than skipped when the final page becomes incremental.
func (a *Adapter) initialSync(ctx context.Context, box mail.MailboxID, pageToken string, historyID uint64) (*mail.Changes, error) {
	call := a.svc.Users.Messages.List(a.user).
		LabelIds(string(box)).
		// Spam and Trash are discoverable mailboxes in the mirror; the API
		// excludes them from listings unless explicitly included, so a
		// first scan of those labels would silently see nothing (audit
		// GMAIL-04). The label filter still selects the intended mailbox.
		IncludeSpamTrash(true).
		MaxResults(initialPageSize)
	if pageToken != "" {
		call = call.PageToken(pageToken)
	}
	res, err := readCall(ctx, a, func() (*gmail.ListMessagesResponse, error) { return call.Context(ctx).Do() })
	if err != nil {
		return nil, classify(err)
	}

	ids := make([]mail.MessageID, 0, len(res.Messages))
	for _, m := range res.Messages {
		ids = append(ids, mail.NativeMessageID(mail.ProviderGmail, m.Id))
	}

	envs, err := a.Envelopes(ctx, ids)
	if err != nil {
		return nil, err
	}

	changes := &mail.Changes{
		More:             res.NextPageToken != "",
		EnumerationStart: pageToken == "",
		Complete:         res.NextPageToken == "",
	}
	if changes.More {
		changes.Next = encodeInitialCursor(res.NextPageToken, historyID)
	} else {
		changes.Next = mail.Cursor(strconv.FormatUint(historyID, 10))
	}
	for i := range envs {
		e := envs[i]
		changes.Changes = append(changes.Changes, mail.Change{
			Kind: mail.ChangeCreated, ID: e.ID, Envelope: &e,
		})
	}
	return changes, nil
}

func encodeInitialCursor(pageToken string, historyID uint64) mail.Cursor {
	raw, _ := json.Marshal(struct {
		Page    string `json:"page"`
		History uint64 `json:"history"`
	}{pageToken, historyID})
	return mail.Cursor("gmail-initial:" + base64.RawURLEncoding.EncodeToString(raw))
}

func decodeInitialCursor(cur mail.Cursor) (string, uint64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(string(cur), "gmail-initial:"))
	if err != nil {
		return "", 0, err
	}
	var state struct {
		Page    string `json:"page"`
		History uint64 `json:"history"`
	}
	if err := json.Unmarshal(raw, &state); err != nil || state.Page == "" || state.History == 0 {
		if err == nil {
			err = fmt.Errorf("missing initial cursor fields")
		}
		return "", 0, err
	}
	return state.Page, state.History, nil
}

func encodeHistoryCursor(pageToken string, start uint64) mail.Cursor {
	raw, _ := json.Marshal(struct {
		Page  string `json:"page"`
		Start uint64 `json:"start"`
	}{pageToken, start})
	return mail.Cursor("gmail-history:" + base64.RawURLEncoding.EncodeToString(raw))
}

func decodeHistoryCursor(cur mail.Cursor) (string, uint64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(string(cur), "gmail-history:"))
	if err != nil {
		return "", 0, err
	}
	var state struct {
		Page  string `json:"page"`
		Start uint64 `json:"start"`
	}
	if err := json.Unmarshal(raw, &state); err != nil || state.Page == "" || state.Start == 0 {
		if err == nil {
			err = fmt.Errorf("missing history cursor fields")
		}
		return "", 0, err
	}
	return state.Page, state.Start, nil
}

// Envelopes fetches message metadata.
func (a *Adapter) Envelopes(ctx context.Context, ids []mail.MessageID) ([]mail.Envelope, error) {
	out := make([]mail.Envelope, 0, len(ids))
	for _, id := range ids {
		// format=metadata returns headers and labels without body content,
		// which reduces bytes, but has the same messages.get quota cost.
		m, err := readCall(ctx, a, func() (*gmail.Message, error) {
			return a.svc.Users.Messages.Get(a.user, nativeID(id)).
				Format("metadata").
				MetadataHeaders("From", "To", "Cc", "Bcc", "Reply-To",
					"Subject", "Date", "Message-ID", "In-Reply-To", "References").
				Context(ctx).Do()
		})
		if err != nil {
			if errors.Is(classify(err), mail.ErrNotFound) {
				continue
			}
			return nil, classify(err)
		}
		out = append(out, toEnvelope(m))
	}
	return out, nil
}

func nativeID(id mail.MessageID) string {
	return strings.TrimPrefix(string(id), "n:"+string(mail.ProviderGmail)+":")
}

func toEnvelope(m *gmail.Message) mail.Envelope {
	env := mail.Envelope{
		ID:                 mail.NativeMessageID(mail.ProviderGmail, m.Id),
		ThreadID:           mail.ThreadID(m.ThreadId),
		Size:               m.SizeEstimate,
		Preview:            m.Snippet,
		MailboxIDsComplete: true,
	}

	// InternalDate is milliseconds since the epoch.
	if m.InternalDate > 0 {
		env.ReceivedAt = time.UnixMilli(m.InternalDate).UTC()
	}

	for _, l := range m.LabelIds {
		env.MailboxIDs = append(env.MailboxIDs, mail.MailboxID(l))
		switch l {
		case "UNREAD":
			// Gmail models read state as the absence of a label, so Seen
			// is the inverse and is set after the loop.
		case "STARRED":
			env.Keywords.Flagged = true
		case "DRAFT":
			env.Keywords.Draft = true
		}
	}
	env.Keywords.Seen = !hasLabel(m.LabelIds, "UNREAD")

	if m.Payload != nil {
		for _, h := range m.Payload.Headers {
			switch strings.ToLower(h.Name) {
			case "subject":
				env.Subject = h.Value
			case "from":
				env.From = parseAddrs(h.Value)
			case "to":
				env.To = parseAddrs(h.Value)
			case "cc":
				env.Cc = parseAddrs(h.Value)
			case "bcc":
				env.Bcc = parseAddrs(h.Value)
			case "reply-to":
				env.ReplyTo = parseAddrs(h.Value)
			case "message-id":
				env.MessageIDHeader = h.Value
			case "in-reply-to":
				env.InReplyTo = mail.ParseReferences(h.Value)
			case "references":
				env.References = mail.ParseReferences(h.Value)
			case "date":
				// RFC 5322 permits named zones, obsolete comment forms and
				// one-digit days; net/mail's parser accepts them all where a
				// single layout string rejected most real mail (audit
				// GMAIL-01).
				if t, err := netmail.ParseDate(h.Value); err == nil {
					env.SentAt = t
				}
			}
		}
		env.HasAttachment = payloadHasAttachment(m.Payload)
	}

	env.Fingerprint = mail.ComputeFingerprint(&env)
	return env
}

func hasLabel(labels []string, want string) bool {
	for _, l := range labels {
		if l == want {
			return true
		}
	}
	return false
}

func payloadHasAttachment(p *gmail.MessagePart) bool {
	if p.Filename != "" {
		return true
	}
	for _, part := range p.Parts {
		if payloadHasAttachment(part) {
			return true
		}
	}
	return false
}

// parseAddrs parses an address header with Go's RFC 5322 parser. Splitting
// on commas mangled every quoted display name containing one ("Doe, Jane"
// became two broken addresses), which corrupted the first sender used by
// screening and replies (audit GMAIL-01). A parse failure yields no
// addresses rather than invented ones; callers already treat a missing
// sender as unscreenable.
func parseAddrs(header string) []mail.Address {
	parser := netmail.AddressParser{WordDecoder: &mime.WordDecoder{}}
	addresses, err := parser.ParseList(header)
	if err != nil {
		return nil
	}
	out := make([]mail.Address, 0, len(addresses))
	for _, address := range addresses {
		out = append(out, mail.Address{Name: address.Name, Email: address.Address})
	}
	return out
}

// Body fetches and decodes a message body.
func (a *Adapter) Body(ctx context.Context, id mail.MessageID) (*mail.Body, error) {
	m, err := readCall(ctx, a, func() (*gmail.Message, error) {
		return a.svc.Users.Messages.Get(a.user, nativeID(id)).
			Format("full").Context(ctx).Do()
	})
	if err != nil {
		return nil, classify(err)
	}

	body := &mail.Body{MessageID: id}
	if m.Payload != nil {
		if err := a.collectParts(ctx, nativeID(id), m.Payload, body); err != nil {
			return nil, err
		}
	}
	return body, nil
}

// collectParts walks the MIME tree, decoding text and cataloguing the rest.
// Errors propagate: a body assembled while one part is unfetchable would be
// cached forever as the complete message (audit GMAIL-02).
func (a *Adapter) collectParts(ctx context.Context, messageID string, p *gmail.MessagePart, body *mail.Body) error {
	switch {
	case strings.HasPrefix(p.MimeType, "multipart/"):
		for _, child := range p.Parts {
			if err := a.collectParts(ctx, messageID, child, body); err != nil {
				return err
			}
		}
		return nil
	case p.MimeType == "text/plain" && p.Filename == "":
		text, err := a.partText(ctx, messageID, p)
		if err != nil {
			return err
		}
		body.Text += text
		return nil
	case p.MimeType == "text/html" && p.Filename == "":
		html, err := a.partText(ctx, messageID, p)
		if err != nil {
			return err
		}
		body.HTML += html
		return nil
	}

	if p.Filename != "" || p.Body != nil && p.Body.AttachmentId != "" {
		disposition := "attachment"
		var cid string
		for _, h := range p.Headers {
			if strings.EqualFold(h.Name, "Content-ID") {
				cid = strings.Trim(h.Value, "<>")
				disposition = "inline"
			}
		}
		var size int64
		if p.Body != nil {
			size = p.Body.Size
		}
		body.Parts = append(body.Parts, mail.BodyPart{
			PartID:      p.PartId,
			Type:        p.MimeType,
			Filename:    p.Filename,
			Disposition: disposition,
			Size:        size,
			ContentID:   cid,
		})
	}
	return nil
}

// decodeURLBytes decodes Gmail's base64url payload, padded or not. A
// hard size bound stops a hostile encoded literal from allocating before
// validation.
func decodeURLBytes(value string) ([]byte, error) {
	if len(value) > 90<<20 {
		return nil, fmt.Errorf("gmail: encoded MIME part exceeds the 90 MB limit")
	}
	if strings.HasSuffix(value, "=") {
		return base64.URLEncoding.DecodeString(value)
	}
	return base64.RawURLEncoding.DecodeString(value)
}

// partBytes resolves one part's bytes from either representation Gmail
// uses: inline Body.Data or a separate attachment fetch by AttachmentId
// (audit GMAIL-02).
func (a *Adapter) partBytes(ctx context.Context, messageID string, p *gmail.MessagePart) ([]byte, error) {
	if p == nil || p.Body == nil {
		return nil, fmt.Errorf("gmail: missing MIME part body")
	}
	if p.Body.Data != "" {
		return decodeURLBytes(p.Body.Data)
	}
	if p.Body.AttachmentId != "" {
		result, err := readCall(ctx, a, func() (*gmail.MessagePartBody, error) {
			return a.svc.Users.Messages.Attachments.
				Get(a.user, messageID, p.Body.AttachmentId).Context(ctx).Do()
		})
		if err != nil {
			return nil, classify(err)
		}
		return decodeURLBytes(result.Data)
	}
	if p.Body.Size == 0 {
		return []byte{}, nil
	}
	return nil, fmt.Errorf("gmail: nonempty MIME part has no data source")
}

// partText resolves and charset-decodes one textual part.
func (a *Adapter) partText(ctx context.Context, messageID string, p *gmail.MessagePart) (string, error) {
	raw, err := a.partBytes(ctx, messageID, p)
	if err != nil {
		return "", err
	}
	charset := "utf-8"
	for _, h := range p.Headers {
		if strings.EqualFold(h.Name, "Content-Type") {
			if _, params, err := mime.ParseMediaType(h.Value); err == nil && params["charset"] != "" {
				charset = params["charset"]
			}
		}
	}
	decoded, err := decodeCharset(raw, charset)
	if err != nil {
		return "", fmt.Errorf("gmail: decode %s part: %w", charset, err)
	}
	return decoded, nil
}

// decodeCharset converts part bytes to UTF-8 when the declared charset says
// they are not already. Unknown charsets surface as errors instead of being
// silently misread as UTF-8.
func decodeCharset(raw []byte, charset string) (string, error) {
	name := strings.ToLower(strings.TrimSpace(charset))
	if name == "" || name == "utf-8" || name == "us-ascii" || name == "ascii" {
		return string(raw), nil
	}
	enc, err := htmlindex.Get(name)
	if err != nil {
		return "", fmt.Errorf("unsupported charset %q", charset)
	}
	out, err := enc.NewDecoder().Bytes(raw)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// Raw returns the original RFC 5322 message.
func (a *Adapter) Raw(ctx context.Context, id mail.MessageID) (io.ReadCloser, error) {
	m, err := readCall(ctx, a, func() (*gmail.Message, error) {
		return a.svc.Users.Messages.Get(a.user, nativeID(id)).
			Format("raw").Context(ctx).Do()
	})
	if err != nil {
		return nil, classify(err)
	}
	// The shared bounded decoder accepts padded and unpadded base64url —
	// the same tolerance the part decoder has always had. A padded raw
	// value used to fail here and fall the export back to a lossy mirror
	// rewrite (audit 5 GMAIL-01).
	raw, err := decodeURLBytes(m.Raw)
	if err != nil {
		return nil, fmt.Errorf("gmail: decode raw: %w", err)
	}
	return io.NopCloser(bytes.NewReader(raw)), nil
}

// Attachment streams one part's decoded content. The part object — not
// merely its attachment id — is what carries the data: a named attachment
// can hold inline Body.Data with no attachment id at all (audit GMAIL-02).
func (a *Adapter) Attachment(ctx context.Context, id mail.MessageID, partID string) (io.ReadCloser, error) {
	m, err := readCall(ctx, a, func() (*gmail.Message, error) {
		return a.svc.Users.Messages.Get(a.user, nativeID(id)).
			Format("full").Context(ctx).Do()
	})
	if err != nil {
		return nil, classify(err)
	}

	part := findPart(m.Payload, partID)
	if part == nil {
		return nil, fmt.Errorf("gmail: %w: part %s of %s", mail.ErrNotFound, partID, id)
	}
	raw, err := a.partBytes(ctx, nativeID(id), part)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader(string(raw))), nil
}

func findPart(p *gmail.MessagePart, partID string) *gmail.MessagePart {
	if p == nil {
		return nil
	}
	if p.PartId == partID {
		return p
	}
	for _, child := range p.Parts {
		if found := findPart(child, partID); found != nil {
			return found
		}
	}
	return nil
}

// Apply pushes a mutation by modifying labels.
func (a *Adapter) Apply(ctx context.Context, op mail.Operation) error {
	ids := make([]string, 0, len(op.IDs))
	for _, id := range op.IDs {
		ids = append(ids, nativeID(id))
	}
	if len(ids) == 0 {
		return nil
	}

	var req gmail.BatchModifyMessagesRequest
	req.Ids = ids

	switch op.Kind {
	case mail.OpAddKeyword:
		// Read state is the absence of UNREAD, so marking seen removes a
		// label rather than adding one.
		if strings.EqualFold(op.Keyword, "seen") {
			req.RemoveLabelIds = []string{"UNREAD"}
		} else {
			req.AddLabelIds = []string{gmailLabel(op.Keyword)}
		}
	case mail.OpRemoveKeyword:
		if strings.EqualFold(op.Keyword, "seen") {
			req.AddLabelIds = []string{"UNREAD"}
		} else {
			req.RemoveLabelIds = []string{gmailLabel(op.Keyword)}
		}
	case mail.OpMove:
		req.AddLabelIds = []string{string(op.Target)}
		req.RemoveLabelIds = []string{"INBOX"}
	case mail.OpDelete:
		req.AddLabelIds = []string{"TRASH"}
		req.RemoveLabelIds = []string{"INBOX"}
	default:
		return fmt.Errorf("gmail: unsupported operation %d", op.Kind)
	}

	err := a.svc.Users.Messages.BatchModify(a.user, &req).Context(ctx).Do()
	return classify(err)
}

func gmailLabel(keyword string) string {
	switch strings.ToLower(keyword) {
	case "flagged":
		return "STARRED"
	case "draft":
		return "DRAFT"
	default:
		return strings.ToUpper(keyword)
	}
}

var _ mail.Adapter = (*Adapter)(nil)
