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
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/neutron-build/neutron/mail"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// Adapter is a Gmail client bound to one account.
type Adapter struct {
	svc *gmail.Service

	// user is always "me" in practice; the API keys off the token.
	user string
}

// New wraps an authenticated Gmail service.
//
// The caller supplies the token source, so refresh, storage, and revocation
// stay outside this package — x/oauth2 already handles refresh correctly and
// reimplementing it here would only add a second thing to get wrong.
func New(ctx context.Context, opts ...option.ClientOption) (*Adapter, error) {
	svc, err := gmail.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gmail: new service: %w", err)
	}
	return &Adapter{svc: svc, user: "me"}, nil
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
	res, err := a.svc.Users.Labels.List(a.user).Context(ctx).Do()
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
		return a.initialSync(ctx, box)
	}

	start, err := strconv.ParseUint(string(cur), 10, 64)
	if err != nil {
		return &mail.Changes{Reset: true}, nil
	}

	call := a.svc.Users.History.List(a.user).
		StartHistoryId(start).
		LabelId(string(box)).
		MaxResults(500)

	res, err := call.Context(ctx).Do()
	if err != nil {
		if errors.Is(classify(err), mail.ErrNotFound) {
			// The history window has moved past this cursor.
			return &mail.Changes{Reset: true}, nil
		}
		return nil, classify(err)
	}

	changes := &mail.Changes{
		Next: mail.Cursor(strconv.FormatUint(res.HistoryId, 10)),
		More: res.NextPageToken != "",
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

// initialSync enumerates a label from empty.
func (a *Adapter) initialSync(ctx context.Context, box mail.MailboxID) (*mail.Changes, error) {
	res, err := a.svc.Users.Messages.List(a.user).
		LabelIds(string(box)).
		MaxResults(500).
		Context(ctx).Do()
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

	// The profile's historyId is the resume point: it is current as of now,
	// whereas the newest message's historyId would skip anything that
	// changed between listing and fetching.
	profile, err := a.svc.Users.GetProfile(a.user).Context(ctx).Do()
	if err != nil {
		return nil, classify(err)
	}

	changes := &mail.Changes{
		Next:     mail.Cursor(strconv.FormatUint(profile.HistoryId, 10)),
		More:     res.NextPageToken != "",
		Complete: res.NextPageToken == "",
	}
	for i := range envs {
		e := envs[i]
		changes.Changes = append(changes.Changes, mail.Change{
			Kind: mail.ChangeCreated, ID: e.ID, Envelope: &e,
		})
	}
	return changes, nil
}

// Envelopes fetches message metadata.
func (a *Adapter) Envelopes(ctx context.Context, ids []mail.MessageID) ([]mail.Envelope, error) {
	out := make([]mail.Envelope, 0, len(ids))
	for _, id := range ids {
		// format=metadata returns headers and labels without body content,
		// which is all an envelope needs and a fraction of the quota cost.
		m, err := a.svc.Users.Messages.Get(a.user, nativeID(id)).
			Format("metadata").
			MetadataHeaders("From", "To", "Cc", "Bcc", "Reply-To",
				"Subject", "Date", "Message-ID", "In-Reply-To", "References").
			Context(ctx).Do()
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
		ID:       mail.NativeMessageID(mail.ProviderGmail, m.Id),
		ThreadID: mail.ThreadID(m.ThreadId),
		Size:     m.SizeEstimate,
		Preview:  m.Snippet,
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
				if t, err := time.Parse(time.RFC1123Z, h.Value); err == nil {
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

func parseAddrs(header string) []mail.Address {
	var out []mail.Address
	for _, raw := range strings.Split(header, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if open := strings.LastIndex(raw, "<"); open >= 0 {
			if close := strings.Index(raw[open:], ">"); close >= 0 {
				name := strings.Trim(strings.TrimSpace(raw[:open]), `"`)
				out = append(out, mail.Address{
					Name:  name,
					Email: raw[open+1 : open+close],
				})
				continue
			}
		}
		out = append(out, mail.Address{Email: raw})
	}
	return out
}

// Body fetches and decodes a message body.
func (a *Adapter) Body(ctx context.Context, id mail.MessageID) (*mail.Body, error) {
	m, err := a.svc.Users.Messages.Get(a.user, nativeID(id)).
		Format("full").Context(ctx).Do()
	if err != nil {
		return nil, classify(err)
	}

	body := &mail.Body{MessageID: id}
	if m.Payload != nil {
		collectParts(m.Payload, body)
	}
	return body, nil
}

// collectParts walks the MIME tree, decoding text and cataloguing the rest.
func collectParts(p *gmail.MessagePart, body *mail.Body) {
	switch {
	case strings.HasPrefix(p.MimeType, "multipart/"):
		for _, child := range p.Parts {
			collectParts(child, body)
		}
		return
	case p.MimeType == "text/plain" && p.Filename == "":
		body.Text += decodeBody(p)
	case p.MimeType == "text/html" && p.Filename == "":
		body.HTML += decodeBody(p)
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
}

func decodeBody(p *gmail.MessagePart) string {
	if p.Body == nil || p.Body.Data == "" {
		return ""
	}
	// Gmail uses base64url without padding.
	raw, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(p.Body.Data)
	if err != nil {
		return ""
	}
	return string(raw)
}

// Raw returns the original RFC 5322 message.
func (a *Adapter) Raw(ctx context.Context, id mail.MessageID) (io.ReadCloser, error) {
	m, err := a.svc.Users.Messages.Get(a.user, nativeID(id)).
		Format("raw").Context(ctx).Do()
	if err != nil {
		return nil, classify(err)
	}
	raw, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(m.Raw)
	if err != nil {
		return nil, fmt.Errorf("gmail: decode raw: %w", err)
	}
	return io.NopCloser(strings.NewReader(string(raw))), nil
}

// Attachment streams one part's decoded content.
func (a *Adapter) Attachment(ctx context.Context, id mail.MessageID, partID string) (io.ReadCloser, error) {
	m, err := a.svc.Users.Messages.Get(a.user, nativeID(id)).
		Format("full").Context(ctx).Do()
	if err != nil {
		return nil, classify(err)
	}

	attachID := findAttachmentID(m.Payload, partID)
	if attachID == "" {
		return nil, fmt.Errorf("gmail: %w: part %s of %s", mail.ErrNotFound, partID, id)
	}

	att, err := a.svc.Users.Messages.Attachments.
		Get(a.user, nativeID(id), attachID).Context(ctx).Do()
	if err != nil {
		return nil, classify(err)
	}
	raw, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(att.Data)
	if err != nil {
		return nil, fmt.Errorf("gmail: decode attachment: %w", err)
	}
	return io.NopCloser(strings.NewReader(string(raw))), nil
}

func findAttachmentID(p *gmail.MessagePart, partID string) string {
	if p == nil {
		return ""
	}
	if p.PartId == partID && p.Body != nil {
		return p.Body.AttachmentId
	}
	for _, child := range p.Parts {
		if id := findAttachmentID(child, partID); id != "" {
			return id
		}
	}
	return ""
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
