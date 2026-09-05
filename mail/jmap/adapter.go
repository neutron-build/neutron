// Package jmap implements the mail.Adapter interface over JMAP (RFC 8620/8621).
//
// JMAP is the protocol the canonical model is shaped after, so this adapter is
// the thinnest of the four: stable email IDs, a real change feed, and
// server-side threading all map across without reconstruction.
package jmap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/neutron-build/neutron/mail"
)

const (
	capCore = "urn:ietf:params:jmap:core"
	capMail = "urn:ietf:params:jmap:mail"
)

// Adapter is a JMAP client bound to one account.
type Adapter struct {
	http        *http.Client
	apiURL      string
	downloadURL string
	accountID   string
	token       string
}

// Config describes how to reach a JMAP server.
type Config struct {
	// SessionURL is the session resource, usually
	// https://host/.well-known/jmap.
	SessionURL string

	// Token is a bearer token: an API token for Fastmail, an OAuth access
	// token elsewhere.
	Token string

	HTTPClient *http.Client
}

type session struct {
	APIURL string `json:"apiUrl"`
	// DownloadURL is a template with {accountId}, {blobId}, {type} and {name}
	// placeholders (RFC 8620 §1.6.2). It is the only way to reach blob bytes,
	// so raw messages and attachments both go through it.
	DownloadURL     string            `json:"downloadUrl"`
	PrimaryAccounts map[string]string `json:"primaryAccounts"`
}

// Dial fetches the session resource and binds to the primary mail account.
func Dial(ctx context.Context, cfg Config) (*Adapter, error) {
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 60 * time.Second}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.SessionURL, nil)
	if err != nil {
		return nil, fmt.Errorf("jmap: session request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jmap: session: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("jmap: session rejected the token: %w", mail.ErrReauthRequired)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jmap: session: unexpected status %d", resp.StatusCode)
	}

	var s session
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, fmt.Errorf("jmap: decode session: %w", err)
	}
	acct := s.PrimaryAccounts[capMail]
	if acct == "" {
		return nil, fmt.Errorf("jmap: session lists no primary mail account")
	}

	return &Adapter{
		http:        hc,
		apiURL:      s.APIURL,
		downloadURL: s.DownloadURL,
		accountID:   acct,
		token:       cfg.Token,
	}, nil
}

func (a *Adapter) Provider() mail.Provider { return mail.ProviderJMAP }
func (a *Adapter) Close() error            { return nil }

// call issues one or more JMAP method calls and returns the raw responses.
func (a *Adapter) call(ctx context.Context, calls ...[3]any) ([]json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{
		"using":       []string{capCore, capMail},
		"methodCalls": calls,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.apiURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jmap: request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, fmt.Errorf("jmap: token rejected: %w", mail.ErrReauthRequired)
	case http.StatusTooManyRequests:
		return nil, fmt.Errorf("jmap: throttled: %w", mail.ErrRateLimited)
	default:
		return nil, fmt.Errorf("jmap: unexpected status %d", resp.StatusCode)
	}

	var out struct {
		MethodResponses [][3]json.RawMessage `json:"methodResponses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("jmap: decode response: %w", err)
	}

	results := make([]json.RawMessage, 0, len(out.MethodResponses))
	for _, r := range out.MethodResponses {
		var name string
		if err := json.Unmarshal(r[0], &name); err != nil {
			return nil, err
		}
		if name == "error" {
			return nil, a.methodError(r[1])
		}
		results = append(results, r[1])
	}
	return results, nil
}

// methodError maps JMAP error types onto the engine's typed errors.
func (a *Adapter) methodError(raw json.RawMessage) error {
	var e struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(raw, &e)

	switch e.Type {
	case "cannotCalculateChanges":
		// The server can no longer express the delta from the given state.
		// This is JMAP's name for what IMAP calls a UIDVALIDITY change.
		return fmt.Errorf("jmap: %s: %w", e.Type, mail.ErrCursorInvalid)
	case "rateLimit":
		return fmt.Errorf("jmap: %s: %w", e.Type, mail.ErrRateLimited)
	case "unauthorized", "forbidden":
		return fmt.Errorf("jmap: %s: %w", e.Type, mail.ErrReauthRequired)
	default:
		return fmt.Errorf("jmap: method error: %s", e.Type)
	}
}

// Mailboxes lists mailboxes with their roles.
func (a *Adapter) Mailboxes(ctx context.Context) ([]mail.Mailbox, error) {
	res, err := a.call(ctx, [3]any{"Mailbox/get", map[string]any{"accountId": a.accountID}, "0"})
	if err != nil {
		return nil, err
	}

	var out struct {
		List []struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			Role     string `json:"role"`
			ParentID string `json:"parentId"`
		} `json:"list"`
	}
	if err := json.Unmarshal(res[0], &out); err != nil {
		return nil, fmt.Errorf("jmap: decode mailboxes: %w", err)
	}

	boxes := make([]mail.Mailbox, 0, len(out.List))
	for _, m := range out.List {
		boxes = append(boxes, mail.Mailbox{
			ID:       mail.MailboxID(m.ID),
			Name:     m.Name,
			Role:     roleFrom(m.Role),
			ParentID: mail.MailboxID(m.ParentID),
			Native:   m.ID,
		})
	}
	return boxes, nil
}

// roleFrom maps JMAP's role property onto the canonical role. JMAP defines
// these by specification, so no name matching is needed.
func roleFrom(role string) mail.Role {
	switch strings.ToLower(role) {
	case "inbox":
		return mail.RoleInbox
	case "archive":
		return mail.RoleArchive
	case "sent":
		return mail.RoleSent
	case "drafts":
		return mail.RoleDrafts
	case "trash":
		return mail.RoleTrash
	case "junk", "spam":
		return mail.RoleJunk
	case "all":
		return mail.RoleAll
	default:
		return mail.RoleNone
	}
}

// Sync returns changes since cur using Email/changes.
func (a *Adapter) Sync(ctx context.Context, box mail.MailboxID, cur mail.Cursor) (*mail.Changes, error) {
	if cur == "" {
		return a.initialSync(ctx, box)
	}

	res, err := a.call(ctx, [3]any{"Email/changes", map[string]any{
		"accountId":  a.accountID,
		"sinceState": string(cur),
		"maxChanges": 500,
	}, "0"})
	if err != nil {
		// cannotCalculateChanges is reported as a reset rather than an
		// error, so it joins the single recovery path shared by all four
		// providers.
		if strings.Contains(err.Error(), "cannotCalculateChanges") {
			return &mail.Changes{Reset: true}, nil
		}
		return nil, err
	}

	var out struct {
		NewState       string   `json:"newState"`
		HasMoreChanges bool     `json:"hasMoreChanges"`
		Created        []string `json:"created"`
		Updated        []string `json:"updated"`
		Destroyed      []string `json:"destroyed"`
	}
	if err := json.Unmarshal(res[0], &out); err != nil {
		return nil, fmt.Errorf("jmap: decode changes: %w", err)
	}

	changes := &mail.Changes{
		Next: mail.Cursor(out.NewState),
		More: out.HasMoreChanges,
	}

	ids := append(append([]string{}, out.Created...), out.Updated...)
	envs, err := a.getEmails(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]mail.Envelope, len(envs))
	for _, e := range envs {
		byID[string(e.ID)] = e
	}

	for _, id := range out.Created {
		if e, ok := byID[string(mail.NativeMessageID(mail.ProviderJMAP, id))]; ok {
			env := e
			changes.Changes = append(changes.Changes, mail.Change{
				Kind: mail.ChangeCreated, ID: env.ID, Envelope: &env,
			})
		}
	}
	for _, id := range out.Updated {
		if e, ok := byID[string(mail.NativeMessageID(mail.ProviderJMAP, id))]; ok {
			env := e
			changes.Changes = append(changes.Changes, mail.Change{
				Kind: mail.ChangeUpdated, ID: env.ID, Envelope: &env,
			})
		}
	}
	for _, id := range out.Destroyed {
		changes.Changes = append(changes.Changes, mail.Change{
			Kind: mail.ChangeDestroyed,
			ID:   mail.NativeMessageID(mail.ProviderJMAP, id),
		})
	}
	return changes, nil
}

// initialSync enumerates a mailbox from empty via Email/query.
func (a *Adapter) initialSync(ctx context.Context, box mail.MailboxID) (*mail.Changes, error) {
	res, err := a.call(ctx,
		[3]any{"Email/query", map[string]any{
			"accountId": a.accountID,
			"filter":    map[string]any{"inMailbox": string(box)},
			"limit":     500,
		}, "0"},
		[3]any{"Email/get", map[string]any{
			"accountId": a.accountID,
			"#ids": map[string]any{
				"resultOf": "0", "name": "Email/query", "path": "/ids",
			},
			"properties": emailProperties,
		}, "1"},
	)
	if err != nil {
		return nil, err
	}

	envs, err := decodeEmails(res[1])
	if err != nil {
		return nil, err
	}

	// The state to resume from is Email/get's, which describes the objects
	// actually fetched.
	var state struct {
		State string `json:"state"`
	}
	_ = json.Unmarshal(res[1], &state)

	changes := &mail.Changes{Next: mail.Cursor(state.State), Complete: true}
	for i := range envs {
		e := envs[i]
		changes.Changes = append(changes.Changes, mail.Change{
			Kind: mail.ChangeCreated, ID: e.ID, Envelope: &e,
		})
	}
	return changes, nil
}

var emailProperties = []string{
	"id", "blobId", "threadId", "mailboxIds", "keywords", "size",
	"receivedAt", "sentAt", "from", "to", "cc", "bcc", "replyTo",
	"subject", "preview", "hasAttachment", "messageId", "inReplyTo", "references",
}

func (a *Adapter) getEmails(ctx context.Context, ids []string) ([]mail.Envelope, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	res, err := a.call(ctx, [3]any{"Email/get", map[string]any{
		"accountId":  a.accountID,
		"ids":        ids,
		"properties": emailProperties,
	}, "0"})
	if err != nil {
		return nil, err
	}
	return decodeEmails(res[0])
}

type jmapEmail struct {
	ID            string          `json:"id"`
	ThreadID      string          `json:"threadId"`
	MailboxIDs    map[string]bool `json:"mailboxIds"`
	Keywords      map[string]bool `json:"keywords"`
	Size          int64           `json:"size"`
	ReceivedAt    time.Time       `json:"receivedAt"`
	SentAt        *time.Time      `json:"sentAt"`
	From          []jmapAddr      `json:"from"`
	To            []jmapAddr      `json:"to"`
	Cc            []jmapAddr      `json:"cc"`
	Bcc           []jmapAddr      `json:"bcc"`
	ReplyTo       []jmapAddr      `json:"replyTo"`
	Subject       string          `json:"subject"`
	Preview       string          `json:"preview"`
	HasAttachment bool            `json:"hasAttachment"`
	MessageID     []string        `json:"messageId"`
	InReplyTo     []string        `json:"inReplyTo"`
	References    []string        `json:"references"`
}

type jmapAddr struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

func decodeEmails(raw json.RawMessage) ([]mail.Envelope, error) {
	var out struct {
		List []jmapEmail `json:"list"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("jmap: decode emails: %w", err)
	}

	envs := make([]mail.Envelope, 0, len(out.List))
	for _, m := range out.List {
		env := mail.Envelope{
			ID:            mail.NativeMessageID(mail.ProviderJMAP, m.ID),
			ThreadID:      mail.ThreadID(m.ThreadID),
			Subject:       m.Subject,
			Preview:       m.Preview,
			Size:          m.Size,
			ReceivedAt:    m.ReceivedAt,
			HasAttachment: m.HasAttachment,
			From:          addrs(m.From),
			To:            addrs(m.To),
			Cc:            addrs(m.Cc),
			Bcc:           addrs(m.Bcc),
			ReplyTo:       addrs(m.ReplyTo),
			InReplyTo:     m.InReplyTo,
			References:    m.References,
		}
		if m.SentAt != nil {
			env.SentAt = *m.SentAt
		}
		if len(m.MessageID) > 0 {
			env.MessageIDHeader = m.MessageID[0]
		}
		for id, in := range m.MailboxIDs {
			if in {
				env.MailboxIDs = append(env.MailboxIDs, mail.MailboxID(id))
			}
		}
		env.Keywords = keywordsFrom(m.Keywords)
		env.Fingerprint = mail.ComputeFingerprint(&env)
		envs = append(envs, env)
	}
	return envs, nil
}

func addrs(in []jmapAddr) []mail.Address {
	if len(in) == 0 {
		return nil
	}
	out := make([]mail.Address, 0, len(in))
	for _, a := range in {
		out = append(out, mail.Address{Name: a.Name, Email: a.Email})
	}
	return out
}

// keywordsFrom maps JMAP's $-prefixed keywords onto the canonical flags.
func keywordsFrom(in map[string]bool) mail.Keywords {
	var kw mail.Keywords
	for k, set := range in {
		if !set {
			continue
		}
		switch strings.ToLower(k) {
		case "$seen":
			kw.Seen = true
		case "$flagged":
			kw.Flagged = true
		case "$draft":
			kw.Draft = true
		case "$answered":
			kw.Answered = true
		default:
			kw.Custom = append(kw.Custom, k)
		}
	}
	return kw
}

func (a *Adapter) Envelopes(ctx context.Context, ids []mail.MessageID) ([]mail.Envelope, error) {
	native := make([]string, 0, len(ids))
	for _, id := range ids {
		native = append(native, nativeID(id))
	}
	return a.getEmails(ctx, native)
}

// nativeID strips the canonical identity prefix back to the server's own ID.
func nativeID(id mail.MessageID) string {
	prefix := "n:" + string(mail.ProviderJMAP) + ":"
	return strings.TrimPrefix(string(id), prefix)
}

func (a *Adapter) Body(ctx context.Context, id mail.MessageID) (*mail.Body, error) {
	res, err := a.call(ctx, [3]any{"Email/get", map[string]any{
		"accountId":          a.accountID,
		"ids":                []string{nativeID(id)},
		"properties":         []string{"id", "textBody", "htmlBody", "bodyValues", "attachments"},
		"fetchAllBodyValues": true,
	}, "0"})
	if err != nil {
		return nil, err
	}

	var out struct {
		List []struct {
			BodyValues map[string]struct {
				Value string `json:"value"`
			} `json:"bodyValues"`
			TextBody []struct {
				PartID string `json:"partId"`
				Type   string `json:"type"`
			} `json:"textBody"`
			HTMLBody []struct {
				PartID string `json:"partId"`
				Type   string `json:"type"`
			} `json:"htmlBody"`
			Attachments []struct {
				PartID      string `json:"partId"`
				BlobID      string `json:"blobId"`
				Type        string `json:"type"`
				Name        string `json:"name"`
				Size        int64  `json:"size"`
				CID         string `json:"cid"`
				Disposition string `json:"disposition"`
			} `json:"attachments"`
		} `json:"list"`
	}
	if err := json.Unmarshal(res[0], &out); err != nil {
		return nil, fmt.Errorf("jmap: decode body: %w", err)
	}
	if len(out.List) == 0 {
		return nil, fmt.Errorf("jmap: %w: message %s", mail.ErrNotFound, id)
	}

	m := out.List[0]
	body := &mail.Body{MessageID: id}
	for _, p := range m.TextBody {
		if v, ok := m.BodyValues[p.PartID]; ok {
			body.Text += v.Value
		}
	}
	for _, p := range m.HTMLBody {
		if v, ok := m.BodyValues[p.PartID]; ok {
			body.HTML += v.Value
		}
	}
	for _, at := range m.Attachments {
		body.Parts = append(body.Parts, mail.BodyPart{
			PartID:      at.PartID,
			Type:        at.Type,
			Filename:    at.Name,
			Size:        at.Size,
			ContentID:   at.CID,
			Disposition: at.Disposition,
		})
	}
	return body, nil
}

// escapeTemplateValue percent-encodes everything outside RFC 3986's unreserved
// set.
//
// The template alone decides whether a placeholder lands in the path or the
// query, so a value has to be safe in both. url.PathEscape is not: it leaves
// '&', '=' and '+' intact, and an attachment filename is chosen by whoever
// sent the mail — "a&x=y.txt" dropped into a query position would append a
// parameter to someone else's URL.
func escapeTemplateValue(s string) string {
	const unreserved = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if c := s[i]; strings.IndexByte(unreserved, c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", s[i])
		}
	}
	return b.String()
}

// expandDownload fills the session's download template (RFC 8620 §1.6.2).
//
// Every placeholder is substituted percent-encoded: a blob id is server-chosen
// and a filename comes from the sender, so neither can be trusted to be path-
// or query-safe. The template decides where each value lands, which is why the
// server hands one out instead of the client assembling a URL.
func (a *Adapter) expandDownload(blobID, mimeType, name string) (string, error) {
	if a.downloadURL == "" {
		return "", fmt.Errorf("jmap: session advertises no downloadUrl")
	}
	if blobID == "" {
		return "", fmt.Errorf("jmap: no blob id to download")
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	if name == "" {
		name = "download"
	}
	r := strings.NewReplacer(
		"{accountId}", escapeTemplateValue(a.accountID),
		"{blobId}", escapeTemplateValue(blobID),
		"{type}", escapeTemplateValue(mimeType),
		"{name}", escapeTemplateValue(name),
	)
	return r.Replace(a.downloadURL), nil
}

// download fetches one blob and hands back the undrained body.
//
// The caller closes it; on any non-200 this closes it here, since a reader the
// caller never receives would otherwise leak the connection.
func (a *Adapter) download(ctx context.Context, blobID, mimeType, name string) (io.ReadCloser, error) {
	endpoint, err := a.expandDownload(blobID, mimeType, name)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.token)

	resp, err := a.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jmap: download: %w", err)
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		return resp.Body, nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		resp.Body.Close()
		return nil, fmt.Errorf("jmap: download rejected the token: %w", mail.ErrReauthRequired)
	case resp.StatusCode == http.StatusTooManyRequests:
		resp.Body.Close()
		return nil, fmt.Errorf("jmap: download throttled: %w", mail.ErrRateLimited)
	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return nil, fmt.Errorf("jmap: %w: blob %s", mail.ErrNotFound, blobID)
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("jmap: download: unexpected status %d", resp.StatusCode)
	}
}

// blobFor returns the message's own blob id — the RFC822 bytes as received.
func (a *Adapter) blobFor(ctx context.Context, id mail.MessageID) (string, error) {
	res, err := a.call(ctx, [3]any{"Email/get", map[string]any{
		"accountId":  a.accountID,
		"ids":        []string{nativeID(id)},
		"properties": []string{"blobId"},
	}, "0"})
	if err != nil {
		return "", err
	}
	var out struct {
		List []struct {
			BlobID string `json:"blobId"`
		} `json:"list"`
	}
	if err := json.Unmarshal(res[0], &out); err != nil {
		return "", fmt.Errorf("jmap: decode blobId: %w", err)
	}
	if len(out.List) == 0 {
		return "", fmt.Errorf("jmap: %w: message %s", mail.ErrNotFound, id)
	}
	return out.List[0].BlobID, nil
}

// Raw downloads the original RFC822 message through the blob endpoint.
func (a *Adapter) Raw(ctx context.Context, id mail.MessageID) (io.ReadCloser, error) {
	blob, err := a.blobFor(ctx, id)
	if err != nil {
		return nil, err
	}
	return a.download(ctx, blob, "message/rfc822", nativeID(id)+".eml")
}

// Attachment downloads one part of a message.
//
// JMAP addresses attachments by blob, not by part, so the part id is resolved
// against the message's own attachment list first. Matching accepts either the
// part id or the blob id: callers that kept a blob id from a previous Body
// should not have to re-fetch to use it.
func (a *Adapter) Attachment(ctx context.Context, id mail.MessageID, partID string) (io.ReadCloser, error) {
	res, err := a.call(ctx, [3]any{"Email/get", map[string]any{
		"accountId":  a.accountID,
		"ids":        []string{nativeID(id)},
		"properties": []string{"attachments"},
	}, "0"})
	if err != nil {
		return nil, err
	}
	var out struct {
		List []struct {
			Attachments []struct {
				PartID string `json:"partId"`
				BlobID string `json:"blobId"`
				Type   string `json:"type"`
				Name   string `json:"name"`
			} `json:"attachments"`
		} `json:"list"`
	}
	if err := json.Unmarshal(res[0], &out); err != nil {
		return nil, fmt.Errorf("jmap: decode attachments: %w", err)
	}
	if len(out.List) == 0 {
		return nil, fmt.Errorf("jmap: %w: message %s", mail.ErrNotFound, id)
	}
	for _, at := range out.List[0].Attachments {
		if at.PartID == partID || (at.BlobID != "" && at.BlobID == partID) {
			return a.download(ctx, at.BlobID, at.Type, at.Name)
		}
	}
	return nil, fmt.Errorf("jmap: %w: part %s of message %s", mail.ErrNotFound, partID, id)
}

// Apply pushes a mutation via Email/set.
func (a *Adapter) Apply(ctx context.Context, op mail.Operation) error {
	update := map[string]any{}

	for _, id := range op.IDs {
		patch := map[string]any{}
		switch op.Kind {
		case mail.OpAddKeyword:
			patch["keywords/"+jmapKeyword(op.Keyword)] = true
		case mail.OpRemoveKeyword:
			patch["keywords/"+jmapKeyword(op.Keyword)] = nil
		case mail.OpMove:
			patch["mailboxIds"] = map[string]bool{string(op.Target): true}
		case mail.OpDelete:
			// Handled below via destroy.
		default:
			return fmt.Errorf("jmap: unsupported operation %d", op.Kind)
		}
		if len(patch) > 0 {
			update[nativeID(id)] = patch
		}
	}

	args := map[string]any{"accountId": a.accountID}
	if op.Kind == mail.OpDelete {
		ids := make([]string, 0, len(op.IDs))
		for _, id := range op.IDs {
			ids = append(ids, nativeID(id))
		}
		args["destroy"] = ids
	} else {
		args["update"] = update
	}

	_, err := a.call(ctx, [3]any{"Email/set", args, "0"})
	return err
}

func jmapKeyword(k string) string {
	switch strings.ToLower(k) {
	case "seen", "flagged", "draft", "answered":
		return "$" + strings.ToLower(k)
	default:
		return k
	}
}

var _ mail.Adapter = (*Adapter)(nil)
