// Package graph implements the mail.Adapter interface over Microsoft Graph.
//
// This talks to the REST endpoints directly rather than through the Microsoft
// Graph SDK. The SDK pulls in a very large dependency tree — Kiota, its
// serializers, and a generated model for every Graph resource — to provide
// typed access to the handful of mail endpoints a mirror needs.
package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/neutron-build/neutron/mail"
)

const baseURL = "https://graph.microsoft.com/v1.0"

// Adapter is a Graph client bound to one mailbox.
type Adapter struct {
	http *http.Client
}

// New wraps an HTTP client that already applies authentication.
//
// The expected client is one from oauth2.Config.Client, which refreshes the
// token transparently. Keeping auth outside this package means token
// lifetime, storage, and revocation are handled in exactly one place.
func New(hc *http.Client) *Adapter {
	if hc == nil {
		hc = &http.Client{Timeout: 60 * time.Second}
	}
	return &Adapter{http: hc}
}

func (a *Adapter) Provider() mail.Provider { return mail.ProviderGraph }
func (a *Adapter) Close() error            { return nil }

// get issues a GET and decodes JSON into out.
func (a *Adapter) get(ctx context.Context, endpoint string, out any) error {
	if !strings.HasPrefix(endpoint, "http") {
		endpoint = baseURL + endpoint
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := a.http.Do(req)
	if err != nil {
		return fmt.Errorf("graph: request: %w", err)
	}
	defer resp.Body.Close()

	if err := statusError(resp); err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("graph: decode: %w", err)
	}
	return nil
}

// statusError maps Graph HTTP statuses onto the engine's typed errors.
func statusError(resp *http.Response) error {
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		return nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("graph: status %d: %w", resp.StatusCode, mail.ErrReauthRequired)
	case http.StatusNotFound:
		return fmt.Errorf("graph: status 404: %w", mail.ErrNotFound)
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return fmt.Errorf("graph: status %d: %w", resp.StatusCode, mail.ErrRateLimited)
	case http.StatusGone:
		// Graph returns 410 when a delta token has aged out. This is the
		// same condition as an IMAP UIDVALIDITY change and takes the same
		// recovery path.
		return fmt.Errorf("graph: delta token expired: %w", mail.ErrCursorInvalid)
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("graph: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

// Mailboxes lists mail folders.
func (a *Adapter) Mailboxes(ctx context.Context) ([]mail.Mailbox, error) {
	var boxes []mail.Mailbox
	for endpoint := "/me/mailFolders?$top=200"; endpoint != ""; {
		var out struct {
			Value []struct {
				ID             string `json:"id"`
				DisplayName    string `json:"displayName"`
				ParentFolderID string `json:"parentFolderId"`
				WellKnownName  string `json:"wellKnownName"`
			} `json:"value"`
			NextLink string `json:"@odata.nextLink"`
		}
		if err := a.get(ctx, endpoint, &out); err != nil {
			return nil, err
		}
		for _, f := range out.Value {
			boxes = append(boxes, mail.Mailbox{
				ID:       mail.MailboxID(f.ID),
				Name:     f.DisplayName,
				Role:     roleFrom(f.WellKnownName),
				ParentID: mail.MailboxID(f.ParentFolderID),
				Native:   f.ID,
			})
		}
		endpoint = out.NextLink
	}
	return boxes, nil
}

// roleFrom maps Graph's wellKnownName onto the canonical role. Display names
// are localised; wellKnownName is not.
func roleFrom(wellKnown string) mail.Role {
	switch strings.ToLower(wellKnown) {
	case "inbox":
		return mail.RoleInbox
	case "sentitems":
		return mail.RoleSent
	case "drafts":
		return mail.RoleDrafts
	case "deleteditems":
		return mail.RoleTrash
	case "junkemail":
		return mail.RoleJunk
	case "archive":
		return mail.RoleArchive
	default:
		return mail.RoleNone
	}
}

const messageFields = "id,conversationId,subject,bodyPreview,receivedDateTime," +
	"sentDateTime,from,toRecipients,ccRecipients,bccRecipients,replyTo," +
	"isRead,isDraft,flag,hasAttachments,internetMessageId,parentFolderId"

// Sync returns changes since cur using the delta query.
func (a *Adapter) Sync(ctx context.Context, box mail.MailboxID, cur mail.Cursor) (*mail.Changes, error) {
	endpoint := string(cur)
	initial := endpoint == ""
	if initial {
		endpoint = fmt.Sprintf("/me/mailFolders/%s/messages/delta?$select=%s&$top=200",
			url.PathEscape(string(box)), url.QueryEscape(messageFields))
	}

	var out struct {
		Value     []graphMessage `json:"value"`
		NextLink  string         `json:"@odata.nextLink"`
		DeltaLink string         `json:"@odata.deltaLink"`
	}
	if err := a.get(ctx, endpoint, &out); err != nil {
		if strings.Contains(err.Error(), "delta token expired") {
			return &mail.Changes{Reset: true}, nil
		}
		return nil, err
	}

	changes := &mail.Changes{More: out.NextLink != "", EnumerationStart: initial}
	if out.NextLink != "" {
		changes.Next = mail.Cursor(out.NextLink)
	} else {
		changes.Next = mail.Cursor(out.DeltaLink)
	}

	// An initial delta run enumerates the folder, so the final page closes
	// a complete listing. Subsequent runs are deltas and report removals
	// directly, so they are never complete.
	changes.Complete = initial && out.NextLink == ""

	for _, m := range out.Value {
		// A deleted item arrives as an annotation rather than a full
		// object; only its id is populated.
		if m.Removed != nil {
			changes.Changes = append(changes.Changes, mail.Change{
				Kind: mail.ChangeDestroyed,
				ID:   mail.NativeMessageID(mail.ProviderGraph, m.ID),
			})
			continue
		}
		env := m.toEnvelope()
		kind := mail.ChangeUpdated
		if initial {
			kind = mail.ChangeCreated
		}
		changes.Changes = append(changes.Changes, mail.Change{
			Kind: kind, ID: env.ID, Envelope: &env,
		})
	}
	return changes, nil
}

type graphRecipient struct {
	EmailAddress struct {
		Name    string `json:"name"`
		Address string `json:"address"`
	} `json:"emailAddress"`
}

type graphMessage struct {
	ID                string           `json:"id"`
	ConversationID    string           `json:"conversationId"`
	Subject           string           `json:"subject"`
	BodyPreview       string           `json:"bodyPreview"`
	ReceivedDateTime  time.Time        `json:"receivedDateTime"`
	SentDateTime      time.Time        `json:"sentDateTime"`
	From              *graphRecipient  `json:"from"`
	ToRecipients      []graphRecipient `json:"toRecipients"`
	CcRecipients      []graphRecipient `json:"ccRecipients"`
	BccRecipients     []graphRecipient `json:"bccRecipients"`
	ReplyTo           []graphRecipient `json:"replyTo"`
	IsRead            bool             `json:"isRead"`
	IsDraft           bool             `json:"isDraft"`
	HasAttachments    bool             `json:"hasAttachments"`
	InternetMessageID string           `json:"internetMessageId"`
	ParentFolderID    string           `json:"parentFolderId"`
	Flag              *struct {
		FlagStatus string `json:"flagStatus"`
	} `json:"flag"`
	Removed *struct {
		Reason string `json:"reason"`
	} `json:"@removed"`
}

func (m graphMessage) toEnvelope() mail.Envelope {
	env := mail.Envelope{
		ID:                 mail.NativeMessageID(mail.ProviderGraph, m.ID),
		ThreadID:           mail.ThreadID(m.ConversationID),
		Subject:            m.Subject,
		Preview:            m.BodyPreview,
		ReceivedAt:         m.ReceivedDateTime,
		SentAt:             m.SentDateTime,
		HasAttachment:      m.HasAttachments,
		MessageIDHeader:    m.InternetMessageID,
		To:                 recipients(m.ToRecipients),
		Cc:                 recipients(m.CcRecipients),
		Bcc:                recipients(m.BccRecipients),
		ReplyTo:            recipients(m.ReplyTo),
		MailboxIDsComplete: true,
	}
	if m.From != nil {
		env.From = recipients([]graphRecipient{*m.From})
	}
	if m.ParentFolderID != "" {
		env.MailboxIDs = []mail.MailboxID{mail.MailboxID(m.ParentFolderID)}
	}

	env.Keywords.Seen = m.IsRead
	env.Keywords.Draft = m.IsDraft
	if m.Flag != nil && strings.EqualFold(m.Flag.FlagStatus, "flagged") {
		env.Keywords.Flagged = true
	}

	env.Fingerprint = mail.ComputeFingerprint(&env)
	return env
}

func recipients(in []graphRecipient) []mail.Address {
	if len(in) == 0 {
		return nil
	}
	out := make([]mail.Address, 0, len(in))
	for _, r := range in {
		out = append(out, mail.Address{
			Name:  r.EmailAddress.Name,
			Email: r.EmailAddress.Address,
		})
	}
	return out
}

func nativeID(id mail.MessageID) string {
	return strings.TrimPrefix(string(id), "n:"+string(mail.ProviderGraph)+":")
}

// Envelopes refetches messages by identity.
func (a *Adapter) Envelopes(ctx context.Context, ids []mail.MessageID) ([]mail.Envelope, error) {
	out := make([]mail.Envelope, 0, len(ids))
	for _, id := range ids {
		var m graphMessage
		endpoint := fmt.Sprintf("/me/messages/%s?$select=%s",
			url.PathEscape(nativeID(id)), url.QueryEscape(messageFields))
		if err := a.get(ctx, endpoint, &m); err != nil {
			if strings.Contains(err.Error(), "404") {
				continue
			}
			return nil, err
		}
		out = append(out, m.toEnvelope())
	}
	return out, nil
}

// Body fetches a message's rendered content and part list.
func (a *Adapter) Body(ctx context.Context, id mail.MessageID) (*mail.Body, error) {
	var m struct {
		Body struct {
			ContentType string `json:"contentType"`
			Content     string `json:"content"`
		} `json:"body"`
	}
	endpoint := fmt.Sprintf("/me/messages/%s?$select=body", url.PathEscape(nativeID(id)))
	if err := a.get(ctx, endpoint, &m); err != nil {
		return nil, err
	}

	body := &mail.Body{MessageID: id}
	if strings.EqualFold(m.Body.ContentType, "html") {
		body.HTML = m.Body.Content
	} else {
		body.Text = m.Body.Content
	}

	var atts struct {
		Value []struct {
			ID          string `json:"id"`
			Name        string `json:"name"`
			ContentType string `json:"contentType"`
			Size        int64  `json:"size"`
			IsInline    bool   `json:"isInline"`
			ContentID   string `json:"contentId"`
		} `json:"value"`
	}
	attEndpoint := fmt.Sprintf("/me/messages/%s/attachments?$select=id,name,contentType,size,isInline,contentId",
		url.PathEscape(nativeID(id)))
	if err := a.get(ctx, attEndpoint, &atts); err == nil {
		for _, at := range atts.Value {
			disposition := "attachment"
			if at.IsInline {
				disposition = "inline"
			}
			body.Parts = append(body.Parts, mail.BodyPart{
				PartID:      at.ID,
				Type:        at.ContentType,
				Filename:    at.Name,
				Size:        at.Size,
				ContentID:   at.ContentID,
				Disposition: disposition,
			})
		}
	}
	return body, nil
}

// Raw returns the original MIME message.
func (a *Adapter) Raw(ctx context.Context, id mail.MessageID) (io.ReadCloser, error) {
	endpoint := fmt.Sprintf("%s/me/messages/%s/$value", baseURL, url.PathEscape(nativeID(id)))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graph: request: %w", err)
	}
	if err := statusError(resp); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}

// Attachment streams one attachment's bytes.
func (a *Adapter) Attachment(ctx context.Context, id mail.MessageID, partID string) (io.ReadCloser, error) {
	endpoint := fmt.Sprintf("%s/me/messages/%s/attachments/%s/$value",
		baseURL, url.PathEscape(nativeID(id)), url.PathEscape(partID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graph: request: %w", err)
	}
	if err := statusError(resp); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}

// Apply pushes a mutation.
func (a *Adapter) Apply(ctx context.Context, op mail.Operation) error {
	for _, id := range op.IDs {
		var err error
		switch op.Kind {
		case mail.OpAddKeyword, mail.OpRemoveKeyword:
			err = a.patch(ctx, id, keywordPatch(op))
		case mail.OpMove:
			err = a.post(ctx, fmt.Sprintf("/me/messages/%s/move", url.PathEscape(nativeID(id))),
				map[string]any{"destinationId": string(op.Target)})
		case mail.OpDelete:
			err = a.post(ctx, fmt.Sprintf("/me/messages/%s/move", url.PathEscape(nativeID(id))),
				map[string]any{"destinationId": "deleteditems"})
		default:
			return fmt.Errorf("graph: unsupported operation %d", op.Kind)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func keywordPatch(op mail.Operation) map[string]any {
	set := op.Kind == mail.OpAddKeyword
	switch strings.ToLower(op.Keyword) {
	case "seen":
		return map[string]any{"isRead": set}
	case "flagged":
		status := "notFlagged"
		if set {
			status = "flagged"
		}
		return map[string]any{"flag": map[string]any{"flagStatus": status}}
	default:
		// Graph has no arbitrary keyword concept; categories are the
		// nearest equivalent and are set wholesale rather than toggled.
		if set {
			return map[string]any{"categories": []string{op.Keyword}}
		}
		return map[string]any{"categories": []string{}}
	}
}

func (a *Adapter) patch(ctx context.Context, id mail.MessageID, body map[string]any) error {
	return a.send(ctx, http.MethodPatch,
		fmt.Sprintf("/me/messages/%s", url.PathEscape(nativeID(id))), body)
}

func (a *Adapter) post(ctx context.Context, endpoint string, body map[string]any) error {
	return a.send(ctx, http.MethodPost, endpoint, body)
}

func (a *Adapter) send(ctx context.Context, method, endpoint string, body map[string]any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, baseURL+endpoint, strings.NewReader(string(raw)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.http.Do(req)
	if err != nil {
		return fmt.Errorf("graph: request: %w", err)
	}
	defer resp.Body.Close()
	return statusError(resp)
}

var _ mail.Adapter = (*Adapter)(nil)
