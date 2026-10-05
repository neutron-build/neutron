// Package graph implements the mail.Adapter interface over Microsoft Graph.
//
// This talks to the REST endpoints directly rather than through the Microsoft
// Graph SDK. The SDK pulls in a very large dependency tree — Kiota, its
// serializers, and a generated model for every Graph resource — to provide
// typed access to the handful of mail endpoints a mirror needs.
package graph

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/neutron-build/neutron/mail"
)

// baseURL is a variable only so tests can point the client at a fake
// service; production always talks to the real endpoint.
var baseURL = "https://graph.microsoft.com/v1.0"

// providerJSONLimit bounds every provider JSON response this adapter
// decodes: fetch responses carry envelopes and bodies, so the cap sits
// far above any legitimate page while still refusing an unbounded
// stream (audit OPS-01).
const providerJSONLimit = 64 << 20

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

// graphEndpoint resolves an endpoint (absolute continuation URL from a
// response body, or a relative API path) against the configured base and
// rejects anything that leaves the approved origin or API path prefix: a
// poisoned or corrupted nextLink/deltaLink must not turn the mirror into
// an open proxy (audit 3 PROVIDER-02). Plain HTTP is tolerated only on
// loopback, where local test fakes (and dev instances) live.
func graphEndpoint(base, endpoint string) (string, error) {
	b, err := url.Parse(base)
	if err != nil || b.Hostname() == "" || b.User != nil ||
		(b.Scheme != "https" && !(b.Scheme == "http" && isLoopbackHostname(b.Hostname()))) {
		return "", errors.New("graph: invalid base origin")
	}
	raw := endpoint
	if !strings.HasPrefix(endpoint, "http") {
		if !strings.HasPrefix(endpoint, "/") {
			return "", errors.New("graph: endpoint must be absolute or root-relative")
		}
		raw = strings.TrimRight(base, "/") + endpoint
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != b.Scheme || u.User != nil || u.Fragment != "" ||
		!strings.EqualFold(u.Host, b.Host) ||
		!strings.HasPrefix(u.Path, strings.TrimRight(b.Path, "/")+"/") {
		return "", errors.New("graph: endpoint left the approved origin/path")
	}
	return u.String(), nil
}

// isLoopbackHostname reports whether a host is a loopback name or address.
func isLoopbackHostname(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// get issues a GET and decodes JSON into out.
func (a *Adapter) get(ctx context.Context, endpoint string, out any) error {
	endpoint, err := graphEndpoint(baseURL, endpoint)
	if err != nil {
		return err
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
	if err := json.NewDecoder(io.LimitReader(resp.Body, providerJSONLimit)).Decode(out); err != nil {
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

// Mailboxes lists every mail folder, nested folders included.
//
// /me/mailFolders returns only the root's direct children; without walking
// childFolders, a nested folder never appears and PutMailboxes treats the
// partial listing as authoritative, pruning state that still exists
// upstream (audit GRAPH-01). Roles come from well-known folder aliases
// rather than a wellKnownName property, which v1.0 does not define —
// display names are localised and cannot identify roles.
func (a *Adapter) Mailboxes(ctx context.Context) ([]mail.Mailbox, error) {
	type folder struct {
		ID               string `json:"id"`
		DisplayName      string `json:"displayName"`
		ParentFolderID   string `json:"parentFolderId"`
		ChildFolderCount int    `json:"childFolderCount"`
	}
	roles, err := a.wellKnownRoles(ctx)
	if err != nil {
		return nil, err
	}

	var boxes []mail.Mailbox
	visited := map[string]bool{}
	queue := []string{"/me/mailFolders?$top=200"}
	for len(queue) > 0 {
		endpoint := queue[0]
		queue = queue[1:]
		pages := 0
		for endpoint != "" {
			pages++
			if pages > 100 {
				return nil, fmt.Errorf("graph: mail folder pagination exceeded 100 pages")
			}
			var out struct {
				Value    []folder `json:"value"`
				NextLink string   `json:"@odata.nextLink"`
			}
			if err := a.get(ctx, endpoint, &out); err != nil {
				// A partial traversal must never look authoritative: the
				// caller prunes to what it is given.
				return nil, err
			}
			for _, f := range out.Value {
				if visited[f.ID] {
					continue
				}
				visited[f.ID] = true
				boxes = append(boxes, mail.Mailbox{
					ID:       mail.MailboxID(f.ID),
					Name:     f.DisplayName,
					Role:     roles[f.ID],
					ParentID: mail.MailboxID(f.ParentFolderID),
					Native:   f.ID,
				})
				if f.ChildFolderCount > 0 {
					queue = append(queue,
						"/me/mailFolders/"+url.PathEscape(f.ID)+"/childFolders?$top=200")
				}
			}
			endpoint = out.NextLink
		}
	}
	return boxes, nil
}

// wellKnownRoles resolves role-to-folder-ID through Graph's documented
// well-known folder aliases, returning folder ID -> canonical role. A
// missing optional folder (no archive, junk disabled) is an absence, not a
// transport failure.
func (a *Adapter) wellKnownRoles(ctx context.Context) (map[string]mail.Role, error) {
	aliases := map[string]mail.Role{
		"inbox":        mail.RoleInbox,
		"sentitems":    mail.RoleSent,
		"drafts":       mail.RoleDrafts,
		"deleteditems": mail.RoleTrash,
		"junkemail":    mail.RoleJunk,
		"archive":      mail.RoleArchive,
	}
	out := make(map[string]mail.Role, len(aliases))
	for alias, role := range aliases {
		var page struct {
			ID string `json:"id"`
		}
		if err := a.get(ctx, "/me/mailFolders/"+url.PathEscape(alias)+"?$select=id", &page); err != nil {
			if errors.Is(err, mail.ErrNotFound) {
				continue
			}
			return nil, err
		}
		if page.ID != "" {
			out[page.ID] = role
		}
	}
	return out, nil
}

const messageFields = "id,conversationId,subject,bodyPreview,receivedDateTime," +
	"sentDateTime,from,toRecipients,ccRecipients,bccRecipients,replyTo," +
	"isRead,isDraft,flag,hasAttachments,internetMessageId,parentFolderId"

// graphCursorV2 carries the enumeration phase through multi-page initial
// syncs (audit 5 SYNC-01): `initial` used to be derived from
// `cur == ""`, which is true only for the FIRST page — every nextLink
// page looked like an incremental delta, the terminal page never set
// Complete, and the staged scan polled its own deltaLink forever. The
// versioned cursor keeps the phase explicit; a bare legacy cursor decodes
// as invalid and enters the engine's typed reset path (a fresh staged
// scan), never a guessed phase.
type graphCursorV2 struct {
	URL         string `json:"url"`
	Enumerating bool   `json:"enumerating"`
}

const graphCursorPrefix = "graph-v2:"

func encodeGraphCursor(c graphCursorV2) mail.Cursor {
	raw, _ := json.Marshal(c) // string + bool cannot fail
	return mail.Cursor(graphCursorPrefix + base64.RawURLEncoding.EncodeToString(raw))
}

func decodeGraphCursor(cur mail.Cursor) (graphCursorV2, error) {
	const prefix = graphCursorPrefix
	if !strings.HasPrefix(string(cur), prefix) || len(cur) > 64<<10 {
		return graphCursorV2{}, mail.ErrCursorInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(string(cur), prefix))
	if err != nil {
		return graphCursorV2{}, mail.ErrCursorInvalid
	}
	var c graphCursorV2
	if json.Unmarshal(raw, &c) != nil || c.URL == "" {
		return graphCursorV2{}, mail.ErrCursorInvalid
	}
	return c, nil
}

// Sync returns changes since cur using the delta query.
func (a *Adapter) Sync(ctx context.Context, box mail.MailboxID, cur mail.Cursor) (*mail.Changes, error) {
	// An empty cursor starts a full enumeration. Anything else decodes
	// through the versioned cursor: a legacy bare URL is cursor-invalid
	// by construction (audit 5 SYNC-01) — the engine replaces it with a
	// fresh staged scan instead of guessing its phase.
	state := graphCursorV2{Enumerating: true}
	if cur != "" {
		decoded, err := decodeGraphCursor(cur)
		if err != nil {
			return nil, fmt.Errorf("graph: cursor: %w", err)
		}
		state = decoded
	}
	endpoint := state.URL
	if cur == "" {
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

	changes := &mail.Changes{More: out.NextLink != "", EnumerationStart: cur == ""}
	if out.NextLink != "" {
		// Mid-enumeration pages keep the phase they came from.
		changes.Next = encodeGraphCursor(graphCursorV2{URL: out.NextLink, Enumerating: state.Enumerating})
	} else {
		if out.DeltaLink == "" {
			return nil, errors.New("graph: terminal delta page carried no deltaLink")
		}
		changes.Next = encodeGraphCursor(graphCursorV2{URL: out.DeltaLink, Enumerating: false})
		// Only the terminal page of a FULL ENUMERATION closes a complete
		// listing; an ordinary incremental delta response never does
		// (audit 5 SYNC-01).
		changes.Complete = state.Enumerating
	}

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
		if state.Enumerating {
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

	// Attachment metadata is paginated, and a failure or truncated page
	// here must not be silently cached as "no attachments" — the body this
	// returns is stored as complete (audit GRAPH-03).
	attEndpoint := fmt.Sprintf("/me/messages/%s/attachments?$select=id,name,contentType,size,isInline,contentId",
		url.PathEscape(nativeID(id)))
	for pages := 0; attEndpoint != ""; pages++ {
		if pages > 100 {
			return nil, fmt.Errorf("graph: attachment pagination exceeded 100 pages")
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
			NextLink string `json:"@odata.nextLink"`
		}
		if err := a.get(ctx, attEndpoint, &atts); err != nil {
			return nil, fmt.Errorf("graph: attachment metadata incomplete: %w", err)
		}
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
		attEndpoint = atts.NextLink
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
	// Categories are a whole-collection replacement in Graph: mapping one
	// arbitrary keyword onto categories:[k] would wipe every unrelated
	// category another client set, and removing one onto categories:[]
	// would erase them all. Only the two keywords with real single-property
	// mappings are supported (audit GRAPH-04).
	if op.Kind == mail.OpAddKeyword || op.Kind == mail.OpRemoveKeyword {
		switch strings.ToLower(op.Keyword) {
		case "seen", "flagged":
		default:
			return fmt.Errorf("graph: arbitrary keyword mutation is unsupported")
		}
	}
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
		// Unreachable: Apply rejects other keywords before patching.
		return map[string]any{}
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
