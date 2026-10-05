// Package jmap implements the mail.Adapter interface over JMAP (RFC 8620/8621).
//
// JMAP is the protocol the canonical model is shaped after, so this adapter is
// the thinnest of the four: stable email IDs, a real change feed, and
// server-side threading all map across without reconstruction.
package jmap

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/neutron-build/neutron/mail"
)

const (
	capCore = "urn:ietf:params:jmap:core"
	capMail = "urn:ietf:params:jmap:mail"
)

// Provider JSON decode caps (audit OPS-01): a session document is small;
// method responses carry envelopes and bodies, so their cap sits far
// above any legitimate page while refusing an unbounded stream.
const (
	sessionJSONLimit  = 4 << 20
	responseJSONLimit = 64 << 20
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
	APIURL          string            `json:"apiUrl"`
	DownloadURL     string            `json:"downloadUrl"`
	PrimaryAccounts map[string]string `json:"primaryAccounts"`
}

// credentialEndpoint validates a discovered JMAP endpoint that will carry
// the bearer token (audit 5 JMAP-02): HTTPS unless loopback (local test
// fakes and dev instances), no userinfo, no fragment, and a real host.
// The session document is provider-controlled metadata, not an
// authorization for the token to travel somewhere new.
func credentialEndpoint(raw, what string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
		return nil, fmt.Errorf("jmap: session %s is not a usable absolute URL", what)
	}
	loopback := strings.EqualFold(u.Hostname(), "localhost") || net.ParseIP(u.Hostname()) != nil && net.ParseIP(u.Hostname()).IsLoopback()
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return nil, fmt.Errorf("jmap: session %s must be HTTPS (HTTP is allowed only on loopback)", what)
	}
	return u, nil
}

// Dial fetches the session resource and binds to the primary mail account.
func Dial(ctx context.Context, cfg Config) (*Adapter, error) {
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 60 * time.Second}
	}
	sessionURL, err := credentialEndpoint(cfg.SessionURL, "URL")
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sessionURL.String(), nil)
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
	if err := json.NewDecoder(io.LimitReader(resp.Body, sessionJSONLimit)).Decode(&s); err != nil {
		return nil, fmt.Errorf("jmap: decode session: %w", err)
	}
	acct := s.PrimaryAccounts[capMail]
	if acct == "" {
		return nil, fmt.Errorf("jmap: session lists no primary mail account")
	}

	if s.APIURL == "" || s.DownloadURL == "" {
		return nil, fmt.Errorf("jmap: session is missing apiUrl or downloadUrl")
	}
	api, err := credentialEndpoint(s.APIURL, "apiUrl")
	if err != nil {
		return nil, err
	}
	download, err := credentialEndpoint(s.DownloadURL, "downloadUrl")
	if err != nil {
		return nil, err
	}
	// Redirects keep credentials inside the two origins the session
	// advertised — a redirect downgrade or a third origin must not become
	// a silent token forwarding (audit 5 JMAP-02).
	client := hc
	if client.CheckRedirect == nil {
		clone := *hc
		client = &clone
	}
	if client.CheckRedirect == nil {
		trusted := map[string]bool{
			strings.ToLower(api.Scheme + "://" + api.Host):           true,
			strings.ToLower(download.Scheme + "://" + download.Host): true,
		}
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("jmap: too many provider redirects")
			}
			u, err := credentialEndpoint(req.URL.String(), "redirect")
			if err != nil {
				return err
			}
			if !trusted[strings.ToLower(u.Scheme+"://"+u.Host)] {
				return errors.New("jmap: redirect left the session-advertised origins")
			}
			return nil
		}
	}
	return &Adapter{http: client, apiURL: s.APIURL, downloadURL: s.DownloadURL, accountID: acct, token: cfg.Token}, nil
}

func (a *Adapter) Provider() mail.Provider { return mail.ProviderJMAP }
func (a *Adapter) Close() error            { return nil }

// call issues one or more JMAP method calls and returns one raw result per
// request, in request order.
//
// Responses are correlated with the request's client-supplied call IDs
// rather than trusted to arrive complete and in order: a malformed or short
// methodResponses array previously indexed past its end and could panic a
// background sync goroutine (audit JMAP-02). An "error" response for a
// requested call is mapped through methodError; responses for unknown call
// IDs (implicit server additions) are ignored.
func (a *Adapter) call(ctx context.Context, calls ...[3]any) ([]json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{
		"using":       []string{capCore, capMail},
		"methodCalls": calls,
	})
	if err != nil {
		return nil, err
	}

	indexes := make(map[string]int, len(calls))
	methods := make([]string, len(calls))
	for i, call := range calls {
		method, ok1 := call[0].(string)
		tag, ok2 := call[2].(string)
		if !ok1 || !ok2 || tag == "" {
			return nil, fmt.Errorf("jmap: invalid call %d", i)
		}
		if _, dup := indexes[tag]; dup {
			return nil, fmt.Errorf("jmap: duplicate call id %q", tag)
		}
		indexes[tag] = i
		methods[i] = method
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
	if err := json.NewDecoder(io.LimitReader(resp.Body, responseJSONLimit)).Decode(&out); err != nil {
		return nil, fmt.Errorf("jmap: decode response: %w", err)
	}

	results := make([]json.RawMessage, len(calls))
	for _, r := range out.MethodResponses {
		if len(r) != 3 {
			return nil, fmt.Errorf("jmap: response tuple has %d parts, want 3", len(r))
		}
		var name, tag string
		if err := json.Unmarshal(r[0], &name); err != nil {
			return nil, fmt.Errorf("jmap: decode response method: %w", err)
		}
		if err := json.Unmarshal(r[2], &tag); err != nil {
			return nil, fmt.Errorf("jmap: decode response call id: %w", err)
		}
		i, wanted := indexes[tag]
		if !wanted {
			continue // implicit response for a call we did not make
		}
		if name == "error" {
			return nil, a.methodError(r[1])
		}
		// Correlation is by client ID; the method name is informational
		// (some servers wrap or alias method names), so the tag decides.
		if results[i] != nil {
			return nil, fmt.Errorf("jmap: duplicate result for call %q", tag)
		}
		results[i] = r[1]
	}
	for i := range results {
		if results[i] == nil {
			return nil, fmt.Errorf("jmap: no result for %s (call %q)", methods[i], calls[i][2])
		}
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
		return a.initialSync(ctx, box, initialCursorState{})
	}
	if state, isInitial, wellFormed := decodeInitialCursor(cur); isInitial {
		if !wellFormed {
			// Malformed versioned cursor: an explicit reset replaces the
			// bookkeeping rather than a silent restart at position 0
			// (audit 5 SYNC-04).
			return &mail.Changes{Reset: true}, nil
		}
		return a.initialSync(ctx, box, state)
	}
	if strings.HasPrefix(string(cur), "jmap-initial:") {
		// Legacy position-only cursor from a pre-baseline build: resume the
		// enumeration without a baseline (the old, weaker contract) rather
		// than discarding a mid-scan mailbox.
		position, err := strconv.Atoi(strings.TrimPrefix(string(cur), "jmap-initial:"))
		if err != nil || position < 0 {
			return &mail.Changes{Reset: true}, nil
		}
		return a.initialSync(ctx, box, initialCursorState{Position: position})
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

// initialCursorState is the versioned JMAP enumeration cursor.
//
// Baseline is an Email state captured BEFORE the first query page. The
// final page of the enumeration replays Email/changes from that baseline,
// so a message modified or destroyed between two pages is neither skipped
// nor duplicated: the old cursor remembered only a position and adopted the
// LAST page's state, which could lie past changes earlier pages never saw
// (audit JMAP-03).
//
// QueryState is the Email/query state the FIRST page answered with (audit
// 5 SYNC-04). JMAP query pagination is positional, and RFC 8620 §5.5 is
// explicit that positions are meaningful only within one queryState: an
// offset shift (a message deleted from the front mid-enumeration) silently
// skips unchanged objects that Email/changes can never report. Each
// page's returned queryState is checked against the cursor's; a change
// invalidates the enumeration as a cursor reset rather than pruning
// around the hole.
type initialCursorState struct {
	Position   int    `json:"position"`
	Baseline   string `json:"baseline,omitempty"`
	QueryState string `json:"queryState,omitempty"`
}

func encodeInitialCursor(position int, baseline, queryState string) mail.Cursor {
	raw, _ := json.Marshal(initialCursorState{Position: position, Baseline: baseline, QueryState: queryState})
	return mail.Cursor("jmap-initial-v1:" + base64.RawURLEncoding.EncodeToString(raw))
}

// decodeInitialCursor reports (state, true) for a decodable v1 cursor.
// A MALFORMED one reports (zero, true) — the caller turns that into an
// explicit Reset so the engine replaces the enumeration bookkeeping
// instead of silently restarting at position 0 inside existing scan
// state (audit 5 SYNC-04).
func decodeInitialCursor(cur mail.Cursor) (initialCursorState, bool, bool) {
	const prefix = "jmap-initial-v1:"
	if !strings.HasPrefix(string(cur), prefix) {
		return initialCursorState{}, false, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(string(cur), prefix))
	if err != nil || len(raw) > 512 {
		return initialCursorState{}, true, false
	}
	var state initialCursorState
	if err := json.Unmarshal(raw, &state); err != nil || state.Position < 0 {
		return initialCursorState{}, true, false
	}
	return state, true, true
}

// initialSync enumerates a mailbox from empty via Email/query.
//
// The first call also captures a baseline state (Email/get for an empty ID
// list in the same request) and the query's own queryState. Both ride
// through every page cursor: the baseline is what the final page catches
// up from, and the queryState is what proves the positional pages still
// describe the same result set (audit 5 SYNC-04).
func (a *Adapter) initialSync(ctx context.Context, box mail.MailboxID, resume initialCursorState) (*mail.Changes, error) {
	position, baseline, queryState := resume.Position, resume.Baseline, resume.QueryState
	calls := [][3]any{}
	first := position == 0 && baseline == ""
	if first {
		calls = append(calls, [3]any{"Email/get", map[string]any{
			"accountId":  a.accountID,
			"ids":        []string{},
			"properties": []string{"id"},
		}, "b"})
	}
	calls = append(calls,
		[3]any{"Email/query", map[string]any{
			"accountId":      a.accountID,
			"filter":         map[string]any{"inMailbox": string(box)},
			"position":       position,
			"limit":          500,
			"calculateTotal": true,
		}, "q"},
		[3]any{"Email/get", map[string]any{
			"accountId": a.accountID,
			"#ids": map[string]any{
				"resultOf": "q", "name": "Email/query", "path": "/ids",
			},
			"properties": emailProperties,
		}, "g"},
	)
	res, err := a.call(ctx, calls...)
	if err != nil {
		return nil, err
	}
	qi := 0
	ri := 1
	if first {
		var base struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(res[0], &base); err != nil || base.State == "" {
			return nil, fmt.Errorf("jmap: baseline Email/get returned no state")
		}
		baseline = base.State
		qi, ri = 1, 2
	}

	envs, err := decodeEmails(res[ri])
	if err != nil {
		return nil, err
	}

	// The state to resume from is Email/get's, which describes the objects
	// actually fetched.
	var state struct {
		State string `json:"state"`
	}
	_ = json.Unmarshal(res[ri], &state)

	var query struct {
		IDs        []string `json:"ids"`
		Position   int      `json:"position"`
		Total      int      `json:"total"`
		QueryState string   `json:"queryState"`
	}
	if err := json.Unmarshal(res[qi], &query); err != nil {
		return nil, fmt.Errorf("jmap: decode query page: %w", err)
	}

	// The positional contract (audit 5 SYNC-04): offsets are valid only
	// inside one queryState. A server that cannot report its query state,
	// echoes a position other than the one requested, or reports a CHANGED
	// state has shifted the result set under the pagination — the safe
	// answer is a cursor reset (the engine restarts the enumeration and
	// prunes nothing), never pruning around objects the shift skipped.
	if query.QueryState == "" {
		return nil, fmt.Errorf("jmap: query page carried no queryState: %w", mail.ErrCursorInvalid)
	}
	if query.Position != position {
		return nil, fmt.Errorf("jmap: query page answered position %d, want %d: %w", query.Position, position, mail.ErrCursorInvalid)
	}
	if queryState == "" {
		queryState = query.QueryState
	} else if query.QueryState != queryState {
		return nil, fmt.Errorf("jmap: query state shifted during enumeration: %w", mail.ErrCursorInvalid)
	}

	more := len(query.IDs) > 0 && query.Position+len(query.IDs) < query.Total
	changes := &mail.Changes{EnumerationStart: position == 0}
	for i := range envs {
		e := envs[i]
		changes.Changes = append(changes.Changes, mail.Change{
			Kind: mail.ChangeCreated, ID: e.ID, Envelope: &e,
		})
	}

	if more {
		changes.Next = encodeInitialCursor(query.Position+len(query.IDs), baseline, queryState)
		changes.More = true
		changes.Complete = false
		return changes, nil
	}

	// Final page: converge on the baseline. Email/changes from the
	// pre-enumeration baseline replays everything that moved while the
	// listing paginated — early-page modifications and destroys included —
	// so the incremental cursor that survives (newState) has observed every
	// change it claims to be past. The catch-up DRAINS every page until
	// hasMoreChanges is false: one 500-change page used to be all of it,
	// and a busier window's remaining changes were silently adopted past
	// (audit 5 SYNC-04).
	if baseline == "" {
		// Legacy position-only cursor: no baseline exists; adopt the page's
		// own get state as before.
		changes.Next = mail.Cursor(state.State)
		changes.Complete = true
		return changes, nil
	}
	nextState, extra, err := a.changesSinceDrained(ctx, baseline)
	if err != nil {
		return nil, err
	}
	changes.Changes = append(changes.Changes, extra...)
	changes.Next = mail.Cursor(nextState)
	changes.More = false
	changes.Complete = true
	return changes, nil
}

// catch-up page bound: 40 pages of maxChanges=500 = 20k changes replayed
// inside one terminal enumeration page; a window busier than that is a
// provider contract problem, not something to loop on unbounded.
const maxCatchUpPages = 40

// changesSinceDrained replays Email/changes from a baseline state until
// hasMoreChanges is false, returning the reached state and the change
// list (with envelopes fetched for created/updated IDs). The bound keeps
// a hostile or broken server from feeding pages forever; hitting it is an
// error the next sync retries from durable progress (audit 5 SYNC-04).
func (a *Adapter) changesSinceDrained(ctx context.Context, baseline string) (string, []mail.Change, error) {
	var extra []mail.Change
	since := baseline
	for page := 0; page < maxCatchUpPages; page++ {
		newState, changes, hasMore, err := a.changesSincePage(ctx, since)
		if err != nil {
			return "", nil, err
		}
		extra = append(extra, changes...)
		if newState == "" || !hasMore {
			if newState == "" {
				newState = since
			}
			return newState, extra, nil
		}
		since = newState
	}
	return "", nil, fmt.Errorf("jmap: baseline catch-up exceeded %d pages", maxCatchUpPages)
}

// changesSincePage fetches exactly one Email/changes page.
func (a *Adapter) changesSincePage(ctx context.Context, since string) (string, []mail.Change, bool, error) {
	res, err := a.call(ctx, [3]any{"Email/changes", map[string]any{
		"accountId":  a.accountID,
		"sinceState": since,
		"maxChanges": 500,
	}, "0"})
	if err != nil {
		if strings.Contains(err.Error(), "cannotCalculateChanges") {
			// Typed cursor-invalid: the engine's bounded replacement
			// restarts the enumeration; a generic error here used to wedge
			// the stored continuation forever (audit 5 SYNC-03/04).
			return "", nil, false, fmt.Errorf("jmap: baseline expired: %w", mail.ErrCursorInvalid)
		}
		return "", nil, false, err
	}
	var out struct {
		NewState       string   `json:"newState"`
		HasMoreChanges bool     `json:"hasMoreChanges"`
		Created        []string `json:"created"`
		Updated        []string `json:"updated"`
		Destroyed      []string `json:"destroyed"`
	}
	if err := json.Unmarshal(res[0], &out); err != nil {
		return "", nil, false, fmt.Errorf("jmap: decode baseline catch-up: %w", err)
	}
	var extra []mail.Change
	ids := append(append([]string{}, out.Created...), out.Updated...)
	if len(ids) > 0 {
		envs, err := a.getEmails(ctx, ids)
		if err != nil {
			return "", nil, false, err
		}
		created := make(map[mail.MessageID]bool, len(out.Created))
		for _, id := range out.Created {
			created[mail.NativeMessageID(mail.ProviderJMAP, id)] = true
		}
		for i := range envs {
			kind := mail.ChangeUpdated
			if created[envs[i].ID] {
				kind = mail.ChangeCreated
			}
			e := envs[i]
			extra = append(extra, mail.Change{Kind: kind, ID: e.ID, Envelope: &e})
		}
	}
	for _, id := range out.Destroyed {
		extra = append(extra, mail.Change{
			Kind: mail.ChangeDestroyed,
			ID:   mail.NativeMessageID(mail.ProviderJMAP, id),
		})
	}
	return out.NewState, extra, out.HasMoreChanges, nil
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
			ID:                 mail.NativeMessageID(mail.ProviderJMAP, m.ID),
			ThreadID:           mail.ThreadID(m.ThreadID),
			Subject:            m.Subject,
			Preview:            m.Preview,
			Size:               m.Size,
			ReceivedAt:         m.ReceivedAt,
			HasAttachment:      m.HasAttachment,
			From:               addrs(m.From),
			To:                 addrs(m.To),
			Cc:                 addrs(m.Cc),
			Bcc:                addrs(m.Bcc),
			ReplyTo:            addrs(m.ReplyTo),
			InReplyTo:          m.InReplyTo,
			References:         m.References,
			MailboxIDsComplete: true,
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
				Value             string `json:"value"`
				IsTruncated       bool   `json:"isTruncated"`
				IsEncodingProblem bool   `json:"isEncodingProblem"`
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
	// A referenced part with NO bodyValues entry used to be silently
	// skipped and cached as complete content; it is as incomplete as a
	// truncated one (audit 5 JMAP-01). A message with no body parts at
	// all is genuinely empty and stays a successful empty body.
	for _, p := range m.TextBody {
		v, ok := m.BodyValues[p.PartID]
		if !ok || v.IsTruncated || v.IsEncodingProblem {
			// A truncated, undecodable, or MISSING part must not be cached
			// as the complete message: refuse so the caller can fall back
			// to the raw blob instead of permanently storing partial
			// content (audit JMAP-04, 5 JMAP-01).
			return nil, fmt.Errorf("jmap: %w: text part %s of %s", errIncompleteBody, p.PartID, id)
		}
		body.Text += v.Value
	}
	for _, p := range m.HTMLBody {
		v, ok := m.BodyValues[p.PartID]
		if !ok || v.IsTruncated || v.IsEncodingProblem {
			return nil, fmt.Errorf("jmap: %w: HTML part %s of %s", errIncompleteBody, p.PartID, id)
		}
		body.HTML += v.Value
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

func (a *Adapter) Raw(ctx context.Context, id mail.MessageID) (io.ReadCloser, error) {
	var out struct {
		List []struct {
			BlobID string `json:"blobId"`
		} `json:"list"`
	}
	res, err := a.call(ctx, [3]any{"Email/get", map[string]any{
		"accountId": a.accountID, "ids": []string{nativeID(id)}, "properties": []string{"blobId"},
	}, "0"})
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(res[0], &out); err != nil {
		return nil, fmt.Errorf("jmap: decode raw blob: %w", err)
	}
	if len(out.List) == 0 || out.List[0].BlobID == "" {
		return nil, fmt.Errorf("jmap: %w: message %s", mail.ErrNotFound, id)
	}
	return a.download(ctx, out.List[0].BlobID, "message.eml", "message/rfc822")
}

func (a *Adapter) Attachment(ctx context.Context, id mail.MessageID, partID string) (io.ReadCloser, error) {
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
	res, err := a.call(ctx, [3]any{"Email/get", map[string]any{
		"accountId": a.accountID, "ids": []string{nativeID(id)}, "properties": []string{"attachments"},
	}, "0"})
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(res[0], &out); err != nil {
		return nil, fmt.Errorf("jmap: decode attachment blobs: %w", err)
	}
	if len(out.List) > 0 {
		for _, part := range out.List[0].Attachments {
			if part.PartID == partID && part.BlobID != "" {
				return a.download(ctx, part.BlobID, part.Name, part.Type)
			}
		}
	}
	return nil, fmt.Errorf("jmap: %w: attachment %s", mail.ErrNotFound, partID)
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

// download expands the level-1 URI template advertised by the JMAP session
// and returns the authenticated immutable blob stream.
func (a *Adapter) download(ctx context.Context, blobID, name, mediaType string) (io.ReadCloser, error) {
	if name == "" {
		name = "download"
	}
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	values := map[string]string{
		"accountId": a.accountID, "blobId": blobID, "name": name, "type": mediaType,
	}
	endpoint := a.downloadURL
	for key, value := range values {
		endpoint = strings.ReplaceAll(endpoint, "{"+key+"}", escapeTemplateValue(value))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("jmap: download request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	resp, err := a.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jmap: download: %w", err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return resp.Body, nil
	case http.StatusUnauthorized, http.StatusForbidden:
		resp.Body.Close()
		return nil, fmt.Errorf("jmap: download token rejected: %w", mail.ErrReauthRequired)
	case http.StatusTooManyRequests:
		resp.Body.Close()
		return nil, fmt.Errorf("jmap: download throttled: %w", mail.ErrRateLimited)
	case http.StatusNotFound:
		resp.Body.Close()
		return nil, fmt.Errorf("jmap: download: %w", mail.ErrNotFound)
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("jmap: download: unexpected status %d", resp.StatusCode)
	}
}

// Apply pushes a mutation via Email/set.
func (a *Adapter) Apply(ctx context.Context, op mail.Operation) error {
	update := map[string]any{}

	for _, id := range op.IDs {
		patch := map[string]any{}
		switch op.Kind {
		case mail.OpAddKeyword:
			patch["keywords/"+pointerToken(jmapKeyword(op.Keyword))] = true
		case mail.OpRemoveKeyword:
			patch["keywords/"+pointerToken(jmapKeyword(op.Keyword))] = nil
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
	destroy := op.Kind == mail.OpDelete
	if destroy {
		ids := make([]string, 0, len(op.IDs))
		for _, id := range op.IDs {
			ids = append(ids, nativeID(id))
		}
		args["destroy"] = ids
	} else {
		args["update"] = update
	}

	res, err := a.call(ctx, [3]any{"Email/set", args, "0"})
	if err != nil {
		return err
	}
	return checkSetResult(res[0], op.IDs, destroy)
}

// checkSetResult verifies that Email/set actually applied every requested
// mutation. A 200 response whose payload carries notUpdated/notDestroyed
// entries — or simply omits a requested ID — is a partial or total failure
// that the old code reported as success (audit JMAP-01).
func checkSetResult(raw json.RawMessage, ids []mail.MessageID, destroy bool) error {
	var result struct {
		Updated      map[string]json.RawMessage `json:"updated"`
		Destroyed    []string                   `json:"destroyed"`
		NotUpdated   map[string]json.RawMessage `json:"notUpdated"`
		NotDestroyed map[string]json.RawMessage `json:"notDestroyed"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return fmt.Errorf("jmap: decode Email/set result: %w", err)
	}
	destroyed := make(map[string]bool, len(result.Destroyed))
	for _, id := range result.Destroyed {
		destroyed[id] = true
	}
	var failures []error
	for _, id := range ids {
		native := nativeID(id)
		if destroy {
			if _, failed := result.NotDestroyed[native]; failed || !destroyed[native] {
				failures = append(failures, fmt.Errorf("jmap: destroy failed for %s", native))
			}
			continue
		}
		_, updated := result.Updated[native]
		_, failed := result.NotUpdated[native]
		if failed || !updated {
			failures = append(failures, fmt.Errorf("jmap: update failed for %s", native))
		}
	}
	return errors.Join(failures...)
}

// pointerToken escapes a keyword for use inside a JSON Pointer patch path
// (RFC 6901): ~ becomes ~0 and / becomes ~1.
func pointerToken(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
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

// errIncompleteBody reports a provider body value that explicitly flagged
// itself truncated or undecodable; the engine must not cache it as complete.
var errIncompleteBody = errors.New("provider returned incomplete body content")
