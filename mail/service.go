package mail

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Version is reported by the health endpoint.
const Version = "0.1.0"

// Service exposes the mirror over HTTP.
//
// Consumers never speak a mail protocol: Akiroo renders an inbox from these
// endpoints and Fylun's chat calls the same ones as a tool. That is the whole
// point of the split — one engine, two faces, and neither face has to know
// what IMAP is.
type Service struct {
	store Store
	eng   *Engine

	// Adapters resolves an account to its live adapter. Sync and body
	// fetches need one; pure reads against the mirror do not, which is why
	// an account with no adapter still serves search and thread requests.
	Adapters func(AccountID) (Adapter, bool)

	// Senders resolves an account to its outbound SMTP sender. Sending is
	// SMTP submission on every provider that supports it, so it is resolved
	// separately from the read adapter rather than folded into it.
	Senders func(AccountID) (*Sender, Address, bool)
}

// NewService builds the HTTP surface over a store and engine.
func NewService(store Store, eng *Engine) *Service {
	return &Service{store: store, eng: eng}
}

// Handler returns the routed HTTP handler.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /v1/accounts", s.listAccounts)
	mux.HandleFunc("GET /v1/accounts/{account}/mailboxes", s.listMailboxes)
	mux.HandleFunc("GET /v1/accounts/{account}/search", s.search)
	mux.HandleFunc("GET /v1/accounts/{account}/threads/{thread}", s.thread)
	mux.HandleFunc("GET /v1/accounts/{account}/messages/{message}", s.message)
	mux.HandleFunc("GET /v1/accounts/{account}/messages/{message}/body", s.body)
	mux.HandleFunc("POST /v1/accounts/{account}/sync", s.sync)
	mux.HandleFunc("POST /v1/accounts/{account}/operations", s.operation)
	mux.HandleFunc("POST /v1/accounts/{account}/send", s.send)

	return mux
}

// send submits a new message or a reply.
//
// A reply carries reply_to_message_id, and the threading chain is built from
// the stored parent rather than trusted from the caller: In-Reply-To and
// References have to reference the real parent or the reply silently starts a
// new conversation in the recipient's client.
func (s *Service) send(w http.ResponseWriter, r *http.Request) {
	acct := AccountID(r.PathValue("account"))

	var req struct {
		To               []Address `json:"to"`
		Cc               []Address `json:"cc,omitempty"`
		Bcc              []Address `json:"bcc,omitempty"`
		Subject          string    `json:"subject"`
		Text             string    `json:"text,omitempty"`
		HTML             string    `json:"html,omitempty"`
		ReplyToMessageID MessageID `json:"reply_to_message_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Malformed Request", err.Error())
		return
	}

	sender, from, ok := s.sender(acct)
	if !ok {
		writeProblem(w, http.StatusServiceUnavailable, "No Sender",
			fmt.Sprintf("account %s has no configured outbound sender", acct))
		return
	}

	var msg *Outgoing
	if req.ReplyToMessageID != "" {
		parent, err := s.store.Envelope(r.Context(), acct, req.ReplyToMessageID)
		if err != nil {
			writeError(w, err)
			return
		}
		msg = ReplyTo(parent, from, req.Text)
		if req.Subject != "" {
			msg.Subject = req.Subject
		}
		if len(req.To) > 0 {
			msg.To = req.To
		}
		msg.HTML = req.HTML
	} else {
		msg = &Outgoing{
			From: from, To: req.To, Cc: req.Cc, Bcc: req.Bcc,
			Subject: req.Subject, Text: req.Text, HTML: req.HTML,
		}
	}

	messageID, err := sender.Send(r.Context(), msg)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, map[string]any{"message_id": messageID})
}

func (s *Service) sender(acct AccountID) (*Sender, Address, bool) {
	if s.Senders == nil {
		return nil, Address{}, false
	}
	return s.Senders(acct)
}

// ---------------------------------------------------------------------------
// Errors — RFC 7807, matching every other Neutron SDK.
// ---------------------------------------------------------------------------

type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

func writeProblem(w http.ResponseWriter, status int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{
		Type:   "about:blank",
		Title:  title,
		Status: status,
		Detail: detail,
	})
}

// writeError maps engine errors onto status codes.
//
// The mapping is not cosmetic: a client has to be able to tell "reconnect
// this account" from "try again shortly" without parsing prose.
func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNoStore), errors.Is(err, ErrNotFound):
		writeProblem(w, http.StatusNotFound, "Not Found", err.Error())
	case errors.Is(err, ErrReauthRequired):
		// 401 rather than 403: the credential is the problem, and the fix
		// is for the user to reconnect the account.
		writeProblem(w, http.StatusUnauthorized, "Reauthentication Required", err.Error())
	case errors.Is(err, ErrRateLimited):
		writeProblem(w, http.StatusTooManyRequests, "Rate Limited", err.Error())
	default:
		writeProblem(w, http.StatusInternalServerError, "Internal Server Error", err.Error())
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		writeProblem(w, http.StatusInternalServerError, "Encoding Failed", err.Error())
	}
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func (s *Service) health(w http.ResponseWriter, r *http.Request) {
	status := "ok"
	nucleus := true
	if _, err := s.store.Accounts(r.Context()); err != nil {
		status = "degraded"
		nucleus = false
	}
	writeJSON(w, map[string]any{
		"status":  status,
		"nucleus": nucleus,
		"version": Version,
	})
}

func (s *Service) listAccounts(w http.ResponseWriter, r *http.Request) {
	accounts, err := s.store.Accounts(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, map[string]any{"accounts": accounts})
}

func (s *Service) listMailboxes(w http.ResponseWriter, r *http.Request) {
	acct := AccountID(r.PathValue("account"))
	boxes, err := s.store.Mailboxes(r.Context(), acct)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, map[string]any{"mailboxes": boxes})
}

func (s *Service) search(w http.ResponseWriter, r *http.Request) {
	acct := AccountID(r.PathValue("account"))
	q := r.URL.Query().Get("q")
	if strings.TrimSpace(q) == "" {
		writeProblem(w, http.StatusBadRequest, "Missing Query", "the q parameter is required")
		return
	}

	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			// Capped so one call cannot ask for the whole mailbox and time
			// out behind a chat tool's request budget.
			limit = min(n, 200)
		}
	}

	results, err := s.store.Search(r.Context(), acct, q, limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, map[string]any{"messages": results, "count": len(results)})
}

func (s *Service) thread(w http.ResponseWriter, r *http.Request) {
	acct := AccountID(r.PathValue("account"))
	msgs, err := s.store.Thread(r.Context(), acct, ThreadID(r.PathValue("thread")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, map[string]any{"messages": msgs, "count": len(msgs)})
}

func (s *Service) message(w http.ResponseWriter, r *http.Request) {
	acct := AccountID(r.PathValue("account"))
	env, err := s.store.Envelope(r.Context(), acct, MessageID(r.PathValue("message")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, env)
}

func (s *Service) body(w http.ResponseWriter, r *http.Request) {
	acct := AccountID(r.PathValue("account"))
	id := MessageID(r.PathValue("message"))

	// Cached bodies are served without touching the provider. Only a miss
	// needs an adapter, which is why the lookup order is store first.
	if body, err := s.store.Body(r.Context(), acct, id); err == nil {
		writeJSON(w, body)
		return
	}

	ad, ok := s.adapter(acct)
	if !ok {
		writeProblem(w, http.StatusServiceUnavailable, "No Adapter",
			fmt.Sprintf("account %s has no connected adapter to fetch from", acct))
		return
	}

	body, err := s.eng.Body(r.Context(), acct, id, ad)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, body)
}

func (s *Service) sync(w http.ResponseWriter, r *http.Request) {
	acct := AccountID(r.PathValue("account"))
	ad, ok := s.adapter(acct)
	if !ok {
		writeProblem(w, http.StatusServiceUnavailable, "No Adapter",
			fmt.Sprintf("account %s has no connected adapter", acct))
		return
	}

	reports, err := s.eng.SyncAccount(r.Context(), acct, ad)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, map[string]any{"reports": reports})
}

func (s *Service) operation(w http.ResponseWriter, r *http.Request) {
	acct := AccountID(r.PathValue("account"))

	var req struct {
		Kind    string      `json:"kind"`
		IDs     []MessageID `json:"ids"`
		Keyword string      `json:"keyword,omitempty"`
		Target  MailboxID   `json:"target,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Malformed Request", err.Error())
		return
	}

	kind, ok := parseOpKind(req.Kind)
	if !ok {
		writeProblem(w, http.StatusBadRequest, "Unknown Operation",
			fmt.Sprintf("%q is not one of add_keyword, remove_keyword, move, delete", req.Kind))
		return
	}
	if len(req.IDs) == 0 {
		writeProblem(w, http.StatusBadRequest, "No Messages", "ids must not be empty")
		return
	}

	ad, ok := s.adapter(acct)
	if !ok {
		writeProblem(w, http.StatusServiceUnavailable, "No Adapter",
			fmt.Sprintf("account %s has no connected adapter", acct))
		return
	}

	err := s.eng.Apply(r.Context(), acct, Operation{
		Kind: kind, IDs: req.IDs, Keyword: req.Keyword, Target: req.Target,
	}, ad)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, map[string]any{"applied": len(req.IDs)})
}

func parseOpKind(s string) (OpKind, bool) {
	switch strings.ToLower(s) {
	case "add_keyword":
		return OpAddKeyword, true
	case "remove_keyword":
		return OpRemoveKeyword, true
	case "move":
		return OpMove, true
	case "delete":
		return OpDelete, true
	default:
		return 0, false
	}
}

func (s *Service) adapter(acct AccountID) (Adapter, bool) {
	if s.Adapters == nil {
		return nil, false
	}
	return s.Adapters(acct)
}
