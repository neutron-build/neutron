package studio

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// sanitizeError strips potentially sensitive information (e.g. credentials in URLs)
// from error messages before sending them to the HTTP client.
func sanitizeError(err error) string {
	msg := err.Error()
	// Strip credentials from any postgres:// or other scheme URLs in the error
	// by redacting userinfo portions.
	for _, scheme := range []string{"postgres://", "postgresql://", "http://", "https://"} {
		idx := strings.Index(msg, scheme)
		if idx < 0 {
			continue
		}
		// Parse the URL portion out of the error string
		urlStart := idx
		urlEnd := len(msg)
		for i := urlStart; i < len(msg); i++ {
			if msg[i] == ' ' || msg[i] == '"' || msg[i] == '\'' {
				urlEnd = i
				break
			}
		}
		rawURL := msg[urlStart:urlEnd]
		if u, parseErr := url.Parse(rawURL); parseErr == nil && u.User != nil {
			redacted := u.Redacted()
			msg = msg[:urlStart] + redacted + msg[urlEnd:]
		}
	}
	return msg
}

// --- /api/connections ---

func (s *Server) handleConnections(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listConnections(w, r)
	case http.MethodPost:
		s.addConnection(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

type connResponse struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	URL       string `json:"url"` // masked
	IsNucleus bool   `json:"isNucleus"`
}

func toResponse(c SavedConnection) connResponse {
	return connResponse{
		ID:        c.ID,
		Name:      c.Name,
		URL:       MaskedURL(c.URL),
		IsNucleus: c.IsNucleus,
	}
}

func (s *Server) listConnections(w http.ResponseWriter, r *http.Request) {
	list := s.store.List()
	out := make([]connResponse, len(list))
	for i, c := range list {
		out[i] = toResponse(c)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) addConnection(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if body.Name == "" || body.URL == "" {
		writeError(w, http.StatusBadRequest, "name and url are required")
		return
	}
	conn, err := s.store.Add(body.Name, body.URL)
	if err != nil {
		log.Printf("studio: add connection error: %v", err)
		writeError(w, http.StatusInternalServerError, "Internal server error")
		return
	}
	writeJSON(w, http.StatusOK, toResponse(conn))
}

// --- /api/connections/test ---

func (s *Server) handleTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var body struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	client, err := db.Connect(ctx, body.URL)
	if err != nil {
		log.Printf("studio: test connection error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "isNucleus": false, "version": "", "error": sanitizeError(err)})
		return
	}
	defer client.Close()

	isNucleus, version, err := client.IsNucleus(ctx)
	if err != nil {
		log.Printf("studio: test connection nucleus check error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "isNucleus": false, "version": "", "error": sanitizeError(err)})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "isNucleus": isNucleus, "version": version})
}

// --- /api/connections/:id and /api/connections/:id/connect ---

func (s *Server) handleConnection(w http.ResponseWriter, r *http.Request) {
	// Path: /api/connections/{id} or /api/connections/{id}/connect
	path := strings.TrimPrefix(r.URL.Path, "/api/connections/")
	parts := strings.SplitN(path, "/", 2)
	id := parts[0]
	sub := ""
	if len(parts) == 2 {
		sub = parts[1]
	}

	switch {
	case sub == "connect" && r.Method == http.MethodPost:
		s.connectDB(w, r, id)
	case sub == "" && r.Method == http.MethodDelete:
		s.removeConnection(w, r, id)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) removeConnection(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.store.Remove(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	// Close any active client
	s.mu.Lock()
	if c, ok := s.clients[id]; ok {
		c.Close()
		delete(s.clients, id)
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) connectDB(w http.ResponseWriter, r *http.Request, id string) {
	saved, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("connection %q not found", id))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	client, err := db.Connect(ctx, saved.URL)
	if err != nil {
		log.Printf("studio: connect error for %s: %v", id, err)
		writeError(w, http.StatusBadGateway, fmt.Sprintf("connect: %s", sanitizeError(err)))
		return
	}
	s.setClient(id, client)

	isNucleus, version, err := client.IsNucleus(ctx)
	if err != nil {
		log.Printf("studio: nucleus check error for %s: %v", id, err)
		writeError(w, http.StatusBadGateway, sanitizeError(err))
		return
	}
	s.store.SetNucleus(id, isNucleus)

	features := map[string]any{
		"isNucleus": isNucleus,
		"version":   version,
		"models":    nucleusModels(isNucleus),
	}

	sc, err := FetchSchema(ctx, client, isNucleus)
	if err != nil {
		log.Printf("studio: fetch schema error for %s: %v", id, err)
		writeError(w, http.StatusInternalServerError, "Failed to fetch schema")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"features": features,
		"schema":   sc,
	})
}

func nucleusModels(isNucleus bool) []string {
	if !isNucleus {
		return []string{"sql"}
	}
	return []string{"sql", "kv", "vector", "timeseries", "document", "graph", "fts", "geo", "blob", "pubsub", "streams", "columnar", "datalog", "cdc"}
}

// --- /api/query ---

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var body struct {
		SQL          string `json:"sql"`
		ConnectionID string `json:"connectionId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	client, ok := s.clientFor(body.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected — call /api/connections/:id/connect first")
		return
	}

	start := time.Now()
	rows, err := client.Query(r.Context(), body.SQL)
	if err != nil {
		log.Printf("studio: query error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{
			"columns":  []string{},
			"rows":     [][]any{},
			"rowCount": 0,
			"duration": time.Since(start).Milliseconds(),
			"error":    sanitizeError(err),
		})
		return
	}
	defer rows.Close()

	cols := make([]string, len(rows.FieldDescriptions()))
	for i, fd := range rows.FieldDescriptions() {
		cols[i] = string(fd.Name)
	}

	var data [][]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			continue
		}
		data = append(data, vals)
	}
	if data == nil {
		data = [][]any{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"columns":  cols,
		"rows":     data,
		"rowCount": len(data),
		"duration": time.Since(start).Milliseconds(),
	})
}

// --- /api/schema ---

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	connID := r.URL.Query().Get("connectionId")
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	saved, _ := s.store.Get(connID)
	sc, err := FetchSchema(r.Context(), client, saved.IsNucleus)
	if err != nil {
		log.Printf("studio: schema fetch error: %v", err)
		writeError(w, http.StatusInternalServerError, "Failed to fetch schema")
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

// --- /api/features ---

func (s *Server) handleFeatures(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	connID := r.URL.Query().Get("connectionId")
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	isNucleus, version, _ := client.IsNucleus(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{
		"isNucleus": isNucleus,
		"version":   version,
		"models":    nucleusModels(isNucleus),
	})
}

// --- /api/table ---

func (s *Server) handleTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	connID := q.Get("connectionId")
	schemaName := q.Get("schema")
	tableName := q.Get("table")
	limit := parseInt(q.Get("limit"), 200)
	offset := parseInt(q.Get("offset"), 0)

	if connID == "" || schemaName == "" || tableName == "" {
		writeError(w, http.StatusBadRequest, "connectionId, schema, and table are required")
		return
	}
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}

	// Use quoted identifiers to prevent SQL injection
	sql := fmt.Sprintf(
		`SELECT * FROM %s.%s LIMIT %d OFFSET %d`,
		quoteIdent(schemaName), quoteIdent(tableName), limit, offset,
	)

	start := time.Now()
	rows, err := client.Query(r.Context(), sql)
	if err != nil {
		log.Printf("studio: table query error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{
			"columns":  []string{},
			"rows":     [][]any{},
			"rowCount": 0,
			"duration": time.Since(start).Milliseconds(),
			"error":    sanitizeError(err),
		})
		return
	}
	defer rows.Close()

	cols := make([]string, len(rows.FieldDescriptions()))
	for i, fd := range rows.FieldDescriptions() {
		cols[i] = string(fd.Name)
	}

	var data [][]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			continue
		}
		data = append(data, vals)
	}
	if data == nil {
		data = [][]any{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"columns":  cols,
		"rows":     data,
		"rowCount": len(data),
		"duration": time.Since(start).Milliseconds(),
	})
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

// quoteIdent safely quotes a PostgreSQL identifier.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
