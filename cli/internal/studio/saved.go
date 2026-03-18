package studio

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// SavedQuery is a named, persisted SQL query.
type SavedQuery struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	SQL       string `json:"sql"`
	CreatedAt string `json:"createdAt"`
}

// savedQueryStore persists saved queries to ~/.neutron/studio-saved.json.
type savedQueryStore struct {
	mu   sync.RWMutex
	path string
	data []SavedQuery
}

func newSavedQueryStore() (*savedQueryStore, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(home, ".neutron")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s := &savedQueryStore{path: filepath.Join(dir, "studio-saved.json")}
	_ = s.load() // ignore missing file on first run
	return s, nil
}

func (s *savedQueryStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &s.data)
}

func (s *savedQueryStore) flush() error {
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o600)
}

func (s *savedQueryStore) List() []SavedQuery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SavedQuery, len(s.data))
	copy(out, s.data)
	return out
}

func (s *savedQueryStore) Add(name, sql string) (SavedQuery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := SavedQuery{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Name:      name,
		SQL:       sql,
		CreatedAt: time.Now().Format(time.RFC3339),
	}
	s.data = append([]SavedQuery{q}, s.data...) // newest first
	return q, s.flush()
}

func (s *savedQueryStore) Remove(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	filtered := s.data[:0]
	found := false
	for _, q := range s.data {
		if q.ID == id {
			found = true
		} else {
			filtered = append(filtered, q)
		}
	}
	if !found {
		return fmt.Errorf("saved query %q not found", id)
	}
	s.data = filtered
	return s.flush()
}

// --- HTTP handlers ---

func (s *Server) handleSavedQueries(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.saved.List())
	case http.MethodPost:
		var body struct {
			Name string `json:"name"`
			SQL  string `json:"sql"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if body.Name == "" || body.SQL == "" {
			writeError(w, http.StatusBadRequest, "name and sql are required")
			return
		}
		q, err := s.saved.Add(body.Name, body.SQL)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, q)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleSavedQuery(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/saved-queries/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id required")
		return
	}
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "DELETE required")
		return
	}
	if err := s.saved.Remove(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
