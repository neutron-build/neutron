package studio

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// SavedConnection is stored server-side in ~/.neutron/studio.json.
// The raw URL is kept here; only a masked version is sent to the browser.
type SavedConnection struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	URL       string `json:"url"` // full URL — never exposed to browser
	IsNucleus bool   `json:"isNucleus"`
}

// MaskedURL replaces the password portion of a Postgres URL with ***.
func MaskedURL(url string) string {
	// postgres://user:pass@host/db → postgres://user:***@host/db
	if i := strings.Index(url, "://"); i >= 0 {
		rest := url[i+3:]
		if at := strings.LastIndex(rest, "@"); at >= 0 {
			userInfo := rest[:at]
			hostPart := rest[at:]
			if colon := strings.Index(userInfo, ":"); colon >= 0 {
				return url[:i+3] + userInfo[:colon+1] + "***" + hostPart
			}
		}
	}
	return url
}

type connectionStore struct {
	mu          sync.RWMutex
	connections []SavedConnection
	path        string
}

func newConnectionStore() (*connectionStore, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("home dir: %w", err)
	}
	dir := filepath.Join(home, ".neutron")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create ~/.neutron: %w", err)
	}
	path := filepath.Join(dir, "studio.json")
	s := &connectionStore{path: path}
	if err := s.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load connections: %w", err)
	}
	return s, nil
}

func (s *connectionStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &s.connections)
}

func (s *connectionStore) save() error {
	data, err := json.MarshalIndent(s.connections, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0600)
}

func (s *connectionStore) List() []SavedConnection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SavedConnection, len(s.connections))
	copy(out, s.connections)
	return out
}

func (s *connectionStore) Add(name, url string) (SavedConnection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, err := newID()
	if err != nil {
		return SavedConnection{}, err
	}
	conn := SavedConnection{ID: id, Name: name, URL: url}
	s.connections = append(s.connections, conn)
	return conn, s.save()
}

func (s *connectionStore) Remove(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	filtered := s.connections[:0]
	for _, c := range s.connections {
		if c.ID == id {
			found = true
		} else {
			filtered = append(filtered, c)
		}
	}
	if !found {
		return fmt.Errorf("connection %q not found", id)
	}
	s.connections = filtered
	return s.save()
}

func (s *connectionStore) Get(id string) (SavedConnection, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.connections {
		if c.ID == id {
			return c, true
		}
	}
	return SavedConnection{}, false
}

func (s *connectionStore) SetNucleus(id string, isNucleus bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.connections {
		if s.connections[i].ID == id {
			s.connections[i].IsNucleus = isNucleus
			s.save() //nolint
			return
		}
	}
}

func newID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
