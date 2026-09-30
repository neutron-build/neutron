package neutronauth

import (
	"encoding/json"
	"errors"
	"sync"
	"time"
)

var ErrCredentialConflict = errors.New("neutronauth: credential revision changed")

type ceremonyRecord struct {
	state   string
	expires time.Time
}

// MemoryWebAuthnStore atomically consumes challenges and fences credential
// commits. It is bounded and single-process; use an equivalent database-backed
// AtomicWebAuthnStore for persistent credentials or multiple application nodes.
type MemoryWebAuthnStore struct {
	mu          sync.Mutex
	challenges  map[string]ceremonyRecord
	credentials map[string]WebAuthnCredential
	capacity    int
}

func NewMemoryWebAuthnStore(capacity ...int) *MemoryWebAuthnStore {
	n := 10000
	if len(capacity) > 0 && capacity[0] > 0 {
		n = capacity[0]
	}
	return &MemoryWebAuthnStore{challenges: make(map[string]ceremonyRecord), credentials: make(map[string]WebAuthnCredential), capacity: n}
}
func (m *MemoryWebAuthnStore) StoreChallenge(key, state string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for k, v := range m.challenges {
		if !v.expires.After(now) {
			delete(m.challenges, k)
		}
	}
	if _, exists := m.challenges[key]; !exists && len(m.challenges) >= m.capacity {
		return errors.New("neutronauth: ceremony capacity exceeded")
	}
	if ttl <= 0 {
		return errors.New("neutronauth: ceremony TTL must be positive")
	}
	m.challenges[key] = ceremonyRecord{state: state, expires: now.Add(ttl)}
	return nil
}
func (m *MemoryWebAuthnStore) GetChallenge(key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.challenges[key]
	delete(m.challenges, key)
	if !ok || !r.expires.After(time.Now()) {
		return "", errors.New("neutronauth: ceremony expired or absent")
	}
	return r.state, nil
}
func cloneWebAuthnCredential(c WebAuthnCredential) (WebAuthnCredential, error) {
	b, e := json.Marshal(c)
	if e != nil {
		return WebAuthnCredential{}, e
	}
	var r WebAuthnCredential
	e = json.Unmarshal(b, &r)
	return r, e
}
func (m *MemoryWebAuthnStore) SaveCredential(c WebAuthnCredential) error {
	clone, e := cloneWebAuthnCredential(c)
	if e != nil {
		return e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.credentials[c.CredentialID]; ok {
		return ErrCredentialConflict
	}
	if len(m.credentials) >= m.capacity {
		return errors.New("neutronauth: credential capacity exceeded")
	}
	m.credentials[c.CredentialID] = clone
	return nil
}
func (m *MemoryWebAuthnStore) GetCredentialsByUser(user string) ([]WebAuthnCredential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := []WebAuthnCredential{}
	for _, r := range m.credentials {
		if r.UserID == user {
			clone, e := cloneWebAuthnCredential(r)
			if e != nil {
				return nil, e
			}
			result = append(result, clone)
		}
	}
	return result, nil
}

// UpdateSignCount is retained for source compatibility only. Verified login
// always calls CompareAndSwapCredential with the loaded revision instead.
func (m *MemoryWebAuthnStore) UpdateSignCount(id string, count uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.credentials[id]
	if !ok {
		return ErrCredentialConflict
	}
	r.SignCount = count
	if r.VerifiedCredential != nil {
		r.VerifiedCredential.Authenticator.SignCount = count
	}
	r.Revision = generateSessionID()
	m.credentials[id] = r
	return nil
}
func (m *MemoryWebAuthnStore) CompareAndSwapCredential(id, expected string, replacement WebAuthnCredential) error {
	clone, e := cloneWebAuthnCredential(replacement)
	if e != nil {
		return e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.credentials[id]
	if !ok || expected == "" || current.Revision != expected || replacement.CredentialID != id || replacement.UserID != current.UserID || replacement.Revision == "" || replacement.Revision == expected {
		return ErrCredentialConflict
	}
	m.credentials[id] = clone
	return nil
}
