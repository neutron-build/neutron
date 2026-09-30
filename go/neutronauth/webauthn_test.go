package neutronauth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fxamacker/cbor/v2"
	"testing"
	"time"
)

type webauthnTestStore struct {
	challenges  map[string]string
	credentials []WebAuthnCredential
}

func newWebAuthnTestStore() *webauthnTestStore {
	return &webauthnTestStore{challenges: map[string]string{}}
}
func (m *webauthnTestStore) StoreChallenge(key, value string, _ time.Duration) error {
	m.challenges[key] = value
	return nil
}
func (m *webauthnTestStore) GetChallenge(key string) (string, error) {
	v, ok := m.challenges[key]
	delete(m.challenges, key)
	if !ok {
		return "", fmt.Errorf("missing challenge")
	}
	return v, nil
}
func (m *webauthnTestStore) SaveCredential(c WebAuthnCredential) error {
	m.credentials = append(m.credentials, c)
	return nil
}
func (m *webauthnTestStore) GetCredentialsByUser(user string) ([]WebAuthnCredential, error) {
	var result []WebAuthnCredential
	for _, c := range m.credentials {
		if c.UserID == user {
			result = append(result, c)
		}
	}
	return result, nil
}
func (m *webauthnTestStore) UpdateSignCount(id string, count uint32) error {
	for i := range m.credentials {
		if m.credentials[i].CredentialID == id {
			m.credentials[i].SignCount = count
			return nil
		}
	}
	return fmt.Errorf("missing credential")
}
func TestWebAuthnRejectsArbitraryByteScannedAttestation(t *testing.T) {
	store := newWebAuthnTestStore()
	service := NewWebAuthnService(WebAuthnConfig{RPID: "example.test", RPName: "Example", RPOrigin: "https://example.test"}, store)
	options, err := service.BeginRegistration("alice", "alice", "Alice")
	if err != nil {
		t.Fatal(err)
	}
	cd, _ := json.Marshal(map[string]any{"type": "webauthn.create", "challenge": options.Challenge, "origin": "https://example.test"})
	// Not a CBOR attestation at all, merely two byte-string markers and a
	// valid P256 point. The old scaffold accepted this as a credential.
	curve := elliptic.P256().Params()
	junk := []byte("not-an-attestation")
	junk = append(junk, 0x58, 0x20)
	junk = append(junk, curve.Gx.FillBytes(make([]byte, 32))...)
	junk = append(junk, 0x58, 0x20)
	junk = append(junk, curve.Gy.FillBytes(make([]byte, 32))...)
	response := RegistrationResponse{ID: base64.RawURLEncoding.EncodeToString([]byte("fake-id")), RawID: base64.RawURLEncoding.EncodeToString([]byte("fake-id")), Type: "public-key"}
	response.Response.ClientDataJSON = base64.RawURLEncoding.EncodeToString(cd)
	response.Response.AttestationObject = base64.RawURLEncoding.EncodeToString(junk)
	if _, err = service.FinishRegistration("alice", response); err == nil {
		t.Fatal("accepted arbitrary byte markers as verified WebAuthn registration")
	}
	if len(store.credentials) != 0 {
		t.Fatal("invalid registration persisted credential")
	}
}

// These fixtures implement the WebAuthn byte layout directly: no mocked
// verifier, canned library success result or browser-independent bypass.
func registrationFixture(t *testing.T, challenge, origin, rp string, flags byte) (RegistrationResponse, *ecdsa.PrivateKey) {
	t.Helper()
	return registrationFixtureWithID(t, challenge, origin, rp, flags, []byte("self-contained-credential"))
}
func registrationFixtureWithID(t *testing.T, challenge, origin, rp string, flags byte, id []byte) (RegistrationResponse, *ecdsa.PrivateKey) {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	idEncoded := base64.RawURLEncoding.EncodeToString(id)
	cose, e := cbor.Marshal(map[int]any{1: 2, 3: -7, -1: 1, -2: key.X.FillBytes(make([]byte, 32)), -3: key.Y.FillBytes(make([]byte, 32))})
	if e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256([]byte(rp))
	auth := append([]byte{}, hash[:]...)
	auth = append(auth, flags)
	auth = append(auth, 0, 0, 0, 0)
	auth = append(auth, make([]byte, 16)...)
	auth = binary.BigEndian.AppendUint16(auth, uint16(len(id)))
	auth = append(auth, id...)
	auth = append(auth, cose...)
	attestation, e := cbor.Marshal(map[string]any{"fmt": "none", "attStmt": map[string]any{}, "authData": auth})
	if e != nil {
		t.Fatal(e)
	}
	data, _ := json.Marshal(map[string]any{"type": "webauthn.create", "challenge": challenge, "origin": origin})
	response := RegistrationResponse{ID: idEncoded, RawID: idEncoded, Type: "public-key"}
	response.Response.ClientDataJSON = base64.RawURLEncoding.EncodeToString(data)
	response.Response.AttestationObject = base64.RawURLEncoding.EncodeToString(attestation)
	return response, key
}
func assertionFixture(t *testing.T, key *ecdsa.PrivateKey, challenge, origin, rp, id string, count uint32, flags byte) AuthenticationResponse {
	t.Helper()
	data, _ := json.Marshal(map[string]any{"type": "webauthn.get", "challenge": challenge, "origin": origin})
	hash := sha256.Sum256([]byte(rp))
	auth := append([]byte{}, hash[:]...)
	auth = append(auth, flags)
	auth = binary.BigEndian.AppendUint32(auth, count)
	clientHash := sha256.Sum256(data)
	signed := append(append([]byte{}, auth...), clientHash[:]...)
	digest := sha256.Sum256(signed)
	signature, e := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if e != nil {
		t.Fatal(e)
	}
	response := AuthenticationResponse{ID: id, RawID: id, Type: "public-key"}
	response.Response.ClientDataJSON = base64.RawURLEncoding.EncodeToString(data)
	response.Response.AuthenticatorData = base64.RawURLEncoding.EncodeToString(auth)
	response.Response.Signature = base64.RawURLEncoding.EncodeToString(signature)
	response.Response.UserHandle = base64.RawURLEncoding.EncodeToString([]byte("alice"))
	return response
}
func registeredFixture(t *testing.T) (*WebAuthnService, *MemoryWebAuthnStore, *ecdsa.PrivateKey, *WebAuthnCredential) {
	t.Helper()
	store := NewMemoryWebAuthnStore()
	service := NewWebAuthnService(WebAuthnConfig{RPID: "example.test", RPName: "Example", RPOrigin: "https://example.test"}, store)
	opts, e := service.BeginRegistration("alice", "alice", "Alice")
	if e != nil {
		t.Fatal(e)
	}
	if opts.AuthenticatorSel.UserVerification != "required" {
		t.Fatal("unsafe default verification policy")
	}
	response, key := registrationFixture(t, opts.Challenge, "https://example.test", "example.test", 0x45)
	credential, e := service.FinishRegistration("alice", response)
	if e != nil {
		t.Fatal(e)
	}
	return service, store, key, credential
}
func TestWebAuthnCompleteRegistrationAndAssertion(t *testing.T) {
	service, store, key, credential := registeredFixture(t)
	if credential.VerifiedCredential == nil || credential.Revision == "" {
		t.Fatal("lost verified credential metadata")
	}
	opts, e := service.BeginAuthentication("alice")
	if e != nil {
		t.Fatal(e)
	}
	response := assertionFixture(t, key, opts.Challenge, "https://example.test", "example.test", credential.CredentialID, 1, 0x05)
	updated, e := service.FinishAuthentication("alice", response)
	if e != nil {
		t.Fatal(e)
	}
	if updated.SignCount != 1 || updated.Revision == credential.Revision {
		t.Fatal("assertion not revision-committed")
	}
	persisted, _ := store.GetCredentialsByUser("alice")
	if len(persisted) != 1 || persisted[0].SignCount != 1 {
		t.Fatal("counter not persisted")
	}
	if _, e = service.FinishAuthentication("alice", response); e == nil {
		t.Fatal("replayed consumed challenge succeeded")
	}
}
func TestWebAuthnRegistrationNegativeControls(t *testing.T) {
	cases := []struct {
		name, origin, rp string
		flags            byte
		wrongChallenge   bool
	}{
		{"origin", "https://attacker.test", "example.test", 0x45, false},
		{"RPID", "https://example.test", "attacker.test", 0x45, false},
		{"challenge", "https://example.test", "example.test", 0x45, true},
		{"presence", "https://example.test", "example.test", 0x44, false},
		{"verification", "https://example.test", "example.test", 0x41, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := NewMemoryWebAuthnStore()
			service := NewWebAuthnService(WebAuthnConfig{RPID: "example.test", RPName: "Example", RPOrigin: "https://example.test"}, store)
			opts, e := service.BeginRegistration("alice", "alice", "Alice")
			if e != nil {
				t.Fatal(e)
			}
			challenge := opts.Challenge
			if c.wrongChallenge {
				challenge = "wrong"
			}
			response, _ := registrationFixture(t, challenge, c.origin, c.rp, c.flags)
			if _, e = service.FinishRegistration("alice", response); e == nil {
				t.Fatal("invalid registration accepted")
			}
			rows, _ := store.GetCredentialsByUser("alice")
			if len(rows) != 0 {
				t.Fatal("invalid credential persisted")
			}
		})
	}
}
func TestWebAuthnAssertionNegativeControls(t *testing.T) {
	for _, name := range []string{"origin", "RPID", "challenge", "presence", "verification", "signature", "counter"} {
		t.Run(name, func(t *testing.T) {
			service, store, key, credential := registeredFixture(t)
			opts, e := service.BeginAuthentication("alice")
			if e != nil {
				t.Fatal(e)
			}
			origin, rp, challenge, flags := "https://example.test", "example.test", opts.Challenge, byte(0x05)
			if name == "origin" {
				origin = "https://attacker.test"
			}
			if name == "RPID" {
				rp = "attacker.test"
			}
			if name == "challenge" {
				challenge = "wrong"
			}
			if name == "presence" {
				flags = 0x04
			}
			if name == "verification" {
				flags = 0x01
			}
			if name == "counter" {
				store.UpdateSignCount(credential.CredentialID, 10)
			}
			response := assertionFixture(t, key, challenge, origin, rp, credential.CredentialID, 1, flags)
			if name == "signature" {
				response.Response.Signature = base64.RawURLEncoding.EncodeToString([]byte("invalid"))
			}
			if _, e = service.FinishAuthentication("alice", response); e == nil {
				t.Fatal("invalid assertion accepted")
			}
			rows, _ := store.GetCredentialsByUser("alice")
			want := uint32(0)
			if name == "counter" {
				want = 10
			}
			if rows[0].SignCount != want {
				t.Fatal("failed assertion changed counter")
			}
		})
	}
}

type conflictingCredentialStore struct{ *MemoryWebAuthnStore }

func (s conflictingCredentialStore) CompareAndSwapCredential(string, string, WebAuthnCredential) error {
	return ErrCredentialConflict
}
func TestWebAuthnAtomicCommitFailureRejectsAuthentication(t *testing.T) {
	service, store, key, credential := registeredFixture(t)
	service.store = conflictingCredentialStore{store}
	opts, e := service.BeginAuthentication("alice")
	if e != nil {
		t.Fatal(e)
	}
	response := assertionFixture(t, key, opts.Challenge, "https://example.test", "example.test", credential.CredentialID, 1, 0x05)
	if _, e = service.FinishAuthentication("alice", response); !errors.Is(e, ErrCredentialConflict) {
		t.Fatalf("CAS failure did not fail login: %v", e)
	}
	rows, _ := store.GetCredentialsByUser("alice")
	if rows[0].SignCount != 0 {
		t.Fatal("uncommitted counter changed")
	}
}
func TestWebAuthnStoredRevisionCASAndCeremonyExpiry(t *testing.T) {
	_, store, _, credential := registeredFixture(t)
	expected := credential.Revision
	replacement := *credential
	replacement.Revision = generateSessionID()
	if e := store.CompareAndSwapCredential(credential.CredentialID, expected, replacement); e != nil {
		t.Fatal(e)
	}
	replacement.Revision = generateSessionID()
	if e := store.CompareAndSwapCredential(credential.CredentialID, expected, replacement); !errors.Is(e, ErrCredentialConflict) {
		t.Fatal("stale credential overwrite")
	}
	store.StoreChallenge("expired", "state", time.Nanosecond)
	time.Sleep(time.Millisecond)
	if _, e := store.GetChallenge("expired"); e == nil {
		t.Fatal("expired challenge returned")
	}
}
func TestWebAuthnLegacyCredentialAndUnversionedStoreRefused(t *testing.T) {
	legacy := newWebAuthnTestStore()
	service := NewWebAuthnService(WebAuthnConfig{RPID: "example.test", RPName: "Example", RPOrigin: "https://example.test"}, legacy)
	if _, e := service.BeginAuthentication("alice"); e == nil {
		t.Fatal("unversioned credential store accepted")
	}
	store := NewMemoryWebAuthnStore()
	store.SaveCredential(WebAuthnCredential{CredentialID: "legacy", UserID: "alice", PublicKey: "unverified"})
	service.store = store
	if _, e := service.BeginAuthentication("alice"); e == nil {
		t.Fatal("legacy unverified credential accepted")
	}
}
