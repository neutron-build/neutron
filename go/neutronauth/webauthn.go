package neutronauth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/protocol/webauthncose"
	wa "github.com/go-webauthn/webauthn/webauthn"
	"github.com/neutron-build/neutron/go/neutron"
)

// ---------------------------------------------------------------------------
// Data types — WebAuthn credentials and ceremony payloads
// ---------------------------------------------------------------------------

// WebAuthnCredential represents a stored passkey / platform authenticator credential.
type WebAuthnCredential struct {
	// Complete library-verified COSE key, flags and attestation metadata.
	VerifiedCredential *wa.Credential `json:"verified_credential,omitempty"`
	// Opaque revision fences concurrent assertion commits, including zero counters.
	Revision string `json:"revision,omitempty"`
	// CredentialID is the unique identifier assigned by the authenticator (base64url).
	CredentialID string `json:"credential_id"`
	// PublicKey is the legacy SEC1 field. New ceremonies use VerifiedCredential;
	// credentials lacking verified metadata must be registered again.
	PublicKey string `json:"public_key"`
	// UserID is the application's user identifier that owns this credential.
	UserID string `json:"user_id"`
	// SignCount is the last known signature counter for clone detection.
	SignCount uint32 `json:"sign_count"`
	// CreatedAt is the time the credential was registered.
	CreatedAt time.Time `json:"created_at"`
	// AAGUID is the authenticator attestation GUID, if available.
	AAGUID string `json:"aaguid,omitempty"`
}

// WebAuthnConfig configures the WebAuthn relying party.
type WebAuthnConfig struct {
	// Defaults to "required". Explicit "preferred" allows presence-only
	// authenticators; applications must not claim MFA for that policy.
	UserVerification string
	// RPID is the relying party identifier (typically the domain, e.g. "example.com").
	RPID string
	// RPName is the human-readable relying party name shown to the user.
	RPName string
	// RPOrigin is the expected origin (e.g. "https://example.com").
	RPOrigin string
	// Timeout is the ceremony timeout sent to the browser (default: 60s).
	Timeout time.Duration
}

// ---------------------------------------------------------------------------
// Registration ceremony types
// ---------------------------------------------------------------------------

// RegistrationOptions is sent to the browser to start navigator.credentials.create().
type RegistrationOptions struct {
	Challenge        string                 `json:"challenge"`
	RP               rpEntity               `json:"rp"`
	User             userEntity             `json:"user"`
	PubKeyCredParams []pubKeyCredParam      `json:"pubKeyCredParams"`
	Timeout          int                    `json:"timeout"`
	Attestation      string                 `json:"attestation"`
	AuthenticatorSel authenticatorSelection `json:"authenticatorSelection"`
}

type rpEntity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type userEntity struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
}

type pubKeyCredParam struct {
	Type string `json:"type"`
	Alg  int    `json:"alg"`
}

type authenticatorSelection struct {
	AuthenticatorAttachment string `json:"authenticatorAttachment,omitempty"`
	ResidentKey             string `json:"residentKey"`
	UserVerification        string `json:"userVerification"`
}

// RegistrationResponse is the JSON payload the browser sends back after
// navigator.credentials.create() succeeds.
type RegistrationResponse struct {
	ClientExtensionResults  map[string]any `json:"clientExtensionResults,omitempty"`
	AuthenticatorAttachment string         `json:"authenticatorAttachment,omitempty"`
	ID                      string         `json:"id"`
	RawID                   string         `json:"rawId"`
	Type                    string         `json:"type"`
	Response                struct {
		AttestationObject string                            `json:"attestationObject"`
		Transports        []protocol.AuthenticatorTransport `json:"transports,omitempty"`
		ClientDataJSON    string                            `json:"clientDataJSON"`
	} `json:"response"`
}

// ---------------------------------------------------------------------------
// Authentication ceremony types
// ---------------------------------------------------------------------------

// AuthenticationOptions is sent to the browser to start navigator.credentials.get().
type AuthenticationOptions struct {
	Challenge        string                `json:"challenge"`
	RPID             string                `json:"rpId"`
	Timeout          int                   `json:"timeout"`
	UserVerification string                `json:"userVerification"`
	AllowCredentials []allowCredentialDesc `json:"allowCredentials,omitempty"`
}

type allowCredentialDesc struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// AuthenticationResponse is the JSON payload the browser sends back after
// navigator.credentials.get() succeeds.
type AuthenticationResponse struct {
	ClientExtensionResults  map[string]any `json:"clientExtensionResults,omitempty"`
	AuthenticatorAttachment string         `json:"authenticatorAttachment,omitempty"`
	ID                      string         `json:"id"`
	RawID                   string         `json:"rawId"`
	Type                    string         `json:"type"`
	Response                struct {
		AuthenticatorData string `json:"authenticatorData"`
		ClientDataJSON    string `json:"clientDataJSON"`
		Signature         string `json:"signature"`
		UserHandle        string `json:"userHandle,omitempty"`
	} `json:"response"`
}

// ---------------------------------------------------------------------------
// ClientData — parsed clientDataJSON
// ---------------------------------------------------------------------------

type clientData struct {
	Type      string `json:"type"`
	Challenge string `json:"challenge"`
	Origin    string `json:"origin"`
}

// ---------------------------------------------------------------------------
// WebAuthnService — registration and authentication flow orchestration
// ---------------------------------------------------------------------------

// WebAuthnStore is the interface for persisting WebAuthn credentials and challenges.
type WebAuthnStore interface {
	// StoreChallenge stores complete serialized SessionData with enforced TTL.
	StoreChallenge(key, challenge string, ttl time.Duration) error
	// GetChallenge atomically retrieves and deletes serialized ceremony state;
	// concurrent callers must not receive the same state.
	GetChallenge(key string) (string, error)
	// SaveCredential persists the complete credential and rejects duplicate IDs.
	SaveCredential(cred WebAuthnCredential) error
	// GetCredentialsByUser returns all credentials for a user.
	GetCredentialsByUser(userID string) ([]WebAuthnCredential, error)
	// UpdateSignCount updates the signature counter for clone detection.
	UpdateSignCount(credentialID string, newCount uint32) error
}

// AtomicWebAuthnStore commits an assertion only if expectedRevision matches
// the stored credential, in one atomic operation. Legacy stores remain
// source-compatible but authentication fails closed without this capability.
type AtomicWebAuthnStore interface {
	WebAuthnStore
	CompareAndSwapCredential(credentialID, expectedRevision string, replacement WebAuthnCredential) error
}

// WebAuthnService delegates complete ceremony verification to go-webauthn.
// StoreChallenge/GetChallenge persist and atomically consume the ENTIRE
// serialized library SessionData, not just its challenge string.
type WebAuthnService struct {
	config   WebAuthnConfig
	store    WebAuthnStore
	verifier *wa.WebAuthn
	initErr  error
}

func NewWebAuthnService(config WebAuthnConfig, store WebAuthnStore) *WebAuthnService {
	if config.Timeout == 0 {
		config.Timeout = 60 * time.Second
	}
	verification := protocol.UserVerificationRequirement(config.UserVerification)
	if verification == "" {
		verification = protocol.VerificationRequired
	}
	s := &WebAuthnService{config: config, store: store}
	if config.Timeout <= 0 || (verification != protocol.VerificationRequired && verification != protocol.VerificationPreferred && verification != protocol.VerificationDiscouraged) {
		s.initErr = fmt.Errorf("neutronauth: invalid WebAuthn policy")
		return s
	}
	s.verifier, s.initErr = wa.New(&wa.Config{RPID: config.RPID, RPDisplayName: config.RPName, RPOrigins: []string{config.RPOrigin}, AttestationPreference: protocol.PreferNoAttestation, AuthenticatorSelection: protocol.AuthenticatorSelection{ResidentKey: protocol.ResidentKeyRequirementPreferred, UserVerification: verification}})
	if store == nil {
		s.initErr = fmt.Errorf("neutronauth: WebAuthn store is required")
	}
	return s
}

type webauthnUser struct {
	id, name, display string
	credentials       []wa.Credential
}

func (u webauthnUser) WebAuthnID() []byte                   { return []byte(u.id) }
func (u webauthnUser) WebAuthnName() string                 { return u.name }
func (u webauthnUser) WebAuthnDisplayName() string          { return u.display }
func (u webauthnUser) WebAuthnCredentials() []wa.Credential { return u.credentials }
func (s *WebAuthnService) user(id, name, display string, requireCredentials bool) (webauthnUser, []WebAuthnCredential, error) {
	u := webauthnUser{id: id, name: name, display: display}
	if s.initErr != nil {
		return u, nil, s.initErr
	}
	if len([]byte(id)) == 0 || len([]byte(id)) > 64 {
		return u, nil, fmt.Errorf("neutronauth: WebAuthn user ID must contain 1-64 bytes")
	}
	records, err := s.store.GetCredentialsByUser(id)
	if err != nil {
		return u, nil, err
	}
	for _, r := range records {
		if r.VerifiedCredential == nil || r.Revision == "" {
			if requireCredentials {
				return u, nil, fmt.Errorf("neutronauth: legacy unverified credential requires re-registration")
			}
			continue
		}
		if r.UserID != id || base64.RawURLEncoding.EncodeToString(r.VerifiedCredential.ID) != r.CredentialID || r.SignCount != r.VerifiedCredential.Authenticator.SignCount {
			return u, nil, fmt.Errorf("neutronauth: inconsistent stored credential")
		}
		raw, err := json.Marshal(r.VerifiedCredential)
		if err != nil {
			return u, nil, err
		}
		var credential wa.Credential
		if err = json.Unmarshal(raw, &credential); err != nil {
			return u, nil, err
		}
		u.credentials = append(u.credentials, credential)
	}
	return u, records, nil
}
func (s *WebAuthnService) persistCeremony(key string, session *wa.SessionData) error {
	session.Expires = time.Now().Add(s.config.Timeout)
	raw, err := json.Marshal(session)
	if err != nil {
		return err
	}
	return s.store.StoreChallenge(key, string(raw), s.config.Timeout)
}
func (s *WebAuthnService) consumeCeremony(key string) (wa.SessionData, error) {
	var session wa.SessionData
	if s.initErr != nil {
		return session, s.initErr
	}
	raw, err := s.store.GetChallenge(key)
	if err != nil {
		return session, err
	}
	if len(raw) > 64*1024 {
		return session, fmt.Errorf("neutronauth: ceremony state exceeds limit")
	}
	if err = json.Unmarshal([]byte(raw), &session); err != nil {
		return session, fmt.Errorf("neutronauth: invalid ceremony state: %w", err)
	}
	if session.Challenge == "" || session.Expires.IsZero() || !session.Expires.After(time.Now()) {
		return session, fmt.Errorf("neutronauth: ceremony expired or invalid")
	}
	return session, nil
}
func (s *WebAuthnService) BeginRegistration(userID, userName, displayName string) (*RegistrationOptions, error) {
	user, _, err := s.user(userID, userName, displayName, false)
	if err != nil {
		return nil, err
	}
	options, session, err := s.verifier.BeginRegistration(user, wa.WithCredentialParameters([]protocol.CredentialParameter{{Type: "public-key", Algorithm: webauthncose.AlgES256}}))
	if err != nil {
		return nil, err
	}
	if err = s.persistCeremony("reg:"+userID, session); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(options.Response)
	if err != nil {
		return nil, err
	}
	var result RegistrationOptions
	err = json.Unmarshal(raw, &result)
	result.Timeout = int(s.config.Timeout.Milliseconds())
	return &result, err
}
func (s *WebAuthnService) FinishRegistration(userID string, response RegistrationResponse) (*WebAuthnCredential, error) {
	session, err := s.consumeCeremony("reg:" + userID)
	if err != nil {
		return nil, err
	}
	user, _, err := s.user(userID, userID, userID, false)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	if len(raw) > 1024*1024 {
		return nil, fmt.Errorf("neutronauth: registration response exceeds limit")
	}
	parsed, err := protocol.ParseCredentialCreationResponseBytes(raw)
	if err != nil {
		return nil, err
	}
	credential, err := s.verifier.CreateCredential(user, session, parsed)
	if err != nil {
		return nil, err
	}
	result := WebAuthnCredential{CredentialID: base64.RawURLEncoding.EncodeToString(credential.ID), UserID: userID, SignCount: credential.Authenticator.SignCount, CreatedAt: time.Now(), VerifiedCredential: credential, Revision: generateSessionID()}
	if err = s.store.SaveCredential(result); err != nil {
		return nil, err
	}
	return &result, nil
}
func (s *WebAuthnService) BeginAuthentication(userID string) (*AuthenticationOptions, error) {
	if s.initErr != nil {
		return nil, s.initErr
	}
	if _, ok := s.store.(AtomicWebAuthnStore); !ok {
		return nil, fmt.Errorf("neutronauth: authentication requires atomic credential revision commits")
	}
	user, _, err := s.user(userID, userID, userID, true)
	if err != nil {
		return nil, err
	}
	options, session, err := s.verifier.BeginLogin(user)
	if err != nil {
		return nil, err
	}
	if err = s.persistCeremony("auth:"+userID, session); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(options.Response)
	if err != nil {
		return nil, err
	}
	var result AuthenticationOptions
	err = json.Unmarshal(raw, &result)
	result.Timeout = int(s.config.Timeout.Milliseconds())
	return &result, err
}
func (s *WebAuthnService) FinishAuthentication(userID string, response AuthenticationResponse) (*WebAuthnCredential, error) {
	if s.initErr != nil {
		return nil, s.initErr
	}
	store, ok := s.store.(AtomicWebAuthnStore)
	if !ok {
		return nil, fmt.Errorf("neutronauth: authentication requires atomic credential revision commits")
	}
	session, err := s.consumeCeremony("auth:" + userID)
	if err != nil {
		return nil, err
	}
	user, records, err := s.user(userID, userID, userID, true)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	if len(raw) > 1024*1024 {
		return nil, fmt.Errorf("neutronauth: assertion response exceeds limit")
	}
	parsed, err := protocol.ParseCredentialRequestResponseBytes(raw)
	if err != nil {
		return nil, err
	}
	credential, err := s.verifier.ValidateLogin(user, session, parsed)
	if err != nil {
		return nil, err
	}
	if credential.Authenticator.CloneWarning {
		return nil, fmt.Errorf("neutronauth: authenticator counter regression")
	}
	id := base64.RawURLEncoding.EncodeToString(credential.ID)
	for _, record := range records {
		if record.CredentialID == id {
			expected := record.Revision
			record.VerifiedCredential = credential
			record.SignCount = credential.Authenticator.SignCount
			record.Revision = generateSessionID()
			if err = store.CompareAndSwapCredential(id, expected, record); err != nil {
				return nil, err
			}
			return &record, nil
		}
	}
	return nil, fmt.Errorf("neutronauth: credential not found")
}

// ---------------------------------------------------------------------------
// HTTP handler helpers
// ---------------------------------------------------------------------------

// BeginRegistrationHandler returns an http.Handler that starts the WebAuthn
// registration ceremony.  The userID, userName, and displayName must be
// provided by the caller (typically from a session).
func (s *WebAuthnService) BeginRegistrationHandler(getUserInfo func(r *http.Request) (userID, userName, displayName string, err error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, userName, displayName, err := getUserInfo(r)
		if err != nil {
			neutron.WriteError(w, r, neutron.ErrUnauthorized("authentication required"))
			return
		}

		opts, err := s.BeginRegistration(userID, userName, displayName)
		if err != nil {
			log.Printf("[neutronauth] begin registration failed: %v", err)
			neutron.WriteError(w, r, neutron.ErrInternal("Registration setup failed"))
			return
		}

		neutron.JSON(w, http.StatusOK, opts)
	})
}

// BeginAuthenticationHandler returns an http.Handler that starts the WebAuthn
// authentication ceremony.
func (s *WebAuthnService) BeginAuthenticationHandler(getUserID func(r *http.Request) (string, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, err := getUserID(r)
		if err != nil {
			neutron.WriteError(w, r, neutron.ErrBadRequest("user identification required"))
			return
		}

		opts, err := s.BeginAuthentication(userID)
		if err != nil {
			log.Printf("[neutronauth] begin authentication failed: %v", err)
			neutron.WriteError(w, r, neutron.ErrInternal("Authentication setup failed"))
			return
		}

		neutron.JSON(w, http.StatusOK, opts)
	})
}
