package neutronauth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/neutron-dev/neutron-go/neutron"
)

// ---------------------------------------------------------------------------
// Data types — WebAuthn credentials and ceremony payloads
// ---------------------------------------------------------------------------

// WebAuthnCredential represents a stored passkey / platform authenticator credential.
type WebAuthnCredential struct {
	// CredentialID is the unique identifier assigned by the authenticator (base64url).
	CredentialID string `json:"credential_id"`
	// PublicKey is the ECDSA P-256 public key in uncompressed SEC1 form (base64url).
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
	Challenge        string                   `json:"challenge"`
	RP               rpEntity                 `json:"rp"`
	User             userEntity               `json:"user"`
	PubKeyCredParams []pubKeyCredParam        `json:"pubKeyCredParams"`
	Timeout          int                      `json:"timeout"`
	Attestation      string                   `json:"attestation"`
	AuthenticatorSel authenticatorSelection   `json:"authenticatorSelection"`
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
	ID       string `json:"id"`
	RawID    string `json:"rawId"`
	Type     string `json:"type"`
	Response struct {
		AttestationObject string `json:"attestationObject"`
		ClientDataJSON    string `json:"clientDataJSON"`
	} `json:"response"`
}

// ---------------------------------------------------------------------------
// Authentication ceremony types
// ---------------------------------------------------------------------------

// AuthenticationOptions is sent to the browser to start navigator.credentials.get().
type AuthenticationOptions struct {
	Challenge        string               `json:"challenge"`
	RPID             string               `json:"rpId"`
	Timeout          int                  `json:"timeout"`
	UserVerification string               `json:"userVerification"`
	AllowCredentials []allowCredentialDesc `json:"allowCredentials,omitempty"`
}

type allowCredentialDesc struct {
	Type string   `json:"type"`
	ID   string   `json:"id"`
}

// AuthenticationResponse is the JSON payload the browser sends back after
// navigator.credentials.get() succeeds.
type AuthenticationResponse struct {
	ID       string `json:"id"`
	RawID    string `json:"rawId"`
	Type     string `json:"type"`
	Response struct {
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
	// StoreChallenge persists a challenge for later verification (keyed by userID or session).
	StoreChallenge(key, challenge string, ttl time.Duration) error
	// GetChallenge retrieves and deletes a stored challenge.
	GetChallenge(key string) (string, error)
	// SaveCredential persists a new credential.
	SaveCredential(cred WebAuthnCredential) error
	// GetCredentialsByUser returns all credentials for a user.
	GetCredentialsByUser(userID string) ([]WebAuthnCredential, error)
	// UpdateSignCount updates the signature counter for clone detection.
	UpdateSignCount(credentialID string, newCount uint32) error
}

// WebAuthnService handles the server side of WebAuthn registration and
// authentication ceremonies using P-256 ECDSA.
type WebAuthnService struct {
	config WebAuthnConfig
	store  WebAuthnStore
}

// NewWebAuthnService creates a WebAuthnService with the given config and store.
func NewWebAuthnService(config WebAuthnConfig, store WebAuthnStore) *WebAuthnService {
	if config.Timeout == 0 {
		config.Timeout = 60 * time.Second
	}
	return &WebAuthnService{config: config, store: store}
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

// BeginRegistration generates a RegistrationOptions payload for the browser.
// The challenge is stored server-side keyed by userID.
func (s *WebAuthnService) BeginRegistration(userID, userName, displayName string) (*RegistrationOptions, error) {
	challenge, err := generateWebAuthnChallenge()
	if err != nil {
		return nil, fmt.Errorf("neutronauth: generate challenge: %w", err)
	}

	if err := s.store.StoreChallenge("reg:"+userID, challenge, s.config.Timeout+30*time.Second); err != nil {
		return nil, fmt.Errorf("neutronauth: store challenge: %w", err)
	}

	opts := &RegistrationOptions{
		Challenge: challenge,
		RP: rpEntity{
			ID:   s.config.RPID,
			Name: s.config.RPName,
		},
		User: userEntity{
			ID:          base64.RawURLEncoding.EncodeToString([]byte(userID)),
			Name:        userName,
			DisplayName: displayName,
		},
		PubKeyCredParams: []pubKeyCredParam{
			{Type: "public-key", Alg: -7}, // ES256 (ECDSA w/ SHA-256 on P-256)
		},
		Timeout:     int(s.config.Timeout.Milliseconds()),
		Attestation: "none",
		AuthenticatorSel: authenticatorSelection{
			ResidentKey:      "preferred",
			UserVerification: "preferred",
		},
	}

	return opts, nil
}

// FinishRegistration verifies the browser's RegistrationResponse, extracts
// the P-256 public key, and stores the credential.
func (s *WebAuthnService) FinishRegistration(userID string, resp RegistrationResponse) (*WebAuthnCredential, error) {
	// 1. Retrieve the stored challenge.
	challenge, err := s.store.GetChallenge("reg:" + userID)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: retrieve challenge: %w", err)
	}

	// 2. Parse and verify clientDataJSON.
	clientDataBytes, err := base64.RawURLEncoding.DecodeString(resp.Response.ClientDataJSON)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: decode clientDataJSON: %w", err)
	}

	var cd clientData
	if err := json.Unmarshal(clientDataBytes, &cd); err != nil {
		return nil, fmt.Errorf("neutronauth: parse clientDataJSON: %w", err)
	}

	if cd.Type != "webauthn.create" {
		return nil, fmt.Errorf("neutronauth: unexpected ceremony type: %s", cd.Type)
	}
	if cd.Challenge != challenge {
		return nil, fmt.Errorf("neutronauth: challenge mismatch")
	}
	if cd.Origin != s.config.RPOrigin {
		return nil, fmt.Errorf("neutronauth: origin mismatch: got %s, want %s", cd.Origin, s.config.RPOrigin)
	}

	// 3. Parse attestationObject to extract the public key.
	// In a production implementation this would fully parse the CBOR
	// attestation object, verify attestation statements, and extract the
	// COSE key.  For this scaffold we extract the credential ID and expect
	// the caller to supply the public key via the raw attestation or an
	// external CBOR library.
	//
	// Minimal extraction: we store the credential ID from the response and
	// generate a placeholder for the public key that must be replaced with
	// actual CBOR parsing in production.
	pubKey, credErr := extractPublicKeyFromAttestation(resp.Response.AttestationObject)
	if credErr != nil {
		return nil, fmt.Errorf("neutronauth: extract public key: %w", credErr)
	}

	cred := WebAuthnCredential{
		CredentialID: resp.ID,
		PublicKey:    pubKey,
		UserID:       userID,
		SignCount:    0,
		CreatedAt:    time.Now(),
	}

	if err := s.store.SaveCredential(cred); err != nil {
		return nil, fmt.Errorf("neutronauth: save credential: %w", err)
	}

	return &cred, nil
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

// BeginAuthentication generates an AuthenticationOptions payload for the browser.
func (s *WebAuthnService) BeginAuthentication(userID string) (*AuthenticationOptions, error) {
	challenge, err := generateWebAuthnChallenge()
	if err != nil {
		return nil, fmt.Errorf("neutronauth: generate challenge: %w", err)
	}

	if err := s.store.StoreChallenge("auth:"+userID, challenge, s.config.Timeout+30*time.Second); err != nil {
		return nil, fmt.Errorf("neutronauth: store challenge: %w", err)
	}

	creds, err := s.store.GetCredentialsByUser(userID)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: get credentials: %w", err)
	}

	allowList := make([]allowCredentialDesc, len(creds))
	for i, c := range creds {
		allowList[i] = allowCredentialDesc{
			Type: "public-key",
			ID:   c.CredentialID,
		}
	}

	opts := &AuthenticationOptions{
		Challenge:        challenge,
		RPID:             s.config.RPID,
		Timeout:          int(s.config.Timeout.Milliseconds()),
		UserVerification: "preferred",
		AllowCredentials: allowList,
	}

	return opts, nil
}

// FinishAuthentication verifies the browser's AuthenticationResponse against
// the stored credential using P-256 ECDSA.
func (s *WebAuthnService) FinishAuthentication(userID string, resp AuthenticationResponse) (*WebAuthnCredential, error) {
	// 1. Retrieve the stored challenge.
	challenge, err := s.store.GetChallenge("auth:" + userID)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: retrieve challenge: %w", err)
	}

	// 2. Parse and verify clientDataJSON.
	clientDataBytes, err := base64.RawURLEncoding.DecodeString(resp.Response.ClientDataJSON)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: decode clientDataJSON: %w", err)
	}

	var cd clientData
	if err := json.Unmarshal(clientDataBytes, &cd); err != nil {
		return nil, fmt.Errorf("neutronauth: parse clientDataJSON: %w", err)
	}

	if cd.Type != "webauthn.get" {
		return nil, fmt.Errorf("neutronauth: unexpected ceremony type: %s", cd.Type)
	}
	if cd.Challenge != challenge {
		return nil, fmt.Errorf("neutronauth: challenge mismatch")
	}
	if cd.Origin != s.config.RPOrigin {
		return nil, fmt.Errorf("neutronauth: origin mismatch: got %s, want %s", cd.Origin, s.config.RPOrigin)
	}

	// 3. Find the matching credential.
	creds, err := s.store.GetCredentialsByUser(userID)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: get credentials: %w", err)
	}

	var matched *WebAuthnCredential
	for i := range creds {
		if creds[i].CredentialID == resp.ID {
			matched = &creds[i]
			break
		}
	}
	if matched == nil {
		return nil, fmt.Errorf("neutronauth: credential not found: %s", resp.ID)
	}

	// 4. Verify the signature.
	// The signed data is: authenticatorData || SHA-256(clientDataJSON)
	authData, err := base64.RawURLEncoding.DecodeString(resp.Response.AuthenticatorData)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: decode authenticatorData: %w", err)
	}

	clientDataHash := sha256.Sum256(clientDataBytes)
	signedData := make([]byte, len(authData)+len(clientDataHash))
	copy(signedData, authData)
	copy(signedData[len(authData):], clientDataHash[:])

	sigBytes, err := base64.RawURLEncoding.DecodeString(resp.Response.Signature)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: decode signature: %w", err)
	}

	pubKeyBytes, err := base64.RawURLEncoding.DecodeString(matched.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: decode public key: %w", err)
	}

	if err := verifyES256(pubKeyBytes, signedData, sigBytes); err != nil {
		return nil, fmt.Errorf("neutronauth: signature verification failed: %w", err)
	}

	// 5. Update the signature counter (clone detection).
	if len(authData) >= 37 {
		newCount := uint32(authData[33])<<24 | uint32(authData[34])<<16 |
			uint32(authData[35])<<8 | uint32(authData[36])
		if newCount > 0 && newCount <= matched.SignCount {
			return nil, fmt.Errorf("neutronauth: signature counter regression (possible cloned authenticator)")
		}
		if newCount > matched.SignCount {
			_ = s.store.UpdateSignCount(matched.CredentialID, newCount)
			matched.SignCount = newCount
		}
	}

	return matched, nil
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

// ---------------------------------------------------------------------------
// P-256 ECDSA verification
// ---------------------------------------------------------------------------

// verifyES256 verifies an ECDSA P-256 signature over SHA-256.
// pubKeyBytes must be the uncompressed SEC1 point (65 bytes: 0x04 || X || Y).
// sig must be the DER-encoded ASN.1 ECDSA signature.
func verifyES256(pubKeyBytes, data, sig []byte) error {
	if len(pubKeyBytes) != 65 || pubKeyBytes[0] != 0x04 {
		return fmt.Errorf("invalid uncompressed P-256 public key (expected 65 bytes starting with 0x04)")
	}

	x := new(big.Int).SetBytes(pubKeyBytes[1:33])
	y := new(big.Int).SetBytes(pubKeyBytes[33:65])
	pubKey := &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     x,
		Y:     y,
	}

	hash := sha256.Sum256(data)
	if !ecdsa.VerifyASN1(pubKey, hash[:], sig) {
		return fmt.Errorf("ECDSA signature verification failed")
	}

	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func generateWebAuthnChallenge() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// extractPublicKeyFromAttestation performs minimal parsing of the attestation
// object to extract the COSE public key.  This is a simplified implementation
// that expects the "none" attestation format with a P-256 key.
//
// In production, use a full CBOR parser (e.g. fxamacker/cbor) to properly
// decode the attestation object and verify attestation statements.
func extractPublicKeyFromAttestation(attestationObjectB64 string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(attestationObjectB64)
	if err != nil {
		return "", fmt.Errorf("decode attestation object: %w", err)
	}

	// The attestation object is CBOR-encoded.  We scan for the COSE key
	// markers for a P-256 key.  The uncompressed public key is 65 bytes
	// starting with 0x04.
	//
	// This is intentionally minimal — a production deployment must use a
	// proper CBOR library.  We search for the x-coordinate tag (-2, CBOR
	// negative int 0x21) followed by a 32-byte byte string (0x5820) to
	// locate the key material.
	xIdx := findCBORByteString(data, 32)
	if xIdx < 0 || xIdx+32 > len(data) {
		return "", fmt.Errorf("could not locate P-256 x-coordinate in attestation object")
	}
	xCoord := data[xIdx : xIdx+32]

	// Look for the y-coordinate after the x-coordinate
	remaining := data[xIdx+32:]
	yIdx := findCBORByteString(remaining, 32)
	if yIdx < 0 || yIdx+32 > len(remaining) {
		return "", fmt.Errorf("could not locate P-256 y-coordinate in attestation object")
	}
	yCoord := remaining[yIdx : yIdx+32]

	// Build uncompressed SEC1 point: 0x04 || X || Y
	uncompressed := make([]byte, 65)
	uncompressed[0] = 0x04
	copy(uncompressed[1:33], xCoord)
	copy(uncompressed[33:65], yCoord)

	return base64.RawURLEncoding.EncodeToString(uncompressed), nil
}

// findCBORByteString scans for a CBOR byte string header (major type 2) of
// the given length and returns the index of the first payload byte, or -1.
func findCBORByteString(data []byte, length int) int {
	// CBOR byte string of 32 bytes: 0x5820 (major type 2, additional info 24, length 32)
	if length == 32 {
		for i := 0; i < len(data)-1; i++ {
			if data[i] == 0x58 && data[i+1] == 0x20 && i+2+32 <= len(data) {
				return i + 2
			}
		}
	}
	return -1
}
