//! Page-level encryption — transparent AES-256-GCM encryption for all on-disk pages.
//!
//! Per Principle 1: subsystems never know if data is encrypted. Encryption is
//! handled transparently at the storage layer. Every page written to disk is
//! encrypted; every page read is decrypted. The rest of Nucleus only sees plaintext.
//!
//! Layout of an encrypted page on disk:
//!   [nonce: 12 bytes] [ciphertext: PAGE_SIZE bytes] [auth_tag: 16 bytes]
//!
//! Total on-disk size per page: PAGE_SIZE + 28 bytes (nonce + tag overhead).

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Nonce};
use argon2::Argon2;

use super::page::{PageBuf, PAGE_SIZE};

/// Size of AES-GCM nonce (96 bits).
pub const NONCE_SIZE: usize = 12;
/// Size of AES-GCM authentication tag.
pub const TAG_SIZE: usize = 16;
/// Total overhead per encrypted page.
pub const ENCRYPTION_OVERHEAD: usize = NONCE_SIZE + TAG_SIZE;
/// Size of encrypted page on disk.
pub const ENCRYPTED_PAGE_SIZE: usize = PAGE_SIZE + ENCRYPTION_OVERHEAD;

/// Page encryptor — holds the derived key and provides encrypt/decrypt operations.
#[derive(Clone)]
pub struct PageEncryptor {
    cipher: Aes256Gcm,
}

impl PageEncryptor {
    /// Create a new encryptor from a raw 256-bit key.
    pub fn from_key(key: &[u8; 32]) -> Self {
        let cipher = Aes256Gcm::new_from_slice(key).expect("valid 32-byte key");
        Self { cipher }
    }

    /// Derive an encryption key from a passphrase and salt using Argon2id,
    /// then create a PageEncryptor.
    pub fn from_passphrase(passphrase: &[u8], salt: &[u8; 16]) -> Self {
        let mut key = [0u8; 32];
        Argon2::default()
            .hash_password_into(passphrase, salt, &mut key)
            .expect("argon2 key derivation failed");
        let enc = Self::from_key(&key);
        // Zero the key material on the stack
        key.fill(0);
        enc
    }

    /// Generate a random 16-byte salt suitable for key derivation.
    pub fn generate_salt() -> [u8; 16] {
        use rand::Rng;
        let mut salt = [0u8; 16];
        rand::thread_rng().fill(&mut salt);
        salt
    }

    /// Encrypt a plaintext page. Returns nonce || ciphertext || tag.
    pub fn encrypt_page(&self, page: &PageBuf) -> Vec<u8> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, page.as_ref())
            .expect("AES-GCM encryption should not fail with valid key");

        let mut out = Vec::with_capacity(ENCRYPTED_PAGE_SIZE);
        out.extend_from_slice(nonce.as_slice());
        out.extend_from_slice(&ciphertext);
        out
    }

    /// Encrypt arbitrary bytes. Returns nonce || ciphertext || tag.
    pub fn encrypt_bytes(&self, data: &[u8]) -> Vec<u8> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, data)
            .expect("AES-GCM encryption should not fail with valid key");

        let mut out = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
        out.extend_from_slice(nonce.as_slice());
        out.extend_from_slice(&ciphertext);
        out
    }

    /// Decrypt arbitrary bytes (nonce || ciphertext || tag) back to plaintext.
    pub fn decrypt_bytes(&self, encrypted: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        if encrypted.len() < NONCE_SIZE + TAG_SIZE {
            return Err(EncryptionError::InvalidCiphertext);
        }

        let nonce = Nonce::from_slice(&encrypted[..NONCE_SIZE]);
        let ciphertext = &encrypted[NONCE_SIZE..];

        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| EncryptionError::DecryptionFailed)
    }

    /// Decrypt an encrypted page (nonce || ciphertext || tag) back to a plaintext PageBuf.
    pub fn decrypt_page(&self, encrypted: &[u8]) -> Result<PageBuf, EncryptionError> {
        if encrypted.len() < NONCE_SIZE + TAG_SIZE {
            return Err(EncryptionError::InvalidCiphertext);
        }

        let nonce = Nonce::from_slice(&encrypted[..NONCE_SIZE]);
        let ciphertext = &encrypted[NONCE_SIZE..];

        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| EncryptionError::DecryptionFailed)?;

        if plaintext.len() != PAGE_SIZE {
            return Err(EncryptionError::SizeMismatch {
                expected: PAGE_SIZE,
                got: plaintext.len(),
            });
        }

        let mut page = [0u8; PAGE_SIZE];
        page.copy_from_slice(&plaintext);
        Ok(page)
    }
}

impl std::fmt::Debug for PageEncryptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageEncryptor")
            .field("algorithm", &"AES-256-GCM")
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EncryptionError {
    #[error("invalid ciphertext (too short)")]
    InvalidCiphertext,
    #[error("decryption failed (wrong key or corrupted data)")]
    DecryptionFailed,
    #[error("decrypted page size mismatch: expected {expected}, got {got}")]
    SizeMismatch { expected: usize, got: usize },
}

// ---------------------------------------------------------------------------
// Per-tenant key isolation & key rotation
// ---------------------------------------------------------------------------

use rand::Rng;
use std::collections::HashMap;

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as u64
}

/// Lifecycle state of a tenant's encryption key.
#[derive(Debug, Clone)]
pub enum TenantKeyState {
    /// Key is active and in normal use.
    Active,
    /// Key is being rotated. `new_key` is the replacement key and
    /// `pages_remaining` tracks how many pages still need re-encryption
    /// under the new key.
    Rotating {
        new_key: [u8; 32],
        pages_remaining: u64,
    },
    /// Key has been cryptographically erased — all operations must fail.
    Revoked,
}

/// Holds a single tenant's encryption key material and derived encryptor.
#[derive(Debug, Clone)]
pub struct TenantKey {
    pub tenant_id: String,
    pub key: [u8; 32],
    pub state: TenantKeyState,
    pub created_at: u64,
    pub version: u32,
    pub encryptor: PageEncryptor,
}

/// Describes the type of lifecycle event that occurred on a key.
#[derive(Debug, Clone)]
pub enum KeyEventType {
    Created,
    Rotated,
    Revoked,
}

/// Audit record for a key lifecycle event.
#[derive(Debug, Clone)]
pub struct KeyEvent {
    pub tenant_id: String,
    pub event_type: KeyEventType,
    pub timestamp: u64,
    pub key_version: u32,
}

/// Errors specific to the key-management layer.
#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("tenant not found: {0}")]
    TenantNotFound(String),
    #[error("tenant already exists: {0}")]
    TenantExists(String),
    #[error("key revoked for tenant: {0}")]
    KeyRevoked(String),
    #[error("encryption error: {0}")]
    EncryptionError(#[from] EncryptionError),
}

/// Manages per-tenant encryption keys, rotation, revocation, and audit trail.
pub struct KeyManager {
    #[allow(dead_code)]
    master_key: [u8; 32],
    tenant_keys: HashMap<String, TenantKey>,
    key_history: Vec<KeyEvent>,
}

impl KeyManager {
    /// Create a new `KeyManager` with the given master key.
    pub fn new(master_key: [u8; 32]) -> Self {
        Self {
            master_key,
            tenant_keys: HashMap::new(),
            key_history: Vec::new(),
        }
    }

    /// Generate a fresh random key for `tenant_id`.
    ///
    /// Fails if the tenant already exists.
    pub fn create_tenant_key(&mut self, tenant_id: &str) -> Result<(), KeyError> {
        if self.tenant_keys.contains_key(tenant_id) {
            return Err(KeyError::TenantExists(tenant_id.to_string()));
        }

        let mut key = [0u8; 32];
        rand::thread_rng().fill(&mut key);

        let encryptor = PageEncryptor::from_key(&key);
        let now = now_ms();

        self.tenant_keys.insert(
            tenant_id.to_string(),
            TenantKey {
                tenant_id: tenant_id.to_string(),
                key,
                state: TenantKeyState::Active,
                created_at: now,
                version: 1,
                encryptor,
            },
        );

        self.key_history.push(KeyEvent {
            tenant_id: tenant_id.to_string(),
            event_type: KeyEventType::Created,
            timestamp: now,
            key_version: 1,
        });

        Ok(())
    }

    /// Retrieve the `PageEncryptor` for a tenant (if the key is active or rotating).
    pub fn get_encryptor(&self, tenant_id: &str) -> Option<&PageEncryptor> {
        self.tenant_keys.get(tenant_id).and_then(|tk| {
            match &tk.state {
                TenantKeyState::Active | TenantKeyState::Rotating { .. } => Some(&tk.encryptor),
                TenantKeyState::Revoked => None,
            }
        })
    }

    /// Begin key rotation for a tenant.
    ///
    /// A new random key is generated and stored in the `Rotating` state. The
    /// existing encryptor continues to work (for decrypting old pages) while
    /// the caller re-encrypts pages under the new key.
    pub fn rotate_key(&mut self, tenant_id: &str) -> Result<(), KeyError> {
        let tk = self
            .tenant_keys
            .get_mut(tenant_id)
            .ok_or_else(|| KeyError::TenantNotFound(tenant_id.to_string()))?;

        if matches!(tk.state, TenantKeyState::Revoked) {
            return Err(KeyError::KeyRevoked(tenant_id.to_string()));
        }

        let mut new_key = [0u8; 32];
        rand::thread_rng().fill(&mut new_key);

        tk.state = TenantKeyState::Rotating {
            new_key,
            pages_remaining: 0, // caller should set the real count
        };

        let version = tk.version + 1;
        tk.version = version;

        self.key_history.push(KeyEvent {
            tenant_id: tenant_id.to_string(),
            event_type: KeyEventType::Rotated,
            timestamp: now_ms(),
            key_version: version,
        });

        Ok(())
    }

    /// Complete a rotation — swap the new key in as the active key.
    ///
    /// After this call the old key material is zeroed.
    pub fn complete_rotation(&mut self, tenant_id: &str) -> Result<(), KeyError> {
        let tk = self
            .tenant_keys
            .get_mut(tenant_id)
            .ok_or_else(|| KeyError::TenantNotFound(tenant_id.to_string()))?;

        let new_key = match &tk.state {
            TenantKeyState::Rotating { new_key, .. } => *new_key,
            _ => return Err(KeyError::TenantNotFound(tenant_id.to_string())),
        };

        // Zero old key material
        tk.key.fill(0);

        tk.key = new_key;
        tk.encryptor = PageEncryptor::from_key(&new_key);
        tk.state = TenantKeyState::Active;

        Ok(())
    }

    /// Cryptographic erasure — zero the key and mark the tenant as revoked.
    ///
    /// All subsequent encrypt/decrypt calls for this tenant will fail.
    pub fn revoke_key(&mut self, tenant_id: &str) -> Result<(), KeyError> {
        let tk = self
            .tenant_keys
            .get_mut(tenant_id)
            .ok_or_else(|| KeyError::TenantNotFound(tenant_id.to_string()))?;

        if matches!(tk.state, TenantKeyState::Revoked) {
            return Err(KeyError::KeyRevoked(tenant_id.to_string()));
        }

        let version = tk.version;

        // Cryptographic erasure — zero the key bytes
        tk.key.fill(0);
        tk.state = TenantKeyState::Revoked;

        self.key_history.push(KeyEvent {
            tenant_id: tenant_id.to_string(),
            event_type: KeyEventType::Revoked,
            timestamp: now_ms(),
            key_version: version,
        });

        Ok(())
    }

    /// Encrypt a page using the active key for `tenant_id`.
    pub fn encrypt_page_for_tenant(
        &self,
        tenant_id: &str,
        page: &PageBuf,
    ) -> Result<Vec<u8>, KeyError> {
        let tk = self
            .tenant_keys
            .get(tenant_id)
            .ok_or_else(|| KeyError::TenantNotFound(tenant_id.to_string()))?;

        match &tk.state {
            TenantKeyState::Revoked => Err(KeyError::KeyRevoked(tenant_id.to_string())),
            _ => Ok(tk.encryptor.encrypt_page(page)),
        }
    }

    /// Decrypt a page using the active key for `tenant_id`.
    pub fn decrypt_page_for_tenant(
        &self,
        tenant_id: &str,
        encrypted: &[u8],
    ) -> Result<PageBuf, KeyError> {
        let tk = self
            .tenant_keys
            .get(tenant_id)
            .ok_or_else(|| KeyError::TenantNotFound(tenant_id.to_string()))?;

        match &tk.state {
            TenantKeyState::Revoked => Err(KeyError::KeyRevoked(tenant_id.to_string())),
            _ => Ok(tk.encryptor.decrypt_page(encrypted)?),
        }
    }

    /// List all tenants and their current key states.
    pub fn list_tenants(&self) -> Vec<(&str, &TenantKeyState)> {
        self.tenant_keys
            .iter()
            .map(|(id, tk)| (id.as_str(), &tk.state))
            .collect()
    }

    /// Return the full audit log of key lifecycle events.
    pub fn audit_log(&self) -> &[KeyEvent] {
        &self.key_history
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = [0x42u8; 32];
        let enc = PageEncryptor::from_key(&key);

        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0xDE;
        page[1] = 0xAD;
        page[PAGE_SIZE - 1] = 0xFF;

        let encrypted = enc.encrypt_page(&page);
        assert_eq!(encrypted.len(), ENCRYPTED_PAGE_SIZE);
        // Encrypted data should differ from plaintext
        assert_ne!(&encrypted[NONCE_SIZE..NONCE_SIZE + PAGE_SIZE], &page[..]);

        let decrypted = enc.decrypt_page(&encrypted).unwrap();
        assert_eq!(&decrypted[..], &page[..]);
    }

    #[test]
    fn wrong_key_fails() {
        let enc1 = PageEncryptor::from_key(&[0x01; 32]);
        let enc2 = PageEncryptor::from_key(&[0x02; 32]);

        let page = [0u8; PAGE_SIZE];
        let encrypted = enc1.encrypt_page(&page);

        let result = enc2.decrypt_page(&encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn passphrase_key_derivation() {
        let salt = [0xAB; 16];
        let enc = PageEncryptor::from_passphrase(b"my-secret-password", &salt);

        let page = [0u8; PAGE_SIZE];
        let encrypted = enc.encrypt_page(&page);
        let decrypted = enc.decrypt_page(&encrypted).unwrap();
        assert_eq!(&decrypted[..], &page[..]);
    }

    #[test]
    fn same_page_different_ciphertext() {
        // Each encryption should produce different nonce → different ciphertext
        let enc = PageEncryptor::from_key(&[0x42; 32]);
        let page = [0u8; PAGE_SIZE];

        let e1 = enc.encrypt_page(&page);
        let e2 = enc.encrypt_page(&page);
        assert_ne!(e1, e2); // Different nonces → different ciphertext
    }

    #[test]
    fn tampered_data_fails() {
        let enc = PageEncryptor::from_key(&[0x42; 32]);
        let page = [0u8; PAGE_SIZE];
        let mut encrypted = enc.encrypt_page(&page);

        // Tamper with ciphertext
        encrypted[NONCE_SIZE + 10] ^= 0xFF;

        let result = enc.decrypt_page(&encrypted);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Per-tenant key management tests
    // -----------------------------------------------------------------------

    fn make_manager() -> KeyManager {
        KeyManager::new([0xAA; 32])
    }

    fn test_page() -> PageBuf {
        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0xCA;
        page[1] = 0xFE;
        page[PAGE_SIZE - 1] = 0x01;
        page
    }

    #[test]
    fn tenant_key_creation() {
        let mut mgr = make_manager();
        mgr.create_tenant_key("tenant-a").unwrap();

        // Encryptor should be available
        assert!(mgr.get_encryptor("tenant-a").is_some());

        // Encrypt/decrypt roundtrip through the manager
        let page = test_page();
        let encrypted = mgr.encrypt_page_for_tenant("tenant-a", &page).unwrap();
        let decrypted = mgr.decrypt_page_for_tenant("tenant-a", &encrypted).unwrap();
        assert_eq!(&decrypted[..], &page[..]);

        // Creating the same tenant again should fail
        let err = mgr.create_tenant_key("tenant-a").unwrap_err();
        assert!(matches!(err, KeyError::TenantExists(_)));
    }

    #[test]
    fn tenant_isolation() {
        let mut mgr = make_manager();
        mgr.create_tenant_key("alice").unwrap();
        mgr.create_tenant_key("bob").unwrap();

        let page = test_page();
        let encrypted_alice = mgr.encrypt_page_for_tenant("alice", &page).unwrap();
        let encrypted_bob = mgr.encrypt_page_for_tenant("bob", &page).unwrap();

        // Each tenant can decrypt their own data
        let dec_alice = mgr.decrypt_page_for_tenant("alice", &encrypted_alice).unwrap();
        let dec_bob = mgr.decrypt_page_for_tenant("bob", &encrypted_bob).unwrap();
        assert_eq!(&dec_alice[..], &page[..]);
        assert_eq!(&dec_bob[..], &page[..]);

        // Cross-tenant decryption must fail (different keys → auth tag mismatch)
        let cross_ab = mgr.decrypt_page_for_tenant("alice", &encrypted_bob);
        assert!(cross_ab.is_err());
        let cross_ba = mgr.decrypt_page_for_tenant("bob", &encrypted_alice);
        assert!(cross_ba.is_err());
    }

    #[test]
    fn key_rotation() {
        let mut mgr = make_manager();
        mgr.create_tenant_key("tenant-r").unwrap();

        let page = test_page();
        let encrypted_old = mgr.encrypt_page_for_tenant("tenant-r", &page).unwrap();

        // Initiate rotation
        mgr.rotate_key("tenant-r").unwrap();

        // During rotation the old encryptor is still active, so old data is
        // still decryptable.
        let dec = mgr.decrypt_page_for_tenant("tenant-r", &encrypted_old).unwrap();
        assert_eq!(&dec[..], &page[..]);

        // We can still encrypt new pages during rotation (uses current key).
        let encrypted_mid = mgr.encrypt_page_for_tenant("tenant-r", &page).unwrap();
        let dec_mid = mgr.decrypt_page_for_tenant("tenant-r", &encrypted_mid).unwrap();
        assert_eq!(&dec_mid[..], &page[..]);

        // Complete the rotation — now the new key is active.
        mgr.complete_rotation("tenant-r").unwrap();

        // Encrypt under the new key and verify roundtrip.
        let encrypted_new = mgr.encrypt_page_for_tenant("tenant-r", &page).unwrap();
        let dec_new = mgr.decrypt_page_for_tenant("tenant-r", &encrypted_new).unwrap();
        assert_eq!(&dec_new[..], &page[..]);

        // Old ciphertext encrypted under the previous key should no longer
        // decrypt with the new key.
        let dec_old_after = mgr.decrypt_page_for_tenant("tenant-r", &encrypted_old);
        assert!(dec_old_after.is_err());
    }

    #[test]
    fn key_revocation() {
        let mut mgr = make_manager();
        mgr.create_tenant_key("tenant-x").unwrap();

        let page = test_page();
        let encrypted = mgr.encrypt_page_for_tenant("tenant-x", &page).unwrap();

        // Revoke the key
        mgr.revoke_key("tenant-x").unwrap();

        // Encryptor should no longer be available
        assert!(mgr.get_encryptor("tenant-x").is_none());

        // Encrypt must fail
        let enc_err = mgr.encrypt_page_for_tenant("tenant-x", &page).unwrap_err();
        assert!(matches!(enc_err, KeyError::KeyRevoked(_)));

        // Decrypt must fail
        let dec_err = mgr.decrypt_page_for_tenant("tenant-x", &encrypted).unwrap_err();
        assert!(matches!(dec_err, KeyError::KeyRevoked(_)));

        // Double-revoke must fail
        let rev_err = mgr.revoke_key("tenant-x").unwrap_err();
        assert!(matches!(rev_err, KeyError::KeyRevoked(_)));
    }

    #[test]
    fn audit_trail() {
        let mut mgr = make_manager();

        mgr.create_tenant_key("t1").unwrap();
        mgr.create_tenant_key("t2").unwrap();
        mgr.rotate_key("t1").unwrap();
        mgr.revoke_key("t2").unwrap();

        let log = mgr.audit_log();
        assert_eq!(log.len(), 4);

        // Event 0: t1 created
        assert_eq!(log[0].tenant_id, "t1");
        assert!(matches!(log[0].event_type, KeyEventType::Created));
        assert_eq!(log[0].key_version, 1);

        // Event 1: t2 created
        assert_eq!(log[1].tenant_id, "t2");
        assert!(matches!(log[1].event_type, KeyEventType::Created));
        assert_eq!(log[1].key_version, 1);

        // Event 2: t1 rotated
        assert_eq!(log[2].tenant_id, "t1");
        assert!(matches!(log[2].event_type, KeyEventType::Rotated));
        assert_eq!(log[2].key_version, 2);

        // Event 3: t2 revoked
        assert_eq!(log[3].tenant_id, "t2");
        assert!(matches!(log[3].event_type, KeyEventType::Revoked));
        assert_eq!(log[3].key_version, 1);

        // Timestamps should be monotonically non-decreasing
        for window in log.windows(2) {
            assert!(window[0].timestamp <= window[1].timestamp);
        }

        // list_tenants should show both
        let tenants = mgr.list_tenants();
        assert_eq!(tenants.len(), 2);
    }
}
