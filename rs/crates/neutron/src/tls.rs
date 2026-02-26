//! TLS support for HTTPS.
//!
//! Uses `rustls` and `tokio-rustls` for TLS termination.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::tls::TlsConfig;
//!
//! let tls = TlsConfig::from_pem("cert.pem", "key.pem").unwrap();
//!
//! Neutron::new()
//!     .router(router)
//!     .listen_tls("0.0.0.0:443".parse().unwrap(), tls)
//!     .await
//!     .unwrap();
//! ```
//!
//! ## ALPN Negotiation
//!
//! HTTP/2 and HTTP/1.1 ALPN protocols are automatically configured, enabling
//! transparent HTTP/2 over TLS.

use std::io;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// TLS configuration for HTTPS.
///
/// Load certificates and private keys from PEM or DER files.
pub struct TlsConfig {
    pub(crate) server_config: Arc<rustls::ServerConfig>,
}

impl TlsConfig {
    /// Create TLS config from PEM-encoded certificate and private key files.
    ///
    /// The certificate file should contain the full chain (server cert + intermediates).
    pub fn from_pem(
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> Result<Self, TlsError> {
        let certs = load_certs_pem(cert_path.as_ref())?;
        let key = load_key_pem(key_path.as_ref())?;
        Self::from_parts(certs, key)
    }

    /// Create TLS config from DER-encoded certificate and private key bytes.
    pub fn from_der(cert_der: Vec<Vec<u8>>, key_der: Vec<u8>) -> Result<Self, TlsError> {
        let certs: Vec<CertificateDer<'static>> = cert_der
            .into_iter()
            .map(CertificateDer::from)
            .collect();
        let key = PrivateKeyDer::try_from(key_der).map_err(|e| {
            TlsError::Config(format!("invalid DER private key: {e}"))
        })?;
        Self::from_parts(certs, key)
    }

    /// Create TLS config from pre-loaded certificate chain and private key.
    fn from_parts(
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Self, TlsError> {
        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Config(e.to_string()))?;

        // Enable ALPN for HTTP/2 and HTTP/1.1
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        Ok(Self {
            server_config: Arc::new(config),
        })
    }

    /// Create TLS config from in-memory PEM bytes.
    pub fn from_pem_bytes(cert_pem: &[u8], key_pem: &[u8]) -> Result<Self, TlsError> {
        let certs = load_certs_from_reader(&mut io::BufReader::new(cert_pem))?;
        let key = load_key_from_reader(&mut io::BufReader::new(key_pem))?;
        Self::from_parts(certs, key)
    }

    /// Get the inner `rustls::ServerConfig` (for advanced usage).
    pub fn server_config(&self) -> &Arc<rustls::ServerConfig> {
        &self.server_config
    }
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("alpn", &self.server_config.alpn_protocols)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// TlsError
// ---------------------------------------------------------------------------

/// Error type for TLS configuration.
#[derive(Debug)]
pub enum TlsError {
    /// IO error reading cert/key files.
    Io(io::Error),
    /// TLS configuration error.
    Config(String),
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "TLS IO error: {e}"),
            Self::Config(e) => write!(f, "TLS config error: {e}"),
        }
    }
}

impl std::error::Error for TlsError {}

impl From<io::Error> for TlsError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

// ---------------------------------------------------------------------------
// PEM loading helpers
// ---------------------------------------------------------------------------

fn load_certs_pem(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file = std::fs::File::open(path)?;
    let mut reader = io::BufReader::new(file);
    load_certs_from_reader(&mut reader)
}

fn load_certs_from_reader(
    reader: &mut dyn io::BufRead,
) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(reader)
        .collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(TlsError::Config("no certificates found in PEM".to_string()));
    }
    Ok(certs)
}

fn load_key_pem(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file = std::fs::File::open(path)?;
    let mut reader = io::BufReader::new(file);
    load_key_from_reader(&mut reader)
}

fn load_key_from_reader(
    reader: &mut dyn io::BufRead,
) -> Result<PrivateKeyDer<'static>, TlsError> {
    rustls_pemfile::private_key(reader)?
        .ok_or_else(|| TlsError::Config("no private key found in PEM".to_string()))
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_self_signed() -> (String, String) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_pem = cert.cert.pem();
        let key_pem = cert.key_pair.serialize_pem();
        (cert_pem, key_pem)
    }

    #[test]
    fn from_pem_bytes_valid() {
        let (cert, key) = generate_self_signed();
        let config = TlsConfig::from_pem_bytes(cert.as_bytes(), key.as_bytes());
        assert!(config.is_ok());
    }

    #[test]
    fn alpn_protocols_configured() {
        let (cert, key) = generate_self_signed();
        let config = TlsConfig::from_pem_bytes(cert.as_bytes(), key.as_bytes()).unwrap();
        assert_eq!(
            config.server_config.alpn_protocols,
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        );
    }

    #[test]
    fn from_pem_bytes_invalid_cert() {
        let result = TlsConfig::from_pem_bytes(b"not a cert", b"not a key");
        assert!(result.is_err());
    }

    #[test]
    fn from_pem_bytes_empty() {
        let result = TlsConfig::from_pem_bytes(b"", b"");
        assert!(result.is_err());
    }

    #[test]
    fn from_pem_file_not_found() {
        let result = TlsConfig::from_pem("/nonexistent/cert.pem", "/nonexistent/key.pem");
        assert!(result.is_err());
        match result.unwrap_err() {
            TlsError::Io(_) => {} // expected
            other => panic!("expected Io error, got: {other}"),
        }
    }

    #[test]
    fn from_pem_files() {
        let (cert_pem, key_pem) = generate_self_signed();

        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, cert_pem).unwrap();
        std::fs::write(&key_path, key_pem).unwrap();

        let config = TlsConfig::from_pem(&cert_path, &key_path);
        assert!(config.is_ok());
    }

    #[test]
    fn debug_format() {
        let (cert, key) = generate_self_signed();
        let config = TlsConfig::from_pem_bytes(cert.as_bytes(), key.as_bytes()).unwrap();
        let debug = format!("{config:?}");
        assert!(debug.contains("TlsConfig"));
    }

    #[test]
    fn error_display() {
        let err = TlsError::Config("test error".into());
        assert_eq!(err.to_string(), "TLS config error: test error");

        let err = TlsError::Io(io::Error::new(io::ErrorKind::NotFound, "not found"));
        assert!(err.to_string().contains("not found"));
    }
}
