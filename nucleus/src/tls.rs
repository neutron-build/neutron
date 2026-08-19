//! TLS support — auto-generates self-signed certificates or loads user-provided ones.
//!
//! By default, Nucleus generates a self-signed certificate at startup and enables
//! TLS on all connections. Users can provide their own certificates via environment
//! variables or config. Non-TLS connections are accepted but logged as warnings.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use pgwire::tokio::TlsAcceptor;
use pgwire::tokio::tokio_rustls::{TlsConnector, rustls};

/// Shared TLS material for encrypted internal node-to-node channels
/// (cluster transport + replication).
#[derive(Clone)]
pub struct InternalTlsConfig {
    pub acceptor: TlsAcceptor,
    pub connector: TlsConnector,
    pub server_name: String,
}

fn load_cert_chain(
    cert_path: &Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, TlsError> {
    let file = File::open(cert_path)
        .map_err(|e| TlsError::CertLoad(format!("{}: {e}", cert_path.display())))?;
    rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::CertLoad(e.to_string()))
}

fn load_private_key(
    key_path: &Path,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, TlsError> {
    let file = File::open(key_path)
        .map_err(|e| TlsError::KeyLoad(format!("{}: {e}", key_path.display())))?;
    let mut keys: Vec<_> = rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::KeyLoad(e.to_string()))?;
    if keys.is_empty() {
        return Err(TlsError::KeyLoad("no PKCS8 private key found".into()));
    }
    Ok(rustls::pki_types::PrivateKeyDer::from(keys.remove(0)))
}

fn build_tls_acceptor(
    certs: Vec<rustls::pki_types::CertificateDer<'static>>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
    client_ca_path: Option<&Path>,
) -> Result<TlsAcceptor, TlsError> {
    let mut config = if let Some(client_ca) = client_ca_path {
        let file = File::open(client_ca)
            .map_err(|e| TlsError::ClientCaLoad(format!("{}: {e}", client_ca.display())))?;
        let client_roots = rustls_pemfile::certs(&mut BufReader::new(file))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TlsError::ClientCaLoad(e.to_string()))?;
        if client_roots.is_empty() {
            return Err(TlsError::ClientCaLoad(format!(
                "{}: no CA certificates found",
                client_ca.display()
            )));
        }

        let mut roots = rustls::RootCertStore::empty();
        for cert in client_roots {
            roots
                .add(cert)
                .map_err(|e| TlsError::ClientAuthConfig(e.to_string()))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| TlsError::ClientAuthConfig(e.to_string()))?;

        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Config(e.to_string()))?
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Config(e.to_string()))?
    };

    // PostgreSQL ALPN identifier
    config.alpn_protocols = vec![b"postgresql".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Build a TLS acceptor from user-provided certificate and key files.
pub fn load_tls_config(cert_path: &Path, key_path: &Path) -> Result<TlsAcceptor, TlsError> {
    load_tls_config_with_client_ca(cert_path, key_path, None)
}

/// Build a TLS acceptor from user-provided server cert/key and optional client CA.
/// When `client_ca_path` is provided, client certificates are required (mTLS).
pub fn load_tls_config_with_client_ca(
    cert_path: &Path,
    key_path: &Path,
    client_ca_path: Option<&Path>,
) -> Result<TlsAcceptor, TlsError> {
    let certs = load_cert_chain(cert_path)?;
    let key = load_private_key(key_path)?;
    build_tls_acceptor(certs, key, client_ca_path)
}

/// Build internal node-to-node TLS config: mutual TLS, in both directions.
///
/// Every node presents the certificate at `cert_path` and requires its peer to
/// present one signed by `ca_path`. Neither half was true before: the acceptor
/// was built with `with_no_client_auth()`, so a node accepted any TLS client at
/// all, and the connector presented no certificate, so there was nothing for a
/// peer to check even if it had looked. Node identity rested entirely on the
/// shared `NUCLEUS_CLUSTER_TOKEN` — one secret, held by every node, that
/// authorises joining the cluster and can be replayed by anyone who learns it.
///
/// This is not a compatibility break for a working deployment: the CA here is
/// already the one that signs node server certificates (that is what makes the
/// connector's verification succeed), so requiring client certificates from the
/// same CA asks for nothing an operating cluster does not already have.
pub fn load_internal_tls_config(
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
    server_name: impl Into<String>,
) -> Result<InternalTlsConfig, TlsError> {
    // The CA verifies peers in BOTH directions, so the acceptor gets it as its
    // client-certificate CA rather than being built with no client auth.
    let acceptor = load_tls_config_with_client_ca(cert_path, key_path, Some(ca_path))?;

    let file = File::open(ca_path)
        .map_err(|e| TlsError::ClientCaLoad(format!("{}: {e}", ca_path.display())))?;
    let ca_certs = rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::ClientCaLoad(e.to_string()))?;
    if ca_certs.is_empty() {
        return Err(TlsError::ClientCaLoad(format!(
            "{}: no CA certificates found",
            ca_path.display()
        )));
    }

    let mut roots = rustls::RootCertStore::empty();
    for cert in ca_certs {
        roots
            .add(cert)
            .map_err(|e| TlsError::ClientAuthConfig(e.to_string()))?;
    }

    // And the connector presents this node's own certificate, so the peer's
    // verifier has something to verify.
    let client_certs = load_cert_chain(cert_path)?;
    let client_key = load_private_key(key_path)?;
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(client_certs, client_key)
        .map_err(|e| TlsError::ClientAuthConfig(e.to_string()))?;
    let connector = TlsConnector::from(Arc::new(client_config));

    Ok(InternalTlsConfig {
        acceptor,
        connector,
        server_name: server_name.into(),
    })
}

/// Generate a self-signed certificate and return a TLS acceptor.
/// Uses rcgen to create an ECDSA P-256 certificate valid for localhost.
pub fn generate_self_signed_tls() -> Result<TlsAcceptor, TlsError> {
    let key_pair = rcgen::KeyPair::generate().map_err(|e| TlsError::Generate(e.to_string()))?;
    let params = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .map_err(|e| TlsError::Generate(e.to_string()))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| TlsError::Generate(e.to_string()))?;

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    let certs = rustls_pemfile::certs(&mut BufReader::new(cert_pem.as_bytes()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::Generate(e.to_string()))?;

    let mut keys: Vec<_> =
        rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(key_pem.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TlsError::Generate(e.to_string()))?;

    let key = rustls::pki_types::PrivateKeyDer::from(keys.remove(0));

    build_tls_acceptor(certs, key, None)
}

/// Create a TLS acceptor from configuration.
/// Priority: user-provided certs > auto-generated self-signed.
/// Returns None if TLS is explicitly disabled.
pub fn setup_tls() -> Result<Option<TlsAcceptor>, TlsError> {
    setup_tls_with_client_ca(None)
}

/// Create a TLS acceptor from configuration with optional client-CA mTLS.
pub fn setup_tls_with_client_ca(
    client_ca_path: Option<&Path>,
) -> Result<Option<TlsAcceptor>, TlsError> {
    // Check for explicit disable
    if std::env::var("NUCLEUS_TLS").unwrap_or_default() == "off" {
        tracing::warn!("TLS disabled — connections will be unencrypted");
        return Ok(None);
    }

    // Check for user-provided certs
    let cert_path = std::env::var("NUCLEUS_TLS_CERT").ok();
    let key_path = std::env::var("NUCLEUS_TLS_KEY").ok();

    if let (Some(cert), Some(key)) = (cert_path, key_path) {
        tracing::info!("Loading TLS certificate from {cert}");
        let acceptor =
            load_tls_config_with_client_ca(Path::new(&cert), Path::new(&key), client_ca_path)?;
        return Ok(Some(acceptor));
    }

    // Auto-generate self-signed
    tracing::info!("Generating self-signed TLS certificate for localhost");
    if client_ca_path.is_some() {
        tracing::warn!(
            "Client CA was configured but no TLS cert/key was provided; \
             auto-generated certificates do not enable mTLS client verification"
        );
    }
    let acceptor = generate_self_signed_tls()?;
    Ok(Some(acceptor))
}

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("failed to load certificate: {0}")]
    CertLoad(String),
    #[error("failed to load private key: {0}")]
    KeyLoad(String),
    #[error("failed to load client CA certificates: {0}")]
    ClientCaLoad(String),
    #[error("client authentication configuration error: {0}")]
    ClientAuthConfig(String),
    #[error("TLS configuration error: {0}")]
    Config(String),
    #[error("failed to generate self-signed certificate: {0}")]
    Generate(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn generate_pem_pair() -> (String, String) {
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        (cert.pem(), key_pair.serialize_pem())
    }

    fn write_temp_file(dir: &tempfile::TempDir, name: &str, contents: &str) -> std::path::PathBuf {
        let path = dir.path().join(name);
        let mut f = File::create(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        path
    }

    #[test]
    fn test_generate_self_signed_tls_succeeds() {
        let result = generate_self_signed_tls();
        assert!(
            result.is_ok(),
            "self-signed TLS generation failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_generate_self_signed_produces_valid_pem() {
        let (cert_pem, key_pem) = generate_pem_pair();
        assert!(cert_pem.contains("-----BEGIN CERTIFICATE-----"));
        assert!(key_pem.contains("-----BEGIN PRIVATE KEY-----"));
    }

    #[test]
    fn test_cert_pem_parses_to_one_cert() {
        let (cert_pem, _) = generate_pem_pair();
        let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(cert_pem.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn test_key_pem_parses_to_one_key() {
        let (_, key_pem) = generate_pem_pair();
        let keys: Vec<_> =
            rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(key_pem.as_bytes()))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn test_load_tls_config_with_valid_files() {
        let (cert_pem, key_pem) = generate_pem_pair();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = write_temp_file(&dir, "cert.pem", &cert_pem);
        let key_path = write_temp_file(&dir, "key.pem", &key_pem);
        let result = load_tls_config(&cert_path, &key_path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_tls_config_with_client_ca_valid() {
        let (cert_pem, key_pem) = generate_pem_pair();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = write_temp_file(&dir, "cert.pem", &cert_pem);
        let key_path = write_temp_file(&dir, "key.pem", &key_pem);
        let ca_path = write_temp_file(&dir, "ca.pem", &cert_pem);
        let result = load_tls_config_with_client_ca(&cert_path, &key_path, Some(&ca_path));
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_tls_config_with_client_ca_missing_file() {
        let (cert_pem, key_pem) = generate_pem_pair();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = write_temp_file(&dir, "cert.pem", &cert_pem);
        let key_path = write_temp_file(&dir, "key.pem", &key_pem);
        let missing_ca = dir.path().join("missing-ca.pem");
        let result = load_tls_config_with_client_ca(&cert_path, &key_path, Some(&missing_ca));
        assert!(matches!(result, Err(TlsError::ClientCaLoad(_))));
    }

    #[test]
    fn test_load_internal_tls_config_success() {
        let (cert_pem, key_pem) = generate_pem_pair();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = write_temp_file(&dir, "cert.pem", &cert_pem);
        let key_path = write_temp_file(&dir, "key.pem", &key_pem);
        let ca_path = write_temp_file(&dir, "ca.pem", &cert_pem);
        let result = load_internal_tls_config(&cert_path, &key_path, &ca_path, "localhost");
        assert!(result.is_ok());
        let cfg = result.unwrap();
        assert_eq!(cfg.server_name, "localhost");
    }

    #[test]
    fn test_load_internal_tls_config_missing_ca() {
        let (cert_pem, key_pem) = generate_pem_pair();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = write_temp_file(&dir, "cert.pem", &cert_pem);
        let key_path = write_temp_file(&dir, "key.pem", &key_pem);
        let missing_ca = dir.path().join("missing-ca.pem");
        let result = load_internal_tls_config(&cert_path, &key_path, &missing_ca, "localhost");
        assert!(matches!(result, Err(TlsError::ClientCaLoad(_))));
    }

    #[test]
    fn test_load_tls_config_missing_cert_file() {
        let dir = tempfile::tempdir().unwrap();
        let (_, key_pem) = generate_pem_pair();
        let key_path = write_temp_file(&dir, "key.pem", &key_pem);
        let bad_cert = dir.path().join("nonexistent.pem");
        let result = load_tls_config(&bad_cert, &key_path);
        assert!(matches!(result, Err(TlsError::CertLoad(_))));
    }

    #[test]
    fn test_load_tls_config_missing_key_file() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_pem, _) = generate_pem_pair();
        let cert_path = write_temp_file(&dir, "cert.pem", &cert_pem);
        let bad_key = dir.path().join("nonexistent.pem");
        let result = load_tls_config(&cert_path, &bad_key);
        assert!(matches!(result, Err(TlsError::KeyLoad(_))));
    }

    #[test]
    fn test_load_tls_config_empty_key_file() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_pem, _) = generate_pem_pair();
        let cert_path = write_temp_file(&dir, "cert.pem", &cert_pem);
        let key_path = write_temp_file(&dir, "key.pem", "");
        let result = load_tls_config(&cert_path, &key_path);
        assert!(matches!(result, Err(TlsError::KeyLoad(_))));
    }

    #[test]
    fn test_load_tls_config_invalid_cert_pem() {
        let dir = tempfile::tempdir().unwrap();
        let (_, key_pem) = generate_pem_pair();
        let cert_path = write_temp_file(&dir, "cert.pem", "NOT A VALID PEM");
        let key_path = write_temp_file(&dir, "key.pem", &key_pem);
        let result = load_tls_config(&cert_path, &key_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_tls_error_display_cert_load() {
        let err = TlsError::CertLoad("file not found".into());
        let msg = format!("{err}");
        assert!(msg.contains("failed to load certificate"));
        assert!(msg.contains("file not found"));
    }

    #[test]
    fn test_tls_error_display_key_load() {
        let err = TlsError::KeyLoad("denied".into());
        assert!(format!("{err}").contains("failed to load private key"));
    }

    #[test]
    fn test_tls_error_display_client_ca_load() {
        let err = TlsError::ClientCaLoad("missing".into());
        assert!(format!("{err}").contains("failed to load client CA certificates"));
    }

    #[test]
    fn test_tls_error_display_config() {
        let err = TlsError::Config("bad".into());
        assert!(format!("{err}").contains("TLS configuration error"));
    }

    #[test]
    fn test_tls_error_display_generate() {
        let err = TlsError::Generate("rcgen fail".into());
        assert!(format!("{err}").contains("failed to generate self-signed certificate"));
    }

    #[test]
    fn test_tls_error_debug() {
        let err = TlsError::CertLoad("test".into());
        let debug = format!("{err:?}");
        assert!(debug.contains("CertLoad"));
    }

    #[test]
    fn test_load_tls_cert_chain() {
        let kp1 = rcgen::KeyPair::generate().unwrap();
        let params1 = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let cert1 = params1.self_signed(&kp1).unwrap();

        let kp2 = rcgen::KeyPair::generate().unwrap();
        let params2 =
            rcgen::CertificateParams::new(vec!["intermediate.local".to_string()]).unwrap();
        let cert2 = params2.self_signed(&kp2).unwrap();

        let chain_pem = format!("{}{}", cert1.pem(), cert2.pem());
        let dir = tempfile::tempdir().unwrap();
        let cert_path = write_temp_file(&dir, "chain.pem", &chain_pem);
        let key_path = write_temp_file(&dir, "key.pem", &kp1.serialize_pem());
        assert!(load_tls_config(&cert_path, &key_path).is_ok());
    }

    #[test]
    fn test_load_tls_mismatched_cert_key() {
        let (cert_pem, _) = generate_pem_pair();
        let (_, other_key) = generate_pem_pair();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = write_temp_file(&dir, "cert.pem", &cert_pem);
        let key_path = write_temp_file(&dir, "key.pem", &other_key);
        let result = load_tls_config(&cert_path, &key_path);
        assert!(matches!(result, Err(TlsError::Config(_))));
    }
}

/// Mutual-TLS tests for the internal (node-to-node) channel.
///
/// The gate for N17 is "a node without a valid cert is refused", which is a
/// claim about a HANDSHAKE and cannot be checked by inspecting a config
/// object. These run a real TLS handshake over a loopback socket against the
/// acceptor the cluster transport uses.
#[cfg(all(test, feature = "server"))]
mod mtls_tests {
    use super::*;
    use std::io::Write;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    struct Ca {
        cert: rcgen::Certificate,
        key: rcgen::KeyPair,
        pem: String,
    }

    fn make_ca(name: &str) -> Ca {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let cert = params.self_signed(&key).unwrap();
        let pem = cert.pem();
        Ca { cert, key, pem }
    }

    /// A node certificate signed by `ca`, valid for `localhost` as both a
    /// server and a client.
    fn node_cert(ca: &Ca) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&key, &ca.cert, &ca.key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    fn write(dir: &tempfile::TempDir, name: &str, contents: &str) -> std::path::PathBuf {
        let path = dir.path().join(name);
        File::create(&path)
            .unwrap()
            .write_all(contents.as_bytes())
            .unwrap();
        path
    }

    fn internal_config(dir: &tempfile::TempDir, ca: &Ca, tag: &str) -> InternalTlsConfig {
        let (cert_pem, key_pem) = node_cert(ca);
        let cert = write(dir, &format!("{tag}-cert.pem"), &cert_pem);
        let key = write(dir, &format!("{tag}-key.pem"), &key_pem);
        let ca_path = write(dir, &format!("{tag}-ca.pem"), &ca.pem);
        load_internal_tls_config(&cert, &key, &ca_path, "localhost").unwrap()
    }

    /// Accept one connection with `acceptor` and report whether the handshake
    /// completed. A failed client handshake shows up here as an accept error.
    async fn serve_once(listener: TcpListener, acceptor: TlsAcceptor) -> bool {
        let Ok((stream, _)) = listener.accept().await else {
            return false;
        };
        match acceptor.accept(stream).await {
            Ok(mut tls) => {
                let _ = tls.write_all(b"ok").await;
                let _ = tls.flush().await;
                true
            }
            Err(_) => false,
        }
    }

    async fn client_says_ok(connector: &TlsConnector, addr: std::net::SocketAddr) -> bool {
        let Ok(stream) = TcpStream::connect(addr).await else {
            return false;
        };
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let Ok(mut tls) = connector.connect(name, stream).await else {
            return false;
        };
        let mut buf = [0u8; 2];
        tls.read_exact(&mut buf).await.is_ok() && &buf == b"ok"
    }

    /// The control: two nodes holding certificates from the same CA connect.
    #[tokio::test]
    async fn a_node_with_a_cert_from_the_cluster_ca_connects() {
        let dir = tempfile::tempdir().unwrap();
        let ca = make_ca("cluster-ca");
        let server = internal_config(&dir, &ca, "server");
        let client = internal_config(&dir, &ca, "client");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(serve_once(listener, server.acceptor));

        assert!(
            client_says_ok(&client.connector, addr).await,
            "a node with a CA-signed certificate must be able to join"
        );
        assert!(accepted.await.unwrap());
    }

    /// The gate: a client presenting NO certificate is refused.
    #[tokio::test]
    async fn a_node_without_a_certificate_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let ca = make_ca("cluster-ca");
        let server = internal_config(&dir, &ca, "server");

        // A client that trusts the cluster CA but presents nothing of its own —
        // exactly what `load_internal_tls_config` used to build.
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut BufReader::new(ca.pem.as_bytes())) {
            roots.add(cert.unwrap()).unwrap();
        }
        let anonymous = TlsConnector::from(Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(serve_once(listener, server.acceptor));

        assert!(
            !client_says_ok(&anonymous, addr).await,
            "a peer presenting no client certificate must not be served"
        );
        assert!(
            !accepted.await.unwrap(),
            "the acceptor must reject the handshake rather than serving it"
        );
    }

    /// A certificate from a DIFFERENT CA is refused. Holding *a* certificate is
    /// not the property being checked; holding one this cluster issued is.
    #[tokio::test]
    async fn a_node_with_a_cert_from_another_ca_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let ours = make_ca("cluster-ca");
        let theirs = make_ca("some-other-ca");
        let server = internal_config(&dir, &ours, "server");

        // The intruder trusts our CA (so it will accept our server cert) but is
        // signed by its own.
        let (cert_pem, key_pem) = node_cert(&theirs);
        let cert = write(&dir, "rogue-cert.pem", &cert_pem);
        let key = write(&dir, "rogue-key.pem", &key_pem);
        let mut roots = rustls::RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut BufReader::new(ours.pem.as_bytes())) {
            roots.add(c.unwrap()).unwrap();
        }
        let rogue = TlsConnector::from(Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(
                    load_cert_chain(&cert).unwrap(),
                    load_private_key(&key).unwrap(),
                )
                .unwrap(),
        ));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(serve_once(listener, server.acceptor));

        assert!(
            !client_says_ok(&rogue, addr).await,
            "a certificate from another CA must not authenticate a peer"
        );
        assert!(!accepted.await.unwrap());
    }

    /// The other direction: a node refuses to talk to a SERVER whose
    /// certificate the cluster CA did not sign, so a rogue listener cannot
    /// collect replication traffic by answering on a peer's address.
    #[tokio::test]
    async fn a_server_with_a_cert_from_another_ca_is_refused_by_the_client() {
        let dir = tempfile::tempdir().unwrap();
        let ours = make_ca("cluster-ca");
        let theirs = make_ca("some-other-ca");
        let impostor = internal_config(&dir, &theirs, "impostor");
        let client = internal_config(&dir, &ours, "client");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(serve_once(listener, impostor.acceptor));

        assert!(
            !client_says_ok(&client.connector, addr).await,
            "a node must not accept a peer certificate its CA did not sign"
        );
        let _ = accepted.await;
    }
}
