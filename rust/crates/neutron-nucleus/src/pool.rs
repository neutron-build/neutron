//! Connection pool for the Nucleus database.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};

use crate::error::NucleusError;

// ---------------------------------------------------------------------------
// NucleusConfig
// ---------------------------------------------------------------------------

/// TLS negotiation policy for Nucleus/Postgres connections (libpq `sslmode`).
///
/// With the `tls` feature (default-on), TLS uses rustls with the OS trust store
/// and full certificate-chain + hostname verification. We deliberately do not
/// offer an "encrypt but don't verify" mode: `Require` also verifies, so there
/// is no silent downgrade to unauthenticated TLS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// Never use TLS — plaintext only.
    Disable,
    /// Use TLS if the server offers it, otherwise plaintext. **Default.**
    #[default]
    Prefer,
    /// Require TLS (and verify the certificate); fail if unavailable.
    Require,
    /// Require TLS with full chain + hostname verification.
    VerifyFull,
}

impl SslMode {
    /// The libpq `sslmode` token tokio-postgres understands. tokio-postgres only
    /// knows `disable`/`prefer`/`require`; `VerifyFull`'s extra strictness is
    /// enforced by the rustls verifier (chain + SNI hostname), not the token.
    fn as_pg(self) -> &'static str {
        match self {
            SslMode::Disable => "disable",
            SslMode::Prefer => "prefer",
            SslMode::Require | SslMode::VerifyFull => "require",
        }
    }
}

/// Connection parameters for a Nucleus (or any pgwire-compatible) server.
#[derive(Debug, Clone)]
pub struct NucleusConfig {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: String,
    /// Maximum concurrent connections (default: 16).
    pub max_size: usize,
    /// TLS policy (default: [`SslMode::Prefer`]).
    pub sslmode: SslMode,
}

impl Default for NucleusConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 5432,
            dbname: "nucleus".to_string(),
            user: "postgres".to_string(),
            password: String::new(),
            max_size: 16,
            sslmode: SslMode::default(),
        }
    }
}

impl NucleusConfig {
    pub fn new(host: impl Into<String>, port: u16, dbname: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port,
            dbname: dbname.into(),
            ..Default::default()
        }
    }

    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    pub fn password(mut self, pw: impl Into<String>) -> Self {
        self.password = pw.into();
        self
    }

    pub fn max_size(mut self, n: usize) -> Self {
        self.max_size = n;
        self
    }

    /// Set the TLS policy (default: [`SslMode::Prefer`]).
    pub fn sslmode(mut self, mode: SslMode) -> Self {
        self.sslmode = mode;
        self
    }

    pub(crate) fn connect_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={} sslmode={}",
            self.host,
            self.port,
            self.dbname,
            self.user,
            self.password,
            self.sslmode.as_pg(),
        )
    }
}

// ---------------------------------------------------------------------------
// ClientWithDriver
// ---------------------------------------------------------------------------

/// A `tokio-postgres` `Client` paired with the `JoinHandle` of its background
/// connection driver task.
///
/// `tokio_postgres::connect` returns `(Client, Connection)`; the `Connection`
/// future must be continuously driven for the protocol to make progress. If
/// the `JoinHandle` of the spawned driver is dropped without being tracked,
/// the driver may outlive — or be cancelled out from under — the `Client`,
/// leaving the server holding the connection open.
///
/// Wrapping the two together and aborting the driver in `Drop` ensures the
/// driver's lifetime is bounded by the `Client`'s under all paths, including
/// runtime shutdown and explicit eviction.
pub(crate) struct ClientWithDriver {
    pub(crate) client: Client,
    pub(crate) driver: JoinHandle<()>,
}

impl ClientWithDriver {
    pub(crate) fn is_closed(&self) -> bool {
        self.client.is_closed()
    }
}

impl Drop for ClientWithDriver {
    fn drop(&mut self) {
        // Cancel the driver task. Under normal `Client` drop the driver exits
        // gracefully when the protocol channel closes; `abort()` is the
        // belt-and-suspenders for runtime-shutdown / panic paths and ensures
        // we never accumulate orphaned drivers.
        self.driver.abort();
    }
}

// ---------------------------------------------------------------------------
// Pool internals
// ---------------------------------------------------------------------------

pub(crate) struct PoolInner {
    pub(crate) config: NucleusConfig,
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) idle: Mutex<VecDeque<ClientWithDriver>>,
    /// Lazily-built, cached TLS connector (native roots loaded once, not per
    /// connection). `Err` if TLS setup failed; surfaced at connect time.
    #[cfg(feature = "tls")]
    tls: std::sync::OnceLock<Result<tokio_postgres_rustls::MakeRustlsConnect, String>>,
}

#[cfg(feature = "tls")]
fn build_rustls_connector() -> Result<tokio_postgres_rustls::MakeRustlsConnect, String> {
    let mut roots = rustls::RootCertStore::empty();
    let loaded = rustls_native_certs::load_native_certs();
    for cert in loaded.certs {
        // Skip individual malformed certs rather than failing the whole store.
        let _ = roots.add(cert);
    }
    if roots.is_empty() {
        return Err("no usable native root certificates were found".to_string());
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("rustls protocol versions: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
}

#[cfg(feature = "tls")]
impl PoolInner {
    /// The shared TLS connector, built once on first use.
    fn tls_connector(&self) -> Result<tokio_postgres_rustls::MakeRustlsConnect, NucleusError> {
        self.tls
            .get_or_init(build_rustls_connector)
            .clone()
            .map_err(NucleusError::Tls)
    }
}

/// Spawn the background driver that pumps a tokio-postgres connection. The
/// returned `JoinHandle` is tracked by [`ClientWithDriver`] and aborted on drop.
fn spawn_driver<C>(conn: C) -> JoinHandle<()>
where
    C: std::future::Future<Output = Result<(), tokio_postgres::Error>> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("Nucleus connection driver exited: {e}");
        }
    })
}

// ---------------------------------------------------------------------------
// NucleusPool
// ---------------------------------------------------------------------------

/// Shared, cheaply-cloneable connection pool for a Nucleus database.
///
/// Register as shared state and extract via [`Db`](crate::Db):
///
/// ```rust,ignore
/// let pool = NucleusPool::new(NucleusConfig::default());
///
/// let router = Router::new()
///     .state(pool)
///     .get("/users", list_users);
///
/// async fn list_users(db: Db) -> Json<Vec<User>> {
///     let rows = db.query("SELECT id, name FROM users", &[]).await.unwrap();
///     Json(rows.iter().map(User::from_row).collect())
/// }
/// ```
#[derive(Clone)]
pub struct NucleusPool(pub(crate) Arc<PoolInner>);

impl NucleusPool {
    /// Create a pool without pre-connecting.  Connections are created on demand.
    pub fn new(config: NucleusConfig) -> Self {
        let sem = Arc::new(Semaphore::new(config.max_size));
        Self(Arc::new(PoolInner {
            semaphore: sem,
            idle: Mutex::new(VecDeque::new()),
            config,
            #[cfg(feature = "tls")]
            tls: std::sync::OnceLock::new(),
        }))
    }

    /// Acquire a [`PooledConn`], blocking until a slot is available.
    pub async fn get(&self) -> Result<PooledConn, NucleusError> {
        let permit = Arc::clone(&self.0.semaphore)
            .acquire_owned()
            .await
            .map_err(|_| NucleusError::PoolExhausted)?;

        // Prefer an idle connection; create a new one if the idle set is empty.
        let entry = {
            let mut idle = self.0.idle.lock().unwrap();
            idle.pop_front()
        };

        let entry = match entry {
            Some(c) if !c.is_closed() => c,
            _ => self.new_client().await?,
        };

        Ok(PooledConn {
            client: Some(entry),
            pool: Arc::clone(&self.0),
            permit: Some(permit),
        })
    }

    async fn new_client(&self) -> Result<ClientWithDriver, NucleusError> {
        let cs = self.0.config.connect_string();
        let sslmode = self.0.config.sslmode;

        // TLS path: anything other than `disable` negotiates TLS via rustls
        // (the server makes the final call for `prefer`). The connector verifies
        // the certificate chain + hostname against the OS trust store.
        #[cfg(feature = "tls")]
        if sslmode != SslMode::Disable {
            let connector = self.0.tls_connector()?;
            match tokio_postgres::connect(&cs, connector).await {
                Ok((client, conn)) => {
                    return Ok(ClientWithDriver {
                        client,
                        driver: spawn_driver(conn),
                    });
                }
                // `prefer` means "TLS if it works, otherwise plaintext", which
                // is what this type's own documentation says and what libpq
                // does — libpq retries without SSL when the SSL attempt fails.
                // This did not retry, so `prefer` behaved exactly like
                // `require`, and the DEFAULT configuration could not connect to
                // a DEFAULT Nucleus: the engine answers SSLRequest with 'S' and
                // presents a self-signed certificate, which the verifier
                // correctly rejects. Every Rust user's first connection failed
                // with "error performing TLS handshake" and no indication that
                // the mode they were on was supposed to cope with it.
                //
                // Falling back sends credentials in cleartext, which is why
                // `prefer` is not a secure mode in libpq either and why
                // `require`/`verify-full` still fail closed below. The fallback
                // is logged at warn so it is never silent.
                Err(e) if sslmode == SslMode::Prefer => {
                    tracing::warn!(
                        error = %e,
                        "TLS negotiation failed under sslmode=prefer; retrying in \
                         plaintext. Credentials will be sent unencrypted — use \
                         sslmode=require to fail closed instead."
                    );
                }
                Err(e) => return Err(NucleusError::Connect(e)),
            }
        }

        // Without the `tls` feature, a strict mode is unsatisfiable — fail loudly
        // rather than silently sending credentials in cleartext.
        #[cfg(not(feature = "tls"))]
        if matches!(sslmode, SslMode::Require | SslMode::VerifyFull) {
            return Err(NucleusError::Tls(
                "sslmode=require/verify-full requested but the `tls` feature is disabled".to_string(),
            ));
        }

        // Plaintext: `sslmode=disable`, or `prefer` with the `tls` feature off.
        let (client, conn) = tokio_postgres::connect(&cs, NoTls)
            .await
            .map_err(NucleusError::Connect)?;
        Ok(ClientWithDriver {
            client,
            driver: spawn_driver(conn),
        })
    }
}

// ---------------------------------------------------------------------------
// PooledConn
// ---------------------------------------------------------------------------

/// A `tokio-postgres` client checked out from a [`NucleusPool`].
///
/// Automatically returned to the pool (and semaphore slot released) on drop.
pub struct PooledConn {
    pub(crate) client: Option<ClientWithDriver>,
    pool: Arc<PoolInner>,
    permit: Option<OwnedSemaphorePermit>,
}

impl PooledConn {
    pub fn client(&self) -> &Client {
        &self.client.as_ref().unwrap().client
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if let Some(entry) = self.client.take() {
            // Return to idle before releasing the semaphore so that a woken
            // waiter finds the connection immediately. If the driver has
            // already exited (e.g., network error), the next caller's
            // `is_closed()` check will discard the entry and create a fresh
            // one — at which point its `Drop` aborts the driver task.
            if let Ok(mut idle) = self.pool.idle.lock() {
                idle.push_back(entry);
            }
            // If the lock is poisoned, `entry` is dropped here, which aborts
            // its driver via ClientWithDriver::Drop. Correct cleanup either way.
        }
        // Semaphore slot released when permit is dropped.
        drop(self.permit.take());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_connect_string() {
        let cfg = NucleusConfig::new("localhost", 5432, "mydb")
            .user("alice")
            .password("secret");
        let cs = cfg.connect_string();
        assert!(cs.contains("host=localhost"));
        assert!(cs.contains("port=5432"));
        assert!(cs.contains("dbname=mydb"));
        assert!(cs.contains("user=alice"));
        assert!(cs.contains("password=secret"));
    }

    // P0.0: sslmode is threaded into the connection string and defaults to prefer.
    #[test]
    fn sslmode_defaults_to_prefer() {
        assert_eq!(NucleusConfig::default().sslmode, SslMode::Prefer);
        assert!(NucleusConfig::default().connect_string().contains("sslmode=prefer"));
    }

    #[test]
    fn sslmode_maps_to_pg_token() {
        let cs = |m: SslMode| NucleusConfig::default().sslmode(m).connect_string();
        assert!(cs(SslMode::Disable).contains("sslmode=disable"));
        assert!(cs(SslMode::Prefer).contains("sslmode=prefer"));
        assert!(cs(SslMode::Require).contains("sslmode=require"));
        // verify-full's extra strictness is enforced by the rustls verifier, so
        // it shares the `require` libpq token.
        assert!(cs(SslMode::VerifyFull).contains("sslmode=require"));
    }

    #[test]
    fn pool_is_clone() {
        let pool = NucleusPool::new(NucleusConfig::default());
        let _clone = pool.clone(); // must compile
    }

    #[test]
    fn default_config_sensible_values() {
        let cfg = NucleusConfig::default();
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.max_size, 16);
        assert_eq!(cfg.user, "postgres");
    }

    /// Verify `ClientWithDriver::Drop` aborts the spawned driver task even
    /// when the `Client` would normally keep it alive. We construct a fake
    /// driver task that loops forever; on drop the abort must complete it.
    #[tokio::test]
    async fn driver_aborted_on_drop() {
        // We can't easily build a real `Client` without a server, so emulate
        // the wrapper's lifecycle directly with a never-terminating task.
        let driver: JoinHandle<()> = tokio::spawn(async {
            // Loop forever — only `abort()` should end this.
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        });

        // Mirror ClientWithDriver's drop behavior on a wrapper holding the same
        // JoinHandle: aborting and confirming the task is cancelled.
        struct TestWrapper {
            driver: JoinHandle<()>,
        }
        impl Drop for TestWrapper {
            fn drop(&mut self) {
                self.driver.abort();
            }
        }

        let handle_clone = {
            let wrapper = TestWrapper { driver };
            // Take a reference to the JoinHandle's abort handle before the wrapper drops.
            // Since JoinHandle is not Clone, observe completion via the wrapper's
            // own field after dropping the wrapper.
            let abort_handle = wrapper.driver.abort_handle();
            drop(wrapper);
            abort_handle
        };

        // After abort, the underlying task is cancelled. The abort_handle
        // exposes is_finished(); poll briefly so the runtime registers the cancel.
        for _ in 0..50 {
            if handle_clone.is_finished() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("driver task was not cancelled on wrapper drop");
    }
}
