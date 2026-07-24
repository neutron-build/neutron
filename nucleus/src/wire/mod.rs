//! Postgres wire protocol handler — bridges pgwire to the Nucleus executor.
//!
//! Supports both the simple query protocol (text queries) and the extended
//! query protocol (prepared statements with bind parameters).
//!
//! Additional features:
//!   - LISTEN/NOTIFY async notification delivery via pgwire NotificationResponse
//!   - Extended query pipeline mode with per-Sync error isolation
//!   - Large Objects API (lo_creat, lo_open, lo_read, lo_write, lo_close, lo_unlink)

pub mod compression;
pub mod error_codec;
pub mod kv_fast_path;
pub mod overload;

use std::collections::HashSet;
use std::fmt::Debug;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};

use async_trait::async_trait;
use dashmap::DashMap;
use futures::sink::{Sink, SinkExt};
use futures::{StreamExt, stream};
use tokio::sync::broadcast;

use pgwire::api::auth::sasl::{
    SASLState,
    scram::{SCRAM_ITERATIONS, ScramAuth, gen_salted_password},
};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password as AuthPassword,
    StartupHandler, finish_authentication, protocol_negotiation,
    save_startup_parameters_to_metadata,
};
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    CopyResponse, DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat,
    FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, PgWireConnectionState, PgWireServerHandlers, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone};
use pgwire::messages::response::{CommandComplete, NotificationResponse};
use pgwire::api::cancel::CancelHandler;
use pgwire::messages::cancel::CancelRequest;
use pgwire::messages::startup::{Authentication, PasswordMessageFamily, SecretKey};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use compression::WireCompressor;
use error_codec::{ErrorCodec, PgWireErrorCodec};

use crate::executor::{ExecError, ExecResult, Executor};
use crate::types::{DataType, Value};

// ============================================================================
// Error Codec Management
// ============================================================================

/// Build a `PgWireError::UserError` from an `ExecError` with proper SQLSTATE.
/// Uses the PgWireErrorCodec to map errors consistently.
fn exec_error_to_pgwire(e: ExecError) -> PgWireError {
    let codec = PgWireErrorCodec;
    let details = codec.encode(&e);
    let sqlstate = codec.code_to_string(details.code);
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        sqlstate,
        details.message,
    )))
}

// ============================================================================
// Authentication
// ============================================================================

/// Length in bytes of the random salt generated for SCRAM-SHA-256 auth.
/// Matches PostgreSQL's default (16 bytes).
const SCRAM_SALT_LEN: usize = 16;

/// Stores credentials for password-based authentication.
///
/// When the server is configured with a `UserAuthenticator`, clients must
/// provide the correct username and password via the configured auth method
/// (SCRAM-SHA-256 by default, optional cleartext for legacy clients).
///
/// The shape of the credential returned to pgwire depends on `auth_method`:
///   - `Cleartext`: the raw password with no salt (the startup handler compares
///     it directly against the client-supplied password).
///   - `ScramSha256`: an RFC 5802 salted password `Hi(password, salt, iters)`
///     together with the `salt` it was derived from. pgwire's SCRAM flow
///     requires both — returning a salt-less password panics it at
///     `salt.expect("Salt required for SCRAM auth source")`.
#[derive(Debug, Clone)]
pub struct UserAuthenticator {
    username: String,
    password: String,
    /// Negotiated auth method; decides whether `get_password` returns cleartext
    /// or a SCRAM salted password + salt.
    auth_method: AuthMethod,
    /// Random salt used to derive the SCRAM salted password. Generated once at
    /// construction so it stays constant across the multi-round SCRAM exchange
    /// (the client is told this salt in server-first and must derive the same
    /// salted password). Unused for cleartext auth.
    salt: Vec<u8>,
}

impl UserAuthenticator {
    /// Create a new authenticator with the given credentials, defaulting to the
    /// default auth method (SCRAM-SHA-256).
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::with_method(username, password, AuthMethod::default())
    }

    /// Create a new authenticator with an explicit auth method.
    pub fn with_method(
        username: impl Into<String>,
        password: impl Into<String>,
        auth_method: AuthMethod,
    ) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
            auth_method,
            salt: rand::random::<[u8; SCRAM_SALT_LEN]>().to_vec(),
        }
    }

    /// The expected username.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// The expected password.
    pub fn password(&self) -> &str {
        &self.password
    }

    /// The auth method this authenticator produces credentials for.
    pub fn auth_method(&self) -> AuthMethod {
        self.auth_method
    }

    /// Override the auth method (keeps username/password/salt). Used by the
    /// handler to align a caller-supplied authenticator with the negotiated
    /// method so the returned credential is in the shape that flow expects.
    fn set_auth_method(&mut self, auth_method: AuthMethod) {
        self.auth_method = auth_method;
    }
}

#[async_trait]
impl AuthSource for UserAuthenticator {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<AuthPassword> {
        // Verify username first — reject unknown users with a clear error.
        let incoming_user = login.user().unwrap_or("");
        if incoming_user != self.username {
            return Err(PgWireError::InvalidPassword(incoming_user.to_owned()));
        }
        match self.auth_method {
            // Cleartext: the startup handler compares these bytes against the
            // client-supplied password directly, so hand back the raw password
            // with no salt.
            AuthMethod::Cleartext => Ok(AuthPassword::new(None, self.password.as_bytes().to_vec())),
            // SCRAM-SHA-256: pgwire needs the salted password + salt. Derive
            // Hi(password, salt, iterations) per RFC 5802 using pgwire's own
            // helper (SASLprep-normalizes then PBKDF2-HMAC-SHA256), with the
            // same iteration count pgwire advertises to the client.
            AuthMethod::ScramSha256 => {
                let salted = gen_salted_password(&self.password, &self.salt, SCRAM_ITERATIONS);
                Ok(AuthPassword::new(Some(self.salt.clone()), salted))
            }
        }
    }
}

/// Catalog-backed authentication source. It exposes only stored SCRAM salted
/// material and therefore never retains or recovers raw role passwords.
#[derive(Clone)]
struct CatalogAuthenticator {
    executor: Arc<Executor>,
}

impl std::fmt::Debug for CatalogAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogAuthenticator")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthSource for CatalogAuthenticator {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<AuthPassword> {
        let user = login.user().unwrap_or("");
        let (salt, salted) = self
            .executor
            .scram_credentials(user)
            .await
            .ok_or_else(|| PgWireError::InvalidPassword(user.to_owned()))?;
        Ok(AuthPassword::new(Some(salt), salted))
    }
}

/// Password authentication method for the wire protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthMethod {
    /// PostgreSQL cleartext password exchange (only safe with TLS).
    Cleartext,
    /// SCRAM-SHA-256 challenge/response (recommended).
    #[default]
    ScramSha256,
}

// ============================================================================
// Login Rate Limiter
// ============================================================================

/// Tracks failed authentication attempts per source IP to prevent brute-force
/// attacks.  After [`MAX_FAILED_ATTEMPTS`] failures from the same IP within
/// [`LOCKOUT_SECS`] seconds, subsequent attempts are rejected immediately.
struct LoginRateLimiter {
    /// Map from source IP → (failure_count, last_failure_instant).
    attempts: parking_lot::Mutex<std::collections::HashMap<IpAddr, (u32, std::time::Instant)>>,
}

impl LoginRateLimiter {
    /// Maximum consecutive failures before lockout.
    const MAX_FAILED_ATTEMPTS: u32 = 5;
    /// Lockout duration in seconds after exceeding the failure threshold.
    const LOCKOUT_SECS: u64 = 30;

    fn new() -> Self {
        Self {
            attempts: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Returns `true` if the given IP is currently locked out due to too many
    /// recent failures.
    fn is_locked_out(&self, ip: IpAddr) -> bool {
        let attempts = self.attempts.lock();
        if let Some(&(count, last)) = attempts.get(&ip)
            && count >= Self::MAX_FAILED_ATTEMPTS
        {
            return last.elapsed().as_secs() < Self::LOCKOUT_SECS;
        }
        false
    }

    /// Record a failed authentication attempt from `ip`.
    fn record_failure(&self, ip: IpAddr) {
        let mut attempts = self.attempts.lock();
        let entry = attempts.entry(ip).or_insert((0, std::time::Instant::now()));
        // Reset the counter if the lockout window has elapsed.
        if entry.1.elapsed().as_secs() >= Self::LOCKOUT_SECS {
            *entry = (1, std::time::Instant::now());
        } else {
            entry.0 += 1;
            entry.1 = std::time::Instant::now();
        }
    }

    /// Clear the failure record for `ip` (called on successful auth).
    fn clear(&self, ip: IpAddr) {
        self.attempts.lock().remove(&ip);
    }
}

// ============================================================================
// LISTEN/NOTIFY — Notification Registry
// ============================================================================

/// A pending notification to be delivered to a listening connection.
#[derive(Debug, Clone)]
pub struct PendingNotification {
    pub pid: i32,
    pub channel: String,
    pub payload: String,
}

/// Per-connection notification state: tracks which channels this connection
/// listens on and receives pending notifications from those channels.
struct ConnectionNotifyState {
    /// Channels this connection has subscribed to via LISTEN.
    channels: HashSet<String>,
    /// Receiver end for notifications destined for this connection.
    rx: broadcast::Receiver<PendingNotification>,
}

/// Shared notification registry — routes NOTIFY messages to all connections
/// that have called LISTEN on the corresponding channel.
///
/// Thread-safe via DashMap. Each channel maps to a broadcast sender; every
/// connection that LISTENs on a channel receives a clone of that sender's
/// receiver.
pub struct NotificationRegistry {
    /// channel_name → broadcast sender.
    channels: DashMap<String, broadcast::Sender<PendingNotification>>,
    /// Default broadcast capacity per channel.
    capacity: usize,
    /// Monotonic process ID counter (one per connection, exposed in
    /// NotificationResponse as `pid`).
    next_pid: AtomicI32,
}

impl NotificationRegistry {
    fn new(capacity: usize) -> Self {
        Self {
            channels: DashMap::new(),
            capacity,
            next_pid: AtomicI32::new(1),
        }
    }

    /// Allocate a unique process ID for a new connection.
    fn allocate_pid(&self) -> i32 {
        self.next_pid.fetch_add(1, Ordering::Relaxed)
    }

    /// Subscribe to a channel. Returns a receiver for notifications on that channel.
    fn listen(&self, channel: &str) -> broadcast::Receiver<PendingNotification> {
        let entry = self
            .channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(self.capacity).0);
        entry.subscribe()
    }

    /// Unsubscribe from a specific channel (no-op if not subscribed — the
    /// receiver is dropped by the caller's ConnectionNotifyState).
    fn unlisten(&self, _channel: &str) {
        // The receiver is dropped by the caller; the sender stays alive as
        // long as there are other subscribers. We could GC empty channels
        // here but it's not necessary for correctness.
    }

    /// Send a notification to all connections listening on `channel`.
    /// Returns the number of receivers that got the message.
    fn notify(&self, pid: i32, channel: &str, payload: &str) -> usize {
        if let Some(tx) = self.channels.get(channel) {
            tx.send(PendingNotification {
                pid,
                channel: channel.to_string(),
                payload: payload.to_string(),
            })
            .unwrap_or(0)
        } else {
            0
        }
    }

    /// Remove a channel entirely (used during cleanup when no subscribers remain).
    fn remove_channel_if_empty(&self, channel: &str) {
        if let Some(entry) = self.channels.get(channel)
            && entry.receiver_count() == 0
        {
            drop(entry);
            self.channels.remove(channel);
        }
    }
}

// ============================================================================
// Large Objects API
// ============================================================================

/// Modes for lo_open (matches PostgreSQL's INV_READ/INV_WRITE).
const INV_READ: i32 = 0x00040000;
const INV_WRITE: i32 = 0x00020000;

/// State for an open large object descriptor.
#[allow(dead_code)]
struct LargeObjectDescriptor {
    /// The blob key in the BlobStore (format: `lo/{oid}`).
    key: String,
    /// Object ID.
    oid: u32,
    /// Current read/write offset.
    offset: u64,
    /// Open mode flags.
    mode: i32,
}

/// Per-connection large object state: tracks open descriptors.
struct LargeObjectState {
    /// fd → descriptor.
    descriptors: std::collections::HashMap<i32, LargeObjectDescriptor>,
    /// Next file descriptor to allocate.
    next_fd: i32,
}

impl LargeObjectState {
    fn new() -> Self {
        Self {
            descriptors: std::collections::HashMap::new(),
            next_fd: 1,
        }
    }

    fn allocate_fd(&mut self) -> i32 {
        let fd = self.next_fd;
        self.next_fd += 1;
        fd
    }
}

/// Global OID counter for large objects.
static NEXT_LO_OID: AtomicU32 = AtomicU32::new(100_000);

/// Format a large object OID into its BlobStore key.
fn lo_blob_key(oid: u32) -> String {
    format!("_lo/{oid}")
}

// ============================================================================
// Query Parser (Extended Query Protocol)
// ============================================================================

/// Parses SQL strings for the extended query protocol.
///
/// Parsed statement: caches both the raw SQL and the parsed AST from the Parse
/// message. On Execute, the cached AST is cloned and parameter-substituted,
/// skipping the SQL parser entirely.
#[derive(Debug, Clone)]
pub struct ParsedStatement {
    pub sql: String,
    /// Cached AST from `sql::parse()`. `None` if parsing failed (fallback to string path).
    pub ast: Option<Vec<sqlparser::ast::Statement>>,
    /// Normalized SQL key for plan cache lookups (computed during Parse phase).
    /// Avoids the expensive `query.to_string()` + `normalize_sql_for_cache()` on Execute.
    pub plan_cache_key: Option<String>,
}

pub struct NucleusQueryParser {
    executor: Arc<Executor>,
}

impl NucleusQueryParser {
    fn new(executor: Arc<Executor>) -> Self {
        Self { executor }
    }
}

impl std::fmt::Debug for NucleusQueryParser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NucleusQueryParser").finish()
    }
}

impl Clone for NucleusQueryParser {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
        }
    }
}

#[async_trait]
impl QueryParser for NucleusQueryParser {
    type Statement = ParsedStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Use the executor's AST cache for ~5-10x faster repeated parses.
        // On cache hit, clones the cached AST and substitutes literals via
        // DFS walk instead of re-parsing the SQL string.
        let plan_cache_key;
        let ast = match self.executor.parse_with_ast_cache(sql) {
            Ok(stmts) => {
                // Retrieve the plan cache key hint that parse_with_ast_cache
                // stored, so we can carry it through to the Execute phase.
                plan_cache_key = self.executor.take_plan_cache_key_hint();
                Some(stmts)
            }
            Err(_) => {
                plan_cache_key = None;
                // Fall back to raw parse (may still fail, but we store None).
                crate::sql::parse(sql).ok()
            }
        };
        Ok(ParsedStatement {
            sql: sql.to_owned(),
            ast,
            plan_cache_key,
        })
    }
}

// ============================================================================
// COPY FROM STDIN state
// ============================================================================

struct CopyInfo {
    table: String,
    columns: Option<Vec<String>>,
    delimiter: u8,
    is_csv: bool,
    is_binary: bool,
    has_header: bool,
}

struct CopyInProgress {
    table: String,
    columns: Option<Vec<String>>,
    delimiter: u8,
    is_csv: bool,
    is_binary: bool,
    has_header: bool,
    data: Vec<u8>,
    session_id: u64,
}

// ============================================================================
// Handler
// ============================================================================

/// The Nucleus query handler. Implements startup authentication, simple query,
/// and extended query (prepared statement) processing.
///
/// Also provides:
///   - LISTEN/NOTIFY notification delivery (piggy-backed on query responses)
///   - Extended query pipeline mode (error isolation per Sync boundary)
///   - Large Objects API (lo_creat / lo_open / lo_read / lo_write / lo_close / lo_unlink)
pub struct NucleusHandler {
    executor: Arc<Executor>,
    authenticator: Option<UserAuthenticator>,
    catalog_authenticator: Option<CatalogAuthenticator>,
    auth_method: AuthMethod,
    scram_auth: Option<ScramAuth>,
    parameter_provider: DefaultServerParameterProvider,
    query_parser: Arc<NucleusQueryParser>,
    compressor: WireCompressor,
    /// Tracks session IDs created by connections (for cleanup on disconnect).
    /// Maps peer socket address string → session_id.
    session_registry: parking_lot::RwLock<std::collections::HashMap<String, u64>>,
    /// Per-connection SASL state, keyed by peer socket address.
    sasl_registry: parking_lot::RwLock<std::collections::HashMap<String, SASLState>>,
    /// Per-connection COPY FROM STDIN in-flight state.
    copy_state: parking_lot::Mutex<std::collections::HashMap<std::net::SocketAddr, CopyInProgress>>,
    /// Maximum time in seconds a single query may run before cancellation.
    /// Default: 30 seconds. 0 = no timeout.
    statement_timeout_secs: u64,
    /// Maximum query string size in bytes. Default: 16 MB.
    max_query_size: usize,
    /// Rate limiter for failed authentication attempts (brute-force protection).
    login_rate_limiter: LoginRateLimiter,

    // ── LISTEN/NOTIFY ────────────────────────────────────────────────────
    /// Shared notification registry: channel → broadcast sender.
    notification_registry: Arc<NotificationRegistry>,
    /// Per-connection notification state: peer addr → (pid, subscribed channels, receivers).
    notify_state: parking_lot::Mutex<std::collections::HashMap<String, ConnectionNotifyState>>,
    /// Per-connection assigned process IDs: peer addr → pid (for NotificationResponse).
    connection_pids: parking_lot::RwLock<std::collections::HashMap<String, i32>>,

    // ── Large Objects ────────────────────────────────────────────────────
    /// Per-connection large object descriptors: peer addr → LargeObjectState.
    lo_state: parking_lot::Mutex<std::collections::HashMap<String, LargeObjectState>>,

    // ── Query cancellation (wire CancelRequest) ──────────────────────────
    /// Cancel keys handed out in BackendKeyData: pid → (secret, session_id).
    cancel_keys: parking_lot::RwLock<std::collections::HashMap<i32, (SecretKey, u64)>>,
    /// Per-session cancel signal, raced against query execution.
    cancel_notifies: parking_lot::RwLock<std::collections::HashMap<u64, Arc<tokio::sync::Notify>>>,
}

impl NucleusHandler {
    /// Default statement timeout in seconds (30s). Use 0 to disable.
    const DEFAULT_STATEMENT_TIMEOUT_SECS: u64 = 30;
    /// Default maximum query string size (16 MiB).
    const DEFAULT_MAX_QUERY_SIZE: usize = 16 * 1024 * 1024;

    /// Create a handler with no authentication (accepts all connections).
    pub fn new(executor: Arc<Executor>) -> Self {
        let query_parser = Arc::new(NucleusQueryParser::new(executor.clone()));
        Self {
            executor,
            authenticator: None,
            catalog_authenticator: None,
            auth_method: AuthMethod::default(),
            scram_auth: None,
            parameter_provider: DefaultServerParameterProvider::default(),
            query_parser,
            compressor: WireCompressor::new(1024),
            session_registry: parking_lot::RwLock::new(std::collections::HashMap::new()),
            sasl_registry: parking_lot::RwLock::new(std::collections::HashMap::new()),
            copy_state: parking_lot::Mutex::new(std::collections::HashMap::new()),
            statement_timeout_secs: Self::DEFAULT_STATEMENT_TIMEOUT_SECS,
            max_query_size: Self::DEFAULT_MAX_QUERY_SIZE,
            login_rate_limiter: LoginRateLimiter::new(),
            notification_registry: Arc::new(NotificationRegistry::new(256)),
            notify_state: parking_lot::Mutex::new(std::collections::HashMap::new()),
            connection_pids: parking_lot::RwLock::new(std::collections::HashMap::new()),
            lo_state: parking_lot::Mutex::new(std::collections::HashMap::new()),
            cancel_keys: parking_lot::RwLock::new(std::collections::HashMap::new()),
            cancel_notifies: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Create a handler with password-based authentication.
    ///
    /// If `password` is `Some`, auth is required with the default username
    /// "nucleus" using SCRAM-SHA-256 by default. If `None`, all connections
    /// are accepted.
    pub fn with_password(executor: Arc<Executor>, password: Option<String>) -> Self {
        Self::with_password_and_method(executor, password, AuthMethod::default())
    }

    /// Create a handler with password auth and explicit auth method.
    pub fn with_password_and_method(
        executor: Arc<Executor>,
        password: Option<String>,
        auth_method: AuthMethod,
    ) -> Self {
        let authenticator = password.map(|pw| UserAuthenticator::new("nucleus", pw));
        Self::with_auth_and_method(executor, authenticator, auth_method)
    }

    /// Create a handler with full credential configuration.
    pub fn with_auth(executor: Arc<Executor>, authenticator: Option<UserAuthenticator>) -> Self {
        Self::with_auth_and_method(executor, authenticator, AuthMethod::default())
    }

    /// Create a handler with full credential configuration and explicit auth method.
    pub fn with_auth_and_method(
        executor: Arc<Executor>,
        authenticator: Option<UserAuthenticator>,
        auth_method: AuthMethod,
    ) -> Self {
        // Keep the authenticator's credential shape in lock-step with the
        // negotiated auth method: the AuthSource must return cleartext for
        // Cleartext and a salted password + salt for SCRAM.
        let authenticator = authenticator.map(|mut auth| {
            auth.set_auth_method(auth_method);
            auth
        });
        let scram_auth = if auth_method == AuthMethod::ScramSha256 {
            authenticator
                .as_ref()
                .map(|auth| ScramAuth::new(Arc::new(auth.clone())))
        } else {
            None
        };
        let query_parser = Arc::new(NucleusQueryParser::new(executor.clone()));
        Self {
            executor,
            authenticator,
            catalog_authenticator: None,
            auth_method,
            scram_auth,
            parameter_provider: DefaultServerParameterProvider::default(),
            query_parser,
            compressor: WireCompressor::new(1024),
            session_registry: parking_lot::RwLock::new(std::collections::HashMap::new()),
            sasl_registry: parking_lot::RwLock::new(std::collections::HashMap::new()),
            copy_state: parking_lot::Mutex::new(std::collections::HashMap::new()),
            statement_timeout_secs: Self::DEFAULT_STATEMENT_TIMEOUT_SECS,
            max_query_size: Self::DEFAULT_MAX_QUERY_SIZE,
            login_rate_limiter: LoginRateLimiter::new(),
            notification_registry: Arc::new(NotificationRegistry::new(256)),
            notify_state: parking_lot::Mutex::new(std::collections::HashMap::new()),
            connection_pids: parking_lot::RwLock::new(std::collections::HashMap::new()),
            lo_state: parking_lot::Mutex::new(std::collections::HashMap::new()),
            cancel_keys: parking_lot::RwLock::new(std::collections::HashMap::new()),
            cancel_notifies: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Create a multi-user handler backed by the persisted role catalog.
    /// Catalog authentication intentionally supports SCRAM-SHA-256 only.
    pub fn with_catalog_auth(executor: Arc<Executor>) -> Self {
        let catalog_authenticator = CatalogAuthenticator {
            executor: executor.clone(),
        };
        let scram_auth = Some(ScramAuth::new(Arc::new(catalog_authenticator.clone())));
        let mut handler = Self::new(executor);
        handler.auth_method = AuthMethod::ScramSha256;
        handler.catalog_authenticator = Some(catalog_authenticator);
        handler.scram_auth = scram_auth;
        handler
    }

    /// Active authentication method for this handler.
    pub fn auth_method(&self) -> AuthMethod {
        self.auth_method
    }

    async fn handle_scram_password_message<C>(
        &self,
        client: &mut C,
        mut msg: PasswordMessageFamily,
    ) -> PgWireResult<bool>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        const SCRAM_SHA_256: &str = "SCRAM-SHA-256";
        const SCRAM_SHA_256_PLUS: &str = "SCRAM-SHA-256-PLUS";
        let peer_addr = client.socket_addr().to_string();

        let mut state = self
            .sasl_registry
            .write()
            .remove(&peer_addr)
            .unwrap_or(SASLState::Initial);

        if matches!(state, SASLState::Initial) {
            let initial = msg.into_sasl_initial_response()?;
            let selected = initial.auth_method.as_str();
            if selected != SCRAM_SHA_256 && selected != SCRAM_SHA_256_PLUS {
                return Err(PgWireError::UnsupportedSASLAuthMethod(selected.to_string()));
            }
            state = SASLState::ScramClientFirstReceived;
            msg = PasswordMessageFamily::SASLInitialResponse(initial);
        } else {
            let response = msg.into_sasl_response()?;
            msg = PasswordMessageFamily::SASLResponse(response);
        }

        let scram = self
            .scram_auth
            .as_ref()
            .ok_or_else(|| PgWireError::UnsupportedSASLAuthMethod("SCRAM".to_owned()))?;
        let (resp, new_state) = scram.process_scram_message(client, msg, &state).await?;
        client
            .send(PgWireBackendMessage::Authentication(resp))
            .await?;

        let finished = matches!(new_state, SASLState::Finished);
        if finished {
            finish_authentication(client, &self.parameter_provider).await?;
        } else {
            self.sasl_registry.write().insert(peer_addr, new_state);
        }
        Ok(finished)
    }

    /// Build a query response from executor results for a single ExecResult.
    ///
    /// When `text_only` is true (SimpleQuery protocol), all columns use text
    /// format as required by the PostgreSQL wire protocol spec.  When false
    /// (ExtendedQuery protocol), numeric types use binary encoding for
    /// performance.
    /// `formats`: the client-requested result formats from Bind (extended
    /// protocol), or `None` for the simple protocol (always text). The
    /// server must never choose binary unilaterally — a text-mode client
    /// decodes the raw bytes as a number string and reads garbage.
    fn build_response(result: ExecResult, formats: Option<&Format>) -> PgWireResult<Response> {
        match result {
            ExecResult::Select { columns, rows } => {
                let schema: Vec<FieldInfo> = columns
                    .iter()
                    .enumerate()
                    .map(|(i, (name, dt))| {
                        FieldInfo::new(
                            name.clone(),
                            None,
                            None,
                            data_type_to_pg(dt),
                            formats.map_or(FieldFormat::Text, |f| requested_format(f, i)),
                        )
                    })
                    .collect();
                let schema = Arc::new(schema);
                // Declared column types, so binary encoding matches the advertised
                // RowDescription width (see encode_value_typed).
                let col_types: Arc<Vec<DataType>> =
                    Arc::new(columns.iter().map(|(_, dt)| dt.clone()).collect());

                // Fast path for small result sets (≤10 rows): pre-encode all
                // rows into a Vec, avoiding per-row Arc::clone and lazy stream
                // overhead. This is the common case for point queries.
                if rows.len() <= 10 {
                    let mut encoded = Vec::with_capacity(rows.len());
                    for row in &rows {
                        let mut encoder = DataRowEncoder::new(Arc::clone(&schema));
                        for (i, value) in row.iter().enumerate() {
                            let fmt = schema.get(i).map_or(FieldFormat::Text, |f| f.format());
                            match col_types.get(i) {
                                Some(dt) => encode_value_typed(&mut encoder, value, dt, fmt)?,
                                None => encode_value(&mut encoder, value, fmt)?,
                            }
                        }
                        encoded.push(encoder.finish()?);
                    }
                    let data_row_stream = stream::iter(encoded.into_iter().map(Ok));
                    Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
                } else {
                    let schema_ref = Arc::clone(&schema);
                    let col_types_ref = Arc::clone(&col_types);
                    let data_row_stream = stream::iter(rows).map(move |row| {
                        let mut encoder = DataRowEncoder::new(Arc::clone(&schema_ref));
                        for (i, value) in row.iter().enumerate() {
                            let fmt = schema_ref.get(i).map_or(FieldFormat::Text, |f| f.format());
                            match col_types_ref.get(i) {
                                Some(dt) => encode_value_typed(&mut encoder, value, dt, fmt)?,
                                None => encode_value(&mut encoder, value, fmt)?,
                            }
                        }
                        encoder.finish()
                    });
                    Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
                }
            }
            ExecResult::Command { tag, rows_affected } => {
                // Postgres command tags for INSERT are "INSERT 0 <rows>".
                // Tag::with_rows appends the row count, so normalize the base tag.
                let wire_tag = if tag.eq_ignore_ascii_case("INSERT") {
                    "INSERT 0"
                } else {
                    tag.as_str()
                };
                // Transaction boundaries must use the dedicated Response
                // variants: pgwire derives the ReadyForQuery status byte
                // ('I'/'T') from them. A plain Execution response left the
                // status at Idle after BEGIN, so clients that track
                // transaction state (psycopg, Prisma's quaint) believed no
                // transaction was open, re-issued BEGIN, and their COMMIT
                // never fired — buffered transactional writes were lost.
                if tag.eq_ignore_ascii_case("BEGIN") {
                    return Ok(Response::TransactionStart(Tag::new("BEGIN")));
                }
                if tag.eq_ignore_ascii_case("COMMIT") || tag.eq_ignore_ascii_case("ROLLBACK") {
                    return Ok(Response::TransactionEnd(Tag::new(wire_tag)));
                }
                // PostgreSQL appends a row count only to row-affecting tags
                // ("INSERT 0 2", "UPDATE 3"); DDL/utility tags are bare
                // ("CREATE TABLE", "DISCARD ALL" — never "CREATE TABLE 0").
                let counted = matches!(
                    tag.split_whitespace().next().unwrap_or("").to_ascii_uppercase().as_str(),
                    "INSERT" | "UPDATE" | "DELETE" | "SELECT" | "COPY" | "FETCH" | "MOVE" | "MERGE"
                );
                if counted {
                    Ok(Response::Execution(
                        Tag::new(wire_tag).with_rows(rows_affected),
                    ))
                } else {
                    Ok(Response::Execution(Tag::new(wire_tag)))
                }
            }
            ExecResult::CopyOut { row_count, .. } => {
                Ok(Response::Execution(Tag::new("COPY").with_rows(row_count)))
            }
            // Binary COPY TO is sent inline by the simple-query loop; the
            // extended protocol has no inline path (same constraint as the
            // streaming variant below).
            ExecResult::CopyOutBinary { .. } => Err(PgWireError::ApiError(
                "binary COPY TO STDOUT requires the simple query protocol".into(),
            )),
            // Streaming SELECT: pull batches lazily from the producer and encode
            // rows as they arrive, so peak memory is O(batch) and the first row
            // reaches the client without materializing the whole result. Reaches
            // the wire only when the session opted in (SET stream_results = on);
            // otherwise the dispatch boundary materialized it into `Select` above.
            // The per-row encoding is identical to the `Select` arm.
            ExecResult::SelectStream { columns, source } => {
                let schema: Vec<FieldInfo> = columns
                    .iter()
                    .enumerate()
                    .map(|(i, (name, dt))| {
                        FieldInfo::new(
                            name.clone(),
                            None,
                            None,
                            data_type_to_pg(dt),
                            formats.map_or(FieldFormat::Text, |f| requested_format(f, i)),
                        )
                    })
                    .collect();
                let schema = Arc::new(schema);
                let col_types: Arc<Vec<DataType>> =
                    Arc::new(columns.iter().map(|(_, dt)| dt.clone()).collect());

                struct StreamState {
                    source: Box<dyn crate::executor::row_batch::RowBatchIter>,
                    buf: std::vec::IntoIter<crate::types::Row>,
                    done: bool,
                }
                let init = StreamState {
                    source,
                    buf: Vec::new().into_iter(),
                    done: false,
                };
                let schema_ref = Arc::clone(&schema);
                let data_row_stream = stream::unfold(init, move |mut st| {
                    let schema = Arc::clone(&schema_ref);
                    let col_types = Arc::clone(&col_types);
                    async move {
                        if st.done {
                            return None;
                        }
                        loop {
                            if let Some(row) = st.buf.next() {
                                let mut encoder = DataRowEncoder::new(Arc::clone(&schema));
                                let encoded = (|| {
                                    for (i, value) in row.iter().enumerate() {
                                        let fmt =
                                            schema.get(i).map_or(FieldFormat::Text, |f| f.format());
                                        match col_types.get(i) {
                                            Some(dt) => encode_value_typed(&mut encoder, value, dt, fmt)?,
                                            None => encode_value(&mut encoder, value, fmt)?,
                                        }
                                    }
                                    encoder.finish()
                                })();
                                return Some((encoded, st));
                            }
                            // Buffer drained — pull the next batch.
                            match st.source.next_batch().await {
                                Ok(Some(batch)) => {
                                    st.buf = batch.into_iter();
                                    continue;
                                }
                                Ok(None) => {
                                    // End of stream — unfold stops on None, so no
                                    // need to flip `done` (st is dropped here).
                                    return None;
                                }
                                Err(e) => {
                                    // Surface the producer error as one stream error,
                                    // then stop.
                                    st.done = true;
                                    return Some((Err(exec_error_to_pgwire(e)), st));
                                }
                            }
                        }
                    }
                });
                Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
            }
            // Streaming COPY is intercepted and sent inline in the simple-query
            // loop (it needs the client sink); a non-wire caller materializes it
            // to CopyOut first, so build_response never legitimately sees it.
            ExecResult::CopyOutStream { .. } => Err(PgWireError::ApiError(
                "streaming COPY must be sent inline, not via build_response".into(),
            )),
        }
    }

    /// Get the session ID for a client connection from the session registry.
    fn session_id_from_client(&self, client: &impl ClientInfo) -> u64 {
        let addr = client.socket_addr().to_string();
        self.session_registry
            .read()
            .get(&addr)
            .copied()
            .unwrap_or(0)
    }

    /// Mirror the executor's per-session transaction state into the wire
    /// client so ReadyForQuery reports 'T' inside BEGIN..COMMIT. Clients that
    /// verify transaction state (Prisma's quaint) refuse to proceed when a
    /// BEGIN is acknowledged with an idle status.
    fn sync_transaction_status(&self, client: &mut impl ClientInfo, session_id: u64) {
        use pgwire::messages::response::TransactionStatus;
        let status = if self.executor.session_in_transaction(session_id) {
            TransactionStatus::Transaction
        } else {
            TransactionStatus::Idle
        };
        client.set_transaction_status(status);
    }

    /// Execute a SQL query through the executor within the given session,
    /// returning an error suitable for the wire protocol on failure.
    /// Enforces the statement timeout and max query size limits.
    async fn execute_sql_session(
        &self,
        session_id: u64,
        sql: &str,
    ) -> PgWireResult<Vec<ExecResult>> {
        // Enforce max query size
        if sql.len() > self.max_query_size {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "54000".to_owned(), // program_limit_exceeded
                format!(
                    "query too large: {} bytes exceeds limit of {} bytes",
                    sql.len(),
                    self.max_query_size
                ),
            ))));
        }

        let fut = self.executor.execute_with_session(session_id, sql);

        // Race execution against a wire CancelRequest for this session.
        // `biased` checks the cancel signal first so a cancel that arrived
        // while execution was inside a blocking (non-yielding) region still
        // wins at the next poll. Granularity matches statement_timeout: the
        // cancel takes effect at the executor's next await point.
        let cancel = self.cancel_notifies.read().get(&session_id).cloned();
        let fut = async move {
            match cancel {
                Some(notify) => {
                    tokio::select! {
                        biased;
                        _ = notify.notified() => Err(PgWireError::UserError(Box::new(
                            ErrorInfo::new(
                                "ERROR".to_owned(),
                                "57014".to_owned(), // query_canceled
                                "canceling statement due to user request".to_owned(),
                            ),
                        ))),
                        result = fut => result.map_err(exec_error_to_pgwire),
                    }
                }
                None => fut.await.map_err(exec_error_to_pgwire),
            }
        };

        // Per-session statement_timeout overrides the global default.
        // Units follow PostgreSQL: a bare number is MILLISECONDS; "Ns"/"Nms"
        // suffixes are accepted. The server-config default stays in seconds.
        let timeout_ms = self
            .executor
            .get_session_setting(session_id, "statement_timeout")
            .and_then(|v| parse_timeout_ms(&v))
            .unwrap_or(self.statement_timeout_secs * 1000);

        if timeout_ms > 0 {
            match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), fut).await {
                Ok(result) => result,
                Err(_elapsed) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "57014".to_owned(), // query_canceled
                    format!("canceling statement due to statement timeout ({timeout_ms}ms)",),
                )))),
            }
        } else {
            fut.await
        }
    }

    /// Execute a SQL query using the default session (for internal/test use).
    async fn execute_sql(&self, sql: &str) -> PgWireResult<Vec<ExecResult>> {
        self.execute_sql_session(0, sql).await
    }

    /// Infer parameter types from SQL placeholders.
    ///
    /// Counts `$N` placeholders, then for each one tries to resolve a Postgres
    /// wire `Type` in this priority order:
    ///   1. The type explicitly declared by the client in the Parse message.
    ///   2. A type inferred from the AST context (e.g. the column on the
    ///      other side of a comparison, or an explicit `CAST($N AS …)`).
    ///   3. `Type::TEXT` as a safe last resort.
    ///
    /// Step 2 is what unblocks pgx clients: without it, every undeclared `$N`
    /// is advertised as TEXT, so pgx refuses to bind a Go `int64` to a
    /// `WHERE bigint_col >= $1` parameter ("cannot find encode plan").
    #[allow(dead_code)] // kept as a thin wrapper used by unit tests
    fn infer_parameter_types(sql: &str, declared: &[Option<Type>]) -> Vec<Type> {
        Self::infer_parameter_types_with_ast(sql, declared, None, None)
    }

    /// AST-aware variant of `infer_parameter_types`.  When the parsed AST and
    /// executor are available, walks the AST to derive each `$N` parameter\'s
    /// type from sibling expressions (column references resolved through the
    /// catalog, or explicit `CAST` targets).
    fn infer_parameter_types_with_ast(
        sql: &str,
        declared: &[Option<Type>],
        ast: Option<&[sqlparser::ast::Statement]>,
        executor: Option<&Arc<Executor>>,
    ) -> Vec<Type> {
        let param_count = count_placeholders(sql);
        let count = param_count.max(declared.len());

        let inferred: Vec<Option<Type>> = match (ast, executor) {
            (Some(stmts), Some(exec)) => infer_param_types_from_ast(stmts, exec, count),
            _ => vec![None; count],
        };

        (0..count)
            .map(|i| {
                declared
                    .get(i)
                    .and_then(|t| t.clone())
                    .or_else(|| inferred.get(i).cloned().flatten())
                    .unwrap_or(Type::TEXT)
            })
            .collect()
    }

    /// Substitute `$1`, `$2`, ... placeholders with parameter values.
    ///
    /// Parameters are provided as raw bytes from the portal. We decode them
    /// as UTF-8 text (since we use text format) and substitute into the SQL.
    ///
    /// Security: replacements are escaped (single quotes doubled, backslashes
    /// doubled, NUL bytes stripped). Substitution is done in a single pass over
    /// the original SQL text so repeated placeholders are handled correctly and
    /// replacement values cannot trigger recursive substitution.
    fn substitute_parameters(sql: &str, portal: &Portal<ParsedStatement>) -> PgWireResult<String> {
        Self::substitute_parameters_with_executor(sql, portal, None)
    }

    /// Same as [`substitute_parameters`] but accepts an optional executor so
    /// we can run the AST-driven parameter type inference (column lookups via
    /// the catalog).  When the client did not declare a type for `$N`, the
    /// inferred type is used; when even inference fails, we fall back to TEXT.
    fn substitute_parameters_with_executor(
        sql: &str,
        portal: &Portal<ParsedStatement>,
        executor: Option<&Arc<Executor>>,
    ) -> PgWireResult<String> {
        let param_count = portal.parameter_len();
        let mut replacements = Vec::with_capacity(param_count);

        let inferred = Self::infer_parameter_types_with_ast(
            sql,
            &portal.statement.parameter_types,
            portal.statement.statement.ast.as_deref(),
            executor,
        );

        for i in 0..param_count {
            let type_hint = inferred.get(i).cloned().unwrap_or(Type::TEXT);

            let replacement = match decode_pg_param(portal, i, &type_hint) {
                Some(DecodedParam::Null) | None => "NULL".to_owned(),
                Some(DecodedParam::Numeric(s)) | Some(DecodedParam::Bool(s)) => s,
                Some(DecodedParam::Text(s)) => {
                    format!("\'{}\'", sanitize_sql_text_literal(&s))
                }
            };
            replacements.push(replacement);
        }

        Ok(substitute_positional_placeholders(sql, &replacements))
    }

    /// Substitute `$1`, `$2`, ... placeholders in a raw SQL string with the
    /// provided parameter values. This is the non-portal version used for
    /// testing and internal callers.
    ///
    /// Same escaping rules as `substitute_parameters`.
    #[cfg(test)]
    fn substitute_parameters_raw(sql: &str, params: &[&str]) -> String {
        let replacements: Vec<String> = params
            .iter()
            .map(|value| {
                if *value == "NULL" {
                    "NULL".to_owned()
                } else {
                    format!("'{}'", sanitize_sql_text_literal(value))
                }
            })
            .collect();
        substitute_positional_placeholders(sql, &replacements)
    }

    /// Try to execute using the cached AST with parameter substitution.
    /// Returns `Err(())` on any issue (type conversion, etc.) — caller falls back to string path.
    #[allow(clippy::type_complexity)]
    fn try_ast_execute<'a>(
        executor: &'a Arc<Executor>,
        session_id: u64,
        cached_ast: &[sqlparser::ast::Statement],
        portal: &Portal<ParsedStatement>,
    ) -> Result<
        std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Vec<ExecResult>, ExecError>> + Send + 'a>,
        >,
        (),
    > {
        let param_count = portal.parameter_len();
        let mut param_values = Vec::with_capacity(param_count);

        // Re-derive inferred parameter types from the cached AST so binary
        // numeric/bool params get decoded with the right type even when the
        // client did not declare one in Parse.  We pass the executor through
        // so column references in the AST can be resolved against the catalog.
        let inferred = Self::infer_parameter_types_with_ast(
            &portal.statement.statement.sql,
            &portal.statement.parameter_types,
            Some(cached_ast),
            Some(executor),
        );

        for i in 0..param_count {
            let type_hint = inferred.get(i).cloned().unwrap_or(Type::TEXT);

            let value = match decode_pg_param(portal, i, &type_hint) {
                Some(DecodedParam::Null) | None => Value::Null,
                Some(DecodedParam::Numeric(s))
                | Some(DecodedParam::Bool(s))
                | Some(DecodedParam::Text(s)) => Self::pg_string_to_value(&s, &type_hint),
            };
            param_values.push(value);
        }

        // Clone the AST and substitute parameters
        let mut statements = cached_ast.to_vec();
        for stmt in &mut statements {
            crate::executor::param_subst::substitute_params_in_stmt(stmt, &param_values);
        }

        Ok(executor.execute_statements_with_session(session_id, statements))
    }

    /// Convert a postgres text parameter to a Nucleus Value based on the type hint.
    fn pg_string_to_value(s: &str, type_hint: &Type) -> Value {
        match *type_hint {
            Type::INT2 | Type::INT4 => s
                .parse::<i32>()
                .map(Value::Int32)
                .unwrap_or(Value::Text(s.to_owned())),
            Type::INT8 => s
                .parse::<i64>()
                .map(Value::Int64)
                .unwrap_or(Value::Text(s.to_owned())),
            Type::FLOAT4 | Type::FLOAT8 => s
                .parse::<f64>()
                .map(Value::Float64)
                .unwrap_or(Value::Text(s.to_owned())),
            Type::BOOL => match s {
                "t" | "true" | "TRUE" | "1" => Value::Bool(true),
                "f" | "false" | "FALSE" | "0" => Value::Bool(false),
                _ => Value::Text(s.to_owned()),
            },
            _ => Value::Text(s.to_owned()),
        }
    }
}

fn sanitize_sql_text_literal(value: &str) -> String {
    // Nucleus parses with PostgreSqlDialect, which is standard-conforming:
    // backslashes inside '...' are LITERAL characters, so they must NOT be
    // doubled here (doubling corrupted any text parameter containing '\',
    // e.g. Windows paths). Only quote-doubling and NUL-stripping apply.
    value.replace('\0', "").replace('\'', "''")
}

fn substitute_positional_placeholders(sql: &str, replacements: &[String]) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while i < bytes.len() {
        if in_line_comment {
            out.push(bytes[i] as char);
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                out.push('*');
                out.push('/');
                in_block_comment = false;
                i += 2;
            } else {
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }
        if in_single {
            out.push(bytes[i] as char);
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                } else {
                    in_single = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        if in_double {
            out.push(bytes[i] as char);
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    out.push('"');
                    i += 2;
                } else {
                    in_double = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            out.push('-');
            out.push('-');
            in_line_comment = true;
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            out.push('/');
            out.push('*');
            in_block_comment = true;
            i += 2;
            continue;
        }
        if bytes[i] == b'\'' {
            out.push('\'');
            in_single = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'"' {
            out.push('"');
            in_double = true;
            i += 1;
            continue;
        }

        if bytes[i] == b'$' {
            let start = i;
            i += 1;
            let mut idx = 0usize;
            let mut found_digit = false;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                found_digit = true;
                idx = idx * 10 + (bytes[i] - b'0') as usize;
                i += 1;
            }
            if found_digit {
                if idx > 0 && idx <= replacements.len() {
                    out.push_str(&replacements[idx - 1]);
                } else {
                    out.push_str(&sql[start..i]);
                }
                continue;
            }
            out.push('$');
            continue;
        }

        out.push(bytes[i] as char);
        i += 1;
    }

    out
}

// ============================================================================
// Startup Handler
// ============================================================================

#[async_trait]
impl StartupHandler for NucleusHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            // ── Startup message: negotiate protocol + auth flow ──────────
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);
                // Create a per-connection session for state isolation.
                let auth_required =
                    self.authenticator.is_some() || self.catalog_authenticator.is_some();
                let session_id = if auth_required {
                    self.executor.create_unauthenticated_session()
                } else {
                    self.executor.create_session()
                };
                // The simple-query loop drains a streaming COPY inline (CopyData
                // per batch), so this consumer can handle a lazy stream — let COPY
                // TO STDOUT stream by default for it (bounded-memory export).
                self.executor.mark_session_stream_capable(session_id);
                let addr = client.socket_addr().to_string();
                self.session_registry.write().insert(addr.clone(), session_id);

                // Issue the cancellation key sent in BackendKeyData (during
                // finish_authentication) and register it so a later
                // CancelRequest on a fresh connection can interrupt this
                // session's running query.
                let pid = self.connection_pid(&addr);
                let secret = SecretKey::I32(rand::random::<i32>());
                client.set_pid_and_secret_key(pid, secret.clone());
                self.cancel_keys.write().insert(pid, (secret, session_id));
                self.cancel_notifies
                    .write()
                    .insert(session_id, Arc::new(tokio::sync::Notify::new()));

                if !auth_required {
                    finish_authentication(client, &self.parameter_provider).await?;
                } else {
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    match self.auth_method {
                        AuthMethod::Cleartext => {
                            // Reject cleartext password auth over unencrypted connections
                            // to prevent credential sniffing.
                            if !client.is_secure() {
                                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                                    "FATAL".to_owned(),
                                    "28000".to_owned(),
                                    "cleartext password authentication requires a TLS connection"
                                        .to_owned(),
                                ))));
                            }
                            client
                                .send(PgWireBackendMessage::Authentication(
                                    Authentication::CleartextPassword,
                                ))
                                .await?;
                        }
                        AuthMethod::ScramSha256 => {
                            self.sasl_registry
                                .write()
                                .insert(client.socket_addr().to_string(), SASLState::Initial);
                            client
                                .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                                    vec!["SCRAM-SHA-256".to_string()],
                                )))
                                .await?;
                        }
                    }
                }
            }

            // ── Password response: verify against configured auth mode ───
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                if self.authenticator.is_some() || self.catalog_authenticator.is_some() {
                    // ── Rate-limit check: reject if too many recent failures ──
                    let source_ip = client.socket_addr().ip();
                    if self.login_rate_limiter.is_locked_out(source_ip) {
                        self.cleanup_session(&client.socket_addr().to_string());
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "FATAL".to_owned(),
                            "28P01".to_owned(), // invalid_password
                            "too many failed login attempts, try again later".to_owned(),
                        ))));
                    }

                    let (result, authentication_complete) = match self.auth_method {
                        AuthMethod::Cleartext => {
                            let auth = self.authenticator.as_ref().ok_or_else(|| {
                                PgWireError::UserError(Box::new(ErrorInfo::new(
                                    "FATAL".to_owned(),
                                    "0A000".to_owned(),
                                    "catalog authentication supports SCRAM-SHA-256 only".to_owned(),
                                )))
                            })?;
                            let pwd = pwd.into_password()?;
                            let login_info = LoginInfo::from_client_info(client);
                            let expected = auth.get_password(&login_info).await?;
                            if constant_time_eq(expected.password(), pwd.password.as_bytes()) {
                                (
                                    finish_authentication(client, &self.parameter_provider).await,
                                    true,
                                )
                            } else {
                                let user =
                                    login_info.user().map(|u| u.to_owned()).unwrap_or_default();
                                (Err(PgWireError::InvalidPassword(user)), false)
                            }
                        }
                        AuthMethod::ScramSha256 => {
                            match self.handle_scram_password_message(client, pwd).await {
                                Ok(done) => (Ok(()), done),
                                Err(e) => (Err(e), false),
                            }
                        }
                    };

                    if let Err(e) = result {
                        self.login_rate_limiter.record_failure(source_ip);
                        self.cleanup_session(&client.socket_addr().to_string());
                        return Err(e);
                    }
                    if authentication_complete {
                        let login_info = LoginInfo::from_client_info(client);
                        let user = login_info.user().unwrap_or("");
                        let session_id = self.session_id_from_client(client);
                        self.executor
                            .bind_authenticated_session(session_id, user)
                            .await
                            .map_err(exec_error_to_pgwire)?;
                        // Successful auth: clear any prior failure record.
                        self.login_rate_limiter.clear(source_ip);
                    }
                } else {
                    tracing::warn!("Received password message but authentication is disabled");
                }
            }

            // ── Anything else: ignore ────────────────────────────────────
            _ => {
                tracing::warn!("Unexpected startup message, ignoring");
            }
        }

        Ok(())
    }
}

// ============================================================================
// Simple Query Handler
// ============================================================================

#[async_trait]
impl SimpleQueryHandler for NucleusHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let peer_addr_str = client.socket_addr().to_string();
        let session_id = self.session_id_from_client(client);
        // New client command: a cancel for a previous command must not leak
        // into this one.
        self.executor.clear_session_cancel(session_id);

        // Inside an active transaction, all autocommit fast paths are disabled:
        // they bypass the session's MVCC snapshot and write straight to storage,
        // which would auto-commit writes the transaction must be able to
        // ROLLBACK and break read-your-own-writes. Route everything through the
        // session-scoped executor until COMMIT/ROLLBACK.
        let in_txn = self.executor.session_in_transaction(session_id);
        let rls_active = self.executor.session_has_active_rls(session_id);

        // ── Large Objects fast path: intercept lo_* function calls ───────
        if !in_txn
            && !rls_active
            && let Some(lo_result) = self.try_handle_large_object(&peer_addr_str, query)
        {
            let resp = Self::build_response(lo_result, None)?;
            self.flush_pending_notifications(client).await?;
            return Ok(vec![resp]);
        }

        // ── LISTEN/NOTIFY wire-level interception ───────────────────────
        // We intercept LISTEN/NOTIFY/UNLISTEN here to register the
        // connection in the notification registry, in addition to the
        // executor's pubsub hub. The executor still runs the statement
        // (for distributed NOTIFY), but we also track it at the wire level
        // for NotificationResponse delivery.
        {
            let trimmed_upper = query.trim().to_uppercase();
            if trimmed_upper.starts_with("LISTEN ") {
                let channel = query.trim()[7..].trim().trim_end_matches(';').trim();
                self.handle_listen(&peer_addr_str, channel);
            } else if trimmed_upper.starts_with("UNLISTEN ") {
                let channel = query.trim()[9..].trim().trim_end_matches(';').trim();
                self.handle_unlisten(&peer_addr_str, channel);
            } else if trimmed_upper.starts_with("NOTIFY ") {
                // Parse: NOTIFY channel [, 'payload']
                let rest = query.trim()[7..].trim().trim_end_matches(';').trim();
                let (channel, payload) = if let Some(comma) = rest.find(',') {
                    let ch = rest[..comma].trim();
                    let pl = rest[comma + 1..].trim().trim_matches('\'');
                    (ch, pl)
                } else {
                    (rest, "")
                };
                self.handle_notify(&peer_addr_str, channel, payload);
            }
        }

        // ── KV fast path: intercept common KV queries before SQL parsing ──
        if !in_txn
            && !rls_active
            && let Some(kv_cmd) = kv_fast_path::try_parse_kv(query)
        {
            // This path executes against the KV store directly, so it must
            // consult the degraded-mode gate itself — otherwise a read-only
            // server would still accept `SELECT kv_set(...)`.
            if kv_cmd.is_write()
                && let Err(e) = self.executor.service().admit_write("KV write")
            {
                return Err(exec_error_to_pgwire(e));
            }
            let result = kv_fast_path::execute_kv_command(&kv_cmd, self.executor.kv_store());
            // A KV write must be durable before it is acked — this path bypasses
            // execute()'s commit-time force, so force here (no-op under
            // synchronous_commit=off, matching the SQL fast path).
            if kv_cmd.is_write() {
                self.executor
                    .kv_fast_path_durability()
                    .map_err(exec_error_to_pgwire)?;
            }
            self.flush_pending_notifications(client).await?;
            return Ok(vec![Self::build_response(result, None)?]);
        }

        // ── SQL OLTP fast path: intercept simple point queries/mutations ──
        if !in_txn
            && !rls_active
            && let Some(sql_cmd) = kv_fast_path::try_parse_sql_fast_path(query)
            && let Some(result) = self.executor.execute_sql_fast_path(&sql_cmd).await
        {
            self.flush_pending_notifications(client).await?;
            return Ok(vec![Self::build_response(
                result.map_err(exec_error_to_pgwire)?,
                None,
            )?]);
        }
        // Fall through to normal path if fast-path couldn't handle it
        // (e.g. table not found in cache, column mismatch, etc.)

        // Detect COPY ... FROM STDIN and enter copy-in mode.
        if let Some(copy_info) = detect_copy_from_stdin(query) {
            let peer_addr = client.socket_addr();
            let is_binary = copy_info.is_binary;
            // Binary CopyInResponse advertises per-column binary format codes.
            let ncols = if is_binary {
                match &copy_info.columns {
                    Some(cols) => cols.len(),
                    None => self
                        .executor
                        .table_column_types(&copy_info.table)
                        .map_or(0, |c| c.len()),
                }
            } else {
                0
            };
            self.copy_state.lock().insert(
                peer_addr,
                CopyInProgress {
                    table: copy_info.table,
                    columns: copy_info.columns,
                    delimiter: copy_info.delimiter,
                    is_csv: copy_info.is_csv,
                    is_binary,
                    has_header: copy_info.has_header,
                    data: Vec::new(),
                    session_id,
                },
            );
            let response = if is_binary {
                CopyResponse::new(1, ncols, vec![1; ncols])
            } else {
                CopyResponse::new(0, 0, vec![])
            };
            return Ok(vec![Response::CopyIn(response)]);
        }

        let results = self.execute_sql_session(session_id, query).await?;

        let mut responses = Vec::new();
        let mut bytes_estimate: u64 = 0;
        for result in results {
            // COPY TO STDOUT (FORMAT binary): one binary payload under a
            // format=1 CopyOutResponse with per-column binary format codes.
            if let crate::executor::ExecResult::CopyOutBinary {
                data,
                row_count,
                columns,
            } = result
            {
                use pgwire::api::copy::send_copy_out_response;
                bytes_estimate += data.len() as u64;
                send_copy_out_response(
                    client,
                    CopyResponse::new(1, columns, vec![1; columns]),
                )
                .await?;
                const CHUNK_SIZE: usize = 65_536;
                for chunk in data.chunks(CHUNK_SIZE) {
                    client
                        .send(PgWireBackendMessage::CopyData(CopyData::new(
                            bytes::Bytes::copy_from_slice(chunk),
                        )))
                        .await?;
                }
                client
                    .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
                    .await?;
                client
                    .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                        format!("COPY {row_count}"),
                    )))
                    .await?;
                continue;
            }
            // COPY TO STDOUT: stream rows directly rather than returning a Response.
            if let crate::executor::ExecResult::CopyOut { data, row_count } = result {
                use pgwire::api::copy::send_copy_out_response;
                bytes_estimate += data.len() as u64;
                send_copy_out_response(client, CopyResponse::new(0, 0, vec![])).await?;
                if !data.is_empty() {
                    // Send data in 64KB chunks to avoid a single massive allocation
                    // for large COPY TO results. Each chunk is a separate CopyData message.
                    const CHUNK_SIZE: usize = 65_536;
                    let bytes = data.into_bytes();
                    for chunk in bytes.chunks(CHUNK_SIZE) {
                        client
                            .send(PgWireBackendMessage::CopyData(CopyData::new(
                                bytes::Bytes::copy_from_slice(chunk),
                            )))
                            .await?;
                    }
                }
                client
                    .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
                    .await?;
                client
                    .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                        format!("COPY {row_count}"),
                    )))
                    .await?;
                // Return empty — pgwire's on_query will send ReadyForQuery.
                self.executor.metrics().bytes_sent.inc_by(bytes_estimate);
                self.flush_pending_notifications(client).await?;
                return Ok(vec![]);
            }
            // Streaming COPY TO STDOUT: pull row batches and format+send each as
            // CopyData, so a full-table export never buffers the whole table.
            // Byte-identical to the materialized CopyOut path (shared formatters).
            if let crate::executor::ExecResult::CopyOutStream {
                mut source,
                columns,
                is_csv,
                delimiter,
                include_header,
            } = result
            {
                use crate::executor::copy::{format_copy_body, format_copy_header};
                use pgwire::api::copy::send_copy_out_response;
                const CHUNK_SIZE: usize = 65_536;
                send_copy_out_response(client, CopyResponse::new(0, 0, vec![])).await?;

                // Emit the CSV header (if any) as the first CopyData payload.
                if include_header {
                    let header = format_copy_header(&columns, is_csv, delimiter);
                    if !header.is_empty() {
                        bytes_estimate += header.len() as u64;
                        for chunk in header.into_bytes().chunks(CHUNK_SIZE) {
                            client
                                .send(PgWireBackendMessage::CopyData(CopyData::new(
                                    bytes::Bytes::copy_from_slice(chunk),
                                )))
                                .await?;
                        }
                    }
                }
                let mut row_count = 0usize;
                loop {
                    match source.next_batch().await {
                        Ok(Some(batch)) => {
                            row_count += batch.len();
                            let payload = format_copy_body(&batch, is_csv, delimiter);
                            bytes_estimate += payload.len() as u64;
                            for chunk in payload.into_bytes().chunks(CHUNK_SIZE) {
                                client
                                    .send(PgWireBackendMessage::CopyData(CopyData::new(
                                        bytes::Bytes::copy_from_slice(chunk),
                                    )))
                                    .await?;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => return Err(exec_error_to_pgwire(e)),
                    }
                }
                client
                    .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
                    .await?;
                client
                    .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                        format!("COPY {row_count}"),
                    )))
                    .await?;
                self.executor.metrics().bytes_sent.inc_by(bytes_estimate);
                self.flush_pending_notifications(client).await?;
                return Ok(vec![]);
            }
            // Approximate wire bytes: count rows * avg 64 bytes per row + header
            bytes_estimate += Self::estimate_result_bytes(&result);
            responses.push(Self::build_response(result, None)?);
        }
        if bytes_estimate > 0 {
            self.executor.metrics().bytes_sent.inc_by(bytes_estimate);
        }

        // ── Flush pending notifications before ReadyForQuery ────────────
        self.flush_pending_notifications(client).await?;

        // ReadyForQuery must report 'T' inside an open transaction — clients
        // that verify BEGIN took effect (Prisma's quaint) abort otherwise.
        self.sync_transaction_status(client, session_id);

        Ok(responses)
    }
}

// ============================================================================
// Extended Query Handler (Prepared Statements)
// ============================================================================

#[async_trait]
impl ExtendedQueryHandler for NucleusHandler {
    type Statement = ParsedStatement;
    type QueryParser = NucleusQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &stmt.statement.sql;
        let param_types = Self::infer_parameter_types_with_ast(
            sql,
            &stmt.parameter_types,
            stmt.statement.ast.as_deref(),
            Some(&self.executor),
        );

        // Try to determine result columns by examining the query.
        // For SELECT statements, we can execute with dummy values to get the
        // schema. For non-SELECT statements, return no data.
        let fields = if is_select_query(sql) {
            // Statement-level Describe happens BEFORE Bind, so placeholders
            // are unbound. Probe with NULL in their place — NULL comparisons
            // yield no rows but the result SCHEMA is identical, which is all
            // Describe needs. (Prisma describes statements, not portals; the
            // old probe executed `$1` literally, errored, and advertised zero
            // fields — its query engine then panicked on the arity mismatch.)
            let probe_sql = replace_placeholders_with_null(sql);
            match self.describe_select_columns(&probe_sql, None).await {
                Ok(cols) => cols,
                Err(e) => {
                    tracing::warn!("Failed to describe SELECT columns: {e}");
                    Vec::new()
                }
            }
        } else {
            describe_returning_fields(stmt.statement.ast.as_deref(), &self.executor, None)
        };

        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &portal.statement.statement.sql;

        let fields = if is_select_query(sql) {
            // With bound parameters available, we can try to determine columns
            // more accurately by substituting and executing.
            let substituted =
                Self::substitute_parameters_with_executor(sql, portal, Some(&self.executor))?;
            match self
                .describe_select_columns(&substituted, Some(&portal.result_column_format))
                .await
            {
                Ok(cols) => cols,
                Err(e) => {
                    tracing::warn!("Failed to describe SELECT columns: {e}");
                    Vec::new()
                }
            }
        } else {
            describe_returning_fields(
                portal.statement.statement.ast.as_deref(),
                &self.executor,
                Some(&portal.result_column_format),
            )
        };

        Ok(DescribePortalResponse::new(fields))
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let parsed_stmt = &portal.statement.statement;
        let session_id = self.session_id_from_client(client);
        // NOTE: no cancel-flag clear at entry. Extended-protocol clients
        // pipeline Parse+Describe+Bind+Execute in one batch; a cancel that
        // lands while the Describe probe runs targets this same client
        // operation, and clearing at Execute entry would drop it. Instead the
        // flag clears when this Execute finishes (drop guard), so a cancel
        // that arrived too late to stop this command can't leak into the
        // next one.
        struct ClearCancelOnDrop {
            executor: Arc<Executor>,
            session_id: u64,
        }
        impl Drop for ClearCancelOnDrop {
            fn drop(&mut self) {
                self.executor.clear_session_cancel(self.session_id);
            }
        }
        let _clear_cancel = ClearCancelOnDrop {
            executor: self.executor.clone(),
            session_id,
        };
        let peer_addr_str = client.socket_addr().to_string();
        let rls_active = self.executor.session_has_active_rls(session_id);

        // ── Large Objects fast path (extended query) ────────────────────
        if !rls_active
            && let Some(lo_result) = self.try_handle_large_object(&peer_addr_str, &parsed_stmt.sql)
        {
            self.flush_pending_notifications(client).await?;
            return Self::build_response(lo_result, Some(&portal.result_column_format));
        }

        // ── LISTEN/NOTIFY wire-level registration (extended query) ──────
        {
            let trimmed_upper = parsed_stmt.sql.trim().to_uppercase();
            if trimmed_upper.starts_with("LISTEN ") {
                let channel = parsed_stmt.sql.trim()[7..]
                    .trim()
                    .trim_end_matches(';')
                    .trim();
                self.handle_listen(&peer_addr_str, channel);
            } else if trimmed_upper.starts_with("UNLISTEN ") {
                let channel = parsed_stmt.sql.trim()[9..]
                    .trim()
                    .trim_end_matches(';')
                    .trim();
                self.handle_unlisten(&peer_addr_str, channel);
            } else if trimmed_upper.starts_with("NOTIFY ") {
                let rest = parsed_stmt.sql.trim()[7..]
                    .trim()
                    .trim_end_matches(';')
                    .trim();
                let (channel, payload) = if let Some(comma) = rest.find(',') {
                    let ch = rest[..comma].trim();
                    let pl = rest[comma + 1..].trim().trim_matches('\'');
                    (ch, pl)
                } else {
                    (rest, "")
                };
                self.handle_notify(&peer_addr_str, channel, payload);
            }
        }

        // AST fast path: if we have a cached AST, substitute parameters directly
        // in the AST and execute without re-parsing.
        let results = if let Some(ref cached_ast) = parsed_stmt.ast {
            // Pre-populate the plan cache key hint from the Parse phase so that
            // execute_query() can look up cached plans without the expensive
            // query.to_string() + normalize_sql_for_cache() round-trip.
            if let Some(ref key) = parsed_stmt.plan_cache_key {
                self.executor.set_plan_cache_key_hint(key.clone());
            }
            match Self::try_ast_execute(&self.executor, session_id, cached_ast, portal) {
                Ok(fut) => fut.await.map_err(exec_error_to_pgwire),
                Err(_) => {
                    // Fall back to string-based substitution + re-parse
                    let resolved_sql = Self::substitute_parameters(&parsed_stmt.sql, portal)?;
                    self.execute_sql_session(session_id, &resolved_sql).await
                }
            }
        } else {
            // No cached AST — use string path
            let resolved_sql = Self::substitute_parameters_with_executor(
                &parsed_stmt.sql,
                portal,
                Some(&self.executor),
            )?;
            self.execute_sql_session(session_id, &resolved_sql).await
        }?;

        // The extended protocol returns a single Response per Execute.
        // If there are multiple statements, take the last result.
        if let Some(mut result) = results.into_iter().last() {
            // The extended protocol returns a single Response via build_response,
            // which has no inline COPY path and cannot serialize a streaming COPY
            // (that is sent as CopyData in the simple-query loop). Collapse a
            // streaming COPY to its materialized CopyOut here so extended-protocol
            // COPY TO STDOUT keeps working when the session is stream-capable.
            if matches!(result, ExecResult::CopyOutStream { .. }) {
                result = result.materialize().await.map_err(exec_error_to_pgwire)?;
            }
            // Respect max_rows from the Execute message. When max_rows > 0,
            // the client only wants that many rows. (Full cursor/PortalSuspended
            // support would require pgwire to expose that response variant.)
            if max_rows > 0
                && let ExecResult::Select { ref mut rows, .. } = result
            {
                rows.truncate(max_rows);
            }
            let bytes_est = Self::estimate_result_bytes(&result);
            if bytes_est > 0 {
                self.executor.metrics().bytes_sent.inc_by(bytes_est);
            }
            // Flush pending notifications before the response (before ReadyForQuery).
            self.flush_pending_notifications(client).await?;
            self.sync_transaction_status(client, session_id);
            Self::build_response(result, Some(&portal.result_column_format))
        } else {
            self.flush_pending_notifications(client).await?;
            self.sync_transaction_status(client, session_id);
            Ok(Response::EmptyQuery)
        }
    }
}

// ============================================================================
// Pipeline Mode Support
// ============================================================================

/// Pipeline mode documentation and semantics.
///
/// The PostgreSQL extended query protocol inherently supports pipelining:
/// clients can send multiple Parse/Bind/Describe/Execute messages without
/// waiting for responses, followed by a Sync message that marks a
/// synchronization point.
///
/// pgwire's `process_socket` loop already handles this correctly — it reads
/// frontend messages in a loop and dispatches them to the appropriate handler
/// methods. Each Sync triggers a ReadyForQuery response.
///
/// Error isolation between pipeline segments is provided by the pgwire
/// framework: when an error occurs during a pipeline segment (between two
/// Sync messages), the framework sends an ErrorResponse and skips all
/// remaining messages in that segment until the next Sync, at which point
/// it sends ReadyForQuery and resumes normal processing.
///
/// Our handler supports pipeline mode by:
/// 1. Each Parse/Bind/Execute is handled independently via the pgwire traits
/// 2. Errors from one Execute do not corrupt state for subsequent pipeline segments
/// 3. The executor uses per-session state that is safe across pipeline segments
/// 4. Notification delivery happens at each Sync boundary (via flush_pending_notifications)
///
/// No additional code is needed to enable pipeline mode — the pgwire framework
/// and our stateless handler design already provide correct behavior.

// ============================================================================
// COPY Handler
// ============================================================================

#[async_trait]
impl CopyHandler for NucleusHandler {
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Cap the accumulated COPY buffer so a client streaming CopyData forever
        // can't drive unbounded memory growth (DoS).
        const MAX_COPY_BUFFER: usize = 512 * 1024 * 1024; // 512 MB
        let peer_addr = client.socket_addr();
        if let Some(state) = self.copy_state.lock().get_mut(&peer_addr) {
            if state.data.len().saturating_add(copy_data.data.len()) > MAX_COPY_BUFFER {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "54000".to_owned(), // program_limit_exceeded
                    format!("COPY data exceeds the {MAX_COPY_BUFFER}-byte buffer limit"),
                ))));
            }
            state.data.extend_from_slice(&copy_data.data);
        }
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let peer_addr = client.socket_addr();
        let state = self.copy_state.lock().remove(&peer_addr);
        let Some(state) = state else {
            return Ok(());
        };

        let rows = if state.is_binary {
            // Resolve the target column types so each binary field can be
            // decoded into the text-literal form the INSERT path expects.
            let all = self.executor.table_column_types(&state.table).ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42P01".to_owned(),
                    format!("relation \"{}\" does not exist", state.table),
                )))
            })?;
            let types: Vec<DataType> = match &state.columns {
                Some(cols) => cols
                    .iter()
                    .map(|c| {
                        all.iter()
                            .find(|(n, _)| n.eq_ignore_ascii_case(c))
                            .map(|(_, t)| t.clone())
                            .ok_or_else(|| {
                                PgWireError::UserError(Box::new(ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42703".to_owned(),
                                    format!("column \"{c}\" does not exist"),
                                )))
                            })
                    })
                    .collect::<PgWireResult<_>>()?,
                None => all.into_iter().map(|(_, t)| t).collect(),
            };
            parse_copy_binary_rows(&state.data, &types).map_err(|msg| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22P04".to_owned(), // bad_copy_file_format
                    msg,
                )))
            })?
        } else {
            parse_copy_rows(&state.data, state.delimiter, state.is_csv, state.has_header)
        };
        let row_count = rows.len();

        // Insert in batches of 500 rows.
        const BATCH: usize = 500;
        for chunk in rows.chunks(BATCH) {
            if chunk.is_empty() {
                continue;
            }
            let col_clause = match &state.columns {
                Some(cols) => format!(
                    " ({})",
                    cols.iter()
                        .map(|c| format!("\"{c}\""))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                None => String::new(),
            };
            let mut sql = format!("INSERT INTO {}{} VALUES ", state.table, col_clause);
            let mut first_row = true;
            for row_fields in chunk {
                if !first_row {
                    sql.push_str(", ");
                }
                first_row = false;
                sql.push('(');
                for (i, val) in row_fields.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    match val {
                        None => sql.push_str("NULL"),
                        Some(s) => {
                            sql.push('\'');
                            sql.push_str(&sanitize_sql_text_literal(s));
                            sql.push('\'');
                        }
                    }
                }
                sql.push(')');
            }
            self.executor
                .execute_with_session(state.session_id, &sql)
                .await
                .map_err(exec_error_to_pgwire)?;
        }

        client
            .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                format!("COPY {row_count}"),
            )))
            .await?;

        Ok(())
    }
}

impl NucleusHandler {
    /// Cheap approximate byte count of a result for the bytes_sent metric.
    fn estimate_result_bytes(result: &ExecResult) -> u64 {
        match result {
            ExecResult::Select { columns, rows } => {
                // ~32 bytes per column header + ~64 bytes per cell on average
                (columns.len() as u64 * 32) + (rows.len() as u64 * columns.len().max(1) as u64 * 64)
            }
            ExecResult::Command { tag, .. } => tag.len() as u64 + 16,
            ExecResult::CopyOut { data, .. } => data.len() as u64,
            ExecResult::CopyOutBinary { data, .. } => data.len() as u64,
            // Row count unknown until drained; estimate from the header only.
            ExecResult::SelectStream { columns, .. } => columns.len() as u64 * 32,
            ExecResult::CopyOutStream { columns, .. } => columns.len() as u64 * 32,
        }
    }

    /// Get the executor reference.
    pub fn executor(&self) -> &Arc<Executor> {
        &self.executor
    }

    /// Clean up the session for a disconnected client.
    /// Called from main.rs after `process_socket` returns.
    pub fn cleanup_session(&self, peer_addr: &str) {
        if let Some(session_id) = self.session_registry.write().remove(peer_addr) {
            self.executor.drop_session(session_id);
        }
        self.sasl_registry.write().remove(peer_addr);
        // Clean up any dangling COPY state from abrupt disconnects.
        // Parse the string back to SocketAddr to look up in copy_state.
        if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
            self.copy_state.lock().remove(&addr);
        }
        // Clean up notification state (channels are GC'd lazily).
        if let Some(state) = self.notify_state.lock().remove(peer_addr) {
            for ch in &state.channels {
                self.notification_registry.remove_channel_if_empty(ch);
            }
        }
        if let Some(pid) = self.connection_pids.write().remove(peer_addr)
            && let Some((_, session_id)) = self.cancel_keys.write().remove(&pid)
        {
            self.cancel_notifies.write().remove(&session_id);
        }
        // Clean up large object descriptors.
        self.lo_state.lock().remove(peer_addr);
    }

    // ====================================================================
    // LISTEN/NOTIFY helpers
    // ====================================================================

    /// Get (or allocate) the process ID assigned to this connection.
    fn connection_pid(&self, peer_addr: &str) -> i32 {
        if let Some(&pid) = self.connection_pids.read().get(peer_addr) {
            return pid;
        }
        let pid = self.notification_registry.allocate_pid();
        self.connection_pids
            .write()
            .insert(peer_addr.to_string(), pid);
        pid
    }

    /// Register a LISTEN on `channel` for the connection identified by `peer_addr`.
    fn handle_listen(&self, peer_addr: &str, channel: &str) {
        let rx = self.notification_registry.listen(channel);
        let mut map = self.notify_state.lock();
        let state = map.entry(peer_addr.to_string()).or_insert_with(|| {
            // First LISTEN for this connection — create the per-connection state.
            // We use a single broadcast channel per-connection to aggregate all
            // channel notifications. But since broadcast::Receiver cannot be
            // merged, we store one receiver per channel and drain them all in
            // flush_pending_notifications.
            ConnectionNotifyState {
                channels: HashSet::new(),
                rx,
            }
        });
        if !state.channels.contains(channel) {
            state.channels.insert(channel.to_string());
            // Replace the receiver with one for the new channel. In practice
            // we store the latest one here — the flush loop drains from the
            // registry directly using try_recv for each channel.
            state.rx = self.notification_registry.listen(channel);
        }
    }

    /// Unregister a LISTEN on `channel` (or all channels with `*`).
    fn handle_unlisten(&self, peer_addr: &str, channel: &str) {
        let mut map = self.notify_state.lock();
        if let Some(state) = map.get_mut(peer_addr) {
            if channel == "*" {
                for ch in state.channels.drain() {
                    self.notification_registry.unlisten(&ch);
                    self.notification_registry.remove_channel_if_empty(&ch);
                }
            } else {
                state.channels.remove(channel);
                self.notification_registry.unlisten(channel);
                self.notification_registry.remove_channel_if_empty(channel);
            }
        }
    }

    /// Send a NOTIFY on `channel` with `payload`. Delivers to all connections
    /// that have called LISTEN on that channel.
    fn handle_notify(&self, peer_addr: &str, channel: &str, payload: &str) -> usize {
        let pid = self.connection_pid(peer_addr);
        self.notification_registry.notify(pid, channel, payload)
    }

    /// Flush pending notifications for a connection by sending
    /// NotificationResponse messages. Called before ReadyForQuery in both
    /// simple and extended query paths.
    ///
    /// In PostgreSQL, notifications are delivered between command responses
    /// (just before ReadyForQuery). We replicate that behavior here.
    async fn flush_pending_notifications<C>(&self, client: &mut C) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let peer_addr = client.socket_addr().to_string();
        let channels: Vec<String> = {
            let map = self.notify_state.lock();
            match map.get(&peer_addr) {
                Some(state) => state.channels.iter().cloned().collect(),
                None => return Ok(()),
            }
        };

        if channels.is_empty() {
            return Ok(());
        }

        // For each channel this connection listens on, drain pending notifications.
        // We re-subscribe briefly to collect any pending messages.
        for channel in &channels {
            // Get a fresh receiver and try_recv in a loop.
            let mut rx = self.notification_registry.listen(channel);
            loop {
                match rx.try_recv() {
                    Ok(notif) => {
                        let msg = NotificationResponse::new(
                            notif.pid,
                            notif.channel.clone(),
                            notif.payload.clone(),
                        );
                        client
                            .send(PgWireBackendMessage::NotificationResponse(msg))
                            .await?;
                    }
                    Err(broadcast::error::TryRecvError::Empty)
                    | Err(broadcast::error::TryRecvError::Closed) => break,
                    Err(broadcast::error::TryRecvError::Lagged(_)) => {
                        // Missed some messages due to buffer overflow — skip.
                        continue;
                    }
                }
            }
        }

        Ok(())
    }

    // ====================================================================
    // Large Objects API helpers
    // ====================================================================

    /// Try to intercept a large object function call from a SQL query.
    /// Returns `Some(ExecResult)` if the query was handled, `None` otherwise.
    fn try_handle_large_object(&self, peer_addr: &str, sql: &str) -> Option<ExecResult> {
        let trimmed = sql.trim();
        // Fast rejection: must start with "SELECT lo_" (case-insensitive).
        if trimmed.len() < 12 {
            return None;
        }
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("SELECT LO_") {
            return None;
        }

        // Parse function call: SELECT lo_xxx(args...)
        let after_select = trimmed[7..].trim(); // after "SELECT "
        if let Some(paren_start) = after_select.find('(') {
            let func_name = after_select[..paren_start].trim().to_lowercase();
            let args_str = after_select[paren_start + 1..]
                .trim_end_matches(|c: char| c == ')' || c == ';' || c.is_whitespace());
            let args: Vec<&str> = if args_str.is_empty() {
                vec![]
            } else {
                args_str
                    .split(',')
                    .map(|a| a.trim().trim_matches('\''))
                    .collect()
            };

            match func_name.as_str() {
                "lo_creat" | "lo_create" => {
                    return Some(self.lo_creat(peer_addr));
                }
                "lo_open" => {
                    if args.len() >= 2
                        && let (Ok(oid), Ok(mode)) =
                            (args[0].parse::<u32>(), args[1].parse::<i32>())
                    {
                        return Some(self.lo_open(peer_addr, oid, mode));
                    }
                }
                "lo_close" => {
                    if let Some(fd) = args.first().and_then(|a| a.parse::<i32>().ok()) {
                        return Some(self.lo_close(peer_addr, fd));
                    }
                }
                "lo_read" => {
                    if args.len() >= 2
                        && let (Ok(fd), Ok(len)) =
                            (args[0].parse::<i32>(), args[1].parse::<usize>())
                    {
                        return Some(self.lo_read(peer_addr, fd, len));
                    }
                }
                "lo_write" => {
                    if args.len() >= 2
                        && let Ok(fd) = args[0].parse::<i32>()
                    {
                        let data = args[1];
                        return Some(self.lo_write(peer_addr, fd, data.as_bytes()));
                    }
                }
                "lo_unlink" => {
                    if let Some(oid) = args.first().and_then(|a| a.parse::<u32>().ok()) {
                        return Some(self.lo_unlink(oid));
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// lo_creat — create a new large object, return its OID.
    fn lo_creat(&self, _peer_addr: &str) -> ExecResult {
        let oid = NEXT_LO_OID.fetch_add(1, Ordering::Relaxed);
        let key = lo_blob_key(oid);
        // Create an empty blob in the store.
        self.executor.blob_store_put(&key, b"", None);
        ExecResult::Select {
            columns: vec![("lo_creat".to_string(), DataType::Int32)],
            rows: vec![vec![Value::Int32(oid as i32)]],
        }
    }

    /// lo_open — open an existing large object, return a file descriptor.
    fn lo_open(&self, peer_addr: &str, oid: u32, mode: i32) -> ExecResult {
        let key = lo_blob_key(oid);
        // Verify the object exists.
        if !self.executor.blob_store_exists(&key) {
            return ExecResult::Select {
                columns: vec![("lo_open".to_string(), DataType::Int32)],
                rows: vec![vec![Value::Int32(-1)]],
            };
        }
        let mut map = self.lo_state.lock();
        let state = map
            .entry(peer_addr.to_string())
            .or_insert_with(LargeObjectState::new);
        let fd = state.allocate_fd();
        state.descriptors.insert(
            fd,
            LargeObjectDescriptor {
                key,
                oid,
                offset: 0,
                mode,
            },
        );
        ExecResult::Select {
            columns: vec![("lo_open".to_string(), DataType::Int32)],
            rows: vec![vec![Value::Int32(fd)]],
        }
    }

    /// lo_close — close a large object descriptor.
    fn lo_close(&self, peer_addr: &str, fd: i32) -> ExecResult {
        let mut map = self.lo_state.lock();
        let closed = if let Some(state) = map.get_mut(peer_addr) {
            state.descriptors.remove(&fd).is_some()
        } else {
            false
        };
        ExecResult::Select {
            columns: vec![("lo_close".to_string(), DataType::Int32)],
            rows: vec![vec![Value::Int32(if closed { 0 } else { -1 })]],
        }
    }

    /// lo_read — read `len` bytes from the current offset of the descriptor.
    fn lo_read(&self, peer_addr: &str, fd: i32, len: usize) -> ExecResult {
        let mut map = self.lo_state.lock();
        let state = match map.get_mut(peer_addr) {
            Some(s) => s,
            None => {
                return ExecResult::Select {
                    columns: vec![("lo_read".to_string(), DataType::Bytea)],
                    rows: vec![vec![Value::Null]],
                };
            }
        };
        let desc = match state.descriptors.get_mut(&fd) {
            Some(d) => d,
            None => {
                return ExecResult::Select {
                    columns: vec![("lo_read".to_string(), DataType::Bytea)],
                    rows: vec![vec![Value::Null]],
                };
            }
        };
        // Check read permission.
        if desc.mode & INV_READ == 0 {
            return ExecResult::Select {
                columns: vec![("lo_read".to_string(), DataType::Bytea)],
                rows: vec![vec![Value::Null]],
            };
        }
        let data = self
            .executor
            .blob_store_get_range(&desc.key, desc.offset, len as u64)
            .unwrap_or_default();
        desc.offset += data.len() as u64;
        ExecResult::Select {
            columns: vec![("lo_read".to_string(), DataType::Bytea)],
            rows: vec![vec![Value::Bytea(data)]],
        }
    }

    /// lo_write — write bytes at the current offset of the descriptor.
    fn lo_write(&self, peer_addr: &str, fd: i32, data: &[u8]) -> ExecResult {
        let mut map = self.lo_state.lock();
        let state = match map.get_mut(peer_addr) {
            Some(s) => s,
            None => {
                return ExecResult::Select {
                    columns: vec![("lo_write".to_string(), DataType::Int32)],
                    rows: vec![vec![Value::Int32(-1)]],
                };
            }
        };
        let desc = match state.descriptors.get_mut(&fd) {
            Some(d) => d,
            None => {
                return ExecResult::Select {
                    columns: vec![("lo_write".to_string(), DataType::Int32)],
                    rows: vec![vec![Value::Int32(-1)]],
                };
            }
        };
        // Check write permission.
        if desc.mode & INV_WRITE == 0 {
            return ExecResult::Select {
                columns: vec![("lo_write".to_string(), DataType::Int32)],
                rows: vec![vec![Value::Int32(-1)]],
            };
        }
        // Read existing data, splice in the write, put back.
        let mut existing = self.executor.blob_store_get(&desc.key).unwrap_or_default();
        let offset = desc.offset as usize;
        if offset > existing.len() {
            existing.resize(offset, 0);
        }
        let end = offset + data.len();
        if end > existing.len() {
            existing.resize(end, 0);
        }
        existing[offset..end].copy_from_slice(data);
        self.executor.blob_store_put(&desc.key, &existing, None);
        let written = data.len() as i32;
        desc.offset += data.len() as u64;
        ExecResult::Select {
            columns: vec![("lo_write".to_string(), DataType::Int32)],
            rows: vec![vec![Value::Int32(written)]],
        }
    }

    /// lo_unlink — delete a large object by OID.
    fn lo_unlink(&self, oid: u32) -> ExecResult {
        let key = lo_blob_key(oid);
        let deleted = self.executor.blob_store_delete(&key);
        ExecResult::Select {
            columns: vec![("lo_unlink".to_string(), DataType::Int32)],
            rows: vec![vec![Value::Int32(if deleted { 0 } else { -1 })]],
        }
    }

    /// Compress a payload for wire transmission.
    ///
    /// Returns `(data, was_compressed)`. Small payloads below the threshold
    /// are returned unchanged.
    pub fn compress_payload(&self, data: &[u8]) -> (Vec<u8>, bool) {
        self.compressor.compress_if_beneficial(data)
    }

    /// Decompress a payload received over the wire.
    ///
    /// If `is_compressed` is false, the data is returned as-is.
    pub fn decompress_payload(
        &self,
        data: &[u8],
        is_compressed: bool,
    ) -> Result<Vec<u8>, compression::CompressionError> {
        self.compressor.decompress_if_needed(data, is_compressed)
    }

    /// Try to determine the result columns for a SELECT query.
    ///
    /// This executes a `LIMIT 0` version of the query to retrieve schema
    /// information without actually fetching data. Falls back to an empty
    /// column list on any error.
    ///
    /// Statements that invoke side-effecting scalar functions are described
    /// STATICALLY instead: `LIMIT 0` does not stop projection evaluation, so
    /// probe-executing `SELECT KV_SETNX(...)` here fired the write at
    /// Describe time and again at Execute — the client's Execute then saw
    /// the second evaluation (KV_SETNX false with the key actually set).
    async fn describe_select_columns(
        &self,
        sql: &str,
        formats: Option<&Format>,
    ) -> Result<Vec<FieldInfo>, PgWireError> {
        // Try executing the query directly with LIMIT 0 appended, or if that
        // fails, run the original query. This avoids the subquery wrapping
        // that can trigger nesting depth errors for function calls like VERSION().
        let trimmed = sql.trim().trim_end_matches(';').trim();

        if let Some(fields) = describe_static_fields(trimmed, formats) {
            return Ok(fields);
        }

        // First try: add LIMIT 0 to avoid returning data
        let probe_sql = format!("{trimmed} LIMIT 0");
        let result = match self.execute_sql(&probe_sql).await {
            Ok(r) => r,
            Err(_) => {
                // LIMIT 0 might not work for all queries — try the original
                match self.execute_sql(trimmed).await {
                    Ok(r) => r,
                    Err(_) => return Ok(Vec::new()),
                }
            }
        };

        for r in result {
            if let ExecResult::Select { columns, .. } = r {
                return Ok(columns
                    .iter()
                    .enumerate()
                    .map(|(i, (name, dt))| {
                        FieldInfo::new(
                            name.clone(),
                            None,
                            None,
                            data_type_to_pg(dt),
                            formats.map_or(FieldFormat::Text, |f| requested_format(f, i)),
                        )
                    })
                    .collect());
            }
        }
        Ok(Vec::new())
    }
}

// ============================================================================
// Server Handlers
// ============================================================================

/// Server factory that hands out handler references to pgwire.
pub struct NucleusServer {
    handler: Arc<NucleusHandler>,
}

impl NucleusServer {
    pub fn new(handler: Arc<NucleusHandler>) -> Self {
        Self { handler }
    }
}

impl PgWireServerHandlers for NucleusServer {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        self.handler.clone()
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        self.handler.clone()
    }
}

#[async_trait]
impl CancelHandler for NucleusHandler {
    /// Handle a wire CancelRequest (arrives on its own short-lived
    /// connection). The (pid, secret) pair must match what BackendKeyData
    /// advertised; on mismatch the request is silently ignored, per protocol.
    async fn on_cancel_request(&self, request: CancelRequest) {
        // Protocol 3.0 carries the secret as i32, 3.2 as bytes — a client may
        // echo the key in either form, so compare canonical byte values.
        fn secret_matches(a: &SecretKey, b: &SecretKey) -> bool {
            let as_bytes = |k: &SecretKey| -> Vec<u8> {
                match k {
                    SecretKey::I32(v) => v.to_be_bytes().to_vec(),
                    SecretKey::Bytes(b) => b.to_vec(),
                }
            };
            constant_time_eq(&as_bytes(a), &as_bytes(b))
        }
        let session = {
            let keys = self.cancel_keys.read();
            match keys.get(&request.pid) {
                Some((secret, session_id)) if secret_matches(secret, &request.secret_key) => {
                    Some(*session_id)
                }
                _ => None,
            }
        };
        match session {
            Some(session_id) => {
                tracing::info!(pid = request.pid, session_id, "query cancel request accepted");
                // Cooperative flag: long compute loops poll it (rayon filters,
                // cartesian products) — this is what interrupts CPU-bound work.
                self.executor.request_session_cancel(session_id);
                // Notify: wakes the wire-level race for executions parked at
                // an await point.
                if let Some(notify) = self.cancel_notifies.read().get(&session_id) {
                    notify.notify_one();
                }
            }
            None => {
                tracing::debug!(pid = request.pid, "query cancel request ignored (key mismatch)");
            }
        }
    }
}

/// Parse a statement_timeout setting into milliseconds, following
/// PostgreSQL's convention: bare integers are milliseconds; "s"/"ms"/"min"
/// suffixes are honoured. Returns None for unparseable values.
fn parse_timeout_ms(v: &str) -> Option<u64> {
    let v = v.trim().trim_matches('\'');
    if let Ok(n) = v.parse::<u64>() {
        return Some(n);
    }
    let (num, unit) = v.split_at(v.find(|c: char| c.is_ascii_alphabetic())?);
    let n = num.trim().parse::<u64>().ok()?;
    match unit.trim() {
        "ms" => Some(n),
        "s" => Some(n * 1000),
        "min" => Some(n * 60_000),
        "h" => Some(n * 3_600_000),
        _ => None,
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn constant_time_eq(lhs: &[u8], rhs: &[u8]) -> bool {
    let max_len = lhs.len().max(rhs.len());
    let mut diff = lhs.len() ^ rhs.len();
    for i in 0..max_len {
        let l = lhs.get(i).copied().unwrap_or(0);
        let r = rhs.get(i).copied().unwrap_or(0);
        diff |= (l ^ r) as usize;
    }
    diff == 0
}

/// Count the number of `$N` parameter placeholders in a SQL string.
///
/// Returns the highest `N` found (e.g., `$1, $3` returns 3).
fn count_placeholders(sql: &str) -> usize {
    let mut max_idx = 0usize;
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while i < len {
        if in_line_comment {
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            if i + 1 < len && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                in_block_comment = false;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }
        if in_single {
            if bytes[i] == b'\'' {
                if i + 1 < len && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_single = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        if in_double {
            if bytes[i] == b'"' {
                if i + 1 < len && bytes[i + 1] == b'"' {
                    i += 2;
                } else {
                    in_double = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            in_line_comment = true;
            i += 2;
            continue;
        }
        if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            in_block_comment = true;
            i += 2;
            continue;
        }
        if bytes[i] == b'\'' {
            in_single = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'"' {
            in_double = true;
            i += 1;
            continue;
        }

        if bytes[i] == b'$' {
            i += 1;
            let mut num = 0usize;
            let mut found_digit = false;
            while i < len && bytes[i].is_ascii_digit() {
                num = num * 10 + (bytes[i] - b'0') as usize;
                found_digit = true;
                i += 1;
            }
            if found_digit && num > max_idx {
                max_idx = num;
            }
            continue;
        }

        i += 1;
    }

    max_idx
}

/// Result of decoding a single bound parameter from a Portal.
#[derive(Debug)]
enum DecodedParam {
    Null,
    Numeric(String),
    Bool(String),
    Text(String),
}

/// Decode a portal parameter into a `DecodedParam` regardless of whether the
/// client sent it in text or binary format.  Returns `None` only when the
/// portal index is out of bounds.
/// Decode a BINARY-format parameter for the typed OIDs whose wire encoding is
/// NOT a fixed-width integer: temporal, uuid, bytea, numeric, interval. Returns
/// the text-literal form the SQL substitution path expects (the same form a
/// text-mode driver would send), so both formats flow through one path.
///
/// Pure so unit tests can hit it with raw byte patterns. Returns `None` when
/// the OID isn't one of these types (caller falls through to its other arms)
/// and a loud `Some(Err)`-style corrupt-length rejection is expressed by the
/// caller mapping `None` from a matched-but-malformed payload — here malformed
/// lengths return `None` too, which the caller treats as NULL (matching the
/// pre-existing convention for undecodable params).
fn decode_binary_param_typed(oid: u32, bytes: &[u8]) -> Option<DecodedParam> {
    match oid {
        // timestamp / timestamptz: i64 BE microseconds since 2000-01-01.
        // Value::Timestamp's Display renders "YYYY-MM-DD HH:MM:SS[.ffffff]",
        // which parse_timestamp accepts for both ts and tstz columns.
        1114 | 1184 => {
            if bytes.len() != 8 {
                return None;
            }
            let us = i64::from_be_bytes(bytes.try_into().ok()?);
            Some(DecodedParam::Text(Value::Timestamp(us).to_string()))
        }
        // date: i32 BE days since 2000-01-01 → "YYYY-MM-DD".
        1082 => {
            if bytes.len() != 4 {
                return None;
            }
            let days = i32::from_be_bytes(bytes.try_into().ok()?);
            Some(DecodedParam::Text(Value::Date(days).to_string()))
        }
        // uuid: 16 raw bytes → canonical hyphenated lowercase hex.
        2950 => {
            if bytes.len() != 16 {
                return None;
            }
            let h: Vec<String> = bytes.iter().map(|b| format!("{b:02x}")).collect();
            let s = format!(
                "{}{}{}{}-{}{}-{}{}-{}{}-{}{}{}{}{}{}",
                h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7], h[8], h[9], h[10], h[11], h[12],
                h[13], h[14], h[15]
            );
            Some(DecodedParam::Text(s))
        }
        // bytea: raw bytes → Postgres hex text form. The '\' is literal under
        // the standard-conforming dialect; Value's Text→Bytea cast hex-decodes
        // the "\x" prefix, so this round-trips exactly.
        17 => {
            let mut s = String::with_capacity(2 + bytes.len() * 2);
            s.push_str("\\x");
            for b in bytes {
                s.push_str(&format!("{b:02x}"));
            }
            Some(DecodedParam::Text(s))
        }
        // numeric: NBASE-10000 (ndigits, weight, sign, dscale, digit words).
        // Decoded exactly to a decimal string — no float round-trip.
        1700 => decode_binary_numeric(bytes).map(DecodedParam::Numeric),
        // interval: i64 μs, i32 days, i32 months → unit literal the interval
        // parser accepts.
        1186 => {
            if bytes.len() != 16 {
                return None;
            }
            let us = i64::from_be_bytes(bytes[0..8].try_into().ok()?);
            let days = i32::from_be_bytes(bytes[8..12].try_into().ok()?);
            let months = i32::from_be_bytes(bytes[12..16].try_into().ok()?);
            let secs = us / 1_000_000;
            let frac = (us % 1_000_000).unsigned_abs();
            let mut s = format!("{months} months {days} days {secs}");
            if frac != 0 {
                s.push_str(&format!(".{frac:06}"));
            }
            s.push_str(" seconds");
            Some(DecodedParam::Text(s))
        }
        // Arrays of the common element types (text[]/varchar[], int2/4/8[],
        // bool[], uuid[], float4/8[]) — decoded to the Postgres array-literal
        // text form ('{a,b}') that the executor's ANY/ALL and array casts
        // accept. Layout: i32 ndim, i32 dataoffset, u32 elemtype, then per
        // dim (i32 len, i32 lower bound), then per element i32 len + payload.
        1009 | 1015 | 1005 | 1007 | 1016 | 1000 | 2951 | 1021 | 1022 => {
            decode_binary_array(bytes).map(DecodedParam::Text)
        }
        _ => None,
    }
}

/// Decode a binary one-dimensional array parameter into `{...}` literal text.
/// Elements are rendered by element OID; embedded quotes/backslashes in text
/// elements are escaped per the array-literal grammar. NULL elements render
/// as unquoted NULL. Multi-dimensional arrays are refused (None → fail-loud).
fn decode_binary_array(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 12 {
        return None;
    }
    let ndim = i32::from_be_bytes(bytes[0..4].try_into().ok()?);
    let elem_oid = u32::from_be_bytes(bytes[8..12].try_into().ok()?);
    if ndim == 0 {
        return Some("{}".into());
    }
    if ndim != 1 || bytes.len() < 20 {
        return None;
    }
    let count = i32::from_be_bytes(bytes[12..16].try_into().ok()?);
    let mut off = 20;
    let mut parts: Vec<String> = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count {
        if bytes.len() < off + 4 {
            return None;
        }
        let len = i32::from_be_bytes(bytes[off..off + 4].try_into().ok()?);
        off += 4;
        if len < 0 {
            parts.push("NULL".into());
            continue;
        }
        let len = len as usize;
        if bytes.len() < off + len {
            return None;
        }
        let payload = &bytes[off..off + len];
        off += len;
        let rendered = match elem_oid {
            16 => (payload == [1u8]).then(|| "t".to_string()).or(Some("f".to_string()))?,
            21 => i16::from_be_bytes(payload.try_into().ok()?).to_string(),
            23 => i32::from_be_bytes(payload.try_into().ok()?).to_string(),
            20 => i64::from_be_bytes(payload.try_into().ok()?).to_string(),
            700 => f32::from_be_bytes(payload.try_into().ok()?).to_string(),
            701 => f64::from_be_bytes(payload.try_into().ok()?).to_string(),
            2950 => {
                if payload.len() != 16 {
                    return None;
                }
                let h: Vec<String> = payload.iter().map(|b| format!("{b:02x}")).collect();
                format!(
                    "{}{}{}{}-{}{}-{}{}-{}{}-{}{}{}{}{}{}",
                    h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7], h[8], h[9], h[10], h[11],
                    h[12], h[13], h[14], h[15]
                )
            }
            // text / varchar
            25 | 1043 => {
                let s = std::str::from_utf8(payload).ok()?;
                let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
                format!("\"{escaped}\"")
            }
            _ => return None,
        };
        parts.push(rendered);
    }
    Some(format!("{{{}}}", parts.join(",")))
}

/// Decode PostgreSQL's binary NUMERIC wire format into an exact decimal
/// string. Layout: u16 ndigits, i16 weight (in NBASE-10000 words), u16 sign
/// (0x0000 +, 0x4000 -, 0xC000 NaN), u16 dscale, then ndigits u16 words.
fn decode_binary_numeric(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 8 {
        return None;
    }
    let ndigits = u16::from_be_bytes(bytes[0..2].try_into().ok()?) as usize;
    let weight = i16::from_be_bytes(bytes[2..4].try_into().ok()?) as i32;
    let sign = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
    let dscale = u16::from_be_bytes(bytes[6..8].try_into().ok()?) as usize;
    if bytes.len() != 8 + ndigits * 2 {
        return None;
    }
    if sign == 0xC000 {
        return None; // NaN has no SQL literal Nucleus accepts; treat as undecodable
    }
    let digits: Vec<u16> = (0..ndigits)
        .map(|i| u16::from_be_bytes([bytes[8 + i * 2], bytes[9 + i * 2]]))
        .collect();

    // Integer part: words with index <= weight (each word = 4 decimal digits).
    let mut int_part = String::new();
    for w in 0..=weight.max(-1) {
        let d = digits.get(w as usize).copied().unwrap_or(0);
        if int_part.is_empty() {
            int_part.push_str(&d.to_string());
        } else {
            int_part.push_str(&format!("{d:04}"));
        }
    }
    if int_part.is_empty() {
        int_part.push('0');
    }
    // Fraction part: words after the weight position, 4 digits each.
    let mut frac_part = String::new();
    let mut idx = (weight + 1).max(0) as usize;
    // Leading zero-words for numbers like 0.0001 (weight < -1).
    let mut lead = -1 - weight;
    while lead > 0 {
        frac_part.push_str("0000");
        lead -= 1;
    }
    while idx < ndigits {
        frac_part.push_str(&format!("{:04}", digits[idx]));
        idx += 1;
    }
    // Scale the fraction to dscale exactly (pad or trim trailing digits).
    if frac_part.len() < dscale {
        frac_part.push_str(&"0".repeat(dscale - frac_part.len()));
    } else {
        frac_part.truncate(dscale);
    }
    let sign_str = if sign == 0x4000 { "-" } else { "" };
    Some(if frac_part.is_empty() {
        format!("{sign_str}{int_part}")
    } else {
        format!("{sign_str}{int_part}.{frac_part}")
    })
}

fn decode_pg_param(
    portal: &Portal<ParsedStatement>,
    idx: usize,
    type_hint: &Type,
) -> Option<DecodedParam> {
    let raw = portal.parameters.get(idx)?;
    let Some(bytes) = raw.as_ref() else {
        return Some(DecodedParam::Null);
    };
    let is_binary = portal.parameter_format.is_binary(idx);

    // Typed non-integer binary encodings (temporal/uuid/bytea/numeric/interval)
    // — previously these fell through to the fixed-width-integer catch-all and
    // were silently reinterpreted as integers (data corruption for binary-mode
    // drivers: tokio-postgres, pgx default, JDBC).
    if is_binary
        && let Some(decoded) = decode_binary_param_typed(type_hint.oid(), bytes)
    {
        return Some(decoded);
    }
    // Text-format bytea arrives as the '\x...' literal already; other text
    // formats for these OIDs are likewise the literal forms — the existing
    // text handling below passes them through correctly.

    match type_hint.oid() {
        23 => {
            if is_binary && bytes.len() == 4 {
                let n = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Some(DecodedParam::Numeric(n.to_string()))
            } else {
                let s = String::from_utf8_lossy(bytes);
                if let Ok(n) = s.parse::<i32>() {
                    Some(DecodedParam::Numeric(n.to_string()))
                } else {
                    Some(DecodedParam::Text(s.into_owned()))
                }
            }
        }
        20 => {
            if is_binary && bytes.len() == 8 {
                let n = i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Some(DecodedParam::Numeric(n.to_string()))
            } else {
                let s = String::from_utf8_lossy(bytes);
                if let Ok(n) = s.parse::<i64>() {
                    Some(DecodedParam::Numeric(n.to_string()))
                } else {
                    Some(DecodedParam::Text(s.into_owned()))
                }
            }
        }
        21 => {
            if is_binary && bytes.len() == 2 {
                let n = i16::from_be_bytes([bytes[0], bytes[1]]);
                Some(DecodedParam::Numeric(n.to_string()))
            } else {
                let s = String::from_utf8_lossy(bytes);
                if let Ok(n) = s.parse::<i16>() {
                    Some(DecodedParam::Numeric(n.to_string()))
                } else {
                    Some(DecodedParam::Text(s.into_owned()))
                }
            }
        }
        701 => {
            if is_binary && bytes.len() == 8 {
                let n = f64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Some(DecodedParam::Numeric(n.to_string()))
            } else {
                let s = String::from_utf8_lossy(bytes);
                if let Ok(n) = s.parse::<f64>() {
                    Some(DecodedParam::Numeric(n.to_string()))
                } else {
                    Some(DecodedParam::Text(s.into_owned()))
                }
            }
        }
        700 => {
            if is_binary && bytes.len() == 4 {
                let n = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Some(DecodedParam::Numeric(n.to_string()))
            } else {
                let s = String::from_utf8_lossy(bytes);
                if let Ok(n) = s.parse::<f32>() {
                    Some(DecodedParam::Numeric(n.to_string()))
                } else {
                    Some(DecodedParam::Text(s.into_owned()))
                }
            }
        }
        16 => {
            if is_binary {
                let b = matches!(bytes.first(), Some(&n) if n != 0);
                Some(DecodedParam::Bool(if b {
                    "true".into()
                } else {
                    "false".into()
                }))
            } else {
                let s = String::from_utf8_lossy(bytes);
                let b = matches!(
                    s.trim().to_ascii_lowercase().as_str(),
                    "t" | "true" | "1" | "y" | "yes"
                );
                Some(DecodedParam::Bool(if b {
                    "true".into()
                } else {
                    "false".into()
                }))
            }
        }
        _ => {
            // Unknown OID. A text-format value is UTF-8 and decodes losslessly.
            // A binary-format value is NOT UTF-8 — from_utf8_lossy would mangle
            // it. Undeclared binary params are overwhelmingly fixed-width
            // integers (drivers send float/date/timestamp with their OID), so
            // decode the standard int widths; fall back to text otherwise.
            if is_binary {
                match bytes.len() {
                    2 => Some(DecodedParam::Numeric(
                        (i16::from_be_bytes([bytes[0], bytes[1]]) as i64).to_string(),
                    )),
                    4 => Some(DecodedParam::Numeric(
                        (i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64)
                            .to_string(),
                    )),
                    8 => Some(DecodedParam::Numeric(
                        i64::from_be_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7],
                        ])
                        .to_string(),
                    )),
                    _ => Some(DecodedParam::Text(
                        String::from_utf8_lossy(bytes).into_owned(),
                    )),
                }
            } else {
                Some(DecodedParam::Text(
                    String::from_utf8_lossy(bytes).into_owned(),
                ))
            }
        }
    }
}

/// Walk a parsed SQL statement looking for `$N` placeholders and infer each
/// one\'s pgwire `Type` from its surrounding context.
///
/// Two shapes are handled today (they cover the long tail of pgx-driven
/// queries in Observe and other consumers):
///
///   1. `Expr::Cast { expr: $N, data_type: T }`         → infer from T
///   2. `Expr::BinaryOp { left: <typed>, op: cmp/arith, right: $N }`
///      and the symmetric form                         → infer from the other side
///
/// "Typed" means an `Identifier` / `CompoundIdentifier` we can resolve via
/// the catalog (looking at every table referenced in the statement\'s `FROM`
/// clauses), or another `Cast`.  Anything we cannot resolve stays `None`,
/// which the caller turns into `Type::TEXT` — preserving the pre-fix
/// behavior so we do not regress queries we could not infer before.
/// FieldInfos for the RETURNING list of an INSERT/UPDATE/DELETE statement —
/// Describe used to advertise ZERO fields for these, which crashes clients
/// that build their row decoder from the describe response (Prisma's query
/// engine panics with "index out of bounds: the len is 0").
/// Replace `$N` placeholders with NULL for schema-probe execution. Skips
/// dollar signs inside single-quoted literals and dollar-quoted strings are
/// not handled (a describe probe of such SQL degrades to the error path, the
/// same behavior as before).
fn replace_placeholders_with_null(sql: &str) -> String {
    let mut out = Vec::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut in_str = false;
    while i < bytes.len() {
        let c = bytes[i];
        if in_str {
            out.push(c);
            if c == b'\'' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' => {
                in_str = true;
                out.push(c);
                i += 1;
            }
            b'$' if i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() => {
                let mut j = i + 1;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                out.extend_from_slice(b"NULL");
                i = j;
            }
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    // Byte-level edits only touched ASCII, so this cannot fail for valid input.
    String::from_utf8(out).unwrap_or_else(|_| sql.to_string())
}

fn describe_returning_fields(
    ast: Option<&[sqlparser::ast::Statement]>,
    executor: &Arc<Executor>,
    formats: Option<&Format>,
) -> Vec<FieldInfo> {
    use sqlparser::ast::{Expr, FromTable, SelectItem, Statement, TableFactor, TableObject};
    let Some(stmt) = ast.and_then(|stmts| stmts.first()) else {
        return Vec::new();
    };
    let (table_name, returning): (String, &Vec<SelectItem>) = match stmt {
        Statement::Insert(i) => {
            let Some(r) = &i.returning else {
                return Vec::new();
            };
            let TableObject::TableName(n) = &i.table else {
                return Vec::new();
            };
            (crate::sql::object_name_key(n), r)
        }
        Statement::Update(u) => {
            let Some(r) = &u.returning else {
                return Vec::new();
            };
            let TableFactor::Table { name, .. } = &u.table.relation else {
                return Vec::new();
            };
            (crate::sql::object_name_key(name), r)
        }
        Statement::Delete(d) => {
            let Some(r) = &d.returning else {
                return Vec::new();
            };
            let (FromTable::WithFromKeyword(twjs) | FromTable::WithoutKeyword(twjs)) = &d.from;
            let Some(TableFactor::Table { name, .. }) = twjs.first().map(|t| &t.relation) else {
                return Vec::new();
            };
            (crate::sql::object_name_key(name), r)
        }
        _ => return Vec::new(),
    };
    let Some(def) = executor.catalog().get_table_cached(&table_name) else {
        return Vec::new();
    };
    let col_type = |col: &str| {
        def.columns
            .iter()
            .find(|c| c.name == col)
            .map(|c| data_type_to_pg(&c.data_type))
            .unwrap_or(Type::TEXT)
    };
    let mut out = Vec::new();
    for item in returning {
        match item {
            SelectItem::Wildcard(_) => {
                for c in &def.columns {
                    out.push((c.name.clone(), data_type_to_pg(&c.data_type)));
                }
            }
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                let alias = if let SelectItem::ExprWithAlias { alias, .. } = item {
                    Some(alias.value.clone())
                } else {
                    None
                };
                let (name, ty) = match expr {
                    Expr::Identifier(id) => (id.value.clone(), col_type(&id.value)),
                    Expr::CompoundIdentifier(parts) => {
                        let col = parts.last().map(|p| p.value.clone()).unwrap_or_default();
                        let ty = col_type(&col);
                        (col, ty)
                    }
                    other => (other.to_string(), Type::TEXT),
                };
                out.push((alias.unwrap_or(name), ty));
            }
            _ => {}
        }
    }
    out.into_iter()
        .enumerate()
        .map(|(i, (name, ty))| {
            FieldInfo::new(
                name,
                None,
                None,
                ty,
                formats.map_or(FieldFormat::Text, |f| requested_format(f, i)),
            )
        })
        .collect()
}

fn infer_param_types_from_ast(
    stmts: &[sqlparser::ast::Statement],
    executor: &Arc<Executor>,
    param_count: usize,
) -> Vec<Option<Type>> {
    use sqlparser::ast::{SetExpr, Statement};
    let mut result: Vec<Option<Type>> = vec![None; param_count];

    for stmt in stmts {
        let mut tables: Vec<Arc<crate::catalog::TableDef>> = Vec::new();
        collect_referenced_tables(stmt, executor, &mut tables);

        match stmt {
            Statement::Query(q) => {
                walk_query_for_params(q, &tables, &mut result);
            }
            Statement::Insert(insert) => {
                if let Some(source) = &insert.source
                    && let SetExpr::Values(values) = source.body.as_ref()
                {
                    for row in &values.rows {
                        for (col_pos, expr) in row.iter().enumerate() {
                            let target_type = insert
                                .columns
                                .get(col_pos)
                                .and_then(|name| column_type_in_tables(&tables, &name.value))
                                .or_else(|| {
                                    tables
                                        .first()
                                        .and_then(|t| t.columns.get(col_pos))
                                        .map(|c| c.data_type.clone())
                                })
                                .map(|dt| data_type_to_pg(&dt));
                            if let Some(t) = target_type {
                                mark_param(expr, t, &mut result);
                            }
                        }
                    }
                }
            }
            Statement::Update(update) => {
                if let Some(w) = &update.selection {
                    walk_expr_for_params(w, &tables, &mut result);
                }
                for assign in &update.assignments {
                    use sqlparser::ast::AssignmentTarget;
                    if let AssignmentTarget::ColumnName(name) = &assign.target
                        && let Some(part) = name.0.last()
                        && let Some(ident) = part.as_ident()
                        && let Some(dt) = column_type_in_tables(&tables, &ident.value)
                    {
                        mark_param(&assign.value, data_type_to_pg(&dt), &mut result);
                    }
                    walk_expr_for_params(&assign.value, &tables, &mut result);
                }
            }
            Statement::Delete(d) => {
                if let Some(w) = &d.selection {
                    walk_expr_for_params(w, &tables, &mut result);
                }
            }
            _ => {}
        }
    }

    result
}

/// Collect every TableDef referenced by `stmt` into `out`.  Best-effort —
/// missing tables are simply skipped.  Uses the synchronous catalog cache so
/// we never block on a tokio lock.
fn collect_referenced_tables(
    stmt: &sqlparser::ast::Statement,
    executor: &Arc<Executor>,
    out: &mut Vec<Arc<crate::catalog::TableDef>>,
) {
    use sqlparser::ast::{SetExpr, Statement, TableFactor};
    let catalog = executor.catalog();
    let push = |name: &str, out: &mut Vec<Arc<crate::catalog::TableDef>>| {
        if let Some(t) = catalog.get_table_cached(name) {
            out.push(t);
        }
    };

    match stmt {
        Statement::Query(q) => {
            if let SetExpr::Select(select) = q.body.as_ref() {
                for tbl in &select.from {
                    if let TableFactor::Table { name, .. } = &tbl.relation
                        && let Some(part) = name.0.last()
                        && let Some(ident) = part.as_ident()
                    {
                        push(&ident.value, out);
                    }
                    for join in &tbl.joins {
                        if let TableFactor::Table { name, .. } = &join.relation
                            && let Some(part) = name.0.last()
                            && let Some(ident) = part.as_ident()
                        {
                            push(&ident.value, out);
                        }
                    }
                }
            }
        }
        Statement::Insert(insert) => {
            if let sqlparser::ast::TableObject::TableName(name) = &insert.table
                && let Some(part) = name.0.last()
                && let Some(ident) = part.as_ident()
            {
                push(&ident.value, out);
            }
        }
        Statement::Update(update) => {
            if let TableFactor::Table { name, .. } = &update.table.relation
                && let Some(part) = name.0.last()
                && let Some(ident) = part.as_ident()
            {
                push(&ident.value, out);
            }
        }
        Statement::Delete(d) => {
            for tbl in &d.tables {
                if let Some(part) = tbl.0.last()
                    && let Some(ident) = part.as_ident()
                {
                    push(&ident.value, out);
                }
            }
            let from = match &d.from {
                sqlparser::ast::FromTable::WithFromKeyword(f)
                | sqlparser::ast::FromTable::WithoutKeyword(f) => f,
            };
            {
                for t in from {
                    if let TableFactor::Table { name, .. } = &t.relation
                        && let Some(part) = name.0.last()
                        && let Some(ident) = part.as_ident()
                    {
                        push(&ident.value, out);
                    }
                }
            }
        }
        _ => {}
    }
}

/// Find a column\'s nucleus DataType across a slice of TableDefs (case-insensitive).
fn column_type_in_tables(
    tables: &[Arc<crate::catalog::TableDef>],
    col_name: &str,
) -> Option<DataType> {
    for t in tables {
        for c in &t.columns {
            if c.name.eq_ignore_ascii_case(col_name) {
                return Some(c.data_type.clone());
            }
        }
    }
    None
}

/// Recursively walk an expression looking for `$N` placeholders and infer
/// each one\'s pgwire `Type` from the surrounding AST.
/// Recursive Query walker: projection/WHERE/HAVING exprs, LIMIT/OFFSET, and —
/// crucially for ORM introspection SQL — derived tables in FROM and join ON
/// constraints, which the old top-level-only walk never reached (Prisma binds
/// `= ANY($1)` inside a FROM (SELECT ...) subquery).
fn walk_query_for_params(
    q: &sqlparser::ast::Query,
    tables: &[Arc<crate::catalog::TableDef>],
    result: &mut [Option<Type>],
) {
    use sqlparser::ast::{JoinConstraint, JoinOperator, SetExpr, TableFactor};
    fn walk_factor(
        f: &TableFactor,
        tables: &[Arc<crate::catalog::TableDef>],
        result: &mut [Option<Type>],
    ) {
        if let TableFactor::Derived { subquery, .. } = f {
            walk_query_for_params(subquery, tables, result);
        }
    }
    if let SetExpr::Select(select) = q.body.as_ref() {
        for item in &select.projection {
            if let sqlparser::ast::SelectItem::UnnamedExpr(e)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } = item
            {
                walk_expr_for_params(e, tables, result);
            }
        }
        if let Some(ref w) = select.selection {
            walk_expr_for_params(w, tables, result);
        }
        if let Some(ref h) = select.having {
            walk_expr_for_params(h, tables, result);
        }
        for twj in &select.from {
            walk_factor(&twj.relation, tables, result);
            for j in &twj.joins {
                walk_factor(&j.relation, tables, result);
                let constraint = match &j.join_operator {
                    JoinOperator::Inner(c)
                    | JoinOperator::Join(c)
                    | JoinOperator::Left(c)
                    | JoinOperator::LeftOuter(c)
                    | JoinOperator::Right(c)
                    | JoinOperator::RightOuter(c)
                    | JoinOperator::FullOuter(c) => Some(c),
                    _ => None,
                };
                if let Some(JoinConstraint::On(e)) = constraint {
                    walk_expr_for_params(e, tables, result);
                }
            }
        }
    }
    if let Some(ref limit_clause) = q.limit_clause {
        use sqlparser::ast::LimitClause;
        if let LimitClause::LimitOffset { limit, offset, .. } = limit_clause {
            if let Some(e) = limit {
                mark_param(e, Type::INT8, result);
            }
            if let Some(off) = offset {
                mark_param(&off.value, Type::INT8, result);
            }
        }
    }
}

fn walk_expr_for_params(
    expr: &sqlparser::ast::Expr,
    tables: &[Arc<crate::catalog::TableDef>],
    out: &mut [Option<Type>],
) {
    use sqlparser::ast::{BinaryOperator, Expr};

    match expr {
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            if let Ok(dt) = crate::sql::convert_data_type(data_type) {
                mark_param(inner, data_type_to_pg(&dt), out);
            }
            walk_expr_for_params(inner, tables, out);
        }
        Expr::BinaryOp { left, op, right } => {
            if matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
            ) {
                if let Some(t) = expr_pg_type(left, tables) {
                    mark_param(right, t, out);
                }
                if let Some(t) = expr_pg_type(right, tables) {
                    mark_param(left, t, out);
                }
            }
            walk_expr_for_params(left, tables, out);
            walk_expr_for_params(right, tables, out);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => walk_expr_for_params(expr, tables, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            if let Some(t) = expr_pg_type(expr, tables) {
                mark_param(low, t.clone(), out);
                mark_param(high, t, out);
            }
            walk_expr_for_params(expr, tables, out);
            walk_expr_for_params(low, tables, out);
            walk_expr_for_params(high, tables, out);
        }
        Expr::InList { expr, list, .. } => {
            if let Some(t) = expr_pg_type(expr, tables) {
                for item in list {
                    mark_param(item, t.clone(), out);
                }
            }
            walk_expr_for_params(expr, tables, out);
            for item in list {
                walk_expr_for_params(item, tables, out);
            }
        }
        // `x = ANY($1)` / `x = ALL($1)` — the parameter is an ARRAY of x's
        // element type. Without this, ParameterDescription said TEXT and
        // array-binding drivers (Prisma's quaint) refused to serialize.
        Expr::AnyOp {
            left,
            right,
            ..
        }
        | Expr::AllOp {
            left,
            right,
            ..
        } => {
            // The right side of ANY/ALL is definitionally an array, so even
            // when the element type can't be resolved (virtual catalogs are
            // not in the TableDef list) default to text[] rather than text.
            let elem = expr_pg_type(left, tables).unwrap_or(Type::TEXT);
            mark_param(right, scalar_to_array_type(&elem), out);
            walk_expr_for_params(left, tables, out);
            walk_expr_for_params(right, tables, out);
        }
        Expr::Function(func) => {
            // Known Nucleus scalar extensions: advertise proper types for
            // their placeholder args instead of the blanket TEXT default.
            // Without this, `SELECT FTS_SEARCH($1, $2)` described both params
            // as TEXT and pgx refused to bind an int64 limit ("cannot find
            // encode plan") — teploy-observe dogfood finding #22. The list
            // mirrors the executor's scalar_fns signatures.
            let fname = func.name.to_string().to_uppercase();
            let sig: &[Type] = match fname.as_str() {
                "FTS_SEARCH" => &[Type::TEXT, Type::INT8],
                "FTS_FUZZY_SEARCH" => &[Type::TEXT, Type::INT8, Type::INT8],
                "FTS_SEARCH_FILTER" => &[Type::TEXT, Type::INT8, Type::TEXT, Type::TEXT],
                "FTS_INDEX" => &[Type::INT8, Type::TEXT],
                "FTS_INDEX_FACETED" => &[Type::INT8, Type::TEXT, Type::TEXT, Type::TEXT],
                "KV_INCR" => &[Type::TEXT, Type::INT8],
                _ => &[],
            };
            if let sqlparser::ast::FunctionArguments::List(list) = &func.args {
                for (pos, arg) in list.args.iter().enumerate() {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) = arg
                    {
                        if let Some(ty) = sig.get(pos) {
                            mark_param(e, ty.clone(), out);
                        }
                        walk_expr_for_params(e, tables, out);
                    }
                }
            }
        }
        _ => {}
    }
}

/// Return the pgwire `Type` of a non-placeholder expression, if known.
fn expr_pg_type(
    expr: &sqlparser::ast::Expr,
    tables: &[Arc<crate::catalog::TableDef>],
) -> Option<Type> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(ident) => {
            column_type_in_tables(tables, &ident.value).map(|dt| data_type_to_pg(&dt))
        }
        // table.column or schema.table.column (Prisma qualifies all columns
        // three-part) — the column is always the last segment.
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
            column_type_in_tables(tables, &parts.last().unwrap().value)
                .map(|dt| data_type_to_pg(&dt))
        }
        Expr::Cast { data_type, .. } => crate::sql::convert_data_type(data_type)
            .ok()
            .map(|dt| data_type_to_pg(&dt)),
        Expr::Nested(inner) => expr_pg_type(inner, tables),
        _ => None,
    }
}

/// If `expr` is a `$N` placeholder, record `ty` at index N-1 (unless we have
/// already recorded a more specific type).
fn mark_param(expr: &sqlparser::ast::Expr, ty: Type, out: &mut [Option<Type>]) {
    use sqlparser::ast::Expr;
    if let Expr::Value(vws) = expr
        && let sqlparser::ast::Value::Placeholder(p) = &vws.value
        && let Some(idx_str) = p.strip_prefix('$')
        && let Ok(idx) = idx_str.parse::<usize>()
        && idx >= 1
        && idx <= out.len()
        && out[idx - 1].is_none()
    {
        out[idx - 1] = Some(ty);
    }
}

/// Check if a SQL string is a SELECT query (or similar data-returning query).
fn is_select_query(sql: &str) -> bool {
    let trimmed = sql.trim().to_uppercase();
    trimmed.starts_with("SELECT")
        || trimmed.starts_with("WITH")
        || trimmed.starts_with("VALUES")
        || trimmed.starts_with("TABLE")
        || trimmed.starts_with("SHOW")
}

/// Map Nucleus DataType to Postgres wire type.
/// Array pg type whose element type is `t` — for `= ANY($n)` parameter
/// inference. Unknown element types degrade to text[].
fn scalar_to_array_type(t: &Type) -> Type {
    match *t {
        Type::BOOL => Type::BOOL_ARRAY,
        Type::INT2 => Type::INT2_ARRAY,
        Type::INT4 => Type::INT4_ARRAY,
        Type::INT8 => Type::INT8_ARRAY,
        Type::FLOAT4 => Type::FLOAT4_ARRAY,
        Type::FLOAT8 => Type::FLOAT8_ARRAY,
        Type::NUMERIC => Type::NUMERIC_ARRAY,
        Type::VARCHAR => Type::VARCHAR_ARRAY,
        Type::UUID => Type::UUID_ARRAY,
        Type::DATE => Type::DATE_ARRAY,
        Type::TIMESTAMP => Type::TIMESTAMP_ARRAY,
        Type::TIMESTAMPTZ => Type::TIMESTAMPTZ_ARRAY,
        Type::BYTEA => Type::BYTEA_ARRAY,
        _ => Type::TEXT_ARRAY,
    }
}

fn data_type_to_pg(dt: &DataType) -> Type {
    match dt {
        DataType::Bool => Type::BOOL,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::Float64 => Type::FLOAT8,
        DataType::Text => Type::VARCHAR,
        DataType::Jsonb => Type::JSONB,
        DataType::Date => Type::DATE,
        DataType::Timestamp => Type::TIMESTAMP,
        DataType::TimestampTz => Type::TIMESTAMPTZ,
        DataType::Numeric => Type::NUMERIC,
        DataType::Uuid => Type::UUID,
        DataType::Bytea => Type::BYTEA,
        DataType::Array(_) => Type::TEXT, // Arrays sent as text for now
        DataType::Vector(_) => Type::TEXT, // Vectors sent as text for now
        DataType::Interval => Type::VARCHAR, // Intervals sent as text for now
        DataType::UserDefined(_) => Type::VARCHAR, // Enum values sent as text
    }
}

/// The client-requested result format for column `idx` (from Bind).
///
/// The Postgres protocol puts result formats under CLIENT control; the old
/// server-side per-type choice (binary for ints/floats "to avoid text
/// conversion overhead") silently fed binary bytes to text-mode clients,
/// which decode them as garbage numbers. `Individual` with too few codes
/// falls back to text — never invent binary the client didn't ask for.
fn requested_format(fmt: &Format, idx: usize) -> FieldFormat {
    match fmt {
        Format::UnifiedText => FieldFormat::Text,
        Format::UnifiedBinary => FieldFormat::Binary,
        Format::Individual(codes) => codes
            .get(idx)
            .map_or(FieldFormat::Text, |c| FieldFormat::from(*c)),
    }
}

/// Encode a value to match the column's *declared* type width. Nucleus stores
/// small integers as `Int32` even in `BIGINT` columns (and a literal can be
/// `Int64` in an `INT` column), but the RowDescription advertises the declared
/// type. With binary result format the client decodes by that advertised type,
/// so a width mismatch (e.g. a 4-byte int4 payload under an int8 column) makes
/// pgx/any binary consumer fail with "error deserializing column". Coerce the
/// integer/float to the declared width before encoding.
fn encode_value_typed(
    encoder: &mut DataRowEncoder,
    value: &Value,
    target: &DataType,
    fmt: FieldFormat,
) -> PgWireResult<()> {
    match (value, target) {
        (Value::Int32(n), DataType::Int64) => return encoder.encode_field(&Some(*n as i64)),
        (Value::Int64(n), DataType::Int32) => {
            if let Ok(n32) = i32::try_from(*n) {
                return encoder.encode_field(&Some(n32));
            }
        }
        (Value::Int32(n), DataType::Float64) => return encoder.encode_field(&Some(*n as f64)),
        (Value::Int64(n), DataType::Float64) => return encoder.encode_field(&Some(*n as f64)),
        _ => {}
    }
    encode_value(encoder, value, fmt)
}

/// Encode a Nucleus Value into a pgwire DataRowEncoder field. `fmt` is the
/// column's wire format (Text/Binary) — temporal values render differently in
/// text (see below).
fn encode_value(encoder: &mut DataRowEncoder, value: &Value, fmt: FieldFormat) -> PgWireResult<()> {
    match value {
        Value::Null => encoder.encode_field(&None::<&str>),
        Value::Bool(b) => encoder.encode_field(&Some(*b)),
        Value::Int32(n) => encoder.encode_field(&Some(*n)),
        Value::Int64(n) => encoder.encode_field(&Some(*n)),
        // Non-finite floats: PostgreSQL's text wire form is
        // "Infinity"/"-Infinity"/"NaN"; f64's ToSqlText emits "inf"/"NaN".
        // Encode the PG spelling as text so clients (and psql) display it
        // correctly under the float8 RowDescription.
        Value::Float64(n) if !n.is_finite() => {
            let s = if n.is_nan() {
                "NaN"
            } else if *n < 0.0 {
                "-Infinity"
            } else {
                "Infinity"
            };
            encoder.encode_field(&Some(s))
        }
        // TEXT-format floats render via Value's Display, which spells the
        // exponent PostgreSQL-style ("1e+100", not Rust's "1e100").
        Value::Float64(_) if matches!(fmt, FieldFormat::Text) => {
            encoder.encode_field(&Some(value.to_string().as_str()))
        }
        Value::Float64(n) => encoder.encode_field(&Some(*n)),
        Value::Text(s) => encoder.encode_field(&Some(s.as_str())),
        Value::Jsonb(v) => encoder.encode_field(&Some(v.to_string().as_str())),
        // In TEXT format, temporal values render via Nucleus's Display, which
        // matches PostgreSQL's text form — most importantly it OMITS a
        // `.000000` fractional part when microseconds are zero (chrono's
        // ToSqlText always writes it). Binary format still uses the native
        // chrono impls below so binary clients decode correctly.
        Value::Timestamp(_) | Value::TimestampTz(_) | Value::Date(_)
            if matches!(fmt, FieldFormat::Text) =>
        {
            encoder.encode_field(&Some(value.to_string().as_str()))
        }
        // Temporal/decimal/bytea values encode through their native
        // postgres-types impls so BINARY-format result columns carry real
        // binary payloads (a text string under a binary RowDescription made
        // Prisma/pgx misdecode timestamps). Text-format columns still render
        // through ToSqlText, so text clients are unchanged.
        Value::Timestamp(us) => {
            let ts = pg_epoch_naive() + chrono::Duration::microseconds(*us);
            encoder.encode_field(&Some(ts))
        }
        Value::TimestampTz(us) => {
            let ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                pg_epoch_naive() + chrono::Duration::microseconds(*us),
                chrono::Utc,
            );
            encoder.encode_field(&Some(ts))
        }
        Value::Date(days) => {
            let d = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()
                + chrono::Duration::days(i64::from(*days));
            encoder.encode_field(&Some(d))
        }
        Value::Bytea(b) => encoder.encode_field(&Some(b.as_slice())),
        // BINARY-format NUMERIC must carry the NBASE-10000 wire encoding —
        // pgjdbc switches result transfer to binary once a statement is
        // server-prepared and rejects text bytes under a binary column.
        Value::Numeric(s) if matches!(fmt, FieldFormat::Binary) => {
            match rust_decimal::Decimal::from_str_exact(s) {
                Ok(d) => encoder.encode_field(&Some(d)),
                Err(_) => Err(PgWireError::ApiError(
                    format!("numeric value not binary-encodable: {s}").into(),
                )),
            }
        }
        // BINARY-format UUID is the 16 raw bytes (the &[u8] impl writes raw).
        Value::Uuid(b) if matches!(fmt, FieldFormat::Binary) => {
            encoder.encode_field(&Some(b.as_slice()))
        }
        // Text-rendered forms are exact for the remaining types; array/vector/
        // interval have no enabled native binary impl.
        Value::Numeric(_)
        | Value::Uuid(_)
        | Value::Array(_)
        | Value::Vector(_)
        | Value::Interval { .. } => encoder.encode_field(&Some(value.to_string().as_str())),
    }
}

/// 2000-01-01T00:00:00 — the PostgreSQL timestamp epoch Nucleus stores
/// microsecond offsets against.
fn pg_epoch_naive() -> chrono::NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

/// Map a Postgres wire type OID to Nucleus DataType (best effort).
#[allow(dead_code)]
fn pg_type_to_data_type(pg_type: &Type) -> DataType {
    match *pg_type {
        Type::BOOL => DataType::Bool,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT8 | Type::FLOAT4 => DataType::Float64,
        Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::NAME => DataType::Text,
        Type::JSONB | Type::JSON => DataType::Jsonb,
        Type::DATE => DataType::Date,
        Type::TIMESTAMP => DataType::Timestamp,
        Type::TIMESTAMPTZ => DataType::TimestampTz,
        Type::NUMERIC => DataType::Numeric,
        Type::UUID => DataType::Uuid,
        Type::BYTEA => DataType::Bytea,
        _ => DataType::Text, // Default to text for unknown types
    }
}

// ============================================================================
// COPY helpers
// ============================================================================

/// True when `sql` appears to invoke a side-effecting scalar function
/// (an identifier from the executor's registry immediately followed by
/// `(`). A cheap textual scan: false positives (e.g. the name inside a
/// string literal) merely route Describe to the static path below, which
/// degrades type precision but never fires effects.
/// Return type for functions the wire layer describes statically instead of
/// probe-executing: side-effecting extensions (a Describe must never run
/// them) and read-only extensions whose probe would error on unbound
/// placeholders (finding #22 tail).
fn statically_described_fn_type(name: &str) -> Option<DataType> {
    crate::executor::side_effecting_return_type(name)
        .or_else(|| crate::executor::extension_scalar_return_type(name))
}

fn mentions_side_effecting_fn(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let mut j = i;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len()
                && bytes[j] == b'('
                && statically_described_fn_type(&upper[start..i]).is_some()
            {
                return true;
            }
        } else {
            i += 1;
        }
    }
    false
}

/// Describe a SELECT that invokes side-effecting scalar functions WITHOUT
/// executing anything. Returns `None` for statements with no such calls
/// (callers fall through to the probe-execution path); for the rest,
/// derives one field per projection item — registry type for known
/// mutating functions, VARCHAR otherwise — and falls back to a single
/// VARCHAR field when the statement shape defies static analysis.
/// Execution is never a fallback here.
fn describe_static_fields(sql: &str, formats: Option<&Format>) -> Option<Vec<FieldInfo>> {
    use sqlparser::ast::{Expr, ObjectNamePart, SelectItem, SetExpr, Statement};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    if !mentions_side_effecting_fn(sql) {
        return None;
    }

    let fallback = || {
        Some(vec![FieldInfo::new(
            "result".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        )])
    };

    let Ok(stmts) = Parser::parse_sql(&PostgreSqlDialect {}, sql) else {
        return fallback();
    };
    let Some(Statement::Query(query)) = stmts.into_iter().next() else {
        return fallback();
    };
    let SetExpr::Select(select) = *query.body else {
        return fallback();
    };
    if select.projection.is_empty() {
        return fallback();
    }

    let mut fields = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        let (alias, expr) = match item {
            SelectItem::UnnamedExpr(expr) => (None, expr),
            SelectItem::ExprWithAlias { expr, alias } => (Some(alias.value.clone()), expr),
            _ => return fallback(),
        };
        let fn_name = if let Expr::Function(func) = expr {
            func.name.0.last().and_then(|part| match part {
                ObjectNamePart::Identifier(ident) => Some(ident.value.to_ascii_uppercase()),
                _ => None,
            })
        } else {
            None
        };
        let dt = fn_name.as_deref().and_then(statically_described_fn_type);
        let name = alias.unwrap_or_else(|| {
            fn_name
                .map(|n| n.to_ascii_lowercase())
                .unwrap_or_else(|| "?column?".into())
        });
        let idx = fields.len();
        let fmt = formats.map_or(FieldFormat::Text, |f| requested_format(f, idx));
        let field = match dt {
            Some(dt) => FieldInfo::new(name, None, None, data_type_to_pg(&dt), fmt),
            None => FieldInfo::new(name, None, None, Type::VARCHAR, fmt),
        };
        fields.push(field);
    }
    Some(fields)
}

/// Parse a `COPY table [(cols)] FROM STDIN [WITH (...)]` statement and return
/// a `CopyInfo` if it is a valid COPY FROM STDIN.  Returns `None` for all
/// other SQL (errors, COPY TO, …).
fn detect_copy_from_stdin(sql: &str) -> Option<CopyInfo> {
    use sqlparser::ast::{CopyOption, CopySource, CopyTarget, ObjectNamePart, Statement};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    // sqlparser requires a trailing semicolon for COPY statements.
    let sql_with_semi: std::borrow::Cow<str> = if sql.trim_end().ends_with(';') {
        sql.into()
    } else {
        format!("{};", sql.trim_end()).into()
    };
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &sql_with_semi).ok()?;
    let stmt = stmts.into_iter().next()?;

    let Statement::Copy {
        source,
        to: false,
        target: CopyTarget::Stdin,
        options,
        ..
    } = stmt
    else {
        return None;
    };
    let CopySource::Table {
        table_name,
        columns,
    } = source
    else {
        return None;
    };

    // Reconstruct the (possibly qualified) table name from parts.
    let table = table_name
        .0
        .iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(".");

    let col_names: Option<Vec<String>> = if columns.is_empty() {
        None
    } else {
        Some(columns.iter().map(|c| c.value.clone()).collect())
    };

    let mut delimiter = b'\t';
    let mut is_csv = false;
    let mut is_binary = false;
    let mut has_header = false;

    for opt in options {
        match opt {
            CopyOption::Format(f) if f.value.to_uppercase() == "CSV" => {
                is_csv = true;
                delimiter = b',';
            }
            CopyOption::Format(f) if f.value.to_uppercase() == "BINARY" => {
                is_binary = true;
            }
            CopyOption::Delimiter(d) => delimiter = d as u8,
            CopyOption::Header(h) => has_header = h,
            _ => {}
        }
    }

    Some(CopyInfo {
        table,
        columns: col_names,
        delimiter,
        is_csv,
        is_binary,
        has_header,
    })
}

/// Parse a PostgreSQL binary-COPY payload into rows of optional text-literal
/// fields (the same form the text path produces, so both feed one INSERT
/// builder). Fails loudly on a malformed stream or an undecodable type.
fn parse_copy_binary_rows(
    data: &[u8],
    types: &[DataType],
) -> Result<Vec<Vec<Option<String>>>, String> {
    const SIG: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";
    if data.len() < 19 || &data[..11] != SIG {
        return Err("invalid binary COPY signature".into());
    }
    let ext_len = u32::from_be_bytes(data[15..19].try_into().unwrap()) as usize;
    let mut pos = 19 + ext_len;
    let mut rows = Vec::new();
    loop {
        if pos + 2 > data.len() {
            return Err("unexpected end of binary COPY data".into());
        }
        let nfields = i16::from_be_bytes(data[pos..pos + 2].try_into().unwrap());
        pos += 2;
        if nfields == -1 {
            break; // trailer
        }
        let mut row = Vec::with_capacity(nfields as usize);
        for i in 0..nfields as usize {
            if pos + 4 > data.len() {
                return Err("unexpected end of binary COPY tuple".into());
            }
            let len = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            if len == -1 {
                row.push(None);
                continue;
            }
            let len = len as usize;
            if pos + len > data.len() {
                return Err("binary COPY field extends past end of data".into());
            }
            let ty = types.get(i).ok_or_else(|| {
                format!("binary COPY row has more fields than target columns ({nfields})")
            })?;
            let oid = data_type_to_pg(ty).oid();
            let text = decode_copy_binary_field(oid, &data[pos..pos + len]).ok_or_else(|| {
                format!("cannot decode binary COPY field of type {ty}")
            })?;
            row.push(Some(text));
            pos += len;
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Decode one binary COPY field into its text-literal form.
fn decode_copy_binary_field(oid: u32, b: &[u8]) -> Option<String> {
    match oid {
        16 if b.len() == 1 => Some(if b[0] != 0 { "true" } else { "false" }.into()),
        21 if b.len() == 2 => Some(i16::from_be_bytes(b.try_into().ok()?).to_string()),
        23 if b.len() == 4 => Some(i32::from_be_bytes(b.try_into().ok()?).to_string()),
        20 if b.len() == 8 => Some(i64::from_be_bytes(b.try_into().ok()?).to_string()),
        700 if b.len() == 4 => Some(float_literal(f32::from_be_bytes(b.try_into().ok()?) as f64)),
        701 if b.len() == 8 => Some(float_literal(f64::from_be_bytes(b.try_into().ok()?))),
        // Text family: the binary encoding IS the UTF-8 text.
        18 | 19 | 25 | 114 | 1042 | 1043 => Some(String::from_utf8(b.to_vec()).ok()?),
        // jsonb: version byte then JSON text.
        3802 if !b.is_empty() && b[0] == 1 => Some(String::from_utf8(b[1..].to_vec()).ok()?),
        _ => match decode_binary_param_typed(oid, b)? {
            DecodedParam::Null => None,
            DecodedParam::Numeric(s) | DecodedParam::Bool(s) | DecodedParam::Text(s) => Some(s),
        },
    }
}

/// Float text form the SQL parser accepts (PostgreSQL spellings for the
/// non-finite values).
fn float_literal(f: f64) -> String {
    if f.is_nan() {
        "NaN".into()
    } else if f.is_infinite() {
        if f < 0.0 { "-Infinity".into() } else { "Infinity".into() }
    } else {
        f.to_string()
    }
}

/// Parse accumulated COPY data bytes into rows of optional string fields.
fn parse_copy_rows(
    data: &[u8],
    delimiter: u8,
    is_csv: bool,
    has_header: bool,
) -> Vec<Vec<Option<String>>> {
    let text = match std::str::from_utf8(data) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let mut rows = Vec::new();
    let mut lines = text.lines().peekable();
    if has_header {
        lines.next();
    }
    for line in lines {
        let trimmed = line.trim_end_matches('\r');
        if trimmed.is_empty() {
            continue;
        }
        rows.push(split_copy_line(trimmed, delimiter, is_csv));
    }
    rows
}

/// Split one data line into fields, respecting the chosen format.
fn split_copy_line(line: &str, delimiter: u8, is_csv: bool) -> Vec<Option<String>> {
    let delim = delimiter as char;
    if is_csv {
        let mut result = Vec::new();
        let mut chars = line.chars().peekable();
        let mut current = String::new();
        loop {
            match chars.next() {
                None => {
                    result.push(if current.is_empty() {
                        None
                    } else {
                        Some(current)
                    });
                    break;
                }
                Some('"') => {
                    // Quoted field.
                    loop {
                        match chars.next() {
                            None => break,
                            Some('"') => {
                                if chars.peek() == Some(&'"') {
                                    chars.next();
                                    current.push('"');
                                } else {
                                    break; // end of quoted field
                                }
                            }
                            Some(ch) => current.push(ch),
                        }
                    }
                    // Skip optional delimiter after closing quote.
                    if chars.peek() == Some(&delim) {
                        chars.next();
                        result.push(if current.is_empty() {
                            None
                        } else {
                            Some(current.clone())
                        });
                        current.clear();
                    }
                }
                Some(c) if c == delim => {
                    result.push(if current.is_empty() {
                        None
                    } else {
                        Some(current.clone())
                    });
                    current.clear();
                }
                Some(c) => current.push(c),
            }
        }
        result
    } else {
        // PostgreSQL text format: tab (or custom) delimiter, `\N` = NULL.
        line.split(delim)
            .map(|f| {
                if f == "\\N" {
                    None
                } else {
                    Some(unescape_copy_text(f))
                }
            })
            .collect()
    }
}

/// Unescape PostgreSQL text-format escape sequences.
fn unescape_copy_text(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('t') => result.push('\t'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('\\') => result.push('\\'),
                Some(ch) => {
                    result.push('\\');
                    result.push(ch);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    // 3.14/3.14159 here are arbitrary test fixtures, not PI approximations.
    #![allow(clippy::approx_constant)]
    use super::*;

    // ── Binary-parameter typed decoding (corruption-class regression) ──

    #[test]
    fn binary_param_timestamp_decodes_to_literal() {
        // day 8851 after 2000-01-01 = 2024-03-26, 12:34:56.789012
        let us: i64 = 8851 * 86_400_000_000 + 45_296_789_012;
        let got = decode_binary_param_typed(1114, &us.to_be_bytes()).unwrap();
        match got {
            DecodedParam::Text(s) => {
                assert_eq!(s, Value::Timestamp(us).to_string());
                assert_eq!(s, "2024-03-26 12:34:56.789012");
            }
            other => panic!("expected Text, got {other:?}"),
        }
        // timestamptz shares the encoding
        assert!(decode_binary_param_typed(1184, &us.to_be_bytes()).is_some());
        // wrong length → undecodable, NOT reinterpreted
        assert!(decode_binary_param_typed(1114, &[0u8; 4]).is_none());
    }

    #[test]
    fn binary_param_date_decodes_to_literal() {
        let days: i32 = 8845; // 2024-03-19
        match decode_binary_param_typed(1082, &days.to_be_bytes()).unwrap() {
            DecodedParam::Text(s) => assert_eq!(s, Value::Date(days).to_string()),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn binary_param_uuid_decodes_canonical() {
        let bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        match decode_binary_param_typed(2950, &bytes).unwrap() {
            DecodedParam::Text(s) => {
                assert_eq!(s, "550e8400-e29b-41d4-a716-446655440000");
            }
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn binary_param_bytea_decodes_hex_form() {
        match decode_binary_param_typed(17, &[0x00, 0xde, 0xad, 0xbe, 0xef]).unwrap() {
            DecodedParam::Text(s) => assert_eq!(s, "\\x00deadbeef"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn binary_param_numeric_decodes_exact() {
        // 12345.6789: ndigits=3 weight=1 sign=0 dscale=4 digits=[1,2345,6789]
        let mut b = Vec::new();
        b.extend_from_slice(&3u16.to_be_bytes());
        b.extend_from_slice(&1i16.to_be_bytes());
        b.extend_from_slice(&0u16.to_be_bytes());
        b.extend_from_slice(&4u16.to_be_bytes());
        for d in [1u16, 2345, 6789] {
            b.extend_from_slice(&d.to_be_bytes());
        }
        match decode_binary_param_typed(1700, &b).unwrap() {
            DecodedParam::Numeric(s) => assert_eq!(s, "12345.6789"),
            other => panic!("expected Numeric, got {other:?}"),
        }

        // -0.0001: ndigits=1 weight=-1 sign=0x4000 dscale=4 digits=[1]
        // (NBASE word at weight -1 covers the first 4 fraction digits)
        let mut b = Vec::new();
        b.extend_from_slice(&1u16.to_be_bytes());
        b.extend_from_slice(&(-1i16).to_be_bytes());
        b.extend_from_slice(&0x4000u16.to_be_bytes());
        b.extend_from_slice(&4u16.to_be_bytes());
        b.extend_from_slice(&1u16.to_be_bytes());
        match decode_binary_param_typed(1700, &b).unwrap() {
            DecodedParam::Numeric(s) => assert_eq!(s, "-0.0001"),
            other => panic!("expected Numeric, got {other:?}"),
        }

        // 0.00001: weight=-2 (one leading zero word), digits=[1000], dscale=5
        let mut b = Vec::new();
        b.extend_from_slice(&1u16.to_be_bytes());
        b.extend_from_slice(&(-2i16).to_be_bytes());
        b.extend_from_slice(&0u16.to_be_bytes());
        b.extend_from_slice(&5u16.to_be_bytes());
        b.extend_from_slice(&1000u16.to_be_bytes());
        match decode_binary_param_typed(1700, &b).unwrap() {
            DecodedParam::Numeric(s) => assert_eq!(s, "0.00001"),
            other => panic!("expected Numeric, got {other:?}"),
        }

        // 42 integer: ndigits=1 weight=0 sign=0 dscale=0
        let mut b = Vec::new();
        b.extend_from_slice(&1u16.to_be_bytes());
        b.extend_from_slice(&0i16.to_be_bytes());
        b.extend_from_slice(&0u16.to_be_bytes());
        b.extend_from_slice(&0u16.to_be_bytes());
        b.extend_from_slice(&42u16.to_be_bytes());
        match decode_binary_param_typed(1700, &b).unwrap() {
            DecodedParam::Numeric(s) => assert_eq!(s, "42"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn binary_param_interval_decodes_to_unit_literal() {
        // 1 month, 2 days, 3.5 seconds
        let mut b = Vec::new();
        b.extend_from_slice(&3_500_000i64.to_be_bytes());
        b.extend_from_slice(&2i32.to_be_bytes());
        b.extend_from_slice(&1i32.to_be_bytes());
        match decode_binary_param_typed(1186, &b).unwrap() {
            DecodedParam::Text(s) => assert_eq!(s, "1 months 2 days 3.500000 seconds"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn sanitize_preserves_backslashes_literally() {
        // Standard-conforming dialect: '\' is literal inside '...'. Doubling
        // it (the old behavior) corrupted any text param containing '\'.
        assert_eq!(sanitize_sql_text_literal(r"C:\temp\x"), r"C:\temp\x");
        assert_eq!(sanitize_sql_text_literal("it's"), "it''s");
        assert_eq!(sanitize_sql_text_literal("nul\0byte"), "nulbyte");
    }

    // ── UserAuthenticator unit tests ───────────────────────────────────

    #[test]
    fn authenticator_stores_credentials() {
        let auth = UserAuthenticator::new("alice", "s3cret");
        assert_eq!(auth.username(), "alice");
        assert_eq!(auth.password(), "s3cret");
    }

    #[test]
    fn authenticator_default_credentials() {
        let auth = UserAuthenticator::new("nucleus", "nucleus");
        assert_eq!(auth.username(), "nucleus");
        assert_eq!(auth.password(), "nucleus");
    }

    // ── AuthSource trait tests ─────────────────────────────────────────

    #[tokio::test]
    async fn auth_source_cleartext_returns_raw_password_no_salt() {
        // Cleartext auth: the startup handler compares these bytes directly,
        // so the AuthSource must return the raw password with no salt.
        let auth = UserAuthenticator::with_method("nucleus", "mypass", AuthMethod::Cleartext);
        let login = LoginInfo::new(Some("nucleus"), None, "127.0.0.1".into());
        let result = auth.get_password(&login).await;
        assert!(result.is_ok());
        let pw = result.unwrap();
        assert_eq!(pw.password(), b"mypass");
        assert!(pw.salt().is_none());
    }

    #[tokio::test]
    async fn auth_source_scram_returns_salted_password_with_salt() {
        // SCRAM auth: the AuthSource must return a salt AND the RFC 5802 salted
        // password Hi(password, salt, iters). Returning a salt-less password is
        // what panicked pgwire ("Salt required for SCRAM auth source").
        let auth = UserAuthenticator::with_method("nucleus", "mypass", AuthMethod::ScramSha256);
        let login = LoginInfo::new(Some("nucleus"), None, "127.0.0.1".into());
        let pw = auth.get_password(&login).await.expect("scram get_password");

        // A salt must be present.
        let salt = pw.salt().expect("SCRAM password must carry a salt");
        assert_eq!(salt.len(), SCRAM_SALT_LEN);

        // The returned "password" is the salted password, not the cleartext.
        assert_ne!(pw.password(), b"mypass");
        // SHA-256 output width.
        assert_eq!(pw.password().len(), 32);

        // Derivation is deterministic given the same salt + iteration count:
        // recomputing Hi(password, salt, iters) must reproduce it exactly.
        let expected = gen_salted_password("mypass", salt, SCRAM_ITERATIONS);
        assert_eq!(pw.password(), expected.as_slice());
    }

    #[tokio::test]
    async fn auth_source_scram_salt_is_stable_across_calls() {
        // The salt must be constant across the multi-round SCRAM exchange (the
        // client is told the salt and must derive the same salted password).
        let auth = UserAuthenticator::with_method("nucleus", "mypass", AuthMethod::ScramSha256);
        let login = LoginInfo::new(Some("nucleus"), None, "127.0.0.1".into());
        let a = auth.get_password(&login).await.unwrap();
        let b = auth.get_password(&login).await.unwrap();
        assert_eq!(a.salt(), b.salt());
        assert_eq!(a.password(), b.password());
    }

    #[tokio::test]
    async fn catalog_auth_uses_each_login_roles_persistable_scram_verifier() {
        let executor = make_executor();
        executor
            .execute("CREATE ROLE alice LOGIN PASSWORD 'alice-secret'")
            .await
            .unwrap();
        let auth = CatalogAuthenticator {
            executor: executor.clone(),
        };
        let login = LoginInfo::new(Some("alice"), None, "127.0.0.1".into());
        let password = auth.get_password(&login).await.unwrap();
        let salt = password.salt().expect("catalog SCRAM verifier has a salt");
        assert_eq!(
            password.password(),
            gen_salted_password("alice-secret", salt, SCRAM_ITERATIONS)
        );

        let no_login = LoginInfo::new(Some("missing"), None, "127.0.0.1".into());
        assert!(auth.get_password(&no_login).await.is_err());
        let handler = NucleusHandler::with_catalog_auth(executor);
        assert_eq!(handler.auth_method(), AuthMethod::ScramSha256);
        assert!(handler.catalog_authenticator.is_some());
        assert!(handler.scram_auth.is_some());
    }

    #[tokio::test]
    async fn auth_source_rejects_wrong_user() {
        let auth = UserAuthenticator::new("nucleus", "mypass");
        let login = LoginInfo::new(Some("intruder"), None, "127.0.0.1".into());
        let result = auth.get_password(&login).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn auth_source_rejects_empty_user() {
        let auth = UserAuthenticator::new("nucleus", "mypass");
        let login = LoginInfo::new(None, None, "127.0.0.1".into());
        let result = auth.get_password(&login).await;
        assert!(result.is_err());
    }

    // ── NucleusHandler constructor tests ───────────────────────────────

    fn make_executor() -> Arc<Executor> {
        let catalog = Arc::new(crate::catalog::Catalog::new());
        let storage: Arc<dyn crate::storage::StorageEngine> =
            Arc::new(crate::storage::MemoryEngine::new());
        let ex = Arc::new(Executor::new(catalog, storage));
        ex.install_self_ref();
        ex
    }

    #[test]
    fn handler_new_has_no_auth() {
        let handler = NucleusHandler::new(make_executor());
        assert!(handler.authenticator.is_none());
    }

    #[test]
    fn handler_with_password_some() {
        let handler = NucleusHandler::with_password(make_executor(), Some("secret".into()));
        let auth = handler.authenticator.as_ref().unwrap();
        assert_eq!(auth.username(), "nucleus");
        assert_eq!(auth.password(), "secret");
        assert_eq!(handler.auth_method(), AuthMethod::ScramSha256);
        assert!(handler.scram_auth.is_some());
    }

    #[test]
    fn handler_with_password_none_has_no_auth() {
        let handler = NucleusHandler::with_password(make_executor(), None);
        assert!(handler.authenticator.is_none());
        assert!(handler.scram_auth.is_none());
    }

    #[test]
    fn handler_with_auth_custom_credentials() {
        let auth = UserAuthenticator::new("admin", "hunter2");
        let handler = NucleusHandler::with_auth(make_executor(), Some(auth));
        let a = handler.authenticator.as_ref().unwrap();
        assert_eq!(a.username(), "admin");
        assert_eq!(a.password(), "hunter2");
    }

    #[test]
    fn handler_with_auth_none() {
        let handler = NucleusHandler::with_auth(make_executor(), None);
        assert!(handler.authenticator.is_none());
        assert!(handler.scram_auth.is_none());
    }

    #[test]
    fn handler_aligns_authenticator_method_with_negotiated_cleartext() {
        // An authenticator built with the default method (SCRAM) must be
        // realigned to Cleartext when the handler negotiates cleartext, so the
        // AuthSource hands back the raw password the cleartext path compares.
        let auth = UserAuthenticator::new("nucleus", "mypass");
        assert_eq!(auth.auth_method(), AuthMethod::ScramSha256);
        let handler = NucleusHandler::with_auth_and_method(
            make_executor(),
            Some(auth),
            AuthMethod::Cleartext,
        );
        assert_eq!(handler.auth_method(), AuthMethod::Cleartext);
        assert_eq!(
            handler.authenticator.as_ref().unwrap().auth_method(),
            AuthMethod::Cleartext
        );
        // No SCRAM state should be built for the cleartext handler.
        assert!(handler.scram_auth.is_none());
    }

    #[test]
    fn handler_with_password_cleartext_mode() {
        let handler = NucleusHandler::with_password_and_method(
            make_executor(),
            Some("secret".into()),
            AuthMethod::Cleartext,
        );
        assert_eq!(handler.auth_method(), AuthMethod::Cleartext);
        assert!(handler.authenticator.is_some());
        assert!(handler.scram_auth.is_none());
    }

    // ── Password comparison tests ──────────────────────────────────────

    #[tokio::test]
    async fn password_bytes_match_correctly() {
        // Exercises the cleartext comparison path: the AuthSource returns raw
        // password bytes that the wire handler compares against the incoming ones.
        let auth = UserAuthenticator::with_method("nucleus", "nucleus", AuthMethod::Cleartext);
        let login = LoginInfo::new(Some("nucleus"), None, "127.0.0.1".into());
        let expected = auth.get_password(&login).await.unwrap();
        // Simulate what the wire handler does: compare expected vs incoming bytes
        assert_eq!(expected.password(), b"nucleus");
        assert_eq!(expected.password(), "nucleus".as_bytes());
    }

    #[tokio::test]
    async fn password_bytes_mismatch_detected() {
        let auth = UserAuthenticator::with_method("nucleus", "correct", AuthMethod::Cleartext);
        let login = LoginInfo::new(Some("nucleus"), None, "127.0.0.1".into());
        let expected = auth.get_password(&login).await.unwrap();
        assert_ne!(expected.password(), b"wrong");
    }

    // ── Placeholder counting tests ─────────────────────────────────────

    #[test]
    fn count_placeholders_none() {
        assert_eq!(count_placeholders("SELECT 1"), 0);
    }

    #[test]
    fn count_placeholders_single() {
        assert_eq!(count_placeholders("SELECT $1"), 1);
    }

    #[test]
    fn count_placeholders_multiple() {
        assert_eq!(
            count_placeholders("SELECT * FROM t WHERE a = $1 AND b = $2"),
            2
        );
    }

    #[test]
    fn count_placeholders_out_of_order() {
        assert_eq!(count_placeholders("SELECT $3, $1"), 3);
    }

    #[test]
    fn count_placeholders_repeated() {
        assert_eq!(count_placeholders("SELECT $1, $1, $2"), 2);
    }

    #[test]
    fn count_placeholders_in_string_literal() {
        assert_eq!(count_placeholders("SELECT '$1'"), 0);
    }

    #[test]
    fn count_placeholders_ignores_comments() {
        assert_eq!(count_placeholders("SELECT 1 -- $9\nWHERE id = $2"), 2);
        assert_eq!(count_placeholders("SELECT /* $7 */ $3"), 3);
    }

    // ── is_select_query tests ──────────────────────────────────────────

    #[test]
    fn is_select_detects_select() {
        assert!(is_select_query("SELECT * FROM t"));
        assert!(is_select_query("  select 1"));
        assert!(is_select_query("WITH cte AS (SELECT 1) SELECT * FROM cte"));
    }

    #[test]
    fn is_select_rejects_non_select() {
        assert!(!is_select_query("INSERT INTO t VALUES (1)"));
        assert!(!is_select_query("UPDATE t SET a = 1"));
        assert!(!is_select_query("DELETE FROM t"));
        assert!(!is_select_query("CREATE TABLE t (a INT)"));
    }

    // ── Parameter type inference tests ──────────────────────────────────

    #[test]
    fn infer_types_no_params() {
        let types = NucleusHandler::infer_parameter_types("SELECT 1", &[]);
        assert!(types.is_empty());
    }

    #[test]
    fn infer_types_uses_declared_types() {
        let declared = vec![Some(Type::INT4), Some(Type::BOOL)];
        let types = NucleusHandler::infer_parameter_types("SELECT $1, $2", &declared);
        assert_eq!(types.len(), 2);
        assert_eq!(types[0], Type::INT4);
        assert_eq!(types[1], Type::BOOL);
    }

    #[test]
    fn infer_types_defaults_to_text() {
        let declared = vec![None, None, None];
        let types = NucleusHandler::infer_parameter_types("SELECT $1, $2, $3", &declared);
        assert_eq!(types.len(), 3);
        for t in &types {
            assert_eq!(*t, Type::TEXT);
        }
    }

    #[test]
    fn infer_types_partial_declared() {
        let declared = vec![Some(Type::INT8), None];
        let types = NucleusHandler::infer_parameter_types("SELECT $1, $2", &declared);
        assert_eq!(types.len(), 2);
        assert_eq!(types[0], Type::INT8);
        assert_eq!(types[1], Type::TEXT);
    }

    #[test]
    fn infer_types_more_placeholders_than_declared() {
        let declared = vec![Some(Type::INT4)];
        let types = NucleusHandler::infer_parameter_types("SELECT $1, $2, $3", &declared);
        assert_eq!(types.len(), 3);
        assert_eq!(types[0], Type::INT4);
        assert_eq!(types[1], Type::TEXT);
        assert_eq!(types[2], Type::TEXT);
    }

    // ── NucleusQueryParser tests ───────────────────────────────────────

    #[test]
    fn query_parser_is_clone_and_debug() {
        let parser = NucleusQueryParser::new(make_executor());
        let _cloned = parser.clone();
        let _debug = format!("{:?}", parser);
    }

    // ── Compression integration tests ───────────────────────────────────

    #[test]
    fn handler_has_compressor() {
        let handler = NucleusHandler::new(make_executor());
        // Small payload: not compressed
        let (out, compressed) = handler.compress_payload(b"tiny");
        assert!(!compressed);
        assert_eq!(out, b"tiny");
    }

    #[test]
    fn compress_large_payload_roundtrip() {
        let handler = NucleusHandler::new(make_executor());
        let large = "SELECT * FROM big_table WHERE id = 42; ".repeat(100);
        let (compressed, was_compressed) = handler.compress_payload(large.as_bytes());
        assert!(was_compressed);
        assert!(compressed.len() < large.len());

        let recovered = handler
            .decompress_payload(&compressed, true)
            .expect("decompression should succeed");
        assert_eq!(recovered, large.as_bytes());
    }

    #[test]
    fn decompress_uncompressed_passthrough() {
        let handler = NucleusHandler::new(make_executor());
        let data = b"just plain text";
        let recovered = handler
            .decompress_payload(data, false)
            .expect("passthrough should succeed");
        assert_eq!(recovered, data);
    }

    #[test]
    fn with_password_handler_has_compressor() {
        let handler = NucleusHandler::with_password(make_executor(), Some("pw".into()));
        let large = "INSERT INTO t VALUES (1, 'hello'); ".repeat(100);
        let (compressed, was_compressed) = handler.compress_payload(large.as_bytes());
        assert!(was_compressed);
        let recovered = handler.decompress_payload(&compressed, true).unwrap();
        assert_eq!(recovered, large.as_bytes());
    }

    #[test]
    fn with_auth_handler_has_compressor() {
        let auth = UserAuthenticator::new("admin", "pass");
        let handler = NucleusHandler::with_auth(make_executor(), Some(auth));
        let (_, compressed) = handler.compress_payload(b"small");
        assert!(!compressed);
    }

    // ── pg_type_to_data_type mapping tests ─────────────────────────────

    #[test]
    fn pg_type_roundtrip_bool() {
        let dt = DataType::Bool;
        let pg = data_type_to_pg(&dt);
        assert_eq!(pg, Type::BOOL);
        assert_eq!(pg_type_to_data_type(&pg), DataType::Bool);
    }

    #[test]
    fn pg_type_roundtrip_int32() {
        let dt = DataType::Int32;
        let pg = data_type_to_pg(&dt);
        assert_eq!(pg, Type::INT4);
        assert_eq!(pg_type_to_data_type(&pg), DataType::Int32);
    }

    #[test]
    fn pg_type_roundtrip_int64() {
        let dt = DataType::Int64;
        let pg = data_type_to_pg(&dt);
        assert_eq!(pg, Type::INT8);
        assert_eq!(pg_type_to_data_type(&pg), DataType::Int64);
    }

    #[test]
    fn pg_type_roundtrip_float64() {
        let dt = DataType::Float64;
        let pg = data_type_to_pg(&dt);
        assert_eq!(pg, Type::FLOAT8);
        assert_eq!(pg_type_to_data_type(&pg), DataType::Float64);
    }

    #[test]
    fn pg_type_roundtrip_text() {
        let dt = DataType::Text;
        let pg = data_type_to_pg(&dt);
        assert_eq!(pg, Type::VARCHAR);
        assert_eq!(pg_type_to_data_type(&pg), DataType::Text);
    }

    #[test]
    fn pg_type_unknown_defaults_to_text() {
        // OID types, etc. should default to Text
        assert_eq!(pg_type_to_data_type(&Type::OID), DataType::Text);
    }

    // ── NucleusServer wiring tests ─────────────────────────────────────

    #[test]
    fn server_provides_all_handlers() {
        let handler = Arc::new(NucleusHandler::new(make_executor()));
        let server = NucleusServer::new(handler);

        // Verify that all handler accessors return valid Arc references.
        let _simple = server.simple_query_handler();
        let _extended = server.extended_query_handler();
        let _startup = server.startup_handler();
    }

    // ── Extended query integration tests ───────────────────────────────

    #[tokio::test]
    async fn extended_query_execute_simple_select() {
        let handler = NucleusHandler::new(make_executor());

        // Execute a simple query without parameters through the executor
        let results = handler.execute_sql("SELECT 1 AS num").await;
        assert!(results.is_ok());
        let results = results.unwrap();
        assert_eq!(results.len(), 1);
        match &results[0] {
            ExecResult::Select { columns, rows } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[tokio::test]
    async fn extended_query_execute_ddl() {
        let handler = NucleusHandler::new(make_executor());

        let results = handler
            .execute_sql("CREATE TABLE test_ext (id INTEGER, name TEXT)")
            .await;
        assert!(results.is_ok());
        let results = results.unwrap();
        assert_eq!(results.len(), 1);
        match &results[0] {
            ExecResult::Command { tag, .. } => {
                assert!(tag.contains("CREATE"));
            }
            _ => panic!("Expected Command result"),
        }
    }

    #[tokio::test]
    async fn extended_query_build_response_select() {
        let result = ExecResult::Select {
            columns: vec![
                ("id".to_string(), DataType::Int32),
                ("name".to_string(), DataType::Text),
            ],
            rows: vec![
                vec![Value::Int32(1), Value::Text("alice".into())],
                vec![Value::Int32(2), Value::Text("bob".into())],
            ],
        };
        let response = NucleusHandler::build_response(result, None);
        assert!(response.is_ok());
        match response.unwrap() {
            Response::Query(_) => {} // Expected
            _ => panic!("Expected Query response"),
        }
    }

    /// Phase 1.1: a SelectStream must build a streaming Query response (the arm
    /// that previously `unreachable!`d). Rows/columns correctness is covered by
    /// the executor-level streaming-vs-materialized equivalence tests; the
    /// encoder here is byte-identical to the Select arm.
    #[tokio::test]
    async fn build_response_streams_select_stream() {
        let columns = vec![
            ("id".to_string(), DataType::Int32),
            ("name".to_string(), DataType::Text),
        ];
        let rows = vec![
            vec![Value::Int32(1), Value::Text("alice".into())],
            vec![Value::Int32(2), Value::Text("bob".into())],
        ];
        let source = Box::new(
            crate::executor::row_batch::MaterializedBatchIter::with_batch_size(rows, 1),
        );
        let result = ExecResult::SelectStream { columns, source };
        let response = NucleusHandler::build_response(result, None);
        assert!(response.is_ok());
        match response.unwrap() {
            Response::Query(_) => {}
            _ => panic!("Expected streaming Query response"),
        }
    }

    /// Result formats belong to the CLIENT (Bind): text-mode clients must
    /// get text even for numeric columns — the server unilaterally sending
    /// binary made node-postgres read Float64(1.0) back as ~0.992 garbage.
    #[tokio::test]
    async fn build_response_honors_client_result_formats() {
        let make = || ExecResult::Select {
            columns: vec![
                ("d".to_string(), DataType::Float64),
                ("n".to_string(), DataType::Int64),
                ("s".to_string(), DataType::Text),
            ],
            rows: vec![vec![
                Value::Float64(1.0),
                Value::Int64(7),
                Value::Text("x".into()),
            ]],
        };
        let field_formats = |response: Response| match response {
            Response::Query(q) => q
                .row_schema()
                .iter()
                .map(|f| f.format())
                .collect::<Vec<_>>(),
            _ => panic!("Expected Query response"),
        };

        // Simple protocol (None) and an explicit text request: all text.
        for formats in [None, Some(Format::UnifiedText)] {
            let response =
                NucleusHandler::build_response(make(), formats.as_ref()).expect("response");
            assert!(
                field_formats(response)
                    .iter()
                    .all(|f| *f == FieldFormat::Text),
                "text-mode clients must never receive binary columns"
            );
        }

        // A client that ASKS for binary still gets it.
        let response =
            NucleusHandler::build_response(make(), Some(&Format::UnifiedBinary)).expect("response");
        assert!(
            field_formats(response)
                .iter()
                .all(|f| *f == FieldFormat::Binary)
        );

        // Per-column codes are honored; missing codes fall back to text.
        let response =
            NucleusHandler::build_response(make(), Some(&Format::Individual(vec![1, 0])))
                .expect("response");
        assert_eq!(
            field_formats(response),
            vec![FieldFormat::Binary, FieldFormat::Text, FieldFormat::Text]
        );
    }

    #[tokio::test]
    async fn extended_query_build_response_command() {
        let result = ExecResult::Command {
            tag: "INSERT".to_string(),
            rows_affected: 3,
        };
        let response = NucleusHandler::build_response(result, None);
        assert!(response.is_ok());
        match response.unwrap() {
            Response::Execution(tag) => {
                assert_eq!(tag, Tag::new("INSERT 0").with_rows(3));
            }
            _ => panic!("Expected Execution response"),
        }
    }

    // -- Wire protocol integration tests (6.3) --

    #[test]
    fn test_pg_type_to_data_type_integers() {
        assert_eq!(pg_type_to_data_type(&Type::INT4), DataType::Int32);
        assert_eq!(pg_type_to_data_type(&Type::INT8), DataType::Int64);
    }

    #[test]
    fn test_pg_type_to_data_type_floats() {
        assert_eq!(pg_type_to_data_type(&Type::FLOAT4), DataType::Float64);
        assert_eq!(pg_type_to_data_type(&Type::FLOAT8), DataType::Float64);
    }

    #[test]
    fn test_pg_type_to_data_type_text() {
        assert_eq!(pg_type_to_data_type(&Type::TEXT), DataType::Text);
        assert_eq!(pg_type_to_data_type(&Type::VARCHAR), DataType::Text);
    }

    #[test]
    fn test_pg_type_to_data_type_bool() {
        assert_eq!(pg_type_to_data_type(&Type::BOOL), DataType::Bool);
    }

    #[test]
    fn test_pg_type_to_data_type_bytea() {
        assert_eq!(pg_type_to_data_type(&Type::BYTEA), DataType::Bytea);
    }

    #[test]
    fn test_data_type_to_pg_roundtrip() {
        // Verify that core types map correctly
        assert_eq!(data_type_to_pg(&DataType::Int32), Type::INT4);
        assert_eq!(data_type_to_pg(&DataType::Int64), Type::INT8);
        assert_eq!(data_type_to_pg(&DataType::Text), Type::VARCHAR);
        assert_eq!(data_type_to_pg(&DataType::Bool), Type::BOOL);
        assert_eq!(data_type_to_pg(&DataType::Float64), Type::FLOAT8);
    }

    #[tokio::test]
    async fn build_response_select_empty_rows() {
        let result = ExecResult::Select {
            columns: vec![
                ("id".to_string(), DataType::Int32),
                ("name".to_string(), DataType::Text),
            ],
            rows: vec![],
        };
        let response = NucleusHandler::build_response(result, None);
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn build_response_command_zero_rows() {
        let result = ExecResult::Command {
            tag: "DELETE".to_string(),
            rows_affected: 0,
        };
        let response = NucleusHandler::build_response(result, None);
        assert!(response.is_ok());
        match response.unwrap() {
            Response::Execution(tag) => {
                assert_eq!(tag, Tag::new("DELETE").with_rows(0));
            }
            _ => panic!("Expected Execution response"),
        }
    }

    #[tokio::test]
    async fn build_response_select_with_null_values() {
        let result = ExecResult::Select {
            columns: vec![
                ("id".to_string(), DataType::Int32),
                ("val".to_string(), DataType::Text),
            ],
            rows: vec![
                vec![Value::Int32(1), Value::Null],
                vec![Value::Int32(2), Value::Text("hello".into())],
            ],
        };
        let response = NucleusHandler::build_response(result, None);
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn build_response_select_with_varied_types() {
        let result = ExecResult::Select {
            columns: vec![
                ("int_col".to_string(), DataType::Int32),
                ("float_col".to_string(), DataType::Float64),
                ("text_col".to_string(), DataType::Text),
                ("bool_col".to_string(), DataType::Bool),
            ],
            rows: vec![vec![
                Value::Int32(42),
                Value::Float64(3.14),
                Value::Text("hello".into()),
                Value::Bool(true),
            ]],
        };
        let response = NucleusHandler::build_response(result, None);
        assert!(response.is_ok());
    }

    #[test]
    fn wire_compressor_compress_roundtrip() {
        let compressor = WireCompressor::new(64);
        let data = vec![0xAB; 128]; // above threshold
        let (compressed, was_compressed) = compressor.compress_if_beneficial(&data);
        if was_compressed {
            let decompressed = compressor.decompress_if_needed(&compressed, true).unwrap();
            assert_eq!(decompressed, data);
        }
    }

    #[test]
    fn wire_compressor_below_threshold_skips() {
        let compressor = WireCompressor::new(256);
        let data = vec![0xCD; 100]; // below threshold
        let (output, was_compressed) = compressor.compress_if_beneficial(&data);
        assert!(!was_compressed);
        assert_eq!(output, data);
    }
}

#[cfg(test)]
mod security_tests {
    use super::*;

    fn make_executor() -> Arc<Executor> {
        let catalog = Arc::new(crate::catalog::Catalog::new());
        let storage: Arc<dyn crate::storage::StorageEngine> =
            Arc::new(crate::storage::MemoryEngine::new());
        let ex = Arc::new(Executor::new(catalog, storage));
        ex.install_self_ref();
        ex
    }

    #[test]
    fn parameter_substitution_escapes_single_quotes() {
        // A value containing a single quote must be escaped to ''
        let result = NucleusHandler::substitute_parameters_raw(
            "SELECT * FROM users WHERE name = $1",
            &["O'Reilly"],
        );
        assert_eq!(
            result, "SELECT * FROM users WHERE name = 'O''Reilly'",
            "Single quotes in parameter values must be doubled"
        );
    }

    #[test]
    fn parameter_substitution_strips_nul_bytes() {
        // NUL bytes in parameter values must be stripped
        let result = NucleusHandler::substitute_parameters_raw(
            "SELECT * FROM t WHERE col = $1",
            &["hello\0world"],
        );
        assert_eq!(
            result, "SELECT * FROM t WHERE col = 'helloworld'",
            "NUL bytes must be removed from parameter values"
        );
    }

    #[test]
    fn parameter_substitution_no_double_substitution() {
        // A parameter value containing $2 must NOT cause the $2 placeholder
        // to be replaced with the first parameter's value (double-substitution attack).
        let result =
            NucleusHandler::substitute_parameters_raw("SELECT $1, $2", &["$2", "real_value"]);
        // $1 should become '$2' (literal) and $2 should become 'real_value'.
        assert_eq!(
            result, "SELECT '$2', 'real_value'",
            "Parameter value containing $2 must not cause double-substitution"
        );
    }

    #[test]
    fn parameter_substitution_replaces_repeated_placeholder() {
        let result = NucleusHandler::substitute_parameters_raw("SELECT $1, $1", &["abc"]);
        assert_eq!(result, "SELECT 'abc', 'abc'");
    }

    #[test]
    fn parameter_substitution_skips_string_literal_placeholder() {
        let result = NucleusHandler::substitute_parameters_raw("SELECT '$1', $1", &["abc"]);
        assert_eq!(result, "SELECT '$1', 'abc'");
    }

    #[test]
    fn parameter_substitution_preserves_backslashes() {
        // PostgreSqlDialect is standard-conforming: '\' is a LITERAL character
        // inside '...', so doubling it (the old behavior this test used to
        // assert) corrupted stored values. Injection safety comes from
        // quote-doubling — a param ending in '\' followed by a quote still
        // cannot escape the literal because the quote itself is doubled.
        let result = NucleusHandler::substitute_parameters_raw("SELECT $1", &["back\\slash"]);
        assert_eq!(
            result, "SELECT 'back\\slash'",
            "Backslashes in parameter values must be preserved literally"
        );
        // Attempted escape-out: backslash + quote → quote is doubled, string
        // stays closed exactly where the substitution closes it.
        let tricky = NucleusHandler::substitute_parameters_raw("SELECT $1", &["a\\', 1); --"]);
        assert_eq!(tricky, "SELECT 'a\\'', 1); --'");
    }

    // ── COPY helper tests ──────────────────────────────────────────────

    #[test]
    fn detect_copy_from_stdin_text_format() {
        let info = detect_copy_from_stdin("COPY my_table FROM STDIN").unwrap();
        assert_eq!(info.table, "my_table");
        assert!(info.columns.is_none());
        assert_eq!(info.delimiter, b'\t');
        assert!(!info.is_csv);
        assert!(!info.has_header);
    }

    #[test]
    fn detect_copy_from_stdin_csv_format() {
        let info = detect_copy_from_stdin(
            "COPY orders (id, amount) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')",
        )
        .unwrap();
        assert_eq!(info.table, "orders");
        assert_eq!(
            info.columns.as_deref(),
            Some(&["id".to_owned(), "amount".to_owned()][..])
        );
        assert_eq!(info.delimiter, b',');
        assert!(info.is_csv);
    }

    #[test]
    fn detect_copy_to_stdout_returns_none() {
        assert!(detect_copy_from_stdin("COPY my_table TO STDOUT").is_none());
    }

    #[test]
    fn detect_copy_select_returns_none() {
        assert!(detect_copy_from_stdin("SELECT 1").is_none());
    }

    #[test]
    fn parse_copy_rows_tab_delimited() {
        let data = b"1\thello\t3.14\n2\tworld\t2.71\n";
        let rows = parse_copy_rows(data, b'\t', false, false);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].as_deref(), Some("1"));
        assert_eq!(rows[0][1].as_deref(), Some("hello"));
        assert_eq!(rows[1][1].as_deref(), Some("world"));
    }

    #[test]
    fn parse_copy_rows_tab_null_value() {
        let data = b"1\t\\N\t3.14\n";
        let rows = parse_copy_rows(data, b'\t', false, false);
        assert_eq!(rows[0][1], None);
        assert_eq!(rows[0][0].as_deref(), Some("1"));
    }

    #[test]
    fn parse_copy_rows_csv() {
        let data = b"1,hello,3.14\n2,world,2.71\n";
        let rows = parse_copy_rows(data, b',', true, false);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].as_deref(), Some("1"));
        assert_eq!(rows[0][1].as_deref(), Some("hello"));
    }

    #[test]
    fn parse_copy_rows_csv_with_header() {
        let data = b"id,name,val\n1,alice,10\n2,bob,20\n";
        let rows = parse_copy_rows(data, b',', true, true);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].as_deref(), Some("alice"));
    }

    #[test]
    fn parse_copy_rows_csv_quoted() {
        let data = b"1,\"hello, world\",3.14\n";
        let rows = parse_copy_rows(data, b',', true, false);
        assert_eq!(rows[0][1].as_deref(), Some("hello, world"));
    }

    #[test]
    fn unescape_copy_text_basic() {
        assert_eq!(unescape_copy_text("hello\\tworld"), "hello\tworld");
        assert_eq!(unescape_copy_text("line1\\nline2"), "line1\nline2");
        assert_eq!(unescape_copy_text("back\\\\slash"), "back\\slash");
        assert_eq!(unescape_copy_text("no_escape"), "no_escape");
    }

    // ── Login rate limiter tests ────────────────────────────────────

    #[test]
    fn rate_limiter_allows_initial_attempts() {
        let limiter = LoginRateLimiter::new();
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        assert!(!limiter.is_locked_out(ip));
    }

    #[test]
    fn rate_limiter_locks_out_after_max_failures() {
        let limiter = LoginRateLimiter::new();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        for _ in 0..LoginRateLimiter::MAX_FAILED_ATTEMPTS {
            limiter.record_failure(ip);
        }
        assert!(
            limiter.is_locked_out(ip),
            "should be locked out after max failures"
        );
    }

    #[test]
    fn rate_limiter_does_not_lock_below_threshold() {
        let limiter = LoginRateLimiter::new();
        let ip: IpAddr = "10.0.0.2".parse().unwrap();
        for _ in 0..(LoginRateLimiter::MAX_FAILED_ATTEMPTS - 1) {
            limiter.record_failure(ip);
        }
        assert!(
            !limiter.is_locked_out(ip),
            "should not lock out below threshold"
        );
    }

    #[test]
    fn rate_limiter_clear_resets() {
        let limiter = LoginRateLimiter::new();
        let ip: IpAddr = "10.0.0.3".parse().unwrap();
        for _ in 0..LoginRateLimiter::MAX_FAILED_ATTEMPTS {
            limiter.record_failure(ip);
        }
        assert!(limiter.is_locked_out(ip));
        limiter.clear(ip);
        assert!(
            !limiter.is_locked_out(ip),
            "should not be locked out after clear"
        );
    }

    #[test]
    fn rate_limiter_different_ips_independent() {
        let limiter = LoginRateLimiter::new();
        let ip_a: IpAddr = "10.0.0.4".parse().unwrap();
        let ip_b: IpAddr = "10.0.0.5".parse().unwrap();
        for _ in 0..LoginRateLimiter::MAX_FAILED_ATTEMPTS {
            limiter.record_failure(ip_a);
        }
        assert!(limiter.is_locked_out(ip_a));
        assert!(
            !limiter.is_locked_out(ip_b),
            "unrelated IP should not be locked out"
        );
    }

    // ── Notification Registry tests ─────────────────────────────────

    #[test]
    fn notification_registry_allocate_pid() {
        let registry = NotificationRegistry::new(16);
        let pid1 = registry.allocate_pid();
        let pid2 = registry.allocate_pid();
        assert_ne!(pid1, pid2);
        assert!(pid1 > 0);
        assert!(pid2 > 0);
    }

    #[test]
    fn notification_registry_listen_and_notify() {
        let registry = NotificationRegistry::new(16);
        let mut rx = registry.listen("test_channel");
        let count = registry.notify(1, "test_channel", "hello");
        assert_eq!(count, 1);
        let notif = rx.try_recv().unwrap();
        assert_eq!(notif.channel, "test_channel");
        assert_eq!(notif.payload, "hello");
        assert_eq!(notif.pid, 1);
    }

    #[test]
    fn notification_registry_multiple_listeners() {
        let registry = NotificationRegistry::new(16);
        let mut rx1 = registry.listen("events");
        let mut rx2 = registry.listen("events");
        let count = registry.notify(1, "events", "data");
        assert_eq!(count, 2);
        assert_eq!(rx1.try_recv().unwrap().payload, "data");
        assert_eq!(rx2.try_recv().unwrap().payload, "data");
    }

    #[test]
    fn notification_registry_no_listeners() {
        let registry = NotificationRegistry::new(16);
        let count = registry.notify(1, "nobody_listens", "hello");
        assert_eq!(count, 0);
    }

    #[test]
    fn notification_registry_remove_empty_channel() {
        let registry = NotificationRegistry::new(16);
        let rx = registry.listen("temp");
        assert!(registry.channels.contains_key("temp"));
        drop(rx);
        registry.remove_channel_if_empty("temp");
        assert!(!registry.channels.contains_key("temp"));
    }

    #[test]
    fn notification_registry_channel_isolation() {
        let registry = NotificationRegistry::new(16);
        let mut rx_a = registry.listen("chan_a");
        let _rx_b = registry.listen("chan_b");
        registry.notify(1, "chan_a", "only_a");
        assert_eq!(rx_a.try_recv().unwrap().payload, "only_a");
    }

    // ── LISTEN/NOTIFY handler integration tests ─────────────────────

    #[test]
    fn handler_listen_registers_channel() {
        let handler = NucleusHandler::new(make_executor());
        handler.handle_listen("peer1", "my_channel");
        let state = handler.notify_state.lock();
        let conn = state.get("peer1").unwrap();
        assert!(conn.channels.contains("my_channel"));
    }

    #[test]
    fn handler_unlisten_removes_channel() {
        let handler = NucleusHandler::new(make_executor());
        handler.handle_listen("peer1", "ch1");
        handler.handle_listen("peer1", "ch2");
        handler.handle_unlisten("peer1", "ch1");
        let state = handler.notify_state.lock();
        let conn = state.get("peer1").unwrap();
        assert!(!conn.channels.contains("ch1"));
        assert!(conn.channels.contains("ch2"));
    }

    #[test]
    fn handler_unlisten_star_removes_all() {
        let handler = NucleusHandler::new(make_executor());
        handler.handle_listen("peer1", "ch1");
        handler.handle_listen("peer1", "ch2");
        handler.handle_listen("peer1", "ch3");
        handler.handle_unlisten("peer1", "*");
        let state = handler.notify_state.lock();
        let conn = state.get("peer1").unwrap();
        assert!(conn.channels.is_empty());
    }

    #[test]
    fn handler_notify_returns_listener_count() {
        let handler = NucleusHandler::new(make_executor());
        handler.handle_listen("peer1", "events");
        handler.handle_listen("peer2", "events");
        let count = handler.handle_notify("peer1", "events", "test");
        // At least 2 listeners registered (our 2 handle_listen calls).
        assert!(count >= 2);
    }

    #[test]
    fn handler_connection_pid_is_stable() {
        let handler = NucleusHandler::new(make_executor());
        let pid1 = handler.connection_pid("peer_x");
        let pid2 = handler.connection_pid("peer_x");
        assert_eq!(pid1, pid2, "same peer should get same pid");
    }

    #[test]
    fn handler_connection_pid_differs_per_peer() {
        let handler = NucleusHandler::new(make_executor());
        let pid1 = handler.connection_pid("peer_a");
        let pid2 = handler.connection_pid("peer_b");
        assert_ne!(pid1, pid2, "different peers should get different pids");
    }

    #[test]
    fn handler_cleanup_removes_notify_state() {
        let handler = NucleusHandler::new(make_executor());
        handler.handle_listen("peer1", "ch");
        handler.cleanup_session("peer1");
        assert!(!handler.notify_state.lock().contains_key("peer1"));
        assert!(!handler.connection_pids.read().contains_key("peer1"));
    }

    // ── Large Objects API tests ─────────────────────────────────────

    #[test]
    fn lo_creat_returns_oid() {
        let handler = NucleusHandler::new(make_executor());
        let result = handler.lo_creat("peer1");
        match result {
            ExecResult::Select { columns, rows } => {
                assert_eq!(columns[0].0, "lo_creat");
                assert!(rows.len() == 1);
                match &rows[0][0] {
                    Value::Int32(oid) => assert!(*oid > 0),
                    _ => panic!("expected Int32 OID"),
                }
            }
            _ => panic!("expected Select result"),
        }
    }

    #[test]
    fn lo_open_close_roundtrip() {
        let handler = NucleusHandler::new(make_executor());
        // Create a large object first.
        let oid = match handler.lo_creat("peer1") {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(oid) => oid as u32,
                _ => panic!("expected oid"),
            },
            _ => panic!("expected select"),
        };
        // Open it.
        let fd = match handler.lo_open("peer1", oid, INV_READ | INV_WRITE) {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(fd) => fd,
                _ => panic!("expected fd"),
            },
            _ => panic!("expected select"),
        };
        assert!(fd > 0);
        // Close it.
        let closed = match handler.lo_close("peer1", fd) {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(v) => v,
                _ => panic!("expected int"),
            },
            _ => panic!("expected select"),
        };
        assert_eq!(closed, 0);
    }

    #[test]
    fn lo_write_and_read() {
        let handler = NucleusHandler::new(make_executor());
        let oid = match handler.lo_creat("peer1") {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(oid) => oid as u32,
                _ => panic!("expected oid"),
            },
            _ => panic!("expected select"),
        };
        let fd = match handler.lo_open("peer1", oid, INV_READ | INV_WRITE) {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(fd) => fd,
                _ => panic!("expected fd"),
            },
            _ => panic!("expected select"),
        };
        // Write data.
        let written = match handler.lo_write("peer1", fd, b"hello world") {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(n) => n,
                _ => panic!("expected int"),
            },
            _ => panic!("expected select"),
        };
        assert_eq!(written, 11);

        // Close and reopen to reset offset.
        handler.lo_close("peer1", fd);
        let fd2 = match handler.lo_open("peer1", oid, INV_READ) {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(fd) => fd,
                _ => panic!("expected fd"),
            },
            _ => panic!("expected select"),
        };
        // Read back.
        let data = match handler.lo_read("peer1", fd2, 11) {
            ExecResult::Select { rows, .. } => match &rows[0][0] {
                Value::Bytea(b) => b.clone(),
                _ => panic!("expected bytea"),
            },
            _ => panic!("expected select"),
        };
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn lo_unlink_deletes_object() {
        let handler = NucleusHandler::new(make_executor());
        let oid = match handler.lo_creat("peer1") {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(oid) => oid as u32,
                _ => panic!("expected oid"),
            },
            _ => panic!("expected select"),
        };
        // Unlink.
        let result = match handler.lo_unlink(oid) {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(v) => v,
                _ => panic!("expected int"),
            },
            _ => panic!("expected select"),
        };
        assert_eq!(result, 0);
        // Opening it should fail now.
        let fd = match handler.lo_open("peer1", oid, INV_READ) {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(fd) => fd,
                _ => panic!("expected fd"),
            },
            _ => panic!("expected select"),
        };
        assert_eq!(fd, -1, "open after unlink should return -1");
    }

    #[test]
    fn lo_read_without_read_permission() {
        let handler = NucleusHandler::new(make_executor());
        let oid = match handler.lo_creat("peer1") {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(oid) => oid as u32,
                _ => panic!("expected oid"),
            },
            _ => panic!("expected select"),
        };
        // Open write-only.
        let fd = match handler.lo_open("peer1", oid, INV_WRITE) {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(fd) => fd,
                _ => panic!("expected fd"),
            },
            _ => panic!("expected select"),
        };
        // Read should return null (no read permission).
        match handler.lo_read("peer1", fd, 10) {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Null);
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn lo_open_nonexistent_returns_minus_one() {
        let handler = NucleusHandler::new(make_executor());
        let fd = match handler.lo_open("peer1", 999_999, INV_READ) {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(fd) => fd,
                _ => panic!("expected fd"),
            },
            _ => panic!("expected select"),
        };
        assert_eq!(fd, -1);
    }

    // ── Large Objects SQL interception tests ─────────────────────────

    #[test]
    fn try_handle_lo_creat() {
        let handler = NucleusHandler::new(make_executor());
        let result = handler.try_handle_large_object("peer1", "SELECT lo_creat(-1)");
        assert!(result.is_some());
        match result.unwrap() {
            ExecResult::Select { columns, rows } => {
                assert_eq!(columns[0].0, "lo_creat");
                assert!(rows.len() == 1);
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn try_handle_lo_non_matching() {
        let handler = NucleusHandler::new(make_executor());
        assert!(
            handler
                .try_handle_large_object("peer1", "SELECT 1")
                .is_none()
        );
        assert!(
            handler
                .try_handle_large_object("peer1", "INSERT INTO t VALUES (1)")
                .is_none()
        );
        assert!(
            handler
                .try_handle_large_object("peer1", "SELECT lower('X')")
                .is_none()
        );
    }

    #[test]
    fn handler_cleanup_removes_lo_state() {
        let handler = NucleusHandler::new(make_executor());
        let oid = match handler.lo_creat("peer1") {
            ExecResult::Select { rows, .. } => match rows[0][0] {
                Value::Int32(oid) => oid as u32,
                _ => panic!("expected oid"),
            },
            _ => panic!("expected select"),
        };
        handler.lo_open("peer1", oid, INV_READ);
        assert!(handler.lo_state.lock().contains_key("peer1"));
        handler.cleanup_session("peer1");
        assert!(!handler.lo_state.lock().contains_key("peer1"));
    }

    // ── lo_blob_key formatting ──────────────────────────────────────

    #[test]
    fn lo_blob_key_format() {
        assert_eq!(lo_blob_key(12345), "_lo/12345");
        assert_eq!(lo_blob_key(0), "_lo/0");
    }

    // ── Describe must not execute side-effecting scalar functions ──

    #[test]
    fn describe_static_fields_covers_mutating_fns() {
        // Bound-parameter shape (statement describe) and substituted
        // literals (portal describe) both resolve statically.
        for sql in [
            "SELECT KV_SETNX($1, $2, 30)",
            "SELECT KV_SETNX('k', 'v', 30)",
            "SELECT kv_setnx('k', 'v')",
        ] {
            let fields = describe_static_fields(sql, None).expect("static describe");
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name(), "kv_setnx");
            assert_eq!(*fields[0].datatype(), Type::BOOL);
        }

        let fields = describe_static_fields("SELECT DOC_INSERT($1)", None).unwrap();
        assert_eq!(*fields[0].datatype(), Type::INT8);
        let fields = describe_static_fields("SELECT STREAM_XADD($1, $2, $3)", None).unwrap();
        assert_eq!(*fields[0].datatype(), Type::VARCHAR);
        let fields = describe_static_fields("SELECT KV_CDEL($1, $2) AS released", None).unwrap();
        assert_eq!(fields[0].name(), "released");
        assert_eq!(*fields[0].datatype(), Type::BOOL);
    }

    #[test]
    fn describe_static_fields_ignores_pure_queries() {
        // Pure reads keep the probe-execution path (None).
        assert!(describe_static_fields("SELECT KV_GET($1)", None).is_none());
        assert!(describe_static_fields("SELECT * FROM users WHERE id = 1", None).is_none());
        assert!(describe_static_fields("SELECT UPPER('a'), STREAM_XLEN('s')", None).is_none());
    }

    #[test]
    fn describe_static_fields_never_falls_through_on_odd_shapes() {
        // Unparseable or non-plain-select statements containing a mutating
        // call still get a static answer — execution is never the fallback.
        let fields = describe_static_fields("SELECT KV_SETNX('k', 'v', 30) UNION SELECT 1", None);
        assert!(fields.is_some());
    }
}
