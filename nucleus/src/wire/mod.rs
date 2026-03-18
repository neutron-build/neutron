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

use pgwire::api::auth::sasl::{SASLState, scram::ScramAuth};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password as AuthPassword,
    StartupHandler, finish_authentication, protocol_negotiation,
    save_startup_parameters_to_metadata,
};
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::Portal;
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
use pgwire::messages::startup::{Authentication, PasswordMessageFamily};
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

/// Stores credentials for password-based authentication.
///
/// When the server is configured with a `UserAuthenticator`, clients must
/// provide the correct username and password via the configured auth method
/// (SCRAM-SHA-256 by default, optional cleartext for legacy clients).
#[derive(Debug, Clone)]
pub struct UserAuthenticator {
    username: String,
    password: String,
}

impl UserAuthenticator {
    /// Create a new authenticator with the given credentials.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
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
}

#[async_trait]
impl AuthSource for UserAuthenticator {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<AuthPassword> {
        // Verify username first — reject unknown users with a clear error.
        let incoming_user = login.user().unwrap_or("");
        if incoming_user != self.username {
            return Err(PgWireError::InvalidPassword(incoming_user.to_owned()));
        }
        Ok(AuthPassword::new(None, self.password.as_bytes().to_vec()))
    }
}

/// Password authentication method for the wire protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
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
            && count >= Self::MAX_FAILED_ATTEMPTS {
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
    has_header: bool,
}

struct CopyInProgress {
    table: String,
    columns: Option<Vec<String>>,
    delimiter: u8,
    is_csv: bool,
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
        }
    }

    /// Active authentication method for this handler.
    pub fn auth_method(&self) -> AuthMethod {
        self.auth_method
    }

    async fn handle_scram_password_message<C>(
        &self,
        client: &mut C,
        mut msg: PasswordMessageFamily,
    ) -> PgWireResult<()>
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

        if matches!(new_state, SASLState::Finished) {
            finish_authentication(client, &self.parameter_provider).await?;
        } else {
            self.sasl_registry.write().insert(peer_addr, new_state);
        }
        Ok(())
    }

    /// Build a query response from executor results for a single ExecResult.
    ///
    /// Performance: For small result sets (≤10 rows, typical of point queries),
    /// pre-encodes all rows into a Vec to avoid per-row Arc::clone overhead
    /// and lazy stream allocation. This reduces protocol overhead for the
    /// common OLTP case. Uses binary encoding for numeric types (Int32, Int64,
    /// Float64, Bool) to avoid text conversion overhead.
    fn build_response(result: ExecResult) -> PgWireResult<Response> {
        match result {
            ExecResult::Select { columns, rows } => {
                let schema: Vec<FieldInfo> = columns
                    .iter()
                    .map(|(name, dt)| {
                        FieldInfo::new(
                            name.clone(),
                            None,
                            None,
                            data_type_to_pg(dt),
                            // Use binary format for numeric types to avoid
                            // text conversion (e.g., 12345 → "12345"). Binary
                            // encoding is faster and produces fewer bytes.
                            data_type_field_format(dt),
                        )
                    })
                    .collect();
                let schema = Arc::new(schema);

                // Fast path for small result sets (≤10 rows): pre-encode all
                // rows into a Vec, avoiding per-row Arc::clone and lazy stream
                // overhead. This is the common case for point queries.
                if rows.len() <= 10 {
                    let mut encoded = Vec::with_capacity(rows.len());
                    for row in &rows {
                        let mut encoder = DataRowEncoder::new(Arc::clone(&schema));
                        for value in row {
                            encode_value(&mut encoder, value)?;
                        }
                        encoded.push(encoder.finish()?);
                    }
                    let data_row_stream = stream::iter(encoded.into_iter().map(Ok));
                    Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
                } else {
                    let schema_ref = Arc::clone(&schema);
                    let data_row_stream = stream::iter(rows).map(move |row| {
                        let mut encoder = DataRowEncoder::new(Arc::clone(&schema_ref));
                        for value in &row {
                            encode_value(&mut encoder, value)?;
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
                Ok(Response::Execution(Tag::new(wire_tag).with_rows(rows_affected)))
            }
            ExecResult::CopyOut { row_count, .. } => {
                Ok(Response::Execution(Tag::new("COPY").with_rows(row_count)))
            }
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

        let fut = self
            .executor
            .execute_with_session(session_id, sql);

        // Per-session statement_timeout overrides the global default.
        // SET statement_timeout = N (seconds) to configure per-connection.
        let timeout_secs = self.executor
            .get_session_setting(session_id, "statement_timeout")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(self.statement_timeout_secs);

        if timeout_secs > 0 {
            match tokio::time::timeout(
                std::time::Duration::from_secs(timeout_secs),
                fut,
            )
            .await
            {
                Ok(result) => result.map_err(exec_error_to_pgwire),
                Err(_elapsed) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "57014".to_owned(), // query_canceled
                    format!(
                        "canceling statement due to statement timeout ({timeout_secs}s)",
                    ),
                )))),
            }
        } else {
            fut.await.map_err(exec_error_to_pgwire)
        }
    }

    /// Execute a SQL query using the default session (for internal/test use).
    async fn execute_sql(&self, sql: &str) -> PgWireResult<Vec<ExecResult>> {
        self.execute_sql_session(0, sql).await
    }

    /// Infer parameter types from SQL placeholders.
    ///
    /// This does basic analysis: counts `$N` placeholders and returns TEXT
    /// for each, since Nucleus does text-based parameter substitution.
    /// If explicit types were provided in the Parse message, those are used
    /// instead.
    fn infer_parameter_types(sql: &str, declared: &[Option<Type>]) -> Vec<Type> {
        // Count the number of $N placeholders to determine parameter count.
        let param_count = count_placeholders(sql);
        let count = param_count.max(declared.len());

        (0..count)
            .map(|i| {
                declared
                    .get(i)
                    .and_then(|t| t.clone())
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
        let param_count = portal.parameter_len();
        let mut replacements = Vec::with_capacity(param_count);

        for i in 0..param_count {
            let type_hint = portal
                .statement
                .parameter_types
                .get(i)
                .and_then(|t| t.clone())
                .unwrap_or(Type::TEXT);

            let value_str: String = match portal.parameter::<String>(i, &type_hint) {
                Ok(Some(s)) => s,
                Ok(None) => "NULL".to_owned(),
                Err(_) => {
                    // Fall back: try to read raw bytes as UTF-8.
                    match &portal.parameters[i] {
                        Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                        None => "NULL".to_owned(),
                    }
                }
            };

            // Quote the value for SQL injection safety, unless it's NULL.
            // Strip NUL bytes, escape backslashes and single quotes.
            let replacement = if value_str == "NULL" {
                "NULL".to_owned()
            } else {
                format!("'{}'", sanitize_sql_text_literal(&value_str))
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
        executor: &'a Executor,
        session_id: u64,
        cached_ast: &[sqlparser::ast::Statement],
        portal: &Portal<ParsedStatement>,
    ) -> Result<std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ExecResult>, ExecError>> + Send + 'a>>, ()> {
        let param_count = portal.parameter_len();
        let mut param_values = Vec::with_capacity(param_count);

        for i in 0..param_count {
            let type_hint = portal
                .statement
                .parameter_types
                .get(i)
                .and_then(|t| t.clone())
                .unwrap_or(Type::TEXT);

            let value = match portal.parameter::<String>(i, &type_hint) {
                Ok(None) => Value::Null,
                Ok(Some(s)) => Self::pg_string_to_value(&s, &type_hint),
                Err(_) => {
                    match &portal.parameters[i] {
                        Some(bytes) => {
                            let s = String::from_utf8_lossy(bytes).into_owned();
                            Self::pg_string_to_value(&s, &type_hint)
                        }
                        None => Value::Null,
                    }
                }
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
            Type::INT2 | Type::INT4 => s.parse::<i32>().map(Value::Int32).unwrap_or(Value::Text(s.to_owned())),
            Type::INT8 => s.parse::<i64>().map(Value::Int64).unwrap_or(Value::Text(s.to_owned())),
            Type::FLOAT4 | Type::FLOAT8 => s.parse::<f64>().map(Value::Float64).unwrap_or(Value::Text(s.to_owned())),
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
    value
        .replace('\0', "")
        .replace('\\', "\\\\")
        .replace('\'', "''")
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
                let session_id = self.executor.create_session();
                let addr = client.socket_addr().to_string();
                self.session_registry.write().insert(addr, session_id);

                if self.authenticator.is_none() {
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
                if let Some(auth) = &self.authenticator {
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

                    let result = match self.auth_method {
                        AuthMethod::Cleartext => {
                            let pwd = pwd.into_password()?;
                            let login_info = LoginInfo::from_client_info(client);
                            let expected = auth.get_password(&login_info).await?;
                            if constant_time_eq(expected.password(), pwd.password.as_bytes()) {
                                finish_authentication(client, &self.parameter_provider).await
                            } else {
                                let user =
                                    login_info.user().map(|u| u.to_owned()).unwrap_or_default();
                                Err(PgWireError::InvalidPassword(user))
                            }
                        }
                        AuthMethod::ScramSha256 => {
                            self.handle_scram_password_message(client, pwd).await
                        }
                    };

                    if let Err(e) = result {
                        self.login_rate_limiter.record_failure(source_ip);
                        self.cleanup_session(&client.socket_addr().to_string());
                        return Err(e);
                    }
                    // Successful auth: clear any prior failure record.
                    self.login_rate_limiter.clear(source_ip);
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

        // ── Large Objects fast path: intercept lo_* function calls ───────
        if let Some(lo_result) = self.try_handle_large_object(&peer_addr_str, query) {
            let resp = Self::build_response(lo_result)?;
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
        if let Some(kv_cmd) = kv_fast_path::try_parse_kv(query) {
            let result = kv_fast_path::execute_kv_command(&kv_cmd, self.executor.kv_store());
            self.flush_pending_notifications(client).await?;
            return Ok(vec![Self::build_response(result)?]);
        }

        // ── SQL OLTP fast path: intercept simple point queries/mutations ──
        if let Some(sql_cmd) = kv_fast_path::try_parse_sql_fast_path(query)
            && let Some(result) = self.executor.execute_sql_fast_path(&sql_cmd).await {
                self.flush_pending_notifications(client).await?;
                return Ok(vec![Self::build_response(result.map_err(exec_error_to_pgwire)?)?]);
            }
            // Fall through to normal path if fast-path couldn't handle it
            // (e.g. table not found in cache, column mismatch, etc.)

        let session_id = self.session_id_from_client(client);

        // Detect COPY ... FROM STDIN and enter copy-in mode.
        if let Some(copy_info) = detect_copy_from_stdin(query) {
            let peer_addr = client.socket_addr();
            self.copy_state.lock().insert(peer_addr, CopyInProgress {
                table: copy_info.table,
                columns: copy_info.columns,
                delimiter: copy_info.delimiter,
                is_csv: copy_info.is_csv,
                has_header: copy_info.has_header,
                data: Vec::new(),
                session_id,
            });
            return Ok(vec![Response::CopyIn(CopyResponse::new(0, 0, vec![]))]);
        }

        let results = self.execute_sql_session(session_id, query).await?;

        let mut responses = Vec::new();
        let mut bytes_estimate: u64 = 0;
        for result in results {
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
                client.send(PgWireBackendMessage::CopyDone(CopyDone::new())).await?;
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
            // Approximate wire bytes: count rows * avg 64 bytes per row + header
            bytes_estimate += Self::estimate_result_bytes(&result);
            responses.push(Self::build_response(result)?);
        }
        if bytes_estimate > 0 {
            self.executor.metrics().bytes_sent.inc_by(bytes_estimate);
        }

        // ── Flush pending notifications before ReadyForQuery ────────────
        self.flush_pending_notifications(client).await?;

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
        let param_types = Self::infer_parameter_types(sql, &stmt.parameter_types);

        // Try to determine result columns by examining the query.
        // For SELECT statements, we can execute with dummy values to get the
        // schema. For non-SELECT statements, return no data.
        let fields = if is_select_query(sql) {
            match self.describe_select_columns(sql).await {
                Ok(cols) => cols,
                Err(e) => {
                    tracing::warn!("Failed to describe SELECT columns: {e}");
                    Vec::new()
                }
            }
        } else {
            Vec::new()
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
            let substituted = Self::substitute_parameters(sql, portal)?;
            match self.describe_select_columns(&substituted).await {
                Ok(cols) => cols,
                Err(e) => {
                    tracing::warn!("Failed to describe SELECT columns: {e}");
                    Vec::new()
                }
            }
        } else {
            Vec::new()
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
        let peer_addr_str = client.socket_addr().to_string();

        // ── Large Objects fast path (extended query) ────────────────────
        if let Some(lo_result) = self.try_handle_large_object(&peer_addr_str, &parsed_stmt.sql) {
            self.flush_pending_notifications(client).await?;
            return Self::build_response(lo_result);
        }

        // ── LISTEN/NOTIFY wire-level registration (extended query) ──────
        {
            let trimmed_upper = parsed_stmt.sql.trim().to_uppercase();
            if trimmed_upper.starts_with("LISTEN ") {
                let channel = parsed_stmt.sql.trim()[7..].trim().trim_end_matches(';').trim();
                self.handle_listen(&peer_addr_str, channel);
            } else if trimmed_upper.starts_with("UNLISTEN ") {
                let channel = parsed_stmt.sql.trim()[9..].trim().trim_end_matches(';').trim();
                self.handle_unlisten(&peer_addr_str, channel);
            } else if trimmed_upper.starts_with("NOTIFY ") {
                let rest = parsed_stmt.sql.trim()[7..].trim().trim_end_matches(';').trim();
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
            let resolved_sql = Self::substitute_parameters(&parsed_stmt.sql, portal)?;
            self.execute_sql_session(session_id, &resolved_sql).await
        }?;

        // The extended protocol returns a single Response per Execute.
        // If there are multiple statements, take the last result.
        if let Some(mut result) = results.into_iter().last() {
            // Respect max_rows from the Execute message. When max_rows > 0,
            // the client only wants that many rows. (Full cursor/PortalSuspended
            // support would require pgwire to expose that response variant.)
            if max_rows > 0
                && let ExecResult::Select { ref mut rows, .. } = result {
                    rows.truncate(max_rows);
                }
            let bytes_est = Self::estimate_result_bytes(&result);
            if bytes_est > 0 {
                self.executor.metrics().bytes_sent.inc_by(bytes_est);
            }
            // Flush pending notifications before the response (before ReadyForQuery).
            self.flush_pending_notifications(client).await?;
            Self::build_response(result)
        } else {
            self.flush_pending_notifications(client).await?;
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
        let peer_addr = client.socket_addr();
        if let Some(state) = self.copy_state.lock().get_mut(&peer_addr) {
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
        let Some(state) = state else { return Ok(()); };

        let rows = parse_copy_rows(&state.data, state.delimiter, state.is_csv, state.has_header);
        let row_count = rows.len();

        // Insert in batches of 500 rows.
        const BATCH: usize = 500;
        for chunk in rows.chunks(BATCH) {
            if chunk.is_empty() { continue; }
            let col_clause = match &state.columns {
                Some(cols) => format!(
                    " ({})",
                    cols.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", ")
                ),
                None => String::new(),
            };
            let mut sql = format!("INSERT INTO {}{} VALUES ", state.table, col_clause);
            let mut first_row = true;
            for row_fields in chunk {
                if !first_row { sql.push_str(", "); }
                first_row = false;
                sql.push('(');
                for (i, val) in row_fields.iter().enumerate() {
                    if i > 0 { sql.push_str(", "); }
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
        self.connection_pids.write().remove(peer_addr);
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
        self.connection_pids.write().insert(peer_addr.to_string(), pid);
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
                args_str.split(',').map(|a| a.trim().trim_matches('\'')).collect()
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
        let state = map.entry(peer_addr.to_string()).or_insert_with(LargeObjectState::new);
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
        self.executor
            .blob_store_put(&desc.key, &existing, None);
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
    async fn describe_select_columns(&self, sql: &str) -> Result<Vec<FieldInfo>, PgWireError> {
        // Strip trailing semicolons and whitespace, wrap in LIMIT 0 subquery.
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let probe_sql = format!("SELECT * FROM ({trimmed}) AS __describe_probe LIMIT 0");

        match self.execute_sql(&probe_sql).await {
            Ok(results) => {
                for result in results {
                    if let ExecResult::Select { columns, .. } = result {
                        return Ok(columns
                            .iter()
                            .map(|(name, dt)| {
                                FieldInfo::new(
                                    name.clone(),
                                    None,
                                    None,
                                    data_type_to_pg(dt),
                                    FieldFormat::Text,
                                )
                            })
                            .collect());
                    }
                }
                Ok(Vec::new())
            }
            Err(_) => Ok(Vec::new()),
        }
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

/// Choose the wire format for a given data type.
///
/// Numeric types (Bool, Int32, Int64, Float64) use binary encoding to avoid
/// the overhead of text conversion (e.g., integer 12345 → "12345" → parse).
/// All other types use text format for maximum compatibility.
fn data_type_field_format(dt: &DataType) -> FieldFormat {
    match dt {
        DataType::Bool | DataType::Int32 | DataType::Int64 | DataType::Float64 => {
            FieldFormat::Binary
        }
        _ => FieldFormat::Text,
    }
}

/// Encode a Nucleus Value into a pgwire DataRowEncoder field.
fn encode_value(encoder: &mut DataRowEncoder, value: &Value) -> PgWireResult<()> {
    match value {
        Value::Null => encoder.encode_field(&None::<&str>),
        Value::Bool(b) => encoder.encode_field(&Some(*b)),
        Value::Int32(n) => encoder.encode_field(&Some(*n)),
        Value::Int64(n) => encoder.encode_field(&Some(*n)),
        Value::Float64(n) => encoder.encode_field(&Some(*n)),
        Value::Text(s) => encoder.encode_field(&Some(s.as_str())),
        Value::Jsonb(v) => encoder.encode_field(&Some(v.to_string().as_str())),
        // New types: encode as text representation for wire protocol
        Value::Date(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_)
        | Value::Numeric(_)
        | Value::Uuid(_)
        | Value::Bytea(_)
        | Value::Array(_)
        | Value::Vector(_)
        | Value::Interval { .. } => encoder.encode_field(&Some(value.to_string().as_str())),
    }
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

    let Statement::Copy { source, to: false, target: CopyTarget::Stdin, options, .. } = stmt
    else {
        return None;
    };
    let CopySource::Table { table_name, columns } = source else { return None; };

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
    let mut has_header = false;

    for opt in options {
        match opt {
            CopyOption::Format(f) if f.value.to_uppercase() == "CSV" => {
                is_csv = true;
                delimiter = b',';
            }
            CopyOption::Delimiter(d) => delimiter = d as u8,
            CopyOption::Header(h) => has_header = h,
            _ => {}
        }
    }

    Some(CopyInfo { table, columns: col_names, delimiter, is_csv, has_header })
}

/// Parse accumulated COPY data bytes into rows of optional string fields.
fn parse_copy_rows(data: &[u8], delimiter: u8, is_csv: bool, has_header: bool) -> Vec<Vec<Option<String>>> {
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
                    result.push(if current.is_empty() { None } else { Some(current) });
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
                        result.push(if current.is_empty() { None } else { Some(current.clone()) });
                        current.clear();
                    }
                }
                Some(c) if c == delim => {
                    result.push(if current.is_empty() { None } else { Some(current.clone()) });
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
                Some(ch) => { result.push('\\'); result.push(ch); }
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
    use super::*;

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
    async fn auth_source_returns_password_for_valid_user() {
        let auth = UserAuthenticator::new("nucleus", "mypass");
        let login = LoginInfo::new(Some("nucleus"), None, "127.0.0.1".into());
        let result = auth.get_password(&login).await;
        assert!(result.is_ok());
        let pw = result.unwrap();
        assert_eq!(pw.password(), b"mypass");
        assert!(pw.salt().is_none());
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
        Arc::new(Executor::new(catalog, storage))
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
        let auth = UserAuthenticator::new("nucleus", "nucleus");
        let login = LoginInfo::new(Some("nucleus"), None, "127.0.0.1".into());
        let expected = auth.get_password(&login).await.unwrap();
        // Simulate what the wire handler does: compare expected vs incoming bytes
        assert_eq!(expected.password(), b"nucleus");
        assert_eq!(expected.password(), "nucleus".as_bytes());
    }

    #[tokio::test]
    async fn password_bytes_mismatch_detected() {
        let auth = UserAuthenticator::new("nucleus", "correct");
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
        let response = NucleusHandler::build_response(result);
        assert!(response.is_ok());
        match response.unwrap() {
            Response::Query(_) => {} // Expected
            _ => panic!("Expected Query response"),
        }
    }

    #[tokio::test]
    async fn extended_query_build_response_command() {
        let result = ExecResult::Command {
            tag: "INSERT".to_string(),
            rows_affected: 3,
        };
        let response = NucleusHandler::build_response(result);
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
        let response = NucleusHandler::build_response(result);
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn build_response_command_zero_rows() {
        let result = ExecResult::Command {
            tag: "DELETE".to_string(),
            rows_affected: 0,
        };
        let response = NucleusHandler::build_response(result);
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
        let response = NucleusHandler::build_response(result);
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
        let response = NucleusHandler::build_response(result);
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
        Arc::new(Executor::new(catalog, storage))
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
    fn parameter_substitution_escapes_backslashes() {
        let result = NucleusHandler::substitute_parameters_raw("SELECT $1", &["back\\slash"]);
        assert_eq!(
            result, "SELECT 'back\\\\slash'",
            "Backslashes in parameter values must be doubled"
        );
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
            "COPY orders (id, amount) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')"
        ).unwrap();
        assert_eq!(info.table, "orders");
        assert_eq!(info.columns.as_deref(), Some(&["id".to_owned(), "amount".to_owned()][..]));
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
        assert!(limiter.is_locked_out(ip), "should be locked out after max failures");
    }

    #[test]
    fn rate_limiter_does_not_lock_below_threshold() {
        let limiter = LoginRateLimiter::new();
        let ip: IpAddr = "10.0.0.2".parse().unwrap();
        for _ in 0..(LoginRateLimiter::MAX_FAILED_ATTEMPTS - 1) {
            limiter.record_failure(ip);
        }
        assert!(!limiter.is_locked_out(ip), "should not lock out below threshold");
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
        assert!(!limiter.is_locked_out(ip), "should not be locked out after clear");
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
        assert!(!limiter.is_locked_out(ip_b), "unrelated IP should not be locked out");
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
        assert!(handler.try_handle_large_object("peer1", "SELECT 1").is_none());
        assert!(handler.try_handle_large_object("peer1", "INSERT INTO t VALUES (1)").is_none());
        assert!(handler.try_handle_large_object("peer1", "SELECT lower('X')").is_none());
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
}
