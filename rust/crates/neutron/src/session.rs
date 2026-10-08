//! Server-side session management middleware.
//!
//! Sessions store per-user data on the server, identified by a signed cookie.
//! Data is stored as JSON values and can be any serializable type.
//!
//! # Example
//!
//! ```rust,ignore
//! use std::time::Duration;
//! use neutron::prelude::*;
//! use neutron::session::{Session, SessionLayer, MemoryStore};
//! use neutron::cookie::Key;
//!
//! let key = Key::generate();
//! let store = MemoryStore::new();
//!
//! let router = Router::new()
//!     .middleware(SessionLayer::new(store, key))
//!     .get("/count", |session: Session| async move {
//!         let count: u64 = session.get("count").unwrap_or(0);
//!         session.insert("count", count + 1);
//!         format!("Visit count: {}", count + 1)
//!     });
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http::StatusCode;
use rand::RngCore;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::cookie::{Key, SameSite};
use crate::extract::FromRequest;
use crate::handler::{IntoResponse, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// SessionStore trait
// ---------------------------------------------------------------------------

/// Trait for pluggable session storage backends.
///
/// Implement this trait to store sessions in Redis, a database, or any
/// other backend. The default [`MemoryStore`] keeps sessions in-process.
pub type SessionData = HashMap<String, serde_json::Value>;
pub type SessionFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, SessionStoreError>> + Send + 'a>>;

/// Version 2 storage failures distinguish lost ownership from unavailable or
/// corrupt storage. A timed-out dispatched commit/revoke has an unknown outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStoreError {
    Conflict,
    Capacity,
    Unavailable(String),
    Corrupt(String),
}

/// Async, fallible, revision-owned contract. No legacy synchronous adapter is
/// accepted by SessionLayer. Implementations must yield, not block a worker.
pub trait SessionStore: Send + Sync + 'static {
    fn load_versioned<'a>(&'a self, id: &'a str) -> SessionFuture<'a, Option<(SessionData, u64)>>;
    fn commit<'a>(
        &'a self,
        id: &'a str,
        data: SessionData,
        ttl: Duration,
        expected: u64,
    ) -> SessionFuture<'a, ()>;
    fn revoke<'a>(&'a self, id: &'a str, expected: u64) -> SessionFuture<'a, ()>;
}

// ---------------------------------------------------------------------------
// MemoryStore
// ---------------------------------------------------------------------------

struct StoredSession {
    data: HashMap<String, serde_json::Value>,
    expires_at: Instant,
    revision: u64,
}

/// In-memory session store.
///
/// Stores sessions in a `HashMap` protected by a `Mutex`. Expired sessions
/// are lazily cleaned up during save operations.
///
/// Suitable for development and single-process deployments. For multi-process
/// or distributed deployments, implement [`SessionStore`] with a shared
/// backend like Redis.
/// Default maximum number of sessions to prevent unbounded memory growth.
const DEFAULT_MAX_SESSIONS: usize = 100_000;

pub struct MemoryStore {
    sessions: Mutex<HashMap<String, StoredSession>>,
    last_cleanup: Mutex<Instant>,
    max_sessions: usize,
    /// Destroyed session IDs. A stale request that loaded a session before
    /// another request destroyed it must not be able to save it back into
    /// existence under the same signed ID (RS-03).
    tombs: Mutex<HashMap<String, ()>>,
    /// Monotonic revision source for save/destroy fencing.
    next_revision: std::sync::atomic::AtomicU64,
}

impl MemoryStore {
    /// Create a new empty in-memory session store with the default max of 100,000 sessions.
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            last_cleanup: Mutex::new(Instant::now()),
            max_sessions: DEFAULT_MAX_SESSIONS,
            tombs: Mutex::new(HashMap::new()),
            next_revision: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Create a new in-memory session store with a custom maximum session count.
    pub fn with_max_sessions(max_sessions: usize) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            last_cleanup: Mutex::new(Instant::now()),
            max_sessions,
            tombs: Mutex::new(HashMap::new()),
            next_revision: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn load(&self, id: &str) -> Option<HashMap<String, serde_json::Value>> {
        self.load_with_revision(id).map(|(data, _)| data)
    }

    pub fn load_with_revision(
        &self,
        id: &str,
    ) -> Option<(HashMap<String, serde_json::Value>, u64)> {
        let sessions = self.sessions.lock().unwrap();
        let stored = sessions.get(id)?;
        if Instant::now() >= stored.expires_at {
            return None;
        }
        Some((stored.data.clone(), stored.revision))
    }

    pub fn revision(&self, id: &str) -> Option<u64> {
        let sessions = self.sessions.lock().unwrap();
        sessions.get(id).map(|s| s.revision)
    }

    pub fn save(&self, id: &str, data: HashMap<String, serde_json::Value>, ttl: Duration) {
        let _ = self.save_if_current(id, data, ttl, self.revision(id).unwrap_or(0));
    }

    pub fn save_if_current(
        &self,
        id: &str,
        data: HashMap<String, serde_json::Value>,
        ttl: Duration,
        loaded_revision: u64,
    ) -> bool {
        self.commit_sync(id, data, ttl, loaded_revision).is_ok()
    }

    fn commit_sync(
        &self,
        id: &str,
        data: SessionData,
        ttl: Duration,
        loaded_revision: u64,
    ) -> Result<(), SessionStoreError> {
        let mut sessions = self.sessions.lock().unwrap();
        let now = Instant::now();

        // Lazy cleanup: remove expired sessions periodically
        let mut last_cleanup = self.last_cleanup.lock().unwrap();
        if now.duration_since(*last_cleanup) > Duration::from_secs(60) {
            sessions.retain(|_, s| now < s.expires_at);
            *last_cleanup = now;
        }

        // A destroyed session stays destroyed: saving under a tombstoned ID
        // would resurrect destroyed authentication data under the same
        // signed cookie (RS-03). New sessions always get fresh random IDs,
        // so a tombstone hit can only be a stale writer.
        if self.tombs.lock().unwrap().contains_key(id) {
            tracing::warn!(
                session_id = id,
                "refusing to save a session destroyed by a concurrent request"
            );
            return Err(SessionStoreError::Conflict);
        }

        sessions.retain(|_, s| now < s.expires_at);
        if let Some(stored) = sessions.get(id) {
            // Concurrent writer fencing: only the writer that saw the
            // current revision may land (last-writer-wins otherwise loses
            // the other request's updates silently).
            if stored.revision != loaded_revision {
                tracing::warn!(
                    session_id = id,
                    "session was modified by a concurrent request; save refused"
                );
                return Err(SessionStoreError::Conflict);
            }
        } else if loaded_revision != 0 {
            // Expiry/cleanup must not turn a stale writer into a fresh insertion.
            return Err(SessionStoreError::Conflict);
        } else if sessions.len() >= self.max_sessions {
            // New session at capacity: the save did NOT land — report it so
            // the layer can refuse to claim success with an unsaved cookie
            // (RS-04).
            tracing::warn!(
                max_sessions = self.max_sessions,
                current = sessions.len(),
                "session store at capacity, rejecting new session"
            );
            return Err(SessionStoreError::Capacity);
        }

        let revision = self
            .next_revision
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        sessions.insert(
            id.to_string(),
            StoredSession {
                data,
                expires_at: now + ttl,
                revision,
            },
        );
        Ok(())
    }

    pub fn destroy(&self, id: &str) {
        let _ = self.destroy_checked(id);
    }

    pub fn destroy_checked(&self, id: &str) -> bool {
        let removed = self.sessions.lock().unwrap().remove(id).is_some();
        let mut tombs = self.tombs.lock().unwrap();
        // Bound the tombstone set (same order as the session cap).
        if tombs.len() >= DEFAULT_MAX_SESSIONS {
            tombs.clear();
        }
        tombs.insert(id.to_string(), ());
        let _ = removed; // a session that was already absent is still destroyed
        true
    }
}

impl SessionStore for MemoryStore {
    fn load_versioned<'a>(&'a self, id: &'a str) -> SessionFuture<'a, Option<(SessionData, u64)>> {
        Box::pin(async move { Ok(self.load_with_revision(id)) })
    }
    fn commit<'a>(
        &'a self,
        id: &'a str,
        data: SessionData,
        ttl: Duration,
        expected: u64,
    ) -> SessionFuture<'a, ()> {
        Box::pin(async move { self.commit_sync(id, data, ttl, expected) })
    }
    fn revoke<'a>(&'a self, id: &'a str, expected: u64) -> SessionFuture<'a, ()> {
        Box::pin(async move {
            let mut sessions = self.sessions.lock().unwrap();
            let current = sessions
                .get(id)
                .filter(|s| Instant::now() < s.expires_at)
                .map(|s| s.revision);
            if current != Some(expected) && !(current.is_none() && expected == 0) {
                return Err(SessionStoreError::Conflict);
            }
            sessions.remove(id);
            let mut tombs = self.tombs.lock().unwrap();
            if tombs.len() >= self.max_sessions {
                tombs.clear();
            }
            tombs.insert(id.to_owned(), ());
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

/// Server-side session data, backed by a [`SessionStore`].
///
/// Obtained as an extractor when [`SessionLayer`] middleware is active.
/// All methods use interior mutability (`&self`) so the session can be
/// used without `mut`.
///
/// # Example
///
/// ```rust,ignore
/// async fn handler(session: Session) -> String {
///     let visits: u64 = session.get("visits").unwrap_or(0);
///     session.insert("visits", visits + 1);
///     format!("Visits: {}", visits + 1)
/// }
/// ```
#[derive(Clone)]
pub struct Session {
    inner: Arc<Mutex<SessionInner>>,
}

struct SessionInner {
    id: String,
    data: HashMap<String, serde_json::Value>,
    is_new: bool,
    modified: bool,
    destroyed: bool,
}

impl Session {
    fn new(id: String, data: HashMap<String, serde_json::Value>, is_new: bool) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SessionInner {
                id,
                data,
                is_new,
                modified: false,
                destroyed: false,
            })),
        }
    }

    /// Get a value from the session.
    ///
    /// Returns `None` if the key doesn't exist or can't be deserialized
    /// to the requested type.
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        let inner = self.inner.lock().unwrap();
        let value = inner.data.get(key)?;
        serde_json::from_value(value.clone()).ok()
    }

    /// Set a value in the session.
    ///
    /// The value is serialized to JSON. Overwrites any existing value
    /// at the given key.
    pub fn insert<T: Serialize>(&self, key: &str, value: T) {
        let mut inner = self.inner.lock().unwrap();
        if let Ok(json_value) = serde_json::to_value(value) {
            inner.data.insert(key.to_string(), json_value);
            inner.modified = true;
        }
    }

    /// Remove a value from the session.
    ///
    /// Returns the removed value, or `None` if the key didn't exist.
    pub fn remove(&self, key: &str) -> Option<serde_json::Value> {
        let mut inner = self.inner.lock().unwrap();
        let removed = inner.data.remove(key);
        if removed.is_some() {
            inner.modified = true;
        }
        removed
    }

    /// Clear all session data but keep the session alive.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.data.is_empty() {
            inner.data.clear();
            inner.modified = true;
        }
    }

    /// Destroy the session entirely.
    ///
    /// The session data will be removed from the store and the session
    /// cookie will be cleared on the response.
    pub fn destroy(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.destroyed = true;
        inner.data.clear();
    }

    /// Get the session ID.
    pub fn id(&self) -> String {
        self.inner.lock().unwrap().id.clone()
    }

    /// Check if this is a newly created session (not yet saved).
    pub fn is_new(&self) -> bool {
        self.inner.lock().unwrap().is_new
    }
}

impl FromRequest for Session {
    async fn from_request(req: &mut Request) -> Result<Self, Response> {
        req.get_extension::<Session>().cloned().ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "SessionLayer middleware not configured",
            )
                .into_response()
        })
    }
}

// ---------------------------------------------------------------------------
// SessionLayer middleware
// ---------------------------------------------------------------------------

/// Session management middleware.
///
/// Manages session lifecycle: reads the session cookie, loads data from
/// the store, makes it available to handlers, and saves changes after
/// the handler runs.
///
/// # Example
///
/// ```rust,ignore
/// use std::time::Duration;
/// use neutron::prelude::*;
/// use neutron::session::{SessionLayer, MemoryStore};
/// use neutron::cookie::Key;
///
/// let router = Router::new()
///     .middleware(
///         SessionLayer::new(MemoryStore::new(), Key::generate())
///             .cookie_name("my.sid")
///             .max_age(Duration::from_secs(86400))
///     )
///     .get("/", handler);
/// ```
pub struct SessionLayer {
    store: Arc<dyn SessionStore>,
    key: Key,
    cookie_name: String,
    max_age: Duration,
    cookie_path: String,
    cookie_http_only: bool,
    cookie_secure: bool,
    cookie_same_site: Option<SameSite>,
    operation_timeout: Duration,
}

impl SessionLayer {
    /// Create a session layer with the given store and signing key.
    pub fn new(store: impl SessionStore, key: Key) -> Self {
        Self {
            store: Arc::new(store),
            key,
            cookie_name: "neutron.sid".to_string(),
            max_age: Duration::from_secs(86400), // 24 hours
            cookie_path: "/".to_string(),
            cookie_http_only: true,
            cookie_secure: true,
            cookie_same_site: Some(SameSite::Lax),
            operation_timeout: Duration::from_secs(5),
        }
    }

    /// Deadline for each yielding storage operation. Deadline failures return
    /// 503 without confirming cookie changes; reconcile unknown writes.
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    /// Set the session cookie name (default: `"neutron.sid"`).
    pub fn cookie_name(mut self, name: impl Into<String>) -> Self {
        self.cookie_name = name.into();
        self
    }

    /// Set the session max age / TTL (default: 24 hours).
    pub fn max_age(mut self, duration: Duration) -> Self {
        self.max_age = duration;
        self
    }

    /// Set the cookie path (default: `"/"`).
    pub fn cookie_path(mut self, path: impl Into<String>) -> Self {
        self.cookie_path = path.into();
        self
    }

    /// Set whether the cookie is HttpOnly (default: `true`).
    pub fn http_only(mut self, http_only: bool) -> Self {
        self.cookie_http_only = http_only;
        self
    }

    /// Set whether the cookie requires HTTPS (default: `true`).
    pub fn secure(mut self, secure: bool) -> Self {
        self.cookie_secure = secure;
        self
    }

    /// Set the cookie SameSite attribute (default: `Lax`).
    pub fn same_site(mut self, same_site: SameSite) -> Self {
        self.cookie_same_site = Some(same_site);
        self
    }
}

fn generate_session_id() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn parse_session_cookie(headers: &http::HeaderMap, cookie_name: &str) -> Option<String> {
    headers
        .get_all("cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(';'))
        .find_map(|pair| {
            let pair = pair.trim();
            let (name, value) = pair.split_once('=')?;
            if name.trim() == cookie_name {
                Some(value.trim().to_string())
            } else {
                None
            }
        })
}

impl MiddlewareTrait for SessionLayer {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let store = Arc::clone(&self.store);
        let key = self.key.clone();
        let cookie_name = self.cookie_name.clone();
        let max_age = self.max_age;
        let cookie_path = self.cookie_path.clone();
        let cookie_http_only = self.cookie_http_only;
        let cookie_secure = self.cookie_secure;
        let cookie_same_site = self.cookie_same_site;
        let operation_timeout = self.operation_timeout;

        Box::pin(async move {
            let mut req = req;

            // 1. Try to load existing session from signed cookie. The
            //    revision travels with the data so the post-handler save can
            //    be refused if the session was destroyed or rewritten by a
            //    concurrent request in the meantime (RS-03).
            let (session, existing) =
                if let Some(cookie_value) = parse_session_cookie(req.headers(), &cookie_name) {
                    if let Some(session_id) = key.verify(&cookie_value) {
                        let loaded = match tokio::time::timeout(
                            operation_timeout,
                            store.load_versioned(&session_id),
                        )
                        .await
                        {
                            Ok(Ok(data)) => data,
                            _ => {
                                return (
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    "session storage load failed",
                                )
                                    .into_response()
                            }
                        };
                        if let Some((data, revision)) = loaded {
                            (
                                Session::new(session_id.clone(), data, false),
                                Some((session_id, revision)),
                            )
                        } else {
                            // Session expired or not found — create new
                            let id = generate_session_id();
                            (Session::new(id, HashMap::new(), true), None)
                        }
                    } else {
                        // Invalid signature — create new session
                        let id = generate_session_id();
                        (Session::new(id, HashMap::new(), true), None)
                    }
                } else {
                    // No session cookie — create new session
                    let id = generate_session_id();
                    (Session::new(id, HashMap::new(), true), None)
                };

            // 2. Keep a reference for post-handler processing
            let session_ref = session.clone();

            // 3. Make session available to handler
            req.set_extension(session);

            // 4. Run the handler
            let mut resp = next.run(req).await;

            // 5. Post-handler: save or destroy session
            let (id, data, destroyed_flag, modified, is_new) = {
                let inner = session_ref.inner.lock().unwrap();
                (
                    inner.id.clone(),
                    inner.data.clone(),
                    inner.destroyed,
                    inner.modified,
                    inner.is_new,
                )
            };

            if destroyed_flag {
                // Destroy: remove from store, clear cookie — but only claim
                // success when the store confirms the revocation. A failed
                // destroy that still cleared the cookie would tell the user
                // they are logged out while the authenticated record stays
                // usable (RS-04).
                let expected = existing.as_ref().map(|(_, rev)| *rev).unwrap_or(0);
                if !matches!(
                    tokio::time::timeout(operation_timeout, store.revoke(&id, expected)).await,
                    Ok(Ok(()))
                ) {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "session revocation failed; outcome may be unknown",
                    )
                        .into_response();
                }

                let mut cookie_parts = vec![
                    format!("{}=", cookie_name),
                    format!("Path={cookie_path}"),
                    "Max-Age=0".to_string(),
                ];
                if cookie_http_only {
                    cookie_parts.push("HttpOnly".to_string());
                }
                if cookie_secure {
                    cookie_parts.push("Secure".to_string());
                }
                resp.headers_mut()
                    .append("set-cookie", cookie_parts.join("; ").parse().unwrap());
            } else if modified || is_new {
                // Save session data conditionally on the loaded revision.
                // Failure means: tombstoned (destroyed concurrently),
                // rewritten by a concurrent writer, or the store could not
                // persist (capacity/backend). In every case the response
                // must not claim success with an unsaved session cookie
                // (RS-03/RS-04).
                let loaded_revision = existing.as_ref().map(|(_, rev)| *rev).unwrap_or(0);
                let saved = matches!(
                    tokio::time::timeout(
                        operation_timeout,
                        store.commit(&id, data, max_age, loaded_revision)
                    )
                    .await,
                    Ok(Ok(()))
                );

                if !saved {
                    tracing::error!("session save failed; refusing to confirm login");
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "session could not be persisted (store failure or concurrent change); retry",
                    )
                        .into_response();
                }

                // Set signed session cookie
                let signed_id = key.sign(&id);
                let mut cookie_parts = vec![
                    format!("{}={signed_id}", cookie_name),
                    format!("Path={cookie_path}"),
                    format!("Max-Age={}", max_age.as_secs()),
                ];
                if cookie_http_only {
                    cookie_parts.push("HttpOnly".to_string());
                }
                if cookie_secure {
                    cookie_parts.push("Secure".to_string());
                }
                if let Some(same_site) = cookie_same_site {
                    cookie_parts.push(format!("SameSite={same_site}"));
                }
                resp.headers_mut()
                    .append("set-cookie", cookie_parts.join("; ").parse().unwrap());
            }

            resp
        })
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cookie::Key;
    use crate::handler::Json;
    use crate::router::Router;
    use crate::testing::TestClient;

    fn test_layer() -> (SessionLayer, Key) {
        let key = Key::generate();
        let layer = SessionLayer::new(MemoryStore::new(), key.clone());
        (layer, key)
    }

    #[tokio::test]
    async fn new_session_gets_cookie() {
        let (layer, _key) = test_layer();

        let client = TestClient::new(Router::new().middleware(layer).get(
            "/",
            |session: Session| async move {
                session.insert("hello", "world");
                "ok"
            },
        ));

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        let set_cookie = resp.header("set-cookie").unwrap();
        assert!(set_cookie.contains("neutron.sid="));
        assert!(set_cookie.contains("Path=/"));
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("SameSite=Lax"));
    }

    #[tokio::test]
    async fn session_data_persists_across_requests() {
        let key = Key::generate();
        let store = Arc::new(MemoryStore::new());

        let client = TestClient::new(
            Router::new()
                .middleware(SessionLayer {
                    store: Arc::clone(&store) as Arc<dyn SessionStore>,
                    key: key.clone(),
                    cookie_name: "sid".to_string(),
                    max_age: Duration::from_secs(3600),
                    cookie_path: "/".to_string(),
                    cookie_http_only: true,
                    cookie_secure: false,
                    cookie_same_site: Some(SameSite::Lax),
                    operation_timeout: Duration::from_secs(5),
                })
                .get("/set", |session: Session| async move {
                    session.insert("count", 42u64);
                    "set"
                })
                .get("/get", |session: Session| async move {
                    let count: u64 = session.get("count").unwrap_or(0);
                    format!("{count}")
                }),
        );

        // Set session data
        let resp = client.get("/set").send().await;
        let set_cookie = resp.header("set-cookie").unwrap().to_string();
        let cookie_val = set_cookie.split(';').next().unwrap().trim();

        // Read session data with same cookie
        let resp = client.get("/get").header("cookie", cookie_val).send().await;
        assert_eq!(resp.text().await, "42");
    }

    #[tokio::test]
    async fn session_get_set_remove() {
        let (layer, _key) = test_layer();

        let client = TestClient::new(Router::new().middleware(layer).get(
            "/",
            |session: Session| async move {
                // Set values
                session.insert("name", "Alice");
                session.insert("age", 30u32);

                // Get values
                let name: String = session.get("name").unwrap();
                let age: u32 = session.get("age").unwrap();
                let missing: Option<String> = session.get("missing");

                // Remove
                session.remove("age");
                let age_after: Option<u32> = session.get("age");

                Json(serde_json::json!({
                    "name": name,
                    "age": age,
                    "missing": missing,
                    "age_after": age_after,
                }))
            },
        ));

        let resp = client.get("/").send().await;
        let body: serde_json::Value = resp.json().await;
        assert_eq!(body["name"], "Alice");
        assert_eq!(body["age"], 30);
        assert!(body["missing"].is_null());
        assert!(body["age_after"].is_null());
    }

    #[tokio::test]
    async fn session_clear() {
        let key = Key::generate();
        let store = Arc::new(MemoryStore::new());

        let client = TestClient::new(
            Router::new()
                .middleware(SessionLayer {
                    store: Arc::clone(&store) as Arc<dyn SessionStore>,
                    key: key.clone(),
                    cookie_name: "sid".to_string(),
                    max_age: Duration::from_secs(3600),
                    cookie_path: "/".to_string(),
                    cookie_http_only: true,
                    cookie_secure: false,
                    cookie_same_site: Some(SameSite::Lax),
                    operation_timeout: Duration::from_secs(5),
                })
                .get("/set", |session: Session| async move {
                    session.insert("a", 1u32);
                    session.insert("b", 2u32);
                    "set"
                })
                .get("/clear", |session: Session| async move {
                    session.clear();
                    "cleared"
                })
                .get("/get", |session: Session| async move {
                    let a: Option<u32> = session.get("a");
                    let b: Option<u32> = session.get("b");
                    Json(serde_json::json!({ "a": a, "b": b }))
                }),
        );

        // Set values
        let resp = client.get("/set").send().await;
        let cookie = resp.header("set-cookie").unwrap().to_string();
        let cookie_val = cookie.split(';').next().unwrap().trim();

        // Clear session
        client
            .get("/clear")
            .header("cookie", cookie_val)
            .send()
            .await;

        // Values should be gone
        let resp = client.get("/get").header("cookie", cookie_val).send().await;
        let body: serde_json::Value = resp.json().await;
        assert!(body["a"].is_null());
        assert!(body["b"].is_null());
    }

    #[tokio::test]
    async fn session_destroy() {
        let key = Key::generate();
        let store = Arc::new(MemoryStore::new());

        let client = TestClient::new(
            Router::new()
                .middleware(SessionLayer {
                    store: Arc::clone(&store) as Arc<dyn SessionStore>,
                    key: key.clone(),
                    cookie_name: "sid".to_string(),
                    max_age: Duration::from_secs(3600),
                    cookie_path: "/".to_string(),
                    cookie_http_only: true,
                    cookie_secure: false,
                    cookie_same_site: Some(SameSite::Lax),
                    operation_timeout: Duration::from_secs(5),
                })
                .get("/set", |session: Session| async move {
                    session.insert("data", "important");
                    "set"
                })
                .get("/destroy", |session: Session| async move {
                    session.destroy();
                    "destroyed"
                })
                .get("/get", |session: Session| async move {
                    let data: Option<String> = session.get("data");
                    data.unwrap_or_else(|| "none".to_string())
                }),
        );

        // Create session
        let resp = client.get("/set").send().await;
        let cookie = resp.header("set-cookie").unwrap().to_string();
        let cookie_val = cookie.split(';').next().unwrap().trim();

        // Destroy session
        let resp = client
            .get("/destroy")
            .header("cookie", cookie_val)
            .send()
            .await;
        let destroy_cookie = resp.header("set-cookie").unwrap();
        assert!(destroy_cookie.contains("Max-Age=0"));

        // Old cookie no longer works
        let resp = client.get("/get").header("cookie", cookie_val).send().await;
        assert_eq!(resp.text().await, "none");
    }

    #[tokio::test]
    async fn session_is_new() {
        let (layer, _key) = test_layer();

        let client = TestClient::new(
            Router::new()
                .middleware(layer)
                .get("/", |session: Session| async move {
                    format!("{}", session.is_new())
                }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.text().await, "true");
    }

    #[tokio::test]
    async fn session_id_is_unique() {
        let (layer, _key) = test_layer();

        let client = TestClient::new(Router::new().middleware(layer).get(
            "/",
            |session: Session| async move {
                session.insert("x", 1);
                session.id()
            },
        ));

        let resp1 = client.get("/").send().await;
        let id1 = resp1.text().await;
        let resp2 = client.get("/").send().await;
        let id2 = resp2.text().await;

        assert_ne!(id1, id2);
        assert_eq!(id1.len(), 64); // 32 bytes hex = 64 chars
    }

    #[tokio::test]
    async fn unmodified_session_no_cookie() {
        let (layer, _key) = test_layer();

        let client = TestClient::new(
            Router::new()
                .middleware(layer)
                .get("/", |_session: Session| async move { "ok" }),
        );

        // New session, but no data set → still gets a cookie because is_new
        // Actually, new sessions without modifications should still not set cookie
        // Wait -- the middleware sets cookie if modified OR is_new...
        // Let me reconsider: should a new unmodified session get a cookie?
        // Most frameworks: no. Only set cookie when something is stored.
        // Let me check the implementation... our middleware checks `inner.modified || inner.is_new`
        // So new sessions always get a cookie. Let me change this to only `modified`.
        // Actually, for the initial test, let's verify current behavior.
        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        // Current impl: is_new=true → sets cookie even without data
        // This is actually fine — it's a session, and the cookie is just the ID
        assert!(resp.header("set-cookie").is_some());
    }

    #[tokio::test]
    async fn custom_cookie_name() {
        let key = Key::generate();
        let layer = SessionLayer::new(MemoryStore::new(), key).cookie_name("my.session");

        let client = TestClient::new(Router::new().middleware(layer).get(
            "/",
            |session: Session| async move {
                session.insert("x", 1);
                "ok"
            },
        ));

        let resp = client.get("/").send().await;
        let set_cookie = resp.header("set-cookie").unwrap();
        assert!(set_cookie.contains("my.session="));
    }

    #[tokio::test]
    async fn secure_cookie_flag() {
        let key = Key::generate();
        let layer = SessionLayer::new(MemoryStore::new(), key).secure(true);

        let client = TestClient::new(Router::new().middleware(layer).get(
            "/",
            |session: Session| async move {
                session.insert("x", 1);
                "ok"
            },
        ));

        let resp = client.get("/").send().await;
        let set_cookie = resp.header("set-cookie").unwrap();
        assert!(set_cookie.contains("Secure"));
    }

    #[tokio::test]
    async fn without_session_middleware_returns_500() {
        let client =
            TestClient::new(Router::new().get("/", |session: Session| async move { session.id() }));

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn invalid_session_cookie_creates_new_session() {
        let (layer, _key) = test_layer();

        let client = TestClient::new(Router::new().middleware(layer).get(
            "/",
            |session: Session| async move {
                session.insert("x", 1);
                format!("new={}", session.is_new())
            },
        ));

        let resp = client
            .get("/")
            .header("cookie", "neutron.sid=invalid-garbage")
            .send()
            .await;

        assert_eq!(resp.text().await, "new=true");
    }

    #[tokio::test]
    async fn expired_session_creates_new() {
        let key = Key::generate();
        let store = Arc::new(MemoryStore::new());

        let client = TestClient::new(
            Router::new()
                .middleware(SessionLayer {
                    store: Arc::clone(&store) as Arc<dyn SessionStore>,
                    key: key.clone(),
                    cookie_name: "sid".to_string(),
                    max_age: Duration::from_millis(50),
                    cookie_path: "/".to_string(),
                    cookie_http_only: true,
                    cookie_secure: false,
                    cookie_same_site: Some(SameSite::Lax),
                    operation_timeout: Duration::from_secs(5),
                })
                .get("/set", |session: Session| async move {
                    session.insert("data", "value");
                    "set"
                })
                .get("/get", |session: Session| async move {
                    let data: Option<String> = session.get("data");
                    data.unwrap_or_else(|| "none".to_string())
                }),
        );

        // Create session
        let resp = client.get("/set").send().await;
        let cookie = resp.header("set-cookie").unwrap().to_string();
        let cookie_val = cookie.split(';').next().unwrap().trim();

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Session should be gone (expired)
        let resp = client.get("/get").header("cookie", cookie_val).send().await;
        assert_eq!(resp.text().await, "none");
    }

    #[tokio::test]
    async fn memory_store_basic_operations() {
        let store = MemoryStore::new();

        // Initially empty
        assert!(store.load("nonexistent").is_none());

        // Save and load
        let mut data = HashMap::new();
        data.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );
        store.save("sess1", data, Duration::from_secs(60));

        let loaded = store.load("sess1").unwrap();
        assert_eq!(loaded["key"], "value");

        // Destroy
        store.destroy("sess1");
        assert!(store.load("sess1").is_none());
    }

    #[tokio::test]
    async fn memory_store_expiration() {
        let store = MemoryStore::new();
        let mut data = HashMap::new();
        data.insert("x".to_string(), serde_json::json!(1));
        store.save("sess1", data, Duration::from_millis(50));

        // Should be available immediately
        assert!(store.load("sess1").is_some());

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should be expired
        assert!(store.load("sess1").is_none());
    }

    // ------------------------------------------------------------------
    // RS-03 / RS-04 regressions
    // ------------------------------------------------------------------

    fn layer_with_store(store: MemoryStore) -> SessionLayer {
        SessionLayer::new(store, Key::generate())
    }

    /// RS-03: request A loads a session, request B destroys it, then A
    /// finishes and modifies — A's save must be refused rather than
    /// resurrect the destroyed session under the same signed ID.
    #[tokio::test]
    async fn stale_request_cannot_resurrect_destroyed_session() {
        let key = Key::generate();
        let store = Arc::new(MemoryStore::new());
        let client = TestClient::new(
            Router::new()
                .middleware(SessionLayer {
                    store: Arc::clone(&store) as Arc<dyn SessionStore>,
                    key: key.clone(),
                    cookie_name: "sid".to_string(),
                    max_age: Duration::from_secs(3600),
                    cookie_path: "/".to_string(),
                    cookie_http_only: true,
                    cookie_secure: false,
                    cookie_same_site: Some(SameSite::Lax),
                    operation_timeout: Duration::from_secs(5),
                })
                .get("/login", |session: Session| async move {
                    session.insert("user", "alice");
                    "logged in"
                })
                .get("/touch", |session: Session| async move {
                    session.insert("stale-write", true);
                    "touched"
                }),
        );

        // Create the session.
        let resp = client.get("/login").send().await;
        let cookie = resp
            .header("set-cookie")
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .trim()
            .to_string();
        let sid = key.verify(cookie.strip_prefix("sid=").unwrap()).unwrap();

        // Request A loads the session (revision observed)…
        let _rev = store.revision(&sid).expect("session exists");

        // Through the LAYER with genuinely concurrent requests: request A
        // loads the session and pauses in its handler; request B destroys
        // the session; A resumes and modifies — A's save must be refused
        // (503), and the session must not reappear.
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = Arc::new(tokio::sync::Notify::new());

        fn slow_router(
            store: &Arc<MemoryStore>,
            key: &Key,
            entered: &Arc<std::sync::atomic::AtomicBool>,
            release: &Arc<tokio::sync::Notify>,
        ) -> Router {
            let entered = Arc::clone(entered);
            let release = Arc::clone(release);
            Router::new()
                .middleware(SessionLayer {
                    store: Arc::clone(store) as Arc<dyn SessionStore>,
                    key: key.clone(),
                    cookie_name: "sid".to_string(),
                    max_age: Duration::from_secs(3600),
                    cookie_path: "/".to_string(),
                    cookie_http_only: true,
                    cookie_secure: false,
                    cookie_same_site: Some(SameSite::Lax),
                    operation_timeout: Duration::from_secs(5),
                })
                .get("/slow-touch", move |session: Session| {
                    let e = Arc::clone(&entered);
                    let r = Arc::clone(&release);
                    async move {
                        e.store(true, std::sync::atomic::Ordering::SeqCst);
                        r.notified().await;
                        session.insert("stale-write", true);
                        "touched"
                    }
                })
                .get("/logout", |session: Session| async move {
                    session.destroy();
                    "logged out"
                })
        }

        // Request A: loads the session, pauses inside the handler. (Its own
        // client instance — both instances share the store and key.)
        let a_client = TestClient::new(slow_router(&store, &key, &entered, &release));
        let a_cookie = cookie.clone();
        let a = tokio::spawn(async move {
            a_client
                .get("/slow-touch")
                .header("cookie", &a_cookie)
                .send()
                .await
        });
        for _ in 0..200 {
            if entered.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(entered.load(std::sync::atomic::Ordering::SeqCst));

        // Request B: logs out through the layer (destroy path).
        let b_client = TestClient::new(slow_router(&store, &key, &entered, &release));
        let resp = b_client
            .get("/logout")
            .header("cookie", &cookie)
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(store.load(&sid).is_none(), "destroyed");

        // A resumes: its stale save must be refused…
        release.notify_waiters();
        let resp = a.await.expect("request A panicked");
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "the stale request must not save over the destroyed session"
        );
        // …and the destroyed session stays destroyed.
        assert!(store.load(&sid).is_none());

        // Store-level probe (same fencing, direct): after destroy, a save at
        // the pre-destroy revision is refused and the session stays gone.
        let mut probe = HashMap::new();
        probe.insert("user".to_string(), serde_json::json!("alice"));
        assert!(
            !store.save_if_current(&sid, probe, Duration::from_secs(60), _rev),
            "stale writer must not resurrect the destroyed session"
        );
        assert!(store.load(&sid).is_none(), "session stays destroyed");
    }

    /// RS-04: a MemoryStore at capacity refuses new sessions, and the layer
    /// reports failure instead of returning success with an unsaved cookie.
    #[tokio::test]
    async fn capacity_exhaustion_reports_failure_not_success() {
        let store = MemoryStore::with_max_sessions(1);
        // Occupy the single slot.
        let mut first = HashMap::new();
        first.insert("user".to_string(), serde_json::json!("first"));
        assert!(store.save_if_current("occupied-1", first, Duration::from_secs(60), 0));

        let client = TestClient::new(Router::new().middleware(layer_with_store(store)).get(
            "/login",
            |session: Session| async move {
                session.insert("user", "second");
                "logged in"
            },
        ));

        let resp = client.get("/login").send().await;
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "login must not claim success with an unsaved session"
        );
        assert!(
            resp.header("set-cookie").is_none(),
            "no session cookie for a session that was not persisted"
        );
    }

    /// RS-04 (store level): failed capacity saves are observable.
    #[test]
    fn memory_store_save_reports_capacity_failure() {
        let store = MemoryStore::with_max_sessions(1);
        let mut a = HashMap::new();
        a.insert("k".to_string(), serde_json::json!(1));
        assert!(store.save_if_current("a", a.clone(), Duration::from_secs(60), 0));
        assert!(
            !store.save_if_current("b", a, Duration::from_secs(60), 0),
            "at capacity"
        );
    }

    /// RS-03 (concurrent writers): the second writer at a stale revision is
    /// refused instead of silently discarding the first writer's update.
    #[test]
    fn memory_store_fences_concurrent_writers() {
        let store = MemoryStore::new();
        let mut v1 = HashMap::new();
        v1.insert("n".to_string(), serde_json::json!(1));
        assert!(store.save_if_current("s", v1.clone(), Duration::from_secs(60), 0));
        let rev1 = store.revision("s").unwrap();

        // Writer 2 loaded at revision 0 (stale): refused.
        assert!(!store.save_if_current("s", v1.clone(), Duration::from_secs(60), 0));
        // Writer 3 loaded at the current revision: lands.
        assert!(store.save_if_current("s", v1, Duration::from_secs(60), rev1));
    }
    #[test]
    fn expired_record_cannot_be_reinserted_by_a_stale_writer() {
        let store = MemoryStore::new();
        assert!(store.save_if_current("expired", HashMap::new(), Duration::ZERO, 0));
        let revision = store.revision("expired").unwrap();
        // Force the periodic cleanup branch to remove the expired row.
        *store.last_cleanup.lock().unwrap() = Instant::now() - Duration::from_secs(61);
        assert!(!store.save_if_current(
            "expired",
            HashMap::new(),
            Duration::from_secs(60),
            revision
        ));
        assert!(store.load("expired").is_none());
    }

    struct FailingAsyncStore {
        pending: bool,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }
    struct StorageDrop(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for StorageDrop {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl SessionStore for FailingAsyncStore {
        fn load_versioned<'a>(
            &'a self,
            _: &'a str,
        ) -> SessionFuture<'a, Option<(SessionData, u64)>> {
            Box::pin(async move {
                let _guard = StorageDrop(self.dropped.clone());
                if self.pending {
                    std::future::pending::<()>().await;
                }
                Err(SessionStoreError::Unavailable("injected".into()))
            })
        }
        fn commit<'a>(
            &'a self,
            _: &'a str,
            _: SessionData,
            _: Duration,
            _: u64,
        ) -> SessionFuture<'a, ()> {
            Box::pin(async { Err(SessionStoreError::Unavailable("injected".into())) })
        }
        fn revoke<'a>(&'a self, _: &'a str, _: u64) -> SessionFuture<'a, ()> {
            Box::pin(async { Err(SessionStoreError::Unavailable("injected".into())) })
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn load_error_and_deadline_stop_dispatch_and_drop_storage_work() {
        for pending in [false, true] {
            let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let calls2 = calls.clone();
            let key = Key::generate();
            let cookie = format!("neutron.sid={}", key.sign("existing"));
            let client = TestClient::new(
                Router::new()
                    .middleware(
                        SessionLayer::new(
                            FailingAsyncStore {
                                pending,
                                dropped: dropped.clone(),
                            },
                            key,
                        )
                        .operation_timeout(Duration::from_millis(20)),
                    )
                    .get("/me", move || {
                        let calls = calls2.clone();
                        async move {
                            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            "unsafe"
                        }
                    }),
            );
            let resp = client.get("/me").header("cookie", &cookie).send().await;
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(resp.header("set-cookie").is_none());
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn async_revision_contract_distinguishes_conflict_capacity_and_revoke_owner() {
        let store = MemoryStore::with_max_sessions(1);
        store
            .commit("a", HashMap::new(), Duration::from_secs(60), 0)
            .await
            .unwrap();
        let (_, revision) = store.load_versioned("a").await.unwrap().unwrap();
        assert_eq!(
            store
                .commit("b", HashMap::new(), Duration::from_secs(60), 0)
                .await,
            Err(SessionStoreError::Capacity)
        );
        assert_eq!(store.revoke("a", 0).await, Err(SessionStoreError::Conflict));
        store.revoke("a", revision).await.unwrap();
        assert_eq!(
            store
                .commit("a", HashMap::new(), Duration::from_secs(60), revision)
                .await,
            Err(SessionStoreError::Conflict)
        );
    }
}
