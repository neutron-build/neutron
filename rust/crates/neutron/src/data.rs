//! Parallel data loading utilities.
//!
//! Provides [`DataLoader`] for request-scoped batch loading with automatic
//! deduplication, plus [`join_all`] / [`try_join_all`] for running dynamic
//! collections of futures concurrently.
//!
//! For fixed-arity parallel loading, use `tokio::join!` or `tokio::try_join!`
//! directly.
//!
//! # Example — DataLoader
//!
//! ```rust,ignore
//! use neutron::data::{Loader, DataLoader};
//! use std::collections::HashMap;
//!
//! struct UserLoader { db: Db }
//!
//! impl Loader for UserLoader {
//!     type Key = u64;
//!     type Value = User;
//!     type Error = DbError;
//!
//!     async fn load_one(&self, key: u64) -> Result<User, DbError> {
//!         self.db.find_user(key).await
//!     }
//!
//!     async fn load_batch(&self, keys: &[u64]) -> Result<HashMap<u64, User>, DbError> {
//!         self.db.find_users(keys).await  // single query for all keys
//!     }
//! }
//!
//! async fn handler(State(db): State<Db>) -> Result<Json<Page>, AppError> {
//!     let loader = DataLoader::new(UserLoader { db });
//!     let user = loader.load(1).await?;     // fetches from DB
//!     let same = loader.load(1).await?;      // cached — no DB call
//!     let batch = loader.load_many(&[2, 3, 4]).await?; // single batch query
//!     Ok(Json(Page { user, related: batch }))
//! }
//! ```
//!
//! # Example — Dynamic parallel execution
//!
//! ```rust,ignore
//! use neutron::data::join_all;
//!
//! async fn handler() -> Json<Vec<Item>> {
//!     let ids = vec![1, 2, 3, 4, 5];
//!     let items = join_all(ids.into_iter().map(|id| fetch_item(id))).await;
//!     Json(items)
//! }
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Loader trait
// ---------------------------------------------------------------------------

/// Trait for data sources that can load values by key.
///
/// Implement [`load_one`](Loader::load_one) for single-key loading.
/// Optionally override [`load_batch`](Loader::load_batch) to optimize
/// multi-key loading (e.g. a single SQL `WHERE id IN (...)` query).
pub trait Loader: Send + Sync + 'static {
    /// The key type used to identify values.
    type Key: Hash + Eq + Clone + Send + Sync + 'static;
    /// The value type returned by the loader.
    type Value: Clone + Send + Sync + 'static;
    /// The error type for failed loads.
    type Error: Send + 'static;

    /// Load a single value by key.
    fn load_one(
        &self,
        key: Self::Key,
    ) -> impl Future<Output = Result<Self::Value, Self::Error>> + Send;

    /// Load multiple values by key in a single batch.
    ///
    /// Default implementation calls [`load_one`](Loader::load_one) sequentially.
    /// Override this for optimized batch loading (e.g. a single DB query).
    fn load_batch(
        &self,
        keys: &[Self::Key],
    ) -> impl Future<Output = Result<HashMap<Self::Key, Self::Value>, Self::Error>> + Send {
        async move {
            let mut map = HashMap::with_capacity(keys.len());
            for key in keys {
                let value = self.load_one(key.clone()).await?;
                map.insert(key.clone(), value);
            }
            Ok(map)
        }
    }
}

// ---------------------------------------------------------------------------
// DataLoader
// ---------------------------------------------------------------------------

/// Request-scoped data loader with automatic deduplication cache.
///
/// Wraps a [`Loader`] implementation and caches results so the same key
/// is never loaded twice within a request's lifetime.
pub struct DataLoader<L: Loader> {
    loader: L,
    cache: Mutex<HashMap<L::Key, L::Value>>,
}

impl<L: Loader> DataLoader<L> {
    /// Create a new loader. Each request should get its own `DataLoader`
    /// instance so the cache is request-scoped.
    pub fn new(loader: L) -> Self {
        Self {
            loader,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Load a single value by key, returning a cached result if available.
    pub async fn load(&self, key: L::Key) -> Result<L::Value, L::Error> {
        // Check cache
        {
            let cache = self.cache.lock().unwrap();
            if let Some(value) = cache.get(&key) {
                return Ok(value.clone());
            }
        }

        // Load from source
        let value = self.loader.load_one(key.clone()).await?;

        // Store in cache
        self.cache.lock().unwrap().insert(key, value.clone());

        Ok(value)
    }

    /// Load multiple values by key, using the cache for already-loaded keys
    /// and batching the rest via [`Loader::load_batch`].
    ///
    /// Results are returned in the same order as the input keys.
    pub async fn load_many(&self, keys: &[L::Key]) -> Result<Vec<L::Value>, L::Error> {
        let mut results: Vec<Option<L::Value>> = Vec::with_capacity(keys.len());
        let mut missing_keys: Vec<L::Key> = Vec::new();
        let mut missing_indices: Vec<usize> = Vec::new();

        // Check cache for each key
        {
            let cache = self.cache.lock().unwrap();
            for (i, key) in keys.iter().enumerate() {
                if let Some(value) = cache.get(key) {
                    results.push(Some(value.clone()));
                } else {
                    results.push(None);
                    // Dedup: only load each unique missing key once
                    if !missing_keys.contains(key) {
                        missing_keys.push(key.clone());
                    }
                    missing_indices.push(i);
                }
            }
        }

        // Batch-load missing keys
        if !missing_keys.is_empty() {
            let loaded = self.loader.load_batch(&missing_keys).await?;

            // Cache loaded values and fill results
            let mut cache = self.cache.lock().unwrap();
            for &idx in &missing_indices {
                if let Some(value) = loaded.get(&keys[idx]) {
                    results[idx] = Some(value.clone());
                    cache.insert(keys[idx].clone(), value.clone());
                }
            }
        }

        // Unwrap all results (missing keys that weren't found will cause a panic — should not happen
        // if the loader returns all requested keys)
        Ok(results.into_iter().flatten().collect())
    }

    /// Pre-populate the cache with a known value.
    pub fn prime(&self, key: L::Key, value: L::Value) {
        self.cache.lock().unwrap().insert(key, value);
    }

    /// Clear the cache.
    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Number of cached entries.
    pub fn cached_count(&self) -> usize {
        self.cache.lock().unwrap().len()
    }
}

// ---------------------------------------------------------------------------
// join_all / try_join_all
// ---------------------------------------------------------------------------

/// Run a collection of futures concurrently using a [`tokio::task::JoinSet`].
///
/// Results are returned in the **original order** (not completion order).
///
/// Each future is spawned as a separate Tokio task, so they must be `Send + 'static`.
///
/// ```rust,ignore
/// let items: Vec<Item> = join_all(
///     ids.into_iter().map(|id| fetch_item(id))
/// ).await;
/// ```
pub async fn join_all<T, Fut, I>(futures: I) -> Vec<T>
where
    T: Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    I: IntoIterator<Item = Fut>,
{
    let mut set = tokio::task::JoinSet::new();
    let mut count = 0usize;
    for (i, fut) in futures.into_iter().enumerate() {
        set.spawn(async move { (i, fut.await) });
        count = i + 1;
    }

    let mut results: Vec<Option<T>> = (0..count).map(|_| None).collect();
    while let Some(result) = set.join_next().await {
        let (i, value) = result.expect("spawned task panicked");
        results[i] = Some(value);
    }

    results.into_iter().map(|v| v.unwrap()).collect()
}

/// Run a collection of fallible futures concurrently.
///
/// Returns `Ok(Vec<T>)` in the **original order** if all succeed.
/// On the first error, remaining tasks are cancelled and the error is returned.
///
/// ```rust,ignore
/// let items: Result<Vec<Item>, Error> = try_join_all(
///     ids.into_iter().map(|id| fetch_item(id))
/// ).await;
/// ```
pub async fn try_join_all<T, E, Fut, I>(futures: I) -> Result<Vec<T>, E>
where
    T: Send + 'static,
    E: Send + 'static,
    Fut: Future<Output = Result<T, E>> + Send + 'static,
    I: IntoIterator<Item = Fut>,
{
    let mut set = tokio::task::JoinSet::new();
    let mut count = 0usize;
    for (i, fut) in futures.into_iter().enumerate() {
        set.spawn(async move { (i, fut.await) });
        count = i + 1;
    }

    let mut results: Vec<Option<T>> = (0..count).map(|_| None).collect();
    while let Some(result) = set.join_next().await {
        let (i, res) = result.expect("spawned task panicked");
        match res {
            Ok(value) => {
                results[i] = Some(value);
            }
            Err(e) => {
                set.abort_all();
                return Err(e);
            }
        }
    }

    Ok(results.into_iter().map(|v| v.unwrap()).collect())
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // -----------------------------------------------------------------------
    // Test loader
    // -----------------------------------------------------------------------

    struct TestLoader {
        call_count: Arc<AtomicUsize>,
    }

    impl TestLoader {
        fn new() -> (Self, Arc<AtomicUsize>) {
            let count = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    call_count: Arc::clone(&count),
                },
                count,
            )
        }
    }

    impl Loader for TestLoader {
        type Key = u64;
        type Value = String;
        type Error = String;

        async fn load_one(&self, key: u64) -> Result<String, String> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(format!("value-{key}"))
        }
    }

    struct BatchLoader {
        batch_count: Arc<AtomicUsize>,
    }

    impl BatchLoader {
        fn new() -> (Self, Arc<AtomicUsize>) {
            let count = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    batch_count: Arc::clone(&count),
                },
                count,
            )
        }
    }

    impl Loader for BatchLoader {
        type Key = u64;
        type Value = String;
        type Error = String;

        async fn load_one(&self, key: u64) -> Result<String, String> {
            Ok(format!("value-{key}"))
        }

        async fn load_batch(
            &self,
            keys: &[u64],
        ) -> Result<HashMap<u64, String>, String> {
            self.batch_count.fetch_add(1, Ordering::SeqCst);
            Ok(keys
                .iter()
                .map(|k| (*k, format!("value-{k}")))
                .collect())
        }
    }

    struct FailingLoader;

    impl Loader for FailingLoader {
        type Key = u64;
        type Value = String;
        type Error = String;

        async fn load_one(&self, key: u64) -> Result<String, String> {
            if key == 99 {
                Err("not found".to_string())
            } else {
                Ok(format!("value-{key}"))
            }
        }
    }

    // -----------------------------------------------------------------------
    // DataLoader tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn load_single_value() {
        let (loader, _count) = TestLoader::new();
        let dl = DataLoader::new(loader);

        let value = dl.load(1).await.unwrap();
        assert_eq!(value, "value-1");
    }

    #[tokio::test]
    async fn load_caches_result() {
        let (loader, count) = TestLoader::new();
        let dl = DataLoader::new(loader);

        let v1 = dl.load(1).await.unwrap();
        let v2 = dl.load(1).await.unwrap();

        assert_eq!(v1, v2);
        assert_eq!(count.load(Ordering::SeqCst), 1); // Only one actual load
    }

    #[tokio::test]
    async fn load_different_keys() {
        let (loader, count) = TestLoader::new();
        let dl = DataLoader::new(loader);

        dl.load(1).await.unwrap();
        dl.load(2).await.unwrap();
        dl.load(3).await.unwrap();

        assert_eq!(count.load(Ordering::SeqCst), 3);
        assert_eq!(dl.cached_count(), 3);
    }

    #[tokio::test]
    async fn load_many_uses_batch() {
        let (loader, batch_count) = BatchLoader::new();
        let dl = DataLoader::new(loader);

        let results = dl.load_many(&[1, 2, 3]).await.unwrap();

        assert_eq!(results, vec!["value-1", "value-2", "value-3"]);
        assert_eq!(batch_count.load(Ordering::SeqCst), 1); // Single batch call
    }

    #[tokio::test]
    async fn load_many_uses_cache() {
        let (loader, batch_count) = BatchLoader::new();
        let dl = DataLoader::new(loader);

        // Prime cache with key 1
        dl.prime(1, "cached-1".to_string());

        // load_many should only batch-load keys 2 and 3
        let results = dl.load_many(&[1, 2, 3]).await.unwrap();

        assert_eq!(results[0], "cached-1"); // From cache
        assert_eq!(results[1], "value-2"); // From batch
        assert_eq!(results[2], "value-3"); // From batch
        assert_eq!(batch_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn load_many_deduplicates_keys() {
        let (loader, batch_count) = BatchLoader::new();
        let dl = DataLoader::new(loader);

        let results = dl.load_many(&[1, 2, 1, 2, 3]).await.unwrap();

        assert_eq!(results.len(), 5);
        assert_eq!(results[0], "value-1");
        assert_eq!(results[2], "value-1"); // Same as [0]
        assert_eq!(batch_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn load_propagates_error() {
        let dl = DataLoader::new(FailingLoader);

        let result = dl.load(99).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "not found");
    }

    #[tokio::test]
    async fn prime_populates_cache() {
        let (loader, count) = TestLoader::new();
        let dl = DataLoader::new(loader);

        dl.prime(42, "primed".to_string());
        let value = dl.load(42).await.unwrap();

        assert_eq!(value, "primed");
        assert_eq!(count.load(Ordering::SeqCst), 0); // No actual load
    }

    #[tokio::test]
    async fn clear_empties_cache() {
        let (loader, _count) = TestLoader::new();
        let dl = DataLoader::new(loader);

        dl.load(1).await.unwrap();
        dl.load(2).await.unwrap();
        assert_eq!(dl.cached_count(), 2);

        dl.clear();
        assert_eq!(dl.cached_count(), 0);
    }

    // -----------------------------------------------------------------------
    // join_all tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn join_all_preserves_order() {
        let results: Vec<i32> = join_all((0..5).map(|i| async move { i * 10 })).await;

        assert_eq!(results, vec![0, 10, 20, 30, 40]);
    }

    #[tokio::test]
    async fn join_all_empty() {
        let results: Vec<i32> = join_all(std::iter::empty::<Pin<Box<dyn Future<Output = i32> + Send>>>()).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn join_all_concurrent_execution() {
        use std::time::Instant;

        let start = Instant::now();

        let _results: Vec<()> = join_all((0..3).map(|_| async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }))
        .await;

        let elapsed = start.elapsed();
        // All 3 should run concurrently, so total time should be ~50ms, not ~150ms
        assert!(elapsed.as_millis() < 120);
    }

    // -----------------------------------------------------------------------
    // try_join_all tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn try_join_all_all_ok() {
        let results: Result<Vec<i32>, String> =
            try_join_all((0..3).map(|i| async move { Ok(i * 10) })).await;

        assert_eq!(results.unwrap(), vec![0, 10, 20]);
    }

    #[tokio::test]
    async fn try_join_all_first_error() {
        let result: Result<Vec<i32>, String> = try_join_all(vec![
            Box::pin(async { Ok(1) }) as Pin<Box<dyn Future<Output = Result<i32, String>> + Send>>,
            Box::pin(async { Err("fail".to_string()) }),
            Box::pin(async { Ok(3) }),
        ])
        .await;

        assert_eq!(result.unwrap_err(), "fail");
    }

    #[tokio::test]
    async fn try_join_all_empty() {
        let results: Result<Vec<i32>, String> =
            try_join_all(std::iter::empty::<Pin<Box<dyn Future<Output = Result<i32, String>> + Send>>>()).await;
        assert_eq!(results.unwrap(), Vec::<i32>::new());
    }
}
