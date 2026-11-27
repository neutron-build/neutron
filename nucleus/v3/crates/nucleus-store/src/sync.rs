//! The store's sync layer (card A-4c0; S-STORE §15 "The interleaving harness").
//!
//! S-STORE §15: "every lock, wait, wake-up, atomic and yield of the store code
//! that such a test runs goes through one sync layer, which a store build backs
//! with the platform's primitives and a test build with the checker's". This
//! module is that one layer. Store code that a model test runs is written
//! against [`Primitives`], and the backend is a type parameter: production
//! instantiates [`StdPrimitives`], and a test crate implements [`Primitives`]
//! for a backend of its own.
//!
//! Invariants owned here:
//! - **F1, no test dependency and no configuration switch.** This module, and
//!   every module written against it, compiles the same way in every build.
//!   The backend is a type parameter, never a build-time switch: one
//!   `cargo test --workspace` build runs real-thread tests and model tests
//!   side by side, and a model checker's primitives fail outside a model
//!   execution. This file carries no conditional compilation and names no
//!   model checker (PLAN-v3 §6.1; N9).
//! - **F2, complete mediation** (S-STORE §15, card S-MU). In code that a model
//!   test exercises, every lock, wait, notify, atomic and yield goes through
//!   this module, except a reference count's atomics (`Arc`'s), "which never
//!   block and order nothing that store code reads". A primitive outside this
//!   layer is invisible to the model checker: its critical section becomes one
//!   step, and if it is held across a sync-layer operation it deadlocks the
//!   checker's single OS thread. The same code uses no thread-local storage
//!   and no thread identity (the model's threads share one OS thread), spins
//!   only through [`yield_now`], and lets no control flow depend on hash
//!   iteration order, because the schedules must replay.
//! - **F3, sequential consistency.** The atomics are sequentially consistent
//!   only and take no ordering: the model checker explores sequentially
//!   consistent interleavings, so a weaker ordering would go unchecked on
//!   aarch64, a CI target (PLAN-v3 §6.1). A weaker ordering needs its own
//!   backend and its own card.
//! - **F4, no reader-writer lock.** S-STORE §5.3 says `std::sync::RwLock`
//!   "promises no policy and does not qualify". Reader-writer exclusion in
//!   store code is [`crate::latch::Latch`]. [`RwCell`] is not a lock: it
//!   excludes nothing by itself and is never waited on (F7).
//! - **F5, errors and poisoning** (S-STORE §9.2, card S-KAPPA). Poisoning is
//!   [`StoreError::Poisoned`] with context, never a panic. "such a mutex is
//!   poisoned as unwinding releases it, in every backend of the store's sync
//!   layer, as std's `Mutex` is, and a thread that then takes it acts on
//!   nothing it guards: it returns the poison's error". So a backend's
//!   [`Primitives::mutex_lock`] and [`Primitives::condvar_wait`] map a poisoned
//!   lock to [`Poisoned`] and never take the guard out of the poisoning.
//!   Making the mark and mapping the error to XX000 is A-3c's: this layer
//!   stays free of calls into the store.
//! - **F6, wasm32.** The module builds for wasm32-unknown-unknown, which the
//!   gate builds for `nucleus-store` (PLAN-v3 §1.6).
//! - **F7, a cell is never waited on** (A-3b's Work 1). Store code takes a
//!   cell only where a latch's grant makes the try succeed: a shared grant
//!   before [`Primitives::rwcell_try_read`], an exclusive grant before
//!   [`Primitives::rwcell_try_write`]. A failed try is then an engine
//!   invariant violation (S-STORE §9.2: XX000, which poisons; "a latch's or a
//!   lock's state, §5.3"), and the caller returns [`StoreError::Invalid`]. The
//!   cell exists because a frame's bytes need shared and exclusive borrows,
//!   and neither `unsafe` (N9) nor a primitive outside this layer (F2) may
//!   provide them. Its first user is A-3b's frame latch.
//!
//! **The table seam (card A-3b, Work 5's table seam).**
//! [`Primitives::mutex_try_lock`] and [`Mutex::try_lock`]. No
//! store path takes a mutex without waiting: a blocking acquire that may
//! wait, and a `try` that never does, are the latch's ([`crate::latch`]),
//! and a victim's latch is a latch, not a mutex. The one caller is a test
//! seam, `buf::Pool::table_free`, which reports whether the page table's
//! mutex is held as a hook or a reference read runs (BUF-TABLE-LEAF). On
//! A-3a's `std::sync::Mutex` it was `try_lock`; once the table moved onto
//! this layer it had no equivalent here, and without it the tests of that
//! invariant lose their oracle. So the layer grows the primitive the seam
//! needs and nothing more: a try is a scheduling point like a lock, it
//! never waits, and it answers a poisoned mutex with [`Poisoned`] (F5;
//! S-STORE §9.2: the panic that poisoned it poisoned the store).

use core::fmt;
use core::ops::{Deref, DerefMut};

use crate::store::StoreError;

/// A backend's lock or wait was poisoned: a panic released the mutex while a
/// thread held it (S-STORE §9.2; F5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Poisoned;

/// The primitives a backend provides to the sync layer (S-STORE §15 "The
/// interleaving harness"; card A-4c0 Work 2).
///
/// A production build uses [`StdPrimitives`]. A test crate implements the
/// trait for a model checker's primitives, so the same store code runs under
/// a scheduler that chooses which thread runs at every call here.
pub trait Primitives: Sized + Send + Sync + 'static {
    /// The backend's mutex, holding a `T`.
    type RawMutex<T: Send>: Send + Sync;
    /// The backend's mutex guard.
    type RawGuard<'a, T: Send + 'a>: DerefMut<Target = T>;
    /// The backend's condition variable.
    type RawCondvar: Send + Sync;
    /// The backend's 64-bit atomic.
    type RawAtomicU64: Send + Sync;
    /// The backend's cell: a value with shared and exclusive borrows.
    type RawRwCell<T: Send + Sync>: Send + Sync;
    /// The cell's shared borrow.
    type RawReadGuard<'a, T: Send + Sync + 'a>: Deref<Target = T>;
    /// The cell's exclusive borrow.
    type RawWriteGuard<'a, T: Send + Sync + 'a>: DerefMut<Target = T>;

    /// Builds a mutex holding `value`.
    fn mutex_new<T: Send>(value: T) -> Self::RawMutex<T>;
    /// Locks the mutex, waiting for its holder, and reports a poisoned mutex
    /// as [`Poisoned`] without handing out its guard (F5).
    fn mutex_lock<T: Send>(m: &Self::RawMutex<T>) -> Result<Self::RawGuard<'_, T>, Poisoned>;
    /// Tries the mutex without waiting (card A-3b, Work 5's table seam):
    /// `Ok(Some(guard))` when it was free, `Ok(None)` while another thread
    /// holds it, and `Err(Poisoned)` for a poisoned mutex, whose guard is
    /// never handed out (F5; S-STORE §9.2).
    fn mutex_try_lock<T: Send>(
        m: &Self::RawMutex<T>,
    ) -> Result<Option<Self::RawGuard<'_, T>>, Poisoned>;
    /// Builds a condition variable.
    fn condvar_new() -> Self::RawCondvar;
    /// Waits on the condition variable, releasing `guard` for the wait and
    /// taking it back before returning (F5).
    fn condvar_wait<'a, T: Send>(
        c: &Self::RawCondvar,
        g: Self::RawGuard<'a, T>,
    ) -> Result<Self::RawGuard<'a, T>, Poisoned>;
    /// Wakes one waiter. Which waiter a backend wakes is its own choice.
    fn condvar_notify_one(c: &Self::RawCondvar);
    /// Wakes every waiter.
    fn condvar_notify_all(c: &Self::RawCondvar);
    /// Builds an atomic holding `v`.
    fn atomic_u64_new(v: u64) -> Self::RawAtomicU64;
    /// Loads the atomic's value.
    fn atomic_u64_load(a: &Self::RawAtomicU64) -> u64;
    /// Stores `v` into the atomic.
    fn atomic_u64_store(a: &Self::RawAtomicU64, v: u64);
    /// Adds `v` and returns the previous value.
    fn atomic_u64_fetch_add(a: &Self::RawAtomicU64, v: u64) -> u64;
    /// Subtracts `v` and returns the previous value.
    fn atomic_u64_fetch_sub(a: &Self::RawAtomicU64, v: u64) -> u64;
    /// Stores `new` if the value is `current`, and returns the value found:
    /// the old value in `Err` when the exchange did not happen.
    fn atomic_u64_compare_exchange(
        a: &Self::RawAtomicU64,
        current: u64,
        new: u64,
    ) -> Result<u64, u64>;
    /// Builds a cell holding `value`.
    fn rwcell_new<T: Send + Sync>(value: T) -> Self::RawRwCell<T>;
    /// Borrows the cell shared without waiting: `Ok(None)` while an exclusive
    /// borrow is held (F7).
    fn rwcell_try_read<T: Send + Sync>(
        c: &Self::RawRwCell<T>,
    ) -> Result<Option<Self::RawReadGuard<'_, T>>, Poisoned>;
    /// Borrows the cell exclusively without waiting: `Ok(None)` while any
    /// other borrow is held (F7).
    fn rwcell_try_write<T: Send + Sync>(
        c: &Self::RawRwCell<T>,
    ) -> Result<Option<Self::RawWriteGuard<'_, T>>, Poisoned>;
    /// Yields the thread or task to the scheduler (F2).
    fn yield_now();
}

/// A mutex over the backend `P` (F1, F5): the only mutex store code locks.
///
/// A holder's panic poisons it, and every later [`Mutex::lock`] returns
/// [`StoreError::Poisoned`].
pub struct Mutex<P: Primitives, T: Send> {
    raw: P::RawMutex<T>,
}

impl<P: Primitives, T: Send> Mutex<P, T> {
    /// Builds a mutex holding `value`.
    pub fn new(value: T) -> Self {
        Self {
            raw: P::mutex_new(value),
        }
    }

    /// Locks the mutex, waiting for its holder.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if a holder's panic poisoned it (S-STORE
    /// §9.2). The poison's guard is not handed out.
    pub fn lock(&self) -> Result<MutexGuard<'_, P, T>, StoreError> {
        P::mutex_lock(&self.raw)
            .map(|raw| MutexGuard { raw })
            .map_err(|_| StoreError::Poisoned("sync layer mutex".into()))
    }

    /// Tries the mutex without waiting (card A-3b, Work 5's table seam): the
    /// test seam `buf::Pool::table_free`'s one primitive. No store path takes
    /// a mutex this way. `Ok(Some(guard))` when the mutex was free,
    /// `Ok(None)` while another thread holds it.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if a holder's panic poisoned it (S-STORE
    /// §9.2; F5). The poison's guard is not handed out.
    pub fn try_lock(&self) -> Result<Option<MutexGuard<'_, P, T>>, StoreError> {
        P::mutex_try_lock(&self.raw)
            .map(|raw| raw.map(|raw| MutexGuard { raw }))
            .map_err(|_| StoreError::Poisoned("sync layer mutex".into()))
    }
}

impl<P: Primitives, T: Send + Default> Default for Mutex<P, T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<P: Primitives, T: Send> fmt::Debug for Mutex<P, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The value is not shown. A `Debug` that waited for the mutex would
        // never return when a thread prints a mutex it holds itself (the
        // standard library's own never waits), and one that tried it would
        // print a poisoned mutex as a failure of `Debug`.
        f.debug_struct("Mutex").finish_non_exhaustive()
    }
}

/// The guard of a [`Mutex`]: the mutex is held until it drops.
pub struct MutexGuard<'a, P: Primitives, T: Send + 'a> {
    raw: P::RawGuard<'a, T>,
}

impl<P: Primitives, T: Send> Deref for MutexGuard<'_, P, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.raw
    }
}

impl<P: Primitives, T: Send> DerefMut for MutexGuard<'_, P, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.raw
    }
}

/// A condition variable over the backend `P` (F1, F5), waited on with a
/// [`MutexGuard`] of a [`Mutex`] over the same backend.
pub struct Condvar<P: Primitives> {
    raw: P::RawCondvar,
}

impl<P: Primitives> Condvar<P> {
    /// Builds a condition variable.
    pub fn new() -> Self {
        Self {
            raw: P::condvar_new(),
        }
    }

    /// Waits, releasing `guard` for the wait and taking it back before
    /// returning. The caller re-checks its condition after every wake.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the mutex was poisoned while waiting
    /// (S-STORE §9.2). The poison's guard is not handed out.
    pub fn wait<'a, T: Send>(
        &self,
        guard: MutexGuard<'a, P, T>,
    ) -> Result<MutexGuard<'a, P, T>, StoreError> {
        P::condvar_wait(&self.raw, guard.raw)
            .map(|raw| MutexGuard { raw })
            .map_err(|_| StoreError::Poisoned("sync layer mutex after a wait".into()))
    }

    /// Wakes one waiter, whichever the backend chooses.
    pub fn notify_one(&self) {
        P::condvar_notify_one(&self.raw);
    }

    /// Wakes every waiter.
    pub fn notify_all(&self) {
        P::condvar_notify_all(&self.raw);
    }
}

impl<P: Primitives> Default for Condvar<P> {
    fn default() -> Self {
        Self::new()
    }
}

/// A 64-bit atomic over the backend `P` (F3): sequentially consistent, with
/// no ordering argument.
pub struct AtomicU64<P: Primitives> {
    raw: P::RawAtomicU64,
}

impl<P: Primitives> AtomicU64<P> {
    /// Builds an atomic holding `v`.
    pub fn new(v: u64) -> Self {
        Self {
            raw: P::atomic_u64_new(v),
        }
    }

    /// Loads the value.
    pub fn load(&self) -> u64 {
        P::atomic_u64_load(&self.raw)
    }

    /// Stores `v`.
    pub fn store(&self, v: u64) {
        P::atomic_u64_store(&self.raw, v);
    }

    /// Adds `v` and returns the previous value.
    pub fn fetch_add(&self, v: u64) -> u64 {
        P::atomic_u64_fetch_add(&self.raw, v)
    }

    /// Subtracts `v` and returns the previous value.
    pub fn fetch_sub(&self, v: u64) -> u64 {
        P::atomic_u64_fetch_sub(&self.raw, v)
    }

    /// Stores `new` if the value is `current`. Returns the value found: in
    /// `Err` when it was not `current` and nothing was stored.
    pub fn compare_exchange(&self, current: u64, new: u64) -> Result<u64, u64> {
        P::atomic_u64_compare_exchange(&self.raw, current, new)
    }
}

/// A reader-writer cell over the backend `P` (F7): a value with shared and
/// exclusive borrows, and no blocking method.
///
/// Store code takes a borrow only where a latch's grant makes the try
/// succeed (a shared grant before [`RwCell::try_read`], an exclusive grant
/// before [`RwCell::try_write`], as [`crate::latch::Latch`] grants them). A
/// `Ok(None)` is then an engine invariant violation, which the caller returns
/// as [`StoreError::Invalid`] (S-STORE §9.2: XX000).
pub struct RwCell<P: Primitives, T: Send + Sync> {
    raw: P::RawRwCell<T>,
}

impl<P: Primitives, T: Send + Sync> RwCell<P, T> {
    /// Builds a cell holding `value`.
    pub fn new(value: T) -> Self {
        Self {
            raw: P::rwcell_new(value),
        }
    }

    /// Borrows the cell shared without waiting.
    ///
    /// # Errors
    ///
    /// `Ok(None)` while an exclusive borrow is held. [`StoreError::Poisoned`]
    /// if the cell was poisoned (F5).
    pub fn try_read(&self) -> Result<Option<RwCellReadGuard<'_, P, T>>, StoreError> {
        P::rwcell_try_read(&self.raw)
            .map(|raw| raw.map(|raw| RwCellReadGuard { raw }))
            .map_err(|_| StoreError::Poisoned("sync layer cell".into()))
    }

    /// Borrows the cell exclusively without waiting.
    ///
    /// # Errors
    ///
    /// `Ok(None)` while any other borrow is held. [`StoreError::Poisoned`] if
    /// the cell was poisoned (F5).
    pub fn try_write(&self) -> Result<Option<RwCellWriteGuard<'_, P, T>>, StoreError> {
        P::rwcell_try_write(&self.raw)
            .map(|raw| raw.map(|raw| RwCellWriteGuard { raw }))
            .map_err(|_| StoreError::Poisoned("sync layer cell".into()))
    }
}

/// The shared borrow of an [`RwCell`].
pub struct RwCellReadGuard<'a, P: Primitives, T: Send + Sync + 'a> {
    raw: P::RawReadGuard<'a, T>,
}

impl<P: Primitives, T: Send + Sync> Deref for RwCellReadGuard<'_, P, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.raw
    }
}

/// The exclusive borrow of an [`RwCell`].
pub struct RwCellWriteGuard<'a, P: Primitives, T: Send + Sync + 'a> {
    raw: P::RawWriteGuard<'a, T>,
}

impl<P: Primitives, T: Send + Sync> Deref for RwCellWriteGuard<'_, P, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.raw
    }
}

impl<P: Primitives, T: Send + Sync> DerefMut for RwCellWriteGuard<'_, P, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.raw
    }
}

impl<P: Primitives, T: Send + fmt::Debug> fmt::Debug for MutexGuard<'_, P, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<P: Primitives, T: Send + Sync + fmt::Debug> fmt::Debug for RwCellReadGuard<'_, P, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<P: Primitives, T: Send + Sync + fmt::Debug> fmt::Debug for RwCellWriteGuard<'_, P, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

/// Yields the thread or task to the scheduler (F2): the only spin of
/// model-tested store code.
pub fn yield_now<P: Primitives>() {
    P::yield_now();
}

/// The platform's primitives (S-STORE §15: "a store build backs with the
/// platform's primitives"): the standard library's mutex, condition variable
/// and reader-writer lock, a standard 64-bit atomic at its strongest
/// ordering, and the standard thread yield (F3, F5).
#[derive(Debug, Clone, Copy, Default)]
pub struct StdPrimitives;

impl Primitives for StdPrimitives {
    type RawMutex<T: Send> = std::sync::Mutex<T>;
    type RawGuard<'a, T: Send + 'a> = std::sync::MutexGuard<'a, T>;
    type RawCondvar = std::sync::Condvar;
    type RawAtomicU64 = std::sync::atomic::AtomicU64;
    type RawRwCell<T: Send + Sync> = std::sync::RwLock<T>;
    type RawReadGuard<'a, T: Send + Sync + 'a> = std::sync::RwLockReadGuard<'a, T>;
    type RawWriteGuard<'a, T: Send + Sync + 'a> = std::sync::RwLockWriteGuard<'a, T>;

    fn mutex_new<T: Send>(value: T) -> Self::RawMutex<T> {
        std::sync::Mutex::new(value)
    }

    fn mutex_lock<T: Send>(m: &Self::RawMutex<T>) -> Result<Self::RawGuard<'_, T>, Poisoned> {
        m.lock().map_err(|_| Poisoned)
    }

    fn mutex_try_lock<T: Send>(
        m: &Self::RawMutex<T>,
    ) -> Result<Option<Self::RawGuard<'_, T>>, Poisoned> {
        match m.try_lock() {
            Ok(guard) => Ok(Some(guard)),
            Err(std::sync::TryLockError::WouldBlock) => Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => Err(Poisoned),
        }
    }

    fn condvar_new() -> Self::RawCondvar {
        std::sync::Condvar::new()
    }

    fn condvar_wait<'a, T: Send>(
        c: &Self::RawCondvar,
        g: Self::RawGuard<'a, T>,
    ) -> Result<Self::RawGuard<'a, T>, Poisoned> {
        c.wait(g).map_err(|_| Poisoned)
    }

    fn condvar_notify_one(c: &Self::RawCondvar) {
        c.notify_one();
    }

    fn condvar_notify_all(c: &Self::RawCondvar) {
        c.notify_all();
    }

    fn atomic_u64_new(v: u64) -> Self::RawAtomicU64 {
        std::sync::atomic::AtomicU64::new(v)
    }

    fn atomic_u64_load(a: &Self::RawAtomicU64) -> u64 {
        a.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn atomic_u64_store(a: &Self::RawAtomicU64, v: u64) {
        a.store(v, std::sync::atomic::Ordering::SeqCst);
    }

    fn atomic_u64_fetch_add(a: &Self::RawAtomicU64, v: u64) -> u64 {
        a.fetch_add(v, std::sync::atomic::Ordering::SeqCst)
    }

    fn atomic_u64_fetch_sub(a: &Self::RawAtomicU64, v: u64) -> u64 {
        a.fetch_sub(v, std::sync::atomic::Ordering::SeqCst)
    }

    fn atomic_u64_compare_exchange(
        a: &Self::RawAtomicU64,
        current: u64,
        new: u64,
    ) -> Result<u64, u64> {
        // A failed exchange answers with the value it compared against, which
        // the standard library returns in `Err`. A second load would answer
        // with a later value, and would differ from the model checker's
        // backend, which also returns the value compared (F1).
        a.compare_exchange(
            current,
            new,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        )
    }

    fn rwcell_new<T: Send + Sync>(value: T) -> Self::RawRwCell<T> {
        std::sync::RwLock::new(value)
    }

    fn rwcell_try_read<T: Send + Sync>(
        c: &Self::RawRwCell<T>,
    ) -> Result<Option<Self::RawReadGuard<'_, T>>, Poisoned> {
        match c.try_read() {
            Ok(guard) => Ok(Some(guard)),
            Err(std::sync::TryLockError::WouldBlock) => Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => Err(Poisoned),
        }
    }

    fn rwcell_try_write<T: Send + Sync>(
        c: &Self::RawRwCell<T>,
    ) -> Result<Option<Self::RawWriteGuard<'_, T>>, Poisoned> {
        match c.try_write() {
            Ok(guard) => Ok(Some(guard)),
            Err(std::sync::TryLockError::WouldBlock) => Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => Err(Poisoned),
        }
    }

    fn yield_now() {
        std::thread::yield_now();
    }
}
