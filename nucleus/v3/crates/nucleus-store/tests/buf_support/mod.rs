#![allow(dead_code)]
// The three test binaries (buf_pin, buf_race, buf_model) each include this
// module and use a subset of it, so the rest would warn as dead code in each.

//! Shared fixtures for card A-3a's tests of `buf` (Work 10): a page file
//! that logs its reads and can fail one, a test map standing in for card
//! A-3c's current page map (S-STORE §15's stand-in S3), page builders, the
//! race tests' controller, and the test-only evictor and creator that later
//! cards' real callers will replace.
//!
//! Every wait here is on an event a hook or a thread sends: the controller
//! blocks on a channel, and a paused thread on a condition variable that only
//! `release` signals. Nothing waits on time.

use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{Scope, ScopedJoinHandle};

use nucleus_env::{Env, EnvError, File, MemEnv, MemFile, OpenMode};
use nucleus_store::StoreError;
use nucleus_store::buf::{
    CreateOutcome, CreatedPage, CurrentRef, FrameId, Hook, HookPoint, PageKey, Pin, Pool,
    PoolConfig, Removal,
};
use nucleus_store::meta::Ref;
use nucleus_store::page::{self, Kind};

/// The page size of every fixture but `pool_fixed`'s second half.
pub const PS: u32 = 4096;
/// The byte a test writes as a page's marker; the pool never decodes a body.
pub const MARK: usize = 100;
/// A byte that is zero in every page the tests build, which `Corrupt`
/// overwrites with 0xFF to break the page's CRC.
pub const SPOIL: usize = 200;

/// Lock a test mutex, recovering from poison: a panicking test thread must
/// not turn a fixture's lock into a second panic.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The byte offset of `slot`.
pub fn offset(slot: u64) -> u64 {
    slot * u64::from(PS)
}

/// A page file over a `MemFile` that logs every `read_at` and can fail one.
pub struct FaultFile {
    inner: MemFile,
    log: Mutex<Vec<(u64, usize)>>,
    fail_at: Mutex<Option<u64>>,
    writes: AtomicUsize,
}

impl FaultFile {
    /// A new, empty page file `data` in a fresh `MemEnv`.
    pub fn new() -> Result<FaultFile, EnvError> {
        Ok(FaultFile {
            inner: MemEnv::new(1).open("data", OpenMode::CreateNew)?,
            log: Mutex::new(Vec::new()),
            fail_at: Mutex::new(None),
            writes: AtomicUsize::new(0),
        })
    }

    /// Make every later `read_at` at `off` fail with an injected `Io`
    /// error; `None` stops it.
    pub fn fail_read_at(&self, off: Option<u64>) {
        *lock(&self.fail_at) = off;
    }

    /// Every `read_at` so far, as `(offset, length)`, logged before the
    /// read is delegated.
    pub fn read_log(&self) -> Vec<(u64, usize)> {
        lock(&self.log).clone()
    }

    /// How many `read_at` calls were made.
    pub fn reads(&self) -> usize {
        lock(&self.log).len()
    }

    /// How many `write_at` calls were made.
    pub fn writes(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }
}

impl File for FaultFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize, EnvError> {
        lock(&self.log).push((off, buf.len()));
        if *lock(&self.fail_at) == Some(off) {
            return Err(EnvError::Io {
                op: "read_at",
                path: "data".into(),
                kind: ErrorKind::Other,
                detail: "injected".into(),
            });
        }
        self.inner.read_at(off, buf)
    }

    fn write_at(&self, off: u64, buf: &[u8]) -> Result<(), EnvError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.write_at(off, buf)
    }

    fn sync_data(&self) -> Result<(), EnvError> {
        self.inner.sync_data()
    }

    fn sync_all(&self) -> Result<(), EnvError> {
        self.inner.sync_all()
    }

    fn len(&self) -> Result<u64, EnvError> {
        self.inner.len()
    }

    fn set_len(&self, len: u64) -> Result<(), EnvError> {
        self.inner.set_len(len)
    }

    fn lock_exclusive(&self) -> Result<(), EnvError> {
        self.inner.lock_exclusive()
    }

    fn lock_shared(&self) -> Result<(), EnvError> {
        self.inner.lock_shared()
    }

    fn unlock(&self) -> Result<(), EnvError> {
        self.inner.unlock()
    }
}

/// The test double of card A-3c's current page map (S3): a key's reference,
/// `Ref::NULL` when it has none, and a count of lookups.
pub struct TestMap {
    refs: Mutex<HashMap<PageKey, Ref>>,
    lookups: AtomicU64,
}

impl TestMap {
    /// An empty map: every key unmapped.
    pub fn new() -> TestMap {
        TestMap {
            refs: Mutex::new(HashMap::new()),
            lookups: AtomicU64::new(0),
        }
    }

    /// Set `key`'s reference.
    pub fn set(&self, key: PageKey, r: Ref) {
        lock(&self.refs).insert(key, r);
    }

    /// Unmap `key`.
    pub fn clear(&self, key: PageKey) {
        lock(&self.refs).remove(&key);
    }

    /// `key`'s reference, without counting a lookup.
    pub fn get(&self, key: PageKey) -> Ref {
        lock(&self.refs).get(&key).copied().unwrap_or(Ref::NULL)
    }

    /// How many times a load asked for a reference.
    pub fn lookups(&self) -> u64 {
        self.lookups.load(Ordering::SeqCst)
    }
}

impl<H: File> CurrentRef<H> for TestMap {
    fn current_ref(&self, _pool: &Pool<H>, key: PageKey) -> Result<Ref, StoreError> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        Ok(self.get(key))
    }
}

/// A sealed `PS`-byte page: zeroed, its header written (`kind`, `id`,
/// `epoch`), `marker` at byte `MARK`. Returns the bytes and their CRC.
pub fn page(kind: Kind, id: u64, epoch: u32, marker: u8) -> Result<(Vec<u8>, u32), StoreError> {
    let mut b = vec![0u8; PS as usize];
    page::write_header(&mut b, kind, id, epoch)?;
    b[MARK] = marker;
    let crc = page::seal(&mut b)?;
    Ok((b, crc))
}

/// Write `page(kind, id, epoch, marker)` at `slot` and return its reference.
pub fn put(
    file: &FaultFile,
    slot: u64,
    kind: Kind,
    id: u64,
    epoch: u32,
    marker: u8,
) -> Result<Ref, StoreError> {
    let (b, crc) = page(kind, id, epoch, marker)?;
    file.write_at(offset(slot), &b)?;
    Ok(Ref { slot, epoch, crc })
}

/// Write an LPID's Leaf page (epoch 1) at `slot` and map it.
pub fn put_leaf(
    file: &FaultFile,
    map: &TestMap,
    lpid: u64,
    slot: u64,
    marker: u8,
) -> Result<Ref, StoreError> {
    let r = put(file, slot, Kind::Leaf, lpid, 1, marker)?;
    map.set(PageKey::Lpid(lpid), r);
    Ok(r)
}

/// A pool of `frames` frames at `PS` over a fresh `FaultFile`, with an empty
/// test map.
pub fn fixture(frames: u32) -> Result<(Arc<FaultFile>, TestMap, Pool<FaultFile>), StoreError> {
    let file = Arc::new(FaultFile::new()?);
    let pool = Pool::new(
        Arc::clone(&file),
        PoolConfig {
            frames,
            page_size: PS,
        },
    )?;
    Ok((file, TestMap::new(), pool))
}

/// The marker byte of a pinned page, read under its shared latch.
pub fn marker<H: File>(pin: &Pin<'_, H>) -> Result<u8, StoreError> {
    let g = pin.latch_shared()?;
    Ok(g[MARK])
}

/// What one evictor run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evicted {
    /// `try_victim` gave `None` (pinned, latched, or not resident).
    Busy,
    /// The frame left the table (its reservation dropped).
    Removed,
    /// `remove_from_table` kept it.
    Kept,
}

/// The test-only evictor (card A-3a, `evict_sets_entry_first` (a)), as card
/// A-3c's dirty eviction will run it for an LPID page: take frame `f` as a
/// victim; if it is dirty, seal it, write it at `slot`, set the key's entry
/// to the new home, then `mark_written`; then `remove_from_table` and drop
/// the reservation. The entry is set before the frame leaves the table.
pub fn evict_to(
    pool: &Pool<FaultFile>,
    map: &TestMap,
    f: FrameId,
    slot: u64,
) -> Result<Evicted, StoreError> {
    let Some(mut v) = pool.try_victim(f)? else {
        return Ok(Evicted::Busy);
    };
    if v.is_dirty() {
        let crc = page::seal(&mut v)?;
        pool.file().write_at(offset(slot), &v)?;
        let home = Ref {
            slot,
            epoch: v.content_epoch(),
            crc,
        };
        map.set(v.key(), home);
        v.mark_written(home)?;
    }
    Ok(match v.remove_from_table()? {
        Removal::Removed(res) => {
            drop(res);
            Evicted::Removed
        }
        Removal::Kept => Evicted::Kept,
    })
}

/// Take a clean, unpinned frame out of the table and drop its reservation.
pub fn drop_clean(pool: &Pool<FaultFile>, f: FrameId) -> Result<Evicted, StoreError> {
    let Some(v) = pool.try_victim(f)? else {
        return Ok(Evicted::Busy);
    };
    Ok(match v.remove_from_table()? {
        Removal::Removed(res) => {
            drop(res);
            Evicted::Removed
        }
        Removal::Kept => Evicted::Kept,
    })
}

/// What the test-only creator ended with.
pub enum Made<'p> {
    /// The page, created and latched; `attempts` counts `create` calls.
    Created(CreatedPage<'p, FaultFile>, u32),
    /// A frame held the key live.
    Exists(Pin<'p, FaultFile>, u32),
}

/// The test-only creator (S3: "creations … as A-4a's will make them"):
/// reserve a frame and `create` the key in it; when a load of the key is in
/// progress, wait with `wait_settled` and try again with the same
/// reservation. Returns how many `create` calls it made: the step budget the
/// race tests check.
pub fn create_as_allocation<'p>(
    pool: &'p Pool<FaultFile>,
    key: PageKey,
    kind: Kind,
    epoch: u32,
) -> Result<Made<'p>, StoreError> {
    let mut res = pool.reserve()?;
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        match pool.create(res, key, kind, epoch)? {
            CreateOutcome::Created(c) => return Ok(Made::Created(c, attempts)),
            CreateOutcome::Exists(p, r) => {
                drop(r);
                return Ok(Made::Exists(p, attempts));
            }
            CreateOutcome::Loading(r) => {
                pool.wait_settled(key)?;
                res = r;
            }
        }
    }
}

/// An event the race tests' controller receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// A thread reached an armed pause point and is now blocked there.
    Arrived(HookPoint, PageKey, FrameId),
    /// A reported point fired (the thread did not block).
    Seen(HookPoint, PageKey, FrameId),
    /// A thread's closure ended (sent on its way out, even by a panic).
    Done(&'static str),
    /// No sender is left (never expected: the controller holds one).
    Disconnected,
}

/// A pause's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pause {
    Armed,
    Paused,
    Released,
}

/// The state the hook and the controller share.
struct Shared {
    pauses: Mutex<HashMap<(HookPoint, PageKey), Pause>>,
    wake: Condvar,
    reports: Mutex<HashSet<(HookPoint, PageKey)>>,
    tx: Mutex<Sender<Event>>,
}

impl Shared {
    fn send(&self, e: Event) {
        // The controller keeps the receiver for the whole test.
        let _ = lock(&self.tx).send(e);
    }

    /// The hook: report, then pause if armed, until released.
    fn fire(&self, point: HookPoint, key: PageKey, frame: FrameId) {
        if lock(&self.reports).contains(&(point, key)) {
            self.send(Event::Seen(point, key, frame));
        }
        let mut p = lock(&self.pauses);
        if p.get(&(point, key)) == Some(&Pause::Armed) {
            p.insert((point, key), Pause::Paused);
            self.send(Event::Arrived(point, key, frame));
            while p.get(&(point, key)) == Some(&Pause::Paused) {
                p = self.wake.wait(p).unwrap_or_else(PoisonError::into_inner);
            }
        }
    }
}

/// The race tests' controller (card A-3a, Work 10): one-shot pauses at named
/// hook points, reports, and an event channel. No timed wait anywhere.
#[derive(Clone)]
pub struct Pauser {
    shared: Arc<Shared>,
    rx: Arc<Mutex<Receiver<Event>>>,
}

impl Pauser {
    /// A controller with nothing armed.
    pub fn new() -> Pauser {
        let (tx, rx) = mpsc::channel();
        Pauser {
            shared: Arc::new(Shared {
                pauses: Mutex::new(HashMap::new()),
                wake: Condvar::new(),
                reports: Mutex::new(HashSet::new()),
                tx: Mutex::new(tx),
            }),
            rx: Arc::new(Mutex::new(rx)),
        }
    }

    /// Arm a one-shot pause: the first thread to fire `point` for `key` sends
    /// `Arrived` and blocks until `release(point, key)`.
    pub fn pause_at(&self, point: HookPoint, key: PageKey) {
        lock(&self.shared.pauses).insert((point, key), Pause::Armed);
    }

    /// Make every firing of `point` for `key` send `Seen`, without blocking.
    pub fn report(&self, point: HookPoint, key: PageKey) {
        lock(&self.shared.reports).insert((point, key));
    }

    /// Send `Done(name)`.
    pub fn done(&self, name: &'static str) {
        self.shared.send(Event::Done(name));
    }

    /// The next event, blocking until one comes.
    pub fn next_event(&self) -> Event {
        lock(&self.rx).recv().unwrap_or(Event::Disconnected)
    }

    /// The hook to install with `Pool::set_hook`.
    pub fn hook(&self) -> Hook {
        let shared = Arc::clone(&self.shared);
        Arc::new(move |point, key, frame| shared.fire(point, key, frame))
    }

    /// Unblock the thread paused at `(point, key)`, or disarm the pause if no
    /// thread has reached it yet.
    pub fn release(&self, point: HookPoint, key: PageKey) {
        let mut p = lock(&self.shared.pauses);
        if let Some(state) = p.get_mut(&(point, key)) {
            *state = Pause::Released;
        }
        self.shared.wake.notify_all();
    }

    /// `release` every armed or paused point.
    pub fn release_all(&self) {
        let mut p = lock(&self.shared.pauses);
        for state in p.values_mut() {
            *state = Pause::Released;
        }
        self.shared.wake.notify_all();
    }
}

/// Sends `Done(name)` when dropped: at the end of a thread's closure, and on
/// its way out of a panic, so the controller never waits for a thread that
/// has finished.
struct DoneOnDrop {
    pauser: Pauser,
    name: &'static str,
}

impl Drop for DoneOnDrop {
    fn drop(&mut self) {
        self.pauser.done(self.name);
    }
}

/// Spawn `f` on scope `s` as thread `name`; it sends `Done(name)` when its
/// closure ends.
pub fn spawn<'scope, 'env, T: Send + 'scope>(
    s: &'scope Scope<'scope, 'env>,
    pauser: &Pauser,
    name: &'static str,
    f: impl FnOnce() -> T + Send + 'scope,
) -> ScopedJoinHandle<'scope, T> {
    let pauser = pauser.clone();
    s.spawn(move || {
        let _done = DoneOnDrop { pauser, name };
        f()
    })
}
