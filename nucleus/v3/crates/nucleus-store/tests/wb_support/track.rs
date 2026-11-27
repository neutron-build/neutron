//! The lock order tracker (card A-3c, Work 13.6), the hook of
//! `t_freeset_lock_order`: per-thread flags `in_table` and `in_freeset`,
//! keyed by a thread tag, updated by the table-lock and free-set seams. It
//! records a violation when a thread fires `FreesetLocking` with `in_table`
//! set (a thread holding the page table takes the free-set lock, S-STORE
//! §4.2: "never the reverse"), or fires `LoadRead`, `CreateReady`,
//! `EvictWrite` or `LatchTaking` with `in_freeset` set (a page read, a page
//! creation, an eviction or a frame latch under the free-set lock, §4.2:
//! "no I/O, eviction or frame latch runs under it, it creates no page").
//!
//! Its state is under a std mutex that no facade operation runs inside:
//! test code, so A-4c0's rule F2 does not apply to it. Under the model
//! checker the tag is the running thread's task id; a thread fires no hook
//! while it holds the page-table mutex (BUF-TABLE-LEAF), so its flags change
//! only at the seams themselves.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use nucleus_store::buf::{Hook, HookPoint};

/// One thread's flags.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Flags {
    /// The thread holds the page-table mutex.
    pub in_table: bool,
    /// The thread holds the free-set lock.
    pub in_freeset: bool,
}

/// The tracker.
pub struct Tracker<T: Eq + Hash + Copy + Send + 'static> {
    tag: fn() -> T,
    state: Arc<Mutex<HashMap<T, Flags>>>,
    violations: Arc<Mutex<Vec<String>>>,
    /// How many free-set sections and page-table sections began (the
    /// `FreesetLocking` and `TableLocking` firings), so that a test can
    /// require the seams it judges by to have fired at all.
    sections: Arc<Mutex<(u64, u64)>>,
}

/// Lock a test mutex, recovering from poison.
fn lock<U>(m: &Mutex<U>) -> MutexGuard<'_, U> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<T: Eq + Hash + Copy + Send + 'static> Tracker<T> {
    /// A tracker keyed by what `tag` answers for the calling thread.
    pub fn new(tag: fn() -> T) -> Tracker<T> {
        Tracker {
            tag,
            state: Arc::new(Mutex::new(HashMap::new())),
            violations: Arc::new(Mutex::new(Vec::new())),
            sections: Arc::new(Mutex::new((0, 0))),
        }
    }

    /// The hook to install with `Pool::set_hook`.
    pub fn hook(&self) -> Hook {
        let state = Arc::clone(&self.state);
        let tag = self.tag;
        let violations = Arc::clone(&self.violations);
        let sections = Arc::clone(&self.sections);
        Arc::new(move |point, _key, _frame| {
            match point {
                HookPoint::FreesetLocking => lock(&sections).0 += 1,
                HookPoint::TableLocking => lock(&sections).1 += 1,
                _ => {}
            }
            let me = tag();
            let mut held = lock(&state);
            let flags = held.entry(me).or_default();
            match point {
                HookPoint::TableLocking => flags.in_table = true,
                HookPoint::TableUnlocked => flags.in_table = false,
                HookPoint::FreesetLocking => {
                    if flags.in_table {
                        lock(&violations).push(
                            "FreesetLocking while the thread holds the page table (S-STORE §4.2: \
                             never the reverse)"
                                .to_string(),
                        );
                    }
                    flags.in_freeset = true;
                }
                HookPoint::FreesetUnlocked => flags.in_freeset = false,
                HookPoint::LoadRead
                | HookPoint::CreateReady
                | HookPoint::EvictWrite
                | HookPoint::LatchTaking
                    if flags.in_freeset =>
                {
                    lock(&violations).push(format!(
                        "{point:?} while the thread holds the free-set lock (S-STORE §4.2)"
                    ));
                }
                _ => {}
            }
        })
    }

    /// How many free-set sections and page-table sections began.
    pub fn sections(&self) -> (u64, u64) {
        *lock(&self.sections)
    }

    /// The violations recorded, in order.
    pub fn violations(&self) -> Vec<String> {
        lock(&self.violations).clone()
    }
}
