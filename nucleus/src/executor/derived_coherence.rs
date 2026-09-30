//! Coordination for scans that publish derived relational indexes.

#[cfg(test)]
pub(super) struct PublishHook {
    pub kind: &'static str,
    pub table: String,
    pub entered: std::sync::mpsc::Sender<()>,
    pub resume: std::sync::mpsc::Receiver<()>,
}

impl super::Executor {
    #[cfg(test)]
    pub(super) fn pause_derived_publish(&self, kind: &str, table: &str) {
        let hook = {
            let mut slot = self.derived_publish_hook.lock();
            if slot
                .as_ref()
                .is_some_and(|hook| hook.kind == kind && hook.table == table)
            {
                slot.take()
            } else {
                None
            }
        };
        if let Some(hook) = hook {
            hook.entered.send(()).unwrap();
            hook.resume.recv().unwrap();
        }
    }
}

use std::collections::HashMap;

#[derive(Default)]
pub(super) struct DerivedCoherence {
    state: parking_lot::Mutex<State>,
}

#[derive(Default)]
struct State {
    generation: u64,
    writers: usize,
    published: HashMap<String, u64>,
}

#[cfg(feature = "server")]
tokio::task_local! {
    pub(super) static WRITER_GENERATION: u64;
    pub(super) static READ_GENERATION: u64;
}

// Core/WASM has no Tokio runtime. Scope values around each future poll,
// never across suspension, so interleaved embedded executions remain isolated.
#[cfg(not(feature = "server"))]
mod core_scopes {
    use std::cell::Cell;
    use std::future::{Future, poll_fn};

    thread_local! {
        static WRITER: Cell<Option<u64>> = const { Cell::new(None) };
        static READER: Cell<Option<u64>> = const { Cell::new(None) };
    }

    pub static WRITER_GENERATION: GenerationLocal = GenerationLocal(&WRITER);
    pub static READ_GENERATION: GenerationLocal = GenerationLocal(&READER);

    pub struct GenerationLocal(&'static std::thread::LocalKey<Cell<Option<u64>>>);

    #[derive(Debug)]
    pub struct AccessError;

    struct Restore {
        key: &'static std::thread::LocalKey<Cell<Option<u64>>>,
        previous: Option<u64>,
    }

    impl Drop for Restore {
        fn drop(&mut self) {
            self.key.with(|cell| cell.set(self.previous));
        }
    }

    impl GenerationLocal {
        pub fn try_with<R>(&self, f: impl FnOnce(&u64) -> R) -> Result<R, AccessError> {
            self.0
                .try_with(|cell| cell.get().map(|value| f(&value)))
                .map_err(|_| AccessError)?
                .ok_or(AccessError)
        }

        pub fn scope<F: Future>(
            &'static self,
            value: u64,
            future: F,
        ) -> impl Future<Output = F::Output> {
            let mut future = Box::pin(future);
            poll_fn(move |context| {
                let previous = self.0.with(|cell| cell.replace(Some(value)));
                let _restore = Restore {
                    key: self.0,
                    previous,
                };
                future.as_mut().poll(context)
            })
        }
    }
}

#[cfg(not(feature = "server"))]
pub(super) use core_scopes::{READ_GENERATION, WRITER_GENERATION};

pub(super) struct WriterGuard<'a> {
    state: &'a DerivedCoherence,
    pub generation: u64,
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        self.state.state.lock().writers -= 1;
    }
}

impl DerivedCoherence {
    pub fn begin_write(&self) -> WriterGuard<'_> {
        let mut state = self.state.lock();
        state.generation = state.generation.wrapping_add(1);
        state.writers += 1;
        WriterGuard {
            state: self,
            generation: state.generation,
        }
    }

    pub fn generation(&self) -> u64 {
        self.state.lock().generation
    }

    /// Never hold this coordination lock through an await or row-lock wait.
    /// A scan may publish while its own sole writer is finishing maintenance;
    /// any other writer invalidates the scan before its first base/sidecar edit.
    pub fn publish(&self, generation: u64, kind: &str, table: &str, apply: impl FnOnce()) -> bool {
        let mut state = self.state.lock();
        let own_writer = WRITER_GENERATION
            .try_with(|own| *own == generation)
            .unwrap_or(false);
        if state.generation != generation
            || !(state.writers == 0 || (state.writers == 1 && own_writer))
        {
            return false;
        }
        apply();
        state
            .published
            .insert(format!("{kind}/{table}"), generation);
        true
    }

    /// A sole successful writer preserved untouched indexes and ran incremental
    /// hooks for FTS/vector indexes. Only a previously-current image advances;
    /// an image already stale from overlapping writers stays stale.
    pub fn finish_success(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.writers != 1 || state.generation != generation {
            return;
        }
        for (key, stamp) in &mut state.published {
            if (key.starts_with("fts/") || key.starts_with("position/"))
                && *stamp == generation.wrapping_sub(1)
            {
                *stamp = generation;
            }
        }
    }

    pub fn view(&self) -> CurrentView<'_> {
        CurrentView {
            state: self.state.lock(),
        }
    }
}

pub(super) struct CurrentView<'a> {
    state: parking_lot::MutexGuard<'a, State>,
}

impl CurrentView<'_> {
    pub fn current(&self, kind: &str, table: &str) -> bool {
        self.state.writers == 0
            && READ_GENERATION
                .try_with(|read| *read == self.state.generation)
                .unwrap_or(true)
            && self.state.published.get(&format!("{kind}/{table}")) == Some(&self.state.generation)
    }
}

#[cfg(all(test, not(feature = "server")))]
mod core_scope_tests {
    use super::{READ_GENERATION, WRITER_GENERATION};
    use std::future::{Future, poll_fn};
    use std::task::{Context, Poll, Waker};

    #[test]
    fn core_generation_scopes_restore_between_interleaved_polls() {
        let mut first_poll = true;
        let mut first = Box::pin(READ_GENERATION.scope(
            11,
            WRITER_GENERATION.scope(
                7,
                poll_fn(|_| {
                    assert_eq!(READ_GENERATION.try_with(|value| *value).unwrap(), 11);
                    assert_eq!(WRITER_GENERATION.try_with(|value| *value).unwrap(), 7);
                    if first_poll {
                        first_poll = false;
                        Poll::Pending
                    } else {
                        Poll::Ready(())
                    }
                }),
            ),
        ));
        let mut second = Box::pin(WRITER_GENERATION.scope(9, async {
            assert_eq!(WRITER_GENERATION.try_with(|value| *value).unwrap(), 9);
            assert!(READ_GENERATION.try_with(|_| ()).is_err());
        }));
        fn require_send<T: Send>(_: &T) {}
        require_send(&first);
        require_send(&second);
        let mut context = Context::from_waker(Waker::noop());
        assert!(first.as_mut().poll(&mut context).is_pending());
        assert!(WRITER_GENERATION.try_with(|_| ()).is_err());
        assert!(READ_GENERATION.try_with(|_| ()).is_err());
        assert!(second.as_mut().poll(&mut context).is_ready());
        assert!(WRITER_GENERATION.try_with(|_| ()).is_err());
        assert!(first.as_mut().poll(&mut context).is_ready());
        assert!(WRITER_GENERATION.try_with(|_| ()).is_err());
        assert!(READ_GENERATION.try_with(|_| ()).is_err());
    }

    #[test]
    fn core_generation_nested_scope_restores_outer_value() {
        let mut future = Box::pin(WRITER_GENERATION.scope(5, async {
            WRITER_GENERATION
                .scope(9, async {
                    assert_eq!(WRITER_GENERATION.try_with(|value| *value).unwrap(), 9);
                })
                .await;
            assert_eq!(WRITER_GENERATION.try_with(|value| *value).unwrap(), 5);
        }));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        assert!(WRITER_GENERATION.try_with(|_| ()).is_err());
    }

    #[test]
    fn core_generation_scope_restores_after_panic_and_cancellation() {
        let mut panic_future =
            Box::pin(WRITER_GENERATION.scope(13, async { panic!("poll failed") }));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                panic_future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            }))
            .is_err()
        );
        assert!(WRITER_GENERATION.try_with(|_| ()).is_err());
        let mut pending = Box::pin(READ_GENERATION.scope(17, std::future::pending::<()>()));
        assert!(READ_GENERATION.try_with(|_| ()).is_err());
        assert!(
            pending
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(pending);
        assert!(READ_GENERATION.try_with(|_| ()).is_err());
    }
}
