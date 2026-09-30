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

tokio::task_local! {
    pub(super) static WRITER_GENERATION: u64;
    pub(super) static READ_GENERATION: u64;
}

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

    pub fn current(&self, kind: &str, table: &str) -> bool {
        let state = self.state.lock();
        state.writers == 0
            && READ_GENERATION
                .try_with(|read| *read == state.generation)
                .unwrap_or(true)
            && state.published.get(&format!("{kind}/{table}")) == Some(&state.generation)
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
