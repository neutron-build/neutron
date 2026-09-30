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
