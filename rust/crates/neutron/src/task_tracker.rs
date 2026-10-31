//! Server-owned upgraded work: bounded admission, cooperative notification,
//! deadline drain, then abort and join before application shutdown hooks.
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinSet};

pub(crate) struct UpgradeTasks {
    inner: Mutex<Inner>,
    stop: watch::Sender<bool>,
}
struct Inner {
    stopped: bool,
    tasks: JoinSet<()>,
}
pub(crate) struct UpgradeScope(Arc<UpgradeTasks>);
impl Drop for UpgradeScope {
    fn drop(&mut self) {
        self.0.stop();
        self.0.inner.lock().unwrap().tasks.abort_all();
    }
}
impl UpgradeTasks {
    pub(crate) fn new() -> Arc<Self> {
        let (stop, _) = watch::channel(false);
        Arc::new(Self {
            inner: Mutex::new(Inner {
                stopped: false,
                tasks: JoinSet::new(),
            }),
            stop,
        })
    }
    pub(crate) fn scope(self: &Arc<Self>) -> UpgradeScope {
        UpgradeScope(self.clone())
    }
    pub(crate) fn subscribe(&self) -> watch::Receiver<bool> {
        self.stop.subscribe()
    }
    pub(crate) fn spawn(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Option<AbortHandle> {
        let mut inner = self.inner.lock().unwrap();
        while inner.tasks.try_join_next().is_some() {}
        if inner.stopped || inner.tasks.len() >= 1024 {
            return None;
        }
        Some(inner.tasks.spawn(future))
    }
    pub(crate) fn stop(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.stopped = true;
        self.stop.send_replace(true);
    }
    pub(crate) async fn drain(&self, end: tokio::time::Instant) {
        self.stop();
        let mut tasks = {
            let mut inner = self.inner.lock().unwrap();
            std::mem::take(&mut inner.tasks)
        };
        if tokio::time::timeout_at(end, async { while tasks.join_next().await.is_some() {} })
            .await
            .is_err()
        {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
    }
}

pub(crate) fn attach(
    chain: crate::app::DispatchChain,
    tracker: Arc<UpgradeTasks>,
) -> crate::app::DispatchChain {
    Arc::new(move |mut req| {
        req.set_extension(tracker.clone());
        chain(req)
    })
}
