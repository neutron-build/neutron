//! Middleware trait and dispatch chain construction.
//!
//! Any `async fn(Request, Next) -> Response` automatically implements
//! [`MiddlewareTrait`]. The chain is built once at startup with zero
//! per-request allocation.
//!
//! ```rust,ignore
//! async fn timing(req: Request, next: Next) -> Response {
//!     let start = std::time::Instant::now();
//!     let resp = next.run(req).await;
//!     println!("took {:?}", start.elapsed());
//!     resp
//! }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::handler::{Request, Response};

/// Trait for middleware functions.
///
/// Automatically implemented for `async fn(Request, Next) -> Response`.
pub trait MiddlewareTrait: Send + Sync {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>>;
}

impl<F, Fut> MiddlewareTrait for F
where
    F: Fn(Request, Next) -> Fut + Send + Sync,
    Fut: Future<Output = Response> + Send + 'static,
{
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        Box::pin((self)(req, next))
    }
}

/// Represents the remaining middleware chain plus the final handler.
///
/// Call [`Next::run`] to continue processing the request.
pub struct Next {
    inner: Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>,
}

impl Next {
    pub(crate) fn new(
        f: Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>,
    ) -> Self {
        Self { inner: f }
    }

    /// Execute the remaining middleware chain with the given request.
    pub async fn run(self, req: Request) -> Response {
        (self.inner)(req).await
    }
}

/// Build a dispatch chain from middleware + final handler function.
///
/// The chain is constructed once at startup — zero allocation per request.
pub(crate) fn build_chain(
    middlewares: &[Arc<dyn MiddlewareTrait>],
    final_handler: Arc<
        dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync,
    >,
) -> Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync> {
    let mut chain = final_handler;

    for mw in middlewares.iter().rev() {
        let mw = Arc::clone(mw);
        let next_chain = chain;
        chain = Arc::new(
            move |req: Request| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                let next = Next::new(Arc::clone(&next_chain));
                mw.call(req, next)
            },
        );
    }

    chain
}
