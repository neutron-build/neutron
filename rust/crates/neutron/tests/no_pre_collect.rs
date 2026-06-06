//! P1.2 acceptance guard: no dispatch path may pre-buffer the request body.
//!
//! Request bodies must be passed through as a lazy `ReqBody` stream and only
//! collected on demand (with a per-frame ceiling) inside `collect_body`. These
//! source-level assertions fail if a future change reintroduces an
//! unconditional pre-collect in the server or router dispatch path.

#[test]
fn no_unconditional_body_collect_in_dispatch() {
    let app = include_str!("../src/app.rs");
    assert!(
        !app.contains("Limited::new"),
        "app.rs must not pre-collect request bodies (P1.2) — use with_streaming_state"
    );

    let router = include_str!("../src/router.rs");
    // RouterService::call must hand the body to with_streaming_state, not collect it.
    assert!(
        router.contains("with_streaming_state"),
        "RouterService must pass the request body through as a stream (P1.2)"
    );
}
