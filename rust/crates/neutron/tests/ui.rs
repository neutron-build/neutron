//! Compile-time (trybuild) guarantees for the `Router<S>` + `FromRef` surface.
//!
//! Note on scope: this tree delivers the "missing state" guarantee through
//! `with_state` consuming a concrete `S` plus the same-`S` `merge` bound, rather
//! than full Axum-style `Handler<T, S>` threading. The `merge_requires_same_state`
//! case below is the load-bearing negative test; `derive_from_ref_ok` is the
//! positive control. Full per-handler `State<U>: FromRef<S>` enforcement is a
//! larger change tracked separately.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/derive_from_ref_ok.rs");
    t.compile_fail("tests/ui/merge_requires_same_state.rs");
}
