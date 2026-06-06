//! Procedural macros for the Neutron web framework.
//!
//! Currently provides `#[derive(FromRef)]`, which generates `FromRef<S>` impls
//! extracting each field as a sub-state from the composite application state.
