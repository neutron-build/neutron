//! Typed application-state composition.
//!
//! [`FromRef`] lets a handler extract a *sub-state* `Self` out of the composite
//! application state `S`. Combined with `#[derive(FromRef)]` (from
//! `neutron-macros`) and `Router::<S>::with_state`, this gives compile-time
//! verification that every `State<T>` a handler asks for is actually reachable
//! from the app state.

/// Extract a sub-state `Self` from a borrowed app state `S`.
///
/// Derive this for a composite state struct to get one impl per field:
///
/// ```rust,ignore
/// #[derive(Clone, FromRef)]
/// struct AppState { db: Db, cache: Cache }
/// // generates FromRef<AppState> for Db and for Cache
/// ```
pub trait FromRef<S> {
    /// Produce `Self` from a reference to the composite state.
    fn from_ref(state: &S) -> Self;
}

/// Identity: any `Clone` state is `FromRef` of itself.
impl<S: Clone> FromRef<S> for S {
    fn from_ref(state: &S) -> S {
        state.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_ref_identity_clones() {
        #[derive(Clone, Debug, PartialEq)]
        struct AppState {
            name: String,
        }
        let state = AppState { name: "app".into() };
        let cloned = <AppState as FromRef<AppState>>::from_ref(&state);
        assert_eq!(cloned, state);
    }
}
