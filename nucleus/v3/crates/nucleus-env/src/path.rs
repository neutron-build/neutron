//! The E-PATH rules: one path namespace shared by every backend
//! (PLAN-v3 §3.2; card A-1a, "The contract" E-PATH).
//!
//! Invariant owned: **E-PATH**. A path is relative, with `/` separators.
//! Each component is non-empty and is not `.` or `..`. No component
//! contains `\` or NUL. There is no leading `/`. The empty string `""`
//! names the root directory, and only `sync_dir` and `list_dir` accept
//! it ([`check_dir`]); every other method takes a non-empty path
//! ([`check`]). A violation is [`EnvError::InvalidPath`] on every
//! backend, before any I/O.
//!
//! Why: one namespace for all four backends, OPFS included, so a store
//! path means the same thing everywhere (S-STORE §2: one page file plus
//! a `wal/` directory; §12.1: `wal/<first-lsn>.log`).

use crate::error::EnvError;

/// Rejects the empty path; accepts every well-formed relative path.
///
/// This is the check for every [`crate::Env`] method that names one file
/// or directory (`open`, `create_dir`, `sync_dir`, `rename`, `remove`,
/// `exists`, `list_dir`).
pub(crate) fn check(path: &str) -> Result<(), EnvError> {
    if path.is_empty() {
        return Err(invalid(path, "empty path: only sync_dir and list_dir accept the root"));
    }
    check_non_root(path)
}

/// Like [`check`], but the empty path names the root and is accepted.
///
/// Only `sync_dir` and `list_dir` take a directory that may be the root
/// (S-STORE §12.5: the `wal/` directory entry sync; §14.1 has no
/// root-listing rule, but the same shape applies).
pub(crate) fn check_dir(path: &str) -> Result<(), EnvError> {
    if path.is_empty() {
        return Ok(());
    }
    check_non_root(path)
}

/// The clauses every non-root path must satisfy.
fn check_non_root(path: &str) -> Result<(), EnvError> {
    if path.starts_with('/') {
        return Err(invalid(path, "absolute path: paths are relative to the Env root"));
    }
    for comp in path.split('/') {
        if comp.is_empty() {
            return Err(invalid(path, "empty component (\"//\" or trailing \"/\")"));
        }
        if comp == "." || comp == ".." {
            return Err(invalid(path, "\".\" or \"..\" component"));
        }
        if comp.contains('\\') {
            return Err(invalid(path, "backslash in component (\"/\" is the only separator)"));
        }
        if comp.contains('\0') {
            return Err(invalid(path, "NUL in component"));
        }
    }
    Ok(())
}

/// Builds the [`EnvError::InvalidPath`] for a violated clause.
fn invalid(path: &str, why: &'static str) -> EnvError {
    EnvError::InvalidPath { path: path.to_string(), why }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_accepts_relative_paths() {
        assert_eq!(check("page"), Ok(()));
        assert_eq!(check("wal/0000000000000000.log"), Ok(()));
        assert_eq!(check_dir(""), Ok(()));
        assert_eq!(check_dir("wal"), Ok(()));
    }

    #[test]
    fn path_rejects_empty() {
        assert!(matches!(check(""), Err(EnvError::InvalidPath { .. })));
        assert_eq!(check_dir(""), Ok(()));
    }

    #[test]
    fn path_rejects_leading_slash() {
        assert!(matches!(check("/abs"), Err(EnvError::InvalidPath { .. })));
        assert!(matches!(check_dir("/abs"), Err(EnvError::InvalidPath { .. })));
    }

    #[test]
    fn path_rejects_empty_component() {
        assert!(matches!(check("a//b"), Err(EnvError::InvalidPath { .. })));
        assert!(matches!(check("wal/"), Err(EnvError::InvalidPath { .. })));
    }

    #[test]
    fn path_rejects_dotdot() {
        assert!(matches!(check("../x"), Err(EnvError::InvalidPath { .. })));
        assert!(matches!(check("wal/../x"), Err(EnvError::InvalidPath { .. })));
    }

    #[test]
    fn path_rejects_dot() {
        assert!(matches!(check("wal/./x"), Err(EnvError::InvalidPath { .. })));
        assert!(matches!(check("."), Err(EnvError::InvalidPath { .. })));
    }

    #[test]
    fn path_rejects_backslash() {
        assert!(matches!(check("a\\b"), Err(EnvError::InvalidPath { .. })));
    }

    #[test]
    fn path_rejects_nul() {
        assert!(matches!(check("a\0b"), Err(EnvError::InvalidPath { .. })));
    }
}
