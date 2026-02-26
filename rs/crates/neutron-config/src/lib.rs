//! Typed environment-based configuration for Neutron applications.
//!
//! Deserialises environment variables into any serde-compatible struct using
//! [`envy`].  An optional `dotenv` feature loads a `.env` file first.
//!
//! # Quick start
//!
//! ```rust,ignore
//! use neutron_config::Config;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct AppConfig {
//!     #[serde(default = "default_host")]
//!     host: String,
//!     #[serde(default = "default_port")]
//!     port: u16,
//!     database_url: String,
//!     redis_url: Option<String>,
//! }
//!
//! fn default_host() -> String { "0.0.0.0".into() }
//! fn default_port() -> u16 { 3000 }
//!
//! // Load — env vars map to SCREAMING_SNAKE_CASE field names.
//! let config = Config::<AppConfig>::from_env().expect("missing required config");
//! println!("listening on {}:{}", config.host, config.port);
//! ```
//!
//! # Prefix support
//!
//! ```rust,ignore
//! // APP_DATABASE_URL → database_url
//! let config = Config::<AppConfig>::from_env_prefixed("APP").unwrap();
//! ```
//!
//! # `.env` file support (requires `dotenv` feature)
//!
//! ```rust,ignore
//! let config = Config::<AppConfig>::from_dotenv().unwrap();
//! ```

use std::fmt;
use std::ops::Deref;

use serde::de::DeserializeOwned;

// ---------------------------------------------------------------------------
// ConfigError
// ---------------------------------------------------------------------------

/// Error loading configuration from environment variables or a `.env` file.
#[derive(Debug)]
pub enum ConfigError {
    /// A required environment variable was missing or had an invalid value.
    Env(envy::Error),
    /// Failed to read or parse the `.env` file (only with `dotenv` feature).
    #[cfg(feature = "dotenv")]
    DotEnv(dotenvy::Error),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Env(e) => write!(f, "config error: {e}"),
            #[cfg(feature = "dotenv")]
            Self::DotEnv(e) => write!(f, "dotenv error: {e}"),
        }
    }
}

impl std::error::Error for ConfigError {}

// ---------------------------------------------------------------------------
// Config<T>
// ---------------------------------------------------------------------------

/// Typed configuration loaded from environment variables.
///
/// Implements [`Deref<Target = T>`] for ergonomic field access.
pub struct Config<T> {
    inner: T,
}

impl<T: DeserializeOwned> Config<T> {
    /// Load configuration from the current process environment.
    ///
    /// Field names are mapped from `snake_case` to `SCREAMING_SNAKE_CASE`
    /// automatically by `envy`.  A field named `database_url` expects the
    /// env var `DATABASE_URL`.
    pub fn from_env() -> Result<Self, ConfigError> {
        let inner = envy::from_env::<T>().map_err(ConfigError::Env)?;
        tracing::debug!("configuration loaded from environment");
        Ok(Self { inner })
    }

    /// Load configuration from environment with a naming prefix.
    ///
    /// With `prefix = "APP"`, a field `database_url` maps to `APP_DATABASE_URL`.
    pub fn from_env_prefixed(prefix: &str) -> Result<Self, ConfigError> {
        let inner = envy::prefixed(format!("{prefix}_"))
            .from_env::<T>()
            .map_err(ConfigError::Env)?;
        tracing::debug!(prefix, "configuration loaded from environment");
        Ok(Self { inner })
    }

    /// Load a `.env` file, then read configuration from the environment.
    ///
    /// If the `.env` file does not exist the function silently continues —
    /// that is the expected behaviour in production where env vars are set
    /// by the container orchestrator.
    ///
    /// Requires the `dotenv` feature.
    #[cfg(feature = "dotenv")]
    pub fn from_dotenv() -> Result<Self, ConfigError> {
        match dotenvy::dotenv() {
            Ok(path) => tracing::debug!(?path, "loaded .env file"),
            Err(dotenvy::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(".env file not found, continuing with process environment");
            }
            Err(e) => return Err(ConfigError::DotEnv(e)),
        }
        Self::from_env()
    }

    /// Load a `.env` file at an explicit path, then read configuration from
    /// the environment.
    ///
    /// Requires the `dotenv` feature.
    #[cfg(feature = "dotenv")]
    pub fn from_dotenv_path(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, ConfigError> {
        dotenvy::from_path(path.as_ref()).map_err(ConfigError::DotEnv)?;
        Self::from_env()
    }

    /// Consume the `Config` and return the inner typed value.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> Deref for Config<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.inner }
}

impl<T: fmt::Debug> fmt::Debug for Config<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config").field("inner", &self.inner).finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize, Debug, PartialEq)]
    struct TestCfg {
        #[serde(default = "default_key")]
        neutron_test_key: String,
        neutron_test_optional: Option<String>,
    }

    fn default_key() -> String { "default".into() }

    // Guard against test-isolation issues with env vars.
    fn clean() {
        std::env::remove_var("NEUTRON_TEST_KEY");
        std::env::remove_var("NEUTRON_TEST_OPTIONAL");
    }

    #[test]
    fn defaults_when_vars_absent() {
        clean();
        let cfg = Config::<TestCfg>::from_env().unwrap();
        assert_eq!(cfg.neutron_test_key, "default");
        assert!(cfg.neutron_test_optional.is_none());
    }

    #[test]
    fn reads_set_vars() {
        clean();
        std::env::set_var("NEUTRON_TEST_KEY", "hello");
        let cfg = Config::<TestCfg>::from_env().unwrap();
        assert_eq!(cfg.neutron_test_key, "hello");
        clean();
    }

    #[test]
    fn deref_gives_field_access() {
        clean();
        let cfg = Config::<TestCfg>::from_env().unwrap();
        // Access via Deref.
        let _ = &cfg.neutron_test_key;
    }

    #[test]
    fn into_inner_consumes() {
        clean();
        let cfg = Config::<TestCfg>::from_env().unwrap();
        let inner = cfg.into_inner();
        assert_eq!(inner.neutron_test_key, "default");
    }

    #[test]
    fn prefix_strips_prefix_from_var_names() {
        std::env::remove_var("MYAPP_NEUTRON_TEST_KEY");
        std::env::set_var("MYAPP_NEUTRON_TEST_KEY", "prefixed");
        let cfg = Config::<TestCfg>::from_env_prefixed("MYAPP").unwrap();
        assert_eq!(cfg.neutron_test_key, "prefixed");
        std::env::remove_var("MYAPP_NEUTRON_TEST_KEY");
    }

    #[test]
    fn missing_required_field_is_error() {
        clean();
        // `database_url: String` (no default) is required; absence is an error.
        #[derive(Deserialize)]
        struct Required { neutron_required_url: String }
        std::env::remove_var("NEUTRON_REQUIRED_URL");
        let result = Config::<Required>::from_env();
        assert!(result.is_err());
    }
}
