//! Application configuration loaded from environment variables.
//!
//! Implements `FRAMEWORK_CONTRACT.md` §6: every variable uses the framework
//! prefix `NEUTRON_` (matching the Go/Python/Zig SDKs). Reads:
//! `NEUTRON_HOST` (default `0.0.0.0`), `NEUTRON_PORT` (default `8080`),
//! `NEUTRON_DATABASE_URL`, `NEUTRON_LOG_LEVEL` (default `info`), and
//! `NEUTRON_LOG_FORMAT` (default `json`).
//!
//! ```rust,ignore
//! let config = Config::from_env();
//! Neutron::new().router(router).listen(config.socket_addr()).await?;
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Application configuration loaded from `NEUTRON_`-prefixed environment
/// variables (see `FRAMEWORK_CONTRACT.md` §6).
pub struct Config {
    /// Server bind address (`NEUTRON_HOST`, default `0.0.0.0`).
    pub host: String,
    /// Server port (`NEUTRON_PORT`, default `8080`).
    pub port: u16,
    /// PostgreSQL/Nucleus connection URL (`NEUTRON_DATABASE_URL`, optional).
    pub database_url: Option<String>,
    /// Logging level (`NEUTRON_LOG_LEVEL`, default `info`).
    pub log_level: String,
    /// Log format, `json` or `text` (`NEUTRON_LOG_FORMAT`, default `json`).
    pub log_format: String,
}

impl Config {
    /// Load configuration from `NEUTRON_`-prefixed environment variables,
    /// falling back to the contract defaults for any that are unset.
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            host: std::env::var("NEUTRON_HOST").unwrap_or(d.host),
            port: std::env::var("NEUTRON_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(d.port),
            database_url: std::env::var("NEUTRON_DATABASE_URL").ok(),
            log_level: std::env::var("NEUTRON_LOG_LEVEL").unwrap_or(d.log_level),
            log_format: std::env::var("NEUTRON_LOG_FORMAT").unwrap_or(d.log_format),
        }
    }

    /// Parse the host and port into a `SocketAddr`.
    pub fn socket_addr(&self) -> SocketAddr {
        let ip: IpAddr = self
            .host
            .parse()
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        SocketAddr::from((ip, self.port))
    }
}

impl Default for Config {
    /// The contract §6 defaults (no environment access).
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            database_url: None,
            log_level: "info".to_string(),
            log_format: "json".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn defaults_match_contract() {
        std::env::remove_var("NEUTRON_HOST");
        std::env::remove_var("NEUTRON_PORT");
        std::env::remove_var("NEUTRON_DATABASE_URL");
        std::env::remove_var("NEUTRON_LOG_LEVEL");
        std::env::remove_var("NEUTRON_LOG_FORMAT");
        let config = Config::from_env();
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 8080);
        assert!(config.database_url.is_none());
        assert_eq!(config.log_level, "info");
        assert_eq!(config.log_format, "json");
    }

    #[test]
    fn socket_addr_custom_port() {
        let config = Config {
            port: 8080,
            host: "0.0.0.0".to_string(),
            ..Default::default()
        };
        let addr = config.socket_addr();
        assert_eq!(addr.port(), 8080);
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    }

    #[test]
    fn socket_addr_ipv6() {
        let config = Config {
            port: 443,
            host: "::1".to_string(),
            ..Default::default()
        };
        let addr = config.socket_addr();
        assert_eq!(addr.port(), 443);
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
    }

    #[test]
    fn socket_addr_invalid_host_falls_back_to_localhost() {
        let config = Config {
            port: 3000,
            host: "not-an-ip".to_string(),
            ..Default::default()
        };
        let addr = config.socket_addr();
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn socket_addr_empty_host_falls_back_to_localhost() {
        let config = Config {
            port: 5000,
            host: String::new(),
            ..Default::default()
        };
        let addr = config.socket_addr();
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn port_zero() {
        let config = Config {
            port: 0,
            host: "127.0.0.1".to_string(),
            ..Default::default()
        };
        let addr = config.socket_addr();
        assert_eq!(addr.port(), 0);
    }

    #[test]
    fn port_max() {
        let config = Config {
            port: u16::MAX,
            host: "127.0.0.1".to_string(),
            ..Default::default()
        };
        let addr = config.socket_addr();
        assert_eq!(addr.port(), u16::MAX);
    }
}
