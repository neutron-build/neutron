//! Application configuration loaded from environment variables.
//!
//! Reads `PORT` (default 3000) and `HOST` (default `127.0.0.1`) from the
//! environment and exposes a [`SocketAddr`] for server binding.
//!
//! ```rust,ignore
//! let config = Config::from_env();
//! Neutron::new().router(router).listen(config.socket_addr()).await?;
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Application configuration loaded from environment variables.
///
/// This is a placeholder for future Neutron.toml + env var loading.
pub struct Config {
    pub port: u16,
    pub host: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            port: std::env::var("PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3000),
            host: std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
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
    fn default() -> Self {
        Self::from_env()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn default_port_and_host() {
        // Clear env to ensure defaults
        std::env::remove_var("PORT");
        std::env::remove_var("HOST");
        let config = Config::from_env();
        assert_eq!(config.port, 3000);
        assert_eq!(config.host, "127.0.0.1");
    }

    #[test]
    fn socket_addr_from_defaults() {
        let config = Config {
            port: 3000,
            host: "127.0.0.1".to_string(),
        };
        let addr = config.socket_addr();
        assert_eq!(addr.port(), 3000);
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn socket_addr_custom_port() {
        let config = Config {
            port: 8080,
            host: "0.0.0.0".to_string(),
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
        };
        let addr = config.socket_addr();
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn socket_addr_empty_host_falls_back_to_localhost() {
        let config = Config {
            port: 5000,
            host: String::new(),
        };
        let addr = config.socket_addr();
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn port_zero() {
        let config = Config {
            port: 0,
            host: "127.0.0.1".to_string(),
        };
        let addr = config.socket_addr();
        assert_eq!(addr.port(), 0);
    }

    #[test]
    fn port_max() {
        let config = Config {
            port: u16::MAX,
            host: "127.0.0.1".to_string(),
        };
        let addr = config.socket_addr();
        assert_eq!(addr.port(), u16::MAX);
    }
}
