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
