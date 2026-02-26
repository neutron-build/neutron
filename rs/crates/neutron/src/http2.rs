//! HTTP/2 configuration and optimizations.
//!
//! Configures hyper's HTTP/2 settings for optimal performance.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::http2::Http2Config;
//!
//! Neutron::new()
//!     .router(router)
//!     .http2(Http2Config::new()
//!         .initial_stream_window_size(2 * 1024 * 1024)   // 2 MB
//!         .initial_connection_window_size(4 * 1024 * 1024) // 4 MB
//!         .max_concurrent_streams(250))
//!     .listen(addr)
//!     .await
//!     .unwrap();
//! ```
//!
//! ## Defaults
//!
//! | Setting | Default | Description |
//! |---------|---------|-------------|
//! | Initial stream window | 2 MB | Per-stream flow control window |
//! | Initial connection window | 4 MB | Connection-level flow control window |
//! | Max concurrent streams | 200 | Max streams per connection |
//! | Max frame size | 16 KB | Max HTTP/2 frame payload |
//! | Max header list size | 16 KB | Max compressed header block |
//! | Enable connect protocol | true | Allow CONNECT for WebSocket |
//! | Keep-alive interval | 20s | Ping interval (None = disabled) |
//! | Keep-alive timeout | 10s | Timeout for ping response |

use std::time::Duration;

/// HTTP/2 performance configuration.
///
/// Use with [`Neutron::http2()`] to tune HTTP/2 parameters.
#[derive(Debug, Clone)]
pub struct Http2Config {
    /// Initial flow-control window size for each stream (bytes).
    pub initial_stream_window_size: u32,
    /// Initial flow-control window size for the connection (bytes).
    pub initial_connection_window_size: u32,
    /// Maximum number of concurrent streams per connection.
    pub max_concurrent_streams: u32,
    /// Maximum frame payload size (bytes).
    pub max_frame_size: u32,
    /// Maximum size of the header compression table (bytes).
    pub max_header_list_size: u32,
    /// Enable the extended CONNECT protocol (for WebSocket over h2).
    pub enable_connect_protocol: bool,
    /// Interval for HTTP/2 keep-alive pings. `None` disables keep-alive.
    pub keep_alive_interval: Option<Duration>,
    /// Timeout for keep-alive ping responses.
    pub keep_alive_timeout: Duration,
    /// Adaptive flow control window sizing.
    pub adaptive_window: bool,
}

impl Http2Config {
    /// Create a new HTTP/2 config with optimized defaults.
    ///
    /// These defaults are tuned for typical web application workloads,
    /// with larger windows than hyper's defaults for better throughput.
    pub fn new() -> Self {
        Self {
            initial_stream_window_size: 2 * 1024 * 1024,       // 2 MB
            initial_connection_window_size: 4 * 1024 * 1024,    // 4 MB
            max_concurrent_streams: 200,
            max_frame_size: 16 * 1024,                          // 16 KB
            max_header_list_size: 16 * 1024,                    // 16 KB
            enable_connect_protocol: true,
            keep_alive_interval: Some(Duration::from_secs(20)),
            keep_alive_timeout: Duration::from_secs(10),
            adaptive_window: true,
        }
    }

    /// Set the initial stream window size (default: 2 MB).
    ///
    /// Larger values improve throughput for large responses but use more memory.
    pub fn initial_stream_window_size(mut self, size: u32) -> Self {
        self.initial_stream_window_size = size;
        self
    }

    /// Set the initial connection window size (default: 4 MB).
    pub fn initial_connection_window_size(mut self, size: u32) -> Self {
        self.initial_connection_window_size = size;
        self
    }

    /// Set the max concurrent streams per connection (default: 200).
    pub fn max_concurrent_streams(mut self, max: u32) -> Self {
        self.max_concurrent_streams = max;
        self
    }

    /// Set the max frame size (default: 16 KB, range: 16KB-16MB).
    pub fn max_frame_size(mut self, size: u32) -> Self {
        // HTTP/2 spec: must be between 16KB and 16MB
        self.max_frame_size = size.clamp(16 * 1024, 16 * 1024 * 1024);
        self
    }

    /// Set the max header list size (default: 16 KB).
    pub fn max_header_list_size(mut self, size: u32) -> Self {
        self.max_header_list_size = size;
        self
    }

    /// Enable/disable the extended CONNECT protocol (default: true).
    pub fn enable_connect_protocol(mut self, enable: bool) -> Self {
        self.enable_connect_protocol = enable;
        self
    }

    /// Set the keep-alive ping interval (default: 20s, None = disabled).
    pub fn keep_alive_interval(mut self, interval: Option<Duration>) -> Self {
        self.keep_alive_interval = interval;
        self
    }

    /// Set the keep-alive ping timeout (default: 10s).
    pub fn keep_alive_timeout(mut self, timeout: Duration) -> Self {
        self.keep_alive_timeout = timeout;
        self
    }

    /// Enable/disable adaptive window sizing (default: true).
    pub fn adaptive_window(mut self, enable: bool) -> Self {
        self.adaptive_window = enable;
        self
    }
}

impl Default for Http2Config {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values() {
        let config = Http2Config::new();
        assert_eq!(config.initial_stream_window_size, 2 * 1024 * 1024);
        assert_eq!(config.initial_connection_window_size, 4 * 1024 * 1024);
        assert_eq!(config.max_concurrent_streams, 200);
        assert_eq!(config.max_frame_size, 16 * 1024);
        assert_eq!(config.max_header_list_size, 16 * 1024);
        assert!(config.enable_connect_protocol);
        assert_eq!(config.keep_alive_interval, Some(Duration::from_secs(20)));
        assert_eq!(config.keep_alive_timeout, Duration::from_secs(10));
        assert!(config.adaptive_window);
    }

    #[test]
    fn builder_pattern() {
        let config = Http2Config::new()
            .initial_stream_window_size(1024 * 1024)
            .initial_connection_window_size(2 * 1024 * 1024)
            .max_concurrent_streams(100)
            .max_frame_size(32 * 1024)
            .max_header_list_size(8 * 1024)
            .enable_connect_protocol(false)
            .keep_alive_interval(None)
            .keep_alive_timeout(Duration::from_secs(5))
            .adaptive_window(false);

        assert_eq!(config.initial_stream_window_size, 1024 * 1024);
        assert_eq!(config.initial_connection_window_size, 2 * 1024 * 1024);
        assert_eq!(config.max_concurrent_streams, 100);
        assert_eq!(config.max_frame_size, 32 * 1024);
        assert_eq!(config.max_header_list_size, 8 * 1024);
        assert!(!config.enable_connect_protocol);
        assert_eq!(config.keep_alive_interval, None);
        assert_eq!(config.keep_alive_timeout, Duration::from_secs(5));
        assert!(!config.adaptive_window);
    }

    #[test]
    fn max_frame_size_clamped() {
        // Below minimum (16 KB)
        let config = Http2Config::new().max_frame_size(1000);
        assert_eq!(config.max_frame_size, 16 * 1024);

        // Above maximum (16 MB)
        let config = Http2Config::new().max_frame_size(100 * 1024 * 1024);
        assert_eq!(config.max_frame_size, 16 * 1024 * 1024);

        // Within range
        let config = Http2Config::new().max_frame_size(32 * 1024);
        assert_eq!(config.max_frame_size, 32 * 1024);
    }

    #[test]
    fn debug_format() {
        let config = Http2Config::new();
        let debug = format!("{config:?}");
        assert!(debug.contains("Http2Config"));
        assert!(debug.contains("initial_stream_window_size"));
    }

    #[test]
    fn clone() {
        let config = Http2Config::new().max_concurrent_streams(50);
        let cloned = config.clone();
        assert_eq!(cloned.max_concurrent_streams, 50);
    }

    #[test]
    fn default_is_same_as_new() {
        let new = Http2Config::new();
        let default = Http2Config::default();
        assert_eq!(new.initial_stream_window_size, default.initial_stream_window_size);
        assert_eq!(new.max_concurrent_streams, default.max_concurrent_streams);
    }
}
