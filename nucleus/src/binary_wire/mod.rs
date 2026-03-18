//! Binary protocol implementation — Phase 1 (Week 1-2)
//!
//! Custom TLV-based protocol for lower latency than pgwire (~8-12μs vs 30-40μs).
//!
//! # Message Format
//!
//! All messages follow TLV (Type-Length-Value) format:
//! ```
//! [type:1byte][length:4bytes BE][payload:N bytes]
//! ```
//!
//! # Modules
//!
//! - `encoder`: TLV frame encoding (16 message types)
//! - `decoder`: TLV frame parsing with error recovery
//! - `handshake`: Connection startup, auth, TLS negotiation
//! - `query_handler`: SQL parsing, parameter binding, preparation
//! - `result_serializer`: Result row encoding, metadata, status
//! - `connection_handler`: Per-connection message dispatch loop
//! - `server`: TCP listener and connection spawner
//! - `tests`: M3 test suite (M3 parallel test infrastructure)

pub mod encoder;
pub mod decoder;
pub mod handshake;
pub mod query_handler;
pub mod result_serializer;
pub mod connection_handler;
pub mod server;

#[cfg(test)]
pub mod tests;
