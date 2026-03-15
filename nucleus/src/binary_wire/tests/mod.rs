//! Binary protocol test suite (M3) — Week 1-3 parallel testing
//!
//! Organization:
//! - test_server.rs: TestServer and TestClient infrastructure
//! - binary_tests.rs: 200+ test cases for all 14 data models
//! - cross_protocol.rs: pgwire vs binary protocol validation
//! - concurrency_tests.rs: multi-threaded stress tests
//! - property_tests.rs: property-based fuzzing with proptest
//! - isolation_tests.rs: transaction isolation levels
//! - error_tests.rs: error codec and recovery paths

mod test_server;
mod binary_tests;
mod cross_protocol;
mod concurrency_tests;
mod property_tests;
mod isolation_tests;
mod error_tests;

pub use test_server::{spawn_binary_server, TestClient, TestServer};
