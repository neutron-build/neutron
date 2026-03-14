// ── Data-model and core modules (available on all targets, including WASM) ──
pub mod advisor;
pub mod allocator;
pub mod blob;
pub mod branching;
pub mod cache;
pub mod catalog;
pub mod columnar;
pub mod compliance;
pub mod config;
pub mod cost;
pub mod datalog;
pub mod document;
pub mod embedded;
pub mod executor;
pub mod fault;
pub mod fts;
pub mod geo;
pub mod graph;
pub mod inference;
pub mod kv;
pub mod memory;
pub mod metrics;
pub mod planner;
pub mod procedures;
pub mod pubsub;
pub mod quantum;
pub mod security;
pub mod simd;
pub mod sparse;
pub mod sql;
pub mod storage;
pub mod tensor;
pub mod tiered;
pub mod timeseries;
pub mod types;
pub mod vector;
pub mod versioning;

// ── Server-only modules (networking, TCP, TLS, threading, CLI, distributed) ──
#[cfg(feature = "server")]
pub mod background;
#[cfg(feature = "server")]
pub mod cli;
#[cfg(feature = "server")]
pub mod distributed;
#[cfg(feature = "server")]
pub mod pool;
#[cfg(feature = "server")]
pub mod raft;
#[cfg(feature = "server")]
pub mod reactive;
#[cfg(feature = "server")]
pub mod replication;
#[cfg(feature = "server")]
pub mod resp;
#[cfg(feature = "server")]
pub mod runtime;
#[cfg(feature = "server")]
pub mod sharding;
#[cfg(feature = "server")]
pub mod tls;
#[cfg(feature = "server")]
pub mod transport;
#[cfg(feature = "server")]
pub mod wire;

// ── WASM target module ──
#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(test)]
mod integration_tests;
