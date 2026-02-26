pub mod advisor;
pub mod allocator;
pub mod background;
pub mod blob;
pub mod branching;
pub mod cache;
pub mod catalog;
pub mod cli;
pub mod columnar;
#[allow(dead_code)]
pub mod compliance;
pub mod config;
pub mod cost;
pub mod distributed;
#[allow(dead_code)]
pub mod document;
pub mod embedded;
pub mod executor;
pub mod fault;
pub mod fts;
pub mod geo;
pub mod graph;
#[allow(dead_code)]
pub mod inference;
pub mod kv;
pub mod memory;
pub mod metrics;
pub mod planner;
pub mod pool;
#[allow(dead_code)]
pub mod procedures;
pub mod pubsub;
#[allow(dead_code)]
pub mod quantum;
pub mod raft;
pub mod reactive;
pub mod replication;
pub mod runtime;
pub mod security;
pub mod sharding;
pub mod simd;
pub mod sparse;
pub mod sql;
pub mod storage;
pub mod tensor;
pub mod tiered;
pub mod timeseries;
pub mod tls;
pub mod transport;
pub mod types;
pub mod vector;
pub mod versioning;
pub mod wire;

#[cfg(test)]
mod integration_tests;
