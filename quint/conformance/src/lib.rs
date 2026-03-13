// Quint-connect conformance testing harness for Nucleus.
//
// This crate bridges Quint specifications (formal models) with the actual
// Nucleus Rust implementation. The spec acts as an oracle — if the
// implementation diverges from the spec, the test fails.

pub mod multi_raft_conform;
pub mod resharding_conform;
pub mod dtx_conform;

use serde::{Deserialize, Serialize};

/// A state transition in the conformance test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transition {
    pub action: String,
    pub args: serde_json::Value,
}

/// Result of checking an invariant.
#[derive(Debug, Clone)]
pub struct InvariantResult {
    pub name: String,
    pub holds: bool,
    pub message: Option<String>,
}

/// A conformance test runner that checks implementation against spec.
pub struct ConformanceRunner {
    pub transitions: Vec<Transition>,
    pub invariants: Vec<String>,
    pub max_steps: usize,
    pub max_traces: usize,
}

impl ConformanceRunner {
    pub fn new() -> Self {
        Self {
            transitions: Vec::new(),
            invariants: Vec::new(),
            max_steps: 50,
            max_traces: 1000,
        }
    }

    pub fn invariant(mut self, name: &str) -> Self {
        self.invariants.push(name.to_string());
        self
    }

    pub fn max_steps(mut self, steps: usize) -> Self {
        self.max_steps = steps;
        self
    }

    pub fn max_traces(mut self, traces: usize) -> Self {
        self.max_traces = traces;
        self
    }
}

impl Default for ConformanceRunner {
    fn default() -> Self {
        Self::new()
    }
}
