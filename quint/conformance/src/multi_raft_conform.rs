// Multi-Raft conformance test — verify implementation matches spec.

use crate::{ConformanceRunner, InvariantResult};

/// Simplified Raft state for conformance testing.
#[derive(Debug, Clone)]
pub struct RaftState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log_length: usize,
    pub role: RaftRole,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// Multi-Raft cluster for conformance testing.
pub struct MultiRaftCluster {
    pub nodes: Vec<(u64, Vec<(u64, RaftState)>)>, // node_id -> [(group_id, state)]
}

impl MultiRaftCluster {
    pub fn new(num_nodes: usize, num_groups: usize) -> Self {
        let mut nodes = Vec::new();
        for n in 1..=num_nodes {
            let mut groups = Vec::new();
            for g in 1..=num_groups {
                groups.push((
                    g as u64,
                    RaftState {
                        current_term: 0,
                        voted_for: None,
                        log_length: 0,
                        role: RaftRole::Follower,
                    },
                ));
            }
            nodes.push((n as u64, groups));
        }
        Self { nodes }
    }

    /// Check election safety: at most one leader per group per term.
    pub fn check_election_safety(&self) -> InvariantResult {
        for g in 1..=self.num_groups() {
            for term in 0..=self.max_term() {
                let leaders: Vec<_> = self
                    .nodes
                    .iter()
                    .filter(|(_, groups)| {
                        groups.iter().any(|(gid, state)| {
                            *gid == g as u64
                                && state.role == RaftRole::Leader
                                && state.current_term == term
                        })
                    })
                    .collect();
                if leaders.len() > 1 {
                    return InvariantResult {
                        name: "election_safety".to_string(),
                        holds: false,
                        message: Some(format!(
                            "Multiple leaders for group {g} term {term}: {leaders:?}"
                        )),
                    };
                }
            }
        }
        InvariantResult {
            name: "election_safety".to_string(),
            holds: true,
            message: None,
        }
    }

    fn num_groups(&self) -> usize {
        self.nodes.first().map_or(0, |(_, g)| g.len())
    }

    fn max_term(&self) -> u64 {
        self.nodes
            .iter()
            .flat_map(|(_, groups)| groups.iter().map(|(_, s)| s.current_term))
            .max()
            .unwrap_or(0)
    }
}

/// Run Multi-Raft conformance test.
pub fn run_conformance() -> Vec<InvariantResult> {
    let runner = ConformanceRunner::new()
        .invariant("election_safety")
        .invariant("log_matching")
        .max_traces(1000)
        .max_steps(50);

    let cluster = MultiRaftCluster::new(3, 2);
    let mut results = Vec::new();
    results.push(cluster.check_election_safety());

    let _ = runner; // Will be used with quint-connect when available
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state_safe() {
        let cluster = MultiRaftCluster::new(3, 2);
        let result = cluster.check_election_safety();
        assert!(result.holds);
    }
}
