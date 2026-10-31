// Distributed transaction conformance test.

use crate::InvariantResult;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum TxState {
    Pending,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Vote {
    Unknown,
    Prepared,
    Aborted,
}

pub struct TxCoordinator {
    pub transactions: HashMap<u64, TxState>,
    pub votes: HashMap<u64, HashMap<u64, Vote>>, // tx_id -> (node_id -> vote)
    pub nodes: Vec<u64>,
    pub participant_outcomes: HashMap<u64, HashMap<u64, TxState>>,
}

impl TxCoordinator {
    pub fn new(nodes: Vec<u64>) -> Self {
        Self {
            transactions: HashMap::new(),
            participant_outcomes: HashMap::new(),
            votes: HashMap::new(),
            nodes,
        }
    }

    pub fn begin_tx(&mut self, tx_id: u64) {
        if self.transactions.contains_key(&tx_id) { return; }
        self.transactions.entry(tx_id).or_insert(TxState::Pending);
        self.participant_outcomes.entry(tx_id).or_insert_with(|| self.nodes.iter().map(|n| (*n, TxState::Pending)).collect());
        let node_votes: HashMap<u64, Vote> =
            self.nodes.iter().map(|n| (*n, Vote::Unknown)).collect();
        self.votes.insert(tx_id, node_votes);
    }

    pub fn vote_prepare(&mut self, tx_id: u64, node: u64) {
        if self.transactions.get(&tx_id) != Some(&TxState::Pending) || !self.nodes.contains(&node) { return; }
        if let Some(votes) = self.votes.get_mut(&tx_id) {
            votes.insert(node, Vote::Prepared);
        }
    }

    pub fn vote_abort(&mut self, tx_id: u64, node: u64) {
        if self.transactions.get(&tx_id) != Some(&TxState::Pending) || !self.nodes.contains(&node) { return; }
        if let Some(votes) = self.votes.get_mut(&tx_id) {
            votes.insert(node, Vote::Aborted);
        }
    }

    pub fn try_commit(&mut self, tx_id: u64) -> bool {
        if self.nodes.is_empty() || self.transactions.get(&tx_id) != Some(&TxState::Pending) { return false; }
        if let Some(votes) = self.votes.get(&tx_id) {
            let all_prepared = self
                .nodes
                .iter()
                .all(|n| votes.get(n) == Some(&Vote::Prepared));
            if all_prepared {
                self.transactions.insert(tx_id, TxState::Committed);
                self.participant_outcomes.insert(tx_id,self.nodes.iter().map(|n| (*n,TxState::Committed)).collect());
                return true;
            }
        }
        false
    }

    pub fn try_abort(&mut self, tx_id: u64) -> bool {
        if self.nodes.is_empty() || self.transactions.get(&tx_id) != Some(&TxState::Pending) { return false; }
        if let Some(votes) = self.votes.get(&tx_id) {
            let any_aborted = self
                .nodes
                .iter()
                .any(|n| votes.get(n) == Some(&Vote::Aborted));
            if any_aborted {
                self.transactions.insert(tx_id, TxState::Aborted);
                self.participant_outcomes.insert(tx_id,self.nodes.iter().map(|n| (*n,TxState::Aborted)).collect());
                return true;
            }
        }
        false
    }

    pub fn check_commit_validity(&self) -> InvariantResult {
        for (tx_id, state) in &self.transactions {
            if *state == TxState::Committed {
                if let Some(votes) = self.votes.get(tx_id) {
                    let all_prepared = self
                        .nodes
                        .iter()
                        .all(|n| votes.get(n) == Some(&Vote::Prepared));
                    if !all_prepared || self.nodes.is_empty() {
                        return InvariantResult {
                            name: "commit_validity".to_string(),
                            holds: false,
                            message: Some(format!("Tx {tx_id} committed without all prepared")),
                        };
                    }
                } else {
                    return InvariantResult { name: "commit_validity".into(), holds: false,
                        message: Some(format!("Tx {tx_id}: committed without vote inventory")) };
                }
            }
        }
        InvariantResult {
            name: "commit_validity".to_string(),
            holds: true,
            message: None,
        }
    }

    pub fn check_atomicity(&self) -> InvariantResult {
        for (tx_id, state) in &self.transactions {
            let outcomes = self.participant_outcomes.get(tx_id);
            let valid = outcomes.is_some_and(|outcomes| outcomes.len() == self.nodes.len() &&
                self.nodes.iter().all(|n| outcomes.get(n) == Some(state)));
            if !valid { return InvariantResult { name: "atomicity".into(), holds: false,
                message: Some(format!("Tx {tx_id}: missing or disagreeing participant outcome")) }; }
        }
        InvariantResult {
            name: "atomicity".to_string(),
            holds: true,
            message: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_flow() {
        let mut coord = TxCoordinator::new(vec![1, 2, 3]);
        coord.begin_tx(1);
        coord.vote_prepare(1, 1);
        coord.vote_prepare(1, 2);
        coord.vote_prepare(1, 3);
        assert!(coord.try_commit(1));
        assert!(coord.check_commit_validity().holds);
        assert!(coord.check_atomicity().holds);
    }

    #[test]
    fn test_abort_on_vote_abort() {
        let mut coord = TxCoordinator::new(vec![1, 2, 3]);
        coord.begin_tx(1);
        coord.vote_prepare(1, 1);
        coord.vote_abort(1, 2);
        assert!(!coord.try_commit(1));
        assert!(coord.try_abort(1));
    }
    #[test]
    fn mixed_outcomes_after_real_commit_rejected() {
        let mut coord = TxCoordinator::new(vec![1,2]);
        coord.begin_tx(1); coord.vote_prepare(1,1); coord.vote_prepare(1,2);
        assert!(coord.try_commit(1));
        coord.participant_outcomes.get_mut(&1).unwrap().insert(2,TxState::Aborted);
        assert!(!coord.check_atomicity().holds);
    }
    #[test]
    fn repeated_begin_and_late_abort_preserve_terminal_outcome() {
        let mut coord = TxCoordinator::new(vec![1,2]);
        coord.begin_tx(1); coord.vote_prepare(1,1); coord.vote_prepare(1,2);
        assert!(coord.try_commit(1)); coord.begin_tx(1);coord.vote_abort(1,2);
        assert!(!coord.try_abort(1));assert!(coord.check_commit_validity().holds);
        assert!(coord.check_atomicity().holds);
    }
}
