//! G0 (C-T0 §11): small-scope exhaustive model checkers for the transaction protocol.
//!
//! Each model is a pure state machine over an abstract KV. [`check`] explores every
//! reachable state breadth-first, so a reported counterexample is a shortest trace.
//! Seeded bugs are model configurations; every seed must be caught by its model.

pub mod commit;
pub mod gc;
pub mod ssi;

use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};

pub trait Model {
    type State: Clone + Eq + Hash + Debug;
    type Action: Clone + Debug;

    fn init(&self) -> Self::State;
    /// Every action enabled in `s`, in a deterministic order.
    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>);
    fn next(&self, s: &Self::State, a: &Self::Action) -> Self::State;
    /// Safety invariants of a single state.
    fn check(&self, s: &Self::State) -> Result<(), String>;
    /// A state with no enabled action must be final, otherwise the model is stuck.
    fn is_final(&self, s: &Self::State) -> bool;
}

#[derive(Debug)]
pub struct Violation {
    pub message: String,
    /// Actions from the initial state to the violating state.
    pub trace: Vec<String>,
}

#[derive(Debug)]
pub struct Report {
    pub states: usize,
    pub transitions: usize,
    pub violation: Option<Violation>,
    /// True if exploration stopped at `max_states` before covering the space.
    pub truncated: bool,
}

fn fingerprint<T: Hash>(t: &T) -> u64 {
    let mut h = DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}

/// Breadth-first exhaustive search. States are deduplicated by a 64-bit fingerprint.
pub fn check<M: Model>(m: &M, max_states: usize) -> Report {
    // nodes[i] = (parent, index of the action in the parent's action list)
    let mut nodes: Vec<(usize, usize)> = vec![(usize::MAX, 0)];
    let mut seen = HashSet::new();
    let mut frontier = VecDeque::new();
    let init = m.init();
    seen.insert(fingerprint(&init));
    frontier.push_back((init, 0usize));
    let mut transitions = 0;
    let mut acts = Vec::new();
    let fail =
        |nodes: &Vec<(usize, usize)>, id: usize, message: String, transitions: usize| Report {
            states: nodes.len(),
            transitions,
            violation: Some(Violation {
                message,
                trace: replay(m, nodes, id),
            }),
            truncated: false,
        };
    while let Some((s, id)) = frontier.pop_front() {
        if let Err(e) = m.check(&s) {
            return fail(&nodes, id, e, transitions);
        }
        acts.clear();
        m.actions(&s, &mut acts);
        if acts.is_empty() && !m.is_final(&s) {
            return fail(
                &nodes,
                id,
                "stuck: no enabled action in a non-final state".into(),
                transitions,
            );
        }
        for (i, a) in acts.iter().enumerate() {
            transitions += 1;
            let n = m.next(&s, a);
            if seen.insert(fingerprint(&n)) {
                nodes.push((id, i));
                if nodes.len() > max_states {
                    return Report {
                        states: nodes.len(),
                        transitions,
                        violation: None,
                        truncated: true,
                    };
                }
                frontier.push_back((n, nodes.len() - 1));
            }
        }
    }
    Report {
        states: nodes.len(),
        transitions,
        violation: None,
        truncated: false,
    }
}

fn replay<M: Model>(m: &M, nodes: &[(usize, usize)], id: usize) -> Vec<String> {
    let mut path = Vec::new();
    let mut cur = id;
    while cur != 0 {
        path.push(nodes[cur].1);
        cur = nodes[cur].0;
    }
    path.reverse();
    let mut s = m.init();
    let mut acts = Vec::new();
    let mut trace = Vec::new();
    for i in path {
        acts.clear();
        m.actions(&s, &mut acts);
        let a = acts[i].clone();
        trace.push(format!("{a:?}"));
        s = m.next(&s, &a);
    }
    trace
}
