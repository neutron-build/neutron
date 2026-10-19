//! Model tests of the shared components (card X-1a Work 3): the write
//! set against a naive model across random op streams with savepoints
//! and rollbacks, and the sequencer's applied-prefix rule across random
//! out-of-order applies.

use e1_mvcc::common::{Sequencer, WriteSet};
use proptest::prelude::*;

/// One write-set op of the generated stream.
#[derive(Clone, Debug)]
enum WsOp {
    Set(Vec<u8>, Option<Vec<u8>>),
    Savepoint,
    Rollback(usize),
}

/// The map model: present keys and their values (None = deleted).
type Map = std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>;

/// The naive model: a full snapshot of the map per savepoint.
#[derive(Clone, Debug, Default)]
struct Model {
    map: Map,
    sps: Vec<(u32, Map)>,
}

impl Model {
    fn apply(&mut self, op: &WsOp, ws: &mut WriteSet) {
        match op {
            WsOp::Set(k, v) => {
                ws.set(k, v.clone());
                self.map.insert(k.clone(), v.clone());
            }
            WsOp::Savepoint => {
                let id = ws.savepoint().0;
                self.sps.push((id, self.map.clone()));
            }
            WsOp::Rollback(i) => {
                // Roll back to the i-th oldest live savepoint, if any.
                if let Some((id, snap)) = self.sps.get(*i).cloned() {
                    ws.rollback_to(e1_mvcc::api::SavepointId(id));
                    self.map = snap;
                    self.sps.retain(|(sid, _)| *sid <= id);
                }
            }
        }
    }
}

fn ws_op() -> impl Strategy<Value = WsOp> {
    prop_oneof![
        6 => (any::<u8>(), proptest::option::of(proptest::collection::vec(any::<u8>(), 0..16)))
            .prop_map(|(k, v)| WsOp::Set(vec![k, k % 3], v)),
        2 => Just(WsOp::Savepoint),
        2 => (0usize..4).prop_map(WsOp::Rollback),
    ]
}

proptest! {
    /// W-UNDO-EXACT and W-SP-MONOTONE: after every op, the write set's
    /// contents equal the model's, whatever the interleaving of
    /// savepoints and rollbacks.
    #[test]
    fn write_set_matches_model(ops in proptest::collection::vec(ws_op(), 0..200)) {
        let mut ws = WriteSet::new();
        let mut model = Model::default();
        for op in &ops {
            model.apply(op, &mut ws);
            let got: Vec<(Vec<u8>, Option<Vec<u8>>)> = ws.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            let want: Vec<(Vec<u8>, Option<Vec<u8>>)> = model.map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            prop_assert_eq!(got, want, "after op {:?}", op);
        }
    }

    /// S-VISIBLE-PREFIX: whatever the order ts's are marked applied in,
    /// visible_ts is exactly the contiguous applied prefix (and the
    /// VISIBLE-JUMPS mutant breaks this property).
    #[test]
    fn sequencer_visible_is_the_applied_prefix(order in proptest::collection::vec(any::<u64>(), 0..64)) {
        let seq = Sequencer::new();
        let n = order.len() as u64;
        // Hand out n ts's, then mark them applied in a shuffled order
        // derived from the seed bytes.
        let ts: Vec<u64> = (0..n).map(|_| seq.next_commit_ts()).collect();
        let mut remaining: Vec<usize> = (0..ts.len()).collect();
        // Fisher-Yates with the generated values.
        let mut i = remaining.len();
        for x in &order {
            if i <= 1 { break; }
            let j = (*x as usize) % i;
            remaining.swap(i - 1, j);
            i -= 1;
        }
        let mut applied = vec![false; ts.len()];
        for (step, idx) in remaining.iter().enumerate() {
            seq.mark_applied(ts[*idx]);
            applied[*idx] = true;
            // visible = largest prefix of ts's all applied.
            let mut expect = 0u64;
            if n > 0 && applied[0] {
                while (expect as usize) < ts.len() && applied[expect as usize] {
                    expect += 1;
                }
            }
            prop_assert_eq!(seq.visible_ts(), expect, "after step {}", step);
        }
        prop_assert_eq!(seq.visible_ts(), n);
    }
}
