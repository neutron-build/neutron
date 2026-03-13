// MVCC Ghost Specification Types
//
// These types exist only during verification (ghost state).
// They model the abstract MVCC properties we want to prove.

#[cfg(verus_keep_ghost)]
mod spec {
    // verus! {
    //     /// Abstract specification of snapshot visibility.
    //     /// This is the "what" — the verified code must satisfy this spec.
    //     spec fn visible_spec(start_ts: u64, commit_ts: u64, delete_ts: u64) -> bool {
    //         commit_ts > 0
    //         && commit_ts <= start_ts
    //         && (delete_ts == 0 || delete_ts > start_ts)
    //     }
    //
    //     /// Ghost set tracking all committed transaction IDs.
    //     pub tracked struct CommittedSet {
    //         pub ghost txns: Set<u64>,
    //     }
    //
    //     /// Ghost map tracking version chains per row.
    //     pub tracked struct VersionChains {
    //         pub ghost chains: Map<u64, Seq<(u64, u64)>>,  // row_id -> [(commit_ts, delete_ts)]
    //     }
    //
    //     impl VersionChains {
    //         /// A version chain is well-formed if timestamps are monotonically increasing
    //         /// and no two live versions overlap.
    //         spec fn well_formed(&self) -> bool {
    //             forall|row_id: u64| self.chains.contains_key(row_id) ==>
    //                 self.chain_ordered(row_id) && self.no_overlap(row_id)
    //         }
    //
    //         spec fn chain_ordered(&self, row_id: u64) -> bool {
    //             let chain = self.chains[row_id];
    //             forall|i: int, j: int| 0 <= i < j < chain.len() ==>
    //                 chain[i].0 < chain[j].0
    //         }
    //
    //         spec fn no_overlap(&self, row_id: u64) -> bool {
    //             let chain = self.chains[row_id];
    //             forall|i: int, j: int| 0 <= i < j < chain.len() ==>
    //                 chain[i].1 > 0 && chain[i].1 <= chain[j].0
    //         }
    //     }
    // }
}

// Executable placeholder for non-verus builds
pub struct MvccSpec;

impl MvccSpec {
    /// Check the abstract visibility specification.
    pub fn visible(start_ts: u64, commit_ts: u64, delete_ts: u64) -> bool {
        commit_ts > 0 && commit_ts <= start_ts && (delete_ts == 0 || delete_ts > start_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec_matches_impl() {
        // Visible: committed before snapshot, not deleted
        assert!(MvccSpec::visible(10, 5, 0));
        // Invisible: uncommitted
        assert!(!MvccSpec::visible(10, 0, 0));
        // Invisible: committed after snapshot
        assert!(!MvccSpec::visible(10, 15, 0));
        // Invisible: deleted before snapshot
        assert!(!MvccSpec::visible(10, 3, 7));
        // Visible: deleted after snapshot
        assert!(MvccSpec::visible(10, 3, 15));
    }
}
