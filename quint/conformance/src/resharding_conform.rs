// Resharding conformance test.

use crate::InvariantResult;
use std::collections::{HashMap, HashSet};

pub struct ShardCluster {
    pub shard_keys: HashMap<u64, HashSet<u64>>,
    pub total_keys: usize,
}

impl ShardCluster {
    pub fn new(num_shards: usize, num_keys: usize) -> Self {
        let mut shard_keys = HashMap::new();
        let keys_per_shard = num_keys / num_shards;
        for s in 0..num_shards {
            let start = s * keys_per_shard + 1;
            let end = if s == num_shards - 1 {
                num_keys
            } else {
                (s + 1) * keys_per_shard
            };
            let keys: HashSet<u64> = (start as u64..=end as u64).collect();
            shard_keys.insert(s as u64, keys);
        }
        Self {
            shard_keys,
            total_keys: num_keys,
        }
    }

    pub fn check_no_data_loss(&self) -> InvariantResult {
        let all_owned: HashSet<u64> = self.shard_keys.values().flatten().copied().collect();
        let expected: HashSet<u64> = (1..=self.total_keys as u64).collect();
        let missing: Vec<_> = expected.difference(&all_owned).collect();
        InvariantResult {
            name: "no_data_loss".to_string(),
            holds: missing.is_empty(),
            message: if missing.is_empty() {
                None
            } else {
                Some(format!("Missing keys: {missing:?}"))
            },
        }
    }

    pub fn check_no_double_ownership(&self) -> InvariantResult {
        let shards: Vec<_> = self.shard_keys.keys().collect();
        for i in 0..shards.len() {
            for j in (i + 1)..shards.len() {
                let overlap: Vec<_> = self.shard_keys[shards[i]]
                    .intersection(&self.shard_keys[shards[j]])
                    .collect();
                if !overlap.is_empty() {
                    return InvariantResult {
                        name: "no_double_ownership".to_string(),
                        holds: false,
                        message: Some(format!(
                            "Shards {} and {} share keys: {overlap:?}",
                            shards[i], shards[j]
                        )),
                    };
                }
            }
        }
        InvariantResult {
            name: "no_double_ownership".to_string(),
            holds: true,
            message: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_invariants() {
        let cluster = ShardCluster::new(4, 100);
        assert!(cluster.check_no_data_loss().holds);
        assert!(cluster.check_no_double_ownership().holds);
    }
}
