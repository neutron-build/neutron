// Online Resharding — moving data between shards while the system is live.
module resharding {
    import types.*

    pure val SHARDS: Set[ShardId] = Set(1, 2, 3, 4)
    pure val ALL_KEYS: Set[Key] = 1.to(100)

    type ReshardPhase = str  // "idle" | "preparing" | "copying" | "catching_up" | "switching" | "cleaning"

    var shard_keys: ShardId -> Set[Key]
    var reshard_phase: ReshardPhase
    var source_shard: ShardId
    var target_shard: ShardId

    action init = all {
        // Distribute keys evenly across shards
        shard_keys' = (s =>
            if (s == 1) 1.to(25)
            else if (s == 2) 26.to(50)
            else if (s == 3) 51.to(75)
            else 76.to(100)),
        reshard_phase' = "idle",
        source_shard' = 0,
        target_shard' = 0,
    }

    // Begin resharding: move some keys from source to target
    action begin_reshard(src: ShardId, tgt: ShardId): bool = all {
        reshard_phase == "idle",
        src.in(SHARDS),
        tgt.in(SHARDS),
        src != tgt,
        shard_keys(src).size() > 0,
        reshard_phase' = "preparing",
        source_shard' = src,
        target_shard' = tgt,
        shard_keys' = shard_keys,
    }

    // Prepare phase → copying phase
    action start_copying: bool = all {
        reshard_phase == "preparing",
        reshard_phase' = "copying",
        shard_keys' = shard_keys,
        source_shard' = source_shard,
        target_shard' = target_shard,
    }

    // Copy a key from source to target
    action copy_key(k: Key): bool = all {
        reshard_phase == "copying",
        k.in(shard_keys(source_shard)),
        shard_keys' = shard_keys
            .set(source_shard, shard_keys(source_shard).exclude(Set(k)))
            .set(target_shard, shard_keys(target_shard).union(Set(k))),
        reshard_phase' = reshard_phase,
        source_shard' = source_shard,
        target_shard' = target_shard,
    }

    // Finish copying → switching
    action finish_reshard: bool = all {
        reshard_phase == "copying",
        reshard_phase' = "idle",
        shard_keys' = shard_keys,
        source_shard' = 0,
        target_shard' = 0,
    }

    action step = any {
        SHARDS.exists(s => SHARDS.exists(t => begin_reshard(s, t))),
        start_copying,
        ALL_KEYS.exists(k => copy_key(k)),
        finish_reshard,
    }

    // === SAFETY INVARIANTS ===

    // No data loss: every key is owned by exactly one shard
    val no_data_loss: bool =
        ALL_KEYS.forall(k =>
            SHARDS.exists(s => k.in(shard_keys(s)))
        )

    // No double ownership: no key owned by two shards
    val no_double_ownership: bool =
        ALL_KEYS.forall(k =>
            SHARDS.filter(s => k.in(shard_keys(s))).size() <= 1
        )

    // Total key count is conserved
    val key_conservation: bool =
        SHARDS.fold(0, (acc, s) => acc + shard_keys(s).size()) == ALL_KEYS.size()
}
