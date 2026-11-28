# kv-memtier-dragonfly

memtier_benchmark GET/SET 1:10 against Dragonfly v1.27.1 on port 6382,
pipeline 1/16 — the same memtier parameters as `kv-memtier-redis7`
(16 threads x 4 clients, 32-byte values, 10M key range, random keys).

**Durability-unmatched.** The pinned release (v1.27.1) exposes no
AOF-equivalent fsync flag — verified against the image's own `--help`
(133 flags: snapshots only; see `systems/dragonfly/README.md`) — so the
row runs one fixed durability configuration, `variants` is empty, the
sweep is pipeline-only (`sweep_kv_fixed` in `src/rows.rs`), and this
row's number must not be read against the AOF-matched (always/everysec)
Redis/Valkey pairs: PLAN-v3 §5.2 rule 3, stated, never silently
relaxed. The row's report line carries the same note.

Whether an unmatched Dragonfly number may carry a §5.3 label at all is
the C-9/D report's call, not this row's.

The digest uses the strict verification-key protocol of the other KV
rows, through the `redis-cli` 6.0.16 the Dragonfly image itself ships.
The smoke (`smoke.sh`) is a tagged-smoke structural check — not a
measurement.
