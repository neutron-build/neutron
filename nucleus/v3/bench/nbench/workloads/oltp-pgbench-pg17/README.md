# oltp-pgbench-pg17

pgbench TPC-B (builtin `tpcb-like`) against PostgreSQL 17, scale 100,
clients 1/16/64, both `synchronous_commit` variants (matched durability
pairs, PLAN-v3 §5.2 rule 3).

- Driver: `driver.sh` (init / cold / warmup / run)
- Digest: `digest.sh` (init strict, final recorded — see its header)
- Expected digests: `expected/init.txt` (recorded on the first run;
  determinism verified by re-running init before the recorded baseline)

pgbench runs inside the server container over the unix socket, so the
client cost is contained in the measured system (documented in the row's
results note). Per-transaction latencies come from `--log` files; p50 /
p99 / p999 are computed by the harness from those files, tps from the
summary line.
