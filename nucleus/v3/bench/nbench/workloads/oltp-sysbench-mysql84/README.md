# oltp-sysbench-mysql84

sysbench 1.0.20 (Debian package — the build carries the MySQL driver as
well as PostgreSQL, verified against the live 8.4.13 container before
this workload was written, so `systems/sysbenchcli/` is untouched)
against MySQL 8.4 (InnoDB): `oltp_read_write` and
`oltp_point_select`, threads 1/16/64, 10 tables x 100k rows,
`--time=60`, `--percentile=99`.

The row mirrors `oltp-sysbench-pg17` cell for cell (the same sweep,
the same metrics, `reinit: true` per series), with the server
durability-matched to the PG row through the system's base args
(`--innodb_flush_log_at_trx_commit=1`, the per-commit flush; PLAN-v3
§5.2 rule 3) and no binlog (`--skip-log-bin` — the card's
`--log_bin=0` spelling is silently ignored by MySQL 8.4, verified
2026-11-27; see `systems/mysql84/README.md`).

`--rand-seed=42` is passed for provenance, but note: sysbench's prepare
is not value-reproducible across runs (the pg row's rationale, verified
there 2026-10-18). The digest therefore pins the dataset shape (row
counts of every sbtest table), and each series re-creates the dataset
so every config point starts from the same shape and update-state.

sysbench runs in its own container over `--network host`; the server
runs on port 3307 (mysql84's `SystemDef::port()`), and the readiness
probe is a TCP `mysqladmin ping` carrying that port — never the
container's unix socket, which the entrypoint's temporary init server
also answers (see the probe unit test in `src/runner.rs`).

The smoke (`smoke.sh`) is a tagged-smoke structural check — not a
measurement; the full matrix runs later, before C-9's OLTP row
(PLAN-v3 §7.7.4).
