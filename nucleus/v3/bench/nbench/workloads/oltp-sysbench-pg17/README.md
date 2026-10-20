# oltp-sysbench-pg17

sysbench 1.0.20 (Debian package, pgsql driver — Alpine's build rejects
`--pgsql-*` options) against PostgreSQL 17: `oltp_read_write` and
`oltp_point_select`, threads 1/16/64, 10 tables x 100k rows,
`--time=60`, `--percentile=99`.

`--rand-seed=42` is passed for provenance, but note: sysbench's prepare
is not value-reproducible across runs (verified empirically; three
identical prepares differ). The digest therefore pins the dataset shape
(row counts), and each series re-creates the dataset so every config
point starts from the same shape and update-state.

sysbench runs in its own container over `--network host`; the server
runs with the default `synchronous_commit=on` (no off-variant for this
row: the durability pairing is exercised by the pgbench row; noted in
the card RESULT).
