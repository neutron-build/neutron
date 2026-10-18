# oltp-sysbench-pg17

sysbench 1.0.20 (Alpine package, pgsql driver) against PostgreSQL 17:
`oltp_read_write` and `oltp_point_select`, threads 1/16/64, 10 tables x
100k rows, `--rand-seed=42` (deterministic dataset, checked by the
strict init digest), `--time=60`, `--percentile=99`.

sysbench runs in its own container over `--network host`; the server
runs with the default `synchronous_commit=on` (no off-variant for this
row: the durability pairing is exercised by the pgbench row; noted in
the card RESULT).
