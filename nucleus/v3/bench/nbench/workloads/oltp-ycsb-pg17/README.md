# oltp-ycsb-pg17

YCSB 0.17.0 core workloads A–F against PostgreSQL 17 through the JDBC
binding over pgwire — the row C-9's OLTP row later reuses against
Nucleus (both speak pgwire, PLAN-v3 §3.7). Landed by card NB-2; the
Nucleus column is `n/a` until C-9.

- Driver: `driver.sh` (init / cold / warmup / run)
- Digest: `digest.sh` (init strict, final recorded — pins the dataset
  *shape*, the usertable row count; see its header)
- Smoke: `smoke.sh` (workload A, one thread, 1 000 records,
  1 000 operations, `--tag smoke` — §7.7.4 smoke-size form; never a
  measurement)
- Expected digests: `expected/init-smoke.txt` (recorded by the first
  smoke run; gitignored, like every smoke expectation)

## The verified property set (card NB-2 Open point)

Verified live on the first smoke run and against the 0.17.0 tarball's
own `jdbc-binding/README.md`:

```text
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://127.0.0.1:5432/nbench
db.user=postgres
db.passwd=postgres
```

plus the core properties `recordcount` (dataset size, `-p` overriding
the stock file's 1000), `operationcount` (ops per pass, same override
rule) and `threadcount` (the series' thread count). The binding does
NOT auto-create the table — `driver.sh` init/reinit creates
`usertable` with the README's exact schema (`YCSB_KEY VARCHAR(255)
PRIMARY KEY, FIELD0..FIELD9 TEXT`) and truncates before each load so
every series starts from the same pristine state.

## Notes

- The launcher is `bin/ycsb.sh` (POSIX sh, same tarball); the `bin/ycsb`
  launcher is Python 2 and no current base image carries it — see
  `../../systems/ycsbcli/README.md`.
- YCSB runs in its own container over `--network host` (the
  sysbenchcli shape); the server runs with the image default
  `synchronous_commit=on` — no off-variant for this row: the durability
  pairing is owned by the pgbench row, and the fixed setting is
  recorded in the row's provenance.
- `versions.txt` (mandatory) records `postgres --version`, the server's
  `SELECT version()`, the client's `java -version` and the YCSB release
  string.
- Per-workload sections: A/B/C carry `[READ]` (avg/p99 read latencies
  reported), D carries `[READ]`+`[INSERT]`, E is `[SCAN]`+`[INSERT]`,
  F is `[READ-MODIFY-WRITE]`+`[INSERT]` — the read-latency metrics are
  simply absent for E and F, never invented.
