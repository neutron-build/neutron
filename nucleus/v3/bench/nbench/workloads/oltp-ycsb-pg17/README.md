# oltp-ycsb-pg17

YCSB 0.17.0 core workloads A–F against PostgreSQL 17 through the JDBC
binding over pgwire — the row C-9's OLTP row later reuses against
Nucleus (both speak pgwire, PLAN-v3 §3.7). Landed by card NB-2; the
Nucleus column is `n/a` until C-9.

- Driver: `driver.sh` (init / reinit / cold / warmup / run)
- Digest: `digest.sh` (init strict, final recorded — pins the dataset
  *shape*, the usertable row count; see its header)
- Smoke: `smoke.sh` — the card's Host smoke line, run by the gate on a
  fresh clone (workload A, then workload D; one thread, 1 000 records,
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
rule) and `threadcount` (the series' thread count). Verified live on
the first smoke run:

- the **load phase also requires a workload file** (`-P
  workloads/workloada`): YCSB 0.17.0 refuses to start without the
  `workload` property ("Missing property: workload") — the fix is in
  `driver.sh`'s init/reinit arm and recorded in the card's RESULT;
- the binding does NOT auto-create the table — `driver.sh` init/reinit
  creates `usertable` with the README's exact schema (`YCSB_KEY
  VARCHAR(255) PRIMARY KEY, FIELD0..FIELD9 TEXT`) and truncates before
  each load so every series starts from the same pristine state.

## What the driver guarantees

**No pass with a failed operation is published.** YCSB exits 0 whatever
its operations returned, so the driver reads the report itself: any
`[<OP>], Return=<STATUS>` line other than `OK`, any `[<OP>-FAILED]`
section, or a report with no successful operation at all fails the
pass, with the report's own lines on stderr and no `NB_METRICS` file
written. `nbench run` then fails the row with that reason and writes no
results file (the card asks for `"voided": true`; a results document
with that field is the runner's to write — `src/runner.rs`, outside
this card's Touch only list — so the driver's loud failure, the
harness's existing path for a missing throughput line, is what this
card can produce; see its RESULT).

**Every pass starts from the digested dataset.** YCSB 0.17.0 restarts
its run-phase insert key sequence at `recordcount` in every process
(read from `core-0.17.0.jar`: the first insert keynums are
`recordcount`, `recordcount+1`, … whatever `insertstart`/`insertcount`
say — those two move the load and the request key ranges, not this
counter). Stock D and E insert (5 %), so without more the second pass
of a series would re-insert the first pass's keys and fail on
`usertable_pkey` — which is what the NB-2 verdict found (R2). A
measured pass therefore counts `usertable` first: exactly `recordcount`
rows is pristine and the pass runs; more rows means an earlier pass of
the series inserted, and the driver truncates and reloads the full
`recordcount` (the reinit's own steps, logged to `<pass log>.reload`)
before running; fewer rows is a lost dataset and fails the pass. The
non-inserting workloads (A, B, C, F) never trip the rule, so it costs
nothing there; for D and E each pass after the cold one costs one
reload.

**The published numbers are the driver's own, and they are under
test.** The row parses driver-side through the NB_METRICS protocol (the
`VectorSearch` arm): the runner's log-parser dispatch table is outside
this card's Touch only list, so there is no parser variant for this
row. `driver.sh` extracts the `[OVERALL]`/`[READ]` lines the
fixture-pinned reference parser `parse_ycsb` (parse.rs) reads, fails
loudly when the mandatory throughput line is missing, and omits the
read-latency metrics for mixes without reads. Because those `sed`
lines are the row's numbers, `parse.rs` executes the real `driver.sh`
over the recorded fixtures with a stub `nb-podman` and requires its
output to equal the reference parser's (verdict NB-2, rework 1); the
same tests run the failed-operation guard and the restore rule.

## Notes

- **The smoke derives its own environment** (the Host smoke line runs
  it bare — the NB-3 smokes' rule): `smoke.sh` derives ROOT and the
  nbench binary path itself. The real-row run is the gate's (card NB-2,
  Opus ruling ruling-NB-containment.md, 2026-11-27): the worker's
  sandbox cannot start real rows, and the smoke holds no relay and no
  derived transport — the row runs and the checks only, under the
  host's real user manager.
- **The smoke's second run is workload D** (verdict NB-2, R2 fix item
  3): A has no inserts, so A alone can never see a failed insert. D,
  with a warmup and two warm passes, exercises the guard and the
  restore at row level — the smoke checks each D pass log for
  successful inserts only, the cold pass for running on the pristine
  table, and each later pass for having reloaded the full recordcount
  first.
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
- Per-workload sections (real 0.17.0 reports): A and B carry
  `[READ]`+`[UPDATE]`, C `[READ]`, D `[READ]`+`[INSERT]`, E
  `[SCAN]`+`[INSERT]`, F `[READ]`+`[READ-MODIFY-WRITE]`+`[UPDATE]` — F
  reads (its plain reads plus the read half of each read-modify-write)
  and inserts nothing. Only E's series lack `avg_read_us`/`p99_read_us`;
  absent fields are omitted, never invented.
- The fixtures under `src/fixtures/ycsb-*.txt` that the driver tests
  replay are described in `src/parse.rs` (`mod driver`);
  `src/fixtures/ycsb-standin/` is how the offline ones were recorded.
