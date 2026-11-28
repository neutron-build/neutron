Card: NB-2
Class: bench
Card-gate: S
Verdict: MERGE
Gate-tier: S
Gate-model: zai-coding-plan/glm-5.4 (max)
Gate-session: opencode review sub-agent, T3 thread 45584af2-e651-490e-862c-3402320cffa3 (the confirming S gate after rework 1; a fresh agent that wrote none of the code, the card or the rework, and did not write the REWORK verdict dbc77fe6 it confirms — an earlier sub-agent of the same thread did)
Date: 2026-11-28
Gate-exit: GATE EXIT=0
Commit: 80e20430709cee2cf679215e3a91a1e38d95f359
Mutants: SWEEP-NOT-CROSS (zip) -> caught: rows::tests::ycsb_plan_crosses_workloads_with_threads_and_reinits_each_series FAILED at assert_eq!(series.len(), 18) (left: 3, right: 18) | SWEEP-NOT-CROSS (thread dimension dropped) -> caught: same test and assertion (left: 6, right: 18) | REINIT-DROPPED -> caught: same test FAILED at assert_eq!(reinits, 18, "one reinit per series") (left: 0, right: 18) | PARSER-OPS-DROPPED (reference parse_ycsb) -> caught: 8 tests, 148/8 | PARSER-OPS-DROPPED (real path, driver.sh:184 deleted) -> caught: 4 tests, 152/4, "driver.sh: line 186: ops: unbound variable" | PARSER-OPS-DROPPED (real path, driver.sh:184 reads RunTime(ms) instead of Throughput) -> caught: 4 tests, 152/4, left ops_sec 357.0, right 2801.1204481792715 | PARSER-P99-AS-AVG (reference parse_ycsb) -> caught: 8 tests, 148/8 | PARSER-P99-AS-AVG (real path, driver.sh:186) -> caught: 4 tests, 152/4; driver_publishes_the_reference_parsers_numbers: left ("p99_read_us", 127.0767754318618), right ("p99_read_us", 440.0) | VERSIONS-SKIPPED -> caught: driver_init_records_versions_then_loads FAILED ("versions.txt written"), 155/1; and by the Host smoke on the mutant's extract, exit 1, "versions.txt not written by the driver" | DIGEST-CONSTANT -> caught: digest_hashes_the_servers_own_row_count FAILED, digest of a 1500-row table left 83c02ac2…, right d6e02e54…, 155/1 | RETURN-ERROR-IGNORED (guard call removed from the measured arm) -> caught: driver_fails_a_pass_whose_operations_failed FAILED ("a pass with failed operations must fail the driver"), 155/1 | RETURN-ERROR-IGNORED (condition inverted) -> caught: same test among 6, 150/6 | gate's own: p99 read from the 95th percentile (driver.sh:186) -> caught: driver_publishes_the_reference_parsers_numbers, 155/1 | RESTORE-DROPPED (driver.sh:150) -> caught: 2 tests, 154/2, and on the real row exit 1 with no results file | TRUNCATE-DROPPED (driver.sh:79-80) -> caught: 2 tests, 154/2 | OPS-GUARD-DROPPED (driver.sh:187-190; not a card mutant) -> UNCAUGHT in cargo test, 156/156; the row still fails in the runner's JSON parse (NIT N2)
Followups: none

## Verdict in brief

**MERGE** on `80e20430709cee2cf679215e3a91a1e38d95f359`, the rework-1 tip (six
commits, `682ea52e..80e20430`, on the REWORK verdict `dbc77fe6`). Both defects
are fixed, each is held by a test that runs the row's real code, and both were
confirmed again on the real row with containers:

- **R1.** The numbers the row publishes are the driver's own `sed` lines
  (driver.sh:184-194), and `cargo test` now executes that script over the
  recorded fixtures and holds its output to the reference parser.
  `PARSER-P99-AS-AVG` on that path fails 4 tests (left 127.07…, right 440.0);
  it was UNCAUGHT before.
- **R2.** A pass with any `Return=` other than `OK`, any `-FAILED` section, or
  no successful operation fails the driver: the report's own lines go to
  stderr, no `NB_METRICS` file is written, and `nbench run` fails the row with
  exit 1 and no results JSON. The cause is fixed too: a measured pass that
  finds more than `recordcount` rows reloads first. Real runs of D and E at
  smoke size show zero `Return=ERROR` lines in eight passes.

One thing is **not** delivered as the card's Rework 1 text words it: a
`"voided": true` results document. It cannot be produced from the card's Touch
only list, the REWORK verdict's fix 1 named "the driver must fail the pass" as
the first acceptable form, and in this harness `voided` means a digest
mismatch. Details under "R2" below. It is a card-text NIT, not a defect.

## R1 — the published numbers come from code a test reads

- What publishes: `workloads/oltp-ycsb-pg17/driver.sh:184-194` writes the
  NB_METRICS JSON (`ops_sec` from `[OVERALL], Throughput(ops/sec)`,
  `avg_read_us` and `p99_read_us` from `[READ]`). `parse_ycsb`
  (parse.rs:273) stays `#[cfg(test)]` and is documented as the reference
  parser the driver is held to. The card's second option, as allowed.
- What reads it: `parse.rs` `mod driver` (`#[cfg(all(test, unix))]`,
  parse.rs:1045) runs the real `driver.sh` with `Command::new("bash")`, a
  cleared environment and a stub `nb-podman` first on `PATH`, so no call can
  start a container. `driver_publishes_the_reference_parsers_numbers`
  requires the driver's NB_METRICS to equal `parse_ycsb(fixture)` in all three
  measured modes, and to carry the log's own tokens:
  `"ops_sec":2801.1204481792715`, `"avg_read_us":127.0767754318618`,
  `"p99_read_us":440`.
- Mutants, each in its own fresh `git archive | tar -x -m` extract of the
  pinned commit, on a copy of the pristine target without nbench's artifacts
  (the rebuilt binary's mtime was checked for every mutant), running only
  `cargo test -q -p nbench`:
  - `PARSER-P99-AS-AVG`, driver.sh:186 (`99thPercentileLatency(us)` →
    `AverageLatency(us)`): **caught**, 152 passed / 4 failed:
    `driver_publishes_the_reference_parsers_numbers` —
    `left: […, ("p99_read_us", 127.0767754318618)]`,
    `right: […, ("p99_read_us", 440.0)]`; also
    `driver_publishes_only_the_metrics_a_mix_carries`,
    `driver_reloads_before_a_pass_an_earlier_pass_grew`,
    `driver_carries_an_inserting_series_pass_by_pass`.
  - A subtler form of the same fault, p99 read from
    `95thPercentileLatency(us)` (gate's own): **caught**, 155/1, left 279.0,
    right 440.0 — the test compares values, not only presence.
  - `PARSER-OPS-DROPPED`, driver.sh:184 deleted: **caught**, 152/4 —
    `driver.sh: line 186: ops: unbound variable` in the same four tests.
  - `PARSER-OPS-DROPPED`, driver.sh:184 reads `[OVERALL], RunTime(ms)`
    instead of `Throughput(ops/sec)` (gate's own form, so the guard cannot
    catch it by emptiness): **caught**, 152/4 — left `("ops_sec", 357.0)`,
    right `("ops_sec", 2801.1204481792715)`.
  - Both mutants on the reference `parse_ycsb`: caught, 148/8 each.
- On the real row (Host smoke and the D/E runs below): every one of the 14
  pass metrics files equals its own pass log's lines, extracted independently
  by this gate (the three for A and D; `ops_sec` alone for E, whose report
  has no `[READ]` section).

## R2 — a pass with a failed operation is never published, and inserts no longer fail

**The guard.** `assert_all_ok` (driver.sh:96-115) fails unless every
`Return=` line is `[<OP>], Return=OK, <n>`, there is no `[<OP>-FAILED]`
section, and at least one `Return=OK` count is non-zero; a missing or empty
log fails too. It runs
after every measured pass (driver.sh:171), after the init/reinit load (:134)
and after a reload (:159). `driver_fails_a_pass_whose_operations_failed`
feeds it the recorded `ycsb-run-d-failed.txt`: the driver must fail, write no
NB_METRICS and name `[INSERT], Return=ERROR, 44` and
`[INSERT-FAILED], Operations, 44` on stderr; it also covers a report with no
`Return=OK`, one whose OK counts are zero, and an empty log.

- `RETURN-ERROR-IGNORED`, guard call removed from the measured arm
  (driver.sh:171): **caught**, 155/1.
- `RETURN-ERROR-IGNORED`, condition inverted (driver.sh:105): **caught**,
  150/6, including the same test.

**The cause.** Every `ycsb run` is a new process whose insert keys restart at
`recordcount`, so a pass after an inserting one re-inserted its keys. Now a
measured pass counts `usertable` first (driver.sh:141-160): `recordcount`
rows is pristine and the pass runs; more means an earlier pass inserted, and
the driver truncates and reloads before running; fewer is a lost dataset and
fails. `driver_reloads_before_a_pass_an_earlier_pass_grew` pins the order
(count, truncate, reload, run) and both refusals;
`driver_carries_an_inserting_series_pass_by_pass` replays reinit, cold,
warmup, run, run against a table the stub grows.

- `RESTORE-DROPPED` (gate's own; `if false` at driver.sh:150): **caught**,
  154/2 (`driver_reloads_before_a_pass_an_earlier_pass_grew`,
  `driver_carries_an_inserting_series_pass_by_pass`).
- The same mutant on the **real row**, workload D, smoke size (cold, one
  warmup, two runs): the cold pass inserted 55; the warmup pass ran on the
  grown table and got 47 duplicate-key errors. The run now **fails** — exit 1:

  ```
  row oltp-ycsb-pg17 FAILED: command failed (driver oltp-ycsb-pg17 [workload=d,threads=1-warmup-0], exit 1): YCSB reported failed operations in …/workload=d,threads=1-warmup-0.log:
  [INSERT], Return=ERROR, 47
  [INSERT-FAILED], Operations, 47
  [INSERT-FAILED], AverageLatency(us), 386.0851063829787
  …
  ```

  No results JSON was written. That is the REWORK verdict's R2 failure,
  reproduced and refused.
- `TRUNCATE-DROPPED` in the reload (gate's own, driver.sh:79-80): **caught**,
  154/2 ("table truncated"; the stub's table reaches 2088 rows).

**On `"voided": true`.** The card's Rework 1 asks for a results document with
`"voided": true`; the card's Limits keep `src/runner.rs` outside Touch only
(the scope ruling already reverted an edit there, `7d726d26`). Only the
runner writes a results document, and it sets `voided` on a strict digest
mismatch (runner.rs:57, README rule 4; the report prints "VOID (digest
mismatch)", report.rs:88). From the Touch only list, a driver can refuse a
pass in one way: exit non-zero. That is what the REWORK verdict's fix 1 named
first ("The driver must fail the pass (or the runner must void it)"), and it
is the harness's existing path for every driver-detected fault, a missing
throughput line included (the REWORK verdict's "same loud rule"). The delivered
behaviour — exit 1, a non-ok status, the report's own lines as the reason,
nothing published — meets R2's substance ("never publish it as normal") and
PLAN §5.2 ("A result that breaks one is not published"). The worker raised
the gap as ESCALATE 1 rather than editing the card or `runner.rs`, which was
right. If the owner wants a voided document for driver-refused passes, that
is a card-text change plus `runner.rs` (and a status text that is not "digest
mismatch") in some card's Touch only: a NIT here, not a defect in this card.

## Gate run (fresh, on the tip)

GATE-PROMPT step 1: the scaffold's own `fresh-gate.sh` extracted through a
shallow `--no-local` clone of `nucleus/v3-scaffold` (scaffold id
`f7e6c715bcad079814eac363232528d0aac72ab9`; `fresh-gate.sh` sha256
`40a5b232a5ac2c0cd1629ab6f7aa24396e1a6379d36db536a96a84ba35404899`,
`git-env.sh` `8a784782b42b094ff82e5ad1b497feadab49e2a2f05328d765ae5eca0b073977`),
run at once under `systemd-run --user --scope --quiet -p CPUWeight=10000 --`
with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-NB-2`. The scope worked; no
plain fallback was needed. 09:40:08Z to 09:49:27Z.

```
fresh-gate: commit 80e20430709cee2cf679215e3a91a1e38d95f359
fresh-gate: base f7e6c715bcad079814eac363232528d0aac72ab9
fresh-gate: checks passed (5010 files hashed in 1.53 s)
fresh-gate: 2 file(s) matched through their eol conversion
STEP test EXIT=0
STEP clippy EXIT=0
STEP fmt EXIT=0
STEP deny EXIT=0
STEP wasm32 EXIT=0
STEP core-paths-selftest EXIT=0
STEP enforcement-tests SKIPPED (no scripts/ or tools/ change)
STEP api-surface EXIT=0
GATE EXIT=0
```

The literal last line was `GATE EXIT=0`; stderr was empty; the wrapper's exit
status was 0. The test step ran 72 binaries: 935 passed, 0 failed, 1 ignored
(a pre-existing `#[ignore]`); nbench's own binary reports 156 passed. No
known load or zombie row (FAST-TRACK §3) failed and no external SIGTERM
arrived, so there was nothing to re-run. The enforcement skip is what
ENF-FASTGATE-1 gives a change that touches no `scripts/` or `tools/` file.
The scaffold extract was removed afterwards.

## Host smoke (GATE-PROMPT "Host smoke")

Run plainly in this gate's own shell, from the root of a fresh
`git clone --no-local` of the pinned commit (shallow, single-branch
`card/v3-NB-2`; HEAD `80e20430709cee2cf679215e3a91a1e38d95f359`, tree
`426f2e34ac03e36daa015aecc21e758e0705257b`; no hooks, filters or includes),
never the worker's worktree. The only environment change is `PATH` gaining
`~/.cargo/bin`, because this tool shell has no cargo and `smoke.sh` calls
`cargo build`.

| Line | Exit | Last line |
|---|---|---|
| `cd nucleus/v3/bench/nbench && bash workloads/oltp-ycsb-pg17/smoke.sh` | 0 | `SMOKE OK: report at results/smoke/2026-11-28/REPORT.md` |

Last lines (19 s wall, the nbench build included; long paths shortened with `…`):

```
--- run the tiny row again (workload D: inserts, one cold + one warmup + two warm passes)
=== nbench run oltp-ycsb-pg17 (tag smoke)
results: …/results/smoke/2026-11-28/oltp-ycsb-pg17.smoke.json
report:  …/results/smoke/2026-11-28/REPORT.md
--- generate the report
…/results/smoke/2026-11-28/REPORT.md
SMOKE OK: report at results/smoke/2026-11-28/REPORT.md
```

Checked beyond the exit code:

- Workload A (one cold, one warm pass): every operation `Return=OK`
  (`[READ]` 466 / `[UPDATE]` 534, then 473 / 527).
- Workload D (Rework 1, the REWORK verdict's fix 3; one cold, one warmup, two
  warm passes): `[INSERT], Return=OK, 54`, then 58, 46 and 57; no `Return=`
  line other than OK and no `-FAILED` section in any log. The cold pass
  follows the reinit and did not reload; each later pass has a `.reload` log
  with `[INSERT], Return=OK, 1000`. The smoke checks all of this itself
  (`check_pass_log`).
- The results JSON has `"voided":false` and `"anomalies":[]`; the report reads
  `Status: ok` and `workload=d threads=1 | warm (n=2) | 6292 [6173, 6410]`.
- The init digest was recorded as `83c02ac2d48c863dab2ccf6870455aadfc2cec073b8db269b517c879d76aa6d9`,
  which is sha256 of a 1 000-row count line. The final digest
  `99072d8301ebb844f090bb3086f663a4b49d9fdee059df59eea9271623860225` is
  sha256 of 1 057 rows (the last pass's 57 inserts) — recorded, non-strict,
  as the row's `final: false` intends.
- All three metrics are the logs' own values: D cold 4854.37 ops/s,
  97.09 µs, 303 µs; warm samples 6410.26 and 6172.84.
- Provenance carries the four YCSB params, the postgres17 image digest,
  `versions` (PostgreSQL 17.11 twice, Temurin 17.0.21.1, `YCSB release:
  ycsb-0.17.0`) and `nbench_build`.
- The clone stayed git-clean; only the gitignored `results/smoke/`, `work/`,
  `expected/init-smoke.txt` and `target/` were written.
- The image build used cached layers (`nbench-ycsbcli:local` `9039ff9f4c21`,
  the same image the REWORK gate ran), so the fetch-and-verify step did not
  run again here. The pins were checked separately, below.

Smoke, not a measurement (§7.7.3 item 5). Host load1 was 0.86 at the start
and 1.03 at the end; the fresh-gate had already finished.

**`VERSIONS-SKIPPED` through the same smoke** (the card's named catcher for
that mutant, run on the mutant's own fresh extract with driver.sh:125 writing
to `/dev/null`): exit 1, no results file written, last line (path shortened
with `…`):

```
nbench: command failed (nbench run, exit 1): oltp-ycsb-pg17: missing field or wrong type: versions.txt not written by the driver (…/work/2026-11-28/oltp-ycsb-pg17-smoke/versions.txt): No such file or directory (os error 2) — every row's init mode must record tool versions (§5.2 rule 6)
```

## Workloads D and E for real (the brief's R2 check, outside the card's Acceptance)

A second fresh `git clone --no-local` of the pinned commit (same HEAD and
tree), `--tag smoke`, smoke size (1 thread, 1 000 records, 1 000
operations), one cold pass, one warmup and two warm passes each:

```
nbench run oltp-ycsb-pg17 --tag smoke --set ycsb_workloads=d --set ycsb_threads=1 \
  --set recordcount=1000 --set ycsb_operations=1000 --set runs=2 --set cold_runs=1 --set warmup_runs=1
```

and the same with `ycsb_workloads=e`.

| Workload | Exit | `[INSERT], Return=OK` per pass (cold, warmup, run-0, run-1) | `Return=ERROR` lines | Reloads | Results |
|---|---|---|---|---|---|
| d | 0 | 55, 47, 41, 50 (with `[READ]` OK 945, 953, 959, 950) | 0 | 3, each `[INSERT], Return=OK, 1000` | `"voided":false`, `Status: ok` |
| e | 0 | 51, 42, 50, 39 (with `[SCAN]` OK 949, 958, 950, 961) | 0 | 3, each `[INSERT], Return=OK, 1000` | `"voided":false`, `Status: ok` |

No log under either run's work directory holds `Return=ERROR`, a `-FAILED`
section, a duplicate-key error or an exception. E's metrics files carry
`ops_sec` alone (no `[READ]` section: omitted, never invented); D's carry all
three metrics, each equal to its log's own lines. `nbench cleanup` ran
afterwards; no nbench container and no `nbench-*` user unit was left behind
(the host's only container is the unrelated 8-month-old `alpine`).

## Mutation evidence

Tier M: the card's mutants, plus `RETURN-ERROR-IGNORED` (Rework 1) and four
of this gate's own on the riskiest logic. Each in its own fresh extract, on a
copied pristine target with nbench's artifacts deleted, running only
nbench's one test binary (the crate has no lib and no dependency). Every
mutant's diff against the pristine file was saved; an unchanged file, a diff
over 80 lines, or a binary not rebuilt after the mutation aborts the run.
Nothing ran in the worker's worktree.

| Mutant | Applied to | Result |
|---|---|---|
| SWEEP-NOT-CROSS (zip) | rows.rs `sweep_ycsb` | caught: `assert_eq!(series.len(), 18)`, left 3, right 18 |
| SWEEP-NOT-CROSS (thread dimension dropped) | rows.rs `sweep_ycsb` | caught: the same assertion, left 6, right 18 |
| REINIT-DROPPED | rows.rs:321 `reinit: false` | caught: rows.rs:923, `one reinit per series`, left 0, right 18 |
| PARSER-OPS-DROPPED | parse.rs `parse_ycsb` | caught: 8 tests, 148/8 |
| PARSER-OPS-DROPPED | driver.sh:184, line deleted (the real path) | caught: 4 tests, 152/4, `ops: unbound variable` |
| PARSER-OPS-DROPPED | driver.sh:184, reads `RunTime(ms)` (real path, gate's form) | caught: 4 tests, 152/4, left `ops_sec` 357.0, right 2801.12 |
| PARSER-P99-AS-AVG | parse.rs `parse_ycsb` | caught: 8 tests, 148/8 |
| PARSER-P99-AS-AVG | driver.sh:186 (the real path) | caught: 4 tests, 152/4, left `p99_read_us` 127.07…, right 440.0 |
| VERSIONS-SKIPPED | driver.sh:125 | caught: `driver_init_records_versions_then_loads`, 155/1; and by the Host smoke, exit 1 |
| DIGEST-CONSTANT | digest.sh:15-17, `printf '1000\n' \| sha256sum …` | caught: `digest_hashes_the_servers_own_row_count`, 155/1 (the 1 500-row case) |
| RETURN-ERROR-IGNORED | driver.sh:171, guard not called | caught: `driver_fails_a_pass_whose_operations_failed`, 155/1 |
| RETURN-ERROR-IGNORED | driver.sh:105, condition inverted | caught: the same test among 6, 150/6 |
| p99 from the 95th percentile (gate's) | driver.sh:186 | caught: `driver_publishes_the_reference_parsers_numbers`, 155/1 |
| RESTORE-DROPPED (gate's) | driver.sh:150 | caught: 154/2; and on the real row, exit 1, no results file |
| TRUNCATE-DROPPED in the reload (gate's) | driver.sh:79-80 | caught: 154/2 |
| OPS-GUARD-DROPPED (gate's) | driver.sh:187-190 | **UNCAUGHT** in `cargo test` (156/156): the driver's missing-throughput guard has no test on the real path. Not a card mutant; the row still fails, in the runner's JSON parse (NIT N2) |

No card mutant and no Rework 1 mutant survives. `DIGEST-CONSTANT`, which the
REWORK gate could only inspect, now has a real catcher.

## Relay containment check (ruling-NB-containment; GATE-PROMPT Host smoke rule)

- Every added line of the code diff (`8a2df81d..80e20430`, `bench/` only,
  2 062 added lines) was scanned for `systemd-run`, `systemctl`, `systemd/`,
  `XDG_RUNTIME_DIR`, `DBUS`, `sd-bus`, `busctl`, `relay`, `bridge`,
  `re-exec`, `python`, `/run/user`, `socket`, `proxy`, `SCM_RIGHTS`,
  `containment`, `socat`, `netcat`, `sudo`, `pkexec`, `setpriv`, `unshare`,
  `nsenter`, `bwrap`. The hits are prose only: YCSB's own
  `DBWrapper: report latency for each error is false …` line in the recorded
  fixtures (it matches `bwrap`), comments about the tarball's Python 2
  launcher (not used), and the smoke's and the workload README's statements
  that the smoke "holds no relay" and "nothing here bridges" the sandbox.
  No calls.
- The four scripts reach podman only through `nb-podman` (`bin/nb-podman` and
  `lib/nb-lib.sh`, untouched by this diff), the approved wrapper: digest.sh
  once, driver.sh eight times, prepare.sh's build. `smoke.sh` runs `cargo
  build`, `nbench cleanup|run|report`, `grep`, `date` and nothing else; it
  reads no runtime directory and exports only `NBENCH_ROOT`.
- `src/main.rs:970` is B-0's `Command::new("systemd-run") … podman exec …`,
  unchanged and outside this diff; it wraps podman only, as the ruling
  already recorded. `runner.rs`, `main.rs`, `podman.rs`, `bin/` and `lib/`
  are not in the diff.
- The new Rust tests start processes with `Command::new("bash")` only, on the
  row's own two scripts, against a stub.
- Nothing under `work/` is in the branch: the 20 code files are listed under
  Scope.

## Scope

`git diff nucleus/v3-scaffold...HEAD --stat`: 21 files, +2529/−6 against the
merge base `8a2df81d1bd7c8e65192cf0d463044c006e6bfcb`. Twenty are the card's
code, +2175/−6, all inside Touch only:

- `src/rows.rs`, `src/params.rs`, `src/parse.rs`;
- `src/fixtures/`: `ycsb-run.txt`, `ycsb-load.txt`, `ycsb-run-d.txt`,
  `ycsb-run-d-failed.txt`, `ycsb-run-e.txt`, `ycsb-run-f.txt`,
  `ycsb-standin/{README.md,StandinTable.java}`;
- `systems/ycsbcli/{Dockerfile,README.md}`;
- `workloads/oltp-ycsb-pg17/{README.md,digest.sh,driver.sh,prepare.sh,smoke.sh}`;
- `LICENSES.md`, `README.md`.

The twenty-first is this file. Nothing under `docs/`, `scripts/`, `tools/`,
`cards/` or `CORE_PATHS`. `Cargo.toml` is untouched (nbench still has no
dependency), and so are the `postgres17` system, every existing row, parser
and `plan()`. The rework-1 range alone (`dbc77fe6..80e20430`) is 13 files,
+1513/−131, and does not touch the verdict file.

The branch still sits on `8a2df81d`; the scaffold has moved 22 commits since
(`f7e6c715`), none under `bench/nbench`. A read-only
`git merge-tree --write-tree nucleus/v3-scaffold card/v3-NB-2` is clean.

## Correctness review (the rest)

- `driver.sh`: `set -euo pipefail`; the `2>&1 | tee "$NB_LOG"` contract is
  kept, so a failure's reason stays reachable; a non-zero YCSB exit fails the
  pass through `pipefail`. The row-count check refuses anything but digits
  before it is compared. `load_dataset` checks the truncate explicitly because
  the reload calls it from a `||` branch, where `set -e` alone would not
  abort. The restore cannot misfire on a non-inserting workload: A, B, C and
  F leave exactly `recordcount` rows. The first pass after a reinit never
  reloads, so the cold pass is not preceded by one. Read from the image
  today: the six stock workload files differ only in the operation mix and
  E's scan bounds (the read, update, scan, insert and read-modify-write
  proportions, `requestdistribution`, `maxscanlength`); none sets
  `fieldcount`, `fieldlength`, `insertorder` or `insertstart`, so the reload
  with the series' own file builds the dataset `workloada` builds.
- `rows.rs` and `params.rs` hold what the REWORK gate reviewed (the row's
  cells, the cross order, the four knobs and their round trip), plus a
  comment.
- `parse.rs`: no production code changed. `parse_ycsb` stays test-only and
  documented as the reference parser; the `driver` module documents how each
  fixture was produced.
- The fixtures are recorded, not invented: `ycsb-run.txt` and `ycsb-load.txt`
  are the pinned image against PostgreSQL 17 (the `-db` class on their
  `Command line:` lines is `site.ycsb.db.JdbcDBClient`). The D, D-failed, E
  and F fixtures are the real client 0.17.0 run against a file-backed
  stand-in table whose insert returns `ERROR` on a duplicate key; the module
  doc and `ycsb-standin/README.md` say so and limit them to shapes.
  Checked against the real reports this gate produced (digits masked): the
  `[READ]`/`[SCAN]`/`[INSERT]` sections and their `Return=` lines of the real
  D and E passes match the fixtures line for line; the only difference is
  three `[TOTAL_GC_…_Concurrent_GC]` lines the host JDK prints and the
  container's JRE did not, which feed no metric. The refused real pass
  printed the same `[INSERT], Return=ERROR` and `[INSERT-FAILED]` lines as
  `ycsb-run-d-failed.txt`.
- Code rules: no `unsafe`; no `unwrap`, `expect` or `panic` before parse.rs's
  first test module (line 567) or outside the test modules of the other two
  files, enforced by clippy with `expect_used`, `unwrap_used` and `panic`
  denied; pub items documented; std-only.
- The REWORK verdict's NITs that Rework 1 named are applied: the four workload F
  texts (driver.sh:178-183, parse.rs:262-268, parse.rs:720-727 now on the
  recorded F fixture, and the workload README:117-122) say F carries `[READ]`,
  `[READ-MODIFY-WRITE]` and `[UPDATE]` and no `[INSERT]`; only E lacks
  `[READ]`. No code comment calls the row's parser `Ycsb` any more; rows.rs
  and parse.rs say it parses driver-side and why.
- Acceptance line 2 as amended parses each script separately: with a planted
  syntax error in a scratch copy of `smoke.sh`, the loop exits 1 (the old
  form exited 0).

## Pins and licences

- Unchanged by the rework: `systems/ycsbcli/Dockerfile` and
  `systems/ycsbcli/README.md` are blob-identical (`3c512287f1f3`,
  `e485d3c7b82a`) to the tip the REWORK gate verified, which streamed the
  tarball and matched the pin. This gate did not stream it again.
- Checked today: the base image's RepoDigest is
  `sha256:7a93c4f0b26a0bcae4d8737198e3e8dbd5c2759da91863852a120a700f7c50f0`,
  the `FROM` pin; the jar inside the image this gate ran hashes to
  `f2a1cc0352dd5e5c6f65dbadff27bee00c32e432c6ad030d07447f8a5983c42a`, the
  pin; `/opt/ycsb` resolves to `ycsb-0.17.0`.
- PLAN §5.2 rule 9: YCSB Apache-2.0, pgjdbc BSD-2-Clause, Temurin JRE
  GPL-2.0 with Classpath Exception; none restricts publishing benchmark
  results. Three rows in LICENSES.md.

## Acceptance re-run (fresh clone)

- `cd nucleus/v3 && cargo test -q -p nbench`: 156 passed, 0 failed,
  0 ignored.
- Acceptance line 2: `bash -n` on each of driver.sh, digest.sh, prepare.sh
  and smoke.sh: exit 0 each; `shellcheck -S warning` on the four: exit 0.
- `cargo clippy -q -p nbench --all-targets -- -D warnings`: exit 0.
  `cargo fmt --all -- --check`: exit 0.
- `bash scripts/gate.sh`: the fresh-gate run above.

## Notes

- NIT N1: card text. Rework 1 asks for a `"voided": true` document; the row
  delivers the REWORK verdict's first form (the driver fails the pass) and
  cannot deliver the other from Touch only. See the paragraph on
  `"voided": true` under R2.
- NIT N2: the driver's missing-throughput guard (driver.sh:187-190) is not
  itself under test on the real path. With it deleted, a report without the
  line makes the driver write `{"ops_sec":,…}` and exit 0; the runner's JSON
  parse then fails the row, so nothing is published, only the reason is a
  JSON error. No test replays such a report through the driver. One fixture
  case in `driver_fails_a_pass_whose_operations_failed` would close it.
- NIT N3: `smoke.sh` runs A and D under the same tag, and both write
  `oltp-ycsb-pg17.smoke.json`, so the final `REPORT.md` shows D only. A's
  checks run before D starts, and A's pass logs and metrics files stay under
  `work/`.
- NIT N4: the driver tests never delete their
  `$TMPDIR/nbench-tests/<pid>-ycsb-driver-*` directories (1 962 of them,
  20 MB, are on this host now). The runner's and the fresh-gate's private
  `TMPDIR` hide it there.
- NIT N5: card text. Work 2, Work 4 and the Limits still say `parser Ycsb` /
  `Parser::Ycsb`, which the code does not have, and Work 3 still describes an
  A-only smoke. The code's own comments are correct now.
- NIT N6: D and E pay one full reload per pass after the cold one. At the
  default `recordcount` 100 000 and the load's `threadcount=1` that is
  roughly a minute and a half each by the REWORK gate's estimate (not
  measured), 36 in the full matrix. Worth a number before the quiet window.
- NIT N7: the row is tagged `Parser::VectorSearch`; if its metrics file were
  ever missing, the runner would print "vector rows must write NB_METRICS
  json" for a YCSB row. Unreachable with the current driver.
- NIT N8: the four stand-in fixtures carry the worker's absolute path on their
  `Command line:` lines. Harmless.
- NIT N9: (B-0's wrapper, outside this card): a failed `systemd-run --user
  --wait` call leaves a failed transient `run-*.service` unit in the user
  manager. This gate's six row starts left 12 `pg_isready` probe units that
  way; 239 such units from all lanes were loaded at the end of this gate.
  None is active.
- NIT N10: the results JSON lists the `init` digest twice (one record per Digest
  step, both named `init`); B-0's runner behaviour, not this card's.
- NIT N11: the workload README (lines 109-113) says the fixed
  `synchronous_commit=on` "is recorded in the row's provenance"; the results
  JSON has no such field. The sysbench row it mirrors does the same (its
  README plus the no-variant test
  `sysbench_and_ycsb_rows_fix_synchronous_commit_by_container_default`), so
  the setting is pinned by a test and documented, not recorded per run.

## On the worker's RESULT

- Its acceptance counts (156 passed), its mutant table, its pins and its
  description of the fixtures agree with what this gate measured.
- ESCALATE 1 (`"voided": true` not producible): correct, and recorded above
  as a card-text NIT.
- ESCALATE 2 (`-p insertstart=` is not an insert offset in 0.17.0): this gate
  did not re-derive the reading of `core-0.17.0.jar`. It confirmed the fact
  that matters on the real row: with the restore removed, a second D pass
  re-inserts the first pass's keys (47 duplicate-key errors). The dataset
  reset is the REWORK verdict's second form, and it works.
- ESCALATE 3 (card text): the NITs above.
- Its anomaly (a `packed-refs.lock` read-only message on every commit in the
  runner's sandbox): the six commits are on the branch, the worktree was
  clean before this verdict, and it holds no stash.
