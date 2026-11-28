Card: NB-2
Class: bench
Card-gate: S
Verdict: REWORK
Gate-tier: S
Gate-model: zai-coding-plan/glm-5.4 (max)
Gate-session: opencode review sub-agent, T3 thread 45584af2-e651-490e-862c-3402320cffa3 (fresh gate on the rebased tip; this gate wrote none of the code, the rebase or the card)
Date: 2026-11-28
Gate-exit: GATE EXIT=0
Commit: 0f546f870b021ef303cdcdf7ab4bcf1946583a42
Mutants: SWEEP-NOT-CROSS (zip form and no-thread form) -> caught: rows::tests::ycsb_plan_crosses_workloads_with_threads_and_reinits_each_series FAILED at assert_eq!(series.len(), 18) (left: 3, right: 18; and left: 6, right: 18) | REINIT-DROPPED -> caught: same test FAILED at assert_eq!(reinits, 18, "one reinit per series") (left: 0, right: 18) | PARSER-OPS-DROPPED -> caught in parse_ycsb (4 tests, 145/4); on the row's real path (driver.sh:98) caught only by the Host smoke (exit 1, "ops: unbound variable") | PARSER-P99-AS-AVG -> caught in parse_ycsb (3 tests, 146/3); UNCAUGHT on the row's real path (driver.sh:100): 149/149 unit tests pass, Host smoke exit 0, the published p99_read_us is the average | VERSIONS-SKIPPED -> caught by the Host smoke (exit 1, "versions.txt not written by the driver") | DIGEST-CONSTANT -> UNCAUGHT within one run, as the card states (inspection passes: the single pipeline hashes the live SELECT count(*)); a differing constant voids the next run against a recorded expectation (exit 1, "digest mismatch at init … run VOIDED")
Followups: none

## Why REWORK

The gate run is green, the Host smoke passes, the rebase is correct, the
scope is exact and the diff holds no relay code. Two defects keep it from
merging: one of the card's named mutants survives on the path that produces
the row's numbers, and the row publishes passes in which operations failed.

### R1. `PARSER-P99-AS-AVG` is uncaught on the row's real path

- The card (Work 2, Work 4, Limits) says the row uses `Parser::Ycsb`. The
  delivered code has no `Ycsb` variant: `parse_ycsb` is `#[cfg(test)]`
  (parse.rs:260), so no production binary contains it, and the row declares
  `VectorSearch` (rows.rs:316). The runner's `parse_result` (runner.rs:439)
  returns the driver-written NB_METRICS JSON and never calls `parse_ycsb`.
  The code that fills `ops_sec`, `avg_read_us` and `p99_read_us` for this
  row is the sed extraction at driver.sh:98-108. No test reads driver.sh.
- This gate applied the named mutant there (driver.sh:100 reads
  `[READ], AverageLatency(us)` into `p99_read_us`):
  - `cargo test -q -p nbench`: 149 passed, 0 failed;
  - Host smoke on that extract: exit 0, `SMOKE OK`;
  - published: `{"ops_sec":2881.844380403458,"avg_read_us":116.88594704684317,"p99_read_us":116.88594704684317}`,
    while the same pass's log reads `[READ], 99thPercentileLatency(us), 428`
    and `[READ], AverageLatency(us), 116.88594704684317`.
- Nothing executable sees it. The worker's RESULT reports it caught because
  the mutant was applied to `parse_ycsb`, which the row does not run. The
  worker's own attempt-3 RESULT had recorded the same limit ("swapping the
  driver's p99 sed source to AverageLatency leaves the smoke green") and
  raised it as ESCALATE 1, a card defect; the final RESULT (attempt 1) drops
  both and says `ESCALATE: none`.
- Fix, either way, with the card amended to match (the gate cannot edit it):
  (a) add `nucleus/v3/bench/nbench/src/runner.rs` to Touch only, add `Ycsb`
  to `Parser`, dispatch `rows::Parser::Ycsb => parse::parse_ycsb(&text)`
  (NB-3 did exactly this for `DuckdbTpch`), drop `#[cfg(test)]`, set the
  row's parser to `Ycsb` and delete the sed block from driver.sh — the
  card's Work 2 and Work 4 as written; or (b) keep NB_METRICS and add a unit
  test that executes the driver's own extraction lines over
  `fixtures/ycsb-run.txt` and compares the result with `parse_ycsb`
  (std-only: `include_str!` of driver.sh plus `std::process::Command`), and
  re-point the card's two PARSER mutants at the driver.

### R2. Passes with failed operations are published as measurements

The driver never looks at YCSB's `Return=` lines, and YCSB exits 0 with
failed operations. This gate ran the row's own stock workloads beyond A
(`--tag smoke`, smoke size, 1 thread, 1 000 records, 1 000 operations):

- `workload=d` (cold, 1 warmup, 2 runs): cold `[INSERT], Return=OK, 43`;
  warmup `[INSERT], Return=ERROR, 43` and `[INSERT-FAILED], Operations, 43`;
  run-0 `Return=ERROR, 46`; run-1 `Return=ERROR, 55`. Each failure is
  `ERROR: duplicate key value violates unique constraint "usertable_pkey"`
  on the pass's stderr.
- `workload=e` (cold, 1 run): cold `[INSERT], Return=OK, 60`; the run
  `Return=ERROR, 49`, `[INSERT-FAILED], Operations, 49`.
- Both runs: `"voided":false`, `"anomalies":[]`, REPORT `Status: ok`, rows
  such as `workload=d threads=1 | warm (n=2) | 4943 [4651, 5236]`.

Cause, in two parts. Every `ycsb run` is a new process, and the core
workload's insert key sequence restarts at the same `insertstart` in each
one, so every pass after a series' first re-inserts keys the earlier pass
created. The stock files carry `insertproportion=0.05` for D and E and none
for A, B, C, F (read from the pinned image). Under the row's default plan
(cold, warmups, then warm runs) two of the six default workloads would
report warm numbers with every insert failing. The smoke cannot see it: it
runs workload A, which has no inserts.

Fix:

1. The driver must fail the pass (or the runner must void it) when the
   report carries a `Return=` line other than `Return=OK` for a section
   that ran, or any `*-FAILED` section — the same loud rule as the
   missing-throughput guard.
2. Each pass must insert into a key range no earlier pass of the series
   used (for example `-p insertstart=` derived from `NB_MODE`,
   `NB_RUN_INDEX` and the warmup count), or the dataset must be reset
   before every measured pass, so D and E warm passes measure the stock
   mix.
3. The Host smoke, or a second Host-smoke line, must run one insert-bearing
   stock workload (D or E) with at least one warm pass, so 1 and 2 stay
   caught.

## Gate run (fresh, on the rebased tip 0f546f87)

GATE-PROMPT step 1: the scaffold's own `fresh-gate.sh` extracted through a
shallow `--no-local` clone of `nucleus/v3-scaffold` (scaffold id
`8a2df81d1bd7c8e65192cf0d463044c006e6bfcb`; `fresh-gate.sh` sha256
`40a5b232a5ac2c0cd1629ab6f7aa24396e1a6379d36db536a96a84ba35404899`,
`git-env.sh` `8a784782b42b094ff82e5ad1b497feadab49e2a2f05328d765ae5eca0b073977`),
run under `systemd-run --user --scope --quiet -p CPUWeight=10000 --` with
`CARGO_TARGET_DIR=~/.cache/nv3-target/gate-NB-2`. The scope worked; no
plain fallback was needed. 08:05:26Z to 08:19:02Z.

```
fresh-gate: commit 0f546f870b021ef303cdcdf7ab4bcf1946583a42
fresh-gate: base 8a2df81d1bd7c8e65192cf0d463044c006e6bfcb
fresh-gate: checks passed (5002 files hashed in 2.52 s)
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

The literal last line was `GATE EXIT=0`; stderr was empty. The test step
ran 72 binaries: 928 passed, 0 failed, 1 ignored (a pre-existing
`#[ignore]`); nbench's own binary reports 149 passed. None of the known
load or zombie rows (FAST-TRACK §3) failed and no external SIGTERM arrived,
so there was nothing to re-run. The enforcement skip is what ENF-FASTGATE-1
gives a change that touches no `scripts/` or `tools/` file. The scaffold
extract was removed afterwards.

## Host smoke (GATE-PROMPT "Host smoke")

Run plainly in this gate's own shell, from the root of a fresh
`git clone --no-local` of the pinned commit (shallow, single-branch
`card/v3-NB-2`; HEAD and tree `585492ef63482ba30a8dc6ed20baea1c57ed358d`
verified; no hooks, filters or includes), never the worker's worktree. The
only environment change is `PATH` gaining `~/.cargo/bin`, because this tool
shell has no cargo and smoke.sh calls `cargo build`.

| Line | Exit | Last line |
|---|---|---|
| `cd nucleus/v3/bench/nbench && bash workloads/oltp-ycsb-pg17/smoke.sh` | 0 | `SMOKE OK: report at results/smoke/2026-11-28/REPORT.md` |

Last lines, verbatim (34 s wall, the nbench build included):

```
--- generate the report
/home/tyler/.cache/nv3-review/gate-NB-2/hostsmoke/clone/nucleus/v3/bench/nbench/results/smoke/2026-11-28/REPORT.md
SMOKE OK: report at results/smoke/2026-11-28/REPORT.md
```

Checked beyond the exit code:

- The results JSON has `"voided":false`; the init digest was recorded as
  `83c02ac2d48c863dab2ccf6870455aadfc2cec073b8db269b517c879d76aa6d9`, which
  is sha256 of a 1 000-row count line, the recordcount the smoke sets.
- All three metrics are present in cold and warm and are the log's own
  values: cold 1968.5 ops/s, avg 227.86 µs, p99 688 µs; warm 2057.6, 184.9,
  421.
- Provenance carries the four YCSB params, the postgres17 image digest,
  `versions` (PostgreSQL 17.11 twice, Temurin 17.0.21.1, `YCSB release:
  ycsb-0.17.0`) and `nbench_build`.
- The run's log has the recorded fixture's exact line skeleton (identical
  once numbers are masked), and every operation is `Return=OK`.
- The clone stayed git-clean; only gitignored `results/smoke/`, `work/`
  and `expected/` were written.
- The image build used cached layers (`nbench-ycsbcli:local`
  `9039ff9f4c21`, built by earlier runs), so the fetch-and-verify step did
  not run again here. The pins were checked separately, below.

Smoke, not a measurement (§7.7.3 item 5): host load1 was 28 to 33, since
this gate's fresh-gate and an unrelated promote were running. No nbench
container or unit was left behind by this run or by any run below; the
host's only container is an unrelated 8-month-old `alpine`.

## Relay containment check (ruling-NB-containment; GATE-PROMPT Host smoke rule)

- Every added line of the code diff (`nucleus/v3-scaffold...0f546f87`) was
  scanned for `systemd-run`, `systemctl`, `systemd/private`, `systemd/`,
  `XDG_RUNTIME_DIR`, `DBUS`, `sd-bus`, `busctl`, `relay`, `bridge`, `re-exec`,
  `python`, `/run/user`, `socket`, `proxy`, `SCM_RIGHTS`, `containment`.
  The hits are prose only: the words "protocol", "scope", "ruling", "Python
  2" (the tarball's own launcher, not used), and the workload README's
  "the smoke holds no relay and no derived transport" (README:49-50). No
  calls.
- The four scripts reach podman only through `nb-podman` (`bin/nb-podman`
  and `lib/nb-lib.sh`, untouched by this diff), the approved wrapper. What
  is left of smoke.sh is the ROOT and binary derivation, `cleanup`, the
  `run`, the `report` and three checks.
- `src/main.rs:970` is B-0's `Command::new("systemd-run") … podman exec …`,
  unchanged and outside this diff; it wraps podman only, as the ruling
  already recorded. Nothing to hold against this card.
- The worktree's ignored `bench/nbench/work/` (`dbus-proxy.py`,
  `xdg-shim*`) is not in the branch: `git ls-tree` of the tip and of every
  commit on the branch lists nothing under `work/` and no relay, bridge,
  shim or proxy path; it is ignored by `bench/nbench/.gitignore:2`. This
  gate never read it.

## Rebase correctness

- `git range-diff d1559245..7953b9d8 8a2df81d..0f546f87`: commits 2 to 5
  are identical patches; commit 1 differs only in context (LICENSES.md now
  shows NB-3's sysbench MySQL-driver note as context; README.md shows the
  merged `smoke.sh` line).
- 9 of the 13 files are blob-identical between the two tips (the fixture,
  the Dockerfile, `systems/ycsbcli/README.md`, and the workload's README,
  digest, driver, prepare and smoke). The other 5 are the files NB-3 also
  touched.
- On those, `git diff nucleus/v3-scaffold...HEAD --numstat` is
  LICENSES.md +3/-0, params.rs +83/-0, parse.rs +194/-0, rows.rs +108/-0:
  no line removed, so NB-3's rows, systems and parsers are the scaffold's
  own (`oltp-sysbench-mysql84`, `kv-memtier-dragonfly`,
  `embedded-duckdb-tpch1`, `Parser::DuckdbTpch` with its runner arm;
  runner.rs is not in the diff). README.md is +6/-2. Its two removed lines
  are the two NB-2 re-wraps: B-0's `variants (durability pairs), vendor
  tuning sources`, and NB-3's `smoke.sh (tagged-smoke structural check),`,
  which comes back as `smoke.sh (tagged-smoke structural check, e.g.
  oltp-ycsb-pg17),`, so both cards' wording survives. Of NB-3's 506 added
  lines in the six shared files, 505 are present verbatim at the tip and
  that one is the merged line.
- NB-2's additions are all present: the row (rows.rs:315-321), `sweep_ycsb`,
  the four params, `parse_ycsb` with its five fixture tests and
  `fixtures/ycsb-run.txt`, `systems/ycsbcli/`, the workload directory, the
  three LICENSES rows and the README lines. `nbench list` prints
  `oltp-ycsb-pg17 (system: postgres17)` next to NB-3's three rows.
- 149 nbench tests pass on the fresh clone and in the fresh-gate.

## Mutation evidence

Tier M: the card's mutants. Each in its own fresh `git archive | tar -x -m`
extract of the pinned commit (from the gate's clone, `nucleus/v2` beside
`nucleus/v3`), on a copy of one pristine target with nbench's artifacts
deleted, running only nbench's test binary (the crate has no lib). Every
mutant's diff against the pristine file was saved; an unchanged file aborts
the run. Nothing ran in the worker's worktree.

| Mutant | Applied to | Result |
|---|---|---|
| SWEEP-NOT-CROSS (zip) | rows.rs `sweep_ycsb` | caught: `assert_eq!(series.len(), 18)` (pristine line 904), left 3, right 18 |
| SWEEP-NOT-CROSS (thread dimension dropped) | rows.rs `sweep_ycsb` | caught: the same assertion, left 6, right 18 |
| REINIT-DROPPED | rows.rs:320 `reinit: false` | caught: rows.rs:922, `one reinit per series`, left 0, right 18 |
| PARSER-OPS-DROPPED | parse.rs `parse_ycsb` | caught: 4 tests, 145 passed / 4 failed |
| PARSER-OPS-DROPPED | driver.sh:98 (the row's real path) | unit tests 149/149 pass; caught by the Host smoke only: exit 1, `driver.sh: line 101: ops: unbound variable` |
| PARSER-P99-AS-AVG | parse.rs `parse_ycsb` | caught: 3 tests, 146 passed / 3 failed |
| PARSER-P99-AS-AVG | driver.sh:100 (the row's real path) | UNCAUGHT: unit tests 149/149, Host smoke exit 0 (R1) |
| VERSIONS-SKIPPED | driver.sh:47-53 | caught by the Host smoke: exit 1, `row oltp-ycsb-pg17 FAILED: … versions.txt not written by the driver (…) … every row's init mode must record tool versions (§5.2 rule 6)`; no results file written |
| DIGEST-CONSTANT | digest.sh:15-17, `printf 'not-the-row-count\n' | sha256sum …` | UNCAUGHT within one run, as the card states: a fresh extract records the constant and prints `SMOKE OK`. Inspection passes. Against the true recorded expectation the next run is voided: exit 1, `digest mismatch at init: expected 83c02ac2…, got 8ff126ef… — run VOIDED` |

The plan unit test and the smoke are real catchers of real code. The two
`parse_ycsb` rows catch their mutants only in a function the row does not
run; that is R1.

## Scope

`git diff nucleus/v3-scaffold...HEAD --stat`: 13 files, +789/-2. All are in
the card's Touch only list: `src/rows.rs`, `src/params.rs`, `src/parse.rs`,
`src/fixtures/ycsb-run.txt`, `systems/ycsbcli/{Dockerfile,README.md}`,
`workloads/oltp-ycsb-pg17/{README.md,digest.sh,driver.sh,prepare.sh,smoke.sh}`,
`LICENSES.md`, `README.md`. Nothing under `docs/`, `scripts/`, `tools/`,
`cards/`, `verdicts/` or `CORE_PATHS`. `Cargo.toml`, the `postgres17`
system and every existing row and parser are untouched.

## Correctness review (the rest)

- `params.rs`: four knobs with defaults, `set` arms, `to_pairs` entries and
  a round-trip test that also checks bad values error. Matches the card.
- `rows.rs`: `sweep_ycsb` is the sysbench shape; the row's cells
  (`init: true, final: false, reinit: true`, `no_variant`) match the card,
  and a second test pins the no-variant rule for both rows.
- `parse_ycsb`: last occurrence wins, missing throughput is an error,
  read latencies optional. Correct against the fixture; test-only.
- driver.sh: `set -euo pipefail`; a failing YCSB exit fails the pass
  through `pipefail`; the `2>&1 | tee "$NB_LOG"` contract is kept; init
  writes `versions.txt`, creates and truncates `usertable`, loads with the
  series' stock workload file (the load needs one), `threadcount=1`.
  `-p recordcount` does override the stock `recordcount=1000`: with
  `--set recordcount=1500` the load inserted 1 500 rows and the strict init
  digest voided the run (`got d6e02e54…`, sha256 of a 1 500-row line).
- digest.sh is the card's single pipeline. prepare.sh is the sysbenchcli
  shape. smoke.sh mirrors nbench-smoke.sh's three checks.
- Code rules: no `unsafe`; the 12 added `.expect(` are all inside
  `#[cfg(test)]`; pub items documented; std-only.
- Dockerfile: digest-pinned `FROM`, both artifacts fetched at build time
  and sha256-checked, provenance lines in the image history.

## Pins and licences

Checked by this gate, not taken from the RESULT:

- Base: the locally stored `eclipse-temurin:17-jre-jammy` RepoDigest is
  `sha256:7a93c4f0b26a0bcae4d8737198e3e8dbd5c2759da91863852a120a700f7c50f0`,
  the `FROM` pin.
- Jar in the image: sha256
  `f2a1cc0352dd5e5c6f65dbadff27bee00c32e432c6ad030d07447f8a5983c42a` (the
  pin) and sha1 `93b508727bf4af3db27059dc592d90da68ecee8d`, equal to the
  `.sha1` Maven Central serves today for `postgresql-42.7.6.jar`.
- Tarball: streamed today from the GitHub release URL, 708 154 435 bytes,
  sha256 `d80d8b67c4b1da9c7d9dc0d40cdd9a651f0c0f44e580aea00fb3f86cffb6f5cc`,
  the pin. GitHub serves no digest for the asset, so the Dockerfile's
  provenance note is accurate.
- PLAN §5.2 rule 9: YCSB Apache-2.0, pgjdbc BSD-2-Clause, Temurin JRE
  GPL-2.0 with Classpath Exception; none restricts publishing benchmark
  results. Three rows in LICENSES.md.

## Acceptance re-run (fresh clone)

- `cd nucleus/v3 && cargo test -q -p nbench`: 149 passed, 0 failed,
  0 ignored.
- `bash -n` on each of driver.sh, digest.sh, prepare.sh, smoke.sh: exit 0
  each. `shellcheck -S warning` on each: exit 0 each.
- `cargo clippy -q -p nbench --all-targets -- -D warnings`: exit 0.
  `cargo fmt --all -- --check`: exit 0.
- `bash scripts/gate.sh`: the fresh-gate run above.

## Notes

- NIT: four texts state that workload F carries `[READ-MODIFY-WRITE]` and
  no `[READ]` (driver.sh:95-96, the `parse_ycsb` doc at parse.rs:254-256,
  the comment and synthetic `rmw_mix` report of
  `ycsb_read_mixes_without_a_read_section_omit_read_metrics` at
  parse.rs:706-728, and the workload README:72-75, which also says
  `F is [READ-MODIFY-WRITE]+[INSERT]`). Real 0.17.0 output for F is
  `[READ], Operations, 1000`, `[READ-MODIFY-WRITE], Operations, 509`,
  `[UPDATE], Operations, 509`, no `[INSERT]` (stock `workloadf`:
  `readproportion=0.5`, `readmodifywriteproportion=0.5`). F's series do
  carry `avg_read_us` and `p99_read_us`; only E lacks them. The code
  handles the real shape; the texts do not. The RESULT records no run of
  B to F, only of A.
- NIT: the row is tagged `Parser::VectorSearch`; if its metrics file were
  ever missing, the runner would print "vector rows must write NB_METRICS
  json" for a YCSB row. Unreachable with the current driver, and moot under
  R1 fix (a).
- NIT: card text. Work 2, Work 4 and the Limits still say `parser Ycsb` /
  `Parser::Ycsb`; the code has no such variant. Whichever way R1 is fixed,
  card and code must agree.
- NIT: card text. Acceptance line 2's `bash -n a b c d` parses only
  driver.sh: with a syntax error planted in a scratch copy of smoke.sh, the
  card form exits 0 and the single-file form exits 2. Every script passes
  individually, so this is the NIT the brief allows. `shellcheck` does
  take every file.
- NIT: README.md's `Rows:` paragraph lists B-0's and NB-3's rows but not
  `oltp-ycsb-pg17 (card NB-2)`; the layout list and the usage example do
  carry it.
- NIT: smoke.sh:2 still calls itself "card NB-2 Acceptance"; it is the
  Host smoke line now. The smoke also lacks nbench-smoke.sh's `warm (n=1)`
  report check; the card names only the three it has.
- NIT: the load runs with `threadcount=1` (about 1.2 k inserts/s here), so
  a default-size reinit is roughly a minute and a half, 18 times in the
  full matrix. Not measured; worth a number before the quiet window.
- NIT: the image keeps every YCSB binding (1.05 GB) though the row uses
  jdbc only; documented as the stock tarball in the card's Open points.
- NIT: `ycsb_workloads` accepts any letter; `g` fails loudly inside YCSB
  (`-P …/workloadg`). Same pattern as the sysbench row's `workloads`.
- The final RESULT's pins, fixture sections, property set and "no relay"
  claim were re-verified and hold. Its mutant table needs the two run-path
  rows above once R1 is fixed.
