Card: NB-3
Class: bench
Card-gate: S
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: S
Gate-model: zai-coding-plan/glm-5.3 (max)
Gate-session: opencode session ses_ddf8cae74ffetQmjREVFEbeVqK (T3 Code review sub-agent, S gate of record for NB-3, requested by T3 thread 45584af2-e651-490e-862c-3402320cffa3)
Date: 2026-11-28
Gate-exit: GATE EXIT=0
Commit: 2e709024d54ed18fde68a381c5745e086abda2ed
Mutants: MYSQL-PROBE-PORTLESS -> caught: runner::tests::mysql_ping_probe_targets_the_configured_port FAILED ("left: mysqladmin -h 127.0.0.1 ping, right: mysqladmin -h 127.0.0.1 -P 3307 ping") | MYSQL-DURABILITY-SOFT -> caught: rows::tests::mysql84_base_args_carry_the_per_commit_flush FAILED (base args ["…","--innodb_flush_log_at_trx_commit=2","--skip-log-bin"]) | DRAGONFLY-PORT-SWAPPED -> caught: kv-memtier-dragonfly smoke exit 1 "DRAGONFLY SMOKE FAIL: memtier measured 0 ops/sec — did it reach the server?" | SYSBENCH-DRIVER-CONFUSED -> caught: oltp-sysbench-mysql84 smoke exit 1, "FATAL: Connection to database failed: connection to server at 'localhost' (127.0.0.1)/(::1), port 5432 failed: Connection refused" in NB_LOG | QUERIES-COUNT-LIED -> caught: embedded-duckdb-tpch1 smoke exit 1 "DUCKDB TPCH SMOKE FAIL: results JSON does not record queries: 22" (JSON carries samples:[21]) | DUCKDB-EXT-RUNTIME-FETCH -> UNCAUGHT by any test, as the card itself states (inspection-verified: build-time INSTALL + offline LOAD; the gate also ran it live: a runtime INSTALL under the run pass's --network none fetches nothing — the baked extension satisfies it offline and the smoke still passes, confirming the no-fetch property structurally) | TPCH-ANSWERS-UNHASHED -> UNCAUGHT within one run, as the card itself states (inspection-verified: digest.sh hashes the concatenated outputs of all 22 queries plus the per-table dbgen row counts)
Followups: NB-3-F1

## Gate run

GATE-PROMPT step 1, on the scaffold's own `fresh-gate.sh` extracted
through a shallow `--no-local` clone of `nucleus/v3-scaffold`
(scaffold id `d155924534028dfc147aaad3d0991575fae4a2c3`), run
foreground under `systemd-run --user --scope --quiet -p
CPUWeight=10000 --` with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-NB-3`
(the scope worked; no plain fallback needed):

```
fresh-gate: commit 2e709024d54ed18fde68a381c5745e086abda2ed
fresh-gate: base d155924534028dfc147aaad3d0991575fae4a2c3
fresh-gate: checks passed (4989 files hashed in 1.56 s)
fresh-gate: 2 file(s) matched through their eol conversion
STEP test EXIT=0
STEP clippy EXIT=0
STEP fmt EXIT=0
STEP deny EXIT=0
STEP wasm32 EXIT=0
STEP core-paths-selftest EXIT=0
STEP enforcement-tests EXIT=0
STEP api-surface EXIT=0
GATE EXIT=0
```

The literal last line was `GATE EXIT=0`; wrapper exit 0; stderr empty.
No known load or zombie row (FAST-TRACK §3 / QUEUE ENF-TIMING-1)
failed, and no external SIGTERM arrived, so no enforcement re-run was
needed. This was the card's one full gate run (the runner's own
`gate.sh` PASS after attempt 3 is the runner's record; the implementer
ran none, per FAST-TRACK §3). The scaffold extract was removed
afterwards.

## Licence gate (PLAN §5.2 rule 9 — the card's Work 1, checked first)

Verified genuine, not asserted. The worker's logs show the reads
happened before any row was built or measured, and this gate
re-checked every claim:

- **MySQL 8.4.13** — licence text read from inside the pinned image
  (`cat /usr/share/doc/mysql-community-server-minimal/LICENSE` via
  `nb-podman run --rm docker.io/library/mysql:8.4 …`) and grepped for
  benchmark/publication clauses: every "publish" occurrence is GPLv2's
  own distribution terms or a bundled third-party grant (Apache/MIT
  style); **no benchmark-restriction clause**. GPLv2 restricts
  distributing the software, not publishing results about it. Recorded
  in `LICENSES.md` with the path and date 2026-11-27.
- **Dragonfly v1.27.1** — the image itself carries no Dragonfly
  licence file (verified from inside the image: only Ubuntu's
  `/usr/share/common-licenses/`; the binary embeds no licence text —
  grepped). The governing text is upstream's LICENSE.md: BSL 1.1 with
  Dragonfly's Additional Use Grant. The gate independently fetched the
  **v1.27.1-tagged** LICENSE.md and confirms the worker's reading: the
  grant wording is identical to what was recorded (only the Change
  Date differs from main's — 2029 vs 2030 — irrelevant to publication).
  BSL 1.1 grants non-production use (a benchmark run is one) and the
  grant is a field-of-use restriction; **neither text restricts
  publishing benchmark results**. Recorded with the date.
- **DuckDB 2.0.1** — MIT, verified from the upstream repository
  licence; no restriction. The v2.0.1 release page publishes the CLI
  artifact's sha256 (`b676268b…b5eca`), and the worker re-measured the
  downloaded artifact byte-identical — the card's disclosed
  fetch-time-pin open point is closed with *better* provenance than
  the card asked for.

No clause restricts publication on any of the three engines; nothing
to exclude, no permission needed, no ESCALATE. The licence-gate
evidence trail in `LICENSES.md` (rows, paths, dates) is complete.

## Scope

`git diff nucleus/v3-scaffold...HEAD --stat`: 51 files, 2930
insertions. Every changed file is in the card's Touch only list;
nothing in `docs/`, `scripts/`, `tools/`, `cards/`, `verdicts/` or
`CORE_PATHS`. `systems/sysbenchcli/` is **untouched**: the card's
driver check ran live against the MySQL container (Debian's sysbench
1.0.20 carries the MySQL driver — `oltp_read_write prepare` and `run`
both worked through `--db-driver=mysql`), so the source-build
fallback was correctly not taken.

## Correctness review

- `rows.rs`: three `SystemDef`s (mysql84/dragonfly/duckdb), three
  rows, `Ready::MysqlPing`, `sweep_kv_fixed`, the `port()` entries;
  `plan()`, `Step` and every existing row's definition unchanged. The
  mysql84 row mirrors pg17 cell for cell; the dragonfly row's
  description carries the durability-unmatched note the report prints
  verbatim (checked in a real smoke REPORT.md); the duckdb row mirrors
  the sqlite embedded shape.
- `runner.rs`: the `MysqlPing` arm of `wait_ready` with a 300 s window
  (MySQL's first-time datadir init) and the pure-probe unit test; plus
  the `Parser::DuckdbTpch` dispatch arm the Limits didn't name but the
  exhaustive match forces (Work 5 adds the variant) — disclosed in the
  RESULT; minimal and correct.
- `parse.rs`/`params.rs`: `parse_duckdb_tpch` pinned to a **real
  captured driver fixture** (758 lines, header names the exact build;
  the fixture's tail matches the RESULT's verbatim time/RSS lines);
  missing summary lines are errors, never zeros; `tpch_sf` defaults to
  1 with a set arm and defaults test, no dead knobs
  (`mysql_*`/`dragonfly_*` correctly not added).
- Shell: all 12 scripts `bash -n` + `shellcheck -S warning` clean
  (re-run by this gate on a fresh extract); drivers `set -euo
  pipefail`; stderr never swallowed (the mysql digest's `-D` fix,
  commit 0d87bcd0).
- Code rules: no `unsafe`; all 14 `expect(` additions are inside
  `#[cfg(test)]`; pub items documented; the crate stays std-only;
  `Cargo.toml` untouched.
- Judgment calls all reviewed and accepted: `--skip-log-bin` replaces
  the card's `--log_bin=0` (silently ignored by MySQL 8.4 — verified
  live, `@@log_bin` stayed 1; the card's intent is implemented and
  provenance-recorded); the Dragonfly digest/probe use the image's own
  redis-cli 6.0.16 (the card's "ships no CLI" premise was false,
  recorded in the README); smokes derive their own env (the attempt-3
  rework; the acceptance lines pass bare).

## Pinning and no test-time fetch

mysql84 and dragonfly are digest-pinned in `image.txt` with the
recording date and method; the duckdb builder is `FROM` a
digest-pinned Debian 12, downloads the CLI artifact at **build** time
and verifies the upstream-published sha256, and bakes the tpch
extension with a build-time `INSTALL`. The run and dbgen passes use
`--network none`, so no fetch is even possible at test time — the
DUCKDB-EXT-RUNTIME-FETCH live run above confirms it. `versions.txt`
exists for all three rows; digests exist for all three.

## Smoke only, no measurements

All three smokes run `--tag smoke` and write only under gitignored
`results/smoke/` (worktree clean; only `expected/*-smoke.txt` files,
which the scaffold's `.gitignore` design covers). Every number in the
RESULT is labelled smoke; this gate re-ran all three smokes pristine
itself: `MYSQL84 SMOKE OK (wall 30s)`, `DRAGONFLY SMOKE OK (wall
26s)`, `DUCKDB TPCH SMOKE OK (wall 7s)`, all `"voided":false`,
duckdb `queries: 22`. `cargo test -q -p nbench`: 141 passed, 0 failed
(matches the RESULT; 131 before the card). Nothing is claimed as a
measurement; the full matrices are correctly deferred to the
release-hardening window (§7.7.4).

## Follow-up

**NB-3-F1 — the smoke containment relay needs an owner ruling.** The
card's Acceptance lines start real NBench rows, which a contained
runner line cannot do through `unit_start`'s `systemd-run --user
--unit` shape (run_card.py documents this: "A card that needs one must
ask: there is no exemption here"); the card, drafted 2026-11-27, did
not ask. The worker's resolution: each smoke.sh probes `unit_start`'s
exact shape, and when it fails (a contained shell) re-execs itself
through `systemd-run --user --wait --pipe` — the owner-approved podman
door, this card being `Container: podman` — loudly (a two-line stderr
banner), with unchanged row code and assertions, and as a no-op on an
uncontained host (the gate's and runner's bare runs prove both
branches). The worker disclosed it in the RESULT and escalated for a
ruling; removing it is one block per script. The ruling — bless the
self-relay as the documented pattern for smokes that must start real
rows, or replace it with a runner-side mechanism — is a process/spec
gap for a later card, not a defect in this one: no wrong result, no
missing test, no uncaught mutant, no scope breach. It touches only
smoke scripts, so future real measurements (uncontained `nbench run`)
are unaffected.

## Nits

- NIT: `LICENSES.md`'s Dragonfly row cites the project's LICENSE.md
  without naming the ref; the v1.27.1 tag's own LICENSE.md is the
  stronger source and reads identically (this gate checked). The next
  card touching that file can pin the citation to the tag.
- NIT: the card's Open point "DuckDB publishes no upstream hash for
  the CLI zip" is stale — v2.0.1's release page lists asset sha256s
  and the pin uses one. The code and LICENSES.md already record this
  correctly; only the card text is behind (local-only, no edit
  needed).
- NIT: `systems/mysql84/README.md`'s tuning table cites "38 GiB host"
  for the 6GB buffer pool while `rows.rs` says "~25% of host RAM";
  6/38 ≈ 16%, not 25%. The PG row's own guidance text has the same
  rounding looseness, so the mirror is faithful, but the percentage
  claim should not be repeated as exact.
- NIT: `sweep_kv_fixed`'s doc says "no AOF-equivalent fsync flag
  (v1.27.1, verified against its own `--help`)" — accurate, but the
  133-flag count and the no-fsync grep evidence live only in
  `systems/dragonfly/README.md` prose; a future sweep change can't
  unit-test a flag absence. Acceptable for an M-tier harness card.
