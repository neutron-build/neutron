Card: NB-3
Class: bench
Card-gate: S
Verdict: MERGE
Gate-tier: S
Gate-model: zai-coding-plan/glm-5.3 (max)
Gate-session: opencode review sub-agent, T3 thread 45584af2-e651-490e-862c-3402320cffa3 (fresh gate; the earlier verdict a2c8bf63 is void per Opus ruling ruling-NB-containment.md)
Date: 2026-11-28
Gate-exit: GATE EXIT=0
Commit: e40cb6b8973d0ba2fd25bc95cf2f72b474243812
Mutants: MYSQL-PROBE-PORTLESS -> caught: runner::tests::mysql_ping_probe_targets_the_configured_port FAILED ("left: \"mysqladmin -h 127.0.0.1 ping\" right: \"mysqladmin -h 127.0.0.1 -P 3307 ping\"") | MYSQL-DURABILITY-SOFT -> caught: rows::tests::mysql84_base_args_carry_the_per_commit_flush FAILED (base args carry --innodb_flush_log_at_trx_commit=2) | DRAGONFLY-PORT-SWAPPED -> caught: kv-memtier-dragonfly Host smoke on a mutated extract, exit 1, "DRAGONFLY SMOKE FAIL: memtier measured 0 ops/sec — did it reach the server?" | SYSBENCH-DRIVER-CONFUSED -> caught: oltp-sysbench-mysql84 Host smoke on a mutated extract, exit 1, sysbench "FATAL: Connection to database failed: connection to server at \"localhost\" (::1), port 5432 failed: Connection refused" | QUERIES-COUNT-LIED -> caught: embedded-duckdb-tpch1 Host smoke on a mutated extract, exit 1, "DUCKDB TPCH SMOKE FAIL: results JSON does not record queries: 22" | DUCKDB-EXT-RUNTIME-FETCH -> UNCAUGHT by any test, as the card states (inspection-verified: build-time INSTALL — the baked /root/.duckdb/extensions/v2.0.1/linux_amd64/tpch.duckdb_extension is in the image; the driver's dbgen and run passes run under --network none, so no fetch is possible where it is measured) | TPCH-ANSWERS-UNHASHED -> UNCAUGHT within one run, as the card states (inspection-verified: digest.sh re-executes all 22 queries and hashes the concatenated outputs plus the per-table dbgen row counts)
Followups: none

## Gate run (fresh, on the reworked tip e40cb6b8)

GATE-PROMPT step 1, the scaffold's own `fresh-gate.sh` extracted
through a shallow `--no-local` clone of `nucleus/v3-scaffold` (scaffold
id `84a5cc707efcd42d6ecda2c46e2d77b063d8e2a3`, pass 195's promotion),
run foreground under `systemd-run --user --scope --quiet -p
CPUWeight=10000 --` with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-NB-3`
(the scope worked; no plain fallback needed):

```
fresh-gate: commit e40cb6b8973d0ba2fd25bc95cf2f72b474243812
fresh-gate: base 84a5cc707efcd42d6ecda2c46e2d77b063d8e2a3
fresh-gate: checks passed (4990 files hashed in 1.94 s)
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

The literal last line was `GATE EXIT=0`; stderr empty. This is the
card's one full gate run (FAST-TRACK §2: the implementer ran none; the
runner's `gate.sh` line was deferred to it). No known load or zombie
row (FAST-TRACK §3 / QUEUE ENF-TIMING-1) failed and no external
SIGTERM arrived, so no enforcement re-run was needed. The brief
expected `STEP enforcement-tests SKIPPED` (no scripts/ or tools/
change); the pass-195 scaffold runs the enforcement suite regardless,
and it passed — recorded as found, and harmless. The scaffold extract
was removed afterwards.

## Host smoke (GATE-PROMPT "Host smoke"; the ruling's re-gate)

Run by this gate plainly, each line in its own shell pass, on a fresh
`git clone --no-local` of the pinned commit `e40cb6b8…` (branch
`card/v3-NB-3`, `nucleus/v2` beside `nucleus/v3`), never the worker's
worktree:

| Line | Exit | Last line |
|---|---|---|
| `cd nucleus/v3/bench/nbench && bash workloads/oltp-sysbench-mysql84/smoke.sh` | 0 | `MYSQL84 SMOKE OK (wall 30s)` |
| `cd nucleus/v3/bench/nbench && bash workloads/kv-memtier-dragonfly/smoke.sh` | 0 | `DRAGONFLY SMOKE OK (wall 23s)` |
| `cd nucleus/v3/bench/nbench && bash workloads/embedded-duckdb-tpch1/smoke.sh` | 0 | `DUCKDB TPCH SMOKE OK (wall 9s)` |

Verified beyond the exit codes: `"voided":false` and `nbench_build`
provenance in all three results JSONs; dragonfly `ops_sec` samples
begin `202140.68` (memtier reached the server; the structural >0
check); duckdb `"queries":{"samples":[22` (exactly the 22 that ran);
all pinned images (mysql:8.4, dragonfly:v1.27.1, memtier, debian:12)
matched their `image.txt` digests; the clone stayed git-clean (only
gitignored `results/smoke/`, `work/`, `expected/*-smoke.txt` writes);
no nbench container was left behind (the runner's own cleanup removed
each; the host's only remaining container is an unrelated
8-month-old `alpine`). Smoke, not a measurement (§7.7.3 item 5).

## Relay containment check (ruling Work 8; GATE-PROMPT Host smoke rule)

- The rework commit `e40cb6b8` is exactly Work 8: 68 deletions, 0
  insertions, the three relay blocks (mysql84 smoke.sh:48-68,
  dragonfly:47-67, duckdb:50-72) gone from every `workloads/*/smoke.sh`.
- Grep of every touched path (`workloads/`, `systems/`, `src/`, `bin/`):
  no `NB3_RELAYED`, no `XDG_RUNTIME_DIR`, no `systemd/private`, no dbus
  or bus socket, no containment probe. The only `systemd-run` callers
  in the harness are `src/podman.rs` and `bin/nb-podman` — nbench's
  podman wrapper, untouched by this diff.
- Grep of the added lines of the whole code diff: the only
  "relay"/"systemd-run" hits are three prose comments (the B-0-era
  stdin lesson in `embedded-duckdb-tpch1/driver.sh:26` and the digest's
  header, plus `rows.rs`'s environment note) — no calls.
- What is left of each smoke is the env derivation, the `"$BIN" run`
  pass and the structural checks only, as the ruling requires.

## Licence gate (PLAN §5.2 rule 9 — still holds)

`LICENSES.md` carries all three rows with paths and dates (2026-11-27):
MySQL 8.4.13 (GPLv2 + Universal FOSS Exception, read from the pinned
image at `/usr/share/doc/mysql-community-server-minimal/LICENSE`),
Dragonfly v1.27.1 (BSL 1.1 + Additional Use Grant; the image itself
carries no licence file, so the governing text is upstream's LICENSE.md)
and DuckDB 2.0.1 (MIT, upstream repository licence). This gate re-grepped
the MySQL licence text from inside the pinned image: every "publish"
occurrence is GPLv2's own distribution terms or a bundled third-party
grant — no benchmark-restriction clause. Nothing to exclude, no
permission needed, no ESCALATE.

## Scope

`git diff nucleus/v3-scaffold...HEAD --stat`: 52 files. All 51 code
files are in the card's Touch only list; nothing in `docs/`, `scripts/`,
`tools/`, `cards/`, `verdicts/` or `CORE_PATHS`; `systems/sysbenchcli/`
untouched (the driver check passed, so the source-build fallback was
correctly not taken). The 52nd file is `nucleus/v3/verdicts/NB-3.md`
itself — the void gate's own verdict commit (a2c8bf63), which this
verdict replaces on the card branch, as GATE-PROMPT step 7 provides.

## Correctness review

- `rows.rs` (+276/-0): three `SystemDef`s (mysql84 Server 3307 with
  per-commit flush base args, dragonfly Server 6382, duckdb Transient),
  three rows, `Ready::MysqlPing`, `sweep_kv_fixed` (pipeline-only, the
  durability-unmatched note in the row description the report prints),
  the `port()` entries; `plan()`, `Step` and every existing row's
  definition unchanged.
- `runner.rs`: the `MysqlPing` arm of `wait_ready` (TCP probe
  `mysqladmin -h 127.0.0.1 -P <port> ping`, 300 s window for MySQL's
  first-time datadir init — never the unix socket the entrypoint's
  temporary server answers) plus its pure-probe unit test; and the
  `Parser::DuckdbTpch` dispatch arm the exhaustive match forces (Work 5
  adds the variant; disclosed in the RESULT; one line).
- `parse.rs`: `parse_duckdb_tpch` pinned to the real captured fixture
  (`src/fixtures/duckdb-tpch-run.txt`, 758 lines, header names the
  exact build; this gate's own Host smoke reproduced the same
  `TPCH_*` summary shape with `TPCH_QUERIES 22`); last occurrence of a
  tag wins; missing summary lines are errors, never zeros; the
  `queries` metric rides the pre-existing aggregate path (runner.rs
  aggregates unregistered metrics with samples, by B-0 design).
- `params.rs`: `tpch_sf` (default 1) with a `set` arm and defaults
  test; no dead `mysql_*`/`dragonfly_*` knobs.
- Shell: all 12 scripts `bash -n` + `shellcheck -S warning` clean
  (re-run by this gate on the fresh clone, exit 0); `set -euo
  pipefail` throughout; stderr never swallowed; no stdin piped into
  containers (the B-0 relay-stdin lesson); the duckdb driver refuses
  to start unless exactly `tpc01.sql..tpc22.sql` are present by name.
- Code rules: no `unsafe`; all 14 `expect(` additions are inside
  `#[cfg(test)]`; pub items documented; the crate stays std-only,
  `Cargo.toml` untouched.
- Pins: mysql84/dragonfly digest-pinned in `image.txt` with recording
  date and method; the duckdb builder is `FROM` a digest-pinned
  Debian 12, downloads the CLI artifact at build time and verifies the
  upstream-published sha256, and bakes the tpch extension with a
  build-time `INSTALL` (this gate listed the baked
  `tpch.duckdb_extension` in the image).
- Judgment calls reviewed and accepted (all disclosed in the RESULT):
  `--skip-log-bin` replaces the card's `--log_bin=0` (MySQL 8.4
  silently ignores the latter, verified live, provenance-recorded);
  the Dragonfly digest/probe use the image's own redis-cli 6.0.16
  (the card's "ships no CLI" premise was false, recorded in the
  README); smokes derive their own env (the acceptance lines pass
  none).

## Acceptance re-run (fresh clone)

- `cd nucleus/v3 && cargo test -q -p nbench`: 141 passed, 0 failed,
  0 ignored (matches the RESULT; 131 before the card).
- `bash -n` + `shellcheck -S warning` over the 12 scripts: exit 0.
- `bash scripts/gate.sh`: the fresh-gate run above (the deferred line).

## Notes

- The void verdict's NB-3-F1 (the smoke containment relay needed an
  owner ruling) is **resolved**: the Opus ruling chose design (c), the
  GATE-PROMPT gained the Host smoke section, the card moved its real
  smokes there and deleted the relays, and this gate ran them. No
  follow-up card is warranted from this work.
- SEC-RELAY-1 (the runner's `Container: podman` opt-in unmasking
  `/run/user`) is real but is **not this card's**: NB-3 no longer
  carries a Container line and its code holds no relay; the item is
  already QUEUE row SEC-RELAY-1, waiting on the owner's answer.
- NIT: the brief's "expect `STEP enforcement-tests SKIPPED" is stale —
  the pass-195 scaffold runs the enforcement suite regardless of
  scripts/tools changes (it passed; nothing to fix).
- NIT: `LICENSES.md`'s Dragonfly row cites the project's LICENSE.md
  without naming the ref; the v1.27.1 tag's own LICENSE.md reads
  identically (the void gate checked) and is the stronger citation.
- NIT: the card's Open point "DuckDB publishes no upstream hash for
  the CLI zip" is stale — v2.0.1's release page lists asset sha256s
  and the pin uses one; the code and LICENSES.md already record this.
  Local-only card text, no edit needed.
- NIT: `systems/mysql84/README.md` says "~25% of machine RAM (38 GiB
  host)" for the 6GB buffer pool (≈16%) — loose rounding that mirrors
  the PG row's own guidance text; the percentage should not be read
  as exact.
- NIT: the duckdb driver's versions sub-run (init's first container,
  `--version` + extension listing) starts with default networking;
  no fetch occurs there (the extension is baked and the LOAD is from
  disk), and the measured dbgen/run/digest passes are `--network
  none`. A future card could make that sub-run offline too, for
  uniformity.

Everything the rework was asked to do is done and verified; the card
is harness-side, smoke-tagged only, and publishes no measurement.
MERGE.
