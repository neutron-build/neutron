# NBench harness (PLAN-v3 §5, card B-0)

Reproducible competitor baselines for the §5.1 matrix, run under the
§5.2 fairness rules. This directory is the harness; the Nucleus side of
each ratio arrives with cards C-9 / D-*.

## Layout

```
bench/nbench/
  src/                 Rust bin crate `nbench` (orchestration, parsing,
                       stats, digest enforcement, report generation)
  bin/nb-podman        the single podman wrapper (systemd-run --user;
                       required under NoNewPrivs — card B-0 env note)
  lib/nb-lib.sh        shared helpers for driver scripts
  systems/<name>/      pinned image digests, container args, named
                       variants (durability pairs), vendor tuning sources
  workloads/<row>/     driver.sh (one execution), digest.sh (result-set
                       hash), prepare.sh (client images / datasets),
                       expected/ (recorded digest expectations)
  results/<date>/      <row>.json + REPORT.md (committed evidence)
  work/, data/         raw logs and datasets (gitignored)
  nbench-smoke.sh      acceptance: one tiny row end to end
```

## Usage

From `nucleus/v3` (uses the workspace target dir):

```sh
cargo run -q -p nbench -- list
cargo run -q -p nbench -- run oltp-pgbench-pg17     # one row
cargo run -q -p nbench -- run all                   # every row
cargo run -q -p nbench -- report                    # regenerate REPORT.md
cargo run -q -p nbench -- cleanup                   # stop all nbench servers
```

`NBENCH_ROOT` overrides the harness root when running the binary from
elsewhere (the smoke script sets it).

Overrides: `--set key=value` (repeatable) — e.g. `--set runs=5
--set duration_s=60 --set scale=100 --set clients=1,16,64`. Defaults
implement card B-0 Work item 3 exactly.

## How a row runs (§5.2 mapping)

1. Pin: every system starts from a digest-pinned image (`systems/*/image.txt`).
2. Plan: `nbench run` builds the row's step list (rows.rs `plan`) —
   init, init digest, then per config point: restart into the named
   durability variant, one cold run, warmup runs, `>= 5` warm runs,
   final digest. Sequencing is unit-tested in `rows.rs`.
3. Digest: `workloads/<row>/digest.sh` hashes a result set; the first
   run records the expectation, later runs must match or the row is
   VOID (rule 4). The expectation decision (record / match /
   mismatch-voids) lives in `src/digest.rs` with unit tests on every
   arm, including smoke/production expectation isolation. Tools that
   self-report content digests (speedtest1 `--verify` Verification
   Hash) are additionally cross-checked run-to-run.
4. Stats: median + deterministic 95% bootstrap CI over the warm runs
   (rule 5), seeds derived from row+series+metric identities.
5. Provenance: host (including **load average at row start**, live
   `nbench-*` units, and a second load reading at results-write time),
   image digests, container limits, versions, params and operator
   anomalies all land in `results/<date>/<row>.json` (rules 1, 6).
   `versions.txt` is **mandatory**: a driver that fails to write it
   fails the row loudly instead of shipping empty version provenance.
6. Report: `REPORT.md` per date with ratios and §5.3 labels; Nucleus
   columns are `n/a` until they exist. Anomalies are printed per row.

Cold = first run after a container restart (fresh process, empty caches
of the engine); warm = after warmup. The host page cache cannot be
dropped without root; this is the honest cold available to rootless
containers and is stated in the report.

## Co-tenant policy (shared measurement host)

The benchmark host (n5000) is shared with other card sessions, so
ambient load is part of the environment and is recorded, not assumed
away:

- **Exclusivity among nbench rows:** one `nbench run` at a time on the
  host. The runner records live `nbench-*` units at row start; a row
  measured while another nbench server unit was active notes it in
  `anomalies`.
- **Ambient load:** load1/5/15 at row start and at results-write time
  are in every results file. A row measured while load1 exceeded the
  host's CPU count, or through any obvious load spike, is flagged —
  set `NB_ANOMALIES` (entries separated by `;` or newlines) when
  running the row, e.g.
  `NB_ANOMALIES='co-tenant cargo test running 18:20-18:40, load1 ~22' nbench run <row>`.
  Anomalies are published in the row's report section, never dropped.
- **Comparing runs:** cross-date comparisons read the load fields
  first; the 2026-10-18 gate found a 1.8x spread between identical
  smoke runs that only co-tenant load explains.

## Competitors and licences

See `LICENSES.md` — every competitor's licence was checked for
benchmark-publication restrictions (DeWitt clauses); none was found.
