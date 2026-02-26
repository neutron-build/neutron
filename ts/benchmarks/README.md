# Benchmark Protocol

This benchmark suite is isolated to `neutron/benchmarks` so Neutron framework code stays untouched.

## What it tests

- Frameworks: `neutron`, `neutron-react` (Neutron React-compat mode), `next`, `remix` (Remix 2), `remix3` (React Router 7 framework mode), `astro`
- Scenarios:
  - `static`: `GET /`
  - `dynamic`: `GET /users/1`
  - `compute`: `GET /compute`
  - `big`: `GET /big`
  - `mutate`: `POST /api/mutate` (JSON body)
  - `login`: `GET /login`
  - `protected`: `GET /protected` with `Authorization: Bearer valid-token`
  - `session-refresh`: `POST /api/session/refresh` with `Authorization: Bearer valid-token`
- For each framework/scenario:
  - Production server start
  - Warmup run
  - N measured runs
  - Median reported (RPS, p50, p95, p99, throughput)
- Feature conformance probes (unless `BENCH_CONFORMANCE=0`):
  - `SSG`, `SSR`, `ISR-like cache invalidation`, `Streaming`, `Actions`, `Auth`

## Fairness model

- Current harness is **Node server parity**: each framework runs its production Node server under the same load profile.
- This is good for framework runtime/server comparisons.
- It is **not** a CDN/edge/static-hosting benchmark. If you want "best possible static hosting" results, use a separate profile.
- Dual-track support:
  - `BENCH_TRACK=node` (default)
  - `BENCH_TRACK=optimal-static` (framework-optimal static deployment for `/`)
  - `BENCH_TRACK=both` (single run emits both tracks)

## Default run

```powershell
cd neutron/benchmarks
pnpm run compare
```

Quick cross-framework smoke run:

```powershell
cd neutron/benchmarks
pnpm run compare:quick
```

Canonical regression run (recommended before sharing results):

```powershell
cd neutron/benchmarks
pnpm run compare:full
```

Canonical publish run (both tracks + conformance + payload audit):

```powershell
cd neutron/benchmarks
pnpm run compare:canonical
```

Strict publish-grade protocol (idle-host preflight + repeated median-of-medians + CI95):

```powershell
cd neutron/benchmarks
pnpm run compare:publish-grade
```

Fast publish preflight (sanity pass, not a report run):

```powershell
cd neutron/benchmarks
pnpm run compare:publish:preflight
```

Official publish run (pinned production profile):

```powershell
cd neutron/benchmarks
pnpm run compare:publish:official
```

Stress profile:

```powershell
cd neutron/benchmarks
pnpm run compare:stress
```

Saturation profile:

```powershell
cd neutron/benchmarks
pnpm run compare:saturation
```

Framework-optimal static track:

```powershell
cd neutron/benchmarks
pnpm run compare:optimal
```

Run both tracks in one artifact:

```powershell
cd neutron/benchmarks
pnpm run compare:tracks
```

Quick Bun compatibility run (isolated output, does not overwrite `results/latest.json`):

```powershell
cd neutron/benchmarks
pnpm run compare:bun:quick
```

## Reproducibility

- Benchmark app dependencies are pinned to exact versions.
- Per-app lockfiles are committed.
- Harness installs benchmark apps with `--frozen-lockfile`.

## CI automation

- `Smoke Benchmark`: runs on PRs and `main` pushes using `compare:quick`.
- `Full Benchmark`: runs nightly (05:00 UTC) and on manual dispatch using `compare:full`.
- JSON outputs are uploaded as workflow artifacts for historical comparison.

## Repeat quickly without rebuild/install

```powershell
$env:BENCH_SKIP_PREPARE='1'
pnpm run compare
```

## Useful filters

Only selected frameworks:

```powershell
$env:BENCH_ONLY='neutron,neutron-react,next,remix,astro'
pnpm run compare
```

Only selected scenarios:

```powershell
$env:BENCH_SCENARIOS='static,dynamic,compute,big,mutate,login,protected,session-refresh'
pnpm run compare
```

## Tunables

- `BENCH_PROFILE` (`baseline` | `stress` | `saturation`)
- `BENCH_TRACK` (`node` | `optimal-static` | `both`)
- `BENCH_CONNECTIONS` (default `100`)
- `BENCH_DURATION` (default `20`, seconds per measured run)
- `BENCH_WARMUP` (default `8`, seconds)
- `BENCH_RUNS` (default `5`, median of runs)
- `BENCH_PIPELINING` (default `1`)
- `BENCH_READY_TIMEOUT_MS` (default `60000`)
- `BENCH_VERBOSE_SERVERS` (`1` to show server logs, default hidden)
- `BENCH_PAYLOAD_AUDIT` (`1` by default, set `0` to disable payload parity audit)
- `BENCH_PAYLOAD_WARN_RATIO` (default `1.3`; warns when max/min payload size ratio per scenario exceeds this)
- `BENCH_CONFORMANCE` (`1` by default, set `0` to skip feature conformance probes)
- `BENCH_STATIC_MEMORY_MAX_KB` (default `1024`; in-memory file threshold for static-host benchmark server)
- `PUBLISH_REPEATS` (default `3`; full benchmark repeats for publish-grade mode)
- `PUBLISH_SETTLE_SEC` (default `10`; cooldown delay between publish repeats)
- `PUBLISH_IDLE_SAMPLE_SEC` (default `15`; host CPU sample window before each repeat)
- `PUBLISH_IDLE_MAX_CPU_PCT` (default `20`; fail repeat if host is too busy)
- `PUBLISH_IDLE_ALLOW_BUSY` (`1` to bypass idle-host failure)
- `PUBLISH_BOOTSTRAPS` (default `2000`; bootstrap iterations for 95% CI)
- `PUBLISH_WINDOWS_PRIORITY` (default `High`; `none|idle|belownormal|normal|abovenormal|high|realtime`)
- `PUBLISH_CPU_AFFINITY` (default `auto`; decimal or hex bitmask, `none` to disable)
- `BUN_BENCH_CONNECTIONS` (default `60`)
- `BUN_BENCH_DURATION` (default `4`, seconds)
- `BUN_BENCH_WARMUP` (default `1`, seconds)
- `BUN_BENCH_RUNS` (default `1`)
- `BUN_BENCH_READY_TIMEOUT_MS` (default `60000`)
- `BUN_BENCH_INSTALL` (`1` to run install before build)

## Output

- `results/latest.json`
- `results/run-<timestamp>.json`

Both include raw per-run data and a median summary table.

Bun quick run output:

- `results/bun-quick-<timestamp>.json`

Publish-grade output:

- `results/publish-grade/repeat-*.json`
- `results/publish-grade/summary-*.json`
- `results/publish-grade/summary-*.md`

## Baseline comparison workflow

1. Run a canonical benchmark and pin baseline once.
2. Use the fast Neutron-only dev loop while building.
3. Re-pin baseline when you intentionally accept a new performance level.

```powershell
cd neutron/benchmarks
pnpm run compare:full
pnpm run baseline:pin
pnpm run baseline:pin:dev
pnpm run dev:bench
```

`dev:bench` defaults:

- `BENCH_ONLY=neutron`
- `BENCH_SCENARIOS=static,dynamic`
- `BENCH_SKIP_PREPARE=1`
- `BENCH_RUNS=2`
- `BENCH_DURATION=6`
- `BENCH_WARMUP=2`
- baseline file: `results/baseline-dev.json`

Override examples:

```powershell
$env:BENCH_SKIP_PREPARE='0'
pnpm run dev:bench
```

Enable regression gate during development:

```powershell
pnpm run dev:bench:gate
```

Smoke CI gate policy (Neutron only) is enforced with:

```powershell
pnpm run compare:gate:smoke
```

Default failure thresholds:

- RPS drop > `20%`
- p95 latency increase > `35%`

Override via env vars:

- `BENCH_GATE_BASELINE` (default `results/baseline.json`)
- `BENCH_GATE_FRAMEWORK` (default `neutron`)
- `BENCH_GATE_FAIL_RPS_DROP_PCT` (default `20`)
- `BENCH_GATE_FAIL_P95_INCREASE_PCT` (default `35`)
