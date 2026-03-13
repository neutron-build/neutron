# Benchmarks

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Benchmark harness lives in `benchmarks/` and compares **Neutron TypeScript** against:

- Next.js
- Remix 2
- Remix 3 (React Router Framework)
- Astro
- Neutron React-compat mode

## Commands

From repo root:

```bash
pnpm --dir benchmarks run compare:node
pnpm --dir benchmarks run compare:quick
pnpm --dir benchmarks run compare:full
pnpm --dir benchmarks run compare:canonical
pnpm --dir benchmarks run compare:publish-grade
pnpm --dir benchmarks run compare:publish:preflight
pnpm --dir benchmarks run compare:publish:official
pnpm --dir benchmarks run compare:stress
pnpm --dir benchmarks run compare:saturation
pnpm --dir benchmarks run compare:optimal
pnpm --dir benchmarks run compare:tracks
pnpm --dir benchmarks run compare:gate:smoke
```

Root aliases:

```bash
pnpm run ci:bench:smoke
pnpm run ci:bench:full
pnpm run ci:bench:publish
pnpm run ci:bench:publish:preflight
pnpm run ci:bench:publish:official
```

## Result Files

- `benchmarks/results/latest.json`
- `benchmarks/results/run-*.json`

## Useful Environment Flags

- `BENCH_ONLY=neutron,next,remix,remix3,astro,neutron-react`
- `BENCH_SCENARIOS=static,dynamic,compute,big,mutate,login,protected,session-refresh`
- `BENCH_TRACK=node|optimal-static|both`
- `BENCH_PROFILE=baseline|stress|saturation`
- `BENCH_RUNS=1|3|5`
- `BENCH_DURATION=<seconds>`
- `BENCH_WARMUP=<seconds>`
- `BENCH_CONNECTIONS=<count>`
- `BENCH_VERBOSE_SERVERS=1`
- `BENCH_PAYLOAD_AUDIT=0|1`
- `BENCH_PAYLOAD_WARN_RATIO=<ratio>`
- `BENCH_CONFORMANCE=0|1`
- `BENCH_STATIC_MEMORY_MAX_KB=<kb>` (optional static host in-memory file threshold)
- `PUBLISH_REPEATS=<count>` (default `3`; publish-grade repeat count)
- `PUBLISH_SETTLE_SEC=<seconds>` (default `10`; cooldown between repeats)
- `PUBLISH_IDLE_SAMPLE_SEC=<seconds>` (default `15`; CPU sampling window per repeat)
- `PUBLISH_IDLE_MAX_CPU_PCT=<percent>` (default `20`; abort on busy host)
- `PUBLISH_IDLE_ALLOW_BUSY=0|1` (`1` bypasses idle-host abort)
- `PUBLISH_BOOTSTRAPS=<count>` (default `2000`; bootstrap 95% CI iterations)
- `PUBLISH_WINDOWS_PRIORITY=none|idle|belownormal|normal|abovenormal|high|realtime`
- `PUBLISH_CPU_AFFINITY=auto|none|<mask>` (decimal or hex like `0xFF`)

Example:

```bash
BENCH_ONLY=neutron,astro BENCH_SCENARIOS=static BENCH_RUNS=1 pnpm --dir benchmarks run compare:quick
```

## Scenario Matrix

- `static`: `GET /`
- `dynamic`: `GET /users/1`
- `compute`: `GET /compute`
- `big`: `GET /big`
- `mutate`: `POST /api/mutate` with JSON body
- `login`: `GET /login`
- `protected`: `GET /protected` with `Authorization: Bearer valid-token`
- `session-refresh`: `POST /api/session/refresh` with `Authorization: Bearer valid-token`

## Tracks

- `node`: same-profile Node runtime comparison across frameworks.
- `optimal-static`: framework-optimal static deployment path for `/`.
- `both`: executes both tracks in one run and writes a combined result artifact.

Canonical publish profile:

- `compare:canonical` runs baseline profile with both tracks, payload audit, and conformance enabled.
- Use this for public benchmark reporting.

Strict publish-grade profile:

- `compare:publish-grade` wraps `run-comparison` with repeated runs, host-idle preflight checks, median-of-medians aggregation, and bootstrap 95% confidence intervals.
- Output is written under `benchmarks/results/publish-grade/`.
- `compare:publish:preflight` is a short, low-cost sanity pass (default static-only, single run/repeat).
- `compare:publish:official` pins the full publication profile (both tracks, conformance, payload audit, 3x repeats).

## Conformance Matrix

Each run (unless `BENCH_CONFORMANCE=0`) includes a feature matrix in results:

- `SSG`
- `SSR`
- `ISR-like cache invalidation`
- `Streaming`
- `Actions`
- `Auth`

## Regression Gate

Smoke CI runs `compare:gate:smoke` to enforce a Neutron regression policy on benchmark medians.
