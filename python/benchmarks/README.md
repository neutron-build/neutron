# Python ASGI Benchmark Protocol

Measures `neutron-py` against its real peer set — **FastAPI, Starlette,
Litestar** — on the same eight scenarios the TypeScript harness uses
(`typescript/benchmarks/run-comparison.mjs`), with the same client
(autocannon), so the two suites share vocabulary and scenario shapes.

## What it tests

- Frameworks (5 rows):
  - `neutron` — `App()` with no user middleware (routing/serialization
    overhead on top of the Starlette it wraps)
  - `neutron-default` — `App(middleware=default_stack())`, the documented
    production posture: a uuid4 request-id plus a structlog event per request
  - `starlette` — bare Starlette, the floor both neutron-py and FastAPI sit on
  - `fastapi` — `FastAPI()` defaults, pydantic-validated body
  - `litestar` — `Litestar()` defaults, pydantic body via DTO
- Scenarios (exact ports of the TS routes; see `bench_apps/common.py` for the
  provenance of every constant):
  - `static`: `GET /` (constant HTML)
  - `dynamic`: `GET /users/1` (dict lookup + render)
  - `compute`: `GET /compute` (140k-iteration arithmetic loop)
  - `big`: `GET /big` (400 rows rendered per request, ~16 KB HTML)
  - `mutate`: `POST /api/mutate` with `{"seed":13,"repeat":6000}`
  - `login`: `GET /login` (constant HTML)
  - `protected`: `GET /protected` with `Authorization: Bearer valid-token`
  - `session-refresh`: `POST /api/session/refresh` (auth check, body `{}`)
- Parity probes run against every app before any measurement: status codes,
  byte-identical bodies, content types, and the negative-auth 401 paths.
  `compute` and `mutate` return values are asserted equal to the TypeScript
  implementation's values (JS and Python verified to produce 719963 and
  258368509 respectively).

## Fairness model

- Same server for everyone: one uvicorn worker, `--no-access-log`, default
  uvloop + httptools, app stdout/stderr to DEVNULL.
- Same client for everyone: autocannon (borrowed read-only from
  `typescript/benchmarks/node_modules`; override with `AUTOCANNON_MODULE`),
  100 connections, HTTP/1.1, pipelining 1.
- Frameworks run as shipped: FastAPI and Litestar keep their default
  openapi/docs routes (not measured); `neutron-default`'s per-request logging
  cost is measured and reported as its own row rather than hidden.
- The TS harness measures SSR frameworks rendering HTML; these are ASGI apps
  doing the same per-route work. In-suite comparisons are apples-to-apples;
  cross-suite (TS vs Python) numbers are directional only.

## Error policy

Every run asserts zero socket errors, zero timeouts, and zero non-2xx
responses. Violations are recorded, excluded from medians, printed, and fail
the invocation (exit 1). A framework serving 500s fast cannot look like a
win — this is the exact defect that produced the bad Nucleus numbers.

## Run it

```bash
# one-time setup (venv lives inside python/benchmarks)
uv venv benchmarks/.venv-bench --python 3.12
uv pip install --python benchmarks/.venv-bench/bin/python \
  -e ".[crypto]" fastapi "litestar[standard]" "uvicorn[standard]" httpx editables
# peer versions are frozen in benchmarks/requirements-bench.txt — a rerun
# should install from that file, not from latest

cd python/benchmarks
.venv-bench/bin/python run_bench.py          # main matrix
.venv-bench/bin/python measure_noise.py --repeats 6   # noise floor + gate check
```

If `typescript/benchmarks/node_modules` is absent (it is a borrowed install,
not a dependency of this suite), point `AUTOCANNON_MODULE` at any autocannon
8.x — e.g. `npm install autocannon@8.0.0` in a scratch dir and export
`AUTOCANNON_MODULE=<dir>/node_modules/autocannon`. The recorded provenance
follows the override, so the artifact still names the client it used.

Tunables (local defaults, NOT the TS CI profile): `PYBENCH_CONNECTIONS`
(100), `PYBENCH_DURATION` (5), `PYBENCH_WARMUP` (2), `PYBENCH_RUNS` (3),
`PYBENCH_FRAMEWORKS`, `PYBENCH_SCENARIOS`, `PYBENCH_PORT`.

Outputs: `results/run-<ts>.json`, `results/latest.json`,
`results/noise-<ts>.json` — every file carries a provenance block (machine,
Python, package versions, client, profile, exclusions). Raw per-run rows are
kept, not just medians.

## Noise, gates, and honest limits

`measure_noise.py` repeats the full matrix with rotated framework order
(this machine's throughput drifts with warm-up state, so fixed order would
confound framework with position), computes per-cell medians, and reports the
worst single-repeat deviation for the neutron rows. That deviation is the
noise floor. Do not wire a CI regression gate below it. The suggested
thresholds follow the same derivation as
`typescript/benchmarks/scripts/measure-gate-noise.mjs`; if the floor is too
wide for a gate at the current profile, that is the finding — say so, don't
tune the number.

**The floor was measured (2026-08-19, Apple M4 / 10 cores / macOS): no gate
is possible on this harness on this machine class, so none is wired.**
Three runs, all 6 repeats of the full matrix:

- Quietest attainable window (background load still swung the 1-min loadavg
  from 7 to 35 during the run; a dev laptop is never idle while in use):
  worst single-repeat rps drop on neutron rows **95.4%**, six cells above
  79%.
- Contaminated window (a concurrent 4-core `rustc` build; loadavg 8 → 98):
  worst drop **100%** — one neutron/compute repeat measured 0 rps with zero
  errors, a green run indistinguishable from a dead framework.
- The interrupted 2026-08-18 run (4 repeats, "idle-ish"): worst drop
  **76.8%**.

A 10x regression is a 90% rps drop — at or below the green-run floor, so a
threshold high enough to never fire on a green run can never fire on a real
regression either. A paired within-repeat ratio (neutron/starlette, which
load bursts should cancel if anything could) deviates up to 257% from its
own median, so ratio gates are out too. The suggested-threshold column that
`measure_noise.py` prints is a derivation, not a recommendation: when it
says 135%, the answer is "no gate", not "gate at 135%".

No competitive figures are publishable from this machine either: framework
medians differ by less than ~26% on every scenario while single cells swing
1.6x–36x across green repeats of the same framework. Revisit only on a
machine whose background load is controlled (dedicated runner; the noise
artifacts now record the per-repeat loadavg precisely so contamination is
visible in the artifact, not inferred afterwards).

## Known caveats

- `compute`/`mutate` are GIL-bound pure-Python arithmetic. CPython executes
  these loops ~10x slower than V8 executes the identical TS loops, so those
  two scenarios measure the language runtime, not the framework; the
  framework signal there is small relative to the interpreter cost.
- Single machine, single worker, loopback. Numbers are not comparable to
  other machines; the reproducible artifact is the harness plus the pinned
  environment, not any individual figure.
