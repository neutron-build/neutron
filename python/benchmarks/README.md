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

## Known caveats

- `compute`/`mutate` are GIL-bound pure-Python arithmetic. CPython executes
  these loops ~10x slower than V8 executes the identical TS loops, so those
  two scenarios measure the language runtime, not the framework; the
  framework signal there is small relative to the interpreter cost.
- Single machine, single worker, loopback. Numbers are not comparable to
  other machines; the reproducible artifact is the harness plus the pinned
  environment, not any individual figure.
