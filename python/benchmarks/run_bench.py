"""ASGI benchmark orchestrator: neutron-py vs FastAPI, Starlette, Litestar.

Measures the same eight scenarios as the TypeScript harness
(``typescript/benchmarks/run-comparison.mjs``), with the same client
(autocannon), so results are comparable across the two suites. The TS apps
render HTML through full SSR frameworks; these are ASGI apps doing the same
per-route work (see bench_apps/common.py for the ported semantics), so
cross-language numbers are directional and in-suite numbers are the
framework-overhead comparison.

Usage (from python/benchmarks, with .venv-bench active or via its python):

    .venv-bench/bin/python run_bench.py
    .venv-bench/bin/python measure_noise.py --repeats 6

Tunables (defaults tuned for a local macOS run, NOT the TS CI profile):
    PYBENCH_CONNECTIONS (100)  PYBENCH_DURATION (5)
    PYBENCH_WARMUP (2)         PYBENCH_RUNS (3)
    PYBENCH_FRAMEWORKS         PYBENCH_SCENARIOS
    PYBENCH_PORT (8931)

Error policy: every run asserts zero socket errors and zero non-2xx
responses. Non-zero counts fail the whole invocation (exit 1) after results
are written; those runs are excluded from medians and reported separately.
Fast 500s must not be able to look like a win.
"""

from __future__ import annotations

import json
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

BENCH = Path(__file__).resolve().parent
PY_ROOT = BENCH.parent
REPO_ROOT = PY_ROOT.parent
VENV_PY = BENCH / ".venv-bench" / "bin" / "python"
RESULTS = BENCH / "results"

PORT = int(os.environ.get("PYBENCH_PORT", "8931"))
BASE_URL = f"http://127.0.0.1:{PORT}"

MUTATION_BODY = json.dumps({"seed": 13, "repeat": 6000})

# Exactly the TS node-track scenario set (run-comparison.mjs SCENARIO_SETS.node).
SCENARIOS = [
    {"id": "static", "method": "GET", "path": "/", "headers": {}},
    {"id": "dynamic", "method": "GET", "path": "/users/1", "headers": {}},
    {"id": "compute", "method": "GET", "path": "/compute", "headers": {}},
    {"id": "big", "method": "GET", "path": "/big", "headers": {}},
    {
        "id": "mutate",
        "method": "POST",
        "path": "/api/mutate",
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        "body": MUTATION_BODY,
    },
    {"id": "login", "method": "GET", "path": "/login", "headers": {}},
    {
        "id": "protected",
        "method": "GET",
        "path": "/protected",
        "headers": {"Authorization": "Bearer valid-token"},
    },
    {
        "id": "session-refresh",
        "method": "POST",
        "path": "/api/session/refresh",
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "Bearer valid-token",
        },
        "body": "{}",
    },
]

FRAMEWORKS = [
    {
        "id": "neutron",
        "module": "bench_apps.neutron_app:app",
        "env": {},
    },
    {
        "id": "neutron-default",
        "module": "bench_apps.neutron_app:app",
        "env": {"NEUTRON_BENCH_STACK": "default"},
    },
    {"id": "starlette", "module": "bench_apps.starlette_app:app", "env": {}},
    {"id": "fastapi", "module": "bench_apps.fastapi_app:app", "env": {}},
    {"id": "litestar", "module": "bench_apps.litestar_app:app", "env": {}},
]


def int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


CONFIG = {
    "connections": int_env("PYBENCH_CONNECTIONS", 100),
    "durationSec": int_env("PYBENCH_DURATION", 5),
    "warmupSec": int_env("PYBENCH_WARMUP", 2),
    "runs": int_env("PYBENCH_RUNS", 3),
}


# --- Server lifecycle -------------------------------------------------------


def start_server(fw: dict) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(fw["env"])
    env["PYTHONUNBUFFERED"] = "1"
    cmd = [
        str(VENV_PY),
        "-m",
        "uvicorn",
        fw["module"],
        "--host",
        "127.0.0.1",
        "--port",
        str(PORT),
        "--no-access-log",
        "--log-level",
        "warning",
    ]
    # DEVNULL matters: neutron-default logs one structlog event per request,
    # and a blocked or slow pipe would distort the very thing we measure.
    return subprocess.Popen(
        cmd,
        cwd=BENCH,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def wait_ready(timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{BASE_URL}/", timeout=1.0)
            if r.status_code == 200:
                return
        except Exception as e:  # noqa: BLE001 - race with server boot
            last_exc = e
        time.sleep(0.1)
    raise RuntimeError(f"server never became ready: {last_exc}")


def stop_server(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


# --- Parity probes: every app must do the same work, byte-identically -------


def parity_probe() -> None:
    sys.path.insert(0, str(BENCH))
    from bench_apps import common

    expected_compute = common.compute_html(common.bench_compute())
    expected_mutate = {
        "ok": True,
        "seed": 13,
        "repeat": 6000,
        "value": common.run_mutation(13, 6000),
    }
    expected_refresh = {"ok": True, "refreshed": True, "token": "valid-token"}
    auth = {"Authorization": "Bearer valid-token"}

    checks: list[tuple[str, str, dict, int, object, str | None]] = [
        ("GET", "/", {}, 200, common.STATIC_HTML, "text/html"),
        ("GET", "/users/1", {}, 200, common.user_html("1"), "text/html"),
        ("GET", "/compute", {}, 200, expected_compute, "text/html"),
        ("GET", "/big", {}, 200, common.big_html(), "text/html"),
        ("GET", "/login", {}, 200, common.LOGIN_HTML, "text/html"),
        (
            "POST",
            "/api/mutate",
            {"Content-Type": "application/json"},
            200,
            expected_mutate,
            "application/json",
        ),
        ("GET", "/protected", auth, 200, common.protected_html(True), "text/html"),
        (
            "POST",
            "/api/session/refresh",
            {"Content-Type": "application/json", **auth},
            200,
            expected_refresh,
            "application/json",
        ),
        # Negative auth paths: 401 everywhere, same body.
        (
            "GET",
            "/protected",
            {},
            401,
            {"ok": False, "error": "Unauthorized"},
            "application/json",
        ),
        (
            "POST",
            "/api/session/refresh",
            {"Content-Type": "application/json"},
            401,
            {"ok": False, "error": "Unauthorized"},
            "application/json",
        ),
    ]

    failures = []
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as client:
        for method, path, headers, status, expected, ctype in checks:
            body = None
            if method == "POST":
                body = (
                    MUTATION_BODY
                    if path == "/api/mutate"
                    else "{}"
                )
            r = client.request(method, path, headers=headers, content=body)
            tag = f"{method} {path}"
            if r.status_code != status:
                failures.append(
                    f"{tag}: status {r.status_code} != {status}"
                )
                continue
            actual = r.json() if ctype == "application/json" else r.text
            if actual != expected:
                failures.append(f"{tag}: body mismatch")
                continue
            ct = r.headers.get("content-type", "")
            if not ct.startswith(ctype):
                failures.append(f"{tag}: content-type {ct!r} !~ {ctype!r}")
    if failures:
        raise RuntimeError("parity probe failed:\n  " + "\n  ".join(failures))


# --- Measurement ------------------------------------------------------------


def run_client(scenario: dict, duration: int) -> dict:
    cfg = {
        "baseUrl": BASE_URL,
        "path": scenario["path"],
        "method": scenario["method"],
        "headers": scenario["headers"],
        "body": scenario.get("body"),
        "connections": CONFIG["connections"],
        "durationSec": duration,
    }
    out = subprocess.run(
        ["node", str(BENCH / "client.mjs"), json.dumps(cfg)],
        capture_output=True,
        text=True,
        timeout=duration + 60,
    )
    if out.returncode != 0:
        raise RuntimeError(
            f"client failed for {scenario['id']}: {out.stderr.strip()}"
        )
    return json.loads(out.stdout)


def normalize(scenario_id: str, fw_id: str, raw: dict) -> dict:
    lat = raw.get("latency", {})
    return {
        "framework": fw_id,
        "scenario": scenario_id,
        "requestsPerSec": round(raw["requests"]["average"], 2),
        "totalRequests": raw["requests"].get("total"),
        "p50Ms": lat.get("p50"),
        "p90Ms": lat.get("p90"),
        "p97_5Ms": lat.get("p97_5"),
        "p99Ms": lat.get("p99"),
        "throughputMBps": round(
            raw["throughput"]["average"] / (1024 * 1024), 2
        ),
        "socketErrors": raw.get("errors", 0),
        "timeouts": raw.get("timeouts", 0),
        "resets": raw.get("resets", 0),
        "non2xx": raw.get("non2xx", 0),
        "statusCodes": raw.get("statusCodeStats", {}),
    }


def run_cell(fw: dict, scenario: dict, *, runs: int, warmup: bool) -> list[dict]:
    if warmup:
        run_client(scenario, CONFIG["warmupSec"])
    return [
        normalize(scenario["id"], fw["id"], run_client(scenario, CONFIG["durationSec"]))
        for _ in range(runs)
    ]


# --- Provenance -------------------------------------------------------------


def provenance() -> dict:
    from importlib.metadata import version, PackageNotFoundError

    def ver(dist: str) -> str:
        try:
            return version(dist)
        except PackageNotFoundError:
            return "not-installed"

    cpu = ""
    if sys.platform == "darwin":
        try:
            cpu = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip()
        except Exception:  # noqa: BLE001
            pass
    node_ver = subprocess.run(
        ["node", "--version"], capture_output=True, text=True
    ).stdout.strip()
    # Same precedence as client.mjs: an AUTOCANNON_MODULE override changes what
    # actually drives the load, so it must also change what provenance records —
    # otherwise the artifact claims an unknown client for a known install.
    autocannon_dir = Path(
        os.environ.get("AUTOCANNON_MODULE")
        or REPO_ROOT / "typescript/benchmarks/node_modules/autocannon"
    )
    autocannon_pkg = autocannon_dir / "package.json"
    autocannon_ver = "?"
    if autocannon_pkg.exists():
        autocannon_ver = json.loads(autocannon_pkg.read_text())["version"]

    return {
        "recorded": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "machine": {
            "cpu": cpu or platform.processor() or "unknown",
            "cores": os.cpu_count(),
            "platform": platform.platform(),
            "host": "developer laptop, wall-power, idle-ish",
        },
        "python": sys.version.split()[0],
        "packages": {
            "neutron-py": ver("neutron-py"),
            "starlette": ver("starlette"),
            "fastapi": ver("fastapi"),
            "litestar": ver("litestar"),
            "uvicorn": ver("uvicorn"),
            "pydantic": ver("pydantic"),
            "httptools": ver("httptools"),
            "uvloop": ver("uvloop"),
            "httpx": ver("httpx"),
        },
        "client": f"autocannon {autocannon_ver} via node {node_ver}",
        "server": (
            "uvicorn CLI, 1 worker, --no-access-log, default loop (uvloop) "
            "and http (httptools) implementations; app stdout/stderr to DEVNULL"
        ),
        "profile": {
            **CONFIG,
            "pipelining": 1,
            "workers": 1,
        },
        "exclusions": [
            "uvicorn access log disabled for all frameworks (server-level, "
            "not framework-level)",
            "neutron-default's per-request structlog output goes to "
            "DEVNULL; the formatting cost is still measured, the pipe is not",
            "no CDN/proxy; plain HTTP/1.1 over loopback",
        ],
    }


# --- Matrix -----------------------------------------------------------------


def select_frameworks(offset: int = 0) -> list[dict]:
    wanted = os.environ.get("PYBENCH_FRAMEWORKS")
    fw_list = FRAMEWORKS
    if wanted:
        ids = [w.strip() for w in wanted.split(",")]
        fw_list = [f for f in FRAMEWORKS if f["id"] in ids]
    if offset:
        # Rotate measurement order between repeats so that framework and
        # within-run position (this machine drifts as it warms) are not
        # confounded. Position effects cancel across a full rotation.
        fw_list = fw_list[offset % len(fw_list) :] + fw_list[: offset % len(fw_list)]
    return fw_list


def select_scenarios() -> list[dict]:
    wanted = os.environ.get("PYBENCH_SCENARIOS")
    if not wanted:
        return SCENARIOS
    ids = [w.strip() for w in wanted.split(",")]
    return [s for s in SCENARIOS if s["id"] in ids]


def run_matrix(
    *, runs: int, warmup_sec: int | None = None, parity: bool, rotate: int = 0
) -> list[dict]:
    """One pass over every selected framework x scenario. Returns raw rows."""
    global CONFIG
    if warmup_sec is not None:
        CONFIG = {**CONFIG, "warmupSec": warmup_sec}
    CONFIG = {**CONFIG, "runs": runs}
    rows: list[dict] = []
    for fw in select_frameworks(offset=rotate):
        print(f"[{fw['id']}] starting server...", flush=True)
        proc = start_server(fw)
        try:
            wait_ready()
            if parity:
                parity_probe()
                print(f"[{fw['id']}] parity probes passed", flush=True)
            for scenario in select_scenarios():
                cell = run_cell(fw, scenario, runs=runs, warmup=warmup_sec != 0)
                rows.extend(cell)
                last = cell[-1]
                print(
                    f"[{fw['id']}] {scenario['id']:<16} "
                    f"rps={last['requestsPerSec']:>9.1f} "
                    f"p99={last['p99Ms']}ms "
                    f"non2xx={last['non2xx']} errs={last['socketErrors']}",
                    flush=True,
                )
        finally:
            stop_server(proc)
        time.sleep(1.0)
    return rows


def summarize(rows: list[dict]) -> list[dict]:
    """Median over the OK runs of each cell; failed runs excluded and counted."""
    buckets: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        buckets.setdefault((r["framework"], r["scenario"]), []).append(r)

    summary = []
    for (fw, sc), runs_rows in buckets.items():
        ok = [
            r
            for r in runs_rows
            if r["non2xx"] == 0
            and r["socketErrors"] == 0
            and r["timeouts"] == 0
        ]
        entry = {
            "framework": fw,
            "scenario": sc,
            "okRuns": len(ok),
            "failedRuns": len(runs_rows) - len(ok),
            "_samples": len(runs_rows),
        }
        for field in (
            "requestsPerSec",
            "p50Ms",
            "p90Ms",
            "p97_5Ms",
            "p99Ms",
            "throughputMBps",
        ):
            vals = [r[field] for r in ok if r[field] is not None]
            if vals:
                entry[field] = round(statistics.median(vals), 2)
        summary.append(entry)
    return summary


def gate(rows: list[dict]) -> list[str]:
    problems = []
    for r in rows:
        bad = r["non2xx"] or r["socketErrors"] or r["timeouts"]
        if bad:
            problems.append(
                f"{r['framework']}/{r['scenario']}: "
                f"non2xx={r['non2xx']} socketErrors={r['socketErrors']} "
                f"timeouts={r['timeouts']} -> excluded from medians"
            )
    return problems


def print_table(summary: list[dict]) -> None:
    scenarios = sorted({s["scenario"] for s in summary})
    frameworks = [f["id"] for f in select_frameworks()]
    print(f"\n{'scenario':<18}" + "".join(f"{f:>18}" for f in frameworks))
    for sc in scenarios:
        cells = []
        for fw in frameworks:
            match = next(
                (s for s in summary if s["framework"] == fw and s["scenario"] == sc),
                None,
            )
            cells.append(
                f"{match['requestsPerSec']:>18.1f}" if match and "requestsPerSec" in match
                else f"{'-':>18}"
            )
        print(f"{sc:<18}" + "".join(cells))
    print("\n(median RPS over OK runs; p50/p90/p97.5/p99 in results JSON)")


def main() -> int:
    if not VENV_PY.exists():
        print(
            "bench venv missing — create it first:\n"
            "  uv venv benchmarks/.venv-bench --python 3.12\n"
            "  uv pip install --python benchmarks/.venv-bench/bin/python "
            '-e ".[crypto]" fastapi "litestar[standard]" "uvicorn[standard]" httpx editables',
            file=sys.stderr,
        )
        return 2

    prov = provenance()
    print(f"benchmarking on python {prov['python']}, {prov['client']}")
    rows = run_matrix(runs=CONFIG["runs"], parity=True)
    summary = summarize(rows)
    problems = gate(rows)

    RESULTS.mkdir(exist_ok=True)
    doc = {
        "provenance": prov,
        "summary": summary,
        "results": rows,
    }
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    (RESULTS / f"run-{ts}.json").write_text(json.dumps(doc, indent=2) + "\n")
    (RESULTS / "latest.json").write_text(json.dumps(doc, indent=2) + "\n")
    print(f"\nwrote results/run-{ts}.json and results/latest.json")

    print_table(summary)
    if problems:
        print("\nERROR-COUNT VIOLATIONS (runs above were excluded from medians):")
        for p in problems:
            print(f"  {p}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
