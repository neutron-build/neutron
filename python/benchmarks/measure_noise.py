"""Measure the local noise floor and derive a regression-gate threshold.

The TypeScript harness derives its CI gate thresholds from repeated runs on
the real infrastructure (scripts/measure-gate-noise.mjs), because GitHub
runners come in at least two performance classes and a hand-tuned threshold
either cries wolf or misses everything. This script does the same derivation
for the Python suite, but on THIS machine: it re-runs the full matrix R
times, restarting servers each repeat (so process-restart variance is in the
noise too), computes the per-cell median across repeats, and reports the
worst single-repeat deviation for the neutron rows. That deviation IS the
noise floor; any gate threshold must sit above it.

Usage:
    .venv-bench/bin/python measure_noise.py [--repeats 6]

Writes results/noise-<ts>.json.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

import run_bench

BENCH = Path(__file__).resolve().parent
RESULTS = BENCH / "results"

# The gate would guard neutron itself. The default-stack variant is reported
# too, but a gate should be per-configuration; both are shown.
GATE_FRAMEWORKS = ["neutron", "neutron-default"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=6)
    args = parser.parse_args()

    prov = run_bench.provenance()
    print(
        f"noise measurement: {args.repeats} repeats of the full matrix, "
        f"profile {prov['profile']}"
    )

    all_rows: list[dict] = []
    repeat_loads: list[dict] = []
    for i in range(args.repeats):
        # A noise run is only meaningful if the machine stayed comparably
        # loaded throughout; 2026-08-19's first full run was contaminated by
        # a concurrent 4-core rustc build and showed 100% phantom "drops".
        # Recording load per repeat makes that taint visible in the artifact
        # instead of discoverable only by correlating with ps afterwards.
        load1 = os.getloadavg()[0] if hasattr(os, "getloadavg") else None
        repeat_loads.append({"repeat": i, "load1": round(load1, 2) if load1 else None})
        suffix = f" (load1={load1:.1f})" if load1 else ""
        print(f"\n=== repeat {i + 1}/{args.repeats}{suffix} ===", flush=True)
        rows = run_bench.run_matrix(
            runs=1,
            warmup_sec=run_bench.CONFIG["warmupSec"],
            parity=False,
            rotate=i,
        )
        for r in rows:
            r["repeat"] = i
        all_rows.extend(rows)

    problems = run_bench.gate(all_rows)
    if problems:
        print("\nERROR-COUNT VIOLATIONS — noise data is tainted, not deriving:")
        for p in problems:
            print(f"  {p}")
        return 1

    # Per-cell median across repeats, then worst deviation of any single
    # repeat from that median — same derivation as measure-gate-noise.mjs.
    buckets: dict[tuple[str, str], list[dict]] = {}
    for r in all_rows:
        buckets.setdefault((r["framework"], r["scenario"]), []).append(r)

    per_scenario: dict[str, dict] = {}
    worst_drop = 0.0
    worst_rise = 0.0
    for (fw, sc), rows in sorted(buckets.items()):
        if fw in GATE_FRAMEWORKS:
            rps_vals = [r["requestsPerSec"] for r in rows]
            p99_vals = [r["p99Ms"] for r in rows if r["p99Ms"] is not None]
            med_rps = statistics.median(rps_vals)
            med_p99 = statistics.median(p99_vals) if p99_vals else None
            drops = [
                (med_rps - r["requestsPerSec"]) / med_rps * 100 for r in rows
            ]
            rises = [
                (r["p99Ms"] - med_p99) / med_p99 * 100
                for r in rows
                if r["p99Ms"] is not None
            ] if med_p99 else [0.0]
            worst_drop = max(worst_drop, max(drops))
            worst_rise = max(worst_rise, max(rises))
            cur = per_scenario.get(f"{fw}/{sc}", {"drop": 0.0, "rise": 0.0})
            per_scenario[f"{fw}/{sc}"] = {
                "drop": max(cur["drop"], max(drops)),
                "rise": max(cur["rise"], max(rises)),
            }

    # Build per-cell summary for output (raw rows already in all_rows).
    cell_summary = {}
    for (fw, sc), rows in buckets.items():
        rps_vals = [r["requestsPerSec"] for r in rows]
        p99_vals = [r["p99Ms"] for r in rows if r["p99Ms"] is not None]
        med_rps = statistics.median(rps_vals)
        med_p99 = statistics.median(p99_vals) if p99_vals else None
        entry = {
            "medianRps": round(med_rps, 2),
            "rawRps": rps_vals,
            "rpsSpreadPct": round(
                (max(rps_vals) - min(rps_vals)) / med_rps * 100, 1
            ),
        }
        if med_p99:
            entry["medianP99Ms"] = med_p99
            entry["worstRpsDropPct"] = round(
                max((med_rps - v) / med_rps * 100 for v in rps_vals), 1
            )
            entry["worstP99RisePct"] = round(
                max((v - med_p99) / med_p99 * 100 for v in p99_vals), 1
            )
        cell_summary[f"{fw}/{sc}"] = entry

    print(f"\n{'neutron cell':<40}{'rps drop':>10}{'p99 rise':>10}   gate")
    for key, v in sorted(per_scenario.items(), key=lambda kv: kv[1]["drop"]):
        suggested = (
            25
            if v["drop"] < 5
            else math.ceil((v["drop"] * 1.4) / 5) * 5
        )
        print(
            f"{key:<40}{v['drop']:>9.1f}%{v['rise']:>9.1f}%"
            f"   {suggested:>3}%"
            + ("   <- stable enough to gate tightly" if v["drop"] < 5 else "")
        )

    print(
        f"\nworst overall (neutron rows): {worst_drop:.1f}% rps drop, "
        f"{worst_rise:.1f}% p99 rise across {args.repeats} repeats"
    )

    RESULTS.mkdir(exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    doc = {
        "provenance": prov,
        "repeats": args.repeats,
        "loadAvgPerRepeat": repeat_loads,
        "gateFrameworks": GATE_FRAMEWORKS,
        "worst": {
            "rpsDropPct": round(worst_drop, 1),
            "p99RisePct": round(worst_rise, 1),
        },
        "cells": cell_summary,
    }
    (RESULTS / f"noise-{ts}.json").write_text(json.dumps(doc, indent=2) + "\n")
    print(f"wrote results/noise-{ts}.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
