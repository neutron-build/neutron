#!/bin/sh
# Consolidate criterion's per-benchmark estimates into one uploadable file.
#
# `cargo bench` prints its numbers and criterion writes them under
# target/criterion/**/new/estimates.json — and CI then throws the whole lot away
# when the runner is reclaimed. So the benchmark has run on every qualifying
# push for months and has never once been compared against its own past. That is
# the "uploaded as artifacts nobody opens" problem one step worse: not even
# uploaded.
#
# This does NOT gate. It cannot yet: a threshold set before the noise is
# measured is a guess, and on shared runners the noise is the dominant term —
# the TypeScript gate's own arming measurement found GitHub's pool has at least
# two performance classes, worth 40% on I/O-bound work. The equivalent number
# for these benchmarks is unknown because nothing has ever recorded it.
#
# So: record first. Once BENCH_MIN_RUNS runs of this artifact exist, derive the
# per-benchmark noise floor the same way the TypeScript gate did
# (typescript/benchmarks/scripts/measure-gate-noise.mjs is the worked example)
# and arm from the data.
#
# Usage: sh scripts/collect-bench-estimates.sh [output.json]
set -eu

OUT="${1:-bench-estimates.json}"
ROOT="target/criterion"

if [ ! -d "$ROOT" ]; then
    echo "no $ROOT — did cargo bench run?" >&2
    exit 1
fi

python3 - "$ROOT" "$OUT" <<'PY'
import json, os, sys, datetime

root, out = sys.argv[1], sys.argv[2]
entries = {}
for dirpath, _dirs, files in os.walk(root):
    # criterion writes the current run under <bench>/new/ and the prior under
    # <bench>/base/. Only `new` is this run.
    if os.path.basename(dirpath) != "new" or "estimates.json" not in files:
        continue
    name = os.path.relpath(os.path.dirname(dirpath), root).replace(os.sep, "/")
    try:
        with open(os.path.join(dirpath, "estimates.json")) as fh:
            est = json.load(fh)
    except (OSError, ValueError) as exc:
        print(f"skipping {name}: {exc}", file=sys.stderr)
        continue
    # Median is the stable summary; criterion's own point estimate for it.
    median = est.get("median", {}).get("point_estimate")
    mean = est.get("mean", {}).get("point_estimate")
    if median is None and mean is None:
        continue
    entries[name] = {
        "medianNs": median,
        "meanNs": mean,
        "stdDevNs": est.get("std_dev", {}).get("point_estimate"),
    }

if not entries:
    print(f"no estimates found under {root}", file=sys.stderr)
    sys.exit(1)

doc = {
    "schema": 1,
    "recorded": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    "commit": os.environ.get("GITHUB_SHA", ""),
    "runId": os.environ.get("GITHUB_RUN_ID", ""),
    "runner": os.environ.get("RUNNER_NAME", ""),
    "benchmarks": dict(sorted(entries.items())),
}
with open(out, "w") as fh:
    json.dump(doc, fh, indent=2)
    fh.write("\n")
print(f"wrote {out} with {len(entries)} benchmark estimate(s)")
PY
