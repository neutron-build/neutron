#!/usr/bin/env bash
# X-5c-F4-F3 step 2: the probe tool (card Work, "The probe").
#
# Forms:
#   x5_probe.sh <name> <outdir>        five 200-seed `run --seeds
#                                       <b>..<b+200> --threads 16` blocks
#                                       over 0..1000, under `ulimit -v
#                                       25165824`, the release binary run
#                                       directly
#   x5_probe.sh --mp <name> <outdir>   one `run --probe mp-pressure
#                                       --threads 16` (the 200 MP-class
#                                       seeds of 30000..31000)
#
# Output contract (stdout), everything else going to <outdir>/<name>.log:
#   - `PROBE_ROW <id> raw=<sum> per10k=<sum × 10>` for every
#     `PROBE_QUOTA` row the run prints (the same `quotas()`/`q4i_count`
#     `verify` uses, so a probe cannot count differently). For `--mp`
#     the `per10k` field holds `raw × 10`, which is the 2,000-seed
#     set's count.
#   - `PROBE_HOOK <hook> raw=<n> per10k=<n × 10>` per crash hook: the
#     per-hook half of the Q1 rows, which `PROBE_QUOTA` prints without
#     the hook name (one `Q1` line per hook). Added so the stop rule's
#     hold set ("no row that met its minimum ... falls below that
#     minimum") can be read per hook, as the Q1 rows are defined.
#   - one `PROBE_WALL raw_s=<s> min_per_10k=<x>` (for `--mp`,
#     `min_per_2000=<x>`): the summed wall of the blocks, scaled by
#     10000/1000 (2000/200): minutes per 10k (2k) seeds.
#     `load_at_start` is /proc/loadavg's 1/5/15-minute averages.
#   - one `PROBE_CPU user_sys_s=<s>`: the summed user+sys of the
#     blocks, by bash `time`.
#   - one `PROBE_FAIL <class> <seed> r<n> <text>` per failing round
#     (a round whose `pass` is false — the `FAILING_ROUNDS` fold), the
#     text the round's o1, o2 or o3 field, cut to 80 characters, read
#     from the JSON with `python3 -I`.
#
# Invariants (card Run hygiene): every process runs at most 200
# contiguous seeds under `ulimit -v 25165824`; no seed in
# 10000..30000 is ever run (the `--mp` form runs exactly the driver's
# own `--probe mp-pressure` seed list); a probe records no set. The
# binary must already be built (${CARGO_TARGET_DIR}/release/e4-campaign):
# the tool never invokes cargo, so a probe is the binary and nothing
# else. Every number it prints is a probe, not evidence.
set -uo pipefail

MP=0
if [ "${1:-}" = "--mp" ]; then
  MP=1
  shift
fi
if [ $# -ne 2 ]; then
  echo "usage: x5_probe.sh [--mp] <name> <outdir>" >&2
  exit 2
fi
NAME=$1
OUT=$2
BIN=${CARGO_TARGET_DIR:?CARGO_TARGET_DIR must be set}/release/e4-campaign
if [ ! -x "$BIN" ]; then
  echo "x5_probe: $BIN not built (cargo build --release -p e4-recovery --bin e4-campaign)" >&2
  exit 2
fi
mkdir -p "$OUT" || exit 2
LOG="$OUT/$NAME.log"
: >"$LOG"

loadavg() { read -r a b c _ < /proc/loadavg && echo "$a $b $c"; }

# One timed block: stdout to <base>.out, the bash `time` report
# (TIMEFORMAT '%U %S %R' = user sys real, seconds) to <base>.time.
# The redirections live inside the timed subshell and there is no
# `exec`: bash 5.2 prints no report for `time ( exec ... )` (measured;
# the child's user+sys is counted — verified against a CPU burner).
block() {
  local base=$1
  shift
  {
    TIMEFORMAT='%U %S %R'
    time (ulimit -v 25165824; "$BIN" "$@" >"$base.out" 2>"$base.err")
  } 2>"$base.time"
  local rc=$?
  echo "block $base rc=$rc load=$(loadavg) time=$(cat "$base.time")" >>"$LOG"
  return $rc
}

LOAD_AT_START=$(loadavg)
echo "load_at_start=$LOAD_AT_START" >>"$LOG"
RC=0
if [ "$MP" -eq 1 ]; then
  block "$OUT/$NAME.mp" run --probe mp-pressure --threads 16 --out "$OUT/$NAME.mp.json" || RC=1
  BLOBS=("$OUT/$NAME.mp")
else
  for b in 0 200 400 600 800; do
    block "$OUT/$NAME.$b" run --seeds "$b..$((b + 200))" --threads 16 \
      --out "$OUT/$NAME.$b.json" || RC=1
  done
  BLOBS=("$OUT/$NAME.0" "$OUT/$NAME.200" "$OUT/$NAME.400" "$OUT/$NAME.600" "$OUT/$NAME.800")
fi
if [ "$RC" -ne 0 ]; then
  echo "x5_probe: a block failed (see $LOG)" >&2
  exit 1
fi

# PROBE_FAIL (and PROBE_HOOK), read from the JSON with python3 -I.
JSONS=()
for b in "${BLOBS[@]}"; do
  JSONS+=("$b.json")
done
python3 -I - "$NAME" "${JSONS[@]}" <<'PY'
import json
import sys

hooks = {}
for path in sys.argv[2:]:
    with open(path) as f:
        d = json.load(f)
    for s in d["seeds"]:
        for r in s["rounds"]:
            if r["fired"] and r["hook"]:
                hooks[r["hook"]] = hooks.get(r["hook"], 0) + 1
        # The failing rounds of the pass fold (FAILING_ROUNDS' shape):
        # the text is the round's o1, o2 or o3, cut to 80 characters.
        for i, r in enumerate(s["rounds"]):
            if not r["pass"]:
                t = r.get("o1") or r.get("o2") or r.get("o3") or "(pass=false, no oracle text)"
                t = " ".join(str(t).split())
                print(f"PROBE_FAIL {s['class']} {s['seed']} r{i} {t[:80]}")
for h in sorted(hooks):
    print(f"PROBE_HOOK {h} raw={hooks[h]} per10k={hooks[h] * 10}")
PY

# PROBE_ROW: sum every PROBE_QUOTA row the runs printed.
cat "${BLOBS[@]/%/.out}" | awk '/^PROBE_QUOTA / { sum[$2] += $3 }
  END { for (id in sum) printf "PROBE_ROW %s raw=%d per10k=%d\n", id, sum[id], sum[id] * 10 }' | sort

# PROBE_WALL and PROBE_CPU from the bash `time` reports.
RAW_S=$(awk '{ u += $1; s += $2; w += $3 }
  END { printf "%.1f %.1f", w, u + s }' "${BLOBS[@]/%/.time}")
WALL_S=${RAW_S%% *}
CPU_S=${RAW_S##* }
if [ "$MP" -eq 1 ]; then
  echo "PROBE_WALL raw_s=$WALL_S min_per_2000=$(awk -v s="$WALL_S" 'BEGIN { printf "%.1f", s / 6 }') load_at_start=$LOAD_AT_START"
else
  echo "PROBE_WALL raw_s=$WALL_S min_per_10k=$(awk -v s="$WALL_S" 'BEGIN { printf "%.1f", s / 6 }') load_at_start=$LOAD_AT_START"
fi
echo "PROBE_CPU user_sys_s=$CPU_S"
