#!/usr/bin/env bash
# Card B-0-F7-F1 Work 7: the two-run reproducibility protocol.
#
# J6, pre-registered verbatim (card B-0-F7-F1):
#   Two fresh-build runs, A then B, on a quiet host.
#   REPRODUCED iff, at EVERY ef_search point (the two runs must have
#   the same set of points):
#     - warm-median qps agree within 15%;
#     - warm-median p50_ms agree within 15%;
#     - warm-median recall_at_10 agree within 0.002 absolute.
#   "Within 15%" means |a - b| <= 0.15 x min(a, b). Boundaries are
#   inclusive, with a 1e-9 tolerance so float rounding cannot decide a
#   decimal boundary.
#   INVALID (checked first) if either run is digest.voided, lacks
#   provenance.nbench_build.profile == "release", or has any pass
#   without the index-state metrics.
#   Otherwise the verdict is NOT-REPRODUCED.
#   Run A is the row. Run B is evidence only. There is no third run.
#
# B-0-F7-F1b no-rerun rules (binding): if the verdict is
# NOT-REPRODUCED the row is refused and escalated to P; a refused run
# is recorded, never re-run until they agree; there is no third run.
#
# Usage: reproduce.sh a|b|verdict|check
# Environment: NB_HARNESS (this harness root) and NB_BIN (a RELEASE
# build — `nbench run` refuses a debug client for a measured row, and
# `a`/`b` stop with that message if you pass one).
#
# Exit codes: 0 ok; 1 unexpected failure; 2 usage; 3 "host not quiet"
# (no run happened — that is a retry-later situation, not a refusal);
# 4 protocol refusal (a run already exists; see the per-mode checks).
set -euo pipefail
. "${NB_HARNESS:?NB_HARNESS not set}/lib/nb-lib.sh"

BIN=${NB_BIN:?NB_BIN not set}
ROOT=${NB_HARNESS:?}
ROW=vector-sift1m-qdrant

# ---- quiet-host window (B-0-F7-F1b) ----
#
# 13 consecutive samples 10 s apart (120 s) in which load1 < 4, no
# process whose comm is cargo or rustc exists, and no live nbench-*
# user unit. Unlike remeasure.sh's quiet_wait (2 samples 60 s apart),
# this window cannot straddle a build finishing or an nbench unit
# winding down. Deadline 30 min, then exit 3: that is no run, not a
# refusal.
live_nbench_units() {
  systemctl --user list-units --plain --no-legend --no-pager 'nbench-*' \
    2>/dev/null | head -n 1
}

quiet_wait() {
  local deadline=$(( $(date +%s) + 1800 )) n=0 first="" a
  while [ "$(date +%s)" -lt "$deadline" ]; do
    a=$(cut -d' ' -f1 /proc/loadavg)
    if awk -v v="$a" 'BEGIN { exit !(v < 4) }' \
       && ! pgrep -x cargo >/dev/null 2>&1 \
       && ! pgrep -x rustc >/dev/null 2>&1 \
       && [ -z "$(live_nbench_units)" ]; then
      if [ "$n" -eq 0 ]; then first=$a; fi
      n=$((n + 1))
      if [ "$n" -ge 13 ]; then
        printf '%s %s\n' "$(date -u +%FT%TZ)" \
          "quiet-host window satisfied (load1 $first -> $a, 13 consecutive samples; no cargo/rustc; no nbench unit)" >&2
        return 0
      fi
    else
      n=0
    fi
    sleep 10
  done
  echo "host not quiet: no 120 s window with load1<4, no cargo/rustc and no live nbench unit within 30 min" >&2
  exit 3
}

# ---- repro evidence locations ----

# Every repro/ dir under results/*/.
all_repro_dirs() {
  local d
  for d in "$ROOT"/results/*/repro; do
    [ -d "$d" ] && printf '%s\n' "$d"
  done
}

# The one repro/ dir holding run A evidence (run-a.json, or a failed
# run-a.log). Prints nothing when run A has not started.
dir_of_run_a() {
  local d
  for d in $(all_repro_dirs); do
    if [ -f "$d/run-a.json" ] \
       || { [ -f "$d/run-a.log" ] && grep -q 'FAILED' "$d/run-a.log"; }; then
      printf '%s\n' "$d"
    fi
  done
}

refuse() { echo "reproduce.sh: $*" >&2; exit 4; }

# ---- one run (a or b) ----
#
# $1 = a|b. Shared steps: protocol refusals, the quiet window, the row
# itself, moving the results JSON and the index-state evidence into
# $REPRO/run-<x>/, and leaving the date's report consistent with what
# is still in results/<date>/ (a REPORT.md with no rows would be
# misleading, so it is removed instead when the dir holds no results
# JSON — regenerate_report would otherwise write a header-only file).
run_one() {
  local x=$1 log json
  log="run-$x.log"
  json="$ROW.run-$x.json"

  # Protocol refusals: this run must not already exist (a recorded
  # JSON, or a log containing FAILED — a refused run is never re-run).
  local d
  for d in $(all_repro_dirs); do
    [ -f "$d/$json" ] && refuse "$d/$json already exists (run $x was recorded; no re-runs)"
    if [ -f "$d/$log" ] && grep -q 'FAILED' "$d/$log"; then
      refuse "$d/$log records a FAILED run $x (recorded, never re-run; escalate)"
    fi
  done
  # A log without FAILED is a killed attempt: keep it as .killed.log
  # and continue — but only one such retry is allowed.
  for d in $(all_repro_dirs); do
    if [ -f "$d/$log" ] && ! grep -q 'FAILED' "$d/$log"; then
      [ -f "$d/run-$x.killed.log" ] && refuse "$d/run-$x.killed.log already exists (one killed retry only)"
      mv "$d/$log" "$d/run-$x.killed.log"
      echo "renamed killed attempt $d/$log -> run-$x.killed.log" >&2
    fi
  done

  # Where this run's evidence lives. Run A opens today's repro dir; run
  # B must land in the SAME dir as run A (refuse if run A has not
  # finished, or if more than one dir claims it). Note: a killed
  # attempt that began before a UTC midnight keeps its .killed.log in
  # the earlier date's repro dir; `check` will then (correctly) report
  # more than one repro dir — run the protocol inside one UTC day.
  local dirs dir
  if [ "$x" = a ]; then
    REPRO="$ROOT/results/$(date -u +%F)/repro"
  else
    dirs=$(dir_of_run_a)
    dir=$(printf '%s\n' "$dirs" | head -n 1)
    local count
    count=$(printf '%s\n' "$dirs" | grep -c . || true)
    [ "$count" -eq 0 ] && refuse "run A has not finished (no repro dir holds run-a evidence)"
    [ "$count" -gt 1 ] && refuse "more than one repro dir holds run-a evidence: $dirs"
    REPRO=$dir
  fi
  mkdir -p "$REPRO"

  quiet_wait
  export NBENCH_ROOT="$ROOT"
  export PATH="${ROOT}/bin:$PATH"

  "$BIN" run "$ROW" 2>&1 | tee "$REPRO/$log"
  # `nbench run` prints the results path on success; take the row's
  # JSON from that line (the date dir it names is authoritative — the
  # runner's own UTC date, which is also the date of the work dir).
  local results_json date evidence_work date_dir remaining
  results_json=$(grep '^results: ' "$REPRO/$log" | tail -n 1 | sed 's/^results: *//')
  [ -n "$results_json" ] || { echo "no 'results:' line in $REPRO/$log" >&2; exit 1; }
  mv "$results_json" "$REPRO/$json"

  # Index-state evidence (J4): every settle file, the init collection
  # info and the init log.
  date=$(basename "$(dirname "$results_json")")
  evidence_work="$ROOT/work/$date/$ROW"
  mkdir -p "$REPRO/run-$x"
  cp "$evidence_work"/index-state-*.json "$evidence_work/collection-info.json" \
     "$evidence_work/init-0.log" "$REPRO/run-$x/"

  # Leave the date's report consistent with what remains there. With
  # the row's JSON moved into repro/, a date dir may hold no results
  # JSON at all — regenerate_report would then write a header-only
  # REPORT.md, which is removed instead (the row's committed home is
  # decided by `verdict`, not by this run).
  date_dir=$(dirname "$results_json")
  remaining=$(find "$date_dir" -maxdepth 1 -name '*.json' -type f | wc -l)
  if [ "$remaining" -gt 0 ]; then
    "$BIN" report --date "$date"
  else
    rm -f "$date_dir/REPORT.md"
    echo "no results JSON left in $date_dir; removed its REPORT.md (the repro dir holds the evidence)" >&2
  fi

  echo "run $x recorded: $REPRO/$json" >&2
}

# ---- verdict ----
#
# Both JSONs present -> qdrant-repro-verdict into verdict.txt. A run
# that failed -> REPRO VERDICT=INVALID with its FAILED line. On
# REPRODUCED only, run A becomes the row (copied back into the date
# dir) and the report is regenerated. The script exits 0 whatever the
# word: the word carries the outcome (B-0-F7-F1b: a NOT-REPRODUCED row
# is refused by NOT copying it, and escalated to P by hand).
do_verdict() {
  local dirs dir count word
  dirs=$(dir_of_run_a)
  dir=$(printf '%s\n' "$dirs" | head -n 1)
  count=$(printf '%s\n' "$dirs" | grep -c . || true)
  [ "$count" -eq 0 ] && refuse "run A has not finished (nothing to verdict on)"
  [ "$count" -gt 1 ] && refuse "more than one repro dir holds run-a evidence: $dirs"

  if [ ! -f "$dir/$ROW.run-a.json" ]; then
    echo "REPRO VERDICT=INVALID (run A refused: $(grep 'FAILED' "$dir/run-a.log" | tail -n 1))" > "$dir/verdict.txt"
  elif [ ! -f "$dir/$ROW.run-b.json" ]; then
    if [ -f "$dir/run-b.log" ] && grep -q 'FAILED' "$dir/run-b.log"; then
      echo "REPRO VERDICT=INVALID (run B refused: $(grep 'FAILED' "$dir/run-b.log" | tail -n 1))" > "$dir/verdict.txt"
    else
      refuse "run B has not finished (no run-b.json and no FAILED run-b.log)"
    fi
  else
    "$BIN" qdrant-repro-verdict \
      --a "$dir/$ROW.run-a.json" --b "$dir/$ROW.run-b.json" \
      | tee "$dir/verdict.txt"
  fi

  word=$(tail -n 1 "$dir/verdict.txt" | sed 's/^REPRO VERDICT=//')
  local date_dir
  date_dir=$(dirname "$dir")
  if [ "$word" = REPRODUCED ]; then
    cp "$dir/$ROW.run-a.json" "$date_dir/$ROW.json"
    "$BIN" report --date "$(basename "$date_dir")"
    echo "REPRODUCED: $ROW.json recorded in $date_dir and the report regenerated" >&2
  else
    echo "verdict $word: the row is refused (no $ROW.json written); escalate to P per B-0-F7-F1b" >&2
  fi
  cat "$dir/verdict.txt"
}

# ---- check (outcome-neutral; the F1b Acceptance) ----
#
# Re-derives the verdict from the committed files and requires the
# evidence to be internally consistent, whatever the word:
#   - the re-derived last line equals verdict.txt's last line;
#   - REPRODUCED <=> the row file exists and is byte-identical to
#     run-a.json;
#   - exactly one repro/ dir exists.
do_check() {
  local dirs dir count word rederived date_dir row_file ok=0 total
  dirs=$(dir_of_run_a)
  dir=$(printf '%s\n' "$dirs" | head -n 1)
  count=$(printf '%s\n' "$dirs" | grep -c . || true)
  total=$(all_repro_dirs | grep -c . || true)

  if [ "$total" -ne 1 ]; then
    echo "check: exactly one repro/ dir must exist under results/*/, found $total:" >&2
    all_repro_dirs >&2
    return 1
  fi
  if [ "$count" -ne 1 ] || [ ! -f "$dir/verdict.txt" ]; then
    echo "check: no finished, verdicted repro evidence in $dir" >&2
    return 1
  fi
  word=$(tail -n 1 "$dir/verdict.txt" | sed 's/^REPRO VERDICT=//')

  # Re-derive: same rules `verdict` applied, from the files alone. The
  # FAILED-line greps are guarded (`|| true`) so a missing log yields
  # an empty reason and a mismatch report, not a silent set -e exit —
  # check's job is to state inconsistencies, whatever they are.
  if [ ! -f "$dir/$ROW.run-a.json" ]; then
    rederived="REPRO VERDICT=INVALID (run A refused: $(grep 'FAILED' "$dir/run-a.log" 2>/dev/null | tail -n 1 || true))"
  elif [ ! -f "$dir/$ROW.run-b.json" ]; then
    rederived="REPRO VERDICT=INVALID (run B refused: $(grep 'FAILED' "$dir/run-b.log" 2>/dev/null | tail -n 1 || true))"
  else
    rederived=$("$BIN" qdrant-repro-verdict \
      --a "$dir/$ROW.run-a.json" --b "$dir/$ROW.run-b.json" | tail -n 1)
  fi
  if [ "$rederived" != "$(tail -n 1 "$dir/verdict.txt")" ]; then
    echo "check: verdict.txt says '$(tail -n 1 "$dir/verdict.txt")' but the files re-derive '$rederived'" >&2
    ok=1
  fi

  # REPRODUCED <=> row file exists and is byte-identical to run-a.json.
  date_dir=$(dirname "$dir")
  row_file="$date_dir/$ROW.json"
  if [ "$word" = REPRODUCED ]; then
    if [ ! -f "$row_file" ]; then
      echo "check: verdict is REPRODUCED but $row_file is missing" >&2; ok=1
    elif ! cmp -s "$row_file" "$dir/$ROW.run-a.json"; then
      echo "check: $row_file is not byte-identical to run-a.json" >&2; ok=1
    fi
  else
    if [ -f "$row_file" ]; then
      echo "check: verdict is $word but $row_file exists" >&2; ok=1
    fi
  fi
  [ "$ok" -eq 0 ] && echo "check: consistent ($word)" >&2
  return "$ok"
}

case ${1:-} in
  a)       run_one a ;;
  b)       run_one b ;;
  verdict) do_verdict ;;
  check)   do_check ;;
  *)       echo "usage: reproduce.sh a|b|verdict|check" >&2; exit 2 ;;
esac
