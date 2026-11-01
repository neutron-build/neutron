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
# B-0-F7-F1b findings 1 and 2 (card B-0-F7-F1b-F2):
#   - a/b refuse (exit 4) when results/<UTC date>/$ROW.json is
#     git-tracked or already exists — a repro run would overwrite a
#     committed row. Checked before the quiet window (while nothing
#     has been created, R5) and again right after it: the wait can
#     last up to 30 min, long enough for the UTC date to roll over.
#   - the line the quiet window ended on (the satisfied line, or the
#     loud selftest BYPASSED line, both UTC-stamped) is appended to
#     repro/run-<x>.quiet.txt before the run starts: run-*.log is
#     gitignored (*.log), so the window's evidence must be committed
#     in its own tracked file.
#
# Run outcomes (gate round 1, findings R2/R3): `nbench run` prints a
# `results:` line whenever it RECORDED a results JSON — on success and
# on VOID (a digest mismatch writes the JSON, then exits 1). Such a
# run is final: its JSON moves into repro/ and it is never re-done,
# so J6's voided->INVALID arm can actually fire. A FAILED run (no
# results line) keeps its log, and its work-dir evidence — every
# settle transcript and pass log — is copied into repro/ before the
# script exits non-zero, so run B on the same UTC date can never
# overwrite the only copy.
#
# Run B is executed even when run A FAILED or was VOID (gate round 2,
# finding R2-1): B-0-F7-F1b Work 3 needs both runs as evidence for
# the P escalation, and `verdict` derives INVALID from run A first,
# so run B can never change the word — only complete the evidence.
#
# Usage: reproduce.sh a|b|verdict|check|selftest
# Environment: NB_HARNESS (this harness root) and NB_BIN (a RELEASE
# build — `a`/`b` probe NB_BIN's profile before starting anything and
# stop with nbench's own refusal message if it is debug, J5).
# `selftest` is the stub-driven, outcome-neutral protocol test from
# the gate round-1 rework acceptance: it needs no environment, no
# quiet host, no podman and no real row, and touches nothing outside
# its throwaway mktemp root.
#
# Exit codes: 0 ok; 1 unexpected failure — also a run recorded but
# VOID (final evidence; run `verdict`); 2 usage; 3 "host not quiet"
# (no run happened — that is a retry-later situation, not a refusal);
# 4 protocol refusal (a run already exists; the date's row file is
# committed or already exists; debug NB_BIN; see the per-mode checks).
set -euo pipefail

# `selftest` builds its own harness root and stub binary; every other
# mode needs the real environment.
if [ "${1:-}" != "selftest" ]; then
  . "${NB_HARNESS:?NB_HARNESS not set}/lib/nb-lib.sh"
  BIN=${NB_BIN:?NB_BIN not set}
  ROOT=${NB_HARNESS:?}
  # The binary may live in a CARGO_TARGET_DIR outside the harness
  # tree, where nbench's own exe-relative root discovery would resolve
  # to the wrong directory — every mode that invokes it (run, report,
  # qdrant-repro-verdict) needs the explicit root, not just run_one.
  export NBENCH_ROOT="$ROOT"
  export PATH="${ROOT}/bin:$PATH"
fi
ROW=vector-sift1m-qdrant

# ---- quiet-host window (B-0-F7-F1b) ----
#
# 13 consecutive samples 10 s apart (120 s) in which load1 < 4, no
# process whose comm is cargo or rustc exists, and no live nbench-*
# user unit. Unlike remeasure.sh's quiet_wait (2 samples 60 s apart),
# this window cannot straddle a build finishing or an nbench unit
# winding down. Deadline 30 min, then exit 3: that is no run, not a
# refusal.
#
# live_nbench_units lists ACTIVE units only: the default `list-units`
# also lists failed ones, and a stale failed nbench-* unit would keep
# the host "not quiet" forever without ever becoming load (gate round
# 1, finding R5). A failed unit is history, not activity.
live_nbench_units() {
  systemctl --user list-units --state=active,activating \
    --plain --no-legend --no-pager 'nbench-*' \
    2>/dev/null | head -n 1
}

# The line quiet_wait ended on — the satisfied line or the selftest
# BYPASSED line, both with their UTC timestamp. run_one appends it to
# repro/run-<x>.quiet.txt so the window's evidence is committed
# (B-0-F7-F1b finding 2: run-*.log is gitignored by *.log).
QUIET_LINE=""

quiet_wait() {
  # Selftest only (rework acceptance): the stub protocol test must not
  # sit out a real 120 s window. The bypass is loud on purpose — any
  # production log that contains it was not quiet-checked, visibly.
  # The line carries the same UTC prefix as the satisfied line, so the
  # recorded evidence is uniform (B-0-F7-F1b finding 2).
  if [ "${NB_REPRO_SELFTEST:-}" = "1" ]; then
    QUIET_LINE="$(date -u +%FT%TZ) quiet-host window BYPASSED (NB_REPRO_SELFTEST=1: stub-driven selftest, not a measurement)"
    printf '%s\n' "$QUIET_LINE" >&2
    return 0
  fi
  # NB_REPRO_SELFTEST=window (selftest only, card B-0-F7-F1b-F2 Work 3):
  # run the REAL window rule, but read load1 from
  # $NB_REPRO_SELFTEST_LOADAVG instead of /proc/loadavg while the
  # selftest puts stubbed sleep/pgrep/systemctl first on PATH. This
  # mode exists only so the selftest can prove the satisfied line is
  # recorded (S7b) without sitting out a real 120 s window; it is
  # unreachable without NB_REPRO_SELFTEST, and in every other mode the
  # window is byte-for-byte the production rule.
  local loadavg=/proc/loadavg
  if [ "${NB_REPRO_SELFTEST:-}" = "window" ]; then
    loadavg=${NB_REPRO_SELFTEST_LOADAVG:?NB_REPRO_SELFTEST_LOADAVG not set}
  fi
  local deadline=$(( $(date +%s) + 1800 )) n=0 first="" a
  while [ "$(date +%s)" -lt "$deadline" ]; do
    a=$(cut -d' ' -f1 "$loadavg")
    if awk -v v="$a" 'BEGIN { exit !(v < 4) }' \
       && ! pgrep -x cargo >/dev/null 2>&1 \
       && ! pgrep -x rustc >/dev/null 2>&1 \
       && [ -z "$(live_nbench_units)" ]; then
      if [ "$n" -eq 0 ]; then first=$a; fi
      n=$((n + 1))
      if [ "$n" -ge 13 ]; then
        QUIET_LINE="$(date -u +%FT%TZ) quiet-host window satisfied (load1 $first -> $a, 13 consecutive samples; no cargo/rustc; no nbench unit)"
        printf '%s\n' "$QUIET_LINE" >&2
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

# The one repro/ dir holding run A evidence ($ROW.run-a.json, or a
# failed run-a.log). Prints nothing when run A has not started.
# The JSON is named as run_one names it (gate round 1, finding R1:
# this used to grep for a run-a.json that was never written, so `b`,
# `verdict` and `check` could not find a successful run A).
dir_of_run_a() {
  local d
  for d in $(all_repro_dirs); do
    if [ -f "$d/$ROW.run-a.json" ] \
       || { [ -f "$d/run-a.log" ] && grep -q 'FAILED' "$d/run-a.log"; }; then
      printf '%s\n' "$d"
    fi
  done
}

refuse() { echo "reproduce.sh: $*" >&2; exit 4; }

# `a`/`b` must not discover a debug NB_BIN by burning the quiet window
# and the one killed-attempt retry (gate round 1, finding R5): a debug
# `nbench run` exits 1 with no FAILED line, which the protocol would
# class as a killed attempt. `nbench run` checks its build profile
# FIRST (card J5), before Params, harness discovery or podman, so
# invoking it with a row id that cannot exist is a side-effect-free
# probe: a debug build answers with the J5 refusal text, a release
# build with "unknown row". Callers must export NBENCH_ROOT first so
# the release path cannot fail on harness discovery instead.
probe_build_profile() {
  local out
  out=$("$BIN" run __reproduce-profile-probe__ 2>&1 || true)
  case $out in
    *"needs a release build"*)
      refuse "NB_BIN is a debug build — nbench refuses a measured row from it (${out#nbench: })" ;;
  esac
}

# A repro must never overwrite a committed row (verdict B-0-F7-F1b,
# finding 1): `nbench run` writes results/<UTC date>/$ROW.json, so a
# repro on a date that already holds a row would clobber that file in
# the working tree — exactly what run A of B-0-F7-F1b did to B-0-F7's
# committed row on 2026-10-31. Refuse (exit 4) when the path is
# git-tracked — ls-files reads the index, so this also catches a
# tracked row since deleted from the worktree — or when it merely
# exists, which is the only check when $ROOT is not in a git work
# tree. Called from run_one for both a and b, before anything is
# created (R5) and again right after the quiet window: that wait can
# last up to 30 min, long enough for the UTC date — and with it this
# path — to roll over mid-run.
refuse_if_row_committed() { # refuse_if_row_committed <UTC date>
  local date=$1
  local rel="results/$date/$ROW.json"
  if git -C "$ROOT" ls-files --error-unmatch -- "$rel" >/dev/null 2>&1; then
    refuse "$rel is committed (git-tracked); a repro run would overwrite it — run the protocol on a date with no row (verdict B-0-F7-F1b finding 1)"
  fi
  if [ -e "$ROOT/$rel" ]; then
    refuse "$rel already exists (not git-tracked); a repro run would overwrite it — run the protocol on a date with no row (verdict B-0-F7-F1b finding 1)"
  fi
}

# Copy this run's evidence (J4) into $REPRO/run-<x>/: every settle
# state file, the init collection info, and every log the work dir
# holds — init log, settle transcripts AND the per-pass logs, whose
# tail is what the runner's FAILED line falls back to when driver
# stderr is empty (gate round 2, finding R2-2) — plus the pass
# metrics and latency CSVs. Called on EVERY outcome (gate round 1,
# finding R3): on a failure this is the only chance — run B on the
# same UTC date overwrites the work dir. Globs are guarded so a file
# that a refusal-before-init never wrote is skipped, not an error.
#
# $3 is this run's marker, touched just before `nbench run` started
# (verdict B-0-F7-F1 finding 1): the dated work dir is shared by every
# run on that UTC date, so only a matched file strictly NEWER than the
# marker — one this run wrote or rewrote, getting a fresh mtime — is
# copied; an earlier run's leftovers are not. A missing marker is a
# script bug, not "no filter": refuse loudly instead of copying
# everything, which would re-file another run's evidence.
copy_evidence() {
  local x=$1 work=$2 marker=$3 f
  if [ ! -f "$marker" ]; then
    echo "reproduce.sh: run marker $marker is missing — refusing to copy evidence (script bug; copy_evidence must not run unfiltered)" >&2
    exit 1
  fi
  mkdir -p "$REPRO/run-$x"
  for f in "$work"/index-state-*.json \
           "$work"/collection-info.json \
           "$work"/*.log \
           "$work"/*.metrics.json \
           "$work"/*.latency.csv; do
    [ -f "$f" ] && [ "$f" -nt "$marker" ] && cp "$f" "$REPRO/run-$x/"
  done
  return 0
}

# ---- one run (a or b) ----
#
# $1 = a|b. Shared steps: protocol refusals, the build-profile probe,
# the quiet window, the row itself, moving the results JSON and the
# index-state evidence into $REPRO/run-<x>/, and leaving the date's
# report consistent with what is still in results/<date>/ (a REPORT.md
# with no rows would be misleading, so it is removed instead when the
# dir holds no results JSON — regenerate_report would otherwise write
# a header-only file).
run_one() {
  local x=$1 log json marker
  log="run-$x.log"
  json="$ROW.run-$x.json"

  # Protocol refusals: this run must not already exist (a recorded
  # JSON — success or VOID — or a log containing FAILED; a refused run
  # is never re-run).
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
  # finished, or if more than one dir claims it). A FAILED or VOID run
  # A does NOT refuse run B (gate round 2, finding R2-1): both runs
  # are evidence for the P escalation (B-0-F7-F1b Work 3), and
  # `verdict` derives INVALID from run A first. Note: a killed
  # attempt that began before a UTC midnight keeps its .killed.log in
  # the earlier date's repro dir; `check` will then (correctly) report
  # more than one repro dir — run the protocol inside one UTC day.
  local dirs dir count
  if [ "$x" = a ]; then
    REPRO="$ROOT/results/$(date -u +%F)/repro"
  else
    dirs=$(dir_of_run_a)
    dir=$(printf '%s\n' "$dirs" | head -n 1)
    count=$(printf '%s\n' "$dirs" | grep -c . || true)
    [ "$count" -eq 0 ] && refuse "run A has not finished (no repro dir holds run-a evidence)"
    [ "$count" -gt 1 ] && refuse "more than one repro dir holds run-a evidence: $dirs"
    REPRO=$dir
  fi

  # The committed-row guard (B-0-F7-F1b finding 1), while nothing has
  # been created yet (R5): after the protocol refusals above, before
  # the quiet window burns its 30 min and before NB_BIN is probed.
  refuse_if_row_committed "$(date -u +%F)"

  # Fail fast on a debug client (R5): before the quiet window and
  # before any directory exists, so a wrong NB_BIN costs nothing.
  probe_build_profile

  quiet_wait
  # The committed-row guard again (B-0-F7-F1b finding 1): the wait
  # above can outlast a UTC midnight, and this run is about to make
  # nbench write results/<the new UTC date>/$ROW.json.
  refuse_if_row_committed "$(date -u +%F)"
  # Only now does today's repro dir come into being (R5): an exit 3
  # above must not leave an empty results/<date>/repro behind, which
  # would break `check`'s "exactly one repro dir" for a later run.
  mkdir -p "$REPRO"
  # The quiet-window evidence must be committed (B-0-F7-F1b finding
  # 2): run-*.log is gitignored by *.log, so the line the window ended
  # on — satisfied, or the loud selftest BYPASSED line — is appended
  # (never overwritten: >>) to a tracked file before the run starts.
  # A killed-attempt retry adds a second line instead of hiding the
  # first. An exit 3 above never reaches this: it creates nothing.
  printf '%s\n' "$QUIET_LINE" >> "$REPRO/run-$x.quiet.txt"

  # The per-run evidence marker (verdict B-0-F7-F1 finding 1): the
  # work dir work/<UTC date>/$ROW is shared by EVERY run on that date,
  # and copy_evidence must not file an earlier run's leftovers as this
  # run's evidence. Touched just before the run starts; copy_evidence
  # copies only files strictly newer than it (a file this run rewrites
  # gets a fresh mtime, so it is still copied). It lives under the
  # gitignored work/ runtime state, never under results/ or repro/
  # (those are committed). Overwriting it on a killed-attempt retry is
  # correct: the retry is a new run.
  mkdir -p "$ROOT/work"
  marker="$ROOT/work/.reproduce-run-$x.marker"
  touch "$marker"

  # The run itself. `nbench run` exits 1 both when a row FAILED and
  # when it was recorded-but-VOID; the two are told apart below by
  # what the run actually printed, so set -e must not abort before the
  # evidence is safe (R2/R3).
  local rc=0
  "$BIN" run "$ROW" 2>&1 | tee "$REPRO/$log" || rc=$?
  # `nbench run` prints the results path whenever it recorded a JSON;
  # the date dir it names is authoritative — the runner's own UTC
  # date, which is also the date of the work dir. The `|| true` is
  # load-bearing: a FAILED run prints no such line, and with
  # `set -e -o pipefail` the no-match grep would abort the script
  # right here — before the evidence copy below. That abort is how
  # run B overwrote a failed run A's only index-state evidence (gate
  # round 1, finding R3).
  local results_json date evidence_work date_dir remaining
  results_json=$(grep '^results: ' "$REPRO/$log" | tail -n 1 | sed 's/^results: *//' || true)

  if [ "$rc" -ne 0 ] && [ -z "$results_json" ]; then
    # A FAILED run: no results JSON exists. Preserve the evidence (the
    # work dir is dated today; run B would overwrite it) and keep the
    # date's report consistent before surfacing the failure.
    date=$(date -u +%F)
    copy_evidence "$x" "$ROOT/work/$date/$ROW" "$marker"
    date_dir="$ROOT/results/$date"
    if [ -d "$date_dir" ]; then
      remaining=$(find "$date_dir" -maxdepth 1 -name '*.json' -type f | wc -l)
      if [ "$remaining" -gt 0 ]; then
        "$BIN" report --date "$date"
      else
        rm -f "$date_dir/REPORT.md"
      fi
    fi
    echo "run $x FAILED (exit $rc): log $REPRO/$log, evidence in $REPRO/run-$x/ — run 'verdict'" >&2
    exit 1
  fi

  [ -n "$results_json" ] || { echo "no 'results:' line in $REPRO/$log" >&2; exit 1; }

  # A recorded run (success or VOID): the JSON is final evidence and
  # moves into repro/ now — a VOID run is never re-runnable, and J6's
  # voided->INVALID arm needs the JSON here (R2).
  mv "$results_json" "$REPRO/$json"
  date=$(basename "$(dirname "$results_json")")
  evidence_work="$ROOT/work/$date/$ROW"
  copy_evidence "$x" "$evidence_work" "$marker"

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

  if [ "$rc" -ne 0 ]; then
    if grep -q 'VOID (digest mismatch)' "$REPRO/$log"; then
      echo "run $x recorded but VOID (digest mismatch): $REPRO/$json is final, J6 says INVALID — run 'verdict'" >&2
    else
      echo "run $x recorded a results JSON but exited $rc: $REPRO/$json is final — run 'verdict'" >&2
    fi
    exit 1
  fi
  echo "run $x recorded: $REPRO/$json" >&2
}

# ---- verdict ----
#
# The verdict implied by the recorded evidence alone, derived by ONE
# function that both `verdict` (writes verdict.txt through it) and
# `check` (re-derives through it) call — the two can never diverge
# (that was the shape of gate round 1, finding R1). Branches: a run
# that FAILED (no JSON, FAILED log), a run that was recorded but
# VOID (J6 INVALID — the arm that R2's fix makes reachable), or two
# recorded runs compared by the real rule.
# The VOID line of the log (nbench's own words) is quoted when
# present; "digest.voided" names the JSON field otherwise.
void_reason() {
  local dir=$1 x=$2 line
  line=$(grep 'VOID (digest mismatch)' "$dir/run-$x.log" 2>/dev/null | tail -n 1 || true)
  printf '%s' "${line:-digest.voided}"
}

derive_verdict_output() {
  local dir=$1
  if [ ! -f "$dir/$ROW.run-a.json" ]; then
    printf 'REPRO VERDICT=INVALID (run A refused: %s)\n' \
      "$(grep 'FAILED' "$dir/run-a.log" 2>/dev/null | tail -n 1 || true)"
  elif grep -q '"voided":true' "$dir/$ROW.run-a.json"; then
    printf 'REPRO VERDICT=INVALID (run A voided: %s)\n' "$(void_reason "$dir" a)"
  elif [ ! -f "$dir/$ROW.run-b.json" ]; then
    printf 'REPRO VERDICT=INVALID (run B refused: %s)\n' \
      "$(grep 'FAILED' "$dir/run-b.log" 2>/dev/null | tail -n 1 || true)"
  elif grep -q '"voided":true' "$dir/$ROW.run-b.json"; then
    printf 'REPRO VERDICT=INVALID (run B voided: %s)\n' "$(void_reason "$dir" b)"
  else
    "$BIN" qdrant-repro-verdict \
      --a "$dir/$ROW.run-a.json" --b "$dir/$ROW.run-b.json"
  fi
}

# Both JSONs present -> qdrant-repro-verdict into verdict.txt. A run
# that failed or was voided -> REPRO VERDICT=INVALID with its FAILED
# or VOID line. On REPRODUCED only, run A becomes the row (copied back
# into the date dir) and the report is regenerated. The script exits 0
# whatever the word: the word carries the outcome (B-0-F7-F1b: a
# NOT-REPRODUCED row is refused by NOT copying it, and escalated to P
# by hand).
do_verdict() {
  local dirs dir count word date_dir
  dirs=$(dir_of_run_a)
  dir=$(printf '%s\n' "$dirs" | head -n 1)
  count=$(printf '%s\n' "$dirs" | grep -c . || true)
  [ "$count" -eq 0 ] && refuse "run A has not finished (nothing to verdict on)"
  [ "$count" -gt 1 ] && refuse "more than one repro dir holds run-a evidence: $dirs"

  # A run B that FAILED, or was never needed because run A already
  # decided INVALID (failed or voided), is recordable; a run B that
  # simply has not happened yet is not — refuse until it has. The
  # branch order mirrors derive_verdict_output's, so the refusal can
  # only ever fire where the derivation would need run B.
  if [ ! -f "$dir/$ROW.run-a.json" ]; then
    : # run A FAILED — INVALID is recordable from its log alone
  elif grep -q '"voided":true' "$dir/$ROW.run-a.json"; then
    : # run A is VOID — INVALID is recordable from its JSON alone
  elif [ ! -f "$dir/$ROW.run-b.json" ] \
     && ! { [ -f "$dir/run-b.log" ] && grep -q 'FAILED' "$dir/run-b.log"; }; then
    refuse "run B has not finished (no run-b.json and no FAILED run-b.log)"
  fi

  derive_verdict_output "$dir" | tee "$dir/verdict.txt"

  word=$(tail -n 1 "$dir/verdict.txt" | sed 's/^REPRO VERDICT=//')
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

  # Same derivation `verdict` used, from the files alone.
  rederived=$(derive_verdict_output "$dir" | tail -n 1)
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

# ---- selftest (gate round 1 rework acceptance) ----
#
# A stub-driven test of THIS script's protocol: a fake NB_BIN mimics
# `nbench run`'s recorded outcomes (success, VOID, FAILED, the J5
# debug refusal), the quiet wait is bypassed via NB_REPRO_SELFTEST=1
# (loudly), and each scenario runs against its own throwaway root
# under mktemp. Covers the rework acceptance: success a -> b ->
# verdict -> check (finding R1's regression), a FAILED run A (R3's
# evidence copy), a VOID run A (R2's finality), and a debug NB_BIN
# (R5's probe). Outcome-neutral: no timing, no podman, no real row,
# nothing outside the temp root is read or written. The scratch dir is
# left in place for inspection (its path is printed); the only rm in
# this script's test path is S6b's, of a single file inside its own
# throwaway root.
#
# Assert helpers: st_rc <name> <want-rc> <root> <stub-mode> <args...>
# runs this script and asserts its exit code; st_ok <name> <cmd...>
# asserts a shell condition.
selftest() {
  local script lib t r rc fails=0
  script=$(readlink -f "$0")
  lib=$(dirname "$script")/../../lib/nb-lib.sh
  t=$(mktemp -d "${TMPDIR:-/tmp}/nbench-repro-selftest.XXXXXXXX")
  echo "selftest scratch: $t (left in place for inspection)"

  # The stub nbench. Its `run` reproduces the three outcomes of the
  # real one: a recorded success (results: + exit 0), a recorded VOID
  # (results: + "VOID (digest mismatch)" + exit 1), and a FAILED row
  # (a FAILED line, no results:, exit 1). NB_STUB_MODE picks one; the
  # profile probe answers with the J5 text when NB_STUB_DEBUG=1.
  cat > "$t/nbench-stub" <<'STUB'
#!/usr/bin/env bash
set -u
row=vector-sift1m-qdrant
case "$1" in
  run)
    if [ "$2" = "__reproduce-profile-probe__" ]; then
      if [ "${NB_STUB_DEBUG:-}" = "1" ]; then
        echo "nbench: usage: nbench run of a measured row needs a release build (cargo build --release -p nbench); debug clients add harness time inside measured latencies - verdict B-0-F7 finding 3" >&2
      else
        echo "nbench: unknown row __reproduce-profile-probe__" >&2
      fi
      exit 1
    fi
    # The stub must behave like the real `nbench run` on one point: its
    # work-dir writes must land strictly LATER than the per-run marker
    # reproduce.sh touches just before starting it. Kernel inode
    # timestamps come from the coarse clock — writes inside the same
    # ~few-ms tick compare EQUAL under -nt — and this stub starts
    # writing within a tick of its parent, unlike the real run (which
    # takes seconds before its first work-dir file). Sleep past a few
    # ticks so the marker filter is decided by run order, never by
    # same-tick luck.
    sleep 0.2
    date=$(date -u +%F)
    work="$NBENCH_ROOT/work/$date/$row"
    case "${NB_STUB_MODE:-success}" in
      success|voided)
        mkdir -p "$NBENCH_ROOT/results/$date" "$work"
        out="$NBENCH_ROOT/results/$date/$row.json"
        if [ "${NB_STUB_MODE}" = voided ]; then
          printf '{"row":"%s","digest":{"voided":true}}\n' "$row" > "$out"
        else
          printf '{"row":"%s","digest":{"voided":false}}\n' "$row" > "$out"
        fi
        : > "$work/index-state-init.json"
        : > "$work/collection-info.json"
        : > "$work/init-0.log"
        : > "$work/ef_search=16-cold-0.settle.log"
        echo "results: $out"
        if [ "${NB_STUB_MODE}" = voided ]; then
          echo "nbench: command failed (nbench run, exit 1): $row: VOID (digest mismatch)" >&2
          exit 1
        fi
        exit 0
        ;;
      failed|failed-backdated)
        # `failed-backdated` (B-0-F7-F1-F2-F1 Work 2, used only by S5's
        # run b) reproduces `failed`'s observable outcome exactly — the
        # same writes, the same FAILED line, exit 1 — and adds ONE extra
        # pass log no existing assertion names, written after the 0.2 s
        # sleep and then mtime-backdated: its mtime says an hour before
        # this run, so the marker's -nt (mtime) filter excludes it,
        # while its ctime is the backdating touch itself — inside this
        # run, >= 0.2 s after the marker — so a full-granularity ctime
        # filter would copy it. Pins mtime semantics (verdict
        # B-0-F7-F1-F2 finding 3).
        mkdir -p "$work"
        : > "$work/index-state-init.json"
        : > "$work/ef_search=16-cold-0.settle.log"
        : > "$work/ef_search=16-cold-0.log"
        : > "$work/ef_search=16-cold-0.metrics.json"
        : > "$work/ef_search=16-cold-0.latency.csv"
        if [ "${NB_STUB_MODE}" = failed-backdated ]; then
          : > "$work/ef_search=16-cold-1.log"
          touch -d '-1 hour' "$work/ef_search=16-cold-1.log"
        fi
        echo "row $row FAILED: command failed (driver [ef_search=16-cold-0], exit 1): index state differs: init {segments:8,points:1000000,indexed:1000000,threshold_kb:1} vs now {segments:92,points:1000000,indexed:3861632,threshold_kb:1}" >&2
        exit 1
        ;;
    esac
    ;;
  report)
    # nbench report --date D
    mkdir -p "$NBENCH_ROOT/results/$3"
    echo "# stub report" > "$NBENCH_ROOT/results/$3/REPORT.md"
    ;;
  qdrant-repro-verdict)
    # The real rule is unit-tested in Rust (repro_verdict_*); the stub
    # fixes the word so the SCRIPT's plumbing is what is under test.
    echo "stub table: both runs recorded"
    echo "REPRO VERDICT=REPRODUCED"
    ;;
  *)
    echo "stub nbench: unhandled: $*" >&2
    exit 2
    ;;
esac
STUB
  chmod +x "$t/nbench-stub"

  # Run this script as the protocol would, under root $1 / stub mode
  # $2; the transcript lands in $t/last.out and the exit code is the
  # script's (the callers guard it with `|| got=$?`).
  st_run() {
    local r=$1 m=$2; shift 2
    NB_HARNESS="$r" NB_BIN="$t/nbench-stub" NB_REPRO_SELFTEST=1 \
      NB_STUB_MODE="$m" NB_STUB_DEBUG="${NB_STUB_DEBUG:-0}" \
      bash "$script" "$@" > "$t/last.out" 2>&1
  }
  # Like st_run, but for S7b only: quiet_wait's REAL loop in window
  # mode (card B-0-F7-F1b-F2 Work 3) — load1 comes from $win_loadavg,
  # and the Work 3 PATH stubs (sleep instant, pgrep matchless,
  # systemctl silent) sit first on PATH, so 13 samples pass in
  # milliseconds and the satisfied line is the real one.
  st_run_win() {
    local r=$1 m=$2; shift 2
    NB_HARNESS="$r" NB_BIN="$t/nbench-stub" NB_REPRO_SELFTEST=window \
      NB_REPRO_SELFTEST_LOADAVG="$win_loadavg" NB_STUB_MODE="$m" \
      NB_STUB_DEBUG=0 PATH="$win_bin:$PATH" \
      bash "$script" "$@" > "$t/last.out" 2>&1
  }
  st_rc() { # st_rc <name> <want-rc> <st_run args...>
    local name=$1 want=$2 got
    shift 2
    got=0; "$@" || got=$?
    if [ "$got" -eq "$want" ]; then
      echo "  ok   $name (rc $got)"
    else
      echo "  FAIL $name: expected rc $want, got $got" >&2
      sed 's/^/       | /' "$t/last.out" >&2
      fails=$((fails + 1))
    fi
  }
  st_ok() { # st_ok <name> <condition cmd...>
    local name=$1; shift
    if "$@"; then
      echo "  ok   $name"
    else
      echo "  FAIL $name: $*" >&2
      fails=$((fails + 1))
    fi
  }

  D=$(date -u +%F)

  echo "S1: success a -> b -> verdict -> check (R1: b/verdict/check must find run A)"
  r=$t/s1; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  st_rc "a succeeds" 0 st_run "$r" success a
  st_ok "run-a.json recorded" test -f "$r/results/$D/repro/$ROW.run-a.json"
  st_ok "run-a evidence copied" test -f "$r/results/$D/repro/run-a/index-state-init.json"
  st_ok "settle transcript preserved" test -f "$r/results/$D/repro/run-a/ef_search=16-cold-0.settle.log"
  st_rc "b succeeds (R1)" 0 st_run "$r" success b
  st_ok "run-b.json in the same repro dir (R1)" test -f "$r/results/$D/repro/$ROW.run-b.json"
  st_ok "run-b evidence copied" test -f "$r/results/$D/repro/run-b/collection-info.json"
  st_rc "verdict succeeds" 0 st_run "$r" success verdict
  st_ok "verdict is REPRODUCED" test "$(tail -n 1 "$r/results/$D/repro/verdict.txt")" = "REPRO VERDICT=REPRODUCED"
  st_ok "row copied back on REPRODUCED" cmp -s "$r/results/$D/$ROW.json" "$r/results/$D/repro/$ROW.run-a.json"
  st_rc "check consistent" 0 st_run "$r" success check
  st_ok "exactly one repro dir" test "$(find "$r" -name repro -type d | wc -l)" -eq 1

  echo "S2: FAILED run A (R3: evidence survives; R2-1: run B still runs; the FAILED line reaches the verdict)"
  r=$t/s2; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  st_rc "a fails loudly" 1 st_run "$r" failed a
  st_ok "no run-a.json (nothing was recorded)" test ! -f "$r/results/$D/repro/$ROW.run-a.json"
  st_ok "FAILED evidence copied (R3)" test -f "$r/results/$D/repro/run-a/ef_search=16-cold-0.settle.log"
  st_ok "FAILED pass log copied (R2-2)" test -f "$r/results/$D/repro/run-a/ef_search=16-cold-0.log"
  st_ok "FAILED pass metrics copied (R2-2)" test -f "$r/results/$D/repro/run-a/ef_search=16-cold-0.metrics.json"
  st_ok "FAILED log kept" grep -q FAILED "$r/results/$D/repro/run-a.log"
  st_rc "a again is refused (no re-runs)" 4 st_run "$r" failed a
  st_rc "b runs after a FAILED A (R2-1: evidence, not a refusal)" 0 st_run "$r" success b
  st_ok "run-b.json recorded in the same repro dir" test -f "$r/results/$D/repro/$ROW.run-b.json"
  st_rc "verdict records INVALID" 0 st_run "$r" failed verdict
  st_ok "verdict still names run A's FAILED line" grep -q "INVALID (run A refused: row $ROW FAILED" "$r/results/$D/repro/verdict.txt"
  st_rc "check consistent" 0 st_run "$r" failed check

  echo "S3: VOID run A (R2: recorded and final; R2-1: run B still runs; J6's voided arm fires)"
  r=$t/s3; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  st_rc "a exits 1 (recorded but VOID)" 1 st_run "$r" voided a
  st_ok "voided JSON moved into repro (R2)" grep -q '"voided":true' "$r/results/$D/repro/$ROW.run-a.json"
  st_ok "date dir no longer holds it" test ! -f "$r/results/$D/$ROW.json"
  st_ok "voided run evidence copied" test -f "$r/results/$D/repro/run-a/index-state-init.json"
  st_rc "a again is refused (R2)" 4 st_run "$r" voided a
  st_ok "no killed-rename happened" test ! -f "$r/results/$D/repro/run-a.killed.log"
  st_rc "b runs after a VOID A (R2-1: evidence, not a refusal)" 0 st_run "$r" success b
  st_ok "run-b.json recorded in the same repro dir" test -f "$r/results/$D/repro/$ROW.run-b.json"
  st_rc "verdict records INVALID (voided)" 0 st_run "$r" voided verdict
  st_ok "verdict still names run A (the VOID run)" grep -q "INVALID (run A voided:" "$r/results/$D/repro/verdict.txt"
  st_rc "check consistent" 0 st_run "$r" voided check

  echo "S4: debug NB_BIN (R5: refused before anything starts)"
  r=$t/s4; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  NB_STUB_DEBUG=1
  st_rc "a refuses a debug client" 4 st_run "$r" success a
  NB_STUB_DEBUG=0
  st_ok "refusal quotes the J5 rule" grep -q "needs a release build" "$t/last.out"
  st_ok "nothing was created" test ! -e "$r/results"

  echo "S5: a stale work-dir file is not copied into run-<x>/ (B-0-F7-F1 finding 1: the dated work dir is shared by every run)"
  r=$t/s5; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  st_rc "a succeeds" 0 st_run "$r" success a
  # Plant a pass log run A's ef set never wrote, then backdate it and
  # everything else the work dir already holds by an hour: a
  # same-millisecond mtime must not decide this test. Run B
  # (failed-backdated mode: `failed`'s outcome plus one mtime-backdated
  # extra write) rewrites only the ef16 files and index-state-init.json;
  # the marker it touched before its run separates the two ages.
  printf 'stale-from-run-a\n' > "$r/work/$D/$ROW/ef_search=64-run-0.log"
  find "$r/work/$D/$ROW" -type f -exec touch -d '-1 hour' {} +
  # B-0-F7-F1-F2-F1 Work 1: a FRESH leftover, planted after run A fully
  # finished (run A's stub slept 0.2 s past marker a, so this write is
  # >= 20 coarse-clock ticks newer than marker a) and never backdated.
  # The per-run marker b excludes it — it is written before run b
  # starts, so it can never be newer than marker b (a same-tick landing
  # compares not-newer); a shared or not-refreshed marker, still at
  # marker a's time, copies it. Decided by run order, never tick luck
  # (verdict B-0-F7-F1-F2 finding 2).
  printf 'fresh-between-runs' > "$r/work/$D/$ROW/ef_search=32-run-0.log"
  st_rc "b fails at ef16" 1 st_run "$r" failed-backdated b
  st_ok "stale ef64 pass log NOT copied into run-b (finding 1)" test ! -e "$r/results/$D/repro/run-b/ef_search=64-run-0.log"
  st_ok "run A's collection-info NOT copied (the failed stub does not rewrite it)" test ! -e "$r/results/$D/repro/run-b/collection-info.json"
  st_ok "run A's init log NOT copied" test ! -e "$r/results/$D/repro/run-b/init-0.log"
  st_ok "run B's ef16 pass log copied (rewritten by B)" test -f "$r/results/$D/repro/run-b/ef_search=16-cold-0.log"
  st_ok "run B's ef16 settle log copied" test -f "$r/results/$D/repro/run-b/ef_search=16-cold-0.settle.log"
  st_ok "run B's init state file copied" test -f "$r/results/$D/repro/run-b/index-state-init.json"
  st_ok "run A's evidence untouched (no regression for run A)" test -f "$r/results/$D/repro/run-a/collection-info.json" -a -f "$r/results/$D/repro/run-a/init-0.log"
  # Work 1 (B-0-F7-F1-F2-F1): the fresh leftover stayed in the work dir
  # and never reached run-b/ — only a marker refreshed per run excludes
  # a file newer than run A's own marker (verdict finding 2).
  st_ok "fresh between-runs leftover exists in the work dir" test -f "$r/work/$D/$ROW/ef_search=32-run-0.log"
  st_ok "fresh leftover NOT copied into run-b (marker refreshed per run)" test ! -e "$r/results/$D/repro/run-b/ef_search=32-run-0.log"
  # Work 2: the stub's mtime-backdated write stayed in the work dir and
  # never reached run-b/ — an mtime (-nt) filter excludes it, a
  # full-granularity ctime filter would include it (verdict finding 3).
  st_ok "mtime-backdated stub write exists in the work dir" test -f "$r/work/$D/$ROW/ef_search=16-cold-1.log"
  st_ok "mtime-backdated stub write NOT copied into run-b (-nt is mtime, not ctime)" test ! -e "$r/results/$D/repro/run-b/ef_search=16-cold-1.log"
  # Work 3: the markers live directly under the gitignored work/ root
  # (bench/nbench/.gitignore ignores work/), never under the committed
  # results/ tree (verdict finding 4).
  st_ok "markers live under the work/ root" test -f "$r/work/.reproduce-run-a.marker" -a -f "$r/work/.reproduce-run-b.marker"
  st_ok "no marker under the committed results/ tree" test -z "$(find "$r/results" -name '*.marker')"

  echo "S6: a/b refuse to overwrite a committed row (B-0-F7-F1b finding 1)"
  # Every case gets its own root; the git cases are made repos here,
  # and git init/add/commit run ONLY inside these throwaway roots,
  # never against the real checkout. s6_commit_row plants a fake row
  # at results/<date>/ and commits it with a selftest identity (the
  # card's flags: gpgsign off, no global user needed).
  s6_commit_row() { # s6_commit_row <root> <date>
    local r=$1 d=$2
    mkdir -p "$r/results/$d"
    printf '{"row":"%s","s6":"committed fake row"}\n' "$ROW" > "$r/results/$d/$ROW.json"
    git -C "$r" add "results/$d/$ROW.json"
    git -C "$r" -c user.name=selftest -c user.email=selftest@invalid \
      -c commit.gpgsign=false commit -q -m "selftest fake row"
  }
  # S6a: a committed row at today's path — refuse before anything.
  r=$t/s6a; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  git -C "$r" init -q
  s6_commit_row "$r" "$D"
  st_rc "a refuses when today's row is committed" 4 st_run "$r" success a
  st_ok "the committed row is byte-identical in the worktree" git -C "$r" diff --quiet
  st_ok "no repro dir was created (the guard fires before anything)" test ! -e "$r/results/$D/repro"
  st_ok "the refusal says committed" grep -q "is committed (git-tracked)" "$t/last.out"
  # S6b: tracked, but deleted from the worktree — ls-files reads the
  # index, so the row is still committed as far as git is concerned.
  r=$t/s6b; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  git -C "$r" init -q
  s6_commit_row "$r" "$D"
  rm -f "$r/results/$D/$ROW.json"
  st_rc "a still refuses a tracked row deleted from the worktree" 4 st_run "$r" success a
  st_ok "the refusal still says committed (the index, not the disk, decides)" grep -q "is committed (git-tracked)" "$t/last.out"
  # S6c: no git at all, an untracked row exists — the -e arm alone.
  r=$t/s6c; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  mkdir -p "$r/results/$D"
  printf '{"row":"%s","s6":"untracked row"}\n' "$ROW" > "$r/results/$D/$ROW.json"
  cp "$r/results/$D/$ROW.json" "$t/s6c.row.json"
  st_rc "a refuses an existing untracked row (non-git root)" 4 st_run "$r" success a
  st_ok "the untracked row is unchanged" cmp -s "$r/results/$D/$ROW.json" "$t/s6c.row.json"
  st_ok "the refusal names an existing, untracked row" grep -q "already exists (not git-tracked)" "$t/last.out"
  # S6d: run A finished clean, then someone commits a row at today's
  # path — run B must refuse and leave run A's evidence alone.
  r=$t/s6d; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  git -C "$r" init -q
  st_rc "a succeeds (no row at today's path yet)" 0 st_run "$r" success a
  s6_commit_row "$r" "$D"
  # `|| true` on the copies: under a mutant that stops writing the
  # quiet record they would not exist, and the runner must survive its
  # own setup (set -e) to report the cmp FAILs below, not die on them.
  cp "$r/results/$D/repro/$ROW.run-a.json" "$t/s6d.run-a.json" || true
  cp "$r/results/$D/repro/run-a.quiet.txt" "$t/s6d.run-a.quiet.txt" || true
  st_rc "b refuses: today's row path became committed after run A" 4 st_run "$r" success b
  st_ok "run A's JSON is untouched" cmp -s "$r/results/$D/repro/$ROW.run-a.json" "$t/s6d.run-a.json"
  st_ok "run A's quiet record is untouched" cmp -s "$r/results/$D/repro/run-a.quiet.txt" "$t/s6d.run-a.quiet.txt"
  # S6e (no regression): a git root with no row at the path just runs.
  # S1 already covers the non-git root.
  r=$t/s6e; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  git -C "$r" init -q
  st_rc "a succeeds in a git root with no row" 0 st_run "$r" success a
  # The quiet record must be committable evidence: with a copy of the
  # real harness .gitignore in place, git must NOT ignore it (the
  # repo-root *.log rule hides run-*.log; a .txt under results/ stays
  # trackable — that asymmetry is the point of the file).
  cp "$(dirname "$script")/../../.gitignore" "$r/.gitignore"
  st_ok "the quiet record is git-trackable (the real .gitignore does not ignore it)" \
    test -z "$(git -C "$r" check-ignore -v "results/$D/repro/run-a.quiet.txt" || true)"

  echo "S7: the line the quiet window ended on is recorded in repro/run-<x>.quiet.txt (B-0-F7-F1b finding 2)"
  # S7a: bypass mode — each run's quiet file holds exactly the one
  # UTC-stamped BYPASSED line.
  r=$t/s7a; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  st_rc "a succeeds (bypassed window)" 0 st_run "$r" success a
  st_rc "b succeeds (bypassed window)" 0 st_run "$r" success b
  st_ok "run-a.quiet.txt holds exactly one line" test "$(wc -l < "$r/results/$D/repro/run-a.quiet.txt")" -eq 1
  st_ok "run-a's quiet line is the UTC-stamped BYPASSED line" \
    grep -Eq '^[0-9-]+T[0-9:]+Z quiet-host window BYPASSED' "$r/results/$D/repro/run-a.quiet.txt"
  st_ok "run-b.quiet.txt holds exactly one line" test "$(wc -l < "$r/results/$D/repro/run-b.quiet.txt")" -eq 1
  st_ok "run-b's quiet line is the UTC-stamped BYPASSED line" \
    grep -Eq '^[0-9-]+T[0-9:]+Z quiet-host window BYPASSED' "$r/results/$D/repro/run-b.quiet.txt"
  # S7b: window mode (Work 3) — the real loop, stubbed peripherals, so
  # the satisfied line itself is what gets recorded. No busy-load
  # case: with sleep stubbed it would spin to the 30-min deadline, and
  # the window's refusal path is unchanged and not under test here.
  r=$t/s7b; mkdir -p "$r/lib"; cp "$lib" "$r/lib/nb-lib.sh"
  win_loadavg="$t/s7b.loadavg"
  printf '0.50 0.40 0.30 1/100 1\n' > "$win_loadavg"
  win_bin="$t/s7b-bin"
  mkdir -p "$win_bin"
  printf '#!/usr/bin/env bash\nexit 0\n' > "$win_bin/sleep"
  printf '#!/usr/bin/env bash\nexit 1\n' > "$win_bin/pgrep"
  printf '#!/usr/bin/env bash\nexit 0\n' > "$win_bin/systemctl"
  chmod +x "$win_bin/sleep" "$win_bin/pgrep" "$win_bin/systemctl"
  st_rc "a succeeds through the real (stubbed) window" 0 st_run_win "$r" success a
  st_ok "run-a.quiet.txt holds exactly one line" test "$(wc -l < "$r/results/$D/repro/run-a.quiet.txt")" -eq 1
  st_ok "the recorded line is the satisfied line (first -> last load1, 13 samples)" \
    grep -Eq '^[0-9-]+T[0-9:]+Z quiet-host window satisfied \(load1 0\.50 -> 0\.50, 13 consecutive samples' "$r/results/$D/repro/run-a.quiet.txt"

  if [ "$fails" -eq 0 ]; then
    echo "REPRO SELFTEST OK ($t)"
    return 0
  fi
  echo "REPRO SELFTEST FAIL: $fails check(s) failed (scratch $t)" >&2
  return 1
}

case ${1:-} in
  a)         run_one a ;;
  b)         run_one b ;;
  verdict)   do_verdict ;;
  check)     do_check ;;
  selftest)  selftest ;;
  *)         echo "usage: reproduce.sh a|b|verdict|check|selftest" >&2; exit 2 ;;
esac
