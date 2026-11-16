#!/usr/bin/env bash
# X-5c-F4-F3 step 10: the CPU term tool (card Work 10, REFRESH 10's
# "CPU, fresh" contract as the owner read it on 2026-11-10).
# X-5c-F4-F4 step 3 (folding X-5c-F4-F3-F2): the --dry-run form, and
# the arm resolution and the band decision extracted into the shared
# functions resolve_arms and band_passes — the measuring form and the
# dry-run resolve the three arms through the same function, not a
# copy; the self-test gains the band-decision and dry-run cases.
#
# Forms:
#   x5_cpu_term.sh --self-test        drive the arithmetic, the band
#                                     decision and the dry-run arm
#                                     resolution with fixture CPU
#                                     numbers and a fixture git repo
#                                     (no build, no run) and print
#                                     CPU_SELFTEST OK, or exit
#                                     non-zero on the first mismatch
#   x5_cpu_term.sh --dry-run <base-ref> <prefix>
#                                     resolve the three arms exactly
#                                     as the measuring form does and
#                                     print exactly one line
#                                     CPU_ARMS base=<h> s1=<h> final=<h>
#                                     (40-hex each; exit 0). Builds
#                                     nothing, runs nothing and writes
#                                     nothing under <prefix>. When the
#                                     S1 subject ("nucleus v3:
#                                     X-5c-F4-F3 step 1 - ") matches
#                                     anything other than one commit
#                                     in <base-ref>..HEAD, prints an
#                                     error and exits 2 with no
#                                     CPU_ARMS line
#   x5_cpu_term.sh <base-ref> <prefix>  measure: three arms — BASE from
#                                     <base-ref>, S1 from the one commit
#                                     in <base-ref>..HEAD whose subject
#                                     starts "nucleus v3: X-5c-F4-F3
#                                     step 1 - ", FINAL from HEAD — each
#                                     from its own `git archive` under
#                                     <prefix>.src.<arm>, built once
#                                     into its own <prefix>.target.<arm>
#                                     and then run directly (the timed
#                                     commands are the binaries alone,
#                                     so cargo is never in the count).
#                                     Must be run from inside the git
#                                     worktree (git runs from the cwd).
#
# The owner's protocol, unchanged: five 200-seed blocks over 0..1000,
# 16 threads, each block running BASE, S1, FINAL back to back; user+sys
# by bash `time`. The bar's term is ΣCPU(S1) / ΣCPU(BASE) − 1 (Σ over
# the pass's five blocks). A first-pass term from +55 to +65 reads as
# the median of three passes. Every pass is reported.
#
# Guards, none of which changes the term: each pass's first block
# waits up to 10 minutes for a 1-minute load below 2, else VOID; each
# pass reads /proc/stat busy time (user+nice+system+irq+softirq+steal),
# and busy minus the measured user+sys of all three arms is foreign
# CPU — foreign above 5% of the measured CPU is VOID. Pass 1's cold
# start stays as measured: no warm-up block, no dropped pass.
#
# Output contract: exactly one line
#   CPU_TERM passes=<n> terms=[…] term=<x> foreign_pct=<y> verdict=OK|OVER|VOID
# (the S1 term; foreign_pct is the worst pass; exit 0 only for OK,
# term ≤ +60.00) and exactly one line
#   CPU_WHOLE_FINAL passes=<n> terms=[…] term=<x> wall_ratio=<r>
# (ΣCPU(FINAL)/ΣCPU(BASE) − 1 over the same passes, and the same for
# wall) — reported and never an exit input. Any other failure prints
# an error and exits 2 with no CPU_TERM line.
set -uo pipefail

# ---- arithmetic core (shared by --self-test and the measuring form) ----
# Inputs are globals: EVAL_PASSES (1 or 3); per pass i (1-indexed):
# EVAL_BASE_CPU[i], EVAL_S1_CPU[i], EVAL_FIN_CPU[i] (Σ user+sys s),
# EVAL_BASE_WALL[i], EVAL_FIN_WALL[i] (Σ wall s), EVAL_BUSY[i]
# (/proc/stat busy delta s); EVAL_VOID_WAIT (a load wait timed out).
# Outputs: the two contract lines on stdout, EVAL_VERDICT, EVAL_EXIT.

# A percentage a/b − 1, two decimals; ERR when b has no value yet.
pct() {
  awk -v a="$1" -v b="$2" 'BEGIN {
    if (b <= 0) { print "ERR"; exit 1 }
    printf "%.2f", 100.0 * (a / b - 1.0)
  }'
}

# A value rounded to integer hundredths ("60.004" -> 6004? no: -> 6000;
# rounding is to the nearest hundredth, so 60.004*100 = 6000.4 -> 6000).
hc() { awk -v v="$1" 'BEGIN { printf "%.0f", v * 100 }'; }

# The median of three values.
median3() { printf '%s\n%s\n%s\n' "$1" "$2" "$3" | sort -g | sed -n 2p; }

# True (0) when a first-pass term is inside the +55..+65 band, which
# owes the median of three passes.
in_band() {
  local h
  h=$(hc "$1")
  [ "$h" -ge 5500 ] && [ "$h" -le 6500 ]
}

# The band decision (X-5c-F4-F4 step 3, folding X-5c-F4-F3-F2): how
# many passes the first pass owes — 3 when the first-pass term sits in
# the +55..+65 band, else 1. **A function of the S1 and BASE pass-1
# columns only** (the inputs the measuring form holds at that point):
# the FINAL column is present among the pass-1 inputs and must not
# enter the decision — the self-test's band case pins that
# (G-CPU-BAND-FROM-FINAL). X-5c-PINS-2 item 3 (c): the answer is set
# in globals, never echoed — the old `np=$(band_passes)` capture ran
# this function in a subshell, so BAND_TERM never reached the parent
# and the BAND line logged an empty term. Sets BAND_TERM (the pass-1
# S1 term) and BAND_PASSES (3 or 1); returns 2 when pass 1 measured no
# BASE cpu.
band_passes() {
  BAND_TERM=$(pct "${EVAL_S1_CPU[1]}" "${EVAL_BASE_CPU[1]}")
  if [ "$BAND_TERM" = ERR ]; then
    echo "x5_cpu_term: pass 1 measured no BASE cpu" >&2
    return 2
  fi
  if in_band "$BAND_TERM"; then BAND_PASSES=3; else BAND_PASSES=1; fi
}

# The decision's shared call site (X-5c-PINS-2 item 3 (c)): run the
# band decision and, when three passes are owed, print the BAND line
# the measuring form logs — emitted in this one place, so the
# self-test drives exactly what the measuring form logs, term included
# (M-BAND-SUBSHELL's subshell capture loses BAND_TERM to a subshell
# and the line logs an empty field).
band_decide() {
  band_passes || return 2
  if [ "$BAND_PASSES" -eq 3 ]; then
    echo "BAND pass1_term=$BAND_TERM inside +55..+65: three passes, the median rules"
  fi
}

cpu_term_eval() {
  local i terms=() wterms=() t void=0 fmax=0 sb=0 ss=0 sf=0 swb=0 swf=0
  for ((i = 1; i <= EVAL_PASSES; i++)); do
    terms[i]=$(pct "${EVAL_S1_CPU[i]}" "${EVAL_BASE_CPU[i]}")
    wterms[i]=$(pct "${EVAL_FIN_CPU[i]}" "${EVAL_BASE_CPU[i]}")
    if [ "${terms[i]}" = ERR ] || [ "${wterms[i]}" = ERR ]; then
      echo "x5_cpu_term: a pass has no BASE cpu time" >&2
      EVAL_EXIT=2
      EVAL_VERDICT=ERR
      return
    fi
    # The foreign guard: busy minus the measured user+sys of all arms.
    local measured foreign fp fh
    measured=$(awk -v a="${EVAL_BASE_CPU[i]}" -v b="${EVAL_S1_CPU[i]}" \
      -v c="${EVAL_FIN_CPU[i]}" 'BEGIN { printf "%.3f", a + b + c }')
    foreign=$(awk -v b="${EVAL_BUSY[i]}" -v m="$measured" \
      'BEGIN { printf "%.3f", b - m }')
    fp=$(awk -v f="$foreign" -v m="$measured" \
      'BEGIN { if (m <= 0) { print "ERR"; exit 1 } printf "%.2f", 100.0 * f / m }')
    if [ "$fp" = ERR ]; then
      echo "x5_cpu_term: a pass measured no cpu time" >&2
      EVAL_EXIT=2
      EVAL_VERDICT=ERR
      return
    fi
    fh=$(hc "$fp")
    [ "$fh" -gt 500 ] && void=1
    fmax=$(awk -v a="$fmax" -v b="$fp" 'BEGIN { printf "%.2f", (a > b) ? a : b }')
    sb=$(awk -v a="$sb" -v b="${EVAL_BASE_CPU[i]}" 'BEGIN { printf "%.3f", a + b }')
    ss=$(awk -v a="$ss" -v b="${EVAL_S1_CPU[i]}" 'BEGIN { printf "%.3f", a + b }')
    sf=$(awk -v a="$sf" -v b="${EVAL_FIN_CPU[i]}" 'BEGIN { printf "%.3f", a + b }')
    swb=$(awk -v a="$swb" -v b="${EVAL_BASE_WALL[i]}" 'BEGIN { printf "%.3f", a + b }')
    swf=$(awk -v a="$swf" -v b="${EVAL_FIN_WALL[i]}" 'BEGIN { printf "%.3f", a + b }')
  done
  # The bar's term: the median of three when the first pass was in the
  # band (three passes were run), else the first pass's term.
  if [ "$EVAL_PASSES" -eq 3 ]; then
    t=$(median3 "${terms[1]}" "${terms[2]}" "${terms[3]}")
  else
    t=${terms[1]}
  fi
  [ "$EVAL_VOID_WAIT" -eq 1 ] && void=1
  EVAL_VERDICT=OVER
  local th
  th=$(hc "$t")
  if [ "$void" -eq 1 ]; then
    EVAL_VERDICT=VOID
  elif [ "$th" -le 6000 ]; then
    EVAL_VERDICT=OK
  fi
  EVAL_EXIT=0
  [ "$EVAL_VERDICT" = OK ] || EVAL_EXIT=1
  local tl wt
  tl=$(IFS=,; echo "${terms[*]}")
  wt=$(IFS=,; echo "${wterms[*]}")
  echo "CPU_TERM passes=$EVAL_PASSES terms=[$tl] term=$t foreign_pct=$fmax verdict=$EVAL_VERDICT"
  echo "CPU_WHOLE_FINAL passes=$EVAL_PASSES terms=[$wt] term=$(pct "$sf" "$sb") wall_ratio=$(pct "$swf" "$swb")"
}

# ---- self-test: fixture CPU numbers for three arms, no build, no run ----
self_test() {
  local fails=0
  expect() {
    # expect <name> <want_term_line> <want_whole_line> <want_exit>
    # (the eval runs inside a command substitution, so its exit rides
    # along as a trailing marker line and is stripped back out)
    local name=$1 wt1=$2 wt2=$3 we=$4 got ge
    got=$(cpu_term_eval; echo "@EXIT=$EVAL_EXIT")
    ge=${got##*@EXIT=}
    got=${got%@EXIT=*}
    got=${got%$'\n'}
    if [ "$got" != "$(printf '%s\n%s' "$wt1" "$wt2")" ] || [ "$ge" != "$we" ]; then
      echo "self-test FAIL $name:" >&2
      echo "  want exit $we, lines:" >&2
      printf '  %s\n  %s\n' "$wt1" "$wt2" >&2
      echo "  got exit $ge, lines:" >&2
      printf '  %s\n' >&2 "$got" >&2
      fails=$((fails + 1))
    fi
  }
  reset() {
    EVAL_PASSES=1 EVAL_VOID_WAIT=0
    EVAL_BASE_CPU=() EVAL_S1_CPU=() EVAL_FIN_CPU=()
    EVAL_BASE_WALL=() EVAL_FIN_WALL=() EVAL_BUSY=()
    BAND_TERM= BAND_PASSES=
  }

  # Case A — the term from the S1 column (and CPU_WHOLE_FINAL from the
  # FINAL column, above +60, leaving CPU_TERM OK); foreign exactly 5%
  # passes. 1000/1500/1800: term +50.00, whole +80.00; busy 4515 =
  # 105% of the measured 4300.
  reset
  EVAL_BASE_CPU[1]=1000 EVAL_S1_CPU[1]=1500 EVAL_FIN_CPU[1]=1800
  EVAL_BASE_WALL[1]=100 EVAL_FIN_WALL[1]=200 EVAL_BUSY[1]=4515
  expect "term-from-s1-not-final" \
    "CPU_TERM passes=1 terms=[50.00] term=50.00 foreign_pct=5.00 verdict=OK" \
    "CPU_WHOLE_FINAL passes=1 terms=[80.00] term=80.00 wall_ratio=100.00" \
    0

  # Case B — a first pass inside the band takes the median of three
  # (62.00, 40.00, 50.00 -> 50.00, not the first pass). busy equals
  # each pass's measured cpu (foreign 0).
  reset
  EVAL_PASSES=3
  EVAL_BASE_CPU=(0 1000 1000 1000)
  EVAL_S1_CPU=(0 1620 1400 1500)
  EVAL_FIN_CPU=(0 1800 1800 1800)
  EVAL_BASE_WALL=(0 100 100 100)
  EVAL_FIN_WALL=(0 200 200 200)
  EVAL_BUSY=(0 4420 4200 4300)
  expect "median-of-three" \
    "CPU_TERM passes=3 terms=[62.00,40.00,50.00] term=50.00 foreign_pct=0.00 verdict=OK" \
    "CPU_WHOLE_FINAL passes=3 terms=[80.00,80.00,80.00] term=80.00 wall_ratio=100.00" \
    0

  # Case C — +60.00 reads OK (median of 60.00, 61.00, 45.00).
  reset
  EVAL_PASSES=3
  EVAL_BASE_CPU=(0 1000 1000 1000)
  EVAL_S1_CPU=(0 1600 1610 1450)
  EVAL_FIN_CPU=(0 1800 1800 1800)
  EVAL_BASE_WALL=(0 100 100 100)
  EVAL_FIN_WALL=(0 200 200 200)
  EVAL_BUSY=(0 4400 4410 4250)
  expect "boundary-60-ok" \
    "CPU_TERM passes=3 terms=[60.00,61.00,45.00] term=60.00 foreign_pct=0.00 verdict=OK" \
    "CPU_WHOLE_FINAL passes=3 terms=[80.00,80.00,80.00] term=80.00 wall_ratio=100.00" \
    0

  # Case D — +60.01 reads OVER (median of 60.01, 62.00, 45.00; the
  # 100000 base keeps 60.01 exact in the arithmetic).
  reset
  EVAL_PASSES=3
  EVAL_BASE_CPU=(0 100000 100000 100000)
  EVAL_S1_CPU=(0 160010 162000 145000)
  EVAL_FIN_CPU=(0 180000 180000 180000)
  EVAL_BASE_WALL=(0 100 100 100)
  EVAL_FIN_WALL=(0 200 200 200)
  EVAL_BUSY=(0 440010 442000 425000)
  expect "boundary-60-01-over" \
    "CPU_TERM passes=3 terms=[60.01,62.00,45.00] term=60.01 foreign_pct=0.00 verdict=OVER" \
    "CPU_WHOLE_FINAL passes=3 terms=[80.00,80.00,80.00] term=80.00 wall_ratio=100.00" \
    1

  # Case E — foreign above 5% is VOID (busy 4516 = 105.02% of 4300).
  reset
  EVAL_BASE_CPU[1]=1000 EVAL_S1_CPU[1]=1500 EVAL_FIN_CPU[1]=1800
  EVAL_BASE_WALL[1]=100 EVAL_FIN_WALL[1]=200 EVAL_BUSY[1]=4516
  expect "foreign-above-5-void" \
    "CPU_TERM passes=1 terms=[50.00] term=50.00 foreign_pct=5.02 verdict=VOID" \
    "CPU_WHOLE_FINAL passes=1 terms=[80.00] term=80.00 wall_ratio=100.00" \
    1

  # Case F — a load-wait timeout is VOID even with clean foreign cpu.
  reset
  EVAL_VOID_WAIT=1
  EVAL_BASE_CPU[1]=1000 EVAL_S1_CPU[1]=1500 EVAL_FIN_CPU[1]=1800
  EVAL_BASE_WALL[1]=100 EVAL_FIN_WALL[1]=200 EVAL_BUSY[1]=4300
  expect "wait-timeout-void" \
    "CPU_TERM passes=1 terms=[50.00] term=50.00 foreign_pct=0.00 verdict=VOID" \
    "CPU_WHOLE_FINAL passes=1 terms=[80.00] term=80.00 wall_ratio=100.00" \
    1

  # Case G (X-5c-F4-F4 step 3, case (a)) — the band decision is a
  # function of the S1 and BASE pass-1 columns only: the FINAL column
  # is set to a value that would flip the answer if it were read. S1
  # at +50 with FINAL at +60 takes one pass; S1 at +58 with FINAL at
  # +70 takes three. (G-CPU-BAND-FROM-FINAL reverts this and fails.)
  # X-5c-PINS-2 item 3 (c): the answer now arrives in BAND_PASSES, not
  # an echo.
  reset
  EVAL_BASE_CPU[1]=1000 EVAL_S1_CPU[1]=1500 EVAL_FIN_CPU[1]=1600
  band_passes
  if [ "$BAND_PASSES" != 1 ]; then
    echo "self-test FAIL band-decision-one-pass (S1 +50, FINAL +60 must take one pass; BAND_PASSES=$BAND_PASSES)" >&2
    fails=$((fails + 1))
  fi
  reset
  EVAL_BASE_CPU[1]=1000 EVAL_S1_CPU[1]=1580 EVAL_FIN_CPU[1]=1700
  band_passes
  if [ "$BAND_PASSES" != 3 ]; then
    echo "self-test FAIL band-decision-three-passes (S1 +58, FINAL +70 must take three; BAND_PASSES=$BAND_PASSES)" >&2
    fails=$((fails + 1))
  fi

  # Case I (X-5c-PINS-2 item 3 (c)) — the value the measuring form
  # logs: at the same fixture pass-1 columns (BASE 1000, S1 1580; term
  # +58.00, inside the band) the BAND line carries 58.00, not an empty
  # field. The old `np=$(band_passes)` site ran the decision in a
  # subshell, so BAND_TERM never reached the parent and this line
  # logged empty (M-BAND-SUBSHELL reverts this and fails this case).
  reset
  EVAL_BASE_CPU[1]=1000 EVAL_S1_CPU[1]=1580 EVAL_FIN_CPU[1]=1700
  local band_out
  band_out=$(band_decide)
  if [ "$band_out" != "BAND pass1_term=58.00 inside +55..+65: three passes, the median rules" ]; then
    echo "self-test FAIL band-logged-term (got: $band_out)" >&2
    fails=$((fails + 1))
  fi

  # Case H (X-5c-F4-F4 step 3, cases (b) and (c)) — the dry-run
  # resolves the three arms through resolve_arms, the same function
  # the measuring form uses, on a fixture git repository built under
  # ${CARGO_TARGET_DIR:?}/x5-cpu-selftest.<pid>/: a base commit, a
  # commit with the S1 subject, and a later HEAD commit (case b:
  # s1 resolves to the middle commit, final to HEAD, and s1 differs
  # from final; nothing is written under the prefix — G-CPU-S1-IS-HEAD
  # reverts this and fails). The same repository with a second
  # S1-subject commit exits 2 with no CPU_ARMS line, and so does a
  # repository with none (case c — M-CPU-DRYRUN-MULTI reverts this
  # and fails). The fixture directory is removed before the verdict.
  local fx repo repo2 base mid tip out rc dry_ok=1
  fx=${CARGO_TARGET_DIR:?}/x5-cpu-selftest.$$
  repo=$fx/repo
  if mkdir -p "$repo" &&
     (cd "$repo" &&
       git init -q &&
       git -c user.name=t -c user.email=t@t commit -q --allow-empty -m base &&
       git -c user.name=t -c user.email=t@t commit -q --allow-empty \
         -m "nucleus v3: X-5c-F4-F3 step 1 - fixture S1" &&
       git -c user.name=t -c user.email=t@t commit -q --allow-empty -m later)
  then
    base=$(cd "$repo" && git rev-parse HEAD~2)
    mid=$(cd "$repo" && git rev-parse HEAD~1)
    tip=$(cd "$repo" && git rev-parse HEAD)
    out=$(cd "$repo" && dry_run "$base" "$fx/prefix" 2>&1)
    rc=$?
    if [ "$rc" -ne 0 ] ||
       [ "$out" != "CPU_ARMS base=$base s1=$mid final=$tip" ] ||
       [ "$mid" = "$tip" ] ||
       [ -e "$fx/prefix.src.S1" ]; then
      echo "self-test FAIL dry-run-resolves (rc=$rc, out=$out)" >&2
      dry_ok=0
    fi
    # Case (c), first half: a second S1-subject commit.
    (cd "$repo" &&
      git -c user.name=t -c user.email=t@t commit -q --allow-empty \
        -m "nucleus v3: X-5c-F4-F3 step 1 - fixture S1 again")
    out=$(cd "$repo" && dry_run "$base" "$fx/prefix" 2>&1)
    rc=$?
    if [ "$rc" -ne 2 ] || printf '%s\n' "$out" | grep -q '^CPU_ARMS '; then
      echo "self-test FAIL dry-run-two-s1 (rc=$rc, out=$out)" >&2
      dry_ok=0
    fi
  else
    echo "self-test FAIL dry-run-fixture (the fixture repo could not be built)" >&2
    dry_ok=0
  fi
  repo2=$fx/repo2
  if mkdir -p "$repo2" &&
     (cd "$repo2" &&
       git init -q &&
       git -c user.name=t -c user.email=t@t commit -q --allow-empty -m base &&
       git -c user.name=t -c user.email=t@t commit -q --allow-empty -m later)
  then
    base=$(cd "$repo2" && git rev-parse HEAD~1)
    out=$(cd "$repo2" && dry_run "$base" "$fx/prefix" 2>&1)
    rc=$?
    if [ "$rc" -ne 2 ] || printf '%s\n' "$out" | grep -q '^CPU_ARMS '; then
      echo "self-test FAIL dry-run-no-s1 (rc=$rc, out=$out)" >&2
      dry_ok=0
    fi
  else
    echo "self-test FAIL dry-run-fixture-2 (the second fixture repo could not be built)" >&2
    dry_ok=0
  fi
  rm -rf "$fx"
  if [ "$dry_ok" -ne 1 ]; then
    fails=$((fails + 1))
  fi

  # The band edges: 55.00 and 65.00 owe three passes; 54.99 and 65.01
  # do not.
  local edges_ok=1
  in_band 55.00 || edges_ok=0
  in_band 65.00 || edges_ok=0
  in_band 54.99 && edges_ok=0
  in_band 65.01 && edges_ok=0
  if [ "$edges_ok" -ne 1 ]; then
    echo "self-test FAIL band-edges" >&2
    fails=$((fails + 1))
  fi

  if [ "$fails" -gt 0 ]; then
    echo "CPU_SELFTEST FAILED ($fails)" >&2
    exit 1
  fi
  echo "CPU_SELFTEST OK"
}

# ---- the measuring form ----
CLK=$(getconf CLK_TCK 2>/dev/null || echo 100)

busy_s() {
  # /proc/stat's cpu line, user+nice+system+irq+softirq+steal, seconds.
  awk -v clk="$CLK" '/^cpu / {
    printf "%.3f", ($2 + $3 + $4 + $7 + $8 + $9) / clk
    exit
  }' /proc/stat
}

load1() { read -r a _ < /proc/loadavg && echo "$a"; }

# Wait up to 600 s for a 1-minute load below 2; set WAIT_VOID on
# timeout (the guard never changes the term — the pass still runs).
wait_load() {
  local waited=0 l
  while :; do
    l=$(load1)
    if awk -v l="$l" 'BEGIN { exit !(l < 2.0) }'; then
      echo "WAIT pass=$1 waited_s=$waited load=$l timeout=0"
      return
    fi
    [ "$waited" -ge 600 ] && break
    sleep 10
    waited=$((waited + 10))
  done
  echo "WAIT pass=$1 waited_s=$waited load=$(load1) timeout=1"
  WAIT_VOID=1
}

# One timed arm run (the bash-`time` quirk from x5_probe.sh applies
# here too: redirections inside the timed subshell, no exec).
timed_run() {
  local bin=$1 out=$2 base=$3
  shift 3
  {
    TIMEFORMAT='%U %S %R'
    time ("$bin" "$@" >"$out" 2>"$base.err")
  } 2>"$base.time"
}

# Resolve the three arms' commits (X-5c-F4-F4 step 3): BASE from
# <base-ref>, S1 from the one commit in <base-ref>..HEAD whose subject
# starts "nucleus v3: X-5c-F4-F3 step 1 - ", FINAL from HEAD. The
# measuring form and --dry-run both resolve through this one function
# — not a copy — so the two forms cannot drift. Sets ARM_BASE, ARM_S1
# and ARM_FINAL (full 40-hex ids). Returns 2 with a message on stderr
# when the base ref does not resolve, or when the S1 subject matches
# anything other than one commit in the range.
resolve_arms() {
  local base_ref=$1 base_commit s1_commit c count subject
  base_commit=$(git rev-parse --verify --quiet "$base_ref^{commit}") || {
    echo "x5_cpu_term: cannot resolve base ref $base_ref" >&2
    return 2
  }
  count=0
  for c in $(git rev-list "$base_commit..HEAD"); do
    subject=$(git log -1 --format=%s "$c")
    case "$subject" in
      "nucleus v3: X-5c-F4-F3 step 1 - "*) count=$((count + 1)); s1_commit=$c ;;
    esac
  done
  if [ "$count" -ne 1 ]; then
    echo "x5_cpu_term: expected 1 'step 1 -' commit in $base_ref..HEAD, found $count" >&2
    return 2
  fi
  ARM_BASE=$base_commit
  ARM_S1=$s1_commit
  ARM_FINAL=$(git rev-parse HEAD)
}

# The dry-run form (X-5c-F4-F4 step 3): resolve the three arms through
# resolve_arms and print the one CPU_ARMS line. Builds nothing, runs
# nothing and writes nothing under <prefix> (the argument is part of
# the form and deliberately unused here). Exits 0, or 2 through
# resolve_arms with no CPU_ARMS line.
dry_run() {
  local base_ref=$1 prefix=$2
  resolve_arms "$base_ref" || return 2
  echo "CPU_ARMS base=$ARM_BASE s1=$ARM_S1 final=$ARM_FINAL"
}

measure() {
  local base_ref=$1 prefix=$2
  local arm ref toplevel
  resolve_arms "$base_ref" || return 2
  toplevel=$(git rev-parse --show-toplevel) || {
    echo "x5_cpu_term: not inside the worktree (git rev-parse failed)" >&2
    return 2
  }
  for arm in BASE S1 FINAL; do
    case "$arm" in
      BASE) ref=$ARM_BASE ;;
      S1) ref=$ARM_S1 ;;
      FINAL) ref=$ARM_FINAL ;;
    esac
    mkdir -p "$prefix.src.$arm" "$prefix.target.$arm" || return 2
    # The archive is taken from the worktree root so nucleus/v2 is
    # present: e1-bench's path dependency (../../../v2/crates/…) is
    # resolved for every workspace member even though only
    # e4-recovery is built (an archive rooted at nucleus/v3 breaks
    # workspace manifest loading — the E4-SILENT-1 gate hit the same
    # thing and excluded e1-bench; archiving the root needs no edit).
    (cd "$toplevel" && git archive "$ref") | tar -x -C "$prefix.src.$arm" || {
      echo "x5_cpu_term: git archive failed for $arm ($ref)" >&2
      return 2
    }
    (cd "$prefix.src.$arm/nucleus/v3" &&
      CARGO_TARGET_DIR="$prefix.target.$arm" cargo build -q --release \
        -p e4-recovery --bin e4-campaign) || {
      echo "x5_cpu_term: build failed for $arm ($ref)" >&2
      return 2
    }
    ARMBIN[$arm]="$prefix.target.$arm/release/e4-campaign"
  done

  local passes=1 pass b arm busy0 busy1 wall u s w
  WAIT_VOID=0
  for pass in 1 2 3; do
    [ "$pass" -le "$passes" ] || break
    wait_load "$pass"
    busy0=$(busy_s)
    mkdir -p "$prefix.pass$pass" || return 2
    for arm in BASE S1 FINAL; do
      PASS_CPU[$arm]=0
      PASS_WALL[$arm]=0
    done
    for b in 0 200 400 600 800; do
      for arm in BASE S1 FINAL; do
        local base="$prefix.pass$pass/$arm.$b"
        timed_run "${ARMBIN[$arm]}" "$base.json" "$base" \
          run --seeds "$b..$((b + 200))" --threads 16 --out "$base.json"
        if [ $? -ne 0 ]; then
          echo "x5_cpu_term: $arm block $b failed (see $base.err)" >&2
          return 2
        fi
        read -r u s w <"$base.time"
        local cpu
        cpu=$(awk -v u="$u" -v s="$s" 'BEGIN { printf "%.3f", u + s }')
        wall=$(awk -v w="$w" 'BEGIN { printf "%.3f", w }')
        PASS_CPU[$arm]=$(awk -v a="${PASS_CPU[$arm]}" -v b="$cpu" 'BEGIN { printf "%.3f", a + b }')
        PASS_WALL[$arm]=$(awk -v a="${PASS_WALL[$arm]}" -v b="$wall" 'BEGIN { printf "%.3f", a + b }')
        echo "BLOCK pass=$pass arm=$arm seeds=$b..$((b + 200)) load=$(load1) wall_s=$wall cpu_s=$cpu"
      done
    done
    busy1=$(busy_s)
    PASS_BUSY[$pass]=$(awk -v a="$busy0" -v b="$busy1" 'BEGIN { printf "%.3f", b - a }')
    EVAL_BASE_CPU[$pass]=${PASS_CPU[BASE]}
    EVAL_S1_CPU[$pass]=${PASS_CPU[S1]}
    EVAL_FIN_CPU[$pass]=${PASS_CPU[FINAL]}
    EVAL_BASE_WALL[$pass]=${PASS_WALL[BASE]}
    EVAL_FIN_WALL[$pass]=${PASS_WALL[FINAL]}
    EVAL_BUSY[$pass]=${PASS_BUSY[$pass]}
    if [ "$pass" -eq 1 ] && [ "$passes" -eq 1 ]; then
      # The band decision, shared with the self-test (X-5c-PINS-2 item
      # 3 (c)): a function of the S1 and BASE pass-1 columns only, run
      # in this shell — no `$(…)` capture, so BAND_TERM and BAND_PASSES
      # reach the parent and the BAND line logs the term it decided on
      # (the old np=$(band_passes) site lost BAND_TERM to a subshell).
      band_decide || return 2
      if [ "$BAND_PASSES" -eq 3 ]; then
        passes=3
      fi
    fi
  done
  EVAL_PASSES=$([ "$passes" -eq 3 ] && echo 3 || echo 1)
  EVAL_VOID_WAIT=$WAIT_VOID
  cpu_term_eval
  return "$EVAL_EXIT"
}

declare -A ARMBIN PASS_CPU PASS_WALL
declare -a EVAL_BASE_CPU EVAL_S1_CPU EVAL_FIN_CPU EVAL_BASE_WALL \
  EVAL_FIN_WALL EVAL_BUSY PASS_BUSY

if [ "${1:-}" = "--self-test" ]; then
  self_test
  exit $?
fi
if [ "${1:-}" = "--dry-run" ]; then
  if [ $# -ne 3 ]; then
    echo "usage: x5_cpu_term.sh --dry-run <base-ref> <prefix> | <base-ref> <prefix> | --self-test" >&2
    exit 2
  fi
  dry_run "$2" "$3"
  exit $?
fi
if [ $# -ne 2 ]; then
  echo "usage: x5_cpu_term.sh <base-ref> <prefix> | --dry-run <base-ref> <prefix> | --self-test" >&2
  exit 2
fi
measure "$1" "$2"
exit $?
