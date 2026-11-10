#!/usr/bin/env bash
# X-5c-F4-F4 step 4: the load guard, a reusable wrapper so every
# wall-time row reports foreign CPU (this card's 10k and X-5c-F4-F5's
# probes). It wraps one command, passes the command's stdout, stderr
# and exit code through unchanged, and writes exactly one line to
# <guard-file> (nothing to stdout):
#   GUARD wait=<ok|timeout> load_at_start=<a b c> wall_s=<s> cpu_s=<user+sys> busy_s=<s> foreign_pct=<x> verdict=<CLEAN|VOID>
#
# What it does:
#   - waits up to 600 s for a 1-minute load below 2, checking every
#     10 s (x5_cpu_term.sh's wait, silently);
#   - reads /proc/stat's aggregate cpu line before and after: busy =
#     user + nice + system + irq + softirq + steal, jiffies over
#     getconf CLK_TCK;
#   - runs the command under bash `time` with TIMEFORMAT='%U %S' and
#     no `exec` (F3 measured that `time ( exec … )` prints no report;
#     the redirection form below passes the command's stdout/stderr
#     through fd4 while the report follows the subshell's fd2);
#   - foreign = busy − cpu, foreign_pct = foreign / cpu × 100;
#     verdict=VOID when the wait timed out or foreign_pct > 5.00,
#     else CLEAN — the same rule and the same 5% as x5_cpu_term.sh.
#
# The verdict is a report, never the guard's exit code: the guard
# exits with the command's code, or 2 on a usage error. Load can only
# lengthen wall: a VOID row's wall is an upper bound for an idle
# host, never a lower one.
#
# Forms:
#   x5_guard.sh <guard-file> -- <command> [args…]   wrap the command
#   x5_guard.sh --self-test   drive the arithmetic from fixture
#                             /proc/stat pairs and fixture time
#                             output, with no wait (plus one wrapped
#                             sh -c 'exit 3' through the internal
#                             no-wait path), and print
#                             GUARD_SELFTEST OK, or exit non-zero on
#                             the first mismatch.
set -uo pipefail

CLK=$(getconf CLK_TCK 2>/dev/null || echo 100)

loadavg() { read -r a b c _ < /proc/loadavg && echo "$a $b $c"; }

# One /proc/stat aggregate cpu line -> busy seconds (user+nice+system+
# irq+softirq+steal over CLK_TCK).
busy_of_line() {
  awk -v clk="$CLK" '$1 == "cpu" { printf "%.3f", ($2 + $3 + $4 + $7 + $8 + $9) / clk; exit }' <<<"$1"
}

busy_s() { busy_of_line "$(awk '/^cpu /' /proc/stat)"; }

# A value rounded to integer hundredths, as in x5_cpu_term.sh's hc().
hc() { awk -v v="$1" 'BEGIN { printf "%.0f", v * 100 }'; }

# Wait up to 600 s for a 1-minute load below 2, checking every 10 s;
# sets GUARD_WAIT (ok|timeout) and prints nothing. X5_GUARD_NOWAIT=1 is
# the --self-test's internal path: it skips the wait (the load is
# still read) so the self-test never sleeps.
wait_load() {
  GUARD_WAIT=ok
  [ "${X5_GUARD_NOWAIT:-0}" = 1 ] && return
  local waited=0 l
  while :; do
    read -r l _ < /proc/loadavg
    if awk -v l="$l" 'BEGIN { exit !(l < 2.0) }'; then
      return
    fi
    [ "$waited" -ge 600 ] && break
    sleep 10
    waited=$((waited + 10))
  done
  GUARD_WAIT=timeout
}

# The arithmetic core (the --self-test drives this from fixtures).
# Inputs are globals: GUARD_WAIT (ok|timeout), GUARD_LOAD_AT_START,
# GUARD_WALL (s), GUARD_CPU (user+sys s), GUARD_BUSY0, GUARD_BUSY1.
# Outputs: GUARD_LINE and GUARD_VERDICT. Returns 2 when the command
# measured no cpu time (foreign_pct would divide by zero).
guard_eval() {
  local busy foreign fp fh w3 c3
  busy=$(awk -v a="$GUARD_BUSY0" -v b="$GUARD_BUSY1" 'BEGIN { printf "%.3f", b - a }')
  # foreign = busy minus the measured cpu; NOT busy alone (the
  # self-test's busy-minus-cpu case pins this).
  foreign=$(awk -v b="$busy" -v c="$GUARD_CPU" 'BEGIN { printf "%.3f", b - c }')
  fp=$(awk -v f="$foreign" -v c="$GUARD_CPU" 'BEGIN { if (c <= 0) { print "ERR"; exit 1 } printf "%.2f", 100.0 * f / c }')
  if [ "$fp" = ERR ]; then
    echo "x5_guard: the command measured no cpu time" >&2
    return 2
  fi
  GUARD_VERDICT=CLEAN
  fh=$(hc "$fp")
  [ "$fh" -gt 500 ] && GUARD_VERDICT=VOID
  [ "$GUARD_WAIT" = timeout ] && GUARD_VERDICT=VOID
  w3=$(awk -v v="$GUARD_WALL" 'BEGIN { printf "%.3f", v }')
  c3=$(awk -v v="$GUARD_CPU" 'BEGIN { printf "%.3f", v }')
  GUARD_LINE="GUARD wait=$GUARD_WAIT load_at_start=$GUARD_LOAD_AT_START wall_s=$w3 cpu_s=$c3 busy_s=$busy foreign_pct=$fp verdict=$GUARD_VERDICT"
}

# Wall clock now, seconds with subsecond precision.
now_s() {
  if [ -n "${EPOCHREALTIME:-}" ]; then echo "$EPOCHREALTIME"; else date +%s.%N; fi
}

# Wrap one command: passthrough stdout/stderr/exit code, one GUARD
# line to <guard-file>. The time report is captured beside the guard
# file (removed once read; a killed guard may leave it behind).
guard_run() {
  local guard_file=$1
  shift
  local time_file=$guard_file.time busy0 busy1 t0 t1 cmd_rc u s
  GUARD_LOAD_AT_START=$(loadavg)
  wait_load
  busy0=$(busy_s)
  t0=$(now_s)
  {
    TIMEFORMAT='%U %S'
    time ( "$@" 2>&4 )
  } 4>&2 2>"$time_file"
  cmd_rc=$?
  t1=$(now_s)
  busy1=$(busy_s)
  read -r u s <"$time_file"
  rm -f "$time_file"
  GUARD_CPU=$(awk -v u="$u" -v s="$s" 'BEGIN { printf "%.3f", u + s }')
  GUARD_WALL=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.3f", b - a }')
  GUARD_BUSY0=$busy0
  GUARD_BUSY1=$busy1
  guard_eval || return 2
  printf '%s\n' "$GUARD_LINE" >"$guard_file"
  return "$cmd_rc"
}

# ---- self-test ----
self_test() {
  local fails=0

  # A fixture /proc/stat cpu line whose busy total (all user) is <s>
  # seconds at this host's CLK_TCK.
  fx_line() {
    awk -v clk="$CLK" -v s="$1" 'BEGIN { printf "cpu %.0f 0 0 0 0 0 0 0 0 0\n", s * clk }'
  }

  # expect_line <name> <wait> <busy0> <busy1> <cpu> <wall> <want line>
  expect_line() {
    local name=$1 wait=$2 b0=$3 b1=$4 cpu=$5 wall=$6 want=$7
    GUARD_WAIT=$wait GUARD_LOAD_AT_START="0.10 0.20 0.30"
    GUARD_BUSY0=$b0 GUARD_BUSY1=$b1 GUARD_CPU=$cpu GUARD_WALL=$wall
    guard_eval || {
      echo "self-test FAIL $name: guard_eval errored" >&2
      fails=$((fails + 1))
      return
    }
    if [ "$GUARD_LINE" != "$want" ]; then
      echo "self-test FAIL $name:" >&2
      echo "  want: $want" >&2
      echo "  got : $GUARD_LINE" >&2
      fails=$((fails + 1))
    fi
  }

  # The fixtures go through busy_of_line (fixture /proc/stat pairs),
  # so the parser is exercised too.
  local l0=$(fx_line 0) l105=$(fx_line 105) l105_01=$(fx_line 105.01)
  local l100=$(fx_line 100) l210=$(fx_line 210)

  # foreign exactly 5.00% reads CLEAN (busy delta 105, cpu 100).
  expect_line foreign-5-00-clean ok "$(busy_of_line "$l0")" "$(busy_of_line "$l105")" 100 50 \
    "GUARD wait=ok load_at_start=0.10 0.20 0.30 wall_s=50.000 cpu_s=100.000 busy_s=105.000 foreign_pct=5.00 verdict=CLEAN"

  # foreign 5.01% reads VOID.
  expect_line foreign-5-01-void ok "$(busy_of_line "$l0")" "$(busy_of_line "$l105_01")" 100 50 \
    "GUARD wait=ok load_at_start=0.10 0.20 0.30 wall_s=50.000 cpu_s=100.000 busy_s=105.010 foreign_pct=5.01 verdict=VOID"

  # a wait timeout reads VOID whatever the foreign share (here 0.00).
  expect_line wait-timeout-void timeout "$(busy_of_line "$l0")" "$(busy_of_line "$l100")" 100 50 \
    "GUARD wait=timeout load_at_start=0.10 0.20 0.30 wall_s=50.000 cpu_s=100.000 busy_s=100.000 foreign_pct=0.00 verdict=VOID"

  # foreign_pct uses busy minus cpu, not busy alone: busy delta 210
  # with cpu 200 is foreign 10 -> 5.00 CLEAN; busy alone would read
  # 105.00 VOID (this case flips the verdict, not just the number).
  expect_line foreign-is-busy-minus-cpu ok "$(busy_of_line "$l0")" "$(busy_of_line "$l210")" 200 50 \
    "GUARD wait=ok load_at_start=0.10 0.20 0.30 wall_s=50.000 cpu_s=200.000 busy_s=210.000 foreign_pct=5.00 verdict=CLEAN"

  # A wrapped sh -c 'exit 3', run through the internal no-wait path:
  # the guard exits 3 and writes exactly one GUARD line. The busy/cpu
  # numbers are this host's; the shape and the code are what is pinned.
  local fx g out rc
  fx=${CARGO_TARGET_DIR:?}/x5-guard-selftest.$$
  g=$fx/guard
  mkdir -p "$fx"
  out=$(X5_GUARD_NOWAIT=1 bash "$0" "$g" -- sh -c 'exit 3' 2>&1)
  rc=$?
  if [ "$rc" -ne 3 ] || [ "$(grep -c '^GUARD ' "$g")" != 1 ] ||
     ! grep -Eq '^GUARD wait=ok load_at_start=[0-9. ]+ wall_s=[0-9.]+ cpu_s=[0-9.]+ busy_s=[0-9.]+ foreign_pct=[0-9.]+ verdict=(CLEAN|VOID)$' "$g"; then
    echo "self-test FAIL wrapped-exit-3 (rc=$rc, guard file:" >&2
    cat "$g" >&2
    echo ", output: $out)" >&2
    fails=$((fails + 1))
  fi
  rm -rf "$fx"

  if [ "$fails" -gt 0 ]; then
    echo "GUARD_SELFTEST FAILED ($fails)" >&2
    exit 1
  fi
  echo "GUARD_SELFTEST OK"
}

if [ "${1:-}" = "--self-test" ]; then
  self_test
  exit $?
fi
if [ $# -lt 3 ] || [ "$2" != "--" ]; then
  echo "usage: x5_guard.sh <guard-file> -- <command> [args...] | --self-test" >&2
  exit 2
fi
guard_file=$1
shift 2
guard_run "$guard_file" "$@"
exit $?
