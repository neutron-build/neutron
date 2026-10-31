#!/usr/bin/env bash
# Card B-0-F7 diagnosis: attribute the ~10 ms/query the sanity limit
# refused (chain10: warm p50 10.01 ms vs 1.6–1.9 ms single-request
# probes) between the client, the transport and the server, and record
# the index state that could explain the recall shift (0.9457 chain7 →
# 0.8774 chain10) — then apply the pre-registered c−s rule through the
# one tested implementation (`nbench qdrant-latency-verdict`).
#
# The script diagnoses the RUNNING collection (a `nbench run` or a
# manually started pinned container owns the lifecycle); it never
# starts, stops or mutates anything.
#
# Per execution (one client build, NB_LABEL):
#   1. quiet-host wait — chain10's rule, stated by the card: load1 < 4
#      for 2 consecutive minutes before every measured pass;
#   2. collection info before/after each pass (segment count, config,
#      counts — Work 2);
#   3. one ef=16 pass over the full 10k query set (the chain10 shape),
#      recording per-query client/server latencies and per-query ids —
#      a sanity refusal (c > 10 ms) exits non-zero but the latency CSV
#      and ids are already written: a refused pass still happened;
#   4. one exact brute-force control over the first 250 queries —
#      recall must be ~1.0, which validates the distance convention
#      and the harness recall computation against the live index state;
#   5. p50s recomputed from the CSV (type-7, the harness stats
#      definition) and classified by `qdrant-latency-verdict`.
#
# Environment: NB_HARNESS, NB_BIN (the build under test), NB_DATA,
# NB_OUT, NB_LABEL (e.g. debug|release); optional NB_PORT, NB_COLLECTION.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

BIN=${NB_BIN:?}; DATA=${NB_DATA:?}/sift; OUT=${NB_OUT:?}
HOST=127.0.0.1; PORT=${NB_PORT:-6333}; COLLECTION=${NB_COLLECTION:-nbench-sift}
LOG="$OUT/diagnose-${NB_LABEL:?}.log"
TABLE="$OUT/diagnose-${NB_LABEL}.md"
mkdir -p "$OUT"
: > "$LOG"

log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LOG" >&2; }

# load1 < 4, then still < 4 sixty seconds later (card Work 5's quiet
# window, applied before every measured pass). 30-minute budget.
quiet_wait() {
  local deadline=$(( $(date +%s) + 1800 )) a
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if awk '{exit !($1<4)}' /proc/loadavg; then
      a=$(cut -d' ' -f1 /proc/loadavg)
      sleep 60
      if awk '{exit !($1<4)}' /proc/loadavg; then
        log "quiet-host window satisfied (load1 $a -> $(cut -d' ' -f1 /proc/loadavg))"
        return 0
      fi
    fi
    sleep 15
  done
  echo "host never reached load1<4 for 2 consecutive minutes within 30min" >&2
  return 1
}

# Type-7 percentile (stats.rs percentile_sorted) of column `col` of a
# latency CSV (header skipped), matching the harness definition. The
# field separator is the comma: with awk's default whitespace FS the
# whole line is $1 and the server column silently reads empty — the
# first run of this script shipped s=0.000 that way and was caught
# against the raw CSV (the card's "c−s table empty" mutant, live).
col_pctl() { # csv column(1=client,2=server) p
  awk -F, -v c="$2" 'NR>1{print $c}' "$1" | sort -g | awk -v p="$3" '
    { a[NR]=$1 }
    END {
      n=NR
      if (n==1) { printf "%.6f\n", a[1]; exit }
      r=p*(n-1); base=int(r); f=r-base; lo=base+1; hi=base+2
      if (hi>n) hi=n
      printf "%.6f\n", a[lo]+(a[hi]-a[lo])*f
    }'
}

# recall@k of an ids JSONL against the ground truth — recomputed by
# the harness itself (`vec-recall`), so the same tested
# implementation that produced a pass's recall reproduces it from the
# evidence file (binary ivecs are not shell-parseable).
ids_recall() { # ids.jsonl groundtruth.ivecs k
  "$BIN" vec-recall --ids "$1" --truth "$2" --k "$3"
}

info_snapshot() { # file-tag
  "$BIN" vec-qdrant-info --host "$HOST" --port "$PORT" --collection "$COLLECTION" \
    > "$OUT/collection-info-$1.txt" 2>>"$LOG"
}

# One measured pass. Arguments: label, then extra search args.
run_pass() {
  local label=$1; shift
  quiet_wait
  info_snapshot "${NB_LABEL}-${label}-before"
  local rc=0
  "$BIN" vec-qdrant-search \
    --query "$DATA/query.fvecs" --truth "$DATA/groundtruth.ivecs" \
    --host "$HOST" --port "$PORT" --collection "$COLLECTION" \
    --ef 16 --limit 10 --pass-kind run \
    --latency-out "$OUT/${label}.latency.csv" \
    --ids-out "$OUT/${label}.ids.jsonl" \
    --metrics-out "$OUT/${label}.metrics.json" \
    "$@" >"$OUT/${label}.stdout.txt" 2>&1 || rc=$?
  info_snapshot "${NB_LABEL}-${label}-after"
  if [ "$rc" -ne 0 ] && [ ! -s "$OUT/${label}.latency.csv" ]; then
    log "pass $label failed (exit $rc) without a latency CSV — not a sanity refusal:"
    cat "$OUT/${label}.stdout.txt" >&2
    return 1
  fi
  if [ "$rc" -ne 0 ]; then
    # A written CSV plus a non-zero exit is the 10 ms sanity refusal
    # doing its job on a slow client (chain10's exact shape): the pass
    # happened, its evidence stands, only the metrics file is withheld.
    log "pass $label refused by the sanity check (exit $rc) — evidence kept"
  fi
  local c s verdict recall=""
  c=$(col_pctl "$OUT/${label}.latency.csv" 1 0.5)
  s=$(col_pctl "$OUT/${label}.latency.csv" 2 0.5)
  verdict=$("$BIN" qdrant-latency-verdict --client-p50-ms "$c" --server-p50-ms "$s")
  if [ -s "$OUT/${label}.metrics.json" ]; then
    recall=$(grep -o '"recall_at_10":[0-9.]*' "$OUT/${label}.metrics.json" | cut -d: -f2)
  else
    recall="$(ids_recall "$OUT/${label}.ids.jsonl" "$DATA/groundtruth.ivecs" 10) (recomputed from ids; pass refused)"
  fi
  printf '%s\n' "| ${NB_LABEL} | ${label} | ${c} | ${s} | $(awk -v a="$c" -v b="$s" 'BEGIN{printf "%.3f", a-b}') | ${verdict#VERDICT } | recall@10=${recall} |" >> "$TABLE"
  log "pass $label: $verdict recall=$recall"
}

log "=== diagnosis, client ${NB_LABEL}: $BIN"

printf '%s\n' "| client | pass | c p50 (ms) | s p50 (ms) | c−s (ms) | verdict (card Work 4) | recall |" > "$TABLE"
printf '%s\n' "|---|---|---|---|---|---|---|" >> "$TABLE"

info_snapshot "${NB_LABEL}-start"

# The chain10 shape: ef=16, full query set, the client build under test.
run_pass ef16-full

# Recall control: brute force over the same collection must reproduce
# the ground truth (validates distance convention + recall computation
# against the live index; 250 queries keep the full scans bounded).
quiet_wait
if "$BIN" vec-qdrant-search \
    --query "$DATA/query.fvecs" --truth "$DATA/groundtruth.ivecs" \
    --host "$HOST" --port "$PORT" --collection "$COLLECTION" \
    --ef 16 --limit 10 --pass-kind run --exact --max-queries 250 \
    --latency-out "$OUT/exact250.latency.csv" \
    --ids-out "$OUT/exact250.ids.jsonl" \
    --metrics-out "$OUT/exact250.metrics.json" \
    >"$OUT/exact250.stdout.txt" 2>&1
then
  recall=$(grep -o '"recall_at_10":[0-9.]*' "$OUT/exact250.metrics.json" | cut -d: -f2)
  printf '%s\n' "| ${NB_LABEL} | exact control (250 queries) | (control) | (control) | (control) | not classified (control) | recall@10=${recall} |" >> "$TABLE"
  log "exact control: recall@10=$recall"
  if awk -v r="$recall" 'BEGIN{exit !(r>=0.999)}'; then
    log "exact control confirms the truth/convention pair (recall>=0.999)"
  else
    log "EXACT CONTROL BELOW 0.999 — record and report, do not tune anything"
  fi
else
  log "exact control pass failed:"; cat "$OUT/exact250.stdout.txt" >&2
  exit 1
fi

info_snapshot "${NB_LABEL}-end"
log "=== diagnosis ${NB_LABEL} complete: $TABLE"
