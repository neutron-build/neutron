#!/bin/sh
# Differential PostgreSQL regression harness: every curated SQL script runs
# through the SAME psql client against a real PostgreSQL 17 instance and a
# release Nucleus, outputs are normalized (error texts collapsed, notices
# stripped) and diffed. A script PASSES when the normalized outputs are
# identical; otherwise the diff is a DEVIATION to fix or document in
# DEVIATIONS.md.
#
#   sh run.sh                 # build nucleus, boot both servers, run all
#   sh run.sh --no-build      # reuse existing release binary
#   sh run.sh types_null      # only named script(s)
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
NUCLEUS_DIR=$(cd "$HERE/../.." && pwd)
BIN="$NUCLEUS_DIR/target/release/nucleus"
WORK=$(mktemp -d)

BUILD=1
ONLY=""
for a in "$@"; do
    case "$a" in
        --no-build) BUILD=0 ;;
        -*) echo "unknown flag: $a" >&2; exit 2 ;;
        *) ONLY="$ONLY $a" ;;
    esac
done

if [ "$BUILD" = 1 ]; then
    echo "== building release nucleus =="
    (cd "$NUCLEUS_DIR" && cargo build --release 2>&1 | tail -1) || exit 1
fi

free_port() {
    node -e 'const s=require("net").createServer();s.listen(0,"127.0.0.1",()=>{console.log(s.address().port);s.close()})'
}

PG_PORT=$(free_port)
NU_PORT=$(free_port)

cleanup() {
    pg_ctl -D "$WORK/pgdata" stop -m immediate >/dev/null 2>&1
    [ -f "$WORK/nucleus.pid" ] && kill "$(cat "$WORK/nucleus.pid")" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

# ── Boot PostgreSQL 17 ──────────────────────────────────────────────────────
echo "== initdb + start postgres 17 (port $PG_PORT) =="
initdb -D "$WORK/pgdata" -A trust -U nucleus --locale=C --encoding=UTF8 >"$WORK/initdb.log" 2>&1 || {
    echo "initdb failed"; tail -5 "$WORK/initdb.log"; exit 1; }
pg_ctl -D "$WORK/pgdata" -o "-p $PG_PORT -c listen_addresses=127.0.0.1" \
    -l "$WORK/pg.log" start >/dev/null 2>&1 || { echo "pg start failed"; tail -5 "$WORK/pg.log"; exit 1; }
createdb -h 127.0.0.1 -p "$PG_PORT" -U nucleus nucleus >/dev/null 2>&1

# ── Boot Nucleus ────────────────────────────────────────────────────────────
echo "== start nucleus (port $NU_PORT) =="
mkdir -p "$WORK/nudata"
"$BIN" start --port "$NU_PORT" --host 127.0.0.1 --no-tls --data "$WORK/nudata" \
    >"$WORK/nucleus.log" 2>&1 &
echo $! > "$WORK/nucleus.pid"
i=0
while [ $i -lt 100 ]; do
    if node -e 'const n=require("net");const c=n.connect(process.argv[1],"127.0.0.1",()=>{c.end();process.exit(0)});c.on("error",()=>process.exit(1))' "$NU_PORT" 2>/dev/null; then break; fi
    i=$((i+1)); sleep 0.1
done

# ── Run scripts ─────────────────────────────────────────────────────────────
PSQL_FLAGS="-X -q --pset footer=off --pset null=∅ -v ON_ERROR_STOP=0"
PASS=0; DEV=0; DEVLIST=""
for f in "$HERE"/sql/*.sql; do
    name=$(basename "$f" .sql)
    if [ -n "$ONLY" ] && ! echo "$ONLY" | grep -qw "$name"; then continue; fi
    # Fresh schema per script: run in its own database-level namespace by
    # prefixing a DROP-all preamble is overkill — scripts self-contain their
    # DDL with unique table names and drop at the end.
    psql $PSQL_FLAGS -h 127.0.0.1 -p "$PG_PORT" -U nucleus -d nucleus -f "$f" \
        >"$WORK/$name.pg.raw" 2>&1
    psql $PSQL_FLAGS -h 127.0.0.1 -p "$NU_PORT" -U nucleus -d nucleus -f "$f" \
        >"$WORK/$name.nu.raw" 2>&1
    python3 "$HERE/normalize.py" "$WORK/$name.pg.raw" > "$WORK/$name.pg"
    python3 "$HERE/normalize.py" "$WORK/$name.nu.raw" > "$WORK/$name.nu"
    if diff -u "$WORK/$name.pg" "$WORK/$name.nu" > "$WORK/$name.diff" 2>&1; then
        PASS=$((PASS+1))
        printf "  %-28s PASS\n" "$name"
    else
        DEV=$((DEV+1))
        DEVLIST="$DEVLIST $name"
        printf "  %-28s DEVIATES (%s lines)\n" "$name" "$(wc -l < "$WORK/$name.diff" | tr -d ' ')"
        cp "$WORK/$name.diff" "$HERE/last_${name}.diff"
    fi
done

echo ""
echo "== pgregress: $PASS pass, $DEV deviate =="
[ -n "$DEVLIST" ] && echo "deviating:$DEVLIST (diffs copied to compat/pgregress/last_*.diff)"
[ "$DEV" -eq 0 ]
