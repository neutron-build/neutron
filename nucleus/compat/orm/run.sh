#!/bin/sh
# ORM compatibility harness — boots a release Nucleus on an ephemeral port per
# ORM (fresh data dir each), runs each ORM's canonical flow (connect → migrate
# → CRUD → transaction → extras), tears down, prints a PASS/FAIL/SKIP matrix.
#
# Usage:
#   sh run.sh                # build release nucleus + run every available ORM
#   sh run.sh drizzle        # only the named ORM(s)
#   sh run.sh --no-build     # reuse existing target/release/nucleus
#
# An ORM is SKIPped (not failed) when its toolchain is unavailable — the same
# contract as conformance/runner. Exit is non-zero only on FAIL.
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
NUCLEUS_DIR=$(cd "$HERE/../.." && pwd)
BIN="$NUCLEUS_DIR/target/release/nucleus"

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
[ -x "$BIN" ] || { echo "missing $BIN" >&2; exit 1; }

free_port() {
    node -e 'const s=require("net").createServer();s.listen(0,"127.0.0.1",()=>{console.log(s.address().port);s.close()})'
}

# start_server <workdir> -> writes $workdir/server.pid, echoes port
start_server() {
    wd="$1"
    port=$(free_port)
    mkdir -p "$wd/data"
    "$BIN" start --port "$port" --host 127.0.0.1 --no-tls --data "$wd/data" \
        >"$wd/server.log" 2>&1 &
    echo $! > "$wd/server.pid"
    i=0
    while [ $i -lt 100 ]; do
        if grep -q "listening\|ready\|started" "$wd/server.log" 2>/dev/null; then break; fi
        # fall back to a TCP probe (log wording is not a contract)
        if node -e 'const n=require("net");const c=n.connect(process.argv[1],"127.0.0.1",()=>{c.end();process.exit(0)});c.on("error",()=>process.exit(1))' "$port" 2>/dev/null; then break; fi
        i=$((i+1)); sleep 0.1
    done
    echo "$port"
}

stop_server() {
    wd="$1"
    if [ -f "$wd/server.pid" ]; then
        kill "$(cat "$wd/server.pid")" 2>/dev/null
        wait "$(cat "$wd/server.pid")" 2>/dev/null
        rm -f "$wd/server.pid"
    fi
}

RESULTS=""
FAIL=0

record() {
    RESULTS="$RESULTS$1\t$2\n"
    if [ "$2" = "FAIL" ]; then FAIL=1; fi
    return 0
}

want() {
    [ -z "$ONLY" ] && return 0
    case "$ONLY" in *" $1"*|*"$1 "*|*" $1 "*|"$1") return 0 ;; esac
    echo "$ONLY" | grep -qw "$1"
}

TMP=$(mktemp -d)
trap 'for d in "$TMP"/*; do [ -d "$d" ] && stop_server "$d"; done; rm -rf "$TMP"' EXIT INT TERM

# ── Drizzle (drizzle-orm + postgres-js + drizzle-kit push) ──────────────────
if want drizzle; then
    if ! command -v npm >/dev/null 2>&1; then
        record drizzle SKIP
    else
        wd="$TMP/drizzle"; mkdir -p "$wd"
        echo "== drizzle: npm install =="
        (cd "$HERE/drizzle" && npm install --no-audit --no-fund >"$wd/npm.log" 2>&1)
        if [ $? -ne 0 ]; then
            echo "drizzle: npm install failed (offline?) — SKIP"; record drizzle SKIP
        else
            port=$(start_server "$wd")
            export DATABASE_URL="postgres://nucleus@127.0.0.1:$port/nucleus"
            echo "== drizzle: run (port $port) =="
            (cd "$HERE/drizzle" && node main.mjs)
            rc=$?
            stop_server "$wd"
            if [ $rc -eq 0 ]; then record drizzle PASS; else record drizzle FAIL; fi
        fi
    fi
fi

# ── Prisma (prisma db push + @prisma/client) ────────────────────────────────
if want prisma; then
    if ! command -v npm >/dev/null 2>&1; then
        record prisma SKIP
    else
        wd="$TMP/prisma"; mkdir -p "$wd"
        echo "== prisma: npm install =="
        (cd "$HERE/prisma" && npm install --no-audit --no-fund >"$wd/npm.log" 2>&1)
        if [ $? -ne 0 ]; then
            echo "prisma: npm install failed (offline?) — SKIP"; record prisma SKIP
        else
            port=$(start_server "$wd")
            export DATABASE_URL="postgres://nucleus@127.0.0.1:$port/nucleus"
            echo "== prisma: db push (port $port) =="
            (cd "$HERE/prisma" && npx prisma db push --skip-generate --accept-data-loss >"$wd/push.log" 2>&1 \
                && npx prisma generate >"$wd/generate.log" 2>&1 \
                && node main.mjs)
            rc=$?
            [ $rc -ne 0 ] && { echo "-- prisma push.log tail --"; tail -20 "$wd/push.log"; }
            stop_server "$wd"
            if [ $rc -eq 0 ]; then record prisma PASS; else record prisma FAIL; fi
        fi
    fi
fi

# ── SQLAlchemy (metadata.create_all + CRUD + reflection, psycopg v3) ────────
if want sqlalchemy; then
    if ! command -v python3 >/dev/null 2>&1; then
        record sqlalchemy SKIP
    else
        wd="$TMP/sqlalchemy"; mkdir -p "$wd"
        VENV="$HERE/sqlalchemy/.venv"
        if [ ! -x "$VENV/bin/python" ]; then
            echo "== sqlalchemy: creating venv =="
            python3 -m venv "$VENV" >"$wd/venv.log" 2>&1 \
                && "$VENV/bin/pip" install --quiet "sqlalchemy>=2" "psycopg[binary]>=3" >>"$wd/venv.log" 2>&1
        fi
        if ! "$VENV/bin/python" -c "import sqlalchemy, psycopg" >/dev/null 2>&1; then
            echo "sqlalchemy: toolchain unavailable (offline?) — SKIP"; record sqlalchemy SKIP
        else
            port=$(start_server "$wd")
            export DATABASE_URL="postgresql+psycopg://nucleus@127.0.0.1:$port/nucleus"
            echo "== sqlalchemy: run (port $port) =="
            "$VENV/bin/python" "$HERE/sqlalchemy/main.py"
            rc=$?
            stop_server "$wd"
            if [ $rc -eq 0 ]; then record sqlalchemy PASS; else record sqlalchemy FAIL; fi
        fi
    fi
fi

echo ""
echo "== ORM compat matrix =="
printf "$RESULTS" | while IFS="$(printf '\t')" read -r orm res; do
    [ -n "$orm" ] && printf "  %-12s %s\n" "$orm" "$res"
done
exit $FAIL
