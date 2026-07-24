#!/bin/sh
# PgBouncer pooler harness — boots a release Nucleus, fronts it with PgBouncer
# in BOTH pooling modes, and exercises the behaviors poolers stress:
#
#   session mode:     connect/disconnect churn + server_reset_query (DISCARD ALL)
#   transaction mode: server connection swapping between clients mid-session,
#                     protocol-level prepared statements (max_prepared_statements)
#
# Exit non-zero on FAIL; SKIP (exit 0) when pgbouncer/psql are unavailable.
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
NUCLEUS_DIR=$(cd "$HERE/../.." && pwd)
BIN="$NUCLEUS_DIR/target/release/nucleus"

BUILD=1
for a in "$@"; do
    case "$a" in
        --no-build) BUILD=0 ;;
        *) echo "unknown flag: $a" >&2; exit 2 ;;
    esac
done

command -v pgbouncer >/dev/null 2>&1 || { echo "pooler: no pgbouncer — SKIP"; exit 0; }
command -v psql >/dev/null 2>&1 || { echo "pooler: no psql — SKIP"; exit 0; }

if [ "$BUILD" = 1 ]; then
    echo "== building release nucleus =="
    (cd "$NUCLEUS_DIR" && cargo build --release 2>&1 | tail -1) || exit 1
fi
[ -x "$BIN" ] || { echo "missing $BIN" >&2; exit 1; }

TMP=$(mktemp -d)
cleanup() {
    [ -f "$TMP/pgbouncer.pid" ] && kill "$(cat "$TMP/pgbouncer.pid")" 2>/dev/null
    if [ -f "$TMP/server.pid" ]; then
        kill "$(cat "$TMP/server.pid")" 2>/dev/null
        wait "$(cat "$TMP/server.pid")" 2>/dev/null
    fi
    rm -rf "$TMP"
}
trap cleanup EXIT INT TERM

free_port() {
    python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'
}

DB_PORT=$(free_port)
mkdir -p "$TMP/data"
"$BIN" start --port "$DB_PORT" --host 127.0.0.1 --no-tls --data "$TMP/data" \
    >"$TMP/server.log" 2>&1 &
echo $! > "$TMP/server.pid"
i=0
while [ $i -lt 100 ]; do
    if python3 -c "import socket;socket.create_connection(('127.0.0.1',$DB_PORT),0.2).close()" 2>/dev/null; then break; fi
    i=$((i+1)); sleep 0.1
done

FAIL=0
run_mode() {
    mode="$1"
    BOUNCER_PORT=$(free_port)
    cat > "$TMP/pgbouncer-$mode.ini" <<EOF
[databases]
nucleus = host=127.0.0.1 port=$DB_PORT dbname=nucleus
[pgbouncer]
listen_addr = 127.0.0.1
listen_port = $BOUNCER_PORT
auth_type = trust
auth_file = $TMP/userlist.txt
pool_mode = $mode
max_client_conn = 20
default_pool_size = 2
max_prepared_statements = 100
logfile = $TMP/pgbouncer-$mode.log
pidfile = $TMP/pgbouncer-$mode.pid
EOF
    echo '"nucleus" ""' > "$TMP/userlist.txt"
    pgbouncer -d "$TMP/pgbouncer-$mode.ini" 2>>"$TMP/pgbouncer-$mode.log" || {
        echo "pooler[$mode]: pgbouncer failed to start"; FAIL=1; return
    }
    cp "$TMP/pgbouncer-$mode.pid" "$TMP/pgbouncer.pid" 2>/dev/null
    i=0
    while [ $i -lt 50 ]; do
        if python3 -c "import socket;socket.create_connection(('127.0.0.1',$BOUNCER_PORT),0.2).close()" 2>/dev/null; then break; fi
        i=$((i+1)); sleep 0.1
    done

    P="psql -h 127.0.0.1 -p $BOUNCER_PORT -U nucleus -d nucleus -v ON_ERROR_STOP=1 -qtA"

    echo "== pooler[$mode]: basic + DDL/DML =="
    $P -c "DROP TABLE IF EXISTS pool_$mode" \
       -c "CREATE TABLE pool_$mode (id INT PRIMARY KEY, v TEXT)" \
       -c "INSERT INTO pool_$mode VALUES (1,'a'),(2,'b')" \
       -c "SELECT count(*) FROM pool_$mode" > "$TMP/out1" 2>"$TMP/err1"
    if [ $? -ne 0 ] || [ "$(cat "$TMP/out1")" != "2" ]; then
        echo "pooler[$mode]: basic FAIL"; cat "$TMP/err1"; FAIL=1
    fi

    echo "== pooler[$mode]: explicit transaction =="
    $P <<SQL > "$TMP/out2" 2>"$TMP/err2"
BEGIN;
INSERT INTO pool_$mode VALUES (3,'c');
COMMIT;
BEGIN;
INSERT INTO pool_$mode VALUES (4,'d');
ROLLBACK;
SELECT count(*) FROM pool_$mode;
SQL
    if [ $? -ne 0 ] || [ "$(tail -1 "$TMP/out2")" != "3" ]; then
        echo "pooler[$mode]: transaction FAIL"; cat "$TMP/err2"; FAIL=1
    fi

    echo "== pooler[$mode]: connection churn (20 sequential clients) =="
    k=0; churn_fail=0
    while [ $k -lt 20 ]; do
        out=$($P -c "SELECT $k + 1" 2>>"$TMP/err3") || churn_fail=1
        [ "$out" != "$((k + 1))" ] && churn_fail=1
        k=$((k+1))
    done
    if [ "$churn_fail" != 0 ]; then
        echo "pooler[$mode]: churn FAIL"; tail -5 "$TMP/err3"; FAIL=1
    fi

    echo "== pooler[$mode]: concurrent clients (5 parallel, pool of 2) =="
    rm -f "$TMP/conc_fail"
    j=0
    CONC_PIDS=""
    while [ $j -lt 5 ]; do
        (
            out=$($P -c "BEGIN" -c "INSERT INTO pool_$mode VALUES (100+$j, 'x')" -c "COMMIT" -c "SELECT 1" 2>>"$TMP/err4")
            [ "$out" != "1" ] && touch "$TMP/conc_fail"
        ) &
        CONC_PIDS="$CONC_PIDS $!"
        j=$((j+1))
    done
    # Explicit PIDs: a bare `wait` would also wait on the nucleus server.
    wait $CONC_PIDS
    count=$($P -c "SELECT count(*) FROM pool_$mode WHERE id >= 100" 2>>"$TMP/err4")
    if [ -f "$TMP/conc_fail" ] || [ "$count" != "5" ]; then
        echo "pooler[$mode]: concurrency FAIL (count=$count)"; tail -5 "$TMP/err4"; FAIL=1
    fi

    if [ "$mode" = "transaction" ]; then
        echo "== pooler[$mode]: extended-protocol prepared statements across txns =="
        VENV="$NUCLEUS_DIR/compat/orm/sqlalchemy/.venv"
        if [ -x "$VENV/bin/python" ] && "$VENV/bin/python" -c "import psycopg" 2>/dev/null; then
            "$VENV/bin/python" "$HERE/prepared_txn.py" "127.0.0.1" "$BOUNCER_PORT" > "$TMP/out5" 2>&1
            if [ $? -ne 0 ]; then
                echo "pooler[$mode]: prepared FAIL"; cat "$TMP/out5"; FAIL=1
            fi
        else
            echo "pooler[$mode]: psycopg venv unavailable — prepared-statement leg skipped"
        fi
    fi

    # Server-connection reset: after all clients disconnect, pgbouncer issues
    # server_reset_query (session mode default: DISCARD ALL) on the idle
    # server connection. A failing reset kills the connection — visible as
    # close/error lines in the pgbouncer log.
    sleep 1
    if grep -iE "discard|reset" "$TMP/pgbouncer-$mode.log" | grep -qiE "fail|error|bad"; then
        echo "pooler[$mode]: server reset query failed on Nucleus"; FAIL=1
    fi

    kill "$(cat "$TMP/pgbouncer-$mode.pid" 2>/dev/null)" 2>/dev/null
    sleep 0.3
    echo "pooler[$mode]: done"
}

run_mode session
run_mode transaction

if [ "$FAIL" = 0 ]; then
    echo "pooler: PASS"
else
    echo "-- server.log tail --"; tail -10 "$TMP/server.log"
    echo "pooler: FAIL"
fi
exit $FAIL
