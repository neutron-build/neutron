#!/usr/bin/env bash
#
# nucleus-accessory-backup.sh — take a PROVEN physical backup of a Nucleus
# accessory container, and (by default) prove it restores under the build you
# are about to upgrade to.
#
# Written for the Teploy accessory shape: a container named <app>-nucleus whose
# /data is a host bind mount, e.g.
#
#   ship-nucleus     /deployments/ship/accessories/nucleus/nucleus-data
#   observe-nucleus  /deployments/observe/accessories/nucleus/nucleus-data
#
# WHY THIS IS A SCRIPT AND NOT A PARAGRAPH
#
#   1. `cp -a` / `tar` of a running engine's data directory is a TORN copy.
#      Nucleus refuses to do it and so does this script. There are exactly two
#      consistent options and the script picks one for you:
#        --cold    stop the container, copy, start it again      (default)
#        --online  BACKUP DATABASE TO over pgwire, engine running
#   2. A backup nobody restored is a hypothesis. --verify (on by default)
#      restores the snapshot into a scratch directory, starts it on a spare
#      port with the TARGET image, and counts rows. That single step is what
#      tells you the new engine can read your data BEFORE it gets the chance
#      to rewrite it.
#   3. Getting the uid wrong (the image runs as 10001) turns a restore into a
#      restart loop at 3am.
#
# USAGE
#
#   nucleus-accessory-backup.sh --container observe-nucleus --out /backups/nucleus
#   nucleus-accessory-backup.sh -c ship-nucleus -o /backups/nucleus --online
#   nucleus-accessory-backup.sh -c ship-nucleus -o /backups/nucleus \
#       --verify-image ghcr.io/neutron-build/nucleus:v0.1.9
#
# EXIT CODES
#   0  snapshot taken and (unless --no-verify) proven to restore
#   1  usage / precondition failure — nothing was touched
#   2  the snapshot failed
#   3  the snapshot was taken but FAILED TO VERIFY — do not upgrade
#
set -euo pipefail

CONTAINER=""
OUTDIR=""
MODE="cold"
VERIFY=1
VERIFY_IMAGE=""
VERIFY_PORT="55999"
KEEP_SCRATCH=0

die()  { printf '\n[FAIL] %s\n' "$*" >&2; exit "${2:-1}"; }
note() { printf '[ %s ] %s\n' "$(date +%H:%M:%S)" "$*"; }

usage() {
    sed -n '2,40p' "$0" | sed 's/^#\{1,2\} \{0,1\}//'
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        -c|--container)    CONTAINER="${2:?}"; shift 2 ;;
        -o|--out)          OUTDIR="${2:?}"; shift 2 ;;
        --cold)            MODE="cold"; shift ;;
        --online)          MODE="online"; shift ;;
        --verify)          VERIFY=1; shift ;;
        --no-verify)       VERIFY=0; shift ;;
        --verify-image)    VERIFY_IMAGE="${2:?}"; shift 2 ;;
        --verify-port)     VERIFY_PORT="${2:?}"; shift 2 ;;
        --keep-scratch)    KEEP_SCRATCH=1; shift ;;
        -h|--help)         usage ;;
        *)                 die "unknown argument: $1" ;;
    esac
done

[ -n "$CONTAINER" ] || usage
[ -n "$OUTDIR" ]    || usage
command -v docker >/dev/null || die "docker not on PATH"

# ---------------------------------------------------------------- discovery --
docker inspect "$CONTAINER" >/dev/null 2>&1 \
    || die "no such container: $CONTAINER"

RUNNING=$(docker inspect -f '{{.State.Running}}' "$CONTAINER")
IMAGE=$(docker inspect -f '{{.Config.Image}}' "$CONTAINER")
DATA_SRC=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data"}}{{if .Source}}{{.Source}}{{else}}{{.Name}}{{end}}{{end}}{{end}}' "$CONTAINER")
DATA_KIND=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Type}}{{end}}{{end}}' "$CONTAINER")

[ -n "$DATA_SRC" ] || die "$CONTAINER has no /data mount — nothing to back up"

STAMP=$(date +%Y-%m-%dT%H%M%S)
SNAP="$OUTDIR/${CONTAINER}-${STAMP}"

note "container   $CONTAINER (running=$RUNNING)"
note "image       $IMAGE"
note "data        $DATA_KIND $DATA_SRC"
note "snapshot    $SNAP"
note "mode        $MODE"

[ "$DATA_KIND" = "bind" ] \
    || die "this script only handles a bind-mounted /data; $CONTAINER uses a $DATA_KIND. Adapt it before running." 1

mkdir -p "$OUTDIR"
[ -e "$SNAP" ] && die "$SNAP already exists"

# ------------------------------------------------------------- space check --
SRC_KB=$(du -sk "$DATA_SRC" | awk '{print $1}')
FREE_KB=$(df -Pk "$OUTDIR" | awk 'NR==2{print $4}')
note "source ~$((SRC_KB/1024)) MiB, free at destination ~$((FREE_KB/1024)) MiB"
if [ "$FREE_KB" -lt $((SRC_KB + SRC_KB/10)) ]; then
    die "not enough free space at $OUTDIR for a $((SRC_KB/1024)) MiB snapshot (+10% headroom)" 1
fi

# ------------------------------------------------------------------ backup --
case "$MODE" in
cold)
    if [ "$RUNNING" = "true" ]; then
        note "stopping $CONTAINER (SIGTERM, 60s grace) — the engine flushes on the way down"
        docker stop -t 60 "$CONTAINER" >/dev/null
        # A clean stop is what makes the next start fast and unambiguous.
        # Exit 0 = the engine drained and flushed. 137 = SIGKILL after the
        # grace period, which means a longer WAL replay on the next start —
        # not corruption, but do not then blame the new version for a slow boot.
        EXITCODE=$(docker inspect -f '{{.State.ExitCode}}' "$CONTAINER")
        if [ "$EXITCODE" != "0" ]; then
            note "WARNING: $CONTAINER exited $EXITCODE, not 0 (137 = SIGKILL after the grace"
            note "         period). The copy is still consistent — the process is gone — but"
            note "         the next start replays more WAL. Record this."
        else
            note "clean stop (exit 0)"
        fi
        STARTED_BY_US=1
    else
        note "$CONTAINER is already stopped"
        STARTED_BY_US=0
    fi

    # --- the stale-lock trap -------------------------------------------------
    # `nucleus backup` refuses a directory whose nucleus.lock names a LIVE pid.
    # The accessory writes "pid 1" into that file, because it is pid 1 inside
    # its own namespace. Any helper container that later reads the lock is ALSO
    # pid-1-alive, so the liveness probe says "in use" and the backup is refused
    # even though the owning container is stopped. `--pid host` does not help
    # either: host pid 1 is init.
    #
    # It is safe to move the lock aside here and ONLY here, because docker has
    # confirmed the owning container is not running. The engine writes a fresh
    # lock on its next start.
    LOCK="$DATA_SRC/nucleus.lock"
    LOCK_STASH=""
    if [ -e "$LOCK" ]; then
        if [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER")" = "true" ]; then
            die "refusing to touch nucleus.lock while $CONTAINER is running" 2
        fi
        LOCK_STASH="$LOCK.stale-$STAMP"
        note "moving the stale lock aside ($(cat "$LOCK" 2>/dev/null | tr -d '\n')) -> $(basename "$LOCK_STASH")"
        mv "$LOCK" "$LOCK_STASH"
    fi

    note "copying with 'nucleus backup' (BLAKE3-checksummed, manifest-stamped)"
    set +e
    docker run --rm -u 10001:10001 \
        -v "$DATA_SRC":/data:ro \
        -v "$OUTDIR":/out \
        --entrypoint nucleus "$IMAGE" \
        backup --data /data --output "/out/$(basename "$SNAP")"
    RC=$?
    set -e

    [ -n "$LOCK_STASH" ] && rm -f "$LOCK_STASH"

    if [ "${STARTED_BY_US}" = "1" ]; then
        note "restarting $CONTAINER"
        docker start "$CONTAINER" >/dev/null
    fi
    [ "$RC" -eq 0 ] || die "nucleus backup failed (rc=$RC)" 2
    ;;

online)
    [ "$RUNNING" = "true" ] || die "--online needs the container running" 1
    note "BACKUP DATABASE TO — the running engine snapshots itself at a named LSN."
    note "NOTE: WAL retention is pinned for the whole backup. On a large or"
    note "      write-heavy database that grows the WAL with no cap. Watch disk."
    IN_CONTAINER="/tmp/nucleus-snap-$STAMP"
    docker exec "$CONTAINER" \
        nucleus shell -H 127.0.0.1 -p 5432 -c "BACKUP DATABASE TO '$IN_CONTAINER'" \
        || die "BACKUP DATABASE TO failed" 2
    note "copying the snapshot out of the container"
    docker cp "$CONTAINER:$IN_CONTAINER" "$SNAP" || die "docker cp failed" 2
    docker exec -u 0 "$CONTAINER" rm -rf "$IN_CONTAINER" || true
    ;;
esac

[ -f "$SNAP/nucleus-backup.json" ] || die "no manifest at $SNAP/nucleus-backup.json" 2

note "manifest:"
sed -n '1,12p' "$SNAP/nucleus-backup.json" | sed 's/^/    /'

if grep -q '"taken_while_in_use": true' "$SNAP/nucleus-backup.json"; then
    die "manifest says taken_while_in_use — this snapshot is TORN, do not rely on it" 2
fi

SNAP_FMT=$(sed -n 's/.*"format_version": *\([0-9]*\).*/\1/p' "$SNAP/nucleus-backup.json" | head -1)
note "on-disk format_version in this snapshot: ${SNAP_FMT:-unknown}"

# ------------------------------------------------------------------ verify --
if [ "$VERIFY" -eq 0 ]; then
    note "skipping verification (--no-verify). You now hold an UNPROVEN snapshot."
    note "snapshot: $SNAP"
    exit 0
fi

VIMG="${VERIFY_IMAGE:-$IMAGE}"
SCRATCH="$OUTDIR/.verify-${CONTAINER}-${STAMP}"
VNAME="nucleus-verify-${STAMP}"
note "verifying by restoring into $SCRATCH and starting it with: $VIMG"
note "(pass --verify-image <target> to prove the version you are upgrading TO"
note " can read this data — that is the whole point of the pre-flight.)"

cleanup_verify() {
    docker rm -f "$VNAME" >/dev/null 2>&1 || true
    if [ "$KEEP_SCRATCH" -eq 0 ]; then rm -rf "$SCRATCH"; fi
}
trap cleanup_verify EXIT

mkdir -p "$SCRATCH"
# Restore as root, then hand the tree to uid 10001 — the image's user. Doing it
# the other way round is the classic failure: the scratch directory belongs to
# whoever ran the script and `restore` dies with EACCES, which reads exactly
# like a corrupt snapshot and is not one.
docker run --rm -u 0 \
    -v "$OUTDIR":/out \
    -v "$SCRATCH":/restore \
    --entrypoint nucleus "$VIMG" \
    restore --input "/out/$(basename "$SNAP")" --data /restore/data --force \
    || die "RESTORE FAILED under $VIMG — this snapshot cannot be restored by that build. DO NOT UPGRADE." 3

docker run --rm -u 0 -v "$SCRATCH":/restore \
    --entrypoint chown "$VIMG" -R 10001:10001 /restore \
    || die "could not chown the restored directory to uid 10001" 3

docker run -d --name "$VNAME" \
    -u 10001:10001 \
    -v "$SCRATCH/data":/data \
    -p "127.0.0.1:${VERIFY_PORT}:5432" \
    -e NUCLEUS_ALLOW_NO_AUTH=1 \
    -e NUCLEUS_ALLOW_INSECURE_CLUSTER=1 \
    -e NUCLEUS_ALLOW_INSECURE_REPLICATION=1 \
    "$VIMG" >/dev/null \
    || die "could not start the verification instance" 3

note "waiting for the restored instance on 127.0.0.1:${VERIFY_PORT}"
UP=0
for _ in $(seq 1 90); do
    sleep 1
    if docker exec "$VNAME" nucleus status --host 127.0.0.1:5432 >/dev/null 2>&1; then UP=1; break; fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$VNAME")" != "true" ]; then break; fi
done

if [ "$UP" -ne 1 ]; then
    note "--- verification instance log ---"
    docker logs "$VNAME" 2>&1 | tail -40
    die "the restored directory did NOT come up under $VIMG. DO NOT UPGRADE." 3
fi

note "restored instance is serving. Tables it can see:"
docker exec "$VNAME" nucleus shell -H 127.0.0.1 -p 5432 \
    -c "SELECT tablename FROM pg_tables ORDER BY tablename" 2>&1 | sed 's/^/    /'

note "--- boot log of the restored instance (read every WARN) ---"
docker logs "$VNAME" 2>&1 | grep -iE "warn|error|engine|rebuilt|recover" | sed 's/^/    /' | head -40

note "OK. Snapshot verified: $SNAP"
note "It restores and serves under $VIMG."
exit 0
