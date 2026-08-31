#!/usr/bin/env bash
#
# nucleus-accessory-restore.sh — put a Nucleus accessory back onto a snapshot
# taken by nucleus-accessory-backup.sh. This is the rollback path.
#
# READ THIS BEFORE YOU RUN IT
#
#   Rolling an accessory back to an older engine by editing the image tag and
#   redeploying is NOT SAFE once the newer engine has served any traffic.
#   Measured, not theorised (2026-08-31, v0.1.8 vs HEAD c04a2a9f):
#
#     * A single KV_HSET under the newer build writes a record the older build
#       cannot checksum. On the next start the older build logs one ERROR,
#       marks the WHOLE KV model VOLATILE, and comes up serving — with every
#       key gone, including keys written long before the upgrade. It then
#       acknowledges new KV writes and loses them again on the next restart.
#     * Document and time-series writes made under the newer build are dropped
#       silently by the older build, with no log line at all.
#     * The newer build writes LSM SSTables with magic "LSM2". The older build
#       accepts only "LSMS" and errors on the whole store.
#
#   So the rollback is: STOP, RESTORE THE SNAPSHOT, START THE OLD IMAGE.
#   Everything written since the snapshot is lost. There is no merge path.
#   If that loss is unacceptable, the correct move is to stay on the new
#   version and fix forward.
#
# This script does the restore in the order that keeps you able to change your
# mind: it never deletes the post-upgrade directory, it moves it aside.
#
# USAGE
#
#   nucleus-accessory-restore.sh \
#       --container observe-nucleus \
#       --snapshot  /backups/nucleus/observe-nucleus-2026-08-31T0200 \
#       --image     ghcr.io/neutron-build/nucleus:v0.1.8
#
#   --dry-run   print what it would do and stop
#
# EXIT CODES
#   0  restored and serving
#   1  usage / precondition failure — nothing was touched
#   2  the restore failed; the original directory was moved aside, not deleted
#
set -euo pipefail

CONTAINER=""
SNAPSHOT=""
IMAGE=""
DRYRUN=0
YES=0

die()  { printf '\n[FAIL] %s\n' "$*" >&2; exit "${2:-1}"; }
note() { printf '[ %s ] %s\n' "$(date +%H:%M:%S)" "$*"; }
run()  { if [ "$DRYRUN" -eq 1 ]; then printf '    would run: %s\n' "$*"; else "$@"; fi; }

usage() { sed -n '2,40p' "$0" | sed 's/^#\{1,2\} \{0,1\}//'; exit 1; }

while [ $# -gt 0 ]; do
    case "$1" in
        -c|--container) CONTAINER="${2:?}"; shift 2 ;;
        -s|--snapshot)  SNAPSHOT="${2:?}"; shift 2 ;;
        -i|--image)     IMAGE="${2:?}"; shift 2 ;;
        --dry-run)      DRYRUN=1; shift ;;
        --yes)          YES=1; shift ;;
        -h|--help)      usage ;;
        *)              die "unknown argument: $1" ;;
    esac
done

[ -n "$CONTAINER" ] && [ -n "$SNAPSHOT" ] || usage
command -v docker >/dev/null || die "docker not on PATH"
docker inspect "$CONTAINER" >/dev/null 2>&1 || die "no such container: $CONTAINER"
[ -f "$SNAPSHOT/nucleus-backup.json" ] || die "$SNAPSHOT is not a nucleus snapshot (no nucleus-backup.json)"

DATA_SRC=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' "$CONTAINER")
DATA_KIND=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Type}}{{end}}{{end}}' "$CONTAINER")
CUR_IMAGE=$(docker inspect -f '{{.Config.Image}}' "$CONTAINER")
[ "$DATA_KIND" = "bind" ] || die "this script only handles a bind-mounted /data (got $DATA_KIND)"
[ -n "$DATA_SRC" ] || die "$CONTAINER has no /data bind mount"

IMAGE="${IMAGE:-$CUR_IMAGE}"

SNAP_VER=$(sed -n 's/.*"nucleus_version": *"\([^"]*\)".*/\1/p' "$SNAPSHOT/nucleus-backup.json" | head -1)
SNAP_FMT=$(sed -n 's/.*"format_version": *\([0-9]*\).*/\1/p' "$SNAPSHOT/nucleus-backup.json" | head -1)
SNAP_ID=$(sed -n 's/.*"database_id": *"\([^"]*\)".*/\1/p' "$SNAPSHOT/nucleus-backup.json" | head -1)
SNAP_TORN=$(grep -c '"taken_while_in_use": true' "$SNAPSHOT/nucleus-backup.json" || true)
SNAP_WHEN=$(sed -n 's/.*"created_unix": *\([0-9]*\).*/\1/p' "$SNAPSHOT/nucleus-backup.json" | head -1)

note "container        $CONTAINER"
note "data directory   $DATA_SRC"
note "restoring image  $IMAGE   (currently $CUR_IMAGE)"
note "snapshot         $SNAPSHOT"
note "  taken by       nucleus $SNAP_VER, on-disk format v$SNAP_FMT"
note "  database_id    $SNAP_ID"
note "  taken at       $(date -d "@$SNAP_WHEN" 2>/dev/null || date -r "$SNAP_WHEN" 2>/dev/null || echo "unix $SNAP_WHEN")"

[ "$SNAP_TORN" = "0" ] || die "this snapshot is marked taken_while_in_use — it is TORN. Do not restore it." 1

if [ -f "$DATA_SRC/nucleus.id" ]; then
    LIVE_ID=$(tr -d '\n' < "$DATA_SRC/nucleus.id")
    if [ "$LIVE_ID" != "$SNAP_ID" ]; then
        die "database_id mismatch: the live directory is $LIVE_ID, the snapshot is $SNAP_ID. This snapshot belongs to a DIFFERENT database. Stop." 1
    fi
    note "  database_id matches the live directory"
fi

cat <<EOF

  ------------------------------------------------------------------
  EVERY WRITE MADE SINCE $(date -d "@$SNAP_WHEN" 2>/dev/null || date -r "$SNAP_WHEN" 2>/dev/null || echo "the snapshot") WILL BE LOST.
  The current directory is moved to $DATA_SRC.pre-restore-<stamp>, not
  deleted, so the post-upgrade writes remain recoverable by someone
  who fixes forward later. Free space must allow BOTH to exist.
  ------------------------------------------------------------------

EOF

if [ "$DRYRUN" -eq 0 ] && [ "$YES" -eq 0 ]; then
    printf 'Type the container name to proceed: '
    read -r CONFIRM
    [ "$CONFIRM" = "$CONTAINER" ] || die "not confirmed; nothing was touched" 1
fi

STAMP=$(date +%Y-%m-%dT%H%M%S)
ASIDE="$DATA_SRC.pre-restore-$STAMP"

# Space: the moved-aside copy and the restored copy coexist.
SRC_KB=$(du -sk "$DATA_SRC" | awk '{print $1}')
FREE_KB=$(df -Pk "$(dirname "$DATA_SRC")" | awk 'NR==2{print $4}')
note "current data ~$((SRC_KB/1024)) MiB, free ~$((FREE_KB/1024)) MiB"
[ "$FREE_KB" -gt "$SRC_KB" ] || die "not enough free space to keep the current directory AND restore beside it" 1

note "stopping $CONTAINER"
run docker stop -t 60 "$CONTAINER"

note "moving the current directory aside -> $ASIDE"
run mv "$DATA_SRC" "$ASIDE"
run mkdir -p "$DATA_SRC"

note "restoring the snapshot (as root inside the image, then chown to 10001)"
if [ "$DRYRUN" -eq 0 ]; then
    if ! docker run --rm -u 0 \
            -v "$(dirname "$SNAPSHOT")":/snapsrc:ro \
            -v "$DATA_SRC":/restore \
            --entrypoint nucleus "$IMAGE" \
            restore --input "/snapsrc/$(basename "$SNAPSHOT")" --data /restore/data --force
    then
        note "restore FAILED. Your original directory is intact at:"
        note "  $ASIDE"
        note "Put it back with:  rmdir '$DATA_SRC' && mv '$ASIDE' '$DATA_SRC'"
        die "restore failed" 2
    fi
    # `nucleus restore` refuses to write directly into a bind-mount point
    # (EBUSY on removing it), so it restored into a subdirectory. Flatten it.
    ( shopt -s dotglob; mv "$DATA_SRC"/data/* "$DATA_SRC"/ )
    rmdir "$DATA_SRC/data"
    docker run --rm -u 0 -v "$DATA_SRC":/d --entrypoint chown "$IMAGE" -R 10001:10001 /d
else
    printf '    would run: docker run --rm -u 0 ... nucleus restore --input %s --data /restore/data --force\n' "$SNAPSHOT"
fi

if [ "$DRYRUN" -eq 1 ]; then
    note "dry run: stopping here. Nothing was changed."
    exit 0
fi

# If the image being restored differs from the one the container is pinned to,
# say so loudly — the container will start on ITS pin, not on --image.
if [ "$IMAGE" != "$CUR_IMAGE" ]; then
    note "NOTE: $CONTAINER is pinned to $CUR_IMAGE, but you restored with $IMAGE."
    note "      Starting it now runs $CUR_IMAGE against this directory."
    note "      Edit the teploy.yml pin and redeploy instead of starting it here"
    note "      if $CUR_IMAGE is the version you are rolling AWAY from."
    note "      Not starting the container. Data directory is restored and ready."
    exit 0
fi

note "starting $CONTAINER"
docker start "$CONTAINER" >/dev/null

note "waiting for it to serve"
for _ in $(seq 1 120); do
    sleep 1
    docker exec "$CONTAINER" nucleus status --host 127.0.0.1:5432 >/dev/null 2>&1 && break
done

note "--- boot log: read every ERROR and WARN ---"
docker logs --tail 60 "$CONTAINER" 2>&1 | grep -iE "error|warn|volatile|checksum|engine|recover" | sed 's/^/    /'

cat <<EOF

[ done ] Restored. Two things remain YOURS to do:

  1. Verify at the application level, not just that the port answers.
     A "VOLATILE" line above means a model failed to open and the engine is
     serving anyway — that is the failure this whole script exists to avoid.

  2. The pre-restore directory is at
       $ASIDE
     It is the only copy of everything written after the snapshot. Keep it
     until you are certain you do not want it, then remove it to reclaim
     $((SRC_KB/1024)) MiB.

EOF
