# Deployment paths

Three supported shapes: a container, a systemd unit, and a single-instance k3s
StatefulSet. All three run **one** Nucleus process. Distributed mode is
incomplete and unsupported (`DATABASE_COMPLETION.md` Milestone 9) — none of
these files scales past one instance, on purpose.

## What has actually been verified

Written is not validated. This table is the honest state as of 2026-08-26; do
not upgrade a row without running the command in the last column.

| Artifact | Verified | Not verified | How to close the gap |
|---|---|---|---|
| `nucleus/Dockerfile` | **Built, run, and smoke-tested 2026-08-24** (podman machine, applehv, 6 CPU/16 GiB) and **again 2026-08-26 natively on arm64** (podman 6.1 on an M-series host, no QEMU): image builds end-to-end from source; container boots with `NUCLEUS_PASSWORD` alone; serves pgwire DDL/DML/query; graceful SIGTERM shutdown ("Data flushed to disk successfully"); data survives a restart on a named volume | The multi-arch/QEMU path (single arm64 host) | Run the release job's multi-arch build |
| `nucleus/Dockerfile.dist` | **Built and run 2026-08-26, linux/arm64 natively**: built with a bookworm-compiled arm64 binary (extracted from the source image's builder stage — the binary's maximum glibc symbol requirement is **GLIBC_2.34**, vs bookworm's 2.36; v0.1.8's arm64 failure was a 2.38 requirement from an ubuntu-24.04-era builder, fixed by the release job's ubuntu-22.04 pin); image boots with `NUCLEUS_PASSWORD`, serves pgwire DDL/DML, SIGTERM flushes cleanly | The amd64 dist build (needs an x86_64 Linux binary; the release workflow produces it on its pinned runner) | Run the release workflow |
| `nucleus/.dockerignore` | Reduces the build context to ~11 MB | — | — |
| `deploy/systemd/nucleus.service` | **Loaded and run by a real systemd 252 (bookworm) on 2026-08-26**, inside a privileged container on the arm64 host: `daemon-reload` + `enable --now` brings the unit to `active (running)` WITH the entire hardening block enabled (NoNewPrivileges, ProtectSystem=strict, MemoryDenyWriteExecute, SystemCallFilter=@system-service, CapabilityBoundingSet= — none of it blocks startup); serves pgwire DDL/DML on 127.0.0.1:5432; `systemctl stop` drains and flushes ("Data flushed to disk successfully"); a second `start` serves again from the same StateDirectory | Nothing material. (A container is not a bare-metal host: the journal, cgroup and udev surfaces differ slightly.) | Optionally re-run the same sequence on a bare-metal Linux host |
| `deploy/k3s/*.yaml` | `kubeconform -strict -kubernetes-version 1.31.0` — 5 resources, 0 invalid | **Still never applied to a cluster.** Two attempts to run k3s inside the arm64 podman machine failed on environment limits, not the manifests: `--privileged` alone dies at "failed to find cpuset cgroup (v2)", `--cgroupns=host` dies at `open /dev/kmsg: operation not permitted` (the nested-VM device set). Schema-valid is not the same as working: probes, fsGroup/PV permissions and the graceful-shutdown window are all untested | Apply to a k3s cluster on real hardware (the H9 Proxmox lane) |

2026-08-24 findings from the runtime validation (all fixed the same day):

1. **The image could not boot at all.** `start --host 0.0.0.0` (required in any
   container) tripped the cluster/replication token guards even with zero
   clustering configured — the documented `docker run` failed at step one. The
   guards and the listeners are now engagement-gated (`--join`/`--replicate-from`
   or config-driven replica); a single-node server no longer listens on the
   cluster port at all.
2. **The replication listener skipped auth when no token was set** — an
   unauthenticated listener that would hand any caller the full WAL stream on a
   non-loopback bind. Now fail-closed: no token configured means inbound
   replicas are refused outright.
3. **The bootstrap role is `nucleus`, not `postgres`** — the connection
   examples below said `postgres://postgres:...`, which fails auth. Fixed below.
4. **`HEALTHCHECK` is silently dropped when the image is built in OCI format**
   (podman's default): `docker inspect .State.Health.Status` returns nothing.
   Build with `--format docker` if you rely on Docker health status.

Multi-node containers (2026-08-25): the engagement gate above models only the
OUTBOUND direction (`--join`/`--replicate-from`). The node others join — the
seed, first up, joining nothing — also has to listen, or every `--join` against
it fails and the joiners silently serve standalone. A node declares the inbound
role explicitly; it stays OFF by default so a single-node container still opens
no cluster port:

- `--cluster-listen` (or `NUCLEUS_CLUSTER_LISTEN=1`): listen on the cluster
  port (`--cluster-port`, default 5433) so other nodes can `--join` this one.
- A non-loopback listener still requires `NUCLEUS_CLUSTER_TOKEN` (same refusal
  as the outbound guards); loopback is exempt for local development.
- Flag/env naming is provisional — escalate before engraving it in orchestration
  tooling.

Why the container was not built before 2026-08-24: the development machine's
podman VM was faulting in July (overlay mount I/O errors — since healed), and in
August the build was OOM-killed at the final crate: the machine's default 4 GiB
cannot compile this crate. Raised to 6 CPU / 16 GiB
(`podman machine set --cpus 6 --memory 16384`) for the validated build above.

## Container

```bash
# From the repository root. BuildKit is required (buildx, or podman/buildah):
# the builder stage uses `--mount=type=cache`, which the legacy docker builder
# does not understand. The in-container release compile needs ~16 GiB of VM
# memory (see above).
docker buildx build -t nucleus:dev ./nucleus

docker run --rm -d --name nucleus \
  -p 5432:5432 \
  -e NUCLEUS_PASSWORD=changeme \
  -v nucleus-data:/data \
  nucleus:dev

# The bootstrap role is "nucleus" (NOT postgres):
psql "postgres://nucleus:changeme@127.0.0.1:5432/nucleus" -c 'SELECT 1'
```

Properties worth knowing:

- Runs as **uid/gid 10001**, not root. A host bind-mount must be chowned to
  10001 in advance, or the server cannot create its data directory.
- `HEALTHCHECK` is **liveness only**. `nucleus status` opens a TCP connection
  and reports success — it does not authenticate and does not run a query, so a
  healthy container is one that is *listening*, not one that can serve reads.
- The default `CMD` binds `0.0.0.0`. A non-loopback bind with no password
  refuses to start unless `NUCLEUS_ALLOW_NO_AUTH=1` is set. Do not set it
  outside a throwaway dev network.
- `Dockerfile` compiles from source. `Dockerfile.dist` packages a prebuilt
  binary from `dist/<arch>/nucleus` and is what the release workflow uses for
  multi-arch, because compiling this crate under QEMU takes 45-90 minutes per
  foreign architecture.

Acceptance sequence once a working container runtime is available:

```bash
docker buildx build -t nucleus:dev ./nucleus
docker run --rm -d --name nucleus -p 5432:5432 \
  -e NUCLEUS_PASSWORD=changeme -v nucleus-data:/data nucleus:dev
# 1. becomes healthy (only with --format docker builds; OCI images drop HEALTHCHECK)
docker inspect --format '{{.State.Health.Status}}' nucleus
# 2. serves a real query (bootstrap role is "nucleus", NOT postgres)
psql "postgres://nucleus:changeme@127.0.0.1:5432/nucleus" -c 'SELECT 1'
# 3. survives a restart with its data
psql "postgres://nucleus:changeme@127.0.0.1:5432/nucleus" -c 'CREATE TABLE t(id INT PRIMARY KEY); INSERT INTO t VALUES (1);'
docker restart nucleus && sleep 15
psql "postgres://nucleus:changeme@127.0.0.1:5432/nucleus" -c 'SELECT * FROM t'            # must return the row
# 4. does not run as root
docker exec nucleus id                   # uid=10001
```

Steps 2 and 3 (and graceful SIGTERM shutdown) were executed against a real
container on 2026-08-24; step 1 was verified to be a no-op under podman's OCI
format, which is why it carries the caveat above.

## systemd

Target: a Debian host or VM. The same box can carry the Forgejo Actions runner
(`.forgejo/README.md`).

```bash
install -m 0755 nucleus /usr/local/bin/nucleus
groupadd --system nucleus && useradd --system -g nucleus -s /usr/sbin/nologin -M nucleus
install -d -m 0750 -o root -g nucleus /etc/nucleus
install -m 0640 -o root -g nucleus deploy/systemd/nucleus.env.example /etc/nucleus/nucleus.env
$EDITOR /etc/nucleus/nucleus.env          # set NUCLEUS_PASSWORD
install -m 0644 deploy/systemd/nucleus.service /etc/systemd/system/nucleus.service
systemctl daemon-reload
systemctl enable --now nucleus
```

Acceptance sequence — run every step; the hardening block makes several of
these genuinely capable of failing:

```bash
systemd-analyze verify /etc/systemd/system/nucleus.service   # static check
systemctl start nucleus && systemctl is-active nucleus
journalctl -u nucleus -n 50 --no-pager                       # listener bound?
nucleus status --host 127.0.0.1:5432

# Graceful stop: the drain is 2 s, the flush after it is unbounded.
# Time it, and check the journal for "Shutdown drain timed out".
time systemctl stop nucleus

systemctl restart nucleus                # data survives
reboot                                   # unit comes back after a real boot

# PITR: ProtectSystem=strict makes everything outside /var/lib/nucleus
# read-only. If NUCLEUS_WAL_ARCHIVE_DIR is set, archiving fails with EROFS
# until that path is added to ReadWritePaths= in the unit.
systemd-analyze security nucleus.service
```

If startup fails, comment out one hardening directive at a time rather than
deleting the block — `SystemCallFilter=@system-service` and
`MemoryDenyWriteExecute=yes` are the two most likely culprits (the latter
interacts badly with some JIT/allocator configurations).

`Type=simple`, not `notify`: Nucleus has no `sd_notify` support, so systemd
reports the unit started as soon as it forks, before the listener is accepting.
Anything ordered after it must poll `nucleus status`, not rely on `After=`.

## k3s

k3s over managed Kubernetes: one-command install on a Proxmox VM, a working
default `local-path` StorageClass, and — unlike kind or k3d-in-Docker — real
PersistentVolume semantics, which is the only part of this a database cares
about.

```bash
curl -sfL https://get.k3s.io | sh -
kubectl create namespace nucleus
kubectl -n nucleus create secret generic nucleus \
  --from-literal=password="$(head -c 32 /dev/urandom | base64)"
kubectl -n nucleus apply -f deploy/k3s/nucleus.yaml
```

Acceptance sequence:

```bash
kubectl -n nucleus rollout status statefulset/nucleus --timeout=10m
kubectl -n nucleus get pvc                      # Bound, not Pending
kubectl -n nucleus exec -it nucleus-0 -- nucleus status --host 127.0.0.1:5432

# The one that actually matters: does data survive pod replacement?
kubectl -n nucleus exec -it nucleus-0 -- \
  nucleus shell -c 'CREATE TABLE t(id INT PRIMARY KEY); INSERT INTO t VALUES (1);'
kubectl -n nucleus delete pod nucleus-0
kubectl -n nucleus rollout status statefulset/nucleus --timeout=10m
kubectl -n nucleus exec -it nucleus-0 -- nucleus shell -c 'SELECT * FROM t'

# The PDB should make this hang rather than silently take the database down.
kubectl drain <node> --ignore-daemonsets
```

Known limitations, stated rather than discovered:

- **`replicas: 1` is a hard constraint**, not a default. Raft hard state is
  never persisted and replication ships raw SQL strings, so `now()`,
  `random()` and `nextval()` diverge across replicas. Scaling up produces a
  cluster that disagrees with itself.
- Probes are **TCP only**. There is no credential-free health endpoint, so
  "Ready" means "accepting connections", not "can serve a query".
- `local-path` pins the volume to one node, so the pod cannot reschedule
  elsewhere. Correct for a single-node lab; for a multi-node cluster use
  Longhorn or an equivalent and revisit `accessModes`.
- `terminationGracePeriodSeconds: 120` is a guess sized against an unbounded
  flush. Measure the real shutdown time under load before trusting it.
