# Deployment paths

Three supported shapes: a container, a systemd unit, and a single-instance k3s
StatefulSet. All three run **one** Nucleus process. Distributed mode is
incomplete and unsupported (`DATABASE_COMPLETION.md` Milestone 9) — none of
these files scales past one instance, on purpose.

## What has actually been verified

Written is not validated. This table is the honest state as of 2026-07-24; do
not upgrade a row without running the command in the last column.

| Artifact | Verified | Not verified | How to close the gap |
|---|---|---|---|
| `nucleus/Dockerfile` | Parses and is accepted by the buildah/podman frontend (reaches `STEP 1/5`) | **The image has never been built or run.** | `docker buildx build -t nucleus:dev ./nucleus` on a Linux host |
| `nucleus/Dockerfile.dist` | Parses, `STEP 1/13` | Never built; the multi-arch `TARGETARCH` selection is untested | See "Container" below |
| `nucleus/.dockerignore` | Reduces the build context to ~11 MB | — | — |
| `deploy/systemd/nucleus.service` | Hand-checked against the binary's real behaviour (drain budget, signal handling, env-var names) | **Never loaded by systemd.** The hardening block in particular is untested and is the most likely thing to block startup | See "systemd" below |
| `deploy/k3s/*.yaml` | `kubeconform -strict -kubernetes-version 1.31.0` — 5 resources, 0 invalid | **Never applied to a cluster.** Schema-valid is not the same as working: probes, fsGroup/PV permissions and the graceful-shutdown window are all untested | See "k3s" below |

Why the container was not built here: the development machine is macOS, and its
podman VM's overlay storage is currently faulting —

```
Error: mounting new container: ... creating overlay mount to
/var/home/core/.local/share/containers/storage/overlay/.../merged, ...:
input/output error
```

— for *every* image including `debian:bookworm-slim`, so nothing containerised
can run on it until that VM is repaired. That is an environment fault, not a
Dockerfile fault: the build reached `STEP 1/5`, which means the frontend parsed
the file and resolved the base image before failing on the mount.

## Container

```bash
# From the repository root. BuildKit is required (buildx, or podman/buildah):
# the builder stage uses `--mount=type=cache`, which the legacy docker builder
# does not understand.
docker buildx build -t nucleus:dev ./nucleus

docker run --rm -d --name nucleus \
  -p 5432:5432 \
  -e NUCLEUS_PASSWORD=changeme \
  -v nucleus-data:/data \
  nucleus:dev
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
# 1. becomes healthy
docker inspect --format '{{.State.Health.Status}}' nucleus
# 2. serves a real query
psql "postgres://postgres:changeme@127.0.0.1:5432/nucleus" -c 'SELECT 1'
# 3. survives a restart with its data
psql ... -c 'CREATE TABLE t(id INT PRIMARY KEY); INSERT INTO t VALUES (1);'
docker restart nucleus && sleep 15
psql ... -c 'SELECT * FROM t'            # must return the row
# 4. does not run as root
docker exec nucleus id                   # uid=10001
```

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
