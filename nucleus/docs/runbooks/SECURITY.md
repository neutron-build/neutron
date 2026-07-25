# Runbook — security

Hardening a single Nucleus node, and the boundaries it does not enforce.
Authoritative detail on row-level security lives in
[`RLS_SECURITY.md`](../../RLS_SECURITY.md); this is the operational procedure.

## 0. The five things that matter, in order

1. Bind loopback unless you have a reason not to.
2. Set a password. The server already refuses a non-loopback bind without one.
3. Never set `NUCLEUS_ALLOW_NO_AUTH=1` or `NUCLEUS_ALLOW_INSECURE_AUTH=1`
   outside a throwaway dev box.
4. If you use RLS, verify the policies are loaded after **every** restart,
   restore and upgrade. A policy that fails to load is a silent exposure, not
   an error.
5. Keep `NUCLEUS_ENCRYPT_KEY` somewhere you will still have it after losing
   the machine. There is no recovery path without it.

## 1. Network exposure

```bash
nucleus start --host 127.0.0.1 --port 5432 --data /var/lib/nucleus
```

The server refuses to start on a non-loopback bind with no password
authentication configured. That guard is defeated by `NUCLEUS_ALLOW_NO_AUTH=1`,
which exists for development only.

The RESP (Redis) port is **on by default at 6379**. If you do not use the KV
surface over RESP, turn it off explicitly — it is a second door into the same
data:

```bash
nucleus start ... --resp-port 0
```

The Prometheus metrics endpoint binds `127.0.0.1` only and cannot be moved off
loopback. Scrape it through a local exporter or an SSH/Tailscale tunnel, not by
exposing the port.

Cluster (`--cluster-port`) and replication (`--replication-port`) ports are
part of an unsupported, incomplete subsystem. Do not expose them.

## 2. Authentication

```bash
# /etc/nucleus/nucleus.env, mode 0640, root:nucleus
NUCLEUS_PASSWORD=<generated>
NUCLEUS_AUTH_METHOD=scram-sha256
```

- SCRAM-SHA-256 is the default and the only method to use. `cleartext` exists
  for legacy clients and sends the password in the clear.
- Put secrets in the `EnvironmentFile`, never on the command line —
  `/proc/<pid>/cmdline` is world-readable.
- Multi-user logins use per-role SCRAM verifiers in the durable role catalog.
  The authenticated login role stays attached to the connection for its
  lifetime; `SET ROLE` is allowed only to the login role itself, to recursively
  granted memberships, or for a superuser, and membership is **revalidated on
  every statement** — so a revoked grant takes effect on live sessions.

Known boundary: the embedded/server mode that runs unauthenticated retains the
trusted `nucleus` bootstrap identity. **That mode is not a tenant isolation
boundary.** Do not use it to serve untrusted clients.

## 3. TLS

```bash
nucleus start ... --tls-cert /etc/nucleus/tls/server.pem \
                  --tls-key  /etc/nucleus/tls/server.key
```

- `--no-tls` disables transport encryption. Combined with password auth it is
  refused unless `NUCLEUS_ALLOW_INSECURE_AUTH=1` is set — again, development
  only.
- `--tls-client-ca` (or `NUCLEUS_TLS_CLIENT_CA`) requires client certificates,
  i.e. mTLS for SQL clients. Use it where you can.
- Do **not** treat a Tailscale or WireGuard overlay as a substitute for TLS.
  The database's security boundary must not depend on the overlay being
  correctly configured.

## 4. Encryption at rest

```bash
nucleus start ... --encrypt      # with NUCLEUS_ENCRYPT_KEY or _PASSPHRASE
```

AES-256-GCM at the page level. Two operational consequences:

- **Losing the key makes the data directory permanently unreadable.** Store it
  in the same place you store the ability to recover the machine, not on the
  machine.
- **`nucleus backup --online` refuses encrypted or compressed data files.** A
  plain copy needs no key; a coordinated snapshot reads pages. Plan the backup
  method (see [BACKUP_RESTORE_PITR.md](BACKUP_RESTORE_PITR.md)) *before*
  enabling encryption, not after.
- PITR is scoped to the plaintext segmented SQL WAL. Encrypted-WAL PITR does
  not exist.

## 5. Row-level security

RLS policy DDL and fail-closed executor enforcement are implemented. Enable and
verify:

```sql
ALTER TABLE t ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON t USING (tenant_id = current_setting('nucleus.tenant_id'));
```

```bash
psql -c 'SELECT * FROM pg_policies'     # after every restart/restore/upgrade
```

Limitations you must design around, quoted from the source of truth:

- **`nucleus.tenant_id` cannot be set through SQL.** A trusted proxy or
  authentication boundary must install it through the server API. If your
  application sets it in SQL, your isolation does not work.
- `SUPERUSER` and catalog-backed `BYPASSRLS` roles bypass RLS. There is no
  implicit table-owner bypass.
- **Column masking is not enforced.** There is a persisted internal policy
  engine but no SQL DDL and no executor enforcement. Do not rely on it for
  isolation.
- Constraint errors (unique, foreign key) can reveal that a hidden row exists,
  as in PostgreSQL. RLS protects row contents, not existence side channels.
- **Physical backup, restore and replication are privileged raw-storage
  surfaces — they are not tenant-filtered exports.** Anyone who can take a
  backup can read every tenant's data.
- Specialty stores have their own boundaries. RLS prevents SQL cross-model
  bypasses; it does not reinterpret relational policies as RESP KV-key or
  graph-edge policies. **If a tenant can reach the RESP port, relational RLS
  does not protect the KV data.**

## 6. Host hardening

The systemd unit in `deploy/systemd/` ships a full hardening block —
`NoNewPrivileges`, `ProtectSystem=strict`, `SystemCallFilter=@system-service`,
an empty `CapabilityBoundingSet`, and a restricted address-family set. It is
**written but never loaded** (see `deploy/README.md`); verify with
`systemd-analyze security nucleus.service` on first install.

The container image runs as uid/gid 10001 with `readOnlyRootFilesystem` and all
capabilities dropped in the k3s manifest.

## 7. Auditing an existing install

```bash
ss -lntp | grep nucleus                 # which ports are actually exposed?
grep -rE 'ALLOW_NO_AUTH|ALLOW_INSECURE' /etc/nucleus/ /etc/systemd/system/nucleus.service
psql -c '\du'                            # who has SUPERUSER / BYPASSRLS?
psql -c 'SELECT * FROM pg_policies'      # do the policies you expect exist?
ls -la /etc/nucleus/nucleus.env          # 0640 root:nucleus, not 0644
ls -ld /var/lib/nucleus                  # 0700, owned by nucleus
```

The first and third commands find the two most common real problems: a port
exposed that nobody meant to expose, and more superusers than anyone
remembers creating.

## 8. Reporting a vulnerability

See the repository's `SECURITY.md` at the root of the Neutron tree.
