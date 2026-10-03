NP02 source candidate 49587357 + 7859f08b binds pgwire BackendKeyData, notification sender
identity and `pg_backend_pid()` to one executor-owned live backend identity. SQL
and wire cancellation share the cooperative flag and Notify wakeup. Unknown or
disconnected backend IDs cannot target the embedded fallback session. IDs never
wrap or recycle. Client-command clearing serializes flag and wakeup-permit
clearing with concurrent requests; the wire completion fence also catches
self-cancellation during a synchronous executor poll. Both SQL functions bypass
shared query-result caching.

Finite authorization permits a caller with the same effective role as the
authenticated target owner, or an effective superuser. Non-superusers cannot
cancel a superuser target; BYPASSRLS supplies no cancellation privilege. Inherited
role authority and `pg_signal_backend` membership are explicitly **unsupported**
in this candidate. PostgreSQL's pgwire protocol alone establishes none of these
facts. PostgreSQL 17 documents its signaling authority in
[Server Signaling Functions](https://www.postgresql.org/docs/17/functions-admin.html#FUNCTIONS-ADMIN-SIGNAL)
and [Predefined Roles](https://www.postgresql.org/docs/17/predefined-roles.html).
This source implementation does not enable the P40 profile.

The bounded native fact fixture below runs separately against PostgreSQL 17 and
the exact candidate Nucleus binary. It verifies SQL PID against native startup
BackendKeyData, same-role peers, repeated signals, other-role denial (including
BYPASSRLS), superuser restrictions, self-cancellation SQLSTATE 57014 and reuse,
and disconnected-target removal. Its PostgreSQL-only membership control proves
the deliberate candidate restriction. It uses isolated temporary roles, never
prints credentials, and reports cleanup failure as failure. Idle-target signaling
is not evidence of active CPU/lock-wait cancellation latency or rollback semantics.

Coordinator commands (isolated endpoints; role creation needs the admin role):

```sh
python conformance/polyglot/nucleus/cancellation_authority.py \
  --engine postgres --admin-url-env NEUTRON_NATIVE_PG_ADMIN_URL \
  --report /path/to/evidence/np02-postgres-authority.json
python conformance/polyglot/nucleus/cancellation_authority.py \
  --engine nucleus --admin-url-env NEUTRON_NUCLEUS_ADMIN_URL \
  --binary-sha256 EXACT_EXECUTED_BINARY_SHA256 \
  --report /path/to/evidence/np02-nucleus-authority.json
```

Run `cargo fmt --check` first, then `cargo test --lib --features server
backend_cancel`, full library tests, server clippy, core-only check, the complete
`docs/PROBES.md` gate and `probe_sessions --negative-control cancellation` plus
its authority/guards controls. Reconcile doc metrics after obtaining current
counts. Native pg and postgres.js max-one-client cancellation/reuse acceptance
must additionally cover a bounded active query and an unrelated borrower; this
fact fixture does not claim to replace that gate. PostgreSQL can deliver an idle
signal asynchronously as the next query starts; the fixture records one possible
57014 delivery race and requires the next command to succeed. Preserve exact source, binary,
toolchain, fixture and report hashes in the coordinator evidence manifest.
