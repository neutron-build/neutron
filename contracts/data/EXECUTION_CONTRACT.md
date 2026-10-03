# Polyglot PostgreSQL execution contract v1

These required states apply to new certified ORM profiles; current runtime
compliance is unverified. Migration protocol v2 remains normative for migration
identity/authority. Framework HTTP errors remain an adapter concern: native SQL
execution preserves SQLSTATE, cause and known/unknown outcome.

A session owns one active execution lifecycle. Concurrent use by another task,
thread or goroutine is **rejected by default**, rather than implicitly serialized.
A pool/client can support concurrent independent sessions. Ownership must not
leak through global context or detached callbacks. Unknown profile capabilities
fail before the operation; profile names alone do not establish support.

Transactions have explicit acquisition, active, failed, committing, committed,
rolling-back, aborted, indeterminate and closed states. Native transaction errors
require rollback, or explicitly admitted savepoint recovery. Cancellation before
acquisition prevents a later checkout. Cancellation during execution cleans and
releases the connection or discards it; cleanup failure must never return dirty
session state to the pool. Terminal handles reject later operations.

A disconnect during COMMIT can have an indeterminate outcome. Never automatically
replay that write. Retries require a documented safe boundary and do not include
external side effects. Savepoint rollback reconciles tracked ORM state as well
as PostgreSQL state. SET LOCAL role/GUC tenant context is pinned to the transaction;
next borrowers must observe no prior tenant context.

Direct/session/transaction-pool/stateless profiles advertise independently tested
capabilities. Transaction-pooled migration connections cannot provide session
advisory-lock authority. Provider functionality is explicit, not inferred from
PostgreSQL wire compatibility. Unsupported or unknown operations refuse before
mutation; such containment is not certification that the feature is supported.

`execution/v1/` fixtures declare expectations, not newly established support.
