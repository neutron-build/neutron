# Schema format compatibility and coverage decisions

This document specifies the compatibility boundary for extending the schema
contract. Coverage below is a source assessment, not additional live
certification. The normative current shape remains `schema-v2.json`; validation
and canonical rules remain `CANONICAL.md`, `consumer.ts`, and
`cli/internal/db/schema_v2.go`. `compatibility.json` records this assessment in
machine-readable form.

## Preserve existing contracts

Existing version-2 documents retain their canonical bytes, hashes, rejection
codes, ordered tuples, ownership semantics and supported type vocabulary.
An extension must not reinterpret an old document or regenerate its goldens to
hide a disagreement. New fixtures may exercise unchanged semantics. Existing
version-1 upgrades and ambiguity refusals remain explicit.

A capability requiring additional fields or object kinds needs an explicitly
versioned format decision. No version-3 format is introduced here. Before adding
one, specify its reader, canonical rules, upgrade preservation, rejection
behavior, planner coverage and old-reader refusal. A format version is distinct
from an application value codec profile and a migration history protocol.

Migration protocol v2 remains unchanged: exact text identity and numeric-aware
ordering, collision refusal, exact up-SQL checksum bytes, verified/unverified
history, explicit adoption, authority locks and persistent quoted metadata
namespace. CLI text histories and SDK integer histories remain distinct physical
profiles. New Python ORM query support does not imply protocol-v2 migration
runner support. See `MIGRATIONS.md`.

## Current representation versus planned management

Version 2 already represents qualified tables, ordinary views, enums,
constraints, indexes, generated/identity/default metadata and supported column
families. Representation does not prove that every PostgreSQL feature within
those families can be introspected, planned or queried. Each path requires its
own acceptance case.

`cli/internal/db/introspect_v2.go` deliberately inventories materialized views,
foreign and partitioned tables as opaque. Ordinary tables with partitions,
inheritance, unlogged persistence, RLS/policies or user triggers become opaque,
and unsupported FK dependencies propagate that boundary. Extension-owned
objects are not flattened. These are useful safety refusals, not management
parity. A query-only ORM may still use an externally managed table under a
separately tested profile.

Domains, composites, ranges/multiranges, additional temporal/network types and
richer array schema metadata are not part of the current type vocabulary. Do
not flatten their identity into a familiar base type. The current introspection
base-type selection must be adversarially checked against builtin-looking
names in non-pg_catalog namespaces and domain identities before broader
identity claims. Generated lossless-read-v1 has its own narrow identity checks;
it does not establish introspection/write parity.

`managed: false` is a descriptive ownership boundary for representable objects,
not a mechanism for flattening unsupported objects. Its contents are not checked
against the live database, and it is never created, altered, dropped or reported
as drift. Previously known metadata/dependencies must survive scope changes.
Opaque entries are read-only inventory and never DDL inputs. Neutron-internal
metadata is excluded from managed schema plans, with foreign-key references
into protected metadata refused.

## Implementation gates

For each family, first distinguish runtime query support, schema representation,
introspection fidelity, migration management and safe external preservation.
Specify management only where required application cases justify it. Preserved
external objects must remain discoverable with an actionable reason; unknown
objects must never silently disappear from a destructive plan.

Required tests include two schemas with the same table name, hostile
search_path, quoted/reserved identifiers, enum ordering, composite FK identity,
domain/base identity, extension dependency ownership and RLS/trigger-bearing
tables. Snapshot and live round trips must preserve dependencies. New format
readers must keep old-document hashes and explicit downgrade/refusal behavior.

Existing compatibility checks:

```sh
node --experimental-strip-types contracts/data/consumer.ts
```

From `cli/`:

```sh
go test ./internal/db -run 'TestV2|TestDetect|TestValidateSchemaDocument|TestUpgradeV1' -count=1
```

These tests cover existing contract agreement; they do not certify the planned
additional object families. The JSON Schema shape/cross-object validation split
in `.github/workflows/contracts.yml` remains load-bearing.
