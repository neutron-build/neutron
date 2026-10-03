Schema-v3 is an additive, preserve-only catalog representation. It wraps an
unchanged validated relational schema-v2 document and a separate catalog
inventory. Version 3 does not authorize migration DDL and does not replace v2
wire endpoints, canonical bytes, hashes, rejection rules, or migration ledgers.

`cli/internal/db/schema_v3.go` implements parsing, typed export, canonicalization,
SHA256, and explicit v1/v2 upgrade. `ParseV3Document` accepts only version 3;
`UpgradeSchemaDocumentV3` preserves validated v2 bytes or uses the existing v1
upgrade and its ambiguity refusals. `V3Document.RefuseMigration` always refuses.
Legacy upgrades mark newly introduced families `not-inspected`, never empty by
inference. No Nucleus catalog parity is claimed.

Portable object identity is the JSON tuple of catalog namespace, schema, name,
optional qualified parent and, for `pg_proc`, ordered qualified **input/INOUT**
argument type identities. Zero arguments are an explicit empty array. OUT names
and return types describe the object and do not distinguish overloads. Same
names in different schemas, same-name overloads, and a custom domain named like a
builtin remain separate. Catalog OIDs may join native queries internally but are
not serialized as portable identity. OID attributes are refused.

Each inventory object is `managed:false` and carries kind, owner, extension,
reason, descriptive definition, named string attributes, qualified references,
and ordered parts. Unknown catalog kinds and attributes remain uniquely retained
and unmanaged. Definitions and parts are preserved exactly; importing them means
parsing metadata, never running their SQL. There is no unsupported-shape fallback
that deletes an inventory entry or flattens a type into a builtin.

Coverage explicitly records relations, routines, types, policies, grants,
triggers and extensions. Allowed states are `identity-inventory`, `partial`, and
`not-inspected`; no `complete` state or whole-catalog certification exists in this
bounded contract. A family can have every identity inventoried while unsupported
metadata semantics remain described in reasons/coverage. More families remain
mandatory followup work and cannot disappear from the coverage array.

Canonical bytes use the existing v2 UTF-8 JSON object-key and escaping rules.
The nested relational document uses its original v2 canonicalization. Coverage
sorts by family; inventory sorts by its serialized identity tuple; qualified
argument arrays and parts retain semantic order. Maps sort by key. Duplicate
identities, duplicate JSON keys, unknown structural fields, null, missing
required fields, invented coverage, unqualified identities and managed entries
are refused. The golden bytes/hash were frozen independently as JSON data and
must agree with the Go implementation after coordinator execution.

Coordinator gates, in `cli/`:

```sh
go test ./internal/db -run '^TestV3' -count=1
go test ./internal/db -run '^TestV2' -count=1
```

These commands have not been run by the author. Native PostgreSQL qualification
and independent review at the integration hash are required before acceptance.

`Client.IntrospectV3` inventories visible user-schema routines and types using a
single read-only repeatable-read transaction with a local `pg_catalog` deparser
path. Function/procedure definitions, owners, settings, ACL spelling, input
signatures, return types and all argument modes are preserved. Domains retain
qualified base types, defaults and constraint definitions; composites retain
physical attribute order and qualified types; enums retain value order; ranges
retain qualified subtype/range/multirange/opclass/collation references. Extension
membership is captured directly and through internal array/row-type dependencies.
Undefined shell types retain only trustworthy identity and an explicit limitation.
Aggregate definitions, base I/O/storage behavior and complete range semantics
remain incomplete and unmanaged. Policies/grants/triggers/extensions as whole
families still require further implementation; selected object ACL or extension
fields are not whole-family coverage.

PostgreSQL 17's [routine catalog](https://www.postgresql.org/docs/17/catalog-pg-proc.html)
defines `proargtypes` as the input call signature and distinguishes full argument
modes separately. Its [type catalog](https://www.postgresql.org/docs/17/catalog-pg-type.html)
defines shell-type limitations and qualified type relationships; the
[range catalog](https://www.postgresql.org/docs/17/catalog-pg-range.html) provides
range references. The reader does not infer these catalog behaviors for Nucleus.

Native acceptance requires an owned disposable PostgreSQL 17 database with
CREATEDB and the installed `vector` extension available. The existing Q07 fixture
creates/drops its isolated database and refuses to skip when required:

```sh
# cwd cli; coordinator supplies NEUTRON_E2E_DATABASE_URL without printing it
NEUTRON_LIVE_REQUIRED=1 go test ./internal/db \
  -run '^TestV3NativeQualifiedRoutinesTypesAndReadOnlyRoundTrip$' -count=1
```

The native oracle checks persisted domain data and function definitions unchanged,
existing v2 snapshots unchanged, typed v3 export/import and repeated introspection
stable, hostile search paths/catalog lookalikes unable to redirect reads, and a
function drop/recreate receiving a new OID without changing the portable hash.
Quoted names, zero/same-name/domain overloads, INOUT procedures, composite order,
ranges, shell types and direct/internal extension ownership are mandatory cases.

The next reader slice additionally inventories user-schema policies and
non-internal triggers by qualified table parent plus local object name. Policy
command/permissiveness, role sets, USING/WITH CHECK expressions and RLS flags are
preserved; trigger definitions, referenced function, enablement and constraint
flags are preserved. Same policy/trigger names on different tables cannot
collapse. Known table-scoped identities require a parent; routine/type/relation
identities cannot invent a parent to evade duplicate detection.

All extension records retain owner, namespace, version, relocatability,
configuration table/condition pairs, and portable member addresses returned by
PostgreSQL's `pg_identify_object_as_address`. Unknown member kinds stay descriptive
and unmanaged. Failed address extraction or missing configuration-table joins
refuse the entire read rather than silently dropping a member. The native fixture
now includes repeated policy/trigger names, RLS expressions and a disabled
trigger, plus installed vector-extension member addresses.

Grant-family coverage is now `partial`. User-schema table/view/materialized-view/
foreign-table/partition/sequence, schema, routine-overload and defined-type ACLs
retain grantor and grantee names, PUBLIC as a distinct pseudo-role, privilege,
grantability and object/column scope. NULL ACLs retain `aclStorage=default` and
are expanded with PostgreSQL's `acldefault`; explicit empty ACLs remain explicit
and empty. These tuples describe stored/default object authority, not inherited
or effective role access. Column grants preserve the exact column name.

The required native ACL gate creates a unique temporary role, removes its
owned-database dependencies before dropping it, and checks column privileges
and grant options against PostgreSQL's independent `has_*_privilege` functions:

```sh
# cwd cli; owned PG17 test database requires CREATE DATABASE and CREATE ROLE
NEUTRON_LIVE_REQUIRED=1 go test ./internal/db \
  -run '^TestV3NativeExplicitDefaultAndColumnACLs$' -count=1
```

Default ACLs, database privileges and global authority/membership remain an
explicit uninspected mandatory scope. None of these additions provides
policy/trigger/extension/privilege migration support.
