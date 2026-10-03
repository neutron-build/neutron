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
