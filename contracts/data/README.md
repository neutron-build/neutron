# contracts/data — cross-language schema contract

The versioned contract that the TypeScript schema exporter and the Go CLI
share for describing PostgreSQL schema state: a JSON Schema for the document
shape, a canonical-form specification used for stable hashing, and golden
fixtures that two independent implementations must agree on byte-for-byte.

## Contents

| File | What it is |
|---|---|
| `schema-v2.json` | JSON Schema (draft 2020-12) for schema document v2 — the normative shape |
| `CANONICAL.md` | Canonical JSON rules: key sorting, unordered sets vs ordered tuples, number and string serialization, SHA-256, rejection codes, the v1 upgrade reader rules |
| `consumer.ts` | TypeScript reference implementation: validates, canonicalizes and hashes the fixtures; run with `node --experimental-strip-types contracts/data/consumer.ts` |
| `golden/manifest.json` | Fixture index: expected canonical bytes, SHA-256 hashes and rejection codes |
| `golden/valid/` | Valid documents, each with the expected canonical serialization |
| `golden/invalid/` | Invalid documents with the rejection code both implementations must produce |
| `golden/v1/` | Version 1 documents for the Go upgrade reader (expected upgraded output, or the expected ambiguity error) |

The Go implementation lives in `cli/internal/db/schema_v2.go` (validation,
canonicalization, hashing, version detection) and
`cli/internal/db/schema_v1_upgrade.go` (the v1 upgrade reader). Expected
values in the manifest are recorded from the Go side via
`go test ./internal/db -run TestV2Contract -update-golden` and must then be
reproduced by `consumer.ts` — CI runs both and fails on any disagreement.

## Where each rule lives

`schema-v2.json` (the shape) and the reference validators (Go + TypeScript,
the cross-object rules) deliberately split the rejection surface. CI keeps
the split load-bearing: the Schema must reject the shape-level invalid
fixtures and accept the cross-object ones (ajv gate in
`.github/workflows/contracts.yml`).

| Rule | Schema (shape) | Validators (Go + TS) |
|---|---|---|
| Required fields, unknown fields, types/enums/consts | yes | yes |
| Names: non-empty, no control characters (C0 incl. NUL, DEL) | yes | yes |
| SQL text: non-empty, no NUL | yes | yes |
| Literal-default token pattern | yes | yes |
| Integer-only numbers | yes (type) | yes (type + safe-range ±(2^53−1)) |
| Type-parameter ranges (length/precision/scale/dimensions caps) | no | yes |
| U+FFFD / unpaired-surrogate strings | no | yes |
| Duplicate JSON keys | no | Go only (JSON.parse cannot see them; CANONICAL.md §5) |
| Duplicate identities (tables, indexes, schemas, enums, views, columns, constraints) | no | yes |
| Schema declaration for every referenced schema | no | yes |
| Constraint/index columns exist on the table | no | yes |
| Enum reference resolution | no | yes |
| Foreign-key target existence, column existence, PK/unique tuple coverage | no | yes |
| PK columns declare notNull | no | yes |
| Codec/type consistency, vector capability | no | yes |

## Status

Version 2 documents can be validated and hashed by the CLI today; the
migration planning commands still consume version 1 exports and reject
version 2 input explicitly. Introspection, diffing and planning on v2 land
with the migration lifecycle work that follows.
