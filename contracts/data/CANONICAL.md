# Canonical form and hashing — schema document v2

Status: implemented contract (Go: `cli/internal/db/schema_v2.go`, TypeScript:
`contracts/data/consumer.ts`; agreement pinned by `golden/manifest.json` and CI).

The canonical form answers one question: do two schema documents describe the
same schema state? Canonicalization normalizes **formatting only**. It never
injects or drops semantic values: an explicit `"onDelete": "no action"` and an
omitted `onDelete` are different documents with different hashes (the exporter
owns spelling stability; the plan format records hashes of exported bytes).

Canonicalization runs on a **validated** document, parsed to a generic JSON
tree (objects, arrays, strings, booleans, integers). Invalid documents have no
canonical form.

## 1. Serialization

1. Objects: keys sorted lexicographically by the UTF-8 byte sequence of the
   key (all contract keys are ASCII). No whitespace anywhere — one compact
   line, no trailing newline.
2. Strings: minimal escaping. `"` → `\"`, `\` → `\\`, and C0 control
   characters U+0000–U+001F → `\b`, `\t`, `\n`, `\f`, `\r` for those five,
   otherwise `\u00xx` with **lowercase** hex. Every other code point is
   emitted as raw UTF-8. (This is exactly what ECMascript `JSON.stringify`
produces for a string; Go needs a custom writer because `encoding/json`
    HTML-escapes and escapes U+2028/U+2029 by default.) U+2028 and U+2029 are
    emitted raw. Unpaired surrogates cannot appear: producers must not emit
    them and validators reject them (the Go validator rejects U+FFFD after
    parsing as a strict over-approximation, since Go decodes lone surrogate
    escapes to U+FFFD; the TypeScript validator rejects U+FFFD and unpaired
    surrogates directly).
3. Numbers: the document contract contains **integers only** (the JSON Schema
   restricts numeric fields to `integer`; validators additionally reject
   non-integral values and values outside the safe integer range ±(2^53−1) —
   larger magnitudes would need exact bignums in both languages). Canonical
   output is plain decimal digits with a leading `-` for negatives: no
   exponent, no fraction, no `+`, no leading zeros. Input spellings `1`, `1.0`,
   `1e0` all parse to the same integer and serialize identically.
4. Booleans `true`/`false`; `null` never occurs in a valid document (optional
   fields are absent, not null).

## 2. Unordered sets vs ordered tuples

Collections that are **sets** are sorted in the canonical form (input array
order is presentation, not semantics):

| Collection | Sort key |
|---|---|
| `capabilities` | value, bytewise |
| `schemas` | `name` |
| `tables` | `identity.schema`, then `identity.name` |
| `tables[].constraints` | `name` |
| `tables[].indexes` | `identity.schema`, then `identity.name` |
| `enums` | `identity.schema`, then `identity.name` |
| `views` | `identity.schema`, then `identity.name` |
| `opaque` | `identity.schema`, `identity.name`, then `kind` |
| `tables[].indexes[].include` | value, bytewise |

All string comparisons sort by UTF-8 bytes (code-point order). Sort keys are
unique in a valid document (duplicate identities are validation errors), so
the sort is total.

Sequences that are **ordered tuples** are preserved verbatim:

- table `columns` — physical column order (PostgreSQL `attnum`) is part of
  the schema state: two documents that differ only in column order describe
  different schemas (README section 4.1 "include ordered columns"), so the
  array is preserved exactly as declared and never sorted;
- constraint `columns` (primary-key, unique, foreign-key) — composite key
  order is semantic;
- `references.columns` (must be positionally aligned with the FK's `columns`);
- index `key` parts (btree column order is semantic);
- enum `values` (PostgreSQL enum order is semantic).

## 3. Hash

`SHA-256` over the canonical UTF-8 bytes, reported as lowercase hex. The
canonical bytes are the serialization of the whole document object (which
starts with `{"capabilities":` … because `version` sorts last).

## 4. Verbatim text

SQL text (`expression`, `where`, view `definition`, literal `sql`, opaque
`reason`) is hashed exactly as written. Two spellings of the same expression
are two documents. Expression equivalence and catalog-aware normalization are
concerns of introspection and planning, not of this contract.

## 5. Rejection codes

Validators on both sides emit errors tagged with a stable code; fixture
expectations match the bracketed code (for example `[duplicate-table]`):

`invalid-json` `not-object` `unknown-version` `missing-field` `unknown-field`
`invalid-value` `invalid-number` `invalid-identity` `duplicate-schema`
`duplicate-table` `duplicate-column` `duplicate-constraint` `duplicate-index`
`duplicate-enum` `duplicate-view` `undeclared-schema` `unknown-type`
`codec-mismatch` `invalid-type-params` `vector-capability` `enum-unresolved`
`invalid-literal` `invalid-default` `constraint-column` `multiple-primary-key`
`pk-not-null` `fk-target` `fk-column` `fk-not-unique` `check-expression`
`index-key` `duplicate-key` (Go-only duplicate JSON key detection)
`ambiguous-default` (v1 upgrade reader)

Known acceptance asymmetries, deliberate and pinned by tests:

- `duplicate-key`: Go-only. `JSON.parse` cannot see duplicate keys without a
  custom parser; ECMAScript last-wins is deterministic, so the TypeScript
  consumer accepts what Go rejects.
- Deeply nested input (thousands of levels): both sides fail cleanly with
  `invalid-json` — Go via the `encoding/json` depth cap, TypeScript by
  catching the parser's `RangeError` (stack overflow). The exact threshold
  differs by environment; only the rejection code is contractual.
- A non-integer `version` spelling (for example the string `"2"`): both Go
  entry points (`ValidateSchemaDocument` dispatch and `ParseV2Document`)
  report `invalid-number`.

## 6. Version detection and the v1 upgrade reader

Documents are dispatched on the top-level `version` integer: `1` takes the
existing version 1 path (unchanged planning behavior); `2` takes this
contract; anything else is rejected with `unknown-version`.

The v1 upgrade reader (`UpgradeV1Schema` in Go) converts a **validated** v1
document (the `@neutron-build/sql` `exportSchema` shape) into a v2 document.
It upgrades only data the v1 shape determines unambiguously:

- `serial` columns become `int4` plus a `sequence` default on
  `<schema>.<table>_<column>_seq` (the name the v1 DDL produces in PostgreSQL);
- `defaultNow` becomes the expression `now()`;
- a `default` string on numeric/serial columns upgrades to a `literal` when it
  matches the plain numeric-literal pattern, and `true`/`false` on `boolean`
  columns upgrades to a `literal` (both already enforced by v1 validation);
- per-column `primaryKey` flags become one `primary-key` constraint named
  `<table>_pkey` with columns in declaration order; per-column `unique`
  becomes a `unique` constraint named by `uniqueName` when present, else the
  PostgreSQL default `<table>_<column>_key`; per-column foreign keys become
  `foreign-key` constraints named `<table>_<column>_fkey`;
- `vector` columns add the `nucleus` capability;
- everything lands in schema `public`, `managed: true`, with empty `enums`,
  `views` and `opaque`.

Every other v1 default string is reported as `[ambiguous-default]` naming the
column: the v1 shape cannot distinguish the literal string `now()` from the
expression `now()`, and the reader never guesses. Such documents must be
re-exported as v2.
