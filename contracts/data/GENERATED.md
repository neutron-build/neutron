# Generated scalar read profile

`lossless-read-v1` is an optional PostgreSQL scalar selected-row profile for
`neutron generate --profile lossless-read-v1` and Studio Codegen. The default
`legacy` retains its existing six-language output, including potentially lossy
numeric mappings. Unknown profiles and unsupported languages refuse; the new
profile admits TS, Python and Go only.

Identity is mandatory: the column's actual `pg_attribute.atttypid`, joined
`pg_type` name/base kind (`typtype=b`) and `pg_catalog` namespace must match this
matrix. A domain over a builtin or user type with a builtin name is refused.
Missing metadata and query/scan/row errors refuse instead of falling back.

| PostgreSQL identity / OID | TypeScript | Python | Go |
|---|---|---|---|
| int2 / 21 | number | int | int16 |
| int4 / 23 | number | int | int32 |
| int8 / 20 | string | int | int64 |
| numeric / 1700 | string | Decimal | string |
| bool / 16 | boolean | bool | bool |
| text / 25, varchar / 1043, bpchar / 1042 | string | str | string |
| uuid / 2950 | string | UUID | string |
| bytea / 17 | Uint8Array | bytes | []byte |

Python models import native `UUID` for uuid fields and `Decimal` for numeric
fields. Numeric models set `ConfigDict(allow_inf_nan=True)` to preserve native
Decimal NaN and both infinities; this is a read representation, not finite
arithmetic or application validation certification. Applications requiring
finite amounts must validate that separately.

Nullable TypeScript properties are required and have `T | null`; declarations
check static construction only and do not decode/validate raw rows at runtime.
Python nullable properties use required `Optional[T]` without a default;
Pydantic rejects missing fields. Go nullable properties are pointers, including
`*[]byte`, so NULL differs from empty non-NULL binary. Go's public query scanner
maps returned columns and leaves absent projected fields zero-valued; callers
must verify projection completeness rather than assume missing-field rejection.

The native transport scope is default `pg` PostgreSQL reads (int8/numeric
strings, Buffer compatible with Uint8Array), Python asyncpg reads
(int/Decimal/UUID/bytes) through `SQLModel`, and the Go SDK public SQL scanner.
Custom driver parsers must preserve these shapes. These models do not certify
other engines or create SQL binding/write adapters. Applications may use their
native APIs for writes, under independently tested parameter contracts.

Temporal/date/interval, array, enum, domain, composite, JSON/JSONB, floating-point
and other identities are refused for the entire selected generation. No
unsupported field is silently omitted. Invalid/reserved identifiers and Go
field-name collisions refuse rather than being silently escaped or renamed.
Generated columns and default-bearing columns retain selected read fields;
this does not establish insert omission, generated-column write permission,
composite-key metadata or migration authority.

CLI `--all` validates every selected table before writing any output for these
profile/type/identifier failures, preserving existing files. This is not an
atomic filesystem guarantee for later I/O failures. Studio displays contextual
refusals instead of generated code. Repeating generation from unchanged
metadata is deterministic.
