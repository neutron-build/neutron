# Polyglot PostgreSQL value contract v1

This contract defines required outcomes for new ORM profiles. It does not certify
existing clients. Schema document v2, generated lossless-read-v1 and migration
protocol v2 keep their existing semantics. Fixtures under `values/v1/` have an
explicit independent format version.

Cross-language fixtures use tagged textual integer/decimal/temporal values so
JSON cannot round exact values. Public APIs may use idiomatic native types,
but writes and reads must preserve the same database values. Financial finite
validation is an application rule; numeric NaN/infinities must have explicit
supported or refused behavior. Local timestamp and zoned instant are distinct;
JavaScript Date truncation cannot silently count as lossless precision.

Every insert and every update distinguishes four states: **omitted**, **DEFAULT**,
**NULL**, and **present value**. On insert omission uses PostgreSQL defaults; on
update omission leaves the existing value. DEFAULT evaluates the database
column default in either operation. Present false, zero, empty string and empty
binary are never treated as omission. Generated-column writes are rejected;
identity override is an explicit capability. NULL is never a proxy for DEFAULT.

SQL NULL and JSON null differ. Decimal and int8 extremes must round-trip without
IEEE754 conversion. Domain/composite/array/range identity and structure must be
preserved by supported profiles. Lower array bounds, NULL members, empty arrays
and composite NULL require explicit representations. Refusing an unsupported
complex type safely is containment, **not full capability verification**.

Native PostgreSQL text/catalog/state is the independent oracle. A writer and
reader agreeing on a lossy conversion does not pass. Driver parser changes,
session timezone or DateStyle cannot silently change the declared contract.
