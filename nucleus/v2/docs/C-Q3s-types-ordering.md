# C-Q3s: Type ordering, equality and hashing (normative)

Status: draft 1. Implemented by `crates/nucleus-codec` (C-K1) and checked by its property tests (C-K2). The executor's comparison kernels (C-Q3), sort, GROUP BY, DISTINCT, hash join and every index use these rules. A change to a rule here is a key-format change: it needs a spec edit, a golden-corpus update and a format version bump (C-K4).

Reference point: PostgreSQL 17 with a database whose default collation is `C`. Where PostgreSQL has a rule, this file states it and cites the function that implements it in the PostgreSQL source tree, so it can be re-checked. Anything not verified against a running PG17 is listed in §9.

## 1. Principles

- **P-ORDER.** For two values `a`, `b` of the same declared type, `memcmp(enc(a), enc(b))` has the sign of PostgreSQL's btree comparison (`<`, `=`, `>` of the type's default btree operator class).
- **P-EQ.** `a = b` under that comparison implies `enc(a) == enc(b)` byte for byte. Equal values never have two encodings. This is what lets GROUP BY, DISTINCT and unique indexes use raw bytes.
- **P-HASH.** `hash(v)` is a function of `enc(v)` only (§8), so `a = b` implies `hash(a) == hash(b)`.
- **P-PREFIX.** Every column encoding is prefix-free: no valid encoding is a proper prefix of another of the same column. Composite keys are concatenations (§3), DESC is byte inversion (§2), and a commit-timestamp suffix can be appended by the KV layer (C-T0 §1), all without ambiguity.
- **Declared type.** Comparison is defined only within one declared type. Cross-type comparisons (`int4 = int8`, `numeric < float8`) are resolved by the binder by coercing to the column type or adjusting scan bounds before encoding. The codec never compares values of different types.

## 2. One column

```
column = marker value_bytes
marker = 0x00  NULL, when the column is NULLS FIRST
         0x01  non-NULL
         0x02  NULL, when the column is NULLS LAST
value_bytes = enc(v)            ASC
            = !enc(v) bytewise  DESC (every byte XOR 0xFF)
```

- Defaults (PostgreSQL): `ASC` is `NULLS LAST`; `DESC` is `NULLS FIRST`. Explicit `NULLS FIRST` / `NULLS LAST` override either direction.
- The marker is not inverted for DESC: it encodes the placement directly.
- DESC inversion is order-reversing only because `enc` is prefix-free (P-PREFIX): two distinct encodings always differ at some byte position inside both.
- NULL compares equal to NULL for ordering, grouping and DISTINCT (PostgreSQL's "not distinct" rule for sorting). SQL `=` returning NULL is the executor's business. Unique indexes with a NULL key column use the non-unique layout with the pk appended (C-T0 §5.2), so the shared encoding does not weaken uniqueness.

## 3. Composite keys

`key = column_1 column_2 ... column_n`, each column with its own direction and NULL placement. Byte order is the lexicographic order of the columns, which is PostgreSQL's multi-column btree order. Encoding the first `k` columns yields a prefix of the full key, which is what a range scan on a leading-column prefix uses.

Composite *types* (row values, `record_cmp`) are not key types in this draft; see §9.

## 4. Scalar types

Notation: `be(x)` = big-endian bytes. `flip(x)` = two's-complement `x` with its sign bit inverted, then `be`, which maps signed order to unsigned byte order.

| Type | PostgreSQL rule | Encoding |
|---|---|---|
| `bool` | `false < true` (`boolcmp`) | 1 byte: `0x00` / `0x01` |
| `int2`, `int4`, `int8` | signed integer order (`btint2cmp`, `btint4cmp`, `btint8cmp`) | `flip(v)`, 2 / 4 / 8 bytes |
| `float4`, `float8` | §4.1 | 4 / 8 bytes |
| `numeric` | §4.2 | variable |
| `text`, `varchar` | §5 | escaped bytes |
| `bytea` | `memcmp` over the shorter length, then shorter first (`byteacmp`) | escaped bytes (§5.1) |
| `date` | int32 days since 2000-01-01; `-infinity` = `INT32_MIN`, `infinity` = `INT32_MAX` (`date_cmp`) | `flip(i32)` |
| `time` | int64 microseconds since midnight, `0 ..= 86_400_000_000` (`time_cmp`) | `flip(i64)` |
| `timestamp` | int64 microseconds since 2000-01-01; `-infinity` = `INT64_MIN`, `infinity` = `INT64_MAX` (`timestamp_cmp`) | `flip(i64)` |
| `timestamptz` | as `timestamp`, the stored instant is UTC; the session time zone never affects order | `flip(i64)` |
| `interval` | §4.3 | 16 bytes |
| `uuid` | `memcmp` over the 16 bytes (`uuid_internal_cmp`) | the 16 bytes |
| `jsonb` | §6 | variable |
| arrays | §7 | variable |

### 4.1 float4, float8

PostgreSQL (`float8_cmp_internal`, `float4_cmp_internal`):
- `-0 = 0` (IEEE comparison).
- `NaN = NaN`, and `NaN` is greater than every non-NaN value, `+Infinity` included.
- Order: `-Infinity < negatives < -0 = 0 < positives < +Infinity < NaN`.
- `hashfloat8` / `hashfloat4` hash `-0` as `0` and every NaN as one canonical NaN, consistent with the above.

Encoding: canonicalise, then map IEEE bits to unsigned order.
1. Any NaN (any sign, any payload) becomes the canonical quiet NaN `0x7FF8_0000_0000_0000` (float4: `0x7FC0_0000`). `-0` becomes `+0`.
2. `bits = to_bits(v)`. Sign clear: `bits ^ SIGN`. Sign set: `!bits`. Write `be(bits)`.

The canonical NaN has the sign clear and a larger exponent+mantissa than `+Infinity`, so it lands above it. Decoding yields `+0` for both zeros and the canonical NaN for every NaN.

### 4.2 numeric

PostgreSQL (`cmp_numerics`, PG14+ special values):
- Order: `-Infinity < every finite value < +Infinity < NaN`. `NaN = NaN`.
- Finite values compare by mathematical value. Display scale is not part of equality: `1.0 = 1.00 = 1`, `0 = -0 = 0.000`. Negative zero does not exist as a distinct value.
- `hash_numeric` ignores leading and trailing zeros and the display scale, consistent with the above.

Encoding: normalise a finite value to `sign · 0.d1 d2 ... dn · 10^E` with `d1 != 0` and `dn != 0` (strip leading and trailing zeros; this is unique per value). Then:

```
0x01                                   -Infinity
0x02 !flip(E as i32) !mantissa 0xFF    negative finite
0x03                                   zero
0x04 flip(E as i32)  mantissa 0x00     positive finite
0x05                                   +Infinity
0x06                                   NaN
mantissa = digits in pairs (d1 d2)(d3 d4)..., the last pair padded with 0;
           one byte per pair: 10*a + b + 1  (1 ..= 100)
!x = every byte XOR 0xFF
```

Why it orders: for positives, a larger `E` is a larger value (normalised mantissas lie in `[0.1, 1)`); for equal `E`, mantissa bytes compare digit-pair-wise, and when one mantissa is a prefix of the other the shorter is smaller because the longer one's remaining digits are non-zero; the terminator `0x00` is below every pair byte. Negatives use the complement of the positive encoding of their magnitude, which reverses all of that, terminator included (`0xFF`).

Range: PostgreSQL allows up to 131072 digits before the point and 16383 after, so `E` fits in an i32 with large margin. Values outside i32 exponent range are rejected by the encoder.

Decoding yields the normalised value (no trailing zeros, minimal scale). The original display scale is not recoverable from a key (§8.1).

### 4.3 interval

PostgreSQL (`interval_cmp_value`, used by `interval_cmp` and `interval_hash`): an interval `(month, day, time)` compares by the 128-bit total

```
span = (month * 30 + day) * 86_400_000_000 + time      (microseconds)
```

So `'1 mon' = '30 days'`, `'1 day' = '24 hours'`, `'1 mon -30 days' = '0'`. PG17 infinite intervals are `(INT32_MIN, INT32_MIN, INT64_MIN)` for `-infinity` and `(INT32_MAX, INT32_MAX, INT64_MAX)` for `infinity`; the formula already maps them to the unique minimum and maximum span, so they need no special case.

Encoding: `flip(span as i128)`, 16 bytes.

Decoding yields a canonical representative of the span: the two infinities exactly; otherwise, with `days_total = floor(span / 1 day)` and `rem` the non-negative remainder, `months = trunc(days_total / 30)` clamped to the i32 range, `days = days_total - 30 * months` clamped to the i32 range, and `micros = rem + (unclamped days - days) * 1 day`. Every span of a valid finite interval has such a representative. It equals the original under the rule above but is generally not the same triple (§8.1).

## 5. text and varchar

PostgreSQL with collation `C` (`varstr_cmp` C-collation fast path): `memcmp` over the shorter length, then the shorter string first. That is plain bytewise order on the UTF-8 bytes, which equals code point order. Equality is byte equality. `varchar(n)` compares exactly like `text` and uses the same encoding.

### 5.1 Escaped bytes (text, varchar, bytea, jsonb strings and keys)

```
each byte b != 0x00 -> b
each byte 0x00      -> 0x00 0xFF
end                 -> 0x00 0x01
```

Inside the body a `0x00` is always followed by `0xFF`, so the terminator is unambiguous and the encoding is prefix-free. A proper prefix sorts first: where the shorter string ends it has `0x00 0x01`, while the longer one has either a non-zero byte (above `0x00`) or an escaped NUL `0x00 0xFF` (above `0x00 0x01`). PostgreSQL `text` cannot contain NUL; the escape exists for `bytea` and costs nothing for text.

### 5.2 Collation hook (ICU, not implemented)

A text key column carries a collation. Draft 1 defines only `C`. ICU (plan D8) plugs in as a sort-key provider:

```
text column, collation c != C:  escaped(sort_key_c(s)) escaped(utf8(s))
```

- `sort_key_c` is ICU's binary sort key for the pinned ICU data version. ICU sort keys compare with `memcmp` exactly as the collator does.
- The trailing raw bytes are PostgreSQL's deterministic-collation tie-break (`varstr_cmp` falls back to `strcmp` when the collator says equal), so `=` stays byte equality and P-EQ holds.
- Nondeterministic collations (equality by collator) would drop the tie-break and make keys non-invertible; not planned for 2.0.
- The collation version is recorded per index; a mismatch at open refuses the index or forces REINDEX (D8).
- ICU keys are not decodable; an index-only scan on such a column needs the heap row (§8.1).

## 6. jsonb

PostgreSQL rules (`compareJsonbContainers`, `compareJsonbScalarValue` in `src/backend/utils/adt/jsonb_util.c`; user-facing statement in the PostgreSQL manual, "jsonb Indexing", btree ordering paragraph):

1. Type order: `Object > Array > Boolean > Number > String > Null`.
2. An object with more pairs is greater; an array with more elements is greater. Count is compared before any content.
3. Objects with equal pair counts compare pair by pair in **storage order**: key 1, value 1, key 2, value 2, .... Storage order sorts keys by length first, then bytewise (`lengthCompareJsonbStringValue`), so `{"aa":1, "c":1} > {"b":1, "d":1}` (first stored keys are `"c"` and `"b"`).
4. Arrays with equal element counts compare element by element.
5. Scalars compare with their type's rule: strings with the database default collation (here `C`, §5), numbers as `numeric` (§4.2, so `1.0 = 1`), `false < true`, `null = null`. Keys are strings and compare the same way.
6. **Top-level scalars and the empty array.** A top-level scalar is stored as a one-element "raw scalar" pseudo-array. In `compareJsonbContainers`, a raw scalar against a real top-level array first gets the type order (scalar below array) and is then overridden by the element-count comparison when the counts differ. Effect: a top-level `[]` sorts **below every top-level scalar**, `null` included; top-level arrays with one or more elements sort above all scalars. This only applies at top level; nested `[]` follows rule 1. The manual describes this as "an empty top level array sorts less than null".
7. Object input semantics (not comparison, but they fix what is compared): duplicate keys keep the last value; keys are stored sorted as in rule 3.

Numbers in jsonb are finite `numeric` values; jsonb has no NaN or infinity.

Encoding:

```
jsonb   = top_class value
top_class = 0x01  top-level empty array
            0x02  top-level scalar
            0x03  top-level non-empty array or object
value   = 0x01                              null
        | 0x02 escaped(s)                   string
        | 0x03 numeric_enc(n)               number (§4.2 encoding)
        | 0x04 0x00 / 0x04 0x01             false / true
        | 0x05 be(u32 count) value*         array, elements in order
        | 0x06 be(u32 count) (escaped(key) value)*   object, pairs in storage order, after dedup
```

The tags realise rule 1, the fixed-width count right after the tag realises rule 2, and because counts are equal by the time children are compared, the children of two containers line up token for token, exactly as PostgreSQL's paired iterators do. `top_class` realises rule 6. Every `value` is prefix-free, so no end markers are needed.

Decoding yields keys in storage order, duplicates removed, and numbers normalised.

## 7. Arrays

PostgreSQL (`array_cmp` in `src/backend/utils/adt/arrayfuncs.c`):
1. Compare the elements pairwise in storage (row-major) order up to the shorter element count, ignoring dimensions. Element comparison uses the element type's default btree rule and the array's collation. **NULL elements sort after non-NULL elements**; two NULLs are equal. This is fixed and independent of the column's ASC/DESC or NULLS placement.
2. If all compared elements are equal: fewer elements first; then fewer dimensions first; then each dimension length in order; then each lower bound in order.

So `'{1,2}' < '{1,2,3}' < '{1,3}'`, `'{1,NULL}' > '{1,5}'`, and `'{1,2}'` vs `'[0:1]={1,2}'` differ only by lower bound (`'[0:1]={1,2}' < '{1,2}'`). Empty arrays have zero dimensions (`'{}'`) and sort first.

Encoding:

```
array = (0x01 enc(elem) | 0x02)* 0x00  u8(ndims)  flip(i32 len)*ndims  flip(i32 lower)*ndims
                         NULL
```

The element list is a prefix-free sequence ended by `0x00`, below both element markers, giving rule 1 and the element-count part of rule 2. After it, two arrays with the same elements are aligned and the fixed-width dimension fields give the rest of rule 2. Elements use their ASC encoding; a DESC array column inverts the whole array encoding, which reverses everything including NULL-element placement, as PostgreSQL's DESC does.

Constraints (PostgreSQL's): at most 6 dimensions; an empty array has zero dimensions; a non-empty one has every length `>= 1` and their product equals the element count.

## 8. Hashing and decoding

`hash(v) = FNV-1a-64(enc_asc(v))`. It is stable across platforms and releases (the bytes are frozen by the golden corpus). It is the engine's internal hash for hash joins, hash aggregation and in-memory grouping. It is **not** PostgreSQL's hash function; anything that must reproduce PostgreSQL hash values (hash partitioning, `satisfies_hash_partition`) needs PostgreSQL's functions (§9).

### 8.1 Lossy decoding

`decode(enc(v)) = v'` with `v' = v` under this file's equality, but these types do not round-trip their representation:

| Type | Lost |
|---|---|
| float4, float8 | sign of zero, NaN sign and payload |
| numeric | display scale (`1.50` decodes as `1.5`) |
| interval | the month / day / time split |
| jsonb | number scale, input key order, duplicate keys (the last one wins on input anyway) |
| text with a non-C collation | everything (§5.2) |

An index-only scan that must return the original representation fetches it from the row. The planner decides per column from this table.

## 9. Open questions and deferred types

1. **Verify against PG17.** Every rule above is from the PostgreSQL source and manual, not yet from a differential run. C-G3 must check: float NaN/-0 order and grouping; numeric NaN/±Infinity order and `1.0 = 1.00` grouping; interval `'1 mon' = '30 days'` grouping and infinite intervals; jsonb rule 6 (`'[]'::jsonb < 'null'::jsonb`, `'[1]'::jsonb > '1'::jsonb`) and the storage-order key example; array NULL-element and lower-bound order.
2. **jsonb manual wording.** Rule 6 is derived from `compareJsonbContainers`; the quoted manual sentence is from memory and must be checked against the PG17 manual text when C-G3 runs.
3. **Infinite interval bit patterns.** PG17 represents infinite intervals as all-minimum / all-maximum fields. Whether PG17 rejects finite intervals that share some (not all) of those field values on input does not affect ordering, but the binder must reject the exact sentinel triples as finite inputs.
4. **PostgreSQL-identical hashing** (§8): needed only for PG-compatible hash partitioning. Not in 2.0.
5. **Deferred key types**, with the PostgreSQL rule to implement later: `bpchar` (trailing spaces insignificant, `bpcharcmp`); `timetz` (UTC-adjusted time, then zone offset, `timetz_cmp_internal`); `name`, `oid`, `"char"`; enums (by `enumsortorder`, not by label); composite/record values (`record_cmp`: column by column, NULL fields after non-NULL); ranges; `inet`/`cidr`; `money`; `tsvector`; `pg_lsn`. Each needs a spec section here before it gets an encoding.
6. **ICU** (§5.2): the sort-key format and version pinning are decided with D8, not here.

## Changelog
- draft 1 (2026-10-07): initial spec for C-K1/C-K2.
