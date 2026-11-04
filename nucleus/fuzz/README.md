# Nucleus Fuzz Testing

Fuzz testing harnesses for Nucleus using [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) (libFuzzer).

## Setup

```bash
# Install cargo-fuzz (requires nightly)
cargo install cargo-fuzz
```

## Fuzz Targets

### SQL Parser

Feeds arbitrary UTF-8 strings into the SQL parser. The parser must never panic — invalid SQL should return `Err`, not crash.

```bash
cargo +nightly fuzz run fuzz_sql_parser -- -max_len=4096
```

### Wire Protocol

Decodes arbitrary bytes with the `pgwire` frontend decoder and drives Nucleus's own handling of the content: Bind parameter decoding (text and binary, per OID), placeholder substitution and the SQL-literal round trip, Parse parameter-type inference, `statement_timeout` parsing, and COPY text/CSV/binary row parsing. Byte 0 selects the mode (see the target's header).

```bash
cargo +nightly fuzz run fuzz_wire_protocol -- -max_len=4096
```

### RESP

Feeds arbitrary bytes, in fuzzer-chosen chunk sizes, to the RESP reader and command extractor (pre-auth surface).

```bash
cargo +nightly fuzz run fuzz_resp -- -max_len=4096
```

### Value Text

Casts arbitrary text to every scalar type Nucleus parses itself, plus array, INTERVAL and NUMERIC typmod/arithmetic paths, and checks that rendered values parse back.

```bash
cargo +nightly fuzz run fuzz_value_text -- -max_len=4096
```

The targets use the `fuzz-api` feature of `nucleusdb`, which exposes `wire::fuzz_api` and `executor::fuzz_api`. It is never enabled by the server build.

On a disk-constrained machine, build without sanitizers and with an explicit target dir:

```bash
CARGO_TARGET_DIR=/tmp/nucleus-fuzz-target cargo +nightly fuzz build -s none
```

## Running

```bash
# Run a specific target for 60 seconds
cargo +nightly fuzz run fuzz_sql_parser -- -max_total_time=60

# Run with multiple jobs (parallel)
cargo +nightly fuzz run fuzz_sql_parser -- -jobs=4 -workers=4

# Show coverage
cargo +nightly fuzz coverage fuzz_sql_parser
```

## Crash Triage

When a crash is found, the input is saved to `fuzz/artifacts/`. Reproduce with:

```bash
cargo +nightly fuzz run fuzz_sql_parser fuzz/artifacts/fuzz_sql_parser/<crash-file>
```
