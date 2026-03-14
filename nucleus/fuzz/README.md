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

Feeds arbitrary bytes into the PostgreSQL wire protocol decoder. Malformed messages should be handled gracefully.

```bash
cargo +nightly fuzz run fuzz_wire_protocol -- -max_len=1024
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
