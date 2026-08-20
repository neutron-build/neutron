# Neutron Zig

Zig SDK for the Neutron ecosystem: a layered systems library (wire formats up
to an application layer) plus a Nucleus client covering all 14 data models over
pgwire.

## Toolchain

Zig **0.15.2** — the exact version CI pins, `build.zig.zon` requires, and the
suite is verified on (320/320 tests, all layer combinations).

```bash
brew install zig@0.15   # keg-only: /opt/homebrew/opt/zig@0.15/bin/zig
```

Plain `brew install zig` gives 0.16, which does **not** build this SDK: 0.16
removed `std.io`, `std.net`, `std.Thread.Mutex`, `std.time.nanoTimestamp` and
`std.crypto.random`, all used throughout layers 1-3. Porting onto the
allocator-backed `std.Io` model is an open design decision, not a version
bump. On macOS 26/Xcode 26, the official ziglang.org 0.15.2 tarball also fails
to link (arm64e-only SDK stubs); brew's `zig@0.15` bottle works.

## Layers

| Layer | Contents |
|---|---|
| `src/layer0/` | Binary codecs (endian, varint), HTTP/1.1 parser, pgwire codec/auth/types, WebSocket framing |
| `src/layer1/` | TCP listener/stream, connection pool, timers |
| `src/layer2/` | HTTP server, pgwire client, SSE, static files, WebSocket server |
| `src/layer3/` | App/router/middleware, RFC 7807 errors, JWT, JSON, compression, cache, OpenAPI, lifecycle |
| `src/nucleus/` | Nucleus client: feature detection, transactions, retry, and one module per data model — SQL, KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, Columnar, Datalog, CDC, PubSub |

Each layer builds without the ones above it (`zig build -Dlayer2=false ...`,
exercised in CI), so an embedded consumer can take the wire layer alone.

## Nucleus client

```zig
const neutron = @import("neutron");

var client = try neutron.nucleus.NucleusClient.fromUrl(allocator, "postgres://localhost:5432/mydb");
try client.connect();   // detects Nucleus vs plain PostgreSQL via VERSION()

const k = client.kv();  // typed handle per model: .sql(), .vector(), .graph(), ...
```

Query results decode into comptime-typed structs via `@typeInfo`; errors
surface as RFC 7807 problem details. Layer 0 (codecs, parser, pgwire) is
zero-allocation throughout; the full SDK's client is allocator-backed (heap
connection pool, default capacity 25).

## Testing

```bash
zig build test
```

320 tests across the layers (codec round-trips, RFC 7807 serialization, JWT,
router, per-model request encoding, middleware order, signal handling) —
verified 320/320 on 0.15.2, all layer combinations. CI: `.github/workflows/zig.yml`.

---

*This file replaced a pre-implementation design document (2026-08-19). That
document described a different layout (`client.zig`, `comptime/`, `api/`,
`hal/` with lwIP/FreeRTOS HALs), a `QueryType` comptime SQL-string validation
API that was never built, "zero heap allocations", cross-compilation targets
(ARM/RISC-V/AVR/ESP32) with no cross-build evidence, an STM32 benchmark table
with no measurement artifact, and ended with "Status: Planned — not yet
implemented" — for a library that now ships 320 verified tests and a CI
workflow. Found by the S97 claims audit.*
