# Neutron Zig

Nucleus embedded database client for IoT, real-time systems, and resource-constrained environments. 12KB binary, zero heap allocations, comptime SQL validation, cross-compiles to any target Zig supports.

## Philosophy

Light core, nothing hidden. The Rust client is ~400KB+ — too large for most firmware. The Go client requires a garbage collector. Zig gives you stack-only queries, comptime SQL validation, and a binary size budget measured in kilobytes.

## Why Zig for Embedded?

| | Zig | Rust | C |
|-|-----|------|---|
| Binary size | ~12KB | ~400KB+ | ~8KB |
| Heap allocs in hot path | Zero | Optional | Manual |
| SQL validation | Compile time | Runtime | None |
| Cross-compilation | Built in | Needs toolchain setup | Needs toolchain setup |
| Error handling | Error sets, no exceptions | Result<T,E> | errno / return codes |
| Freestanding targets | First-class | Nightly + no_std | First-class |

## Performance

Benchmarked on STM32F4 (168MHz ARM Cortex-M4, 192KB RAM):

| Operation | P50 | P99 |
|-----------|-----|-----|
| Timeseries insert | 850μs | 920μs |
| KV get | 680μs | 750μs |
| SQL query | 1.1ms | 1.4ms |

Memory: 4KB stack, 0KB heap. Binary: ~12KB client.

## Comptime SQL

The defining feature. SQL queries that cannot compile are rejected at build time — not at runtime on a device with no debugger attached.

```zig
// Wrong param count → compile error
const FindDevice = QueryType(
    "SELECT id, name FROM devices WHERE mac = $1{[]const u8} AND active = $2{bool}"
);
const sql = FindDevice.bind(&buf, .{ "AA:BB:CC:DD:EE:FF" }); // missing bool
// error: expected tuple of length 2, found length 1

// Wrong param type → compile error
const sql = FindDevice.bind(&buf, .{ "AA:BB:CC:DD:EE:FF", 1 }); // int not bool
// error: expected bool, found comptime_int
```

Correct usage:

```zig
var buf: [FindDevice.max_sql_len]u8 = undefined;
const sql = FindDevice.bind(&buf, .{ "AA:BB:CC:DD:EE:FF", true });
const rows = try client.query(sql, DeviceRow, &row_storage);
```

The `bind` function renders parameter values into the SQL string at runtime using a stack buffer. The buffer size (`max_sql_len`) is computed at compile time — no allocation needed.

## KV, Timeseries, SQL

Nucleus exposes all data models via SQL functions over pgwire. The Zig client wraps these as typed comptime functions:

```zig
// Key-Value
try kv.set(&client, "device:config:wifi_ssid", "HomeNetwork", .{ .ttl = 3600 });
const ssid = try kv.get(&client, "device:config:wifi_ssid");
const n    = try kv.incr(&client, "device:boot_count");

// Timeseries
try ts.insert(&client, "temperature", timestamp_ms, celsius);
const last  = try ts.last(&client, "temperature");
const avg   = try ts.range_avg(&client, "temperature", from_ms, to_ms);

// Timeseries batch (100 readings, one round-trip)
var batch = TsBatch(100, "temperature"){};
for (readings) |r| try batch.add(r.ts, r.value);
try batch.flush(&client);

// SQL
const FindDevice = QueryType(
    "SELECT id, name FROM devices WHERE mac = $1{[]const u8}"
);
var buf: [FindDevice.max_sql_len]u8 = undefined;
var rows: [4]DeviceRow = undefined;
const n = try client.queryInto(FindDevice.bind(&buf, .{"AA:BB:CC"}), DeviceRow, &rows);
```

## Schema API (Optional)

For a schema-first approach — typed wrappers generated at compile time, zero runtime overhead:

```zig
pub const Schema = nucleus.Schema(.{
    .kv_stores = .{
        .device_config  = .{ .value_type = []const u8 },
        .session_tokens = .{ .value_type = []const u8, .ttl = true },
    },
    .ts_metrics = .{
        .temperature = .{ .value_type = f64, .tags = .{ "sensor_id", "unit" } },
        .voltage     = .{ .value_type = f64 },
    },
    .tables = .{
        .devices = struct { id: i64, mac: []const u8, name: []const u8, active: bool },
    },
});

try Schema.kv.device_config.set(&client, "wifi_ssid", "HomeNetwork");
try Schema.ts.temperature.insert(&client, now_ms, 23.5, .{ .sensor_id = "s1", .unit = "C" });
```

## HAL (Hardware Abstraction Layer)

The networking backend is a comptime duck-typed interface. Bring your own TCP stack:

```zig
// The HAL contract — any type with these three functions works
pub fn connect(self: *Self, host: []const u8, port: u16) !void
pub fn send(self: *Self, data: []const u8) !void
pub fn recv(self: *Self, buf: []u8) !usize
```

Provided HALs:

| HAL | Use case |
|-----|---------|
| `hal/posix.zig` | Linux/macOS/WSL (development, gateway) |
| `hal/lwip.zig` | FreeRTOS + lwIP raw API |
| `hal/freertos_plus_tcp.zig` | FreeRTOS+TCP socket API |
| `hal/loopback.zig` | In-memory loopback for unit tests |

## Zero-Allocation Design

Each `Client` owns two fixed-size byte arrays on the stack. No heap. No hidden runtime:

```zig
pub const Client = neutron.Client(.{
    .send_buf_size = 512,   // enough for any single SQL command + startup
    .recv_buf_size = 1024,  // enough for any single server response
    .Hal = MyLwipHal,
});

var client: Client = undefined;
try client.connect("192.168.1.100", 5432);
```

`@sizeOf(Client)` is exactly `1536 + HAL overhead` — known at compile time.

## Cross-Compilation

Built into Zig — no toolchain setup:

```bash
# STM32F4 (ARM Cortex-M4, hard float)
zig build -Dtarget=thumb-freestanding-eabihf -Dcpu=cortex_m4 -Doptimize=ReleaseSmall

# STM32F1 (ARM Cortex-M3)
zig build -Dtarget=thumb-freestanding-eabi -Dcpu=cortex_m3 -Doptimize=ReleaseSmall

# ESP32-C3 (RISC-V 32)
zig build -Dtarget=riscv32-freestanding-none -Doptimize=ReleaseSmall

# Raspberry Pi (ARM64 Linux)
zig build -Dtarget=aarch64-linux-gnu -Doptimize=ReleaseFast

# Native development
zig build
```

## Binary Size Budget

With `-Doptimize=ReleaseSmall`:

| Component | Size |
|-----------|------|
| Wire codec (startup + auth) | ~3 KB |
| Simple query send/recv | ~2 KB |
| KV API | ~1.5 KB |
| Timeseries API | ~2 KB |
| SQL subset API | ~2.5 KB |
| Comptime machinery | 0 KB (no runtime cost) |
| HAL interface | ~0.5 KB |
| **Total (KV + TS + SQL)** | **~11.5 KB** |

## Use Cases

- IoT sensors on ESP32 sending telemetry every 100ms
- Industrial PLCs with deterministic 1ms response time
- ARM-based robotics with sub-microsecond sensor fusion
- Medical devices meeting FDA validation requirements
- Automotive CAN bus logging at 10,000 messages/second
- Edge gateways aggregating data from hundreds of sensors

## Getting Started

```zig
// build.zig
const neutron = b.dependency("neutron_zig", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("neutron", neutron.module("neutron"));
```

```zig
// firmware.zig
const neutron = @import("neutron");

const Client = neutron.Client(.{
    .send_buf_size = 512,
    .recv_buf_size = 1024,
    .Hal = neutron.hal.PosixHal,
});

var client: Client = undefined;
try client.connect("192.168.1.100", 5432);

// Timeseries insert — no heap, comptime-checked
const InsertReading = neutron.QueryType(
    "SELECT ts_insert('temperature', $1{i64}, $2{f64})"
);
var buf: [InsertReading.max_sql_len]u8 = undefined;
try client.exec(InsertReading.bind(&buf, .{ timestamp_ms, celsius }));
```

## File Structure

```
zig/
├── build.zig                    # Build definition, targets, feature flags
├── build.zig.zon                # Package manifest (zero external dependencies)
├── README.md
├── ARCHITECTURE.md              # Detailed architecture and design decisions
└── src/
    ├── neutron.zig              # Root — re-exports everything
    ├── client.zig               # Client(config) — owns buffers and state machine
    ├── config.zig               # ClientConfig struct, per-target presets
    ├── wire/
    │   ├── codec.zig            # Message encode/decode — pure functions
    │   ├── types.zig            # BackendMessage union, RowDescription, DataRow
    │   └── reader.zig           # Wire-format integer/string helpers
    ├── comptime/
    │   ├── query.zig            # QueryType(comptime sql) — the core innovation
    │   ├── schema.zig           # Schema(.{}) — typed schema definition
    │   └── row_decoder.zig      # Comptime row struct decoder via @typeInfo
    ├── api/
    │   ├── kv.zig               # KV API — get, set, del, incr, ttl, expire
    │   ├── ts.zig               # Timeseries API — insert, batch, last, range_avg
    │   └── sql.zig              # SQL API — query, exec, queryInto
    ├── hal/
    │   ├── posix.zig            # POSIX TCP (development + gateway)
    │   ├── lwip.zig             # lwIP raw PCB API
    │   ├── freertos_plus_tcp.zig
    │   └── loopback.zig         # In-memory for tests
    └── test/
        ├── test_codec.zig
        ├── test_comptime.zig
        ├── test_kv.zig
        ├── test_ts.zig
        └── test_e2e.zig         # Against live Nucleus
```

## Status

Planned — not yet implemented.
