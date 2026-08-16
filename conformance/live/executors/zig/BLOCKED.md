# The Zig executor is written but cannot be built

`src/main.zig` implements the full spec vocabulary against the in-repo Zig
client. It is not registered in `runner/run.mjs` because it does not compile,
and the reason is a toolchain migration rather than anything about conformance.

## What blocks it

The Zig SDK targets **Zig 0.14** — that is what `.github/workflows/zig.yml`
pins. **Zig 0.16 redesigned the standard library** around an explicit `Io`
parameter:

| 0.14 | 0.16 |
|---|---|
| `std.net` | `std.Io.net` |
| `std.Thread.Mutex` | `std.Io.Mutex` |
| `std.time.nanoTimestamp` | `Io.now(Clock)` |
| `std.io` | `std.Io` |
| `std.fs.cwd()` | `std.Io.Dir.cwd()`, needs an `Io` |
| `std.os.environ`, `std.posix.getenv` | `std.process.Init.environ_map` |
| `std.crypto.random` | moved |

`zig/src/layer1` (TCP, pool, timer) and `layer3` (cache, middleware, jwt) use the
0.14 spellings throughout, and the Nucleus client reaches them through
`layer2/pg_client`. So the executor cannot link on 0.16.

**And Zig 0.14 cannot build on this machine at all**: linking fails with
`undefined symbol: __availability_version_check` against the current macOS SDK.

So there is no toolchain on which the executor can currently be run: 0.14 cannot
link, 0.16 cannot compile the SDK.

## What was fixed along the way

`zig/src/nucleus/sql.zig` had a real bug that 0.14 merely failed to catch —
`structFields` returned `&names` where `names` was a stack local, which 0.16
correctly rejects as "returning address of expired local variable". Fixed; the
library now builds on 0.16 even though the layers below it do not.

Nine KV runtime wrappers were also added (`hdel`, `hexists`, `hlen`, `hgetall`,
`sadd`, `srem`, `smembers`, `zadd`, `zrange`). Every one already had its `*Sql`
builder; only the runtime wrapper was missing, so hashes, sets and sorted sets
were unreachable without hand-writing the call.

## What finishing it needs

Porting `zig/src` layers 0–3 from 0.14 to 0.16, which means threading an `Io`
value through every function that opens a socket, takes a lock or reads a clock.
That is an architectural change to the SDK, not a set of renames, and it should
be its own piece of work with `zig build test` green on 0.16 and the CI matrix
moved off 0.14 in the same change.
