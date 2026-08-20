#!/bin/sh
# Runs the Zig live executor on a Zig 0.15 toolchain.
# The SDK pins 0.15.2 (zig/build.zig.zon); plain `zig` on PATH may be 0.16,
# which does not compile the SDK. Prefer the brew keg when present, else
# whatever `zig` resolves to (CI pins 0.15).
ZIG=/opt/homebrew/opt/zig@0.15/bin/zig
[ -x "$ZIG" ] || ZIG=zig
exec "$ZIG" build run
