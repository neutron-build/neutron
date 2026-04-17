import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Zig - Neutron",
    description: "12 KB embedded SDK. Zero heap allocations on the hot path, comptime SQL validation, 307 tests. All 14 Nucleus data models in a binary that fits in L1 cache.",
  };
}

export default function ZigPage() {
  return (
    <ProductPage
      title="Neutron Zig"
      description="Embedded SDK for systems that measure binary size in kilobytes. Zero heap allocations on the hot path, comptime SQL validation, all 14 Nucleus data models &mdash; in 12 KB."
      category="language"
      status="available"
      accent="var(--accent-zig)"
      heroAccentRgb="247, 164, 29"
      heroTagline="Twelve kilobytes. Zero heap. All of Nucleus."
      stats={[
        { value: '12 KB', label: 'Release Binary' },
        { value: '0', label: 'Heap Allocs Hot Path' },
        { value: '307', label: 'Inline Tests' },
        { value: '40+', label: 'Target Architectures' },
      ]}
    >
      <section>
        <h2>Small enough to ship in firmware.</h2>
        <p>Most database clients assume you have a megabyte of RAM and a garbage collector. Neutron Zig assumes neither. The whole SDK &mdash; HAL, wire protocol, all 14 Nucleus model clients, JWT, compression, SSE &mdash; compiles to a 12 KB release binary with zero heap allocations on the hot path. It runs on your server, on your Raspberry Pi Pico, and on your mechanical keyboard.</p>
      </section>

      <CodeBlock filename="src/main.zig" annotation="Comptime-validated SQL. Zero allocations at runtime.">
        <pre><code>{`const std = @import("std");
const neutron = @import("neutron");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var db = try neutron.connect(gpa.allocator(), .{
        .url = "postgres://localhost/app",
    });
    defer db.deinit();

    // SQL is validated at comptime. This fails to compile if the
    // schema changes and the columns don't match.
    const rows = try db.sql(
        "SELECT id, title FROM articles WHERE views > $1",
        .{1000},
        struct { id: i64, title: []const u8 },
    );
    defer rows.deinit();

    for (rows.items) |row| {
        std.debug.print("{d}: {s}\\n", .{ row.id, row.title });
    }
}`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="247, 164, 29">
        <div class="feature-card">
          <div class="feature-card__title">Four-layer architecture</div>
          <div class="feature-card__desc">wire → HAL → protocols → app. Strip the layers you don't need at comptime. A pure KV client without SQL is under 4 KB.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Comptime SQL</div>
          <div class="feature-card__desc">SQL strings are parsed at compile time. Return types are inferred from the query. Column drift breaks the build, not production.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Zero heap hot path</div>
          <div class="feature-card__desc">All buffers are caller-provided or stack-allocated. The hot path never calls malloc. Allocator is explicit, injected at call sites.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">All 14 Nucleus models</div>
          <div class="feature-card__desc">SQL, KV, Vector, Graph, Documents, TimeSeries, FTS, Geo, Blob, Streams, Columnar, Datalog, CDC, PubSub &mdash; each a comptime-optional module.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">JWT + compression + SSE</div>
          <div class="feature-card__desc">Auth middleware, gzip compression, and server-sent events are in-tree. Compiled out of your binary if you don't import them.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">307 inline tests</div>
          <div class="feature-card__desc">Zig's <code>test</code> blocks live next to the code they exercise. Run <code>zig build test</code> and the whole SDK verifies in seconds.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>Binary size, at rest.</h2>
        <p>ReleaseSmall with link-time optimization, stripped. Measured on x86_64-linux. Same ballpark on aarch64, wasm32, and RISC-V.</p>
      </section>

      <BenchmarkBars
        title="Binary footprint"
        bars={[
          { label: 'KV only', value: '3.9 KB — just Nucleus KV client', width: 30, color: '#F7A41D' },
          { label: 'KV + SQL', value: '7.2 KB — add comptime SQL', width: 58, color: '#F9B64F' },
          { label: 'Full SDK', value: '12 KB — all 14 models + JWT + gzip + SSE', width: 100, color: '#FBC881' },
          { label: 'Rust client', value: '~2.1 MB — for reference', width: 4, color: '#555555' },
          { label: 'Go client', value: '~8.4 MB — for reference', width: 2, color: '#444444' },
        ]}
      />

      <section>
        <h3>What it's for</h3>
        <p>Firmware that needs to write telemetry straight into Nucleus. Edge devices where every kilobyte matters. CLI tools that want a real database client without shipping a JVM. WASM modules running in a serverless edge. Anything where you'd reach for C but would rather have Zig's safety and <code>comptime</code>.</p>

        <h3>Why Zig?</h3>
        <p>Because <code>comptime</code> replaces macros, codegen, and half the reasons you reach for C++. Because the allocator-by-reference convention makes memory explicit instead of implicit. Because there's no hidden control flow &mdash; if it allocates, you wrote the allocator. For a database client that has to be small and predictable, it's the right tool.</p>

        <h3>Part of a bigger system</h3>
        <p>Stream telemetry from a Zig edge device into the Nucleus streams model. Serve dashboards from Neutron TypeScript. Train models in Neutron Mojo. Orchestrate in Go. Same contract, same database, same source of truth &mdash; from a microcontroller to a cluster.</p>
      </section>
    </ProductPage>
  );
}
