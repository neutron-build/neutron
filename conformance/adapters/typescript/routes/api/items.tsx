// The one route the TypeScript conformance app serves.
//
// It exists so `mw.compression` has something to compress. The adapter
// otherwise boots with no route tree, so the gzip probe hit a 404 and the
// dimension reported `skip` — recorded in conformance/known-skips.json as the
// one of TypeScript's seven that is an ADAPTER gap rather than an SDK gap:
// `createServer` enables hono/compress by default and the middleware was
// always there, the harness simply gave it nothing to act on.
//
// Deliberately the same path and shape the Go, Rust, Python and Elixir
// conformance apps serve at /api/items, so the five are probed identically.
// The body is ~50 repetitive records: comfortably over any compression
// threshold and highly compressible, so a failure means the middleware did not
// run rather than that the payload was not worth compressing.
export const config = { mode: "app" };

export async function loader() {
  const items = Array.from({ length: 50 }, (_, i) => ({
    id: i + 1,
    name: `conformance-item-${i + 1}`,
    price: i + 1,
  }));

  throw new Response(JSON.stringify(items), {
    status: 200,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}
