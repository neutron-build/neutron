# Neutron Server Modes — Design & Implementation

**Status:** Implemented (Phase 1 + Phase 2 shipped together)
**Scope:** Extends `createServer` in `typescript/packages/neutron/src/server/index.ts` to support non-SSR backend patterns (JSON APIs, raw Hono backends) and an orthogonal WebSocket transport.
**Supersedes:** The original 4-mode proposal (`ssr | api | realtime | raw`). See §13 for why the design changed during review.

---

## 1. Problem statement

Neutron's `createServer` was structurally SSR-only. Every request terminated through an `app.all("*")` catch-all that ran loaders/actions and rendered HTML. There was no first-class way to use Neutron for:

- **WebSocket servers** — live deploy logs, real-time chat, market data, AR/XR relay
- **Pure API gateways** — no HTML rendering, just JSON
- **Raw Hono backends** — custom request handling, no Neutron opinions

This forced the portfolio projects that need these patterns to bypass Neutron entirely — Teploy (live deploy logs), Fylun (LLM streaming + chat), Akiroo (agent streaming), Omni Analyst (market data), the spatial AR platform (WS relay to OpenXR clients). The pattern recurs across 5 projects, not a one-off.

Principle: **Neutron is a full-stack meta-framework, not just an SSR one.** Real-time and API backends are general-purpose patterns, and supporting them strengthens Neutron's positioning without bloating the core.

## 2. The design: two orthogonal axes

The original proposal used a single `mode` enum with four values. Review found that this **conflated two independent decisions** — *how a request is rendered* and *whether the server also speaks WebSocket* — and as a result could not express the one hybrid the use-case table actually required (an SSR dashboard that *also* streams over WS; see the old "Chip 3+: ssr + realtime" row). The shipped design splits these into two orthogonal options:

### Axis 1 — `mode` (rendering)

```ts
mode?: "ssr" | "api" | "raw"   // default "ssr"
```

| Mode | Route discovery + Vite SSR | Asset / image / island routes | HTML catch-all | 404 behavior |
|------|---------------------------|-------------------------------|----------------|--------------|
| `"ssr"` (default) | yes | yes | yes (renders HTML) | SSR/static pipeline |
| `"api"` | **no** | **no** | **no** | `app.notFound` → clean JSON `{ error, path }` |
| `"raw"` | **no** | **no** | **no** | Hono default (`404 Not Found`) |

All three modes keep the **shared batteries**: the request-id middleware (FRAMEWORK_CONTRACT §5), `GET /health`, opt-in trusted-host / CORS / security headers, and opt-out compression. `api` and `raw` differ only in the 404 shape — `api` is the "batteries-included JSON backend," `raw` is "bare Hono, you own everything."

### Axis 2 — `websocket` (transport)

```ts
websocket?: boolean | { path?: string }   // default off
```

Orthogonal to `mode`. Setting it attaches a WebSocket server to the returned HTTP server; the returned object gains a `wss`. Because it's a separate axis, **every combination is expressible**:

| Options | Result |
|---------|--------|
| `{}` | SSR (unchanged default) |
| `{ websocket: true }` | SSR dashboard **+ WS** (the previously-impossible hybrid) |
| `{ mode: "api" }` | JSON API |
| `{ mode: "api", websocket: true }` | JSON API + WS (the old "realtime") |
| `{ mode: "raw", websocket: true }` | bare Hono + WS (relay servers) |
| `{ mode: "raw" }` | bare Hono |

`websocket: true` accepts upgrades on any path (route inside the connection handler via `req.url`). `websocket: { path: "/ws" }` restricts upgrades to that pathname; other upgrade attempts get the socket destroyed.

## 3. Return shape

`createServer` now has an explicit return type (previously inferred):

```ts
export interface NeutronServer {
  app: Hono<{ Variables: { requestId: string } }>;
  server: ServerType;                 // @hono/node-server http.Server
  wss?: WebSocketServer;              // present only when `websocket` is set
  close: () => Promise<void>;          // drains HTTP and closes wss
  url: string;
}
```

Adding `wss` is non-breaking — existing callers destructure `{ app, server, close, url }` unchanged. `close()` forcibly terminates live WS sockets (`wss.clients`) and awaits the WS server's close *before* draining HTTP — an upgraded WS socket is not an idle keep-alive, so without this the drain would hang on any connected client until the caller's shutdown timeout. `startServer` now returns the same `NeutronServer` handle (previously `void`), so realtime callers can attach `wss.on("connection", ...)` and still get its signal-driven 30s graceful shutdown.

## 4. WebSocket implementation — raw `ws`, not a Hono adapter

The original plan recommended `@hono/node-server/ws` (its "Option C"). **That subpath does not exist** — verified against the installed `@hono/node-server@1.19.14`: its `exports` map has only `.`, `./serve-static`, `./vercel`, `./utils/*`, `./conninfo`. There is no `/ws` export and no ws file in `dist/`. (The plan's Appendix A claim that the Chip-1 server already imports it was therefore incorrect.) `@hono/node-ws` is a *different* package with a *different* API — `upgradeWebSocket` route handlers wrapped in Hono's `WSContext`, which has **no** `wss.on("connection")`. So the plan was internally contradictory: it promised a `wss` with `.on("connection")` (a raw-`ws` API) while recommending an adapter that doesn't provide one.

Resolution: use the **`ws` package directly** in `noServer` mode. It is the only option that (a) actually exists, (b) matches the promised `wss.on("connection", (ws, req) => ...)` shape, and (c) suits the portfolio's hub/relay/broadcast use cases (deploy logs, market data, AR relay) better than per-route `WSContext` handlers. The framework owns the upgrade handshake; the caller just attaches connection handlers.

```ts
let wss: WebSocketServer | undefined;
if (websocket) {
  const { WebSocketServer: WSServer } = await import("ws");   // dynamic — zero cost unless used
  const wsOptions = websocket === true ? {} : websocket;
  wss = new WSServer({ noServer: true });
  const httpServer = server as unknown as import("node:http").Server;
  httpServer.on("upgrade", (req, socket, head) => {
    if (wsOptions.path) {
      const pathname = new URL(req.url ?? "/", "http://localhost").pathname;
      if (pathname !== wsOptions.path) { socket.destroy(); return; }
    }
    wss!.handleUpgrade(req, socket, head, (ws) => wss!.emit("connection", ws, req));
  });
}
```

`ws` is added as a direct dependency (`^8.18.0`) with `@types/ws` as a dev dependency. `ws@8.21.0` was already present transitively, so the install footprint is effectively types-only.

**Known boundary (by design):** WS upgrades are handled on the HTTP server's `upgrade` event, *not* through `app.fetch`, so they bypass the Hono middleware stack — request-id, CORS, security headers, and `trustedHosts` do not apply to the handshake. This matches how bare `ws` behaves. Origin/auth checking for sockets is the caller's job inside `wss.on("connection", (ws, req) => ...)` (inspect `req.headers.origin`). Left deliberately un-opinionated rather than bolting on a partial origin allowlist.

## 5. Corrections to the original plan (verified against code)

| Original claim | Reality | Fix |
|---|---|---|
| `discoverRoutes` is `await`ed (§5 step 3) | It is **synchronous**: `const routes = discoverRoutes({ routesDir })` (index.ts) | Gated as `const routes = isSsr ? discoverRoutes(...) : []` — no `await` |
| Use `@hono/node-server/ws` `createNodeWebSocket` (Option C) | Subpath **does not exist** in `@hono/node-server` | Use raw `ws` (§4) |
| Return a `wss` *and* use the Hono WS adapter | The adapter has no `.on("connection")` — contradictory | Raw `ws` gives a real `WebSocketServer` |
| `api` mode 404 via `app.all("*")` | An `"*"` route **shadows** routes the caller mounts later on the returned `app` | Use `app.notFound()`, which fires only when nothing matched |
| `api` mode runs route discovery | Unnecessary once route-rules are skipped for non-ssr (Open Q #2) | `api` skips discovery, SSR runtime, and asset routes entirely |
| 4-mode enum | Cannot express SSR+WS | Split into `mode` × `websocket` (§2) |

## 6. What shipped

All edits in `typescript/packages/neutron/`:

- **`src/server/index.ts`**
  - New types: `NeutronServerMode`, `NeutronWebSocketOptions`, `NeutronServer`; new `mode` and `websocket` fields on `NeutronServerOptions`.
  - `const isSsr = mode === "ssr"` gates route discovery, the Vite SSR runtime, the asset/image/island routes, and the 245-line HTML catch-all.
  - `api` mode adds `app.notFound(...)` JSON 404; `raw` adds nothing.
  - WebSocket wiring (§4) after `serve()`; `close()` terminates live WS clients then closes `wss` before draining HTTP.
  - Explicit `Promise<NeutronServer>` return type; `startServer` returns the handle too.
- **`src/index.ts`** — re-exports `NeutronServerMode`, `NeutronWebSocketOptions`, `NeutronServer`.
- **`src/server/hono.ts`** — **deleted.** `createNeutronHono` was dead code (defined, never imported, not in package exports). See Appendix B for why it wasn't a viable alternative anyway.
- **`package.json`** — `ws` dependency, `@types/ws` dev dependency.
- **`src/server/server-modes.test.ts`** — new suite (6 tests): raw-mode batteries + default 404 + no-throw on missing routes dir; api-mode JSON 404 + non-shadowed user routes; default-mode `wss` undefined; `websocket: true` echo; `websocket: { path }` upgrade gating; `close()` drains promptly with a live WS connection.

**Verification:** `tsc` clean (noEmit + emit). Full suite green — **236 tests / 38 files**, including the three SSR e2e suites (`e2e-matrix`, `protocol-e2e`, `observability-hooks`) that exercise the default path unchanged.

## 7. Use-case mapping (portfolio)

| Project | Options | Reason |
|---|---|---|
| Teploy dashboard | `{}` | Marketing + admin UI (SSR sweet spot) |
| Teploy deploy service | `{ mode: "raw", websocket: true }` | Live deploy-log / build-status relay |
| Fylun web app | `{}` | Chat/dashboard SSR UI |
| Fylun AI backend | `{ mode: "api", websocket: true }` | JSON API + LLM/chat streaming |
| Akiroo SaaS frontends | `{}` | Per-module SSR UIs |
| Akiroo agent/stream backends | `{ mode: "raw", websocket: true }` | Agent streaming, transcription |
| Omni Analyst terminal | `{}` | Bloomberg-style dashboard |
| Omni Analyst data engine | `{ mode: "raw", websocket: true }` | Market-data streaming, sim events |
| Spatial Chip 1 (AR) | `{ mode: "raw", websocket: { path: "/relay" } }` | WS relay to OpenXR clients |
| Spatial Chip 3+ | `{ websocket: true }` | SSR dashboard **+** relay in one server |
| Future API gateways | `{ mode: "api" }` | JSON only |
| Future custom backends | `{ mode: "raw" }` | Full control |

The framework's pitch expands from "Astro/Next.js competitor" to "Astro/Next.js + Hono/Express competitor with a built-in database."

## 8. Migration (non-breaking)

- Default behavior is byte-for-byte unchanged; every existing `createServer({...})` call and every existing test passes untouched.
- New backends opt in with `mode` / `websocket`.
- `npm create neutron@latest` still scaffolds an SSR app.
- `start.ts` / `preview.ts` forward `neutronConfig.server` wholesale, so the new options flow through config automatically — no CLI changes needed.

## 9. Risks & mitigations

- **API surface fragmentation.** Two axes, but each is a single clear question (render-how / WS-or-not). Docs lead with SSR; `api`/`raw`/`websocket` are opt-in specializations.
- **WS maintenance burden.** Real, but contained: raw `ws` in `noServer` mode is a thin, well-understood ~15-line handshake with no adapter version-coupling. Lifecycle (drain on `close()`) is covered by tests.
- **Hidden coupling in the catch-all.** Mitigated by the green e2e suites: `router`/`ssrServer` are only consumed inside the gated catch-all, so gating them out is provably side-effect-free for `api`/`raw`.
- **Default drift.** All docs/examples default to SSR; non-SSR examples live in an "advanced backends" section.

## 10. Open questions — resolved

1. *Should `api`/`raw` serve assets?* **No.** Static serving is SSR's job.
2. *Should non-ssr modes apply route-rules redirects/rewrites?* **No.** Plain 404; callers own routing.
3. *Auto-register a `/ws` path, or leave it to the user?* **Configurable.** `websocket: true` = any path; `{ path }` = restricted. Routing within a connection is the caller's via `req.url`.
4. *SSE helper?* **Deferred.** Use Hono's built-in `c.streamSSE()`; revisit only if a shared helper proves worth it.
5. *Explicit return type?* **Yes** — shipped as `NeutronServer`.
6. *Delete `createNeutronHono`?* **Yes** — done.
7. *Flow `mode` through `start.ts`/`preview.ts`?* **Automatic** via `neutronConfig.server`.

## 11. Scope cuts (unchanged)

Not building a Hono competitor; not patching Hono; not changing the SSR dev loop, the build pipeline, or adapters; not adding pub/sub, queues, or a WS *client*. Modes are general backend patterns — no domain-specific "spatial" or "AI" modes. Hard cap on the rendering axis at three values.

## 12. Appendix A — Spatial Chip-1 server, post-migration

```ts
import { createServer } from "@neutron-build/core/server";

const { app, wss } = await createServer({
  mode: "raw",
  port: 8787,
  websocket: { path: "/relay" },
});

app.get("/health", (c) => c.json({ ok: true }));

wss!.on("connection", (ws, req) => {
  // panel streaming, pinch events, OpenXR relay
});
```

## 13. Appendix B — Why not just expose `createNeutronHono`?

The "simpler alternative" of publicly exporting the old `createNeutronHono` factory was rejected and the factory deleted:

1. It wired compression + 3 static-serving routes assuming `distDir` exists, silently 404ing otherwise — unusable for non-SSR backends.
2. It didn't call `serve()`, return a `server` handle, or integrate with `close()` — you'd need a parallel wrapper anyway.
3. It bypassed the shared middleware (request-id, `/health`, observability hooks) that lives inside `createServer`.
4. Two public entry points fragment the API worse than one `createServer` with explicit axes.

One entry point, one return type, one middleware stack, differences explicit in the options — strictly cleaner.

---

*Implemented and verified: tsc clean, 236/236 tests green.*
