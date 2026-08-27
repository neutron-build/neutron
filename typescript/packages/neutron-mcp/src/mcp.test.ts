import assert from "node:assert/strict";
import test from "node:test";

import { createMcpClient, isToolError, McpToolError, validateEndpoint } from "./client.js";
import { negotiateProtocol, objectSchema, PROTOCOL_LATEST, protocolSupported, stringProp } from "./protocol.js";
import { bearerAuthorizer, createMcpServer, type McpServer, type Principal } from "./server.js";

/** Builds a server plus a client wired straight to its handler — no socket needed. */
function fixture(principal: Principal | null) {
  let calls = 0;
  const server: McpServer = createMcpServer({
    name: "test-server",
    version: "1.0.0",
    instructions: "A server for exercising the protocol.",
    authorize: () => principal,
    tools: [
      {
        name: "read_thing",
        description: "Read a thing",
        inputSchema: objectSchema({ id: stringProp("which thing") }, ["id"]),
        readOnly: true,
        execute: (_args, ctx) => {
          calls++;
          return `read by ${ctx.principal.name}`;
        },
      },
      {
        name: "destroy_thing",
        description: "Destroy a thing",
        scope: "destroy",
        destructive: true,
        execute: () => {
          calls++;
          return "destroyed";
        },
      },
      {
        name: "failing_thing",
        readOnly: true,
        execute: () => {
          calls++;
          throw new Error("the disk is on fire");
        },
      },
    ],
  });

  const client = createMcpClient({
    endpoint: "http://mcp.test/api/mcp",
    token: "tok",
    fetch: ((input: RequestInfo | URL, init?: RequestInit) =>
      server.handler(new Request(String(input), init))) as typeof globalThis.fetch,
  });

  return { client, server, calls: () => calls };
}

test("handshake reports identity, negotiated revision and instructions", async () => {
  const { client } = fixture({ name: "owner", scopes: ["destroy"] });
  const info = await client.initialize();
  assert.equal(info.serverInfo.name, "test-server");
  assert.equal(info.serverInfo.version, "1.0.0");
  assert.equal(info.protocolVersion, PROTOCOL_LATEST);
  // Instructions are the only place to say what the tools mean together.
  assert.ok(info.instructions);
  await client.ping();
});

test("listing follows registration order and advertises schema and hints", async () => {
  const { client } = fixture({ name: "owner", scopes: ["destroy"] });
  const tools = await client.listTools();
  assert.equal(tools.length, 3);
  assert.equal(tools[0]!.name, "read_thing");
  // A tool with no schema cannot be called, even though it is listed.
  assert.ok(tools[0]!.inputSchema);
  assert.equal(tools[1]!.annotations?.destructiveHint, true);
  assert.equal(tools[0]!.annotations?.readOnlyHint, true);
});

// The isError trap: a failing tool is a *successful* JSON-RPC response. A client
// reading only the JSON-RPC error field would treat this as a success, which for
// a rollback or a deploy is the worst available misreading.
test("a failing tool is never read as success", async () => {
  const { client } = fixture({ name: "owner", scopes: ["destroy"] });
  await assert.rejects(
    () => client.callTool("failing_thing"),
    (err: unknown) => {
      assert.ok(isToolError(err), "a tool failure must be distinguishable from a transport failure");
      assert.ok(err instanceof McpToolError);
      assert.match((err as McpToolError).message, /the disk is on fire/);
      return true;
    },
  );
});

test("a transport failure is not a tool failure", async () => {
  const client = createMcpClient({
    endpoint: "http://mcp.test/api/mcp",
    fetch: (() => Promise.reject(new Error("connection refused"))) as typeof globalThis.fetch,
  });
  await assert.rejects(
    () => client.callTool("read_thing"),
    (err: unknown) => {
      // Conflating the two makes a caller retry the wrong things.
      assert.ok(!isToolError(err));
      return true;
    },
  );
});

test("scope is enforced on the call, not only on the listing", async () => {
  const { client, calls } = fixture({ name: "viewer" }); // no scopes

  const tools = await client.listTools();
  assert.ok(!tools.some((t) => t.name === "destroy_thing"), "an unusable tool must not be listed");

  // Name it anyway: hiding is a courtesy, not access control.
  await assert.rejects(() => client.callTool("destroy_thing"), isToolError);
  assert.equal(calls(), 0, "the refused tool must not have run");
});

test("tools/call validates arguments against the advertised inputSchema", async () => {
  const { client, calls } = fixture({ name: "owner", scopes: ["destroy"] });

  await assert.rejects(
    () => client.callTool("read_thing", { id: 42 }),
    (err: unknown) => {
      assert.ok(isToolError(err), "invalid arguments are a tool error, not a transport one");
      assert.match((err as Error).message, /id/, "the violation must name the offending argument");
      return true;
    },
  );
  await assert.rejects(() => client.callTool("read_thing"), /id/); // missing required property
  assert.equal(calls(), 0, "an invalid call must not reach execute");

  assert.equal(await client.callTool("read_thing", { id: "x" }), "read by owner", "valid arguments pass through");
});

test("a tool without an advertised schema skips argument validation", async () => {
  // Back-compat: schema-less tools advertise objectSchema() in tools/list but
  // tools/call must not start rejecting callers that were fine before.
  const server = createMcpServer({
    name: "loose",
    version: "0",
    authorize: () => ({ name: "x" }),
    tools: [{ name: "echo_args", readOnly: true, execute: (args) => JSON.stringify(args) }],
  });
  const client = createMcpClient({
    endpoint: "http://mcp.test/",
    fetch: ((input: RequestInfo | URL, init?: RequestInit) =>
      server.handler(new Request(String(input), init))) as typeof globalThis.fetch,
  });

  assert.equal(await client.callTool("echo_args", { anything: "goes" }), JSON.stringify({ anything: "goes" }));
});

test("a read-only principal cannot mutate even when scoped for it", async () => {
  const { client, calls } = fixture({ name: "readonly", scopes: ["destroy"], readOnly: true });
  await assert.rejects(() => client.callTool("destroy_thing"), isToolError);
  assert.equal(calls(), 0);
  // Reads must still work.
  assert.equal(await client.callTool("read_thing", { id: "x" }), "read by readonly");
});

test("an unauthorized caller reaches nothing", async () => {
  const { client, calls } = fixture(null);
  await assert.rejects(() => client.callTool("read_thing"), /credential/);
  assert.equal(calls(), 0);
});

test("GET is refused and notifications are accepted", async () => {
  const { server } = fixture({ name: "owner" });

  const get = await server.handler(new Request("http://mcp.test/api/mcp"));
  assert.equal(get.status, 405, "this transport is request/response only");
  assert.equal(get.headers.get("allow"), "POST");

  const notification = await server.handler(
    new Request("http://mcp.test/api/mcp", {
      method: "POST",
      headers: { authorization: "Bearer tok", "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }),
    }),
  );
  assert.equal(notification.status, 202, "a notification expects no body back");
});

test("an unknown method is a protocol error; an unknown tool is a tool error", async () => {
  const { client, server } = fixture({ name: "owner" });

  const response = await server.handler(
    new Request("http://mcp.test/api/mcp", {
      method: "POST",
      headers: { authorization: "Bearer tok", "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "resources/list" }),
    }),
  );
  const body = (await response.json()) as { error?: { code: number } };
  assert.equal(body.error?.code, -32601, "an unimplemented method is method-not-found");

  // A well-formed call naming a missing tool is a tool failure, not a protocol one.
  await assert.rejects(() => client.callTool("no_such_tool"), isToolError);
});

test("tool descriptors carry destructive across as the approval signal", async () => {
  const { client } = fixture({ name: "owner", scopes: ["destroy"] });
  const descriptors = await client.toolDescriptors();
  const destroy = descriptors.find((d) => d.name === "destroy_thing");
  assert.ok(destroy, "the destructive tool must be listed to a scoped caller");
  // This is what a caller maps to needsApproval, so it must survive the trip.
  assert.equal(destroy!.destructive, true);
  assert.equal(destroy!.readOnly, false);
  assert.ok(destroy!.inputSchema, "a descriptor without a schema cannot become a usable tool");
});

test("a server with no tools still answers a listing", async () => {
  const server = createMcpServer({ name: "empty", version: "0", authorize: () => ({ name: "x" }) });
  const client = createMcpClient({
    endpoint: "http://mcp.test/",
    fetch: ((input: RequestInfo | URL, init?: RequestInit) =>
      server.handler(new Request(String(input), init))) as typeof globalThis.fetch,
  });
  assert.deepEqual(await client.listTools(), []);
});

test("bearerAuthorizer accepts only a well-formed bearer token", async () => {
  const auth = bearerAuthorizer((token) => (token === "good" ? { name: "ok" } : null));
  const cases: Array<[string | null, boolean, string]> = [
    ["Bearer good", true, "a valid token must pass"],
    ["Bearer bad", false, "an unknown token must fail"],
    ["Bearer ", false, "an empty token must fail rather than reach verify as an empty string"],
    ["good", false, "a token without the scheme must fail"],
    ["Basic good", false, "a different scheme must fail"],
    [null, false, "no header must fail, never be treated as absent-therefore-fine"],
  ];
  for (const [header, expected, why] of cases) {
    const request = new Request("http://mcp.test/", {
      method: "POST",
      headers: header ? { authorization: header } : {},
    });
    const principal = await auth(request);
    assert.equal(principal !== null, expected, `${why} (header ${String(header)})`);
  }
});

test("protocol negotiation honours what we speak and falls back otherwise", () => {
  assert.equal(negotiateProtocol("2024-11-05"), "2024-11-05");
  assert.equal(negotiateProtocol("1999-01-01"), PROTOCOL_LATEST);
  assert.equal(negotiateProtocol(undefined), PROTOCOL_LATEST);
  assert.ok(protocolSupported(PROTOCOL_LATEST));
});

test("endpoint validation accepts private addresses and rejects nonsense", () => {
  for (const ok of ["http://localhost:3456/api/mcp", "https://100.64.0.1/api/mcp", "http://10.0.0.5:8080"]) {
    assert.equal(validateEndpoint(ok), ok.replace(/\/+$/, ""), `${ok} should be accepted`);
  }
  for (const bad of ["", "   ", "not a url", "ftp://host/x", "/relative/path"]) {
    assert.throws(() => validateEndpoint(bad), `${bad} should be rejected`);
  }
  assert.equal(validateEndpoint("  http://host/api/mcp/  "), "http://host/api/mcp");
});

test("objectSchema omits an empty required list", () => {
  const schema = objectSchema({ name: stringProp("a name") }, ["name"]);
  assert.equal(schema.type, "object");
  assert.deepEqual(schema.required, ["name"]);
  assert.ok(objectSchema().properties, "the properties key must exist or clients see no arguments object");
  assert.equal("required" in objectSchema(), false, "an empty required list must be omitted, not sent as []");
});

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Settle-or-fail wrapper: a hang is a failing test, not a slow one. */
function withDeadline<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error(message)), ms)),
  ]);
}

/** A pull-driven body that never ends — memory pressure only if the reader drains it. */
function endlessBody(chunkBytes = 64 * 1024): ReadableStream<Uint8Array> {
  return new ReadableStream({
    async pull(controller) {
      // Yield between chunks: a purely synchronous pull loop starves timers,
      // which would turn a hang into a frozen event loop. The try/catch keeps
      // a post-cancel enqueue from surfacing as an unhandled rejection.
      await delay(0);
      try {
        controller.enqueue(new Uint8Array(chunkBytes));
      } catch {
        // canceled mid-pull
      }
    },
  });
}

/** Headers arrive immediately; the body drips one small chunk forever. */
function drippingBody(chunk: string, intervalMs: number): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    async pull(controller) {
      await delay(intervalMs);
      try {
        controller.enqueue(encoder.encode(chunk));
      } catch {
        // canceled mid-pull
      }
    },
  });
}

test("the request body limit bounds the read, not just the check afterwards", async () => {
  const { server } = fixture({ name: "owner" });
  const request = new Request("http://mcp.test/api/mcp", {
    method: "POST",
    headers: { authorization: "Bearer tok" },
    body: endlessBody(),
    // undici requires duplex for stream bodies; the TS lib here predates it.
    duplex: "half",
  } as RequestInit);
  const response = await withDeadline(
    server.handler(request),
    2000,
    "handler did not settle — it is draining an endless body",
  );
  const body = (await response.json()) as { error?: { code: number; message: string } };
  assert.equal(body.error?.code, -32700);
  assert.match(body.error?.message ?? "", /too large/);
});

test("the response body limit bounds the read, not just the check afterwards", async () => {
  const client = createMcpClient({
    endpoint: "http://mcp.test/api/mcp",
    fetch: (async () =>
      new Response(endlessBody(), { headers: { "content-type": "application/json" } })) as typeof globalThis.fetch,
  });
  await assert.rejects(
    () =>
      withDeadline(
        client.callTool("read_thing"),
        2000,
        "client did not settle — it is draining an endless response",
      ),
    /too large/,
  );
});

test("the per-call timeout covers the response body, not only the headers", async () => {
  const client = createMcpClient({
    endpoint: "http://mcp.test/api/mcp",
    timeoutMs: 100,
    fetch: (async () =>
      new Response(drippingBody("{}", 50), { headers: { "content-type": "application/json" } })) as typeof globalThis.fetch,
  });
  await assert.rejects(
    () =>
      withDeadline(
        client.callTool("read_thing"),
        2000,
        "client did not settle — the body read has no deadline",
      ),
    (err: unknown) => {
      assert.match((err as Error).message, /timed out|abort/i);
      return true;
    },
  );
});
