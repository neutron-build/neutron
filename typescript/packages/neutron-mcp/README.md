# @neutron-build/mcp

Model Context Protocol for Neutron — server and client over streamable HTTP,
with pluggable auth and capability scoping.

The scope is the useful minimum: `initialize`, `ping`, `tools/list`,
`tools/call`. Resources, prompts, sampling and server-initiated messages are
absent because nothing needs them yet. SSE is absent because it is the transport
the specification has moved away from.

## Server

The handler is `(Request) => Promise<Response>`, so the same server mounts in a
Neutron `mode: "api"` route, a Node adapter, or a worker.

```ts
import { bearerAuthorizer, createMcpServer, objectSchema, stringProp } from "@neutron-build/mcp";

const server = createMcpServer({
  name: "my-service",
  version: "1.0.0",
  instructions: "What these tools mean together, not just what each one does.",
  authorize: bearerAuthorizer(async (token) => {
    const session = await lookup(token);
    return session ? { name: session.user, scopes: session.scopes } : null;
  }),
  tools: [
    {
      name: "get_thing",
      description: "Read one thing",
      inputSchema: objectSchema({ id: stringProp("which thing") }, ["id"]),
      readOnly: true,
      execute: async (args, ctx) => JSON.stringify(await read(args.id, ctx.principal)),
    },
  ],
});

export const POST = ({ request }) => server.handler(request);
```

`authorize` is required and there is no permissive default — that would turn one
forgotten line into an open remote-execution endpoint.

Scoping is an allow-list. A tool with a `scope` is hidden from a principal that
lacks it *and* refused if named anyway; hiding is a courtesy to well-behaved
clients, not access control.

## Client

```ts
import { createMcpClient, isToolError } from "@neutron-build/mcp";

const client = createMcpClient({ endpoint: "http://host/api/mcp", token });
await client.initialize();

try {
  const out = await client.callTool("deploy_app", { app: "web" });
} catch (err) {
  if (isToolError(err)) {
    // The deploy definitely did not happen. Retrying is usually wrong.
  } else {
    // Transport failure: we do not know whether anything happened.
  }
}
```

That distinction is the reason `McpToolError` exists. A failed tool comes back as
a **successful** JSON-RPC response carrying `isError`, so a client that checks
only the JSON-RPC error field reads a failed deploy as a success.

## Bridging to @neutron-build/ai

This package has no dependency on the AI SDK, so it emits raw JSON Schema.
Bridging is one line at the call site:

```ts
import { jsonSchema, tool } from "@neutron-build/ai";

const remote = await client.toolDescriptors();
const tools = remote.map((t) =>
  tool({
    name: t.name,
    description: t.description,
    inputSchema: jsonSchema(t.inputSchema),
    needsApproval: t.destructive,
    execute: (args) => client.callTool(t.name, args as Record<string, unknown>),
  }),
);
```

`destructive` becoming `needsApproval` is the point: a remote tool that changes
something arrives already carrying the fact that a human should see it first,
rather than that judgment being re-made by hand per integration.
