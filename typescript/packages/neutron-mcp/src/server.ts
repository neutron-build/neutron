/**
 * An MCP server as a fetch handler.
 *
 * The handler is `(Request) => Promise<Response>` — web standard, so the same
 * server mounts in a Neutron `mode: "api"` route, a Node adapter, or a worker
 * without a shim per host.
 *
 * Stateless by design: no sessions, no subscriptions, every request carries its
 * own credential. That is what lets the endpoint sit behind any load balancer,
 * or run in a dozen replicas, without any of them agreeing on anything.
 */

import {
  negotiateProtocol,
  objectSchema,
  RPC_INVALID_PARAMS,
  RPC_METHOD_NOT_FOUND,
  RPC_PARSE_ERROR,
  textContent,
  type Content,
  type RpcRequest,
  type RpcResponse,
  type ToolInfo,
  type ToolResult,
} from "./protocol.js";

/** Bounds an inbound message. Tool arguments are small; an unbounded read on an authenticated endpoint is still a way to spend a server's memory. */
const MAX_REQUEST_BODY = 1 << 20; // 1 MiB

/**
 * The authenticated caller and what it may do.
 *
 * `scopes` is an allow-list, not a deny-list. A tool whose scope is absent is
 * refused, because the failure mode of the other arrangement is a monitoring
 * integration restarting production.
 */
export interface Principal {
  name: string;
  scopes?: string[];
  readOnly?: boolean;
  /** Whatever the host needs to pass through to its own tools — an org id, a tenant, a user id. Never read here. */
  extra?: Record<string, unknown>;
}

/**
 * Resolves a request to a principal, or null to reject with 401.
 *
 * It receives the whole Request rather than a token string because deployments
 * disagree about where the credential lives — a header, a cookie, a signed
 * body, mutual TLS — and a package that assumed one would be unusable for the
 * rest.
 */
export type Authorizer = (request: Request) => Promise<Principal | null> | Principal | null;

/** Adapt a token-verifying function into an Authorizer. Most callers want this. */
export function bearerAuthorizer(
  verify: (token: string) => Promise<Principal | null> | Principal | null,
): Authorizer {
  return (request) => {
    const auth = request.headers.get("authorization") ?? "";
    if (!auth.startsWith("Bearer ")) return null;
    const token = auth.slice("Bearer ".length).trim();
    if (!token) return null;
    return verify(token);
  };
}

/** Context handed to a tool when it runs. */
export interface McpToolContext {
  principal: Principal;
  request: Request;
  signal: AbortSignal;
}

/**
 * One callable operation a server exposes.
 *
 * `scope` is the capability a caller must hold to see or invoke it; empty means
 * unrestricted, which is right only for reads already safe for anyone the
 * Authorizer let through.
 *
 * `readOnly` and `destructive` are advertised as hints and, for readOnly,
 * enforced. The hints exist so an agent runtime can decide whether a call needs
 * a human first — the same judgment `needsApproval` encodes in @neutron-build/ai.
 */
export interface McpTool {
  name: string;
  description?: string;
  inputSchema?: Record<string, unknown>;
  scope?: string;
  readOnly?: boolean;
  destructive?: boolean;
  /** Returning a rejected promise becomes an isError result, not a protocol error. */
  execute: (args: Record<string, unknown>, context: McpToolContext) => Promise<string> | string;
}

export interface McpServerOptions {
  name: string;
  version: string;
  /**
   * Optional prose telling an agent what this server is for. Worth writing: it
   * is the only place to say what the tools mean *together*, as opposed to what
   * each one does alone.
   */
  instructions?: string;
  /** Required. There is no permissive default, because that would turn one forgotten line into an open remote-execution endpoint. */
  authorize: Authorizer;
  tools?: McpTool[];
  logger?: { info: (msg: string, meta?: unknown) => void; warn: (msg: string, meta?: unknown) => void };
}

export interface McpServer {
  /** The fetch handler to mount. */
  handler: (request: Request) => Promise<Response>;
  /** Add tools after construction. Registration order is listing order. */
  register: (...tools: McpTool[]) => McpServer;
  /** Registered tools, in registration order. */
  tools: () => McpTool[];
}

function principalAllows(principal: Principal, tool: McpTool): boolean {
  if (principal.readOnly && !tool.readOnly) return false;
  if (!tool.scope) return true;
  return (principal.scopes ?? []).includes(tool.scope);
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function rpcErrorResponse(id: RpcRequest["id"], code: number, message: string): Response {
  const body: RpcResponse = { jsonrpc: "2.0", id: id ?? null, error: { code, message } };
  return json(body);
}

function errorResult(message: string): ToolResult {
  return { content: textContent(message), isError: true };
}

export function createMcpServer(options: McpServerOptions): McpServer {
  const order: string[] = [];
  const tools = new Map<string, McpTool>();
  const log = options.logger;

  const server: McpServer = {
    register(...added: McpTool[]) {
      for (const tool of added) {
        if (!tool.name || typeof tool.execute !== "function") continue;
        if (!tools.has(tool.name)) order.push(tool.name);
        tools.set(tool.name, tool);
      }
      return server;
    },
    tools() {
      return order.map((name) => tools.get(name)!);
    },
    handler: async (request: Request): Promise<Response> => {
      if (request.method !== "POST") {
        return new Response("the MCP endpoint accepts POST only", {
          status: 405,
          headers: { allow: "POST" },
        });
      }

      // Authorization comes before the body is read, let alone parsed: an
      // unauthenticated caller should not reach the decoder at all.
      let principal: Principal | null;
      try {
        principal = await options.authorize(request);
      } catch {
        principal = null;
      }
      if (!principal) {
        return new Response("unauthorized", {
          status: 401,
          headers: { "www-authenticate": `Bearer realm="${options.name}"` },
        });
      }

      const raw = await request.text();
      if (raw.length > MAX_REQUEST_BODY) {
        return rpcErrorResponse(null, RPC_PARSE_ERROR, "request body too large");
      }

      let rpc: RpcRequest;
      try {
        rpc = JSON.parse(raw) as RpcRequest;
      } catch {
        return rpcErrorResponse(null, RPC_PARSE_ERROR, "parse error");
      }

      // A notification expects no body back. Streamable HTTP wants 202, and
      // answering one with a result confuses clients that are not waiting.
      if (rpc.id === undefined || rpc.id === null) {
        return new Response(null, { status: 202 });
      }

      const id = rpc.id;
      switch (rpc.method) {
        case "initialize": {
          const params = (rpc.params ?? {}) as { protocolVersion?: string };
          const result: Record<string, unknown> = {
            protocolVersion: negotiateProtocol(params.protocolVersion),
            capabilities: { tools: {} },
            serverInfo: { name: options.name, version: options.version },
          };
          if (options.instructions) result.instructions = options.instructions;
          return json({ jsonrpc: "2.0", id, result } satisfies RpcResponse);
        }

        case "ping":
          return json({ jsonrpc: "2.0", id, result: {} } satisfies RpcResponse);

        case "tools/list": {
          // Hiding a tool the caller cannot use keeps an agent from planning
          // around something it will be refused.
          const visible: ToolInfo[] = server
            .tools()
            .filter((tool) => principalAllows(principal!, tool))
            .map((tool) => ({
              name: tool.name,
              description: tool.description,
              inputSchema: tool.inputSchema ?? objectSchema(),
              annotations: {
                readOnlyHint: tool.readOnly === true,
                destructiveHint: tool.destructive === true,
              },
            }));
          return json({ jsonrpc: "2.0", id, result: { tools: visible } } satisfies RpcResponse);
        }

        case "tools/call": {
          const params = rpc.params as { name?: string; arguments?: Record<string, unknown> } | undefined;
          if (!params || typeof params.name !== "string") {
            return rpcErrorResponse(id, RPC_INVALID_PARAMS, "invalid params");
          }
          const tool = tools.get(params.name);
          if (!tool) {
            return json({ jsonrpc: "2.0", id, result: errorResult(`unknown tool: ${params.name}`) });
          }
          // Permission is re-checked here, not merely at listing time. Hiding a
          // tool is a courtesy to well-behaved clients; it is not access
          // control, because nothing stops a caller naming a tool it never saw.
          if (!principalAllows(principal, tool)) {
            log?.warn("neutron-mcp: tool refused", { tool: tool.name, principal: principal.name });
            return json({
              jsonrpc: "2.0",
              id,
              result: errorResult(`"${tool.name}" is not permitted for "${principal.name}"`),
            });
          }

          log?.info("neutron-mcp: tool call", {
            tool: tool.name,
            principal: principal.name,
            destructive: tool.destructive === true,
          });

          try {
            const out = await tool.execute(params.arguments ?? {}, {
              principal,
              request,
              signal: request.signal,
            });
            const content: Content[] = textContent(typeof out === "string" ? out : JSON.stringify(out));
            return json({ jsonrpc: "2.0", id, result: { content } satisfies ToolResult });
          } catch (err) {
            // A tool that failed is reported in-band. See ToolResult for why
            // this must not become a JSON-RPC error.
            const message = err instanceof Error ? err.message : String(err);
            return json({ jsonrpc: "2.0", id, result: errorResult(message) });
          }
        }

        default:
          return rpcErrorResponse(id, RPC_METHOD_NOT_FOUND, `method not found: ${rpc.method}`);
      }
    },
  };

  if (options.tools) server.register(...options.tools);
  return server;
}
