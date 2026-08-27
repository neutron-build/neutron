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

function matchesType(type: string, value: unknown): boolean {
  switch (type) {
    case "object":
      return typeof value === "object" && value !== null && !Array.isArray(value);
    case "array":
      return Array.isArray(value);
    case "string":
      return typeof value === "string";
    case "boolean":
      return typeof value === "boolean";
    case "null":
      return value === null;
    case "number":
      return typeof value === "number" && !Number.isNaN(value);
    case "integer":
      return typeof value === "number" && Number.isInteger(value);
    default:
      return true; // unknown type words do not constrain
  }
}

/**
 * First violation of a JSON-Schema subset (type, required, properties,
 * enum, items, additionalProperties: false, and the usual bounds), or null
 * when the value conforms. Mirrors how @neutron-build/ai's tool loop
 * validates model-given inputs before execute — name the first problem,
 * `path: message` — without taking a dependency on that package.
 */
function firstArgumentViolation(
  schema: Record<string, unknown>,
  value: unknown,
  path: string,
): string | null {
  const type = schema.type;
  if (typeof type === "string" && !matchesType(type, value)) {
    return `${path}: expected ${type}`;
  }

  const options = schema.enum;
  if (Array.isArray(options) && !options.some((option) => option === value)) {
    return `${path}: must be one of ${options.map((option) => JSON.stringify(option)).join(", ")}`;
  }

  if (typeof value === "string") {
    if (typeof schema.minLength === "number" && value.length < schema.minLength) {
      return `${path}: shorter than ${schema.minLength} characters`;
    }
    if (typeof schema.maxLength === "number" && value.length > schema.maxLength) {
      return `${path}: longer than ${schema.maxLength} characters`;
    }
  }

  if (typeof value === "number") {
    if (typeof schema.minimum === "number" && value < schema.minimum) {
      return `${path}: must be >= ${schema.minimum}`;
    }
    if (typeof schema.maximum === "number" && value > schema.maximum) {
      return `${path}: must be <= ${schema.maximum}`;
    }
  }

  if (Array.isArray(value)) {
    if (typeof schema.minItems === "number" && value.length < schema.minItems) {
      return `${path}: needs at least ${schema.minItems} items`;
    }
    if (typeof schema.maxItems === "number" && value.length > schema.maxItems) {
      return `${path}: allows at most ${schema.maxItems} items`;
    }
    const items = schema.items;
    if (typeof items === "object" && items !== null && !Array.isArray(items)) {
      for (let i = 0; i < value.length; i++) {
        const violation = firstArgumentViolation(items as Record<string, unknown>, value[i], `${path}[${i}]`);
        if (violation !== null) return violation;
      }
    }
  }

  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    if (Array.isArray(schema.required)) {
      for (const key of schema.required) {
        if (typeof key === "string" && !(key in record)) {
          return `${path}: missing required property "${key}"`;
        }
      }
    }
    const properties = schema.properties;
    if (typeof properties === "object" && properties !== null && !Array.isArray(properties)) {
      for (const [key, child] of Object.entries(properties)) {
        if (key in record && typeof child === "object" && child !== null) {
          const violation = firstArgumentViolation(child as Record<string, unknown>, record[key], `${path}.${key}`);
          if (violation !== null) return violation;
        }
      }
      if (schema.additionalProperties === false) {
        for (const key of Object.keys(record)) {
          if (!(key in properties)) {
            return `${path}: unexpected property "${key}"`;
          }
        }
      }
    }
  }

  return null;
}

/**
 * Read a stream to text, stopping at `limit` bytes. The bound must bound the
 * read itself: `await request.text()` buffers the whole body first, so a check
 * placed after it does nothing about memory. Content-length is checked before
 * reading when present; chunked bodies are capped mid-stream.
 */
async function readTextWithLimit(
  body: ReadableStream<Uint8Array> | null,
  limit: number,
): Promise<{ text: string; oversize: boolean }> {
  if (!body) return { text: "", oversize: false };
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let text = "";
  let seen = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    seen += value.byteLength;
    if (seen > limit) {
      await reader.cancel().catch(() => {});
      return { text: "", oversize: true };
    }
    text += decoder.decode(value, { stream: true });
  }
  return { text: text + decoder.decode(), oversize: false };
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

      const declaredLength = Number(request.headers.get("content-length") ?? Number.NaN);
      if (declaredLength > MAX_REQUEST_BODY) {
        return rpcErrorResponse(null, RPC_PARSE_ERROR, "request body too large");
      }
      const read = await readTextWithLimit(request.body, MAX_REQUEST_BODY);
      if (read.oversize) {
        return rpcErrorResponse(null, RPC_PARSE_ERROR, "request body too large");
      }
      const raw = read.text;

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

          // Arguments are validated against the advertised schema before they
          // reach execute — the same rule the AI SDK's tool loop applies to
          // model-given inputs. No advertised schema means no validation, the
          // pre-existing contract.
          if (tool.inputSchema !== undefined) {
            const violation = firstArgumentViolation(tool.inputSchema, params.arguments ?? {}, "arguments");
            if (violation !== null) {
              log?.warn("neutron-mcp: invalid arguments", { tool: tool.name, principal: principal.name });
              return json({
                jsonrpc: "2.0",
                id,
                result: errorResult(`Invalid arguments for "${tool.name}": ${violation}`),
              });
            }
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
