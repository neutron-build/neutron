/**
 * Model Context Protocol wire types and negotiation.
 *
 * The surface is deliberately the useful minimum: initialize, ping, tools/list
 * and tools/call over streamable HTTP, request/response only. Resources,
 * prompts, sampling and server-initiated messages are absent because nothing
 * needs them yet, and a protocol surface nobody exercises is one nobody
 * maintains. SSE is absent because it is the transport the specification has
 * moved away from; inheriting it now would mean carrying it forever.
 */

/** The revision this package prefers when a client offers nothing, or nothing we speak. */
export const PROTOCOL_LATEST = "2025-06-18";

/**
 * Revisions a server will agree to speak. Older ones stay listed because
 * clients pinned to them are common and the differences do not reach the four
 * methods implemented here — refusing them would break working clients for no
 * behavioural gain.
 */
const SUPPORTED = new Set(["2025-06-18", "2025-03-26", "2024-11-05"]);

export function protocolSupported(version: string): boolean {
  return SUPPORTED.has(version);
}

/**
 * Pick the revision to answer with: the client's if we speak it, otherwise
 * ours. A mismatch is not an error — the client decides whether what came back
 * is acceptable.
 */
export function negotiateProtocol(requested: string | undefined): string {
  return requested && SUPPORTED.has(requested) ? requested : PROTOCOL_LATEST;
}

/** JSON-RPC 2.0 error codes. Tool failures deliberately do not use these. */
export const RPC_PARSE_ERROR = -32700;
export const RPC_INVALID_REQUEST = -32600;
export const RPC_METHOD_NOT_FOUND = -32601;
export const RPC_INVALID_PARAMS = -32602;
export const RPC_INTERNAL_ERROR = -32603;

export interface RpcRequest {
  jsonrpc: string;
  /** Absent or null means a notification: no reply is expected. */
  id?: string | number | null;
  method: string;
  params?: unknown;
}

export interface RpcError {
  code: number;
  message: string;
  data?: unknown;
}

export interface RpcResponse {
  jsonrpc: "2.0";
  id?: string | number | null;
  result?: unknown;
  error?: RpcError;
}

/** One piece of a tool's output. Only text is produced today. */
export interface Content {
  type: string;
  text?: string;
}

export function textContent(text: string): Content[] {
  return [{ type: "text", text }];
}

/**
 * The wire shape of tools/call.
 *
 * `isError` is the protocol's sharpest edge: a tool that fails returns a
 * *successful* JSON-RPC response carrying isError true. A client that checks
 * only the JSON-RPC error field reads a failed action as a success, which for
 * something like a rollback is the worst available misreading. The client in
 * this package checks both, and there is a test for exactly that.
 */
export interface ToolResult {
  content: Content[];
  isError?: boolean;
}

/** Advisory hints the protocol carries about what a tool does. */
export interface ToolAnnotations {
  readOnlyHint?: boolean;
  destructiveHint?: boolean;
}

/** A tool as a client sees it in tools/list. */
export interface ToolInfo {
  name: string;
  description?: string;
  inputSchema: Record<string, unknown>;
  annotations?: ToolAnnotations;
}

export interface ServerIdentity {
  name: string;
  version: string;
}

/** What a server reports at initialize. */
export interface InitializeResult {
  protocolVersion: string;
  capabilities: Record<string, unknown>;
  serverInfo: ServerIdentity;
  instructions?: string;
}

/** Build an object JSON Schema. Hand-written schema maps are where typos become uncallable tools. */
export function objectSchema(
  properties: Record<string, unknown> = {},
  required: string[] = [],
): Record<string, unknown> {
  const schema: Record<string, unknown> = { type: "object", properties };
  if (required.length > 0) schema.required = required;
  return schema;
}

export const stringProp = (description: string) => ({ type: "string", description });
export const intProp = (description: string) => ({ type: "integer", description });
export const boolProp = (description: string) => ({ type: "boolean", description });
