/**
 * An MCP client over streamable HTTP.
 *
 * Uses fetch, so it runs anywhere the platform provides it — Node 20+, workers,
 * the browser — without a transport abstraction nobody asked for.
 */

import {
  PROTOCOL_LATEST,
  type InitializeResult,
  type RpcError,
  type ToolInfo,
  type ToolResult,
} from "./protocol.js";

/** Bounds a server's reply. A tool returning logs can be large; gigabytes means broken or hostile. */
const MAX_RESPONSE_BODY = 8 << 20; // 8 MiB

/**
 * A tool-level failure: the protocol call succeeded and the tool reported that
 * what it was asked to do did not happen.
 *
 * A distinct class because the distinction is load-bearing. A transport failure
 * means "we do not know whether anything happened" and may be worth retrying; a
 * McpToolError means "it definitely did not happen, and here is why", which
 * usually is not.
 */
export class McpToolError extends Error {
  readonly tool: string;
  constructor(tool: string, text: string) {
    super(text ? `${tool}: ${text}` : `${tool} failed`);
    this.name = "McpToolError";
    this.tool = tool;
  }
}

/** True when the failure came from a tool rather than the transport. */
export function isToolError(err: unknown): err is McpToolError {
  return err instanceof McpToolError;
}

export interface McpClientOptions {
  endpoint: string;
  token?: string;
  /** Defaults to the platform fetch; injectable so a test can serve a handler directly. */
  fetch?: typeof globalThis.fetch;
  protocolVersion?: string;
  /** Per-call ceiling in milliseconds. Short by default: a caller doing something slow should say so. */
  timeoutMs?: number;
  headers?: Record<string, string>;
}

export interface McpClient {
  initialize: () => Promise<InitializeResult>;
  ping: () => Promise<void>;
  listTools: () => Promise<ToolInfo[]>;
  callTool: (name: string, args?: Record<string, unknown>) => Promise<string>;
  /**
   * The server's tools as plain descriptors, ready to become native tools.
   *
   * This package stays free of any dependency on @neutron-build/ai, so it emits
   * raw JSON Schema rather than a branded Schema. Bridging is one line at the
   * call site:
   *
   *   const remote = await client.toolDescriptors();
   *   const tools = remote.map((t) => tool({
   *     name: t.name,
   *     description: t.description,
   *     inputSchema: jsonSchema(t.inputSchema),
   *     needsApproval: t.destructive,
   *     execute: (args) => client.callTool(t.name, args as Record<string, unknown>),
   *   }));
   *
   * `destructive` becoming `needsApproval` is the point: a remote tool that
   * changes something arrives already carrying the fact that a human should see
   * it first, instead of that judgment being re-made by hand per integration.
   */
  toolDescriptors: () => Promise<McpToolDescriptor[]>;
}

export interface McpToolDescriptor {
  name: string;
  description?: string;
  inputSchema: Record<string, unknown>;
  readOnly: boolean;
  destructive: boolean;
}

/**
 * Validate an MCP endpoint URL, returning the cleaned value.
 *
 * Deliberately does not reject private or loopback addresses. A self-hosted MCP
 * server normally *is* on a tailnet or localhost, and the URL is entered by an
 * operator pointing at their own infrastructure rather than supplied by an
 * untrusted caller — so the SSRF reasoning that would justify a blocklist does
 * not apply, and applying one anyway would reject every correct value. A host
 * that needs that guard should apply it before calling this.
 */
export function validateEndpoint(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) throw new Error("endpoint is required");
  let url: URL;
  try {
    url = new URL(trimmed);
  } catch {
    throw new Error("endpoint must be an absolute URL, e.g. http://100.64.0.1:3456/api/mcp");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("endpoint scheme must be http or https");
  }
  return trimmed.replace(/\/+$/, "");
}

/**
 * Read a stream to text, stopping at `limit` bytes. The bound must bound the
 * read itself: `await response.text()` buffers the whole body first, so a check
 * placed after it does nothing about memory. Content-length is checked before
 * reading when present; chunked bodies are capped mid-stream. On `signal` abort
 * the reader is canceled, which stops the read loop even when the body came
 * from an injected fetch that ignores the signal.
 */
async function readTextWithLimit(
  body: ReadableStream<Uint8Array> | null,
  limit: number,
  signal?: AbortSignal,
): Promise<{ text: string; oversize: boolean; aborted: boolean }> {
  if (!body) return { text: "", oversize: false, aborted: false };
  const reader = body.getReader();
  let aborted = false;
  const onAbort = () => {
    aborted = true;
    void reader.cancel().catch(() => {});
  };
  signal?.addEventListener("abort", onAbort, { once: true });
  const decoder = new TextDecoder();
  let text = "";
  let seen = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      seen += value.byteLength;
      if (seen > limit) {
        await reader.cancel().catch(() => {});
        return { text: "", oversize: true, aborted: false };
      }
      text += decoder.decode(value, { stream: true });
    }
    return { text: text + decoder.decode(), oversize: false, aborted };
  } finally {
    signal?.removeEventListener("abort", onAbort);
  }
}

export function createMcpClient(options: McpClientOptions): McpClient {
  const endpoint = options.endpoint.replace(/\/+$/, "");
  const doFetch = options.fetch ?? globalThis.fetch;
  const protocol = options.protocolVersion ?? PROTOCOL_LATEST;
  const timeoutMs = options.timeoutMs ?? 30_000;
  let nextId = 0;

  async function call(method: string, params: unknown): Promise<unknown> {
    const controller = new AbortController();
    // One deadline for the whole call — headers AND body. Clearing the timer
    // when the headers arrive would let a slow-dripping body hang the call
    // past timeoutMs, so it stays armed until everything settles.
    const deadlineAt = Date.now() + timeoutMs;
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      let response: Response;
      try {
        response = await doFetch(endpoint, {
          method: "POST",
          signal: controller.signal,
          headers: {
            "content-type": "application/json",
            accept: "application/json",
            // Both the header and the initialize parameter are offered because
            // servers disagree about which they read, and sending both costs
            // nothing.
            "mcp-protocol-version": protocol,
            ...(options.token ? { authorization: `Bearer ${options.token}` } : {}),
            ...(options.headers ?? {}),
          },
          body: JSON.stringify({ jsonrpc: "2.0", id: ++nextId, method, params }),
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        throw new Error(`calling ${method}: ${message}`);
      }

      if (response.status === 401) {
        throw new Error(`calling ${method}: the server rejected this client's credential`);
      }
      if (response.status === 202) {
        // The server treated this as a notification. Nothing is coming back.
        return undefined;
      }
      if (!response.ok) {
        throw new Error(`calling ${method}: server returned ${response.status} ${response.statusText}`);
      }

      const declared = Number(response.headers.get("content-length") ?? Number.NaN);
      if (declared > MAX_RESPONSE_BODY) {
        throw new Error(`calling ${method}: response too large`);
      }
      const read = readTextWithLimit(response.body, MAX_RESPONSE_BODY, controller.signal);
      let body: Awaited<typeof read>;
      let bodyTimer: ReturnType<typeof setTimeout> | undefined;
      try {
        body = await Promise.race([
          read,
          new Promise<never>((_, reject) => {
            bodyTimer = setTimeout(
              () => reject(new Error(`calling ${method}: response body read timed out after ${timeoutMs}ms`)),
              Math.max(deadlineAt - Date.now(), 0),
            );
          }),
        ]);
      } catch (err) {
        // The race can reject before the call-wide abort timer fires; abort
        // here so the reader inside readTextWithLimit is canceled and the
        // abandoned read stops instead of draining in the background.
        controller.abort();
        throw err;
      } finally {
        if (bodyTimer !== undefined) clearTimeout(bodyTimer);
      }
      if (body.oversize) {
        throw new Error(`calling ${method}: response too large`);
      }
      if (body.aborted) {
        // The abort timer fired while the read loop was mid-flight: the cancel
        // resolves pending reads as "done", so translate that back into the
        // timeout it actually was.
        throw new Error(`calling ${method}: response body read timed out after ${timeoutMs}ms`);
      }
      const text = body.text;
      let parsed: { result?: unknown; error?: RpcError };
      try {
        parsed = JSON.parse(text);
      } catch {
        throw new Error(`calling ${method}: unreadable response`);
      }
      if (parsed.error) {
        throw new Error(`calling ${method}: ${parsed.error.message} (code ${parsed.error.code})`);
      }
      return parsed.result;
    } finally {
      clearTimeout(timer);
    }
  }

  const client: McpClient = {
    async initialize() {
      return (await call("initialize", {
        protocolVersion: protocol,
        capabilities: {},
        clientInfo: { name: "@neutron-build/mcp", version: PROTOCOL_LATEST },
      })) as InitializeResult;
    },

    async ping() {
      await call("ping", {});
    },

    async listTools() {
      const result = (await call("tools/list", {})) as { tools?: ToolInfo[] } | undefined;
      return result?.tools ?? [];
    },

    /**
     * A tool that reports failure throws McpToolError rather than returning
     * text. This is the one place the protocol invites a serious mistake: a
     * failed tool is a *successful* JSON-RPC response whose result carries
     * isError, so a client checking only the JSON-RPC error field reads a failed
     * deploy as a success.
     */
    async callTool(name, args = {}) {
      const result = (await call("tools/call", { name, arguments: args })) as ToolResult | undefined;
      if (!result) throw new Error(`${name}: the server returned no result`);
      const text = (result.content ?? [])
        .map((part) => part.text ?? "")
        .filter(Boolean)
        .join("\n");
      if (result.isError) throw new McpToolError(name, text);
      return text;
    },

    async toolDescriptors() {
      const tools = await client.listTools();
      return tools.map((tool) => ({
        name: tool.name,
        description: tool.description,
        inputSchema: tool.inputSchema ?? { type: "object", properties: {} },
        readOnly: tool.annotations?.readOnlyHint === true,
        destructive: tool.annotations?.destructiveHint === true,
      }));
    },
  };

  return client;
}
