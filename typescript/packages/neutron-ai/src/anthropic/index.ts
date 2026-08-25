import type {
  AdapterCallOptions,
  AdapterGenerateResult,
  AdapterStreamPart,
  ModelAdapter,
} from "../adapter.js";
import { AIError, problemFromStatus } from "../errors.js";
import { parseSSE } from "../internal/sse.js";
import type {
  AssistantContentPart,
  AssistantMessage,
  FinishReason,
  ReasoningPart,
  ToolChoice,
  ToolDefinition,
  ToolResultPart,
  Usage,
  UserMessage,
} from "../types.js";

const DEFAULT_BASE_URL = "https://api.anthropic.com";
const API_VERSION = "2023-06-01";
const DEFAULT_MAX_OUTPUT_TOKENS = 4096;
const PROVIDER = "anthropic";

export interface AnthropicSettings {
  /** Defaults to ANTHROPIC_API_KEY, read at call time. */
  apiKey?: string;
  /** Point at an AI Gateway or proxy; defaults to the Anthropic API. */
  baseURL?: string;
  headers?: Record<string, string>;
  /**
   * Relabel this adapter for error attribution when routing through a
   * gateway (parity with the OpenAI adapter's `provider`). Default
   * "anthropic".
   */
  provider?: string;
  /** Custom transport; also the test seam for HTTP-boundary mocking. */
  fetch?: typeof globalThis.fetch;
}

export interface AnthropicModelOptions {
  /** Enable extended thinking with this token budget (must be below maxOutputTokens). */
  thinking?: { budgetTokens: number };
  /**
   * Prompt caching (top-level automatic cache_control; GA, no beta
   * header). `true` = 5-minute TTL; `{ ttl: "1h" }` for the hour cache.
   * Reads bill at 0.1x input price — the shape of an agent loop (a
   * growing prefix resent every turn) is the ideal case. Prompts below
   * the model's cacheable minimum are silently processed uncached.
   */
  cache?: boolean | { ttl: "5m" | "1h" };
}

export function createAnthropic(
  settings: AnthropicSettings = {},
): (modelId: string, options?: AnthropicModelOptions) => ModelAdapter {
  return (modelId, options) => new AnthropicAdapter(modelId, settings, options ?? {});
}

/** Default instance, configured via ANTHROPIC_API_KEY. */
export const anthropic = createAnthropic();

interface AnthropicUsage {
  input_tokens?: number;
  output_tokens?: number;
  cache_read_input_tokens?: number;
  cache_creation_input_tokens?: number;
}

interface AnthropicContentBlock {
  type: string;
  text?: string;
  id?: string;
  name?: string;
  input?: unknown;
  thinking?: string;
  signature?: string;
  data?: string;
}

interface AnthropicResponse {
  content?: AnthropicContentBlock[];
  stop_reason?: string | null;
  usage?: AnthropicUsage;
}

interface AnthropicStreamEvent {
  type?: string;
  message?: { usage?: AnthropicUsage };
  index?: number;
  content_block?: AnthropicContentBlock;
  delta?: {
    type?: string;
    text?: string;
    partial_json?: string;
    thinking?: string;
    signature?: string;
    stop_reason?: string | null;
  };
  usage?: AnthropicUsage;
  error?: { type?: string; message?: string };
}

const SSE_ERROR_STATUS: Record<string, number> = {
  invalid_request_error: 400,
  authentication_error: 401,
  permission_error: 403,
  not_found_error: 404,
  rate_limit_error: 429,
  overloaded_error: 500,
  api_error: 500,
};

class AnthropicAdapter implements ModelAdapter {
  readonly provider: string;
  readonly modelId: string;
  readonly #settings: AnthropicSettings;
  readonly #modelOptions: AnthropicModelOptions;

  constructor(modelId: string, settings: AnthropicSettings, modelOptions: AnthropicModelOptions) {
    this.modelId = modelId;
    this.#settings = settings;
    this.#modelOptions = modelOptions;
    this.provider = settings.provider ?? PROVIDER;
  }

  async doGenerate(options: AdapterCallOptions): Promise<AdapterGenerateResult> {
    const response = await this.#post(this.#buildBody(options, false), options);
    const json = (await response.json()) as AnthropicResponse;
    const content: AssistantContentPart[] = [];
    for (const block of json.content ?? []) {
      const part = mapContentBlock(block);
      if (part !== null) content.push(part);
    }
    return {
      content,
      finishReason: mapStopReason(json.stop_reason),
      usage: mapUsage(json.usage),
      raw: json,
    };
  }

  async *doStream(options: AdapterCallOptions): AsyncGenerator<AdapterStreamPart, void, undefined> {
    const response = await this.#post(this.#buildBody(options, true), options);
    if (response.body === null) {
      throw new AIError(problemFromStatus(500, "Anthropic response had no body."), { provider: this.provider });
    }

    let inputTokens = 0;
    let outputTokens = 0;
    let cacheRead = 0;
    let cacheWrite = 0;
    let finishReason: FinishReason = "other";
    let sawMessageStop = false;
    const pendingTools = new Map<number, { id: string; name: string; inputJson: string }>();
    const pendingReasoning = new Map<number, { text: string; signature: string; redactedData?: string }>();

    for await (const event of parseSSE(response.body)) {
      if (event.data === "") continue;
      let payload: AnthropicStreamEvent;
      try {
        payload = JSON.parse(event.data) as AnthropicStreamEvent;
      } catch {
        throw new AIError(problemFromStatus(500, "Anthropic stream sent a malformed event."), { provider: this.provider });
      }

      switch (payload.type) {
        case "message_start":
          inputTokens = payload.message?.usage?.input_tokens ?? 0;
          outputTokens = payload.message?.usage?.output_tokens ?? 0;
          // Cache accounting arrives with message_start when caching is on.
          cacheRead = payload.message?.usage?.cache_read_input_tokens ?? 0;
          cacheWrite = payload.message?.usage?.cache_creation_input_tokens ?? 0;
          break;
        case "content_block_start":
          if (payload.index === undefined) break;
          if (payload.content_block?.type === "tool_use") {
            const pending = {
              id: payload.content_block.id ?? "",
              name: payload.content_block.name ?? "",
              inputJson: "",
            };
            pendingTools.set(payload.index, pending);
            yield { type: "tool-input-start", toolCallId: pending.id, toolName: pending.name };
          } else if (payload.content_block?.type === "thinking") {
            pendingReasoning.set(payload.index, { text: payload.content_block.thinking ?? "", signature: "" });
          } else if (payload.content_block?.type === "redacted_thinking") {
            pendingReasoning.set(payload.index, {
              text: "",
              signature: "",
              redactedData: payload.content_block.data ?? "",
            });
          }
          break;
        case "content_block_delta":
          if (payload.delta?.type === "text_delta" && payload.delta.text !== undefined) {
            yield { type: "text-delta", text: payload.delta.text };
          } else if (payload.delta?.type === "input_json_delta" && payload.index !== undefined) {
            const pending = pendingTools.get(payload.index);
            if (pending !== undefined) {
              const delta = payload.delta.partial_json ?? "";
              pending.inputJson += delta;
              if (delta !== "") yield { type: "tool-input-delta", toolCallId: pending.id, delta };
            }
          } else if (payload.delta?.type === "thinking_delta" && payload.index !== undefined) {
            const pending = pendingReasoning.get(payload.index);
            if (pending !== undefined) {
              const delta = payload.delta.thinking ?? "";
              pending.text += delta;
              if (delta !== "") yield { type: "reasoning-delta", text: delta };
            }
          } else if (payload.delta?.type === "signature_delta" && payload.index !== undefined) {
            const pending = pendingReasoning.get(payload.index);
            if (pending !== undefined) pending.signature += payload.delta.signature ?? "";
          }
          break;
        case "content_block_stop": {
          if (payload.index === undefined) break;
          const pending = pendingTools.get(payload.index);
          if (pending !== undefined) {
            pendingTools.delete(payload.index);
            yield {
              type: "tool-call",
              toolCallId: pending.id,
              toolName: pending.name,
              input: parseToolInput(pending.inputJson, this.provider),
            };
          }
          const reasoning = pendingReasoning.get(payload.index);
          if (reasoning !== undefined) {
            pendingReasoning.delete(payload.index);
            const part: ReasoningPart = { type: "reasoning", text: reasoning.text };
            if (reasoning.signature !== "") part.signature = reasoning.signature;
            if (reasoning.redactedData !== undefined) part.redactedData = reasoning.redactedData;
            yield part;
          }
          break;
        }
        case "message_delta":
          if (payload.delta?.stop_reason != null) {
            finishReason = mapStopReason(payload.delta.stop_reason);
          }
          if (payload.usage?.output_tokens !== undefined) {
            outputTokens = payload.usage.output_tokens;
          }
          break;
        case "message_stop": {
          sawMessageStop = true;
          const usage: Usage = {
            inputTokens,
            outputTokens,
            totalTokens: inputTokens + outputTokens + cacheRead + cacheWrite,
            ...(cacheRead > 0 ? { cacheReadTokens: cacheRead } : {}),
            ...(cacheWrite > 0 ? { cacheWriteTokens: cacheWrite } : {}),
          };
          yield { type: "finish", finishReason, usage };
          break;
        }
        case "error": {
          const status = SSE_ERROR_STATUS[payload.error?.type ?? ""] ?? 500;
          throw new AIError(
            problemFromStatus(status, payload.error?.message ?? "Anthropic stream reported an error."),
            { provider: this.provider },
          );
        }
        default:
          break; // ping and future event types
      }
    }

    // A stream whose connection dropped just ends the loop — no error event,
    // no message_stop. Ending silently would report a truncated response as
    // complete (possibly executing truncated tool calls).
    if (!sawMessageStop) {
      throw new AIError(
        problemFromStatus(500, "Anthropic stream ended without message_stop — the response was truncated."),
        { provider: this.provider },
      );
    }
  }

  async #post(body: Record<string, unknown>, options: AdapterCallOptions): Promise<Response> {
    const apiKey = this.#settings.apiKey ?? globalThis.process?.env?.ANTHROPIC_API_KEY;
    if (apiKey === undefined || apiKey === "") {
      throw new AIError(
        problemFromStatus(401, "Missing Anthropic API key: set ANTHROPIC_API_KEY or pass apiKey to createAnthropic()."),
        { provider: this.provider },
      );
    }

    const fetchImpl = this.#settings.fetch ?? globalThis.fetch;
    const url = `${(this.#settings.baseURL ?? DEFAULT_BASE_URL).replace(/\/+$/, "")}/v1/messages`;

    let response: Response;
    try {
      response = await fetchImpl(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-key": apiKey,
          "anthropic-version": API_VERSION,
          ...this.#settings.headers,
          ...options.headers,
        },
        body: JSON.stringify(body),
        ...(options.abortSignal !== undefined ? { signal: options.abortSignal } : {}),
      });
    } catch (cause) {
      if (options.abortSignal?.aborted) throw cause;
      const message = cause instanceof Error ? cause.message : String(cause);
      throw new AIError(problemFromStatus(500, `Anthropic request failed: ${message}`), {
        provider: this.provider,
        cause,
      });
    }

    if (!response.ok) {
      throw await responseToError(response, this.provider);
    }
    return response;
  }

  #buildBody(options: AdapterCallOptions, stream: boolean): Record<string, unknown> {
    const system: string[] = [];
    // Anthropic requires alternating roles and tool_result blocks directly
    // after their tool_use turn, so consecutive same-role messages (e.g.
    // tool messages merged across a suspension/resume) collapse into one.
    const messages: Array<{ role: string; content: unknown[] }> = [];
    const push = (role: string, content: unknown[]): void => {
      const previous = messages[messages.length - 1];
      if (previous !== undefined && previous.role === role) previous.content.push(...content);
      else messages.push({ role, content });
    };

    for (const message of options.messages) {
      switch (message.role) {
        case "system":
          system.push(message.content);
          break;
        case "user":
          push("user", mapUserContent(message));
          break;
        case "assistant":
          push("assistant", mapAssistantContent(message));
          break;
        case "tool":
          push("user", message.content.map(mapToolResult));
          break;
      }
    }

    const body: Record<string, unknown> = {
      model: this.modelId,
      max_tokens: options.maxOutputTokens ?? DEFAULT_MAX_OUTPUT_TOKENS,
      messages,
    };
    if (system.length > 0) body.system = system.join("\n\n");
    if (this.#modelOptions.cache !== undefined && this.#modelOptions.cache !== false) {
      const ttl = typeof this.#modelOptions.cache === "object" ? this.#modelOptions.cache.ttl : "5m";
      body.cache_control = ttl === "1h" ? { type: "ephemeral", ttl: "1h" } : { type: "ephemeral" };
    }
    if (this.#modelOptions.thinking !== undefined) {
      body.thinking = { type: "enabled", budget_tokens: this.#modelOptions.thinking.budgetTokens };
    }
    if (options.tools !== undefined && options.tools.length > 0) body.tools = options.tools.map(mapTool);
    if (options.toolChoice !== undefined) body.tool_choice = mapToolChoice(options.toolChoice);
    if (options.temperature !== undefined) body.temperature = options.temperature;
    if (options.topP !== undefined) body.top_p = options.topP;
    if (options.stopSequences !== undefined && options.stopSequences.length > 0) {
      body.stop_sequences = options.stopSequences;
    }
    if (stream) body.stream = true;
    return body;
  }
}

async function responseToError(response: Response, provider: string): Promise<AIError> {
  let detail = `Anthropic request failed with status ${response.status}.`;
  try {
    const json = (await response.json()) as { error?: { message?: string } };
    if (json.error?.message !== undefined) detail = json.error.message;
  } catch {
    // non-JSON error body; keep the status-based detail
  }
  return new AIError(problemFromStatus(response.status, detail), { provider });
}

function mapToolChoice(toolChoice: ToolChoice): unknown {
  if (toolChoice === "auto") return { type: "auto" };
  if (toolChoice === "required") return { type: "any" };
  if (toolChoice === "none") return { type: "none" };
  return { type: "tool", name: toolChoice.toolName };
}

function mapUserContent(message: UserMessage): unknown[] {
  if (typeof message.content === "string") {
    return [{ type: "text", text: message.content }];
  }
  return message.content.map((part) => {
    if (part.type === "text") return { type: "text", text: part.text };
    return {
      type: "image",
      source: { type: "base64", media_type: part.mediaType, data: part.data },
    };
  });
}

function mapAssistantContent(message: AssistantMessage): unknown[] {
  if (typeof message.content === "string") {
    return [{ type: "text", text: message.content }];
  }
  const blocks: unknown[] = [];
  for (const part of message.content) {
    if (part.type === "text") {
      blocks.push({ type: "text", text: part.text });
    } else if (part.type === "reasoning") {
      // Only verifiable reasoning can be sent back; unsigned reasoning
      // (e.g. from another provider) is display-only and gets dropped.
      if (part.redactedData !== undefined) {
        blocks.push({ type: "redacted_thinking", data: part.redactedData });
      } else if (part.signature !== undefined) {
        blocks.push({ type: "thinking", thinking: part.text, signature: part.signature });
      }
    } else {
      blocks.push({ type: "tool_use", id: part.toolCallId, name: part.toolName, input: part.input });
    }
  }
  return blocks;
}

function mapToolResult(part: ToolResultPart): unknown {
  const result: Record<string, unknown> = {
    type: "tool_result",
    tool_use_id: part.toolCallId,
    content: typeof part.output === "string" ? part.output : JSON.stringify(part.output),
  };
  if (part.isError === true) result.is_error = true;
  return result;
}

function mapTool(tool: ToolDefinition): unknown {
  return {
    name: tool.name,
    description: tool.description ?? "",
    input_schema: tool.inputSchema,
  };
}

function mapContentBlock(block: AnthropicContentBlock): AssistantContentPart | null {
  if (block.type === "text" && block.text !== undefined) {
    return { type: "text", text: block.text };
  }
  if (block.type === "thinking") {
    const part: ReasoningPart = { type: "reasoning", text: block.thinking ?? "" };
    if (block.signature !== undefined) part.signature = block.signature;
    return part;
  }
  if (block.type === "redacted_thinking") {
    return { type: "reasoning", text: "", redactedData: block.data ?? "" };
  }
  if (block.type === "tool_use") {
    return {
      type: "tool-call",
      toolCallId: block.id ?? "",
      toolName: block.name ?? "",
      input: block.input ?? {},
    };
  }
  return null;
}

function mapStopReason(stopReason: string | null | undefined): FinishReason {
  switch (stopReason) {
    case "end_turn":
    case "stop_sequence":
      return "stop";
    case "max_tokens":
      return "length";
    case "tool_use":
      return "tool-calls";
    case "refusal":
      return "content-filter";
    default:
      return "other";
  }
}

function mapUsage(usage: AnthropicUsage | undefined): Usage {
  const inputTokens = usage?.input_tokens ?? 0;
  const outputTokens = usage?.output_tokens ?? 0;
  const cacheRead = usage?.cache_read_input_tokens;
  const cacheWrite = usage?.cache_creation_input_tokens;
  // Anthropic reports cached tokens SEPARATELY from input_tokens, so the
  // real total is the sum of all four.
  return {
    inputTokens,
    outputTokens,
    totalTokens: inputTokens + outputTokens + (cacheRead ?? 0) + (cacheWrite ?? 0),
    ...(cacheRead !== undefined && cacheRead > 0 ? { cacheReadTokens: cacheRead } : {}),
    ...(cacheWrite !== undefined && cacheWrite > 0 ? { cacheWriteTokens: cacheWrite } : {}),
  };
}

function parseToolInput(inputJson: string, provider: string): unknown {
  if (inputJson === "") return {};
  try {
    return JSON.parse(inputJson);
  } catch {
    throw new AIError(
      problemFromStatus(500, "Anthropic stream sent unparseable tool input JSON."),
      { provider },
    );
  }
}
