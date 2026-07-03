import type {
  AdapterCallOptions,
  AdapterGenerateResult,
  AdapterStreamPart,
  EmbeddingAdapter,
  ModelAdapter,
} from "../adapter.js";
import { AIError, problemFromStatus } from "../errors.js";
import { parseSSE } from "../internal/sse.js";
import type {
  AssistantContentPart,
  AssistantMessage,
  FinishReason,
  ToolChoice,
  ToolDefinition,
  ToolResultPart,
  UserMessage,
} from "../types.js";

const DEFAULT_BASE_URL = "https://api.openai.com";
const PROVIDER = "openai";

/**
 * OpenAI Chat Completions adapter. Because the wire format is the de facto
 * industry standard, this adapter also serves Groq, DeepSeek, Google's
 * OpenAI-compatible Gemini endpoint, vLLM/Ollama, and the AI Gateway — set
 * `baseURL` (and `provider` for error attribution) and it just works.
 */
export interface OpenAISettings {
  /** Defaults to OPENAI_API_KEY, read at call time. */
  apiKey?: string;
  /** Point at any OpenAI-compatible server; defaults to the OpenAI API. */
  baseURL?: string;
  /** Provider label used in errors, e.g. "groq". Defaults to "openai". */
  provider?: string;
  headers?: Record<string, string>;
  /** Custom transport; also the test seam for HTTP-boundary mocking. */
  fetch?: typeof globalThis.fetch;
}

export interface OpenAIProvider {
  /** Chat model handle (POST /v1/chat/completions). */
  (modelId: string): ModelAdapter;
  /** Embedding model handle (POST /v1/embeddings). */
  embedding(modelId: string): EmbeddingAdapter;
}

export function createOpenAI(settings: OpenAISettings = {}): OpenAIProvider {
  const chat = (modelId: string): ModelAdapter => new OpenAIAdapter(modelId, settings);
  return Object.assign(chat, {
    embedding: (modelId: string): EmbeddingAdapter => new OpenAIEmbeddingAdapter(modelId, settings),
  });
}

/** Default instance, configured via OPENAI_API_KEY. */
export const openai = createOpenAI();

interface OpenAIToolCall {
  id?: string;
  function?: { name?: string; arguments?: string };
}

interface OpenAIResponse {
  choices?: Array<{
    message?: { content?: string | null; tool_calls?: OpenAIToolCall[] };
    finish_reason?: string | null;
  }>;
  usage?: { prompt_tokens?: number; completion_tokens?: number };
}

interface OpenAIStreamChunk {
  choices?: Array<{
    delta?: {
      content?: string | null;
      tool_calls?: Array<{ index: number; id?: string; function?: { name?: string; arguments?: string } }>;
    };
    finish_reason?: string | null;
  }>;
  usage?: { prompt_tokens?: number; completion_tokens?: number } | null;
}

class OpenAIAdapter implements ModelAdapter {
  readonly provider: string;
  readonly modelId: string;
  readonly #settings: OpenAISettings;

  constructor(modelId: string, settings: OpenAISettings) {
    this.modelId = modelId;
    this.#settings = settings;
    this.provider = settings.provider ?? PROVIDER;
  }

  async doGenerate(options: AdapterCallOptions): Promise<AdapterGenerateResult> {
    const response = await this.#post(this.#buildBody(options, false), options);
    const json = (await response.json()) as OpenAIResponse;
    const choice = json.choices?.[0];

    const content: AssistantContentPart[] = [];
    if (typeof choice?.message?.content === "string" && choice.message.content !== "") {
      content.push({ type: "text", text: choice.message.content });
    }
    for (const call of choice?.message?.tool_calls ?? []) {
      content.push({
        type: "tool-call",
        toolCallId: call.id ?? "",
        toolName: call.function?.name ?? "",
        input: this.#parseArguments(call.function?.arguments),
      });
    }

    return {
      content,
      finishReason: mapFinishReason(choice?.finish_reason),
      usage: mapUsage(json.usage),
      raw: json,
    };
  }

  async *doStream(options: AdapterCallOptions): AsyncGenerator<AdapterStreamPart, void, undefined> {
    const response = await this.#post(this.#buildBody(options, true), options);
    if (response.body === null) {
      throw new AIError(problemFromStatus(500, "Response had no body."), { provider: this.provider });
    }

    let finishReason: FinishReason = "other";
    let usage = { inputTokens: 0, outputTokens: 0, totalTokens: 0 };
    const pendingTools = new Map<number, { id: string; name: string; argumentsJson: string }>();

    for await (const event of parseSSE(response.body)) {
      if (event.data === "" || event.data === "[DONE]") continue;
      let chunk: OpenAIStreamChunk;
      try {
        chunk = JSON.parse(event.data) as OpenAIStreamChunk;
      } catch {
        throw new AIError(problemFromStatus(500, "Stream sent a malformed event."), { provider: this.provider });
      }

      if (chunk.usage != null) {
        usage = mapUsage(chunk.usage);
      }
      const choice = chunk.choices?.[0];
      if (choice === undefined) continue;

      if (typeof choice.delta?.content === "string" && choice.delta.content !== "") {
        yield { type: "text-delta", text: choice.delta.content };
      }
      for (const delta of choice.delta?.tool_calls ?? []) {
        let pending = pendingTools.get(delta.index);
        if (pending === undefined) {
          pending = {
            id: delta.id ?? "",
            name: delta.function?.name ?? "",
            argumentsJson: "",
          };
          pendingTools.set(delta.index, pending);
          yield { type: "tool-input-start", toolCallId: pending.id, toolName: pending.name };
        } else {
          if (delta.id !== undefined) pending.id = delta.id;
          if (delta.function?.name !== undefined) pending.name += delta.function.name;
        }
        if (delta.function?.arguments !== undefined && delta.function.arguments !== "") {
          pending.argumentsJson += delta.function.arguments;
          yield { type: "tool-input-delta", toolCallId: pending.id, delta: delta.function.arguments };
        }
      }
      if (choice.finish_reason != null) {
        finishReason = mapFinishReason(choice.finish_reason);
      }
    }

    for (const [, pending] of [...pendingTools.entries()].sort(([a], [b]) => a - b)) {
      yield {
        type: "tool-call",
        toolCallId: pending.id,
        toolName: pending.name,
        input: this.#parseArguments(pending.argumentsJson),
      };
    }
    yield { type: "finish", finishReason, usage };
  }

  #parseArguments(argumentsJson: string | undefined): unknown {
    if (argumentsJson === undefined || argumentsJson === "") return {};
    try {
      return JSON.parse(argumentsJson);
    } catch {
      throw new AIError(problemFromStatus(500, "Model sent unparseable tool arguments JSON."), {
        provider: this.provider,
      });
    }
  }

  async #post(body: Record<string, unknown>, options: AdapterCallOptions): Promise<Response> {
    const requestOptions: { headers?: Record<string, string>; abortSignal?: AbortSignal } = {};
    if (options.headers !== undefined) requestOptions.headers = options.headers;
    if (options.abortSignal !== undefined) requestOptions.abortSignal = options.abortSignal;
    return postJson(this.#settings, this.provider, "/v1/chat/completions", body, requestOptions);
  }

  #buildBody(options: AdapterCallOptions, stream: boolean): Record<string, unknown> {
    const messages: unknown[] = [];
    for (const message of options.messages) {
      switch (message.role) {
        case "system":
          messages.push({ role: "system", content: message.content });
          break;
        case "user":
          messages.push({ role: "user", content: mapUserContent(message) });
          break;
        case "assistant":
          messages.push(mapAssistantMessage(message));
          break;
        case "tool":
          for (const result of message.content) messages.push(mapToolResult(result));
          break;
      }
    }

    const body: Record<string, unknown> = { model: this.modelId, messages };
    if (options.tools !== undefined && options.tools.length > 0) body.tools = options.tools.map(mapTool);
    if (options.toolChoice !== undefined) body.tool_choice = mapToolChoice(options.toolChoice);
    if (options.maxOutputTokens !== undefined) body.max_tokens = options.maxOutputTokens;
    if (options.temperature !== undefined) body.temperature = options.temperature;
    if (options.topP !== undefined) body.top_p = options.topP;
    if (options.stopSequences !== undefined && options.stopSequences.length > 0) {
      body.stop = options.stopSequences;
    }
    if (stream) {
      body.stream = true;
      body.stream_options = { include_usage: true };
    }
    return body;
  }
}

class OpenAIEmbeddingAdapter implements EmbeddingAdapter {
  readonly provider: string;
  readonly modelId: string;
  readonly #settings: OpenAISettings;

  constructor(modelId: string, settings: OpenAISettings) {
    this.modelId = modelId;
    this.#settings = settings;
    this.provider = settings.provider ?? PROVIDER;
  }

  async doEmbed(
    values: string[],
    options: { headers?: Record<string, string>; abortSignal?: AbortSignal } = {},
  ): Promise<{ embeddings: number[][]; usage: { inputTokens: number } }> {
    const response = await postJson(
      this.#settings,
      this.provider,
      "/v1/embeddings",
      { model: this.modelId, input: values },
      options,
    );
    const json = (await response.json()) as {
      data?: Array<{ embedding?: number[]; index?: number }>;
      usage?: { prompt_tokens?: number };
    };
    const data = [...(json.data ?? [])].sort((a, b) => (a.index ?? 0) - (b.index ?? 0));
    const embeddings = data.map((item) => item.embedding ?? []);
    if (embeddings.length !== values.length) {
      throw new AIError(
        problemFromStatus(500, `Expected ${values.length} embeddings, got ${embeddings.length}.`),
        { provider: this.provider },
      );
    }
    return { embeddings, usage: { inputTokens: json.usage?.prompt_tokens ?? 0 } };
  }
}

async function postJson(
  settings: OpenAISettings,
  provider: string,
  path: string,
  body: Record<string, unknown>,
  options: { headers?: Record<string, string>; abortSignal?: AbortSignal },
): Promise<Response> {
  const apiKey = settings.apiKey ?? globalThis.process?.env?.OPENAI_API_KEY;
  if (apiKey === undefined || apiKey === "") {
    throw new AIError(
      problemFromStatus(401, "Missing API key: set OPENAI_API_KEY or pass apiKey to createOpenAI()."),
      { provider },
    );
  }

  const fetchImpl = settings.fetch ?? globalThis.fetch;
  const url = `${(settings.baseURL ?? DEFAULT_BASE_URL).replace(/\/+$/, "")}${path}`;

  let response: Response;
  try {
    response = await fetchImpl(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${apiKey}`,
        ...settings.headers,
        ...options.headers,
      },
      body: JSON.stringify(body),
      ...(options.abortSignal !== undefined ? { signal: options.abortSignal } : {}),
    });
  } catch (cause) {
    if (options.abortSignal?.aborted) throw cause;
    const message = cause instanceof Error ? cause.message : String(cause);
    throw new AIError(problemFromStatus(500, `Request failed: ${message}`), { provider, cause });
  }

  if (!response.ok) {
    let detail = `Request failed with status ${response.status}.`;
    try {
      const json = (await response.json()) as { error?: { message?: string } };
      if (json.error?.message !== undefined) detail = json.error.message;
    } catch {
      // non-JSON error body; keep the status-based detail
    }
    throw new AIError(problemFromStatus(response.status, detail), { provider });
  }
  return response;
}

function mapUserContent(message: UserMessage): unknown {
  if (typeof message.content === "string") return message.content;
  return message.content.map((part) => {
    if (part.type === "text") return { type: "text", text: part.text };
    return { type: "image_url", image_url: { url: `data:${part.mediaType};base64,${part.data}` } };
  });
}

function mapAssistantMessage(message: AssistantMessage): unknown {
  if (typeof message.content === "string") {
    return { role: "assistant", content: message.content };
  }
  const text = message.content
    .filter((part) => part.type === "text")
    .map((part) => (part as { text: string }).text)
    .join("");
  const toolCalls = message.content
    .filter((part) => part.type === "tool-call")
    .map((part) => {
      const call = part as { toolCallId: string; toolName: string; input: unknown };
      return {
        id: call.toolCallId,
        type: "function",
        function: { name: call.toolName, arguments: JSON.stringify(call.input) },
      };
    });
  const result: Record<string, unknown> = { role: "assistant", content: text === "" ? null : text };
  if (toolCalls.length > 0) result.tool_calls = toolCalls;
  return result;
}

function mapToolResult(part: ToolResultPart): unknown {
  return {
    role: "tool",
    tool_call_id: part.toolCallId,
    content: typeof part.output === "string" ? part.output : JSON.stringify(part.output),
  };
}

function mapTool(tool: ToolDefinition): unknown {
  return {
    type: "function",
    function: {
      name: tool.name,
      description: tool.description ?? "",
      parameters: tool.inputSchema,
    },
  };
}

function mapToolChoice(toolChoice: ToolChoice): unknown {
  if (toolChoice === "auto" || toolChoice === "none" || toolChoice === "required") return toolChoice;
  return { type: "function", function: { name: toolChoice.toolName } };
}

function mapFinishReason(finishReason: string | null | undefined): FinishReason {
  switch (finishReason) {
    case "stop":
      return "stop";
    case "length":
      return "length";
    case "tool_calls":
    case "function_call":
      return "tool-calls";
    case "content_filter":
      return "content-filter";
    default:
      return "other";
  }
}

function mapUsage(usage: { prompt_tokens?: number; completion_tokens?: number } | undefined): {
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
} {
  const inputTokens = usage?.prompt_tokens ?? 0;
  const outputTokens = usage?.completion_tokens ?? 0;
  return { inputTokens, outputTokens, totalTokens: inputTokens + outputTokens };
}
