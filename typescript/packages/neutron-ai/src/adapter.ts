import type {
  AssistantContentPart,
  FinishReason,
  Message,
  ReasoningPart,
  ToolChoice,
  ToolDefinition,
  Usage,
} from "./types.js";

/** Normalized call options handed to a provider adapter. */
export interface AdapterCallOptions {
  messages: Message[];
  tools?: ToolDefinition[];
  toolChoice?: ToolChoice;
  maxOutputTokens?: number;
  temperature?: number;
  topP?: number;
  stopSequences?: string[];
  headers?: Record<string, string>;
  abortSignal?: AbortSignal;
}

export interface AdapterGenerateResult {
  content: AssistantContentPart[];
  finishReason: FinishReason;
  usage: Usage;
  /** Raw provider response body, as an escape hatch. */
  raw: unknown;
}

/** Parts a provider adapter emits — the tool loop layers the richer public StreamPart on top. */
export type AdapterStreamPart =
  | { type: "text-delta"; text: string }
  | { type: "reasoning-delta"; text: string }
  | ReasoningPart
  | { type: "tool-input-start"; toolCallId: string; toolName: string }
  | { type: "tool-input-delta"; toolCallId: string; delta: string }
  | { type: "tool-call"; toolCallId: string; toolName: string; input: unknown }
  | { type: "finish"; finishReason: FinishReason; usage: Usage };

/** Text embedding boundary, implemented by providers that offer embeddings. */
export interface EmbeddingAdapter {
  readonly provider: string;
  readonly modelId: string;
  doEmbed(
    values: string[],
    options?: { headers?: Record<string, string>; abortSignal?: AbortSignal },
  ): Promise<{ embeddings: number[][]; usage: { inputTokens: number } }>;
}

/**
 * The internal provider boundary. Adapters implement exactly this; the
 * public API (generateText/streamText) is built over it once. An AI
 * Gateway plugs in by pointing an existing adapter at its baseURL — no
 * separate adapter needed.
 *
 * `doStream` is lazy: the request is sent on first iteration, and errors
 * (including HTTP errors) surface as throws from the iterator.
 */
export interface ModelAdapter {
  readonly provider: string;
  readonly modelId: string;
  doGenerate(options: AdapterCallOptions): Promise<AdapterGenerateResult>;
  doStream(options: AdapterCallOptions): AsyncIterable<AdapterStreamPart>;
}
