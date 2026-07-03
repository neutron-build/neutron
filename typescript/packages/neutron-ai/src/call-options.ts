import type { AdapterCallOptions, ModelAdapter } from "./adapter.js";
import { AIError, problemFromStatus } from "./errors.js";
import type { Tool } from "./tool.js";
import type { Message, ToolChoice, ToolDefinition } from "./types.js";

/** Settings shared by generateText and streamText. */
export interface CallSettings {
  maxOutputTokens?: number;
  temperature?: number;
  topP?: number;
  stopSequences?: string[];
  headers?: Record<string, string>;
  abortSignal?: AbortSignal;
  /** Retries after the first attempt for 429/5xx failures (default 2). Streams retry only before producing output. */
  maxRetries?: number;
  /** First backoff delay in ms (default 1000); doubles per attempt, jittered. */
  retryDelayMs?: number;
}

export interface CallOptions extends CallSettings {
  model: ModelAdapter;
  system?: string;
  /** Shorthand for a single user message. Exactly one of prompt/messages. */
  prompt?: string;
  messages?: Message[];
  tools?: Tool[];
  toolChoice?: ToolChoice;
}

export function resolveInitialMessages(options: CallOptions): Message[] {
  if (options.prompt === undefined && options.messages === undefined) {
    throw new AIError(problemFromStatus(400, "Provide one of `prompt` or `messages`."));
  }
  if (options.prompt !== undefined && options.messages !== undefined) {
    throw new AIError(problemFromStatus(400, "Provide either `prompt` or `messages`, not both."));
  }

  const messages: Message[] =
    options.messages !== undefined
      ? [...options.messages]
      : [{ role: "user", content: options.prompt! }];
  if (options.system !== undefined) {
    messages.unshift({ role: "system", content: options.system });
  }
  return messages;
}

export function buildAdapterOptions(
  options: CallOptions,
  messages: Message[],
  definitions?: ToolDefinition[],
): AdapterCallOptions {
  const adapterOptions: AdapterCallOptions = { messages };
  if (definitions !== undefined) adapterOptions.tools = definitions;
  if (options.toolChoice !== undefined) adapterOptions.toolChoice = options.toolChoice;
  if (options.maxOutputTokens !== undefined) adapterOptions.maxOutputTokens = options.maxOutputTokens;
  if (options.temperature !== undefined) adapterOptions.temperature = options.temperature;
  if (options.topP !== undefined) adapterOptions.topP = options.topP;
  if (options.stopSequences !== undefined) adapterOptions.stopSequences = options.stopSequences;
  if (options.headers !== undefined) adapterOptions.headers = options.headers;
  if (options.abortSignal !== undefined) adapterOptions.abortSignal = options.abortSignal;
  return adapterOptions;
}
