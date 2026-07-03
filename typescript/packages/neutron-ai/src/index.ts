export type {
  AssistantContentPart,
  AssistantMessage,
  FinishReason,
  ImagePart,
  Message,
  ReasoningPart,
  StreamPart,
  SystemMessage,
  TextPart,
  ToolApprovalDecision,
  ToolApprovalRequest,
  ToolCallPart,
  ToolChoice,
  ToolDefinition,
  ToolMessage,
  ToolResultPart,
  Usage,
  UserContentPart,
  UserMessage,
} from "./types.js";

export { AIError, problemFromStatus } from "./errors.js";
export type { ProblemDetails } from "./errors.js";

export type {
  AdapterCallOptions,
  AdapterGenerateResult,
  AdapterStreamPart,
  EmbeddingAdapter,
  ModelAdapter,
} from "./adapter.js";

export { isSchema, jsonSchema, resolveSchema } from "./schema.js";
export type {
  FlexibleSchema,
  InferSchema,
  JSONSchemaObject,
  Schema,
  SchemaValidationResult,
} from "./schema.js";
export type { StandardSchemaV1 } from "./standard-schema.js";

export { tool } from "./tool.js";
export type { Tool, ToolExecutionContext } from "./tool.js";

export type { CallOptions, CallSettings } from "./call-options.js";

export { generateText } from "./generate-text.js";
export type { GenerateTextOptions, GenerateTextResult, StepResult } from "./generate-text.js";

export { generateObject } from "./generate-object.js";
export type { GenerateObjectOptions, GenerateObjectResult } from "./generate-object.js";

export { streamText } from "./stream-text.js";
export type { StreamTextOptions, StreamTextResult } from "./stream-text.js";

export { streamObject } from "./stream-object.js";
export type { DeepPartial, StreamObjectOptions, StreamObjectResult } from "./stream-object.js";

export { embed, embedAndStore, embedMany } from "./embed.js";
export type { EmbedAndStoreOptions, EmbedManyOptions, EmbedOptions, VectorSink } from "./embed.js";

export { streamPartsFromResponse, toEventStreamResponse } from "./event-stream.js";

export { ChatStore } from "./chat-store.js";
export type { ChatMessage, ChatState, ChatStatus, ChatStoreOptions } from "./chat-store.js";
