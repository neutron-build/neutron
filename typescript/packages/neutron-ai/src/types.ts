/** Token usage for a single model call. */
export interface Usage {
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
}

/**
 * Why generation stopped. Providers produce the first five;
 * "tool-approval" is produced by the tool loop when it suspends awaiting
 * a human decision (see ToolApprovalRequest).
 */
export type FinishReason =
  | "stop"
  | "length"
  | "tool-calls"
  | "content-filter"
  | "error"
  | "other"
  | "tool-approval";

export interface TextPart {
  type: "text";
  text: string;
}

/** Inline image, base64-encoded. */
export interface ImagePart {
  type: "image";
  mediaType: string;
  data: string;
}

/**
 * Extended-thinking output. `signature` is the provider's verification
 * token and must be preserved to send reasoning back on later turns
 * (Anthropic requires it in tool loops); `redactedData` carries an opaque
 * redacted block. Reasoning without either is display-only and is dropped
 * when round-tripping.
 */
export interface ReasoningPart {
  type: "reasoning";
  text: string;
  signature?: string;
  redactedData?: string;
}

/** A tool invocation requested by the model. */
export interface ToolCallPart {
  type: "tool-call";
  toolCallId: string;
  toolName: string;
  input: unknown;
}

/** The result of executing a tool call, sent back to the model. */
export interface ToolResultPart {
  type: "tool-result";
  toolCallId: string;
  toolName: string;
  output: unknown;
  isError?: boolean;
}

export type UserContentPart = TextPart | ImagePart;
export type AssistantContentPart = TextPart | ReasoningPart | ToolCallPart;

export interface SystemMessage {
  role: "system";
  content: string;
}

export interface UserMessage {
  role: "user";
  content: string | UserContentPart[];
}

export interface AssistantMessage {
  role: "assistant";
  content: string | AssistantContentPart[];
}

export interface ToolMessage {
  role: "tool";
  content: ToolResultPart[];
}

export type Message = SystemMessage | UserMessage | AssistantMessage | ToolMessage;

/**
 * The wire-level tool shape adapters send to providers. Application code
 * uses tool() / the Tool interface; the loop resolves those to this.
 */
export interface ToolDefinition {
  name: string;
  description?: string;
  inputSchema: Record<string, unknown>;
}

export type ToolChoice = "auto" | "required" | "none" | { toolName: string };

/** A tool call awaiting a human decision; serializable, resume via toolApprovals. */
export interface ToolApprovalRequest {
  toolCallId: string;
  toolName: string;
  input: unknown;
}

export interface ToolApprovalDecision {
  toolCallId: string;
  approved: boolean;
  /** Optional context surfaced to the model when the call is denied. */
  reason?: string;
}

/**
 * Parts emitted by streamText's fullStream. Tool calls and results reuse
 * the message content-part shapes; "step-finish" separates the steps of a
 * multi-step tool loop; the final "finish" carries the total usage.
 */
export type StreamPart =
  | { type: "text-delta"; text: string }
  | { type: "reasoning-delta"; text: string }
  | ReasoningPart
  | { type: "tool-input-start"; toolCallId: string; toolName: string }
  | { type: "tool-input-delta"; toolCallId: string; delta: string }
  | ToolCallPart
  | ToolResultPart
  | { type: "approval-request"; request: ToolApprovalRequest }
  | { type: "step-finish"; finishReason: FinishReason; usage: Usage }
  | { type: "finish"; finishReason: FinishReason; usage: Usage };
