import { AIError, problemFromStatus } from "../errors.js";
import { resolveSchema, type Schema } from "../schema.js";
import type { Tool, ToolExecutionContext } from "../tool.js";
import type {
  AssistantMessage,
  Message,
  ToolApprovalDecision,
  ToolApprovalRequest,
  ToolCallPart,
  ToolDefinition,
  ToolResultPart,
  Usage,
} from "../types.js";

export const ZERO_USAGE: Usage = { inputTokens: 0, outputTokens: 0, totalTokens: 0 };

export function addUsage(a: Usage, b: Usage): Usage {
  const cacheRead = (a.cacheReadTokens ?? 0) + (b.cacheReadTokens ?? 0);
  const cacheWrite = (a.cacheWriteTokens ?? 0) + (b.cacheWriteTokens ?? 0);
  return {
    inputTokens: a.inputTokens + b.inputTokens,
    outputTokens: a.outputTokens + b.outputTokens,
    totalTokens: a.totalTokens + b.totalTokens,
    ...(cacheRead > 0 ? { cacheReadTokens: cacheRead } : {}),
    ...(cacheWrite > 0 ? { cacheWriteTokens: cacheWrite } : {}),
  };
}

export function makeApprovalContext(
  toolApprovals: ToolApprovalDecision[] | undefined,
  onApprovalRequest: ((request: ToolApprovalRequest) => boolean | Promise<boolean>) | undefined,
): ApprovalContext {
  const context: ApprovalContext = {
    decisions: new Map((toolApprovals ?? []).map((decision) => [decision.toolCallId, decision])),
    consumedDecisions: new Set(),
  };
  if (onApprovalRequest !== undefined) context.onApprovalRequest = onApprovalRequest;
  return context;
}

/** Fail loud on decisions that matched nothing — a caller bug, not a model condition. */
export function assertDecisionsConsumed(context: ApprovalContext): void {
  for (const id of context.decisions.keys()) {
    if (!context.consumedDecisions.has(id)) {
      throw new AIError(
        problemFromStatus(400, `toolApprovals references a tool call that is not awaiting approval: ${id}.`),
      );
    }
  }
}

export interface ResolvedTool {
  tool: Tool;
  schema: Schema;
  definition: ToolDefinition;
}

export interface ResolvedToolSet {
  definitions: ToolDefinition[];
  byName: Map<string, ResolvedTool>;
}

/** Resolve schemas once per call, not per step. */
export function resolveTools(tools: Tool[] | undefined): ResolvedToolSet | undefined {
  if (tools === undefined || tools.length === 0) return undefined;
  const byName = new Map<string, ResolvedTool>();
  const definitions: ToolDefinition[] = [];
  for (const tool of tools) {
    if (byName.has(tool.name)) {
      throw new AIError(problemFromStatus(400, `Duplicate tool name: ${tool.name}.`));
    }
    const schema = resolveSchema(tool.inputSchema);
    const definition: ToolDefinition = { name: tool.name, inputSchema: schema.jsonSchema };
    if (tool.description !== undefined) definition.description = tool.description;
    byName.set(tool.name, { tool, schema, definition });
    definitions.push(definition);
  }
  return { definitions, byName };
}

/** True when the model called a KNOWN tool that has no execute — a client-side tool the loop must hand back. */
export function hasClientSideCall(toolset: ResolvedToolSet, calls: ToolCallPart[]): boolean {
  return calls.some((call) => {
    const resolved = toolset.byName.get(call.toolName);
    return resolved !== undefined && resolved.tool.execute === undefined;
  });
}

export interface ApprovalContext {
  decisions: Map<string, ToolApprovalDecision>;
  consumedDecisions: Set<string>;
  onApprovalRequest?: (request: ToolApprovalRequest) => boolean | Promise<boolean>;
}

export interface PartitionedCalls {
  /** Calls cleared to execute (includes unknown tools, which execute to an error result). */
  approved: ToolCallPart[];
  /** Denial results for calls a decision or callback rejected. */
  denied: ToolResultPart[];
  /** Calls that still need a human decision — the suspension payload. */
  pending: ToolApprovalRequest[];
}

export async function partitionToolCalls(
  toolset: ResolvedToolSet,
  calls: ToolCallPart[],
  context: ApprovalContext,
): Promise<PartitionedCalls> {
  const approved: ToolCallPart[] = [];
  const denied: ToolResultPart[] = [];
  const pending: ToolApprovalRequest[] = [];

  for (const call of calls) {
    const resolved = toolset.byName.get(call.toolName);
    if (resolved === undefined) {
      approved.push(call);
      continue;
    }

    const decision = context.decisions.get(call.toolCallId);
    if (decision !== undefined) {
      context.consumedDecisions.add(call.toolCallId);
      if (decision.approved) approved.push(call);
      else denied.push(denialResult(call, decision.reason));
      continue;
    }

    const needs = resolved.tool.needsApproval;
    const required =
      needs === true || (typeof needs === "function" && (await needs(call.input)) === true);
    if (!required) {
      approved.push(call);
      continue;
    }

    const request: ToolApprovalRequest = {
      toolCallId: call.toolCallId,
      toolName: call.toolName,
      input: call.input,
    };
    if (context.onApprovalRequest !== undefined) {
      if (await context.onApprovalRequest(request)) approved.push(call);
      else denied.push(denialResult(call));
    } else {
      pending.push(request);
    }
  }

  return { approved, denied, pending };
}

export async function executeToolCalls(
  toolset: ResolvedToolSet,
  calls: ToolCallPart[],
  abortSignal?: AbortSignal,
): Promise<ToolResultPart[]> {
  return Promise.all(calls.map((call) => executeToolCall(toolset.byName.get(call.toolName), call, abortSignal)));
}

async function executeToolCall(
  resolved: ResolvedTool | undefined,
  call: ToolCallPart,
  abortSignal?: AbortSignal,
): Promise<ToolResultPart> {
  if (resolved === undefined) {
    return errorResult(call, `Unknown tool: ${call.toolName}.`);
  }
  const validation = await resolved.schema.validate(call.input);
  if (!validation.success) {
    const details = validation.issues
      .map((issue) => (issue.path !== undefined ? `${issue.path}: ${issue.message}` : issue.message))
      .join("; ");
    return errorResult(call, `Invalid tool input: ${details}`);
  }
  if (resolved.tool.execute === undefined) {
    return errorResult(call, `Tool ${call.toolName} cannot be executed here.`);
  }
  try {
    const context: ToolExecutionContext =
      abortSignal !== undefined ? { toolCallId: call.toolCallId, abortSignal } : { toolCallId: call.toolCallId };
    const output = await resolved.tool.execute(validation.value, context);
    return {
      type: "tool-result",
      toolCallId: call.toolCallId,
      toolName: call.toolName,
      output: output === undefined ? null : output,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return errorResult(call, `Tool execution failed: ${message}`);
  }
}

export function denialResult(
  call: { toolCallId: string; toolName: string },
  reason?: string,
): ToolResultPart {
  return errorResult(
    call,
    reason !== undefined ? `Tool call denied by the user: ${reason}` : "Tool call denied by the user.",
  );
}

function errorResult(call: { toolCallId: string; toolName: string }, message: string): ToolResultPart {
  return {
    type: "tool-result",
    toolCallId: call.toolCallId,
    toolName: call.toolName,
    output: message,
    isError: true,
  };
}

/** Restore the model-visible order after parallel execution. */
export function orderResults(calls: ToolCallPart[], results: ToolResultPart[]): ToolResultPart[] {
  const byId = new Map(results.map((result) => [result.toolCallId, result]));
  return calls
    .map((call) => byId.get(call.toolCallId))
    .filter((result): result is ToolResultPart => result !== undefined);
}

/** Tool calls in the trailing assistant message that have no tool result yet — a prior suspension. */
export function findDanglingToolCalls(messages: Message[]): ToolCallPart[] {
  let lastAssistant = -1;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i]!.role === "assistant") {
      lastAssistant = i;
      break;
    }
  }
  if (lastAssistant === -1) return [];
  const assistant = messages[lastAssistant] as AssistantMessage;
  if (typeof assistant.content === "string") return [];
  const calls = assistant.content.filter((part): part is ToolCallPart => part.type === "tool-call");
  if (calls.length === 0) return [];

  const resulted = new Set<string>();
  for (let i = lastAssistant + 1; i < messages.length; i++) {
    const message = messages[i]!;
    if (message.role === "tool") {
      for (const result of message.content) resulted.add(result.toolCallId);
    }
  }
  return calls.filter((call) => !resulted.has(call.toolCallId));
}
