import type { ModelAdapter } from "./adapter.js";
import type { CallOptions } from "./call-options.js";
import { buildAdapterOptions, resolveInitialMessages } from "./call-options.js";
import { AIError, problemFromStatus } from "./errors.js";
import { retryOptionsFrom, withRetries } from "./internal/retry.js";
import {
  ZERO_USAGE,
  addUsage,
  assertDecisionsConsumed,
  executeToolCalls,
  findDanglingToolCalls,
  hasClientSideCall,
  makeApprovalContext,
  orderResults,
  partitionToolCalls,
  resolveTools,
} from "./internal/tool-execution.js";
import type {
  AssistantContentPart,
  FinishReason,
  Message,
  TextPart,
  ToolApprovalDecision,
  ToolApprovalRequest,
  ToolCallPart,
  ToolResultPart,
  Usage,
} from "./types.js";

/** One model call plus the tool work it triggered. */
export interface StepResult {
  text: string;
  content: AssistantContentPart[];
  toolCalls: ToolCallPart[];
  toolResults: ToolResultPart[];
  finishReason: FinishReason;
  usage: Usage;
  raw?: unknown;
}

export interface GenerateTextOptions extends CallOptions {
  model: ModelAdapter;
  /** Model-call budget for the tool loop. Default 1: one generation, tools of that step still execute. */
  maxSteps?: number;
  onStepFinish?: (step: StepResult) => void | Promise<void>;
  /** Inline approval handler; without it, approval-requiring calls suspend the run instead. */
  onApprovalRequest?: (request: ToolApprovalRequest) => boolean | Promise<boolean>;
  /** Decisions for the approval requests of a previously suspended run (pass its `messages` back too). */
  toolApprovals?: ToolApprovalDecision[];
}

export interface GenerateTextResult {
  /** All text across steps, concatenated. */
  text: string;
  /** All extended-thinking text across steps ("" when the model emitted none). */
  reasoning: string;
  /** Final step's content/toolCalls/toolResults; per-step detail lives in `steps`. */
  content: AssistantContentPart[];
  toolCalls: ToolCallPart[];
  toolResults: ToolResultPart[];
  steps: StepResult[];
  /** "tool-approval" means the run suspended; resume with `messages` + `toolApprovals`. */
  finishReason: FinishReason;
  approvalRequests: ToolApprovalRequest[];
  /** Total usage across steps; per-step usage lives in `steps`. */
  usage: Usage;
  /** The full conversation including generated turns — persistable, resume-ready. */
  messages: Message[];
  raw: unknown;
}

export async function generateText(options: GenerateTextOptions): Promise<GenerateTextResult> {
  const maxSteps = options.maxSteps ?? 1;
  if (!Number.isInteger(maxSteps) || maxSteps < 1) {
    throw new AIError(problemFromStatus(400, "`maxSteps` must be a positive integer."));
  }
  const toolset = resolveTools(options.tools);
  const messages = resolveInitialMessages(options);
  const approvalContext = makeApprovalContext(options.toolApprovals, options.onApprovalRequest);

  const retry = retryOptionsFrom(options);
  const steps: StepResult[] = [];
  const textSegments: string[] = [];
  const reasoningSegments: string[] = [];
  let totalUsage = ZERO_USAGE;

  // Resume path: settle tool calls left dangling by a prior suspension.
  const dangling = toolset !== undefined ? findDanglingToolCalls(messages) : [];
  if (toolset !== undefined && dangling.length > 0) {
    const { approved, denied, pending } = await partitionToolCalls(toolset, dangling, approvalContext);
    assertDecisionsConsumed(approvalContext);
    const executed = await executeToolCalls(toolset, approved, options.abortSignal);
    const settled = orderResults(dangling, [...denied, ...executed]);
    if (settled.length > 0) messages.push({ role: "tool", content: settled });
    if (pending.length > 0) {
      return buildResult(steps, textSegments, reasoningSegments, totalUsage, messages, pending, "tool-approval");
    }
  } else {
    assertDecisionsConsumed(approvalContext);
  }

  let finishReason: FinishReason = "other";
  for (let stepIndex = 0; stepIndex < maxSteps; stepIndex++) {
    const result = await withRetries(
      () => options.model.doGenerate(buildAdapterOptions(options, messages, toolset?.definitions)),
      retry,
    );
    totalUsage = addUsage(totalUsage, result.usage);
    const text = result.content
      .filter((part): part is TextPart => part.type === "text")
      .map((part) => part.text)
      .join("");
    textSegments.push(text);
    reasoningSegments.push(
      result.content
        .filter((part) => part.type === "reasoning")
        .map((part) => (part as { text: string }).text)
        .join(""),
    );
    const toolCalls = result.content.filter((part): part is ToolCallPart => part.type === "tool-call");
    messages.push({ role: "assistant", content: result.content });
    const step: StepResult = {
      text,
      content: result.content,
      toolCalls,
      toolResults: [],
      finishReason: result.finishReason,
      usage: result.usage,
      raw: result.raw,
    };
    steps.push(step);
    finishReason = result.finishReason;

    if (
      toolset === undefined ||
      result.finishReason !== "tool-calls" ||
      toolCalls.length === 0 ||
      hasClientSideCall(toolset, toolCalls)
    ) {
      await options.onStepFinish?.(step);
      break;
    }

    const { approved, denied, pending } = await partitionToolCalls(toolset, toolCalls, approvalContext);
    const executed = await executeToolCalls(toolset, approved, options.abortSignal);
    const settled = orderResults(toolCalls, [...denied, ...executed]);
    step.toolResults = settled;
    if (settled.length > 0) messages.push({ role: "tool", content: settled });
    await options.onStepFinish?.(step);

    if (pending.length > 0) {
      return buildResult(steps, textSegments, reasoningSegments, totalUsage, messages, pending, "tool-approval");
    }
  }

  return buildResult(steps, textSegments, reasoningSegments, totalUsage, messages, [], finishReason);
}

function buildResult(
  steps: StepResult[],
  textSegments: string[],
  reasoningSegments: string[],
  usage: Usage,
  messages: Message[],
  approvalRequests: ToolApprovalRequest[],
  finishReason: FinishReason,
): GenerateTextResult {
  const last = steps[steps.length - 1];
  return {
    text: textSegments.join(""),
    reasoning: reasoningSegments.join(""),
    content: last?.content ?? [],
    toolCalls: last?.toolCalls ?? [],
    toolResults: last?.toolResults ?? [],
    steps,
    finishReason,
    approvalRequests,
    usage,
    messages,
    raw: last?.raw ?? null,
  };
}
