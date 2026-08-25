import type { ModelAdapter } from "./adapter.js";
import type { CallOptions } from "./call-options.js";
import { buildAdapterOptions, resolveInitialMessages } from "./call-options.js";
import { AIError, problemFromStatus } from "./errors.js";
import { deferred } from "./internal/deferred.js";
import { backoff, isRetryableError, retryOptionsFrom } from "./internal/retry.js";
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
import type { StepResult } from "./generate-text.js";
import type {
  AssistantContentPart,
  FinishReason,
  Message,
  ReasoningPart,
  StreamPart,
  ToolApprovalDecision,
  ToolApprovalRequest,
  ToolCallPart,
  ToolResultPart,
  Usage,
} from "./types.js";

export interface StreamTextOptions extends CallOptions {
  model: ModelAdapter;
  /** Model-call budget for the tool loop. Default 1: one generation, tools of that step still execute. */
  maxSteps?: number;
  onStepFinish?: (step: StepResult) => void | Promise<void>;
  /** Inline approval handler; without it, approval-requiring calls suspend the run instead. */
  onApprovalRequest?: (request: ToolApprovalRequest) => boolean | Promise<boolean>;
  /** Decisions for the approval requests of a previously suspended run (pass its `messages` back too). */
  toolApprovals?: ToolApprovalDecision[];
}

export interface StreamTextResult {
  /** Every stream part across all steps. The underlying stream supports a single consumer. */
  readonly fullStream: AsyncIterable<StreamPart>;
  /** Text deltas only, sharing the same single-consumer stream. */
  readonly textStream: AsyncIterable<string>;
  /** Settle once the stream completes; awaiting them drains the stream if nothing else is consuming it. */
  readonly text: Promise<string>;
  readonly toolCalls: Promise<ToolCallPart[]>;
  readonly toolResults: Promise<ToolResultPart[]>;
  readonly finishReason: Promise<FinishReason>;
  readonly usage: Promise<Usage>;
  readonly steps: Promise<StepResult[]>;
  /** The full conversation including generated turns — persistable, resume-ready. */
  readonly messages: Promise<Message[]>;
  readonly approvalRequests: Promise<ToolApprovalRequest[]>;
}

export function streamText(options: StreamTextOptions): StreamTextResult {
  const maxSteps = options.maxSteps ?? 1;
  if (!Number.isInteger(maxSteps) || maxSteps < 1) {
    throw new AIError(problemFromStatus(400, "`maxSteps` must be a positive integer."));
  }
  return new StreamTextResultImpl(options, resolveInitialMessages(options), maxSteps);
}

/** Rejection reason for result promises when the consumer abandons the stream. */
const ABANDONED_STREAM = new AIError(
  problemFromStatus(400, "The stream was abandoned before completion; result promises cannot be fulfilled."),
);

class StreamTextResultImpl implements StreamTextResult {
  #options: StreamTextOptions;
  #messages: Message[];
  #maxSteps: number;
  #consumed = false;
  #textDeferred = deferred<string>();
  #toolCallsDeferred = deferred<ToolCallPart[]>();
  #toolResultsDeferred = deferred<ToolResultPart[]>();
  #finishDeferred = deferred<FinishReason>();
  #usageDeferred = deferred<Usage>();
  #stepsDeferred = deferred<StepResult[]>();
  #messagesDeferred = deferred<Message[]>();
  #approvalsDeferred = deferred<ToolApprovalRequest[]>();

  constructor(options: StreamTextOptions, messages: Message[], maxSteps: number) {
    this.#options = options;
    this.#messages = messages;
    this.#maxSteps = maxSteps;
  }

  /** Claims the single consumption slot synchronously, then hands out the stream. */
  #start(): AsyncGenerator<StreamPart, void, undefined> {
    if (this.#consumed) {
      throw new AIError(
        problemFromStatus(400, "This stream was already consumed; call streamText() again for a new stream."),
      );
    }
    this.#consumed = true;
    return this.#iterate();
  }

  async *#iterate(): AsyncGenerator<StreamPart, void, undefined> {
    try {
      const options = this.#options;
      const toolset = resolveTools(options.tools);
      const messages = this.#messages;
      const approvalContext = makeApprovalContext(options.toolApprovals, options.onApprovalRequest);

      const steps: StepResult[] = [];
      let totalUsage = ZERO_USAGE;
      let finalReason: FinishReason = "other";
      let approvalRequests: ToolApprovalRequest[] = [];
      let text = "";
      let suspended = false;

      // Resume path: settle tool calls left dangling by a prior suspension.
      const dangling = toolset !== undefined ? findDanglingToolCalls(messages) : [];
      if (toolset !== undefined && dangling.length > 0) {
        const { approved, denied, pending } = await partitionToolCalls(toolset, dangling, approvalContext);
        assertDecisionsConsumed(approvalContext);
        const executed = await executeToolCalls(toolset, approved, options.abortSignal);
        const settled = orderResults(dangling, [...denied, ...executed]);
        if (settled.length > 0) {
          messages.push({ role: "tool", content: settled });
          for (const result of settled) yield result;
        }
        if (pending.length > 0) {
          approvalRequests = pending;
          for (const request of pending) yield { type: "approval-request", request };
          finalReason = "tool-approval";
          suspended = true;
        }
      } else {
        assertDecisionsConsumed(approvalContext);
      }

      if (!suspended) {
        const retry = retryOptionsFrom(options);
        for (let stepIndex = 0; stepIndex < this.#maxSteps; stepIndex++) {
          let stepText = "";
          const stepCalls: ToolCallPart[] = [];
          const stepReasoning: ReasoningPart[] = [];
          let stepFinish: FinishReason = "other";
          let stepUsage = ZERO_USAGE;

          // Streams retry only while nothing has been produced; once the
          // consumer saw output, a retry would duplicate it.
          for (let attempt = 0; ; attempt++) {
            let produced = false;
            try {
              const parts = options.model.doStream(buildAdapterOptions(options, messages, toolset?.definitions));
              for await (const part of parts) {
                produced = true;
                if (part.type === "text-delta") {
                  stepText += part.text;
                  yield part;
                } else if (part.type === "reasoning-delta") {
                  yield part;
                } else if (part.type === "reasoning") {
                  stepReasoning.push(part);
                  yield part;
                } else if (part.type === "tool-input-start" || part.type === "tool-input-delta") {
                  yield part;
                } else if (part.type === "tool-call") {
                  stepCalls.push(part);
                  yield part;
                } else {
                  stepFinish = part.finishReason;
                  stepUsage = part.usage;
                }
              }
              break;
            } catch (error) {
              if (
                produced ||
                attempt >= retry.maxRetries ||
                !isRetryableError(error) ||
                options.abortSignal?.aborted === true
              ) {
                throw error;
              }
              await backoff(attempt, retry);
            }
          }

          totalUsage = addUsage(totalUsage, stepUsage);
          text += stepText;
          const content: AssistantContentPart[] = [...stepReasoning];
          if (stepText !== "") content.push({ type: "text", text: stepText });
          content.push(...stepCalls);
          messages.push({ role: "assistant", content });
          const step: StepResult = {
            text: stepText,
            content,
            toolCalls: stepCalls,
            toolResults: [],
            finishReason: stepFinish,
            usage: stepUsage,
          };
          steps.push(step);
          finalReason = stepFinish;

          if (
            toolset === undefined ||
            stepFinish !== "tool-calls" ||
            stepCalls.length === 0 ||
            hasClientSideCall(toolset, stepCalls)
          ) {
            await options.onStepFinish?.(step);
            break;
          }

          const { approved, denied, pending } = await partitionToolCalls(toolset, stepCalls, approvalContext);
          const executed = await executeToolCalls(toolset, approved, options.abortSignal);
          const settled = orderResults(stepCalls, [...denied, ...executed]);
          step.toolResults = settled;
          if (settled.length > 0) {
            messages.push({ role: "tool", content: settled });
            for (const result of settled) yield result;
          }
          await options.onStepFinish?.(step);

          if (pending.length > 0) {
            approvalRequests = pending;
            for (const request of pending) yield { type: "approval-request", request };
            finalReason = "tool-approval";
            break;
          }
          if (stepIndex < this.#maxSteps - 1) {
            yield { type: "step-finish", finishReason: stepFinish, usage: stepUsage };
          }
        }
      }

      yield { type: "finish", finishReason: finalReason, usage: totalUsage };

      const last = steps[steps.length - 1];
      this.#textDeferred.resolve(text);
      this.#toolCallsDeferred.resolve(last?.toolCalls ?? []);
      this.#toolResultsDeferred.resolve(last?.toolResults ?? []);
      this.#finishDeferred.resolve(finalReason);
      this.#usageDeferred.resolve(totalUsage);
      this.#stepsDeferred.resolve(steps);
      this.#messagesDeferred.resolve([...messages]);
      this.#approvalsDeferred.resolve(approvalRequests);
    } catch (error) {
      this.#textDeferred.reject(error);
      this.#toolCallsDeferred.reject(error);
      this.#toolResultsDeferred.reject(error);
      this.#finishDeferred.reject(error);
      this.#usageDeferred.reject(error);
      this.#stepsDeferred.reject(error);
      this.#messagesDeferred.reject(error);
      this.#approvalsDeferred.reject(error);
      throw error;
    } finally {
      // A consumer that breaks out of the stream abandons the generator at a
      // yield: neither the resolve block nor the catch above runs, and every
      // result deferred stays pending forever — `await result.text` hung with
      // no error and no timeout. Settle them here; on the normal and error
      // paths the deferreds are already settled and rejecting again is a
      // no-op.
      this.#textDeferred.reject(ABANDONED_STREAM);
      this.#toolCallsDeferred.reject(ABANDONED_STREAM);
      this.#toolResultsDeferred.reject(ABANDONED_STREAM);
      this.#finishDeferred.reject(ABANDONED_STREAM);
      this.#usageDeferred.reject(ABANDONED_STREAM);
      this.#stepsDeferred.reject(ABANDONED_STREAM);
      this.#messagesDeferred.reject(ABANDONED_STREAM);
      this.#approvalsDeferred.reject(ABANDONED_STREAM);
    }
  }

  #drain(): void {
    if (this.#consumed) return;
    const parts = this.#start();
    void (async () => {
      for await (const _part of parts) {
        // accumulate only; results surface via the promises
      }
    })().catch(() => {});
  }

  get fullStream(): AsyncIterable<StreamPart> {
    return this.#start();
  }

  get textStream(): AsyncIterable<string> {
    const parts = this.#start();
    return (async function* () {
      for await (const part of parts) {
        if (part.type === "text-delta") yield part.text;
      }
    })();
  }

  get text(): Promise<string> {
    this.#drain();
    return this.#textDeferred.promise;
  }

  get toolCalls(): Promise<ToolCallPart[]> {
    this.#drain();
    return this.#toolCallsDeferred.promise;
  }

  get toolResults(): Promise<ToolResultPart[]> {
    this.#drain();
    return this.#toolResultsDeferred.promise;
  }

  get finishReason(): Promise<FinishReason> {
    this.#drain();
    return this.#finishDeferred.promise;
  }

  get usage(): Promise<Usage> {
    this.#drain();
    return this.#usageDeferred.promise;
  }

  get steps(): Promise<StepResult[]> {
    this.#drain();
    return this.#stepsDeferred.promise;
  }

  get messages(): Promise<Message[]> {
    this.#drain();
    return this.#messagesDeferred.promise;
  }

  get approvalRequests(): Promise<ToolApprovalRequest[]> {
    this.#drain();
    return this.#approvalsDeferred.promise;
  }
}
