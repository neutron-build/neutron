import { AIError, problemFromStatus } from "../errors.js";
import type { ProblemDetails } from "../errors.js";
import { deferred } from "../internal/deferred.js";
import { abandonmentSettler, drainEvents } from "../internal/run-to-promise.js";
import type { ModelAdapter } from "../adapter.js";
import { streamText } from "../stream-text.js";
import type { StreamTextOptions } from "../stream-text.js";
import type { Tool } from "../tool.js";
import type {
  Message,
  ToolApprovalDecision,
  ToolApprovalRequest,
  Usage,
} from "../types.js";

export { claudeCode } from "./claude-code.js";
export type { ClaudeCodeSettings, SpawnedProcess, SpawnFn } from "./claude-code.js";

/**
 * The agent-agnostic harness boundary: one interface that an in-process
 * agent (localAgent, on this SDK's own loop), a CLI agent (claudeCode),
 * or any future agent implements. Consumers — Neutron Agents, dashboards,
 * the Teploy agent product — drive all of them identically.
 */
export interface AgentHarness {
  readonly name: string;
  run(options: AgentRunOptions): AgentRun;
}

export interface AgentRunOptions {
  /** The task. Optional only when resuming a session (e.g. with toolApprovals). */
  prompt?: string;
  /** Continue a previous run's session. */
  sessionId?: string;
  model?: string;
  cwd?: string;
  /** Inline approval handler; without it, harnesses that support suspension suspend. */
  onApprovalRequest?: (request: ToolApprovalRequest) => boolean | Promise<boolean>;
  /** Decisions for a previously suspended run (harnesses without suspension ignore them). */
  toolApprovals?: ToolApprovalDecision[];
  abortSignal?: AbortSignal;
}

export type AgentEvent =
  | { type: "session"; sessionId: string }
  | { type: "text-delta"; text: string }
  | { type: "tool-start"; toolCallId: string; toolName: string; input: unknown }
  | { type: "tool-end"; toolCallId: string; toolName: string; output: unknown; isError?: boolean }
  | { type: "approval-request"; request: ToolApprovalRequest }
  | { type: "finish"; status: AgentResultStatus };

export type AgentResultStatus = "completed" | "suspended" | "cancelled" | "error";

export interface AgentResult {
  status: AgentResultStatus;
  /** All assistant text produced by the run. */
  output: string;
  sessionId?: string;
  usage?: Usage;
  costUSD?: number;
  approvalRequests?: ToolApprovalRequest[];
  error?: ProblemDetails;
  raw?: unknown;
}

/**
 * A running agent. `events` is single-consumer and never throws — failures
 * arrive as a `finish` event with status "error" plus `result.error`, so
 * every harness (in-process or subprocess) fails the same way.
 */
export interface AgentRun {
  readonly events: AsyncIterable<AgentEvent>;
  /** Settles when the run ends; awaiting it drains events if nothing else is consuming them. */
  readonly result: Promise<AgentResult>;
  stop(): void;
}

export interface LocalAgentOptions {
  model: ModelAdapter;
  tools?: Tool[];
  system?: string;
  /** Model-call budget per run. */
  maxSteps?: number;
  /** Session history capacity (default 64); the least-recently-used session is evicted beyond it. */
  sessionCapacity?: number;
}

/** Default cap on retained sessions — see LocalAgentOptions.sessionCapacity. */
const DEFAULT_SESSION_CAPACITY = 64;

/**
 * Insertion-ordered session store with a capacity: reads and writes refresh
 * recency, and an insert at capacity evicts the least-recently-used entry.
 * Without a bound, a long-lived harness accumulated every conversation it
 * ever ran.
 */
class BoundedSessions {
  #entries = new Map<string, Message[]>();
  #capacity: number;

  constructor(capacity: number) {
    this.#capacity = capacity;
  }

  get(sessionId: string): Message[] | undefined {
    const messages = this.#entries.get(sessionId);
    if (messages === undefined) return undefined;
    this.#entries.delete(sessionId);
    this.#entries.set(sessionId, messages);
    return messages;
  }

  set(sessionId: string, messages: Message[]): void {
    this.#entries.delete(sessionId);
    this.#entries.set(sessionId, messages);
    while (this.#entries.size > this.#capacity) {
      const oldest = this.#entries.keys().next().value;
      if (oldest === undefined) break;
      this.#entries.delete(oldest);
    }
  }
}

/**
 * Reference harness: the SDK's own tool loop behind the AgentHarness
 * interface, with bounded in-memory sessions. Also the executable proof that
 * the interface is agent-agnostic rather than shaped around any one CLI.
 */
export function localAgent(options: LocalAgentOptions): AgentHarness {
  const capacity = options.sessionCapacity ?? DEFAULT_SESSION_CAPACITY;
  if (!Number.isInteger(capacity) || capacity < 1) {
    throw new AIError(problemFromStatus(400, "`sessionCapacity` must be a positive integer."));
  }
  const sessions = new BoundedSessions(capacity);
  return {
    name: "local",
    run(runOptions: AgentRunOptions): AgentRun {
      if (runOptions.prompt === undefined && runOptions.sessionId === undefined) {
        throw new AIError(problemFromStatus(400, "Provide `prompt`, or `sessionId` to resume a session."));
      }
      return new LocalAgentRun(options, runOptions, sessions);
    },
  };
}

class LocalAgentRun implements AgentRun {
  #agent: LocalAgentOptions;
  #options: AgentRunOptions;
  #sessions: BoundedSessions;
  #consumed = false;
  #abort = new AbortController();
  #resultDeferred = deferred<AgentResult>();
  #onCallerAbort = () => this.#abort.abort();
  #settleAbandoned = abandonmentSettler(
    [this.#resultDeferred],
    // Every exit path (completion, error, abandonment) drops the caller's
    // abort listener instead of leaking one per run.
    () => this.#options.abortSignal?.removeEventListener("abort", this.#onCallerAbort),
  );

  constructor(agent: LocalAgentOptions, options: AgentRunOptions, sessions: BoundedSessions) {
    this.#agent = agent;
    this.#options = options;
    this.#sessions = sessions;
    options.abortSignal?.addEventListener("abort", this.#onCallerAbort);
  }

  #start(): AsyncGenerator<AgentEvent, void, undefined> {
    if (this.#consumed) {
      throw new AIError(problemFromStatus(400, "This run's events were already consumed."));
    }
    this.#consumed = true;
    return this.#iterate();
  }

  // The abandonment guard lives OUTSIDE the body: the `session` event yields
  // before the inner try, and a consumer that breaks on that first event
  // would otherwise return through no finally at all.
  async *#iterate(): AsyncGenerator<AgentEvent, void, undefined> {
    try {
      yield* this.#iterateBody();
    } finally {
      this.#settleAbandoned();
    }
  }

  async *#iterateBody(): AsyncGenerator<AgentEvent, void, undefined> {
    const sessionId = this.#options.sessionId ?? crypto.randomUUID();
    yield { type: "session", sessionId };

    const messages: Message[] = [...(this.#sessions.get(sessionId) ?? [])];
    if (this.#options.prompt !== undefined && this.#options.prompt !== "") {
      messages.push({ role: "user", content: this.#options.prompt });
    }

    const streamOptions: StreamTextOptions = {
      model: this.#agent.model,
      messages,
      maxSteps: this.#agent.maxSteps ?? 8,
      abortSignal: this.#abort.signal,
    };
    if (this.#agent.tools !== undefined) streamOptions.tools = this.#agent.tools;
    if (this.#agent.system !== undefined) streamOptions.system = this.#agent.system;
    if (this.#options.onApprovalRequest !== undefined) {
      streamOptions.onApprovalRequest = this.#options.onApprovalRequest;
    }
    if (this.#options.toolApprovals !== undefined) streamOptions.toolApprovals = this.#options.toolApprovals;

    let status: AgentResultStatus = "completed";
    let output = "";
    let usage: Usage | undefined;
    const approvalRequests: ToolApprovalRequest[] = [];

    try {
      const result = streamText(streamOptions);
      for await (const part of result.fullStream) {
        switch (part.type) {
          case "text-delta":
            output += part.text;
            yield part;
            break;
          case "tool-call":
            yield { type: "tool-start", toolCallId: part.toolCallId, toolName: part.toolName, input: part.input };
            break;
          case "tool-result": {
            const event: AgentEvent = {
              type: "tool-end",
              toolCallId: part.toolCallId,
              toolName: part.toolName,
              output: part.output,
            };
            if (part.isError === true) event.isError = true;
            yield event;
            break;
          }
          case "approval-request":
            approvalRequests.push(part.request);
            yield part;
            break;
          case "finish":
            usage = part.usage;
            if (part.finishReason === "tool-approval") status = "suspended";
            break;
          default:
            break; // tool-input-*, step-finish
        }
      }
      this.#sessions.set(sessionId, await result.messages);

      const agentResult: AgentResult = { status, output, sessionId };
      if (usage !== undefined) agentResult.usage = usage;
      if (approvalRequests.length > 0) agentResult.approvalRequests = approvalRequests;
      yield { type: "finish", status };
      this.#resultDeferred.resolve(agentResult);
    } catch (error) {
      if (this.#abort.signal.aborted) {
        yield { type: "finish", status: "cancelled" };
        this.#resultDeferred.resolve({ status: "cancelled", output, sessionId });
        return;
      }
      const problem =
        error instanceof AIError
          ? error.problem
          : problemFromStatus(500, error instanceof Error ? error.message : String(error));
      yield { type: "finish", status: "error" };
      this.#resultDeferred.resolve({ status: "error", output, sessionId, error: problem, raw: error });
    }
  }

  #drain(): void {
    if (this.#consumed) return;
    drainEvents(this.#start());
  }

  get events(): AsyncIterable<AgentEvent> {
    return this.#start();
  }

  get result(): Promise<AgentResult> {
    this.#drain();
    return this.#resultDeferred.promise;
  }

  stop(): void {
    this.#abort.abort();
  }
}
