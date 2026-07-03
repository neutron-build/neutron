import { generateText } from "@neutron-build/ai";
import type {
  FinishReason,
  GenerateTextOptions,
  Message,
  ToolApprovalDecision,
  ToolApprovalRequest,
  Usage,
} from "@neutron-build/ai";

import type { WorkflowContext } from "../context.js";

/**
 * generateText options minus the approval plumbing the bridge owns and the
 * non-durable bits (abort signals don't survive suspension).
 */
export type AgentStepOptions = Omit<
  GenerateTextOptions,
  "toolApprovals" | "onApprovalRequest" | "abortSignal"
>;

export interface AgentStepResult {
  text: string;
  reasoning: string;
  finishReason: FinishReason;
  usage: Usage;
  /** Full conversation after the final round. */
  messages: Message[];
  /** Model-call rounds, counting one per approval suspension plus the last. */
  rounds: number;
}

/** The event name a suspended agent step waits on; deliver ToolApprovalDecision[] to it. */
export function approvalEventName(stepName: string, round: number): string {
  return `${stepName}[${round}]:approvals`;
}

/**
 * The AI SDK approval bridge: run an agent loop as recorded workflow
 * steps. When a needsApproval tool suspends the loop, the workflow parks
 * on waitForEvent — a human decision delivered days later (deliverEvent /
 * the events webhook, payload: ToolApprovalDecision[]) resumes the loop
 * with those decisions. Nothing runs and nothing is lost in between; the
 * suspended round's recorded step result carries the pending
 * approvalRequests for UIs to render.
 */
export async function agentStep(
  ctx: WorkflowContext,
  name: string,
  options: AgentStepOptions,
): Promise<AgentStepResult> {
  let messages: Message[] | undefined;
  let approvals: ToolApprovalDecision[] | undefined;

  for (let round = 0; ; round++) {
    const call = { ...options } as GenerateTextOptions;
    if (messages !== undefined) {
      // Continuation rounds replay the recorded conversation, never the prompt.
      delete call.prompt;
      delete call.system;
      call.messages = messages;
    }
    if (approvals !== undefined) call.toolApprovals = approvals;

    const result = await ctx.step(`${name}[${round}]`, async () => {
      const r = await generateText(call);
      return {
        text: r.text,
        reasoning: r.reasoning,
        finishReason: r.finishReason,
        usage: r.usage,
        messages: r.messages,
        approvalRequests: r.approvalRequests,
      };
    });

    if (result.finishReason !== "tool-approval") {
      return {
        text: result.text,
        reasoning: result.reasoning,
        finishReason: result.finishReason,
        usage: result.usage,
        messages: result.messages,
        rounds: round + 1,
      };
    }

    approvals = await ctx.waitForEvent<ToolApprovalDecision[]>(approvalEventName(name, round));
    messages = result.messages;
  }
}

export type { ToolApprovalDecision, ToolApprovalRequest };
