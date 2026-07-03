import { generateText, jsonSchema } from "@neutron-build/ai";
import type {
  GenerateTextResult,
  Message,
  Tool,
  ToolApprovalDecision,
  ToolApprovalRequest,
} from "@neutron-build/ai";

import type { LoadedAgent } from "./agent.js";
import type { AgentExecutor } from "./executor.js";
import { skillTool } from "./skills.js";
import type { Skill } from "./skills.js";

export interface RunTurnOptions {
  /** The user message for this turn. Omit when resuming with toolApprovals. */
  input?: string;
  /** Prior conversation (e.g. a previous turn's result.messages). */
  messages?: Message[];
  /** Compute for the built-in exec tool; without it the tool is absent. */
  executor?: AgentExecutor;
  /** Extra skills for this turn, on top of the agent's own. */
  skills?: Skill[];
  onApprovalRequest?: (request: ToolApprovalRequest) => boolean | Promise<boolean>;
  /** Decisions for a previously suspended turn (pass its messages back too). */
  toolApprovals?: ToolApprovalDecision[];
  abortSignal?: AbortSignal;
}

/**
 * One agent turn: instructions + tools (+ the executor-backed exec tool)
 * through the AI SDK's multi-step loop. Approval suspension passes
 * through untouched — result.finishReason "tool-approval" with
 * approvalRequests and resume-ready messages, exactly the shape
 * agentStep() in @neutron-build/workflow makes durable.
 */
export async function runTurn(agent: LoadedAgent, options: RunTurnOptions): Promise<GenerateTextResult> {
  const tools: Tool[] = [...agent.tools];
  if (options.executor !== undefined) {
    tools.push(execTool(options.executor));
  }
  const skills = [...(agent.skills ?? []), ...(options.skills ?? [])];
  if (skills.length > 0) {
    for (const skill of skills) tools.push(...skill.tools);
    tools.push(skillTool(skills));
  }

  const messages: Message[] = [...(options.messages ?? [])];
  if (options.input !== undefined) {
    messages.push({ role: "user", content: options.input });
  }

  return generateText({
    model: agent.definition.model,
    ...(agent.instructions !== "" ? { system: agent.instructions } : {}),
    messages,
    ...(tools.length > 0 ? { tools } : {}),
    maxSteps: agent.definition.maxSteps ?? 8,
    ...(agent.definition.maxOutputTokens !== undefined
      ? { maxOutputTokens: agent.definition.maxOutputTokens }
      : {}),
    ...(agent.definition.temperature !== undefined ? { temperature: agent.definition.temperature } : {}),
    ...(options.onApprovalRequest !== undefined ? { onApprovalRequest: options.onApprovalRequest } : {}),
    ...(options.toolApprovals !== undefined ? { toolApprovals: options.toolApprovals } : {}),
    ...(options.abortSignal !== undefined ? { abortSignal: options.abortSignal } : {}),
  });
}

/** The built-in shell tool the runtime wires to an AgentExecutor. */
export function execTool(executor: AgentExecutor): Tool {
  return {
    name: "exec",
    description:
      "Run a shell command in the agent workspace. Returns exit code, stdout, and stderr.",
    inputSchema: jsonSchema({
      type: "object",
      properties: {
        command: { type: "string", description: "The shell command to run." },
        cwd: { type: "string", description: "Working directory, relative to the workspace root." },
      },
      required: ["command"],
      additionalProperties: false,
    }),
    execute: async (input) => {
      const { command, cwd } = input as { command: string; cwd?: string };
      return executor.exec(command, cwd !== undefined ? { cwd } : {});
    },
  };
}
