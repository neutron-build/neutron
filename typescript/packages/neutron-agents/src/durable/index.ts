import { readdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import type { Tool } from "@neutron-build/ai";
import { parseDuration, workflow } from "@neutron-build/workflow";
import type { WorkflowDefinition } from "@neutron-build/workflow";
import { agentStep } from "@neutron-build/workflow/ai";
import type { AgentStepOptions } from "@neutron-build/workflow/ai";

import type { LoadedAgent } from "../agent.js";
import { AgentError, problemFromStatus } from "../executor.js";
import type { AgentExecutor } from "../executor.js";
import { execTool } from "../runtime.js";
import { skillTool } from "../skills.js";

export interface AgentWorkflowInput {
  input: string;
}

export interface AgentWorkflowOutput {
  text: string;
  rounds: number;
}

/**
 * An agent turn as a durable workflow: every model round is a recorded
 * step, approval-requiring tools park the run on waitForEvent (deliver
 * ToolApprovalDecision[] to resume — see approvalEventName in
 * @neutron-build/workflow/ai), and the run survives crashes, deploys,
 * and weeks of waiting. Requires @neutron-build/workflow (optional peer;
 * this subpath is the only place it loads).
 */
export function agentWorkflow(
  agent: LoadedAgent,
  options: { executor?: AgentExecutor; name?: string; runTimeout?: string | number } = {},
): WorkflowDefinition<AgentWorkflowInput, AgentWorkflowOutput> {
  const tools: Tool[] = [...agent.tools];
  if (options.executor !== undefined) tools.push(execTool(options.executor));
  if (agent.skills !== undefined && agent.skills.length > 0) {
    for (const skill of agent.skills) tools.push(...skill.tools);
    tools.push(skillTool(agent.skills));
  }

  return workflow(
    options.name ?? `agent:${agent.definition.name}`,
    async (ctx, input: AgentWorkflowInput) => {
      const call: AgentStepOptions = {
        model: agent.definition.model,
        prompt: input.input,
        maxSteps: agent.definition.maxSteps ?? 8,
      };
      if (agent.instructions !== "") call.system = agent.instructions;
      if (tools.length > 0) call.tools = tools;
      if (agent.definition.maxOutputTokens !== undefined) call.maxOutputTokens = agent.definition.maxOutputTokens;
      if (agent.definition.temperature !== undefined) call.temperature = agent.definition.temperature;

      const result = await agentStep(ctx, "turn", call);
      return { text: result.text, rounds: result.rounds };
    },
    options.runTimeout !== undefined ? { timeout: options.runTimeout } : {},
  );
}

/**
 * The schedules/ convention: one file per recurring run.
 *
 *   schedules/nightly.js:
 *     export default { name: "nightly", every: "1d", input: "run the nightly review" };
 */
export interface AgentSchedule {
  name: string;
  /** Interval between runs ("1d", "6h", or ms). */
  every: string | number;
  /** The turn input each scheduled run starts with. */
  input: string;
}

export async function loadSchedules(dir: string): Promise<AgentSchedule[]> {
  try {
    if (!(await stat(dir)).isDirectory()) return [];
  } catch {
    return [];
  }
  const schedules: AgentSchedule[] = [];
  for (const entry of (await readdir(dir)).sort()) {
    if (!entry.endsWith(".js") && !entry.endsWith(".mjs")) continue;
    const mod = (await import(pathToFileURL(join(dir, entry)).href)) as { default?: AgentSchedule };
    const schedule = mod.default;
    if (schedule === undefined || typeof schedule.name !== "string" || typeof schedule.input !== "string") {
      throw new AgentError(
        problemFromStatus(400, `schedules/${entry} must default-export { name, every, input }.`),
      );
    }
    parseDuration(schedule.every); // validate now, loudly
    schedules.push(schedule);
  }
  return schedules;
}

/**
 * Pure due-check for a poll loop: start a run when the interval has
 * elapsed since the last one (lastRunAt null = never ran → due). The
 * caller persists lastRunAt (e.g. in the workflow run index or KV).
 */
export function isScheduleDue(schedule: AgentSchedule, lastRunAt: string | null, now: Date): boolean {
  if (lastRunAt === null) return true;
  return Date.parse(lastRunAt) + parseDuration(schedule.every) <= now.getTime();
}
