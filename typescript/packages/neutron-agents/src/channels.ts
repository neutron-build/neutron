import type { Message, ToolApprovalDecision } from "@neutron-build/ai";

import type { LoadedAgent } from "./agent.js";
import { AgentError, problemFromStatus } from "./executor.js";
import type { AgentExecutor } from "./executor.js";
import { runTurn } from "./runtime.js";

/**
 * The v1 http channel: a web-standard route handler that turns
 * POST { input?, messages?, toolApprovals? } into an agent turn and
 * returns { text, finishReason, messages, approvalRequests }. Plugs into
 * any Neutron mode:"api" route. Other surfaces (Slack, Discord, ...) are
 * later channels/ adapters behind the same shape — deliberately absent
 * from v1 per the platform plan (Chat SDK territory).
 */
export function createAgentHandler(options: {
  agent: LoadedAgent;
  executor?: AgentExecutor;
}): (request: Request) => Promise<Response> {
  return async (request: Request): Promise<Response> => {
    if (request.method !== "POST") {
      return problemResponse(problemFromStatus(400, "Use POST with { input, messages?, toolApprovals? }."));
    }
    let body: { input?: string; messages?: Message[]; toolApprovals?: ToolApprovalDecision[] };
    try {
      body = (await request.json()) as typeof body;
    } catch {
      return problemResponse(problemFromStatus(400, "Body must be JSON."));
    }
    if (body.input === undefined && body.messages === undefined) {
      return problemResponse(problemFromStatus(400, "Provide `input` and/or `messages`."));
    }

    try {
      const result = await runTurn(options.agent, {
        ...(body.input !== undefined ? { input: body.input } : {}),
        ...(body.messages !== undefined ? { messages: body.messages } : {}),
        ...(body.toolApprovals !== undefined ? { toolApprovals: body.toolApprovals } : {}),
        ...(options.executor !== undefined ? { executor: options.executor } : {}),
      });
      return Response.json({
        text: result.text,
        finishReason: result.finishReason,
        messages: result.messages,
        approvalRequests: result.approvalRequests,
      });
    } catch (error) {
      if (error instanceof AgentError) return problemResponse(error.problem);
      const problem =
        error instanceof Error && "problem" in error
          ? (error as { problem: { status: number } }).problem
          : problemFromStatus(500, error instanceof Error ? error.message : String(error));
      return problemResponse(problem);
    }
  };
}

function problemResponse(problem: { status: number }): Response {
  return Response.json(problem, {
    status: problem.status,
    headers: { "content-type": "application/problem+json" },
  });
}
