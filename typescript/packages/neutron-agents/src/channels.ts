import type { Message } from "@neutron-build/ai";

import type { LoadedAgent } from "./agent.js";
import { AgentError, problemFromStatus } from "./executor.js";
import type { AgentExecutor } from "./executor.js";
import { runTurn } from "./runtime.js";

/**
 * The v1 http channel: a web-standard route handler that turns
 * POST { input?, messages? } into an agent turn and
 * returns { text, finishReason, messages, approvalRequests }. Plugs into
 * any Neutron mode:"api" route. Other surfaces (Slack, Discord, ...) are
 * later channels/ adapters behind the same shape — deliberately absent
 * from v1 per the platform plan (Chat SDK territory).
 *
 * An optional `auth` hook gates the call before the body is read. Without
 * one, a mount whose turn has any tools (agent tools, skills, or an
 * executor) refuses to run: anyone who can POST would drive those tools.
 * `allowUnauthenticated: true` opts out explicitly for local development.
 *
 * Client-supplied history is untrusted: `messages` may contain only user and
 * assistant text, never system or tool messages or assistant tool calls, and
 * `toolApprovals` is refused — otherwise a caller could forge a pending tool
 * call and approve it. Turns that suspend for approval resume server-side,
 * through agentWorkflow (`@neutron-build/agents/durable`) or runTurn.
 */
export function createAgentHandler(options: {
  agent: LoadedAgent;
  executor?: AgentExecutor;
  /** Return a Response to refuse the call with it, null to continue. Runs before the body is read. */
  auth?: (request: Request) => Promise<Response | null>;
  /** Explicit opt-out of the tools-without-auth refusal, for local development. */
  allowUnauthenticated?: boolean;
  /** Override the exec tool's approval gate (default: required for LocalExecutor). */
  execNeedsApproval?: boolean;
}): (request: Request) => Promise<Response> {
  return async (request: Request): Promise<Response> => {
    if (request.method !== "POST") {
      return problemResponse(problemFromStatus(400, "Use POST with { input, messages? }."));
    }
    const hasTools =
      options.executor !== undefined || options.agent.tools.length > 0 || (options.agent.skills?.length ?? 0) > 0;
    if (options.auth !== undefined) {
      const refusal = await options.auth(request);
      if (refusal !== null) return refusal;
    } else if (hasTools && options.allowUnauthenticated !== true) {
      return problemResponse(
        problemFromStatus(
          500,
          "Refusing to serve an agent with tools without authentication: pass an `auth` hook, or set `allowUnauthenticated: true` for local development.",
        ),
      );
    }
    let body: { input?: unknown; messages?: unknown; toolApprovals?: unknown };
    try {
      body = (await request.json()) as typeof body;
    } catch {
      return problemResponse(problemFromStatus(400, "Body must be JSON."));
    }
    if (body === null || typeof body !== "object") {
      return problemResponse(problemFromStatus(400, "Body must be a JSON object."));
    }
    if (body.input === undefined && body.messages === undefined) {
      return problemResponse(problemFromStatus(400, "Provide `input` and/or `messages`."));
    }
    if (body.input !== undefined && typeof body.input !== "string") {
      return problemResponse(problemFromStatus(400, "`input` must be a string."));
    }
    if (body.toolApprovals !== undefined) {
      return problemResponse(
        problemFromStatus(400, "`toolApprovals` is not accepted from clients; resume approvals server-side."),
      );
    }
    let messages: Message[] | undefined;
    if (body.messages !== undefined) {
      const parsed = parseClientMessages(body.messages);
      if (parsed === null) {
        return problemResponse(
          problemFromStatus(400, "`messages` may contain only user and assistant text messages."),
        );
      }
      messages = parsed;
    }

    try {
      const result = await runTurn(options.agent, {
        ...(typeof body.input === "string" ? { input: body.input } : {}),
        ...(messages !== undefined ? { messages } : {}),
        ...(options.executor !== undefined ? { executor: options.executor } : {}),
        ...(options.execNeedsApproval !== undefined ? { execNeedsApproval: options.execNeedsApproval } : {}),
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

/** Accept only user/assistant messages whose content is a string or text parts; null otherwise. */
function parseClientMessages(value: unknown): Message[] | null {
  if (!Array.isArray(value)) return null;
  const out: Message[] = [];
  for (const raw of value) {
    if (raw === null || typeof raw !== "object") return null;
    const { role, content } = raw as { role?: unknown; content?: unknown };
    if (role !== "user" && role !== "assistant") return null;
    if (typeof content === "string") {
      out.push({ role, content });
      continue;
    }
    if (!Array.isArray(content)) return null;
    const parts: { type: "text"; text: string }[] = [];
    for (const part of content) {
      if (part === null || typeof part !== "object") return null;
      const { type, text } = part as { type?: unknown; text?: unknown };
      if (type !== "text" || typeof text !== "string") return null;
      parts.push({ type: "text", text });
    }
    out.push({ role, content: parts });
  }
  return out;
}

function problemResponse(problem: { status: number }): Response {
  return Response.json(problem, {
    status: problem.status,
    headers: { "content-type": "application/problem+json" },
  });
}
