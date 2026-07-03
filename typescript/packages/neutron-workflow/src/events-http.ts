import { WorkflowError, problemFromStatus } from "./errors.js";
import { deliverEvent } from "./run.js";
import type { RunIndex } from "./scheduler.js";
import type { EventStore } from "./store.js";

/**
 * Web-standard route handler for signaling suspended runs:
 * POST { runId, name, payload? } → 202. Plugs into any Neutron
 * mode:"api" route (or any framework that speaks Request/Response).
 * With an index, the run is flagged so the next scheduler tick resumes it.
 */
export function createEventsHandler(options: {
  store: EventStore;
  index?: RunIndex;
}): (request: Request) => Promise<Response> {
  return async (request: Request): Promise<Response> => {
    if (request.method !== "POST") {
      return problemResponse(problemFromStatus(400, "Use POST with { runId, name, payload? }."));
    }
    let body: { runId?: string; name?: string; payload?: unknown };
    try {
      body = (await request.json()) as typeof body;
    } catch {
      return problemResponse(problemFromStatus(400, "Body must be JSON."));
    }
    if (typeof body.runId !== "string" || typeof body.name !== "string") {
      return problemResponse(problemFromStatus(400, "`runId` and `name` are required strings."));
    }
    try {
      await deliverEvent(options.store, body.runId, body.name, body.payload);
      await options.index?.markWake(body.runId);
      return Response.json({ delivered: true }, { status: 202 });
    } catch (error) {
      if (error instanceof WorkflowError) return problemResponse(error.problem);
      return problemResponse(
        problemFromStatus(500, error instanceof Error ? error.message : String(error)),
      );
    }
  };
}

function problemResponse(problem: { status: number }): Response {
  return Response.json(problem, {
    status: problem.status,
    headers: { "content-type": "application/problem+json" },
  });
}
