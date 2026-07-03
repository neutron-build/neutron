import { AIError, problemFromStatus } from "./errors.js";
import type { ProblemDetails } from "./errors.js";
import { parseSSE } from "./internal/sse.js";
import type { StreamTextResult } from "./stream-text.js";
import type { StreamPart } from "./types.js";

/**
 * The chat wire format: one SSE `data:` event per StreamPart, a terminal
 * `{"type":"error",...}` event if the stream fails, then `data: [DONE]`.
 * Server side is a web-standard Response, so it works in any Neutron
 * mode:"api" route (or any framework that returns Responses).
 */
export function toEventStreamResponse(
  result: StreamTextResult,
  init: { status?: number; headers?: Record<string, string> } = {},
): Response {
  const encoder = new TextEncoder();
  const iterator = result.fullStream[Symbol.asyncIterator]();
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await iterator.next();
        if (done) {
          controller.enqueue(encoder.encode("data: [DONE]\n\n"));
          controller.close();
          return;
        }
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(value)}\n\n`));
      } catch (error) {
        const problem =
          error instanceof AIError
            ? error.problem
            : problemFromStatus(500, error instanceof Error ? error.message : String(error));
        controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: "error", problem })}\n\n`));
        controller.enqueue(encoder.encode("data: [DONE]\n\n"));
        controller.close();
      }
    },
    async cancel() {
      await iterator.return?.();
    },
  });
  return new Response(stream, {
    status: init.status ?? 200,
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      ...init.headers,
    },
  });
}

/** Decode the chat wire format back into StreamParts; error events throw AIError. */
export async function* streamPartsFromResponse(
  response: Response,
): AsyncGenerator<StreamPart, void, undefined> {
  if (!response.ok) {
    let problem: ProblemDetails | undefined;
    try {
      const json = (await response.json()) as Partial<ProblemDetails>;
      if (typeof json.status === "number" && typeof json.detail === "string") {
        problem = json as ProblemDetails;
      }
    } catch {
      // non-JSON body
    }
    throw new AIError(problem ?? problemFromStatus(response.status, `Chat request failed with status ${response.status}.`));
  }
  if (response.body === null) {
    throw new AIError(problemFromStatus(500, "Chat response had no body."));
  }

  for await (const event of parseSSE(response.body)) {
    if (event.data === "") continue;
    if (event.data === "[DONE]") return;
    let parsed: { type?: string; problem?: ProblemDetails };
    try {
      parsed = JSON.parse(event.data) as { type?: string; problem?: ProblemDetails };
    } catch {
      throw new AIError(problemFromStatus(500, "Chat stream sent a malformed event."));
    }
    if (parsed.type === "error") {
      throw new AIError(parsed.problem ?? problemFromStatus(500, "The chat stream reported an error."));
    }
    yield parsed as StreamPart;
  }
}
