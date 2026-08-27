import { AIError, problemFromStatus } from "../errors.js";

/** Rejection reason for result promises when the consumer abandons a run mid-stream. */
export const ABANDONED_RUN = new AIError(
  problemFromStatus(400, "The run was abandoned before completion; its result promises cannot be fulfilled."),
);

/**
 * Drain a generator nobody will consume, so its deferreds settle via the
 * promises instead. Every stream-shaped result's promise getters use this:
 * awaiting `result.text` (or `.result`, ...) must work even when the caller
 * never touches the stream itself.
 */
export function drainEvents(events: AsyncIterable<unknown>): void {
  void (async () => {
    for await (const _event of events) {
      // results surface via the promises
    }
  })().catch(() => {});
}

/**
 * finally semantics for the stream-generator result-promise pattern, shared
 * by streamText, streamObject and both agent harnesses. A consumer that
 * breaks out of the generator abandons it at a yield: neither the resolve
 * block nor the catch runs, so every result deferred stays pending forever —
 * `await result.text` hung with no error and no timeout — and whatever the
 * run holds (child process, abort listener) leaks. Call the returned
 * settle() from the generator's finally: it rejects each deferred with
 * ABANDONED_RUN (a no-op on the normal and error paths, where they are
 * already settled) and runs `cleanup` to release what the run holds.
 */
export function abandonmentSettler(
  deferreds: ReadonlyArray<{ reject: (reason: unknown) => void }>,
  cleanup?: () => void,
): () => void {
  return () => {
    for (const deferred of deferreds) {
      deferred.reject(ABANDONED_RUN);
    }
    cleanup?.();
  };
}
