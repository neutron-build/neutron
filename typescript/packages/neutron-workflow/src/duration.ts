import { WorkflowError, problemFromStatus } from "./errors.js";

const UNIT_MS: Record<string, number> = {
  ms: 1,
  s: 1000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
  w: 604_800_000,
};

/** Parse "500ms" | "30s" | "15m" | "2h" | "7d" | "1w" (or a raw ms number) to milliseconds. */
export function parseDuration(duration: string | number): number {
  if (typeof duration === "number") {
    if (!Number.isFinite(duration) || duration < 0) {
      throw new WorkflowError(problemFromStatus(400, `Invalid duration: ${duration}.`));
    }
    return Math.floor(duration);
  }
  const match = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)$/.exec(duration.trim());
  if (match === null) {
    throw new WorkflowError(
      problemFromStatus(400, `Invalid duration "${duration}". Use e.g. "500ms", "30s", "15m", "2h", "7d".`),
    );
  }
  return Math.floor(Number(match[1]) * UNIT_MS[match[2]!]!);
}
