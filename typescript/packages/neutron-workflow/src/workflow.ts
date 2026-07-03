import type { WorkflowContext } from "./context.js";
import { WorkflowError, problemFromStatus } from "./errors.js";

export interface WorkflowOptions {
  /** Total run budget measured from run-started; an exceeded run fails on its next execution pass. */
  timeout?: string | number;
}

export interface WorkflowDefinition<In = unknown, Out = unknown> {
  name: string;
  run: (ctx: WorkflowContext, input: In) => Promise<Out>;
  options?: WorkflowOptions;
}

/**
 * Define a durable workflow. The function re-executes from the top on
 * every resume; completed steps replay from the event log instead of
 * re-running. Code between context calls must be deterministic — all I/O
 * and randomness go inside ctx.step() / ctx.now() / ctx.random().
 */
export function workflow<In = unknown, Out = unknown>(
  name: string,
  run: (ctx: WorkflowContext, input: In) => Promise<Out>,
  options: WorkflowOptions = {},
): WorkflowDefinition<In, Out> {
  if (name === "") {
    throw new WorkflowError(problemFromStatus(400, "Workflow name must not be empty."));
  }
  return { name, run, options };
}
