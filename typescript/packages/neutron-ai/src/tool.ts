import type { FlexibleSchema, InferSchema } from "./schema.js";

export interface ToolExecutionContext {
  toolCallId: string;
  abortSignal?: AbortSignal;
}

/**
 * A tool the model can call. `execute` is optional: without it the tool is
 * client-side — the loop stops and returns the calls to the caller instead
 * of executing them. `needsApproval` gates execution on a human decision,
 * per-tool (boolean) or per-call (predicate on the input); see
 * ToolApprovalRequest for the suspension/resume protocol.
 */
export interface Tool<TInput = any> {
  name: string;
  description?: string;
  inputSchema: FlexibleSchema<TInput>;
  execute?: (input: TInput, context: ToolExecutionContext) => unknown | Promise<unknown>;
  needsApproval?: boolean | ((input: TInput) => boolean | Promise<boolean>);
}

/** Identity helper that infers the execute/needsApproval input type from the schema. */
export function tool<S extends FlexibleSchema<any>>(config: {
  name: string;
  description?: string;
  inputSchema: S;
  execute?: (input: InferSchema<S>, context: ToolExecutionContext) => unknown | Promise<unknown>;
  needsApproval?: boolean | ((input: InferSchema<S>) => boolean | Promise<boolean>);
}): Tool<InferSchema<S>> {
  return config as Tool<InferSchema<S>>;
}
