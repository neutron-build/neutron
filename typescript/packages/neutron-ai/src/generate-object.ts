import type { ModelAdapter } from "./adapter.js";
import type { CallSettings } from "./call-options.js";
import { buildAdapterOptions, resolveInitialMessages } from "./call-options.js";
import { AIError, problemFromStatus } from "./errors.js";
import { retryOptionsFrom, withRetries } from "./internal/retry.js";
import type { FlexibleSchema, InferSchema } from "./schema.js";
import { resolveSchema } from "./schema.js";
import type { FinishReason, Message, ToolCallPart, Usage } from "./types.js";

export interface GenerateObjectOptions<S extends FlexibleSchema<unknown>> extends CallSettings {
  model: ModelAdapter;
  schema: S;
  system?: string;
  /** Shorthand for a single user message. Exactly one of prompt/messages. */
  prompt?: string;
  messages?: Message[];
  /** Name/description of the schema as some providers surface it to the model. */
  schemaName?: string;
  schemaDescription?: string;
}

export interface GenerateObjectResult<T> {
  object: T;
  finishReason: FinishReason;
  usage: Usage;
  raw: unknown;
}

/**
 * Structured output via a forced tool call — the one mechanism every
 * provider supports identically, so results are portable across adapters
 * (including OpenAI-compatible servers without json_schema support).
 */
export async function generateObject<S extends FlexibleSchema<unknown>>(
  options: GenerateObjectOptions<S>,
): Promise<GenerateObjectResult<InferSchema<S>>> {
  const schema = resolveSchema(options.schema);
  const name = options.schemaName ?? "json";
  const messages = resolveInitialMessages({ model: options.model, ...pickPrompt(options) });
  const definitions = [
    {
      name,
      description: options.schemaDescription ?? "Return the result in the required schema.",
      inputSchema: schema.jsonSchema,
    },
  ];

  const result = await withRetries(
    () =>
      options.model.doGenerate(
        buildAdapterOptions({ ...options, toolChoice: { toolName: name } }, messages, definitions),
      ),
    retryOptionsFrom(options),
  );

  const call = result.content.find(
    (part): part is ToolCallPart => part.type === "tool-call" && part.toolName === name,
  );
  if (call === undefined) {
    throw new AIError(problemFromStatus(500, "The model did not produce the requested structured output."), {
      provider: options.model.provider,
    });
  }

  const validation = await schema.validate(call.input);
  if (!validation.success) {
    const details = validation.issues
      .map((issue) => (issue.path !== undefined ? `${issue.path}: ${issue.message}` : issue.message))
      .join("; ");
    throw new AIError(problemFromStatus(422, `Structured output failed validation: ${details}`), {
      provider: options.model.provider,
    });
  }

  return {
    object: validation.value as InferSchema<S>,
    // The forced tool call IS the answer; surface it as a normal stop.
    finishReason: result.finishReason === "tool-calls" ? "stop" : result.finishReason,
    usage: result.usage,
    raw: result.raw,
  };
}

function pickPrompt(options: GenerateObjectOptions<FlexibleSchema<unknown>>): {
  system?: string;
  prompt?: string;
  messages?: Message[];
} {
  const picked: { system?: string; prompt?: string; messages?: Message[] } = {};
  if (options.system !== undefined) picked.system = options.system;
  if (options.prompt !== undefined) picked.prompt = options.prompt;
  if (options.messages !== undefined) picked.messages = options.messages;
  return picked;
}
