import type { ModelAdapter } from "./adapter.js";
import type { CallSettings } from "./call-options.js";
import { buildAdapterOptions, resolveInitialMessages } from "./call-options.js";
import { AIError, problemFromStatus } from "./errors.js";
import { deferred } from "./internal/deferred.js";
import { parsePartialJson } from "./internal/partial-json.js";
import { backoff, isRetryableError, retryOptionsFrom } from "./internal/retry.js";
import type { FlexibleSchema, InferSchema, Schema } from "./schema.js";
import { resolveSchema } from "./schema.js";
import type { Message, Usage } from "./types.js";

export type DeepPartial<T> = T extends Array<infer U>
  ? Array<DeepPartial<U>>
  : T extends object
    ? { [K in keyof T]?: DeepPartial<T[K]> }
    : T;

export interface StreamObjectOptions<S extends FlexibleSchema<unknown>> extends CallSettings {
  model: ModelAdapter;
  schema: S;
  system?: string;
  /** Shorthand for a single user message. Exactly one of prompt/messages. */
  prompt?: string;
  messages?: Message[];
  schemaName?: string;
  schemaDescription?: string;
}

export interface StreamObjectResult<T> {
  /** Growing snapshots of the object as its JSON streams in. Single consumer. */
  readonly partialObjectStream: AsyncIterable<DeepPartial<T>>;
  /** The final object, validated against the schema. Awaiting it drains the stream. */
  readonly object: Promise<T>;
  readonly usage: Promise<Usage>;
}

/** streamText's sibling for structured output — same forced-tool mechanism as generateObject. */
export function streamObject<S extends FlexibleSchema<unknown>>(
  options: StreamObjectOptions<S>,
): StreamObjectResult<InferSchema<S>> {
  const picked: { system?: string; prompt?: string; messages?: Message[] } = {};
  if (options.system !== undefined) picked.system = options.system;
  if (options.prompt !== undefined) picked.prompt = options.prompt;
  if (options.messages !== undefined) picked.messages = options.messages;
  const messages = resolveInitialMessages({ model: options.model, ...picked });
  return new StreamObjectResultImpl(options, resolveSchema(options.schema), messages);
}

/** Rejection reason for result promises when the consumer abandons the stream. */
const ABANDONED_STREAM = new AIError(
  problemFromStatus(400, "The stream was abandoned before completion; result promises cannot be fulfilled."),
);

class StreamObjectResultImpl<T> implements StreamObjectResult<T> {
  #options: StreamObjectOptions<FlexibleSchema<unknown>>;
  #schema: Schema<unknown>;
  #messages: Message[];
  #consumed = false;
  #objectDeferred = deferred<T>();
  #usageDeferred = deferred<Usage>();

  constructor(
    options: StreamObjectOptions<FlexibleSchema<unknown>>,
    schema: Schema<unknown>,
    messages: Message[],
  ) {
    this.#options = options;
    this.#schema = schema;
    this.#messages = messages;
  }

  #start(): AsyncGenerator<DeepPartial<T>, void, undefined> {
    if (this.#consumed) {
      throw new AIError(
        problemFromStatus(400, "This stream was already consumed; call streamObject() again for a new stream."),
      );
    }
    this.#consumed = true;
    return this.#iterate();
  }

  async *#iterate(): AsyncGenerator<DeepPartial<T>, void, undefined> {
    try {
      const options = this.#options;
      const name = options.schemaName ?? "json";
      const definitions = [
        {
          name,
          description: options.schemaDescription ?? "Return the result in the required schema.",
          inputSchema: this.#schema.jsonSchema,
        },
      ];

      let inputJson = "";
      let finalInput: unknown;
      let usage: Usage = { inputTokens: 0, outputTokens: 0, totalTokens: 0 };
      let lastEmitted: string | undefined;

      const retry = retryOptionsFrom(options);
      for (let attempt = 0; ; attempt++) {
        let produced = false;
        try {
          const parts = options.model.doStream(
            buildAdapterOptions({ ...options, toolChoice: { toolName: name } }, this.#messages, definitions),
          );
          for await (const part of parts) {
            produced = true;
            if (part.type === "tool-input-delta") {
              inputJson += part.delta;
              const partial = parsePartialJson(inputJson);
              if (partial !== undefined) {
                const key = JSON.stringify(partial);
                if (key !== lastEmitted) {
                  lastEmitted = key;
                  yield partial as DeepPartial<T>;
                }
              }
            } else if (part.type === "tool-call" && part.toolName === name) {
              finalInput = part.input;
            } else if (part.type === "finish") {
              usage = part.usage;
            }
          }
          break;
        } catch (error) {
          if (
            produced ||
            attempt >= retry.maxRetries ||
            !isRetryableError(error) ||
            options.abortSignal?.aborted === true
          ) {
            throw error;
          }
          await backoff(attempt, retry);
        }
      }

      if (finalInput === undefined) {
        throw new AIError(problemFromStatus(500, "The model did not produce the requested structured output."), {
          provider: options.model.provider,
        });
      }
      const validation = await this.#schema.validate(finalInput);
      if (!validation.success) {
        const details = validation.issues
          .map((issue) => (issue.path !== undefined ? `${issue.path}: ${issue.message}` : issue.message))
          .join("; ");
        throw new AIError(problemFromStatus(422, `Structured output failed validation: ${details}`), {
          provider: options.model.provider,
        });
      }

      this.#objectDeferred.resolve(validation.value as T);
      this.#usageDeferred.resolve(usage);
    } catch (error) {
      this.#objectDeferred.reject(error);
      this.#usageDeferred.reject(error);
      throw error;
    } finally {
      // A consumer that breaks out of the stream abandons the generator at a
      // yield: neither the resolve block nor the catch above runs, and the
      // result deferreds stay pending forever — `await result.object` hung
      // with no error and no timeout. Settle them here; on the normal and
      // error paths they are already settled and rejecting again is a no-op.
      this.#objectDeferred.reject(ABANDONED_STREAM);
      this.#usageDeferred.reject(ABANDONED_STREAM);
    }
  }

  #drain(): void {
    if (this.#consumed) return;
    const parts = this.#start();
    void (async () => {
      for await (const _part of parts) {
        // results surface via the promises
      }
    })().catch(() => {});
  }

  get partialObjectStream(): AsyncIterable<DeepPartial<T>> {
    return this.#start();
  }

  get object(): Promise<T> {
    this.#drain();
    return this.#objectDeferred.promise;
  }

  get usage(): Promise<Usage> {
    this.#drain();
    return this.#usageDeferred.promise;
  }
}
