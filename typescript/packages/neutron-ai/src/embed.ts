import type { EmbeddingAdapter } from "./adapter.js";
import { AIError, problemFromStatus } from "./errors.js";
import { retryOptionsFrom, withRetries } from "./internal/retry.js";

export interface EmbedOptions {
  model: EmbeddingAdapter;
  value: string;
  headers?: Record<string, string>;
  abortSignal?: AbortSignal;
  maxRetries?: number;
  retryDelayMs?: number;
}

export async function embed(options: EmbedOptions): Promise<{ embedding: number[]; usage: { inputTokens: number } }> {
  const { embeddings, usage } = await withRetries(
    () => options.model.doEmbed([options.value], requestOptions(options)),
    retryOptionsFrom(options),
  );
  const embedding = embeddings[0];
  if (embedding === undefined) {
    throw new AIError(problemFromStatus(500, "The provider returned no embedding."), {
      provider: options.model.provider,
    });
  }
  return { embedding, usage };
}

export interface EmbedManyOptions {
  model: EmbeddingAdapter;
  values: string[];
  headers?: Record<string, string>;
  abortSignal?: AbortSignal;
  maxRetries?: number;
  retryDelayMs?: number;
}

export async function embedMany(
  options: EmbedManyOptions,
): Promise<{ embeddings: number[][]; usage: { inputTokens: number } }> {
  if (options.values.length === 0) {
    return { embeddings: [], usage: { inputTokens: 0 } };
  }
  return withRetries(() => options.model.doEmbed(options.values, requestOptions(options)), retryOptionsFrom(options));
}

/**
 * Structurally matches @neutron-build/nucleus's VectorModel.insert, so a
 * Nucleus client plugs in directly — without this package depending on it.
 */
export interface VectorSink {
  insert(collection: string, id: string, vector: number[], metadata?: Record<string, unknown>): Promise<void>;
}

export interface EmbedAndStoreOptions extends EmbedManyOptions {
  vector: VectorSink;
  collection: string;
  /** One id per value; defaults to random UUIDs. */
  ids?: string[];
  metadata?: (value: string, index: number) => Record<string, unknown>;
}

/** Embed values and write them straight to a Nucleus Vector collection. */
export async function embedAndStore(
  options: EmbedAndStoreOptions,
): Promise<{ ids: string[]; embeddings: number[][]; usage: { inputTokens: number } }> {
  if (options.ids !== undefined && options.ids.length !== options.values.length) {
    throw new AIError(problemFromStatus(400, "`ids` must match `values` in length."));
  }
  const { embeddings, usage } = await embedMany(options);
  const ids = options.ids ?? options.values.map(() => crypto.randomUUID());
  for (let i = 0; i < embeddings.length; i++) {
    await options.vector.insert(
      options.collection,
      ids[i]!,
      embeddings[i]!,
      options.metadata?.(options.values[i]!, i) ?? {},
    );
  }
  return { ids, embeddings, usage };
}

function requestOptions(options: { headers?: Record<string, string>; abortSignal?: AbortSignal }): {
  headers?: Record<string, string>;
  abortSignal?: AbortSignal;
} {
  const request: { headers?: Record<string, string>; abortSignal?: AbortSignal } = {};
  if (options.headers !== undefined) request.headers = options.headers;
  if (options.abortSignal !== undefined) request.abortSignal = options.abortSignal;
  return request;
}
