import { AIError } from "../errors.js";

export interface RetryOptions {
  /** Additional attempts after the first (default 2 → 3 attempts total). */
  maxRetries: number;
  /** First backoff delay; doubles per attempt, with jitter. */
  initialDelayMs: number;
  abortSignal?: AbortSignal;
}

export function retryOptionsFrom(settings: {
  maxRetries?: number;
  retryDelayMs?: number;
  abortSignal?: AbortSignal;
}): RetryOptions {
  const options: RetryOptions = {
    maxRetries: settings.maxRetries ?? 2,
    initialDelayMs: settings.retryDelayMs ?? 1000,
  };
  if (settings.abortSignal !== undefined) options.abortSignal = settings.abortSignal;
  return options;
}

/** Rate limits and server-side failures retry; caller errors never do. */
export function isRetryableError(error: unknown): boolean {
  return error instanceof AIError && (error.problem.status === 429 || error.problem.status >= 500);
}

export async function withRetries<T>(run: () => Promise<T>, options: RetryOptions): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await run();
    } catch (error) {
      if (attempt >= options.maxRetries || !isRetryableError(error) || options.abortSignal?.aborted === true) {
        throw error;
      }
      await backoff(attempt, options);
    }
  }
}

export async function backoff(attempt: number, options: RetryOptions): Promise<void> {
  const delay = options.initialDelayMs * 2 ** attempt * (0.8 + Math.random() * 0.4);
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, delay);
    options.abortSignal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}
