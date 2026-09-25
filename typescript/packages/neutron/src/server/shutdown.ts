/** FRAMEWORK_CONTRACT.md §8 default drain bound. */
export const DEFAULT_SHUTDOWN_TIMEOUT_MS = 30_000;

export interface ShutdownSignalOptions {
  /** Upper bound on `close()` before the process is forced out with code 1. */
  timeoutMs?: number;
  signals?: readonly NodeJS.Signals[];
  /** Injected for tests; defaults to `process.exit`. */
  exit?: (code: number) => void;
}

/**
 * Graceful shutdown on SIGTERM/SIGINT (FRAMEWORK_CONTRACT.md §8). The first
 * signal runs `close()` — which must stop accepting connections and drain
 * in-flight requests — bounded by `timeoutMs`, then exits 0 (1 on error or
 * timeout). A second signal while draining exits immediately with code 1.
 * Returns a function that removes the handlers.
 */
export function onShutdownSignal(
  close: () => Promise<void>,
  options: ShutdownSignalOptions = {},
): () => void {
  const timeoutMs = options.timeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;
  const signals = options.signals ?? (["SIGTERM", "SIGINT"] as const);
  const exit = options.exit ?? ((code: number) => process.exit(code));
  let shuttingDown = false;

  const handler = (signal: NodeJS.Signals) => {
    if (shuttingDown) {
      console.error(`\nReceived ${signal} again; exiting without waiting for the drain.`);
      exit(1);
      return;
    }
    shuttingDown = true;

    console.log(
      `\nReceived ${signal}, draining in-flight requests (up to ${timeoutMs / 1000}s)...`,
    );
    const forceExit = setTimeout(() => {
      console.error("Drain timed out; forcing exit.");
      exit(1);
    }, timeoutMs);

    void close().then(
      () => {
        clearTimeout(forceExit);
        console.log("Drained cleanly.");
        exit(0);
      },
      (err) => {
        clearTimeout(forceExit);
        console.error("Shutdown error:", err);
        exit(1);
      },
    );
  };

  for (const signal of signals) process.on(signal, handler);
  return () => {
    for (const signal of signals) process.off(signal, handler);
  };
}
