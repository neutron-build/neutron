export class RequestShutdownTimeoutError extends Error {
  constructor() { super("SQL request drain deadline exceeded; database remains open until active work settles"); this.name = "RequestShutdownTimeoutError"; }
}

/** Owns admission and draining around an application-owned database lifetime.
 * Request callbacks receive a fresh abort signal and must await all their SQL
 * work, forwarding that signal to query execution options. A transaction scope
 * belongs to its callback and must never be retained between requests. */
export class SqlRequestLifecycle<DB extends { close(): Promise<void> }> {
  readonly #database: DB;
  readonly #active = new Set<AbortController>();
  readonly #waiters = new Set<() => void>();
  #accepting = true;
  #closed = false;
  #shutdown: Promise<void> | undefined;
  constructor(database: DB) { this.#database = database; }
  get activeRequests(): number { return this.#active.size; }
  async run<T>(work: (database: DB, signal: AbortSignal) => Promise<T>): Promise<T> {
    if (!this.#accepting) throw new Error("SQL request admission closed");
    const controller = new AbortController();
    this.#active.add(controller);
    try { return await work(this.#database, controller.signal); }
    finally {
      this.#active.delete(controller);
      if (!this.#active.size) for (const notify of this.#waiters) notify();
    }
  }
  shutdown(options: { graceMs: number; cancelMs: number }): Promise<void> {
    for (const value of [options.graceMs, options.cancelMs]) if (!Number.isInteger(value) || value < 0 || value > 2147483647) return Promise.reject(new Error("shutdown deadlines require bounded non-negative milliseconds"));
    if (this.#closed) return Promise.resolve();
    if (this.#shutdown) return this.#shutdown;
    this.#accepting = false;
    this.#shutdown = (async () => {
      if (!(await this.#drain(options.graceMs))) {
        for (const controller of this.#active) controller.abort();
        if (!(await this.#drain(options.cancelMs))) throw new RequestShutdownTimeoutError();
      }
      await this.#database.close();
      this.#closed = true;
    })().finally(() => { this.#shutdown = undefined; });
    return this.#shutdown;
  }
  #drain(milliseconds: number): Promise<boolean> {
    if (!this.#active.size) return Promise.resolve(true);
    return new Promise(resolve => {
      const finish = (drained: boolean) => { clearTimeout(timer); this.#waiters.delete(notify); resolve(drained); };
      const notify = () => finish(true);
      const timer = setTimeout(() => finish(false), milliseconds);
      this.#waiters.add(notify);
    });
  }
}
