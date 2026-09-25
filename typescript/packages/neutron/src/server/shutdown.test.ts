import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { onShutdownSignal } from "./shutdown.js";

// FRAMEWORK_CONTRACT.md §8, driven by emitting the signal event on `process`
// (no real signal is sent). Listeners the test runner may own are parked for
// the duration so only the handler under test runs.

const SIGNALS = ["SIGTERM", "SIGINT"] as const;
let parked: Map<string, ((...args: unknown[]) => void)[]>;
let dispose: (() => void) | undefined;

beforeEach(() => {
  parked = new Map();
  for (const signal of SIGNALS) {
    parked.set(signal, process.listeners(signal) as (() => void)[]);
    process.removeAllListeners(signal);
  }
  vi.spyOn(console, "log").mockImplementation(() => {});
  vi.spyOn(console, "error").mockImplementation(() => {});
});

afterEach(() => {
  dispose?.();
  dispose = undefined;
  for (const signal of SIGNALS) {
    for (const listener of parked.get(signal) ?? []) process.on(signal, listener);
  }
  vi.restoreAllMocks();
  vi.useRealTimers();
});

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

describe("onShutdownSignal", () => {
  for (const signal of SIGNALS) {
    it(`${signal}: waits for close() to finish, then exits 0`, async () => {
      const gate = deferred();
      const close = vi.fn(() => gate.promise);
      const exit = vi.fn();
      dispose = onShutdownSignal(close, { exit });

      process.emit(signal, signal);
      expect(close).toHaveBeenCalledTimes(1);
      await Promise.resolve();
      expect(exit).not.toHaveBeenCalled();

      gate.resolve();
      await vi.waitFor(() => expect(exit).toHaveBeenCalledWith(0));
    });
  }

  it("a second signal while draining exits immediately with 1", () => {
    const close = vi.fn(() => new Promise<void>(() => {}));
    const exit = vi.fn();
    dispose = onShutdownSignal(close, { exit });

    process.emit("SIGTERM", "SIGTERM");
    expect(exit).not.toHaveBeenCalled();
    process.emit("SIGINT", "SIGINT");
    expect(exit).toHaveBeenCalledWith(1);
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("forces exit 1 when the drain exceeds the timeout (default 30s)", () => {
    vi.useFakeTimers();
    const exit = vi.fn();
    dispose = onShutdownSignal(() => new Promise<void>(() => {}), { exit });

    process.emit("SIGTERM", "SIGTERM");
    vi.advanceTimersByTime(29_999);
    expect(exit).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(exit).toHaveBeenCalledWith(1);
  });

  it("exits 1 when close() rejects", async () => {
    const exit = vi.fn();
    dispose = onShutdownSignal(() => Promise.reject(new Error("boom")), { exit });
    process.emit("SIGTERM", "SIGTERM");
    await vi.waitFor(() => expect(exit).toHaveBeenCalledWith(1));
  });

  it("the returned disposer removes the handlers", () => {
    const close = vi.fn(() => Promise.resolve());
    const remove = onShutdownSignal(close, { exit: vi.fn() });
    expect(process.listenerCount("SIGTERM")).toBe(1);
    remove();
    expect(process.listenerCount("SIGTERM")).toBe(0);
    expect(process.listenerCount("SIGINT")).toBe(0);
  });
});
