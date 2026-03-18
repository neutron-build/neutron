import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { Hono } from "hono";
import { compress } from "hono/compress";

describe("Parallel Loader Execution", () => {
  it("should run loaders in parallel, not sequentially", async () => {
    const loaderDelayMs = 50;
    const delays: number[] = [];
    const start = Date.now();

    // Simulate 3 loaders with different delays
    const loader1 = async () => {
      await new Promise(r => setTimeout(r, loaderDelayMs));
      delays.push(Date.now() - start);
      return { loader: 1 };
    };

    const loader2 = async () => {
      await new Promise(r => setTimeout(r, loaderDelayMs));
      delays.push(Date.now() - start);
      return { loader: 2 };
    };

    const loader3 = async () => {
      await new Promise(r => setTimeout(r, loaderDelayMs));
      delays.push(Date.now() - start);
      return { loader: 3 };
    };

    // Run in PARALLEL (Promise.all)
    const results = await Promise.all([
      loader1(),
      loader2(),
      loader3(),
    ]);

    const totalTime = Date.now() - start;

    // If parallel, total time should be ~50ms (not 150ms)
    expect(totalTime).toBeGreaterThanOrEqual(loaderDelayMs - 10);
    expect(totalTime).toBeLessThan(loaderDelayMs * 3);
    
    // All loaders should complete in roughly the same timing window.
    const minDelay = Math.min(...delays);
    const maxDelay = Math.max(...delays);
    expect(minDelay).toBeGreaterThanOrEqual(loaderDelayMs - 10);
    expect(maxDelay).toBeLessThan(loaderDelayMs * 3);
    expect(maxDelay - minDelay).toBeLessThan(loaderDelayMs);
    
    // Results should be correct
    expect(results).toEqual([
      { loader: 1 },
      { loader: 2 },
      { loader: 3 },
    ]);
  });

  it("should handle loader errors without blocking other loaders", async () => {
    const successfulLoads: string[] = [];

    const loaders = [
      async () => {
        await new Promise(r => setTimeout(r, 10));
        successfulLoads.push("loader1");
        return { id: 1 };
      },
      async () => {
        throw new Error("Loader 2 failed");
      },
      async () => {
        await new Promise(r => setTimeout(r, 10));
        successfulLoads.push("loader3");
        return { id: 3 };
      },
    ];

    // Parallel execution with error handling
    const results = await Promise.all(
      loaders.map(async (loader, i) => {
        try {
          const data = await loader();
          return { index: i, data, error: null };
        } catch (error) {
          return { index: i, data: null, error };
        }
      })
    );

    // All loaders should have been attempted
    expect(results).toHaveLength(3);
    
    // Loader 1 and 3 should succeed
    expect(results[0].data).toEqual({ id: 1 });
    expect(results[2].data).toEqual({ id: 3 });
    
    // Loader 2 should have error
    expect(results[1].error).toBeInstanceOf(Error);
    expect((results[1].error as Error).message).toBe("Loader 2 failed");
    
    // Successful loaders should have run
    expect(successfulLoads).toContain("loader1");
    expect(successfulLoads).toContain("loader3");
  });

  it("should demonstrate sequential vs parallel performance difference", async () => {
    const loaderDelay = 30;
    const numLoaders = 5;

    const createLoader = (id: number) => async () => {
      await new Promise(r => setTimeout(r, loaderDelay));
      return { id };
    };

    // SEQUENTIAL (old way)
    const sequentialStart = Date.now();
    const sequentialResults: any[] = [];
    for (let i = 0; i < numLoaders; i++) {
      sequentialResults.push(await createLoader(i)());
    }
    const sequentialTime = Date.now() - sequentialStart;

    // PARALLEL (new way)
    const parallelStart = Date.now();
    const parallelResults = await Promise.all(
      Array.from({ length: numLoaders }, (_, i) => createLoader(i)())
    );
    const parallelTime = Date.now() - parallelStart;

    // Avoid brittle absolute cutoffs: assert meaningful parallel speedup with jitter tolerance.
    expect(parallelTime).toBeLessThan(sequentialTime);
    expect(sequentialTime).toBeGreaterThanOrEqual(loaderDelay * numLoaders * 0.75);
    expect(sequentialTime - parallelTime).toBeGreaterThanOrEqual(loaderDelay);
    expect(sequentialTime / parallelTime).toBeGreaterThan(1.2);

    // Results should be the same
    expect(parallelResults).toEqual(sequentialResults);

    // Log for visibility
    console.log(`  Sequential: ${sequentialTime}ms`);
    console.log(`  Parallel: ${parallelTime}ms`);
    console.log(`  Speedup: ${(sequentialTime / parallelTime).toFixed(1)}x`);
  });
});
