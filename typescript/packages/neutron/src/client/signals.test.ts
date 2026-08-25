import { describe, it, expect } from "vitest";
import { signal } from "@preact/signals-core";
import { untrack, createRoot, createEffect } from "./signals.js";

function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

describe("untrack", () => {
  it("reads a signal without subscribing the surrounding effect", async () => {
    const count = signal(0);
    const other = signal("a");
    let runs = 0;

    createEffect(() => {
      runs += 1;
      void other.value;
      untrack(() => count.value);
    });
    await tick();

    count.value = 1;
    await tick();
    // The untracked read must not have registered `count` as a dependency.
    expect(runs).toBe(1);

    other.value = "b";
    await tick();
    expect(runs).toBe(2);
  });
});

describe("createRoot", () => {
  it("disposes all effects created inside the root", async () => {
    const count = signal(0);
    let runs = 0;

    const dispose = createRoot(() => {
      createEffect(() => {
        runs += 1;
        void count.value;
      });
    });
    await tick();
    expect(runs).toBe(1);

    count.value = 1;
    await tick();
    expect(runs).toBe(2);

    dispose();

    count.value = 2;
    await tick();
    expect(runs).toBe(2);
  });

  it("disposes only the root's own effects, not effects from other roots", async () => {
    const count = signal(0);
    let outerRuns = 0;
    let innerRuns = 0;

    const dispose = createRoot(() => {
      createEffect(() => {
        innerRuns += 1;
        void count.value;
      });
    });
    createEffect(() => {
      outerRuns += 1;
      void count.value;
    });
    await tick();

    dispose();
    count.value = 1;
    await tick();

    expect(innerRuns).toBe(1);
    expect(outerRuns).toBe(2);
  });
});
