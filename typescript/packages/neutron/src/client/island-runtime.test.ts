// @vitest-environment happy-dom
import { describe, it, expect, beforeEach, vi } from "vitest";
import { h } from "preact";
import { useState } from "preact/hooks";
import { initIslands } from "./island-runtime.js";

/** Flush microtasks + a macrotask so dynamic-import + hydrate settle. */
function flush(): Promise<void> {
  return new Promise((r) => setTimeout(r, 0));
}

// A hook-using component, served via the manifest (no SPA registry entry), to
// mirror a real island whose code lives in its own code-split chunk.
function Counter({ start = 0 }: { start?: number }) {
  const [count, setCount] = useState(start);
  return h(
    "button",
    { onClick: () => setCount((c: number) => c + 1) },
    `count:${count}`
  );
}

describe("initIslands — standalone manifest hydration", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    delete (window as any).__ISLAND_COMPONENTS__;
  });

  it("dynamic-imports the island via manifest[data-src] when the SPA registry misses", async () => {
    // SSR-rendered marker: data-src present, NO SPA registry entry.
    document.body.innerHTML =
      '<neutron-island data-component="Counter" data-client="load" ' +
      'data-props=\'{"start":5}\' data-src="/src/components/Counter.tsx">' +
      "<button>count:5</button></neutron-island>";

    const importer = vi.fn(async () => ({ default: Counter }));
    initIslands({ "/src/components/Counter.tsx": importer });

    await flush();

    expect(importer).toHaveBeenCalledTimes(1);
    const island = document.querySelector("neutron-island") as HTMLElement & {
      __neutronHydrated?: boolean;
    };
    expect(island.__neutronHydrated).toBe(true);

    // Hydrated and interactive: clicking the button mutates state.
    const button = island.querySelector("button")!;
    expect(button.textContent).toBe("count:5");
    button.click();
    await flush();
    expect(button.textContent).toBe("count:6");
  });

  it("prefers the SPA registry over the manifest (back-compat)", async () => {
    document.body.innerHTML =
      '<neutron-island data-component="Counter" data-client="load" ' +
      'data-props="{}" data-src="/src/components/Counter.tsx">' +
      "<button>count:0</button></neutron-island>";

    (window as any).__ISLAND_COMPONENTS__ = { Counter };
    const importer = vi.fn(async () => ({ default: Counter }));
    initIslands({ "/src/components/Counter.tsx": importer });

    await flush();

    // Registry hit ⇒ manifest importer is never called.
    expect(importer).not.toHaveBeenCalled();
    const island = document.querySelector("neutron-island") as HTMLElement & {
      __neutronHydrated?: boolean;
    };
    expect(island.__neutronHydrated).toBe(true);
  });
});
