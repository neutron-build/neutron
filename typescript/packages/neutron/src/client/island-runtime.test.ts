// @vitest-environment happy-dom
import { describe, it, expect, beforeEach, vi } from "vitest";
import { h } from "preact";
import { useState } from "preact/hooks";
import { initIslands, resolveIslandExport } from "./island-runtime.js";

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

describe("resolveIslandExport", () => {
  it("prefers default when it is a function", () => {
    expect(resolveIslandExport({ default: Counter, Other: () => null }, null)).toBe(Counter);
  });

  it("picks the sole named function export (ThemeToggle / CopyCommand shape)", () => {
    const ThemeToggle = () => h("button", null, "t");
    expect(resolveIslandExport({ ThemeToggle }, "theme-toggle")).toBe(ThemeToggle);
  });

  it("matches data-component id to PascalCase export name", () => {
    const ThemeToggle = () => h("button", null, "t");
    const Nav = () => h("nav", null);
    expect(resolveIslandExport({ ThemeToggle, Nav }, "theme-toggle")).toBe(ThemeToggle);
  });

  it("matches fuzzy id (copy-cmd → CopyCommand)", () => {
    const CopyCommand = () => h("code", null, "x");
    const Other = () => h("span", null);
    expect(resolveIslandExport({ CopyCommand, Other }, "copy-cmd")).toBe(CopyCommand);
  });

  it("returns null for empty / non-function modules", () => {
    expect(resolveIslandExport({}, null)).toBeNull();
    expect(resolveIslandExport({ value: 1 }, null)).toBeNull();
    expect(resolveIslandExport(null, null)).toBeNull();
  });
});

describe("initIslands — standalone manifest hydration", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    delete (window as any).__ISLAND_COMPONENTS__;
  });

  it("dynamic-imports the island via manifest[data-src] when the SPA registry misses", async () => {
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

    const button = island.querySelector("button")!;
    expect(button.textContent).toBe("count:5");
    button.click();
    await flush();
    expect(button.textContent).toBe("count:6");
  });

  it("hydrates named-only exports (no default) from the island chunk", async () => {
    // Mirrors Rollup output for `export function ThemeToggle` — no default.
    function ThemeToggle() {
      const [n, setN] = useState(0);
      return h("button", { class: "theme-toggle", onClick: () => setN(n + 1) }, String(n));
    }

    document.body.innerHTML =
      '<neutron-island data-component="theme-toggle" data-client="load" ' +
      'data-props="{}" data-src="/src/components/ThemeToggle.tsx">' +
      '<button class="theme-toggle">0</button></neutron-island>';

    const importer = vi.fn(async () => ({ ThemeToggle }));
    initIslands({ "/src/components/ThemeToggle.tsx": importer });

    await flush();

    expect(importer).toHaveBeenCalledTimes(1);
    const island = document.querySelector("neutron-island") as HTMLElement & {
      __neutronHydrated?: boolean;
    };
    expect(island.__neutronHydrated).toBe(true);
    const button = island.querySelector("button")!;
    button.click();
    await flush();
    expect(button.textContent).toBe("1");
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

    expect(importer).not.toHaveBeenCalled();
    const island = document.querySelector("neutron-island") as HTMLElement & {
      __neutronHydrated?: boolean;
    };
    expect(island.__neutronHydrated).toBe(true);
  });
});
