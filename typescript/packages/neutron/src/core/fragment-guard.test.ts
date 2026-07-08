import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { assertRenderedFragment } from "./fragment-guard.js";
import { renderStatic } from "./render-static.js";

// --- Unit: the shared guard itself -----------------------------------------

describe("assertRenderedFragment", () => {
  it("passes a fragment render unchanged", () => {
    expect(() => assertRenderedFragment("<main>hello</main>")).not.toThrow();
    expect(() =>
      assertRenderedFragment("  \n<section class=\"shell\"><p>hi</p></section>")
    ).not.toThrow();
  });

  it("rejects a full document with a descriptive, actionable message", () => {
    expect(() =>
      assertRenderedFragment("<!doctype html><html><body>x</body></html>", "routes/_layout.ts")
    ).toThrowError(/rendered a full document/);
    // Names the offending source file and tells the author what to do.
    expect(() =>
      assertRenderedFragment("<html lang=\"en\"><body>x</body></html>", "routes/_layout.ts")
    ).toThrowError(/in routes\/_layout\.ts/);
    expect(() =>
      assertRenderedFragment("<html lang=\"en\"><body>x</body></html>", "routes/_layout.ts")
    ).toThrowError(/render a fragment instead/);
  });

  it("detects <html>, <body>, and <!doctype> starts (case-insensitive, leading whitespace)", () => {
    for (const doc of [
      "<!DOCTYPE html><html></html>",
      "<HTML></HTML>",
      "\n\t<body>content</body>",
      "<html/>",
    ]) {
      expect(() => assertRenderedFragment(doc)).toThrow();
    }
  });
});

// --- Integration: the SSG (render-static) compose path ----------------------

describe("renderStatic full-document guard", () => {
  const roots: string[] = [];

  function makeFixture(): string {
    const root = fs.mkdtempSync(path.join(__dirname, ".tmp-neutron-ssg-guard-"));
    roots.push(root);
    fs.mkdirSync(path.join(root, "routes"), { recursive: true });
    fs.mkdirSync(path.join(root, "out"), { recursive: true });
    return root;
  }

  afterEach(() => {
    vi.restoreAllMocks();
  });

  afterAll(async () => {
    for (const root of roots) {
      await fsp.rm(root, { recursive: true, force: true });
    }
  });

  it("composes a fragment-rendering static layout into #app and writes the page", async () => {
    const root = makeFixture();
    fs.writeFileSync(
      path.join(root, "routes", "_layout.js"),
      `import { h } from "preact";
export default function Layout(props) {
  return h("section", { class: "shell" }, props.children);
}
`
    );
    fs.writeFileSync(
      path.join(root, "routes", "index.js"),
      `import { h } from "preact";
export const config = { mode: "static" };
export default function Home() {
  return h("main", null, "home");
}
`
    );

    await renderStatic({
      routesDir: path.join(root, "routes"),
      outputDir: path.join(root, "out"),
    });

    const outFile = path.join(root, "out", "index.html");
    expect(fs.existsSync(outFile)).toBe(true);
    const html = fs.readFileSync(outFile, "utf-8");
    expect((html.match(/<html/gi) || []).length).toBe(1);
    expect(html).toContain('<div id="app">');
    expect(html).toContain('<section class="shell">');
    expect(html).toContain("home");
  });

  it("rejects a static layout that renders a full document with the descriptive message and writes nothing", async () => {
    const root = makeFixture();
    fs.writeFileSync(
      path.join(root, "routes", "_layout.js"),
      `import { h } from "preact";
export default function Layout(props) {
  return h(
    "html",
    { lang: "en" },
    h("head", null, h("title", null, "App")),
    h("body", null, props.children)
  );
}
`
    );
    fs.writeFileSync(
      path.join(root, "routes", "index.js"),
      `import { h } from "preact";
export const config = { mode: "static" };
export default function Home() {
  return h("main", null, "home");
}
`
    );

    // renderStatic keeps building on a per-route failure — it logs the error and
    // skips the offending page rather than shipping a nested document.
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    await renderStatic({
      routesDir: path.join(root, "routes"),
      outputDir: path.join(root, "out"),
    });

    // The nested-document page must not have been written.
    expect(fs.existsSync(path.join(root, "out", "index.html"))).toBe(false);

    // The guard's descriptive error was reported, naming the offending layout.
    const logged = errorSpy.mock.calls
      .map((call) => call.map((arg) => (arg instanceof Error ? arg.message : String(arg))).join(" "))
      .join("\n");
    expect(logged).toContain("rendered a full document");
    expect(logged).toContain("render a fragment instead");
    expect(logged).toContain("_layout.js");
  });
});
