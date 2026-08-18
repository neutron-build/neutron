import * as fs from "node:fs";
import * as path from "node:path";
import { afterAll, describe, expect, it } from "vitest";

import {
  findStaticInteractivity,
  formatStaticInteractivityWarning,
} from "./static-interactivity-check.js";

const roots: string[] = [];

function fixture(files: Record<string, string>): string {
  const root = fs.mkdtempSync(path.join(__dirname, ".tmp-neutron-interactivity-"));
  roots.push(root);
  for (const [rel, contents] of Object.entries(files)) {
    const full = path.join(root, rel);
    fs.mkdirSync(path.dirname(full), { recursive: true });
    fs.writeFileSync(full, contents);
  }
  return root;
}

const staticRoute = (file: string) => ({
  file,
  isLayout: false,
  config: { mode: "static" as const },
});

afterAll(() => {
  for (const root of roots) fs.rmSync(root, { recursive: true, force: true });
});

describe("findStaticInteractivity", () => {
  // The regression this exists for: before client tiering, the router hydrated
  // every route, so a hook in a static page worked. Now it renders and stays
  // inert — with no error anywhere. The build has to say so.
  it("reports a hook used directly in a static route", () => {
    const root = fixture({
      "index.tsx": `import { useState } from "preact/hooks";
export const config = { mode: "static" };
export default function Page() { const [n] = useState(0); return null; }`,
    });
    const found = findStaticInteractivity([staticRoute(path.join(root, "index.tsx"))]);
    expect(found).toHaveLength(1);
  });

  // The common real shape: the route itself looks inert, and the interactivity
  // is two imports away in a component.
  it("follows the import graph to find it", () => {
    const root = fixture({
      "index.tsx": `import { Nav } from "./components/Nav";
export const config = { mode: "static" };
export default function Page() { return <Nav />; }`,
      "components/Nav.tsx": `import { Dropdown } from "./Dropdown";
export function Nav() { return <Dropdown />; }`,
      "components/Dropdown.tsx": `import { useState } from "preact/hooks";
export function Dropdown() { const [open, setOpen] = useState(false); return null; }`,
    });
    const found = findStaticInteractivity([staticRoute(path.join(root, "index.tsx"))]);
    expect(found).toHaveLength(1);
    expect(found[0].sourceFile).toContain("Dropdown.tsx");
  });

  it("says nothing about a genuinely static page", () => {
    const root = fixture({
      "index.tsx": `export const config = { mode: "static" };
export default function Page() { return <p>hello</p>; }`,
    });
    expect(findStaticInteractivity([staticRoute(path.join(root, "index.tsx"))])).toHaveLength(0);
  });

  // A route that uses islands has already opted into explicit hydration.
  // Warning about it would be crying wolf about correct code, and a heuristic
  // that does that gets ignored — which costs more than the misses.
  it("skips a route that declares an island", () => {
    const root = fixture({
      "index.tsx": `import { Island } from "@neutron-build/core";
import { Counter } from "./Counter";
export const config = { mode: "static" };
export default function Page() { return <Island component={Counter} client="load" />; }`,
      "Counter.tsx": `import { useState } from "preact/hooks";
export function Counter() { const [n] = useState(0); return null; }`,
    });
    expect(findStaticInteractivity([staticRoute(path.join(root, "index.tsx"))])).toHaveLength(0);
  });

  it("respects the hydrate: true escape hatch", () => {
    const root = fixture({
      "index.tsx": `import { useState } from "preact/hooks";
export const config = { mode: "static", hydrate: true };
export default function Page() { const [n] = useState(0); return null; }`,
    });
    const found = findStaticInteractivity([
      {
        file: path.join(root, "index.tsx"),
        isLayout: false,
        config: { mode: "static", hydrate: true },
      },
    ]);
    expect(found).toHaveLength(0);
  });

  it("ignores app routes, which hydrate anyway", () => {
    const root = fixture({
      "index.tsx": `import { useState } from "preact/hooks";
export const config = { mode: "app" };
export default function Page() { const [n] = useState(0); return null; }`,
    });
    const found = findStaticInteractivity([
      { file: path.join(root, "index.tsx"), isLayout: false, config: { mode: "app" } },
    ]);
    expect(found).toHaveLength(0);
  });

  it("does not follow package imports out into node_modules", () => {
    const root = fixture({
      "index.tsx": `import { something } from "some-package";
export const config = { mode: "static" };
export default function Page() { return null; }`,
    });
    expect(findStaticInteractivity([staticRoute(path.join(root, "index.tsx"))])).toHaveLength(0);
  });

  it("survives a cyclic import graph", () => {
    const root = fixture({
      "index.tsx": `import { A } from "./a";
export const config = { mode: "static" };
export default function Page() { return <A />; }`,
      "a.tsx": `import { B } from "./b";
export function A() { return <B />; }`,
      "b.tsx": `import { A } from "./a";
export function B() { return <A />; }`,
    });
    expect(findStaticInteractivity([staticRoute(path.join(root, "index.tsx"))])).toHaveLength(0);
  });
});

describe("formatStaticInteractivityWarning", () => {
  it("is empty when there is nothing to report", () => {
    expect(formatStaticInteractivityWarning([], "/root")).toBe("");
  });

  // A diagnostic that names the problem but not the fix just makes people
  // search the docs. Both escape hatches belong in the message, and the
  // <Island> fix has to name where Island is imported from.
  it("names the file, both fixes, and the Island import source", () => {
    const text = formatStaticInteractivityWarning(
      [{ routeFile: "/root/src/routes/index.tsx", sourceFile: "/root/src/components/Nav.tsx" }],
      "/root"
    );
    expect(text).toContain("src/routes/index.tsx");
    expect(text).toContain("src/components/Nav.tsx");
    expect(text).toContain("<Island>");
    expect(text).toContain("@neutron-build/core/client");
    expect(text).toContain("hydrate: true");
  });
});
