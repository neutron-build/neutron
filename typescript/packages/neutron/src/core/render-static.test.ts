import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { renderStatic } from "./render-static.js";

// The SSG shell owns the <html>/<body> open tags, which live outside the
// rendered head HTML — these tests pin that htmlAttrs/bodyAttrs from head()
// actually reach the written document instead of being silently dropped.

describe("renderStatic html/body attributes", () => {
  const roots: string[] = [];

  function makeFixture(): string {
    const root = fs.mkdtempSync(path.join(__dirname, ".tmp-neutron-ssg-attrs-"));
    roots.push(root);
    fs.mkdirSync(path.join(root, "routes"), { recursive: true });
    fs.mkdirSync(path.join(root, "out"), { recursive: true });
    return root;
  }

  afterAll(async () => {
    for (const root of roots) {
      await fsp.rm(root, { recursive: true, force: true });
    }
  });

  it("renders htmlAttrs and bodyAttrs from head() onto the document shell", async () => {
    const root = makeFixture();
    fs.writeFileSync(
      path.join(root, "routes", "_layout.js"),
      `import { h } from "preact";
export function head() {
  return { htmlAttrs: { lang: "en-CA" } };
}
export default function Layout(props) {
  return h("section", null, props.children);
}
`
    );
    fs.writeFileSync(
      path.join(root, "routes", "index.js"),
      `import { h } from "preact";
export const config = { mode: "static" };
export function head() {
  return { title: "Home", bodyAttrs: { "data-page": "home" } };
}
export default function Home() {
  return h("main", null, "home");
}
`
    );

    await renderStatic({
      routesDir: path.join(root, "routes"),
      outputDir: path.join(root, "out"),
    });

    const html = fs.readFileSync(path.join(root, "out", "index.html"), "utf-8");
    expect(html).toContain('<html lang="en-CA">');
    expect(html).toContain('<body data-page="home">');
  });

  it("defaults to lang=\"en\" and a bare <body> when head() sets no attrs", async () => {
    const root = makeFixture();
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

    const html = fs.readFileSync(path.join(root, "out", "index.html"), "utf-8");
    expect(html).toContain('<html lang="en">');
    expect(html).toContain("<body>");
  });
});
