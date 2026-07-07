import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createServer, type NeutronErrorEvent } from "./index.js";

async function getFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const socket = net.createServer();
    socket.listen(0, "127.0.0.1", () => {
      const address = socket.address();
      if (!address || typeof address === "string") {
        reject(new Error("Failed to resolve test port"));
        return;
      }
      const { port } = address;
      socket.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(port);
      });
    });
    socket.on("error", reject);
  });
}

async function writeDistShell(rootDir: string): Promise<void> {
  await fs.mkdir(path.join(rootDir, "dist"), { recursive: true });
  await fs.writeFile(
    path.join(rootDir, "dist", "index.html"),
    "<!doctype html><html><body><div id=\"app\"></div></body></html>",
    "utf-8"
  );
}

// Fixture: a fragment root layout, an app page it wraps, and an app-mode
// /health route whose loader signals unhealthy with a 503. Exercises both the
// user-defined /health branch (Bug 1) and normal fragment composition (Bug 2).
async function writeFragmentFixture(rootDir: string): Promise<void> {
  await writeDistShell(rootDir);
  await fs.mkdir(path.join(rootDir, "src", "routes", "app"), { recursive: true });

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "app", "_layout.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export default function AppLayout({ children }) {
  return h("section", { class: "shell" }, children);
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "app", "index.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  return { ok: true };
}
export default function Home({ data }) {
  return h("main", null, "home " + data.ok);
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "health.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  // Dependency-aware health: report unhealthy instead of a false 200.
  throw new Response("db down", { status: 503 });
}
export default function Health() {
  return h("main", null, "healthy");
}
`,
    "utf-8"
  );
}

// Fixture: a root layout that renders a full document. Composing it into the
// shell's #app would nest <html>/<body> — the server must reject it (Bug 2).
async function writeFullDocumentFixture(rootDir: string): Promise<void> {
  await writeDistShell(rootDir);
  await fs.mkdir(path.join(rootDir, "src", "routes", "app"), { recursive: true });

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "app", "_layout.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export default function AppLayout({ children }) {
  return h(
    "html",
    { lang: "en" },
    h("head", null, h("title", null, "App")),
    h("body", null, children)
  );
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "app", "index.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  return { ok: true };
}
export default function Home({ data }) {
  return h("main", null, "home " + data.ok);
}
`,
    "utf-8"
  );
}

// Fixture: static-only, no /health route — the built-in /health must answer.
async function writeStaticOnlyFixture(rootDir: string): Promise<void> {
  await writeDistShell(rootDir);
  await fs.mkdir(path.join(rootDir, "src", "routes"), { recursive: true });
}

describe("app-mode /health route and full-document guard", () => {
  const roots: string[] = [];
  const closers: Array<() => Promise<void>> = [];

  async function makeFixture(): Promise<string> {
    const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-app-guards-"));
    roots.push(root);
    return root;
  }

  afterAll(async () => {
    for (const close of closers) {
      await close();
    }
    for (const root of roots) {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("serves a user-defined app-mode /health route instead of the built-in (Bug 1: present)", async () => {
    const root = await makeFixture();
    await writeFragmentFixture(root);
    const port = await getFreePort();
    const running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      distDir: "dist",
      routesDir: "src/routes",
      compress: false,
    });
    closers.push(running.close);

    const res = await fetch(`http://127.0.0.1:${port}/health`);
    // The user route reports unhealthy (503); the built-in would have been 200.
    expect(res.status).toBe(503);
    expect(await res.text()).toBe("db down");
  }, 30000);

  it("falls back to the built-in /health when no user route claims it (Bug 1: absent)", async () => {
    const root = await makeFixture();
    await writeStaticOnlyFixture(root);
    const port = await getFreePort();
    const running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      distDir: "dist",
      routesDir: "src/routes",
      compress: false,
      version: "9.9.9",
    });
    closers.push(running.close);

    const res = await fetch(`http://127.0.0.1:${port}/health`);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("application/json");
    const body = (await res.json()) as Record<string, unknown>;
    expect(Object.keys(body).sort()).toEqual(["nucleus", "status", "version"]);
    expect(body.status).toBe("ok");
    expect(body.version).toBe("9.9.9");
  }, 30000);

  it("composes a fragment-rendering layout into #app exactly as before (Bug 2: fragment)", async () => {
    const root = await makeFixture();
    await writeFragmentFixture(root);
    const port = await getFreePort();
    const running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      distDir: "dist",
      routesDir: "src/routes",
      compress: false,
    });
    closers.push(running.close);

    const res = await fetch(`http://127.0.0.1:${port}/app`);
    expect(res.status).toBe(200);
    const html = await res.text();
    // Single document shell owns <html>; the fragment mounts inside #app.
    expect((html.match(/<html/gi) || []).length).toBe(1);
    expect(html).toContain('<div id="app">');
    expect(html).toContain('<section class="shell">');
    expect(html).toContain("home true");
  }, 30000);

  it("rejects a layout that renders a full document with a descriptive error (Bug 2: full document)", async () => {
    const root = await makeFixture();
    await writeFullDocumentFixture(root);
    const errors: NeutronErrorEvent[] = [];
    const port = await getFreePort();
    const running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      distDir: "dist",
      routesDir: "src/routes",
      compress: false,
      hooks: {
        onError: (event) => {
          errors.push(event);
        },
      },
    });
    closers.push(running.close);

    const res = await fetch(`http://127.0.0.1:${port}/app`);
    // The malformed nested document must not ship — the guard turns it into a
    // server-side error response instead.
    expect(res.status).toBe(500);
    expect(await res.text()).toContain("Application Error");

    const renderError = errors.find((event) => event.source === "render");
    expect(renderError).toBeDefined();
    expect(renderError!.error.message).toContain("rendered a full document");
    expect(renderError!.error.message).toContain("render a fragment instead");
    // The offending layout file is named in the message.
    expect(renderError!.error.message).toContain("_layout.ts");
  }, 30000);
});
