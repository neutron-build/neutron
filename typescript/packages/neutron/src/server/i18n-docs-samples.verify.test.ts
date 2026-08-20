import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";
import {
  createI18nMiddleware,
  resolveLocalePath,
  stripLocalePrefix,
  withLocalePath,
} from "../core/i18n.js";

/**
 * TEMPORARY verification harness for the docs page
 * apps/site/src/content/docs/routing/internationalization.mdx.
 * Every snippet and table row from that page is executed against the real
 * server or the real exports. Deleted after the run.
 */

const closers: Array<() => Promise<void>> = [];
const roots: string[] = [];

afterAll(async () => {
  await Promise.all(closers.map((close) => close()));
  for (const root of roots) {
    await fs.rm(root, { recursive: true, force: true });
  }
});

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
      socket.close((error) => (error ? reject(error) : resolve(port)));
    });
    socket.on("error", reject);
  });
}

async function makeApp(files: Record<string, string>): Promise<string> {
  const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-i18n-docs-"));
  roots.push(root);
  await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
  for (const [file, source] of Object.entries(files)) {
    await fs.mkdir(path.dirname(path.join(root, file)), { recursive: true });
    await fs.writeFile(path.join(root, file), source, "utf-8");
  }
  return root;
}

async function boot(root: string): Promise<string> {
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
  return `http://127.0.0.1:${port}`;
}

// DOCS SAMPLE 1 — verbatim from "Setup".
const DOCS_MIDDLEWARE = `// src/middleware.ts
import { createI18nMiddleware } from "@neutron-build/core";

export const middleware = [
  createI18nMiddleware({
    locales: ["en", "es", "fr"],
    defaultLocale: "en",
  }),
];
`;

// DOCS SAMPLE 2 — loader verbatim from "Reading the Locale in a Loader",
// plus the app-mode config and a component so the loader output is observable.
const DOCS_CATCH_ALL = `import { h } from "preact";
import type { LoaderArgs } from "@neutron-build/core";

export const config = { mode: "app" };

export async function loader({ context }: LoaderArgs) {
  const locale = context.locale as string;
  const path = context.pathWithoutLocale as string;

  return { locale, path };
}

export default function Page({ data }: any) {
  return h("div", null, JSON.stringify(data));
}
`;

// "strategy: prefix" sample from "Strategies".
const PREFIX_MIDDLEWARE = `import { createI18nMiddleware } from "@neutron-build/core";

export const middleware = [
  createI18nMiddleware({
    locales: ["en", "es"],
    defaultLocale: "en",
    strategy: "prefix",
  }),
];
`;

const LONE_APP_ROUTE = `import { h } from "preact";
export const config = { mode: "app" };
export default function AppRoute() {
  return h("div", null, "app");
}
`;

describe("docs page: pure-function samples", () => {
  const options = {
    locales: ["en", "es", "fr"],
    defaultLocale: "en",
  };

  it("withLocalePath sample (three calls + comment values)", () => {
    expect(withLocalePath("/pricing", "es", options)).toBe("/es/pricing");
    expect(withLocalePath("/pricing", "en", options)).toBe("/pricing");
    expect(withLocalePath("/", "es", options)).toBe("/es");
  });

  it("withLocalePath throws for an unconfigured locale", () => {
    expect(() => withLocalePath("/pricing", "de", options)).toThrow();
  });

  it("resolveLocalePath sample returns the documented shape", () => {
    expect(resolveLocalePath("/es/pricing", options)).toEqual({
      locale: "es",
      pathname: "/es/pricing",
      pathWithoutLocale: "/pricing",
      hasLocalePrefix: true,
    });
  });

  it("stripLocalePrefix sample", () => {
    expect(stripLocalePrefix("/es/pricing", options)).toBe("/pricing");
  });

  it("throws when defaultLocale is not in locales (both functions)", () => {
    const bad = { locales: ["en", "es"], defaultLocale: "fr" };
    expect(() => resolveLocalePath("/pricing", bad)).toThrow();
    expect(() => withLocalePath("/pricing", "en", bad)).toThrow();
  });
});

describe("docs page: middleware tables and claims, against a live server", () => {
  it("prefix-except-default: /pricing is en, /es/pricing is es, /en/pricing 307s to /pricing", async () => {
    const root = await makeApp({
      "src/middleware.ts": DOCS_MIDDLEWARE,
      "src/routes/[...slug].tsx": DOCS_CATCH_ALL,
    });
    const base = await boot(root);

    const plain = await fetch(`${base}/pricing`);
    expect(plain.status).toBe(200);
    const plainBody = (await plain.text()).replace(/&quot;/g, '"');
    expect(plainBody).toContain('"locale":"en"');
    expect(plainBody).toContain('"path":"/pricing"');

    const es = await fetch(`${base}/es/pricing`);
    expect(es.status).toBe(200);
    const esBody = (await es.text()).replace(/&quot;/g, '"');
    expect(esBody).toContain('"locale":"es"');
    expect(esBody).toContain('"path":"/pricing"');

    const canonical = await fetch(`${base}/en/pricing`, { redirect: "manual" });
    if (canonical.status !== 307) {
      console.log("DEBUG 500 BODY:", (await canonical.text()).slice(0, 2000));
    }
    expect(canonical.status).toBe(307);
    expect(canonical.headers.get("location")).toBe("/pricing");
  }, 30000);

  it("a prefixed URL with no matching route 404s (matching is not rewritten)", async () => {
    const root = await makeApp({
      "src/middleware.ts": DOCS_MIDDLEWARE,
      "src/routes/app-route.tsx": LONE_APP_ROUTE,
    });
    const base = await boot(root);

    const res = await fetch(`${base}/es/pricing`);
    expect(res.status).toBe(404);
  }, 30000);

  it("strategy prefix: /pricing 307s to /en/pricing; non-GET is not redirected", async () => {
    const root = await makeApp({
      "src/middleware.ts": PREFIX_MIDDLEWARE,
      "src/routes/[...slug].tsx": DOCS_CATCH_ALL,
    });
    const base = await boot(root);

    const redirected = await fetch(`${base}/pricing`, { redirect: "manual" });
    expect(redirected.status).toBe(307);
    expect(redirected.headers.get("location")).toBe("/en/pricing");

    const post = await fetch(`${base}/pricing`, {
      method: "POST",
      redirect: "manual",
    });
    expect(post.status).not.toBe(307);
    expect(post.headers.get("location")).toBeNull();

    const es = await fetch(`${base}/es/pricing`);
    expect(es.status).toBe(200);
    expect((await es.text()).replace(/&quot;/g, '"')).toContain('"locale":"es"');
  }, 30000);
});
