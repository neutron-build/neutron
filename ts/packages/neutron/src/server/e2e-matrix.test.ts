import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { decodeSerializedPayload } from "../core/serialization.js";
import { createServer } from "./index.js";

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

async function writeFixtureApp(rootDir: string): Promise<void> {
  await fs.mkdir(path.join(rootDir, "dist", "assets"), { recursive: true });
  await fs.mkdir(path.join(rootDir, "src", "routes", "users"), { recursive: true });
  await fs.mkdir(path.join(rootDir, "src", "routes", "partial"), { recursive: true });

  await fs.writeFile(
    path.join(rootDir, "dist", "index.html"),
    "<!doctype html><html><body><h1>Static Matrix Fixture</h1></body></html>",
    "utf-8"
  );
  await fs.writeFile(
    path.join(rootDir, "dist", "assets", "index-test.js"),
    "console.log('fixture client entry');",
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "users", "[id].ts"),
    `
import { h } from "preact";
export const config = { mode: "app", cache: { maxAge: 30 } };
export function headers() {
  return { "Cache-Control": "public, max-age=30", Vary: "Accept" };
}
export async function loader({ params }) {
  return { user: { id: params.id || "0", name: "User " + (params.id || "0") } };
}
export default function UserPage({ data }) {
  return h("main", null, "User route: " + data.user.name);
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "form.ts"),
    `
import { h } from "preact";
let count = 0;
export const config = { mode: "app" };
export async function loader() {
  return { count };
}
export async function action({ request }) {
  const form = await request.formData();
  if (form.get("intent") === "inc") {
    count += 1;
  }
  return { ok: true, count };
}
export default function FormPage({ data, actionData }) {
  return h("div", null, "count=" + data.count + (actionData ? ";action=" + actionData.count : ""));
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "error.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  throw new Error("matrix boom");
}
export function ErrorBoundary({ error }) {
  return h("section", null, "Boundary says: " + error.message);
}
export default function ErrorPage() {
  return h("div", null, "unreachable");
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "islands.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export default function IslandsPage() {
  return h("div", null,
    h("h1", null, "Islands route"),
    h("neutron-island", {
      "data-client": "load",
      "data-island-id": "demo",
      "data-props": "{}"
    }, "fallback")
  );
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "stream.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  return { items: Array.from({ length: 120 }, (_, i) => "item-" + i) };
}
export default function StreamPage({ data }) {
  return h("ul", null, data.items.map((item) => h("li", { key: item }, item)));
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "head.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export function head() {
  return {
    title: "Head Fixture",
    description: "Matrix head description",
    openGraph: {
      title: "Head OG",
    },
  };
}
export default function HeadPage() {
  return h("main", null, "Head route");
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "cached.ts"),
    `
import { h } from "preact";
let loadCount = 0;
let mutationCount = 0;
export const config = { mode: "app", cache: { loaderMaxAge: 120 } };
export async function loader() {
  loadCount += 1;
  return { loadCount, mutationCount };
}
export async function action() {
  mutationCount += 1;
  return { ok: true, mutationCount };
}
export default function CachedPage({ data }) {
  return h("div", null, "load=" + data.loadCount + ";mutation=" + data.mutationCount);
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "partial", "_layout.ts"),
    `
import { h } from "preact";
const state = ((globalThis).__partialMatrixState ||= { layoutLoads: 0, pageLoads: 0 });
export const config = { mode: "app" };
export async function loader() {
  state.layoutLoads += 1;
  return { layoutLoads: state.layoutLoads };
}
export default function PartialLayout({ children }) {
  return h("section", null, children);
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "partial", "index.ts"),
    `
import { h } from "preact";
const state = ((globalThis).__partialMatrixState ||= { layoutLoads: 0, pageLoads: 0 });
export const config = { mode: "app" };
export async function loader() {
  state.pageLoads += 1;
  return { pageLoads: state.pageLoads };
}
export default function PartialPage({ data }) {
  return h("div", null, "partial:" + data.pageLoads);
}
`,
    "utf-8"
  );
}

describe("server e2e matrix", () => {
  let fixtureRoot = "";
  let closeServer: (() => Promise<void>) | null = null;
  let baseUrl = "";

  beforeAll(async () => {
    fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-e2e-matrix-"));
    await writeFixtureApp(fixtureRoot);

    const port = await getFreePort();
    const running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: fixtureRoot,
      distDir: "dist",
      routesDir: "src/routes",
      compress: false,
    });

    closeServer = running.close;
    baseUrl = `http://127.0.0.1:${port}`;
  });

  afterAll(async () => {
    if (closeServer) {
      await closeServer();
    }
    if (fixtureRoot) {
      await fs.rm(fixtureRoot, { recursive: true, force: true });
    }
  });

  it(
    "covers static/app/islands/forms/errors/streaming behavior",
    async () => {
      const staticResponse = await fetch(`${baseUrl}/`);
      expect(staticResponse.status).toBe(200);
      expect(await staticResponse.text()).toContain("Static Matrix Fixture");

      const appHtml = await fetch(`${baseUrl}/users/1`);
      expect(appHtml.status).toBe(200);
      expect(appHtml.headers.get("content-type")).toContain("text/html");
      expect(await appHtml.text()).toContain("User route: User 1");

      const appJson = await fetch(`${baseUrl}/users/1`, {
        headers: { Accept: "application/json" },
      });
      expect(appJson.status).toBe(200);
      const appPayload = decodeSerializedPayload<Record<string, unknown>>(
        await appJson.json()
      );
      expect((appPayload["route:users/[id].ts"] as { user: { id: string } }).user.id).toBe("1");

      const formInitial = await fetch(`${baseUrl}/form`, {
        headers: { Accept: "application/json" },
      });
      const formInitialPayload = decodeSerializedPayload<Record<string, unknown>>(
        await formInitial.json()
      );
      expect((formInitialPayload["route:form.ts"] as { count: number }).count).toBe(0);

      const formData = new FormData();
      formData.set("intent", "inc");
      const formMutation = await fetch(`${baseUrl}/form`, {
        method: "POST",
        body: formData,
        headers: { Accept: "application/json" },
      });
      expect(formMutation.status).toBe(200);
      const formMutationPayload = decodeSerializedPayload<Record<string, unknown>>(
        await formMutation.json()
      );
      expect((formMutationPayload["route:form.ts"] as { count: number }).count).toBe(1);
      expect((formMutationPayload.__action__ as { count: number }).count).toBe(1);

      const islandsResponse = await fetch(`${baseUrl}/islands`);
      expect(islandsResponse.status).toBe(200);
      const islandsHtml = await islandsResponse.text();
      expect(islandsHtml).toContain("<neutron-island");
      expect(islandsHtml).toContain('src="/assets/index-test.js"');

      const errorResponse = await fetch(`${baseUrl}/error`);
      expect(errorResponse.status).toBe(500);
      expect(await errorResponse.text()).toContain("Boundary says: matrix boom");

      const streamResponse = await fetch(`${baseUrl}/stream`);
      expect(streamResponse.status).toBe(200);
      const streamHtml = await streamResponse.text();
      expect(streamHtml).toContain("item-0");
      expect(streamHtml).toContain("item-119");
      expect(streamResponse.headers.get("content-type")).toContain("text/html");

      const headResponse = await fetch(`${baseUrl}/head`);
      expect(headResponse.status).toBe(200);
      const headHtml = await headResponse.text();
      expect(headHtml).toContain("<title>Head Fixture</title>");
      expect(headHtml).toContain('name="description" content="Matrix head description"');
      expect(headHtml).toContain('property="og:title" content="Head OG"');

      const cachedFirst = await fetch(`${baseUrl}/cached`, {
        headers: { Accept: "application/json" },
      });
      expect(cachedFirst.status).toBe(200);
      const cachedFirstPayload = decodeSerializedPayload<Record<string, unknown>>(
        await cachedFirst.json()
      );
      expect(
        (cachedFirstPayload["route:cached.ts"] as { loadCount: number }).loadCount
      ).toBe(1);

      const cachedSecond = await fetch(`${baseUrl}/cached`, {
        headers: { Accept: "application/json" },
      });
      const cachedSecondPayload = decodeSerializedPayload<Record<string, unknown>>(
        await cachedSecond.json()
      );
      expect(
        (cachedSecondPayload["route:cached.ts"] as { loadCount: number }).loadCount
      ).toBe(1);

      const cachedMutation = await fetch(`${baseUrl}/cached`, {
        method: "POST",
        headers: { Accept: "application/json" },
      });
      expect(cachedMutation.status).toBe(200);
      const cachedMutationPayload = decodeSerializedPayload<Record<string, unknown>>(
        await cachedMutation.json()
      );
      expect(
        (cachedMutationPayload["route:cached.ts"] as { loadCount: number }).loadCount
      ).toBe(2);
      expect(
        (cachedMutationPayload["route:cached.ts"] as { mutationCount: number }).mutationCount
      ).toBe(1);

      const cachedThird = await fetch(`${baseUrl}/cached`, {
        headers: { Accept: "application/json" },
      });
      const cachedThirdPayload = decodeSerializedPayload<Record<string, unknown>>(
        await cachedThird.json()
      );
      expect(
        (cachedThirdPayload["route:cached.ts"] as { loadCount: number }).loadCount
      ).toBe(2);

      const partialFirst = await fetch(`${baseUrl}/partial`, {
        headers: {
          Accept: "application/json",
          "X-Neutron-Data": "true",
        },
      });
      const partialFirstPayload = decodeSerializedPayload<Record<string, unknown>>(
        await partialFirst.json()
      );
      expect(
        (partialFirstPayload["route:partial/_layout.ts"] as { layoutLoads: number }).layoutLoads
      ).toBe(1);
      expect(
        (partialFirstPayload["route:partial/index.ts"] as { pageLoads: number }).pageLoads
      ).toBe(1);

      const partialOnlyPage = await fetch(`${baseUrl}/partial`, {
        headers: {
          Accept: "application/json",
          "X-Neutron-Data": "true",
          "X-Neutron-Routes": "route:partial/index.ts",
        },
      });
      const partialOnlyPagePayload = decodeSerializedPayload<Record<string, unknown>>(
        await partialOnlyPage.json()
      );
      expect(partialOnlyPagePayload["route:partial/_layout.ts"]).toBeUndefined();
      expect(
        (partialOnlyPagePayload["route:partial/index.ts"] as { pageLoads: number }).pageLoads
      ).toBe(2);

      const partialAllAgain = await fetch(`${baseUrl}/partial`, {
        headers: {
          Accept: "application/json",
          "X-Neutron-Data": "true",
        },
      });
      const partialAllAgainPayload = decodeSerializedPayload<Record<string, unknown>>(
        await partialAllAgain.json()
      );
      expect(
        (partialAllAgainPayload["route:partial/_layout.ts"] as { layoutLoads: number }).layoutLoads
      ).toBe(2);
      expect(
        (partialAllAgainPayload["route:partial/index.ts"] as { pageLoads: number }).pageLoads
      ).toBe(3);
    },
    45000
  );

  it("supports fetcher-style JSON load from a different route", async () => {
    // useFetcher().load() fetches loader data from an arbitrary route via JSON
    const response = await fetch(`${baseUrl}/users/42`, {
      headers: {
        Accept: "application/json",
        "X-Neutron-Data": "true",
      },
    });
    expect(response.status).toBe(200);
    const contentType = response.headers.get("content-type") || "";
    expect(contentType).toContain("application/json");

    const payload = decodeSerializedPayload<Record<string, unknown>>(
      await response.json()
    );
    const userData = payload["route:users/[id].ts"] as { user: { id: string; name: string } };
    expect(userData.user.id).toBe("42");
    expect(userData.user.name).toBe("User 42");
  });

  it("supports fetcher-style JSON mutation to a specific action", async () => {
    // useFetcher().submit() POSTs to a route action and gets JSON back
    const formData = new FormData();
    formData.set("intent", "inc");

    const response = await fetch(`${baseUrl}/form`, {
      method: "POST",
      body: formData,
      headers: {
        Accept: "application/json",
        "X-Neutron-Data": "true",
      },
    });

    expect(response.status).toBe(200);
    const contentType = response.headers.get("content-type") || "";
    expect(contentType).toContain("application/json");

    const payload = decodeSerializedPayload<Record<string, unknown>>(
      await response.json()
    );
    // Action data is returned alongside loader data
    expect(payload.__action__).toBeDefined();
    expect((payload.__action__ as { ok: boolean }).ok).toBe(true);
    // Loader data is also returned (re-run after action)
    expect(payload["route:form.ts"]).toBeDefined();
  });

  it("includes __head__ in JSON responses for SPA navigation", async () => {
    const response = await fetch(`${baseUrl}/head`, {
      headers: {
        Accept: "application/json",
        "X-Neutron-Data": "true",
      },
    });
    expect(response.status).toBe(200);
    const payload = decodeSerializedPayload<Record<string, unknown>>(
      await response.json()
    );
    const headHtml = payload.__head__ as string;
    expect(typeof headHtml).toBe("string");
    expect(headHtml).toContain("<title>Head Fixture</title>");
    expect(headHtml).toContain('name="description" content="Matrix head description"');
    expect(headHtml).toContain('property="og:title" content="Head OG"');
  });

  it("returns full HTML for plain form POST without X-Neutron-Data header", async () => {
    const body = new URLSearchParams({ intent: "inc" });
    const response = await fetch(`${baseUrl}/form`, {
      method: "POST",
      body: body.toString(),
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
    });

    expect(response.status).toBe(200);
    const contentType = response.headers.get("content-type") || "";
    expect(contentType).toContain("text/html");

    const html = await response.text();
    expect(html).toContain("<!DOCTYPE html>");
    expect(html).toContain("count=");
    expect(html).toContain("action=");
  }, 15000);
});
