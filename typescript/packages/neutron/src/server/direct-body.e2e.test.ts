import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";
import { rememberBody, sendKnownBody } from "./direct-body.js";

/**
 * A prebuilt page's bytes are written to the socket directly instead of being
 * read back out of the Response body stream. What reaches the client must be
 * what the ordinary path sent: the same bytes and length, and every header a
 * middleware put on the response on its way out.
 */

const closers: Array<() => Promise<void>> = [];
const roots: string[] = [];

afterAll(async () => {
  await Promise.all(closers.map((close) => close()));
  for (const root of roots) {
    await fs.rm(root, { recursive: true, force: true });
  }
});

const STATIC_ROUTE = `
import { h } from "preact";
export const config = { mode: "static" };
export default function Page() {
  return h("div", null, "page");
}
`;

const MIDDLEWARE = `
export default async function middleware(request, context, next) {
  const response = await next();
  const pathname = new URL(request.url).pathname;
  if (pathname === "/cookies") {
    response.headers.append("Set-Cookie", "a=1; Path=/");
    response.headers.append("Set-Cookie", "b=2; Path=/");
  }
  if (pathname === "/replaced") {
    return new Response("replaced by middleware", { status: 202, headers: { "Content-Type": "text/plain" } });
  }
  response.headers.set("X-From-Middleware", "yes");
  return response;
}
`;

const PAGE = '<!doctype html><html><body><div id="app">café ☃ page</div></body></html>';

async function boot(): Promise<string> {
  const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-direct-body-"));
  roots.push(root);
  await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
  await fs.mkdir(path.join(root, "dist"), { recursive: true });
  for (const name of ["index", "cookies", "replaced"]) {
    await fs.writeFile(path.join(root, "src", "routes", `${name}.tsx`), STATIC_ROUTE, "utf-8");
    await fs.writeFile(path.join(root, "dist", `${name}.html`), PAGE, "utf-8");
  }
  await fs.writeFile(path.join(root, "src", "middleware.ts"), MIDDLEWARE, "utf-8");
  const running = await createServer({
    host: "127.0.0.1",
    port: 0,
    rootDir: root,
    distDir: "dist",
    routesDir: "src/routes",
    compress: false,
  });
  closers.push(running.close);
  return running.url;
}

describe("prebuilt pages written straight to the socket", () => {
  let base: string;

  beforeAll(async () => {
    base = await boot();
  }, 60_000);

  it("sends the page's exact bytes, their length, and headers set by middleware", async () => {
    const res = await fetch(`${base}/`);
    const bytes = Buffer.from(await res.arrayBuffer());

    expect(res.status).toBe(200);
    expect(bytes.equals(Buffer.from(PAGE, "utf-8"))).toBe(true);
    expect(res.headers.get("content-length")).toBe(String(Buffer.byteLength(PAGE)));
    expect(res.headers.get("content-type")).toContain("text/html");
    expect(res.headers.get("etag")).toBeTruthy();
    expect(res.headers.get("x-from-middleware")).toBe("yes");
    expect(res.headers.get("transfer-encoding")).toBeNull();
  });

  it("keeps every Set-Cookie a middleware appended", async () => {
    const res = await fetch(`${base}/cookies`);

    expect(await res.text()).toBe(PAGE);
    expect(res.headers.getSetCookie()).toEqual(["a=1; Path=/", "b=2; Path=/"]);
  });

  it("sends a middleware's replacement response, not the page", async () => {
    const res = await fetch(`${base}/replaced`);

    expect(res.status).toBe(202);
    expect(await res.text()).toBe("replaced by middleware");
  });

  it("answers HEAD and a matching If-None-Match without a body", async () => {
    const head = await fetch(`${base}/`, { method: "HEAD" });
    expect(head.status).toBe(200);
    expect(await head.text()).toBe("");

    const etag = (await fetch(`${base}/`)).headers.get("etag")!;
    const revalidated = await fetch(`${base}/`, { headers: { "If-None-Match": etag } });
    expect(revalidated.status).toBe(304);
    expect(await revalidated.text()).toBe("");
  });
});

describe("sendKnownBody", () => {
  async function through(make: () => Response): Promise<{ sent: boolean; status: number; body: string }> {
    let sent = false;
    const server = http.createServer((_request, outgoing) => {
      sent = sendKnownBody(make(), outgoing);
      if (!sent) outgoing.writeHead(599).end("fallback");
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as { port: number };
    try {
      const res = await fetch(`http://127.0.0.1:${port}/`);
      return { sent, status: res.status, body: await res.text() };
    } finally {
      await new Promise((resolve) => server.close(resolve));
    }
  }

  it("leaves a response it was not told about to the ordinary path", async () => {
    const result = await through(() => new Response("plain"));
    expect(result).toEqual({ sent: false, status: 599, body: "fallback" });
  });

  it("leaves a response whose body was already consumed to the ordinary path", async () => {
    const result = await through(() => {
      const response = rememberBody(new Response("gone"), new TextEncoder().encode("gone"));
      void response.text();
      return response;
    });
    expect(result.sent).toBe(false);
  });

  it("writes the remembered bytes with the response's status and headers", async () => {
    const result = await through(() =>
      rememberBody(
        new Response("hello", { status: 203, headers: { "Content-Type": "text/plain", "Content-Length": "999" } }),
        new TextEncoder().encode("hello")
      )
    );
    expect(result).toEqual({ sent: true, status: 203, body: "hello" });
  });
});
