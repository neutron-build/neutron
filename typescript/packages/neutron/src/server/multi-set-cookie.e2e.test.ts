import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { createServer as createViteServer } from "vite";
import { createServer } from "./index.js";
import { neutronPlugin } from "../vite/plugin.js";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";

/**
 * F-B6-2: the dev server copied route responses onto the Node response with
 * headers.forEach → setHeader, which replaces, so of several Set-Cookie
 * headers only the last reached the client (Better-Auth's sign-in sets the
 * session token and the session data, and only the data arrived). Every kind
 * of response that sets three cookies must send three Set-Cookie headers, in
 * dev and from createServer.
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

const COOKIES = (name: string) =>
  `["${name}1=a; Path=/", "${name}2=b; Path=/; HttpOnly", "${name}3=c; Path=/"]`;

const FIXTURE: Record<string, string> = {
  "src/routes/api/resource.ts": `
export function GET() {
  const out = new Headers();
  for (const cookie of ${COOKIES("resource")}) out.append("set-cookie", cookie);
  return new Response("ok", { headers: out });
}
`,
  "src/routes/api/plain.ts": `
export function GET() { return new Response("plain"); }
`,
  "src/middleware.ts": `
export const middleware = [
  async (request, _context, next) => {
    const path = new URL(request.url).pathname;
    if (path === "/denied") {
      const out = new Headers();
      for (const cookie of ${COOKIES("denied")}) out.append("set-cookie", cookie);
      return new Response("denied", { status: 401, headers: out });
    }
    if (path === "/api/thrown") {
      const out = new Headers();
      for (const cookie of ${COOKIES("thrown")}) out.append("set-cookie", cookie);
      throw new Response("thrown", { status: 403, headers: out });
    }
    const response = await next();
    if (path === "/api/plain") {
      for (const cookie of ${COOKIES("middleware")}) response.headers.append("set-cookie", cookie);
    }
    return response;
  },
];
`,
  "src/server.entry.ts": `
import { Hono } from "hono";
const api = new Hono();
api.get("/cookies", (c) => {
  for (const cookie of ${COOKIES("mount")}) c.header("set-cookie", cookie, { append: true });
  return c.text("mount");
});
export default { mount: { "/mount": api } };
`,
};

async function writeFixture(prefix: string): Promise<string> {
  // Inside the package dir so the Vite SSR runtime can resolve node_modules.
  const root = await fs.mkdtemp(path.join(process.cwd(), prefix));
  roots.push(root);
  for (const [file, body] of Object.entries(FIXTURE)) {
    await fs.mkdir(path.dirname(path.join(root, file)), { recursive: true });
    await fs.writeFile(path.join(root, file), body, "utf-8");
  }
  return root;
}

async function bootStart(root: string): Promise<string> {
  const port = await getFreePort();
  const running = await createServer({
    host: "127.0.0.1",
    port,
    rootDir: root,
    compress: false,
    serverBundle: false,
  });
  closers.push(running.close);
  return `http://127.0.0.1:${port}`;
}

async function bootDev(root: string): Promise<string> {
  const port = await getFreePort();
  const preactSsr = resolvePreactSsr(root);
  const vite = await createViteServer({
    configFile: false,
    root,
    logLevel: "error",
    plugins: [neutronPlugin({ routesDir: path.join(root, "src", "routes"), rootDir: root })],
    resolve: { alias: vitePreactAliases(preactSsr) },
    ssr: { noExternal: preactSsr.noExternal },
    server: { host: "127.0.0.1", port, strictPort: true, hmr: false },
  });
  await vite.listen();
  closers.push(() => vite.close());
  return `http://127.0.0.1:${port}`;
}

const CASES: Array<[label: string, pathname: string, name: string, status: number]> = [
  ["resource route", "/api/resource", "resource", 200],
  ["global middleware after next()", "/api/plain", "middleware", 200],
  ["global middleware answering an unmatched path", "/denied", "denied", 401],
  ["global middleware throwing a Response", "/api/thrown", "thrown", 403],
  ["server.entry.ts mount", "/mount/cookies", "mount", 200],
];

async function expectThreeCookies(base: string): Promise<void> {
  for (const [label, pathname, name, status] of CASES) {
    const response = await fetch(`${base}${pathname}`, { headers: { Accept: "text/html" } });
    await response.arrayBuffer();
    expect({ label, status: response.status }).toEqual({ label, status });
    expect({ label, cookies: response.headers.getSetCookie() }).toEqual({
      label,
      cookies: [`${name}1=a; Path=/`, `${name}2=b; Path=/; HttpOnly`, `${name}3=c; Path=/`],
    });
  }
}

describe("every Set-Cookie of a response reaches the client", () => {
  it("dev server", { timeout: 60_000 }, async () => {
    await expectThreeCookies(await bootDev(await writeFixture(".tmp-neutron-dev-set-cookie-")));
  });

  it("createServer", { timeout: 60_000 }, async () => {
    await expectThreeCookies(await bootStart(await writeFixture(".tmp-neutron-start-set-cookie-")));
  });
});
