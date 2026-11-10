import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as http from "node:http";
import { AddressInfo, createServer as createNetServer } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { canonicalRequestPath, normalizePathname } from "../core/route-path.js";
import { createServer } from "./index.js";

describe("canonicalRequestPath", () => {
  it.each([
    ["/%61dmin", "/admin"],
    ["/adm%69n/%7Eme", "/admin/~me"],
    ["//admin", "/admin"],
    ["///a//b/", "/a/b/"],
    ["/a%2fb", "/a%2Fb"],
    ["/caf%c3%a9", "/caf%C3%A9"],
    ["/a%20b", "/a%20b"],
    ["/admin", "/admin"],
    ["/", "/"],
  ])("%s -> %s", (raw, canonical) => {
    expect(canonicalRequestPath(raw)).toBe(canonical);
  });
});

describe("normalizePathname splits before decoding", () => {
  it("keeps an encoded slash inside its segment", () => {
    expect(normalizePathname("/admin%2F")).toBe("/admin%2F");
    expect(normalizePathname("/%2Fadmin")).toBe("/%2Fadmin");
    expect(normalizePathname("/a%2Fb/c")).toBe("/a%2Fb/c");
  });
  it("still decodes everything else and still refuses traversal", () => {
    expect(normalizePathname("/caf%C3%A9/")).toBe("/café");
    expect(normalizePathname("/a%2F..%2Fsecret")).toBeNull();
    expect(normalizePathname("/%E0%A4%A")).toBeNull();
  });
});

const ROUTE = `import { h } from "preact";
export const config = { mode: "static" };
export default function Page() { return h("div", null, "page"); }
`;
const GATE = `export default async function middleware(request, context, next) {
  if (new URL(request.url).pathname === "/private") return new Response("forbidden", { status: 403 });
  return next();
}
`;

describe("a gate on the URL and the router agree on the route", () => {
  let root: string;
  let base: string;
  let port: number;
  let close: () => Promise<void>;

  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-canonical-"));
    await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
    await fs.mkdir(path.join(root, "dist"), { recursive: true });
    await fs.writeFile(path.join(root, "src", "routes", "private.tsx"), ROUTE);
    await fs.writeFile(path.join(root, "dist", "private.html"), "<!doctype html><body>PRIVATE</body>");
    await fs.writeFile(path.join(root, "src", "middleware.ts"), GATE);
    port = await new Promise<number>((resolve) => {
      const socket = createNetServer();
      socket.listen(0, "127.0.0.1", () => {
        const free = (socket.address() as AddressInfo).port;
        socket.close(() => resolve(free));
      });
    });
    const running = await createServer({ host: "127.0.0.1", port, rootDir: root, distDir: "dist", routesDir: "src/routes", compress: false });
    close = running.close;
    base = `http://127.0.0.1:${port}`;
  });

  afterAll(async () => {
    await close?.();
    await fs.rm(root, { recursive: true, force: true });
  });

  // fetch() normalizes some of these away, so the raw request line goes on the wire.
  const raw = (target: string, method = "GET", headers: Record<string, string> = {}) =>
    new Promise<{ status: number; location?: string; body: string }>((resolve, reject) => {
      const req = http.request({ host: "127.0.0.1", port, path: target, method, headers }, (res) => {
        let body = "";
        res.on("data", (chunk) => (body += chunk));
        res.on("end", () => resolve({ status: res.statusCode!, location: res.headers.location, body }));
      });
      req.on("error", reject);
      req.end();
    });

  it("the gate holds for the plain spelling", async () => {
    expect((await raw("/private")).status).toBe(403);
  });

  it.each(["/%70rivate", "/priv%61te", "//private", "///private", "/%70%72ivate"])("%s redirects to the path the gate names", async (target) => {
    const res = await raw(target);
    expect(res.status).toBe(308);
    expect(res.location).toBe("/private");
    expect(res.body).not.toContain("PRIVATE");
  });

  it("keeps the query and the method through the redirect", async () => {
    const res = await raw("/%70rivate?a=1", "POST");
    expect(res.status).toBe(308);
    expect(res.location).toBe("/private?a=1");
  });

  it("cannot be turned into an open redirect", async () => {
    expect((await raw("//evil.example/x")).location).toBe("/evil.example/x");
  });

  it.each(["/private%2F", "/private%2f", "/%2Fprivate"])("%s is not the gated route and serves nothing", async (target) => {
    const res = await raw(target.replace("%2f", "%2F"));
    expect(res.body).not.toContain("PRIVATE");
    expect(res.status).toBe(404);
  });

  it("refuses a declared body over the default cap", async () => {
    const declared = await raw("/private", "POST", { "content-length": String(11 * 1024 * 1024) });
    expect(declared.status).toBe(413);
  });
});
