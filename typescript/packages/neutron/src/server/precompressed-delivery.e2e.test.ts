import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as path from "node:path";
import { brotliDecompressSync, gunzipSync } from "node:zlib";
import { afterAll, describe, expect, it } from "vitest";
import { createServer, type NeutronServerOptions } from "./index.js";
import { precompressBuildOutput } from "../core/precompress.js";

/**
 * Prerendered pages and build assets are sent from the `.br` / `.gz`
 * siblings `neutron-ts build` writes, negotiated on Accept-Encoding, with a
 * strong ETag per representation — not gzipped again on every request.
 */

const closers: Array<() => Promise<void>> = [];
const roots: string[] = [];

afterAll(async () => {
  await Promise.all(closers.map((close) => close()));
  for (const root of roots) await fs.rm(root, { recursive: true, force: true });
});

const PAGE = `<!doctype html><html><body>${"<p>about neutron</p>".repeat(300)}</body></html>`;
const ASSET = `export const data = ${JSON.stringify("asset ".repeat(500))};\n`;
const ROUTE = `
import { h } from "preact";
export const config = { mode: "static" };
export default function Page() { return h("div", null, "page"); }
`;

async function makeApp(options: { middleware?: string; precompress?: boolean } = {}): Promise<string> {
  const root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-precompressed-"));
  roots.push(root);
  const write = async (file: string, content: string) => {
    await fs.mkdir(path.dirname(path.join(root, file)), { recursive: true });
    await fs.writeFile(path.join(root, file), content, "utf-8");
  };
  await write("src/routes/about.tsx", ROUTE);
  await write("dist/about/index.html", PAGE);
  await write("dist/assets/app-abc123.js", ASSET);
  await write("dist/assets/legacy-abc123.js", ASSET.replace("data", "legacy"));
  if (options.middleware) await write("src/middleware.ts", options.middleware);
  if (options.precompress !== false) precompressBuildOutput(path.join(root, "dist"));
  // An output from before build-time precompression: no siblings.
  await fs.rm(path.join(root, "dist/assets/legacy-abc123.js.br"), { force: true });
  await fs.rm(path.join(root, "dist/assets/legacy-abc123.js.gz"), { force: true });
  return root;
}

async function boot(root: string, options: Partial<NeutronServerOptions> = {}): Promise<number> {
  const running = await createServer({ rootDir: root, host: "127.0.0.1", port: 0, ...options });
  closers.push(() => running.close());
  return Number(new URL(running.url).port);
}

interface RawResponse {
  status: number;
  headers: http.IncomingHttpHeaders;
  body: Buffer;
}

/** Raw bytes: fetch would negotiate and decode on its own. */
function request(port: number, pathname: string, headers: Record<string, string> = {}, method = "GET"): Promise<RawResponse> {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: "127.0.0.1", port, path: pathname, method, headers }, (res) => {
      const chunks: Buffer[] = [];
      res.on("data", (chunk: Buffer) => chunks.push(chunk));
      res.on("end", () => resolve({ status: res.statusCode ?? 0, headers: res.headers, body: Buffer.concat(chunks) }));
      res.on("error", reject);
    });
    req.on("error", reject);
    req.end();
  });
}

function decode(response: RawResponse): string {
  const encoding = response.headers["content-encoding"];
  if (encoding === "br") return brotliDecompressSync(response.body).toString("utf-8");
  if (encoding === "gzip") return gunzipSync(response.body).toString("utf-8");
  return response.body.toString("utf-8");
}

function varyTokens(response: RawResponse): string[] {
  return String(response.headers.vary ?? "")
    .split(",")
    .map((token) => token.trim().toLowerCase());
}

describe("precompressed prerendered pages", () => {
  it("negotiates br > gzip > identity and sends the build's bytes", { timeout: 30_000 }, async () => {
    const root = await makeApp();
    const port = await boot(root);
    const brFile = await fs.readFile(path.join(root, "dist/about/index.html.br"));

    const br = await request(port, "/about", { "accept-encoding": "gzip, deflate, br" });
    expect(br.status).toBe(200);
    expect(br.headers["content-encoding"]).toBe("br");
    expect(br.body.equals(brFile)).toBe(true);
    expect(Number(br.headers["content-length"])).toBe(brFile.length);
    expect(varyTokens(br)).toContain("accept-encoding");
    expect(br.headers["cache-control"]).toBe("public, max-age=0, must-revalidate");
    expect(br.headers.etag).toMatch(/^"[^"]+-br"$/);
    expect(decode(br)).toBe(PAGE);

    const gzip = await request(port, "/about", { "accept-encoding": "br;q=0, gzip" });
    expect(gzip.headers["content-encoding"]).toBe("gzip");
    expect(gzip.headers.etag).toMatch(/^"[^"]+-gzip"$/);
    expect(decode(gzip)).toBe(PAGE);

    for (const accept of [undefined, "identity", "br;q=0, gzip;q=0", "gzip;q=0.5, identity"]) {
      const identity = await request(port, "/about", accept ? { "accept-encoding": accept } : {});
      expect(identity.headers["content-encoding"], accept).toBeUndefined();
      expect(identity.body.toString("utf-8"), accept).toBe(PAGE);
      expect(Number(identity.headers["content-length"]), accept).toBe(Buffer.byteLength(PAGE));
      expect(identity.headers.etag, accept).toMatch(/^"[^"-]+-[^"-]+"$/);
    }
  });

  it("answers If-None-Match with 304 for the representation it would send", { timeout: 30_000 }, async () => {
    const port = await boot(await makeApp());
    const first = await request(port, "/about", { "accept-encoding": "br" });
    const etag = String(first.headers.etag);

    const revalidated = await request(port, "/about", { "accept-encoding": "br", "if-none-match": etag });
    expect(revalidated.status).toBe(304);
    expect(revalidated.body.length).toBe(0);
    expect(revalidated.headers.etag).toBe(etag);

    // The br validator does not match the gzip representation.
    const other = await request(port, "/about", { "accept-encoding": "gzip", "if-none-match": etag });
    expect(other.status).toBe(200);
    expect(other.headers["content-encoding"]).toBe("gzip");
  });

  it("answers HEAD with the selected representation's length and no body", { timeout: 30_000 }, async () => {
    const root = await makeApp();
    const port = await boot(root);
    const head = await request(port, "/about", { "accept-encoding": "br" }, "HEAD");
    expect(head.status).toBe(200);
    expect(head.headers["content-encoding"]).toBe("br");
    expect(Number(head.headers["content-length"])).toBe(
      (await fs.readFile(path.join(root, "dist/about/index.html.br"))).length
    );
    expect(head.body.length).toBe(0);
  });

  it("falls back to dynamic compression without siblings, and to identity with compress: false", { timeout: 30_000 }, async () => {
    const port = await boot(await makeApp({ precompress: false }));
    const dynamic = await request(port, "/about", { "accept-encoding": "gzip" });
    expect(dynamic.headers["content-encoding"]).toBe("gzip");
    expect(decode(dynamic)).toBe(PAGE);

    const off = await boot(await makeApp(), { compress: false });
    const identity = await request(off, "/about", { "accept-encoding": "br, gzip" });
    expect(identity.headers["content-encoding"]).toBeUndefined();
    expect(identity.body.toString("utf-8")).toBe(PAGE);
  });

  it("keeps what global middleware adds and never replaces a response it made", { timeout: 60_000 }, async () => {
    const port = await boot(
      await makeApp({
        middleware: `
export default async function middleware(request, context, next) {
  if (new URL(request.url).searchParams.has("own")) return new Response("own answer", { headers: { "content-type": "text/plain" } });
  const response = await next();
  response.headers.set("x-gate", "passed");
  return response;
}
`,
      })
    );
    const passed = await request(port, "/about", { "accept-encoding": "br" });
    expect(passed.headers["content-encoding"]).toBe("br");
    expect(passed.headers["x-gate"]).toBe("passed");
    expect(decode(passed)).toBe(PAGE);

    const own = await request(port, "/about?own=1", { "accept-encoding": "br" });
    expect(own.headers["content-encoding"]).toBeUndefined();
    expect(own.body.toString("utf-8")).toBe("own answer");
  });
});

describe("precompressed build assets", () => {
  it("sends siblings with immutable caching, strong ETags and 304s", { timeout: 30_000 }, async () => {
    const root = await makeApp();
    const port = await boot(root);
    const br = await request(port, "/assets/app-abc123.js", { "accept-encoding": "gzip, br" });
    expect(br.status).toBe(200);
    expect(br.headers["content-encoding"]).toBe("br");
    expect(br.headers["content-type"]).toContain("javascript");
    expect(br.headers["cache-control"]).toBe("public, max-age=31536000, immutable");
    expect(varyTokens(br)).toContain("accept-encoding");
    expect(br.body.equals(await fs.readFile(path.join(root, "dist/assets/app-abc123.js.br")))).toBe(true);
    expect(br.headers.etag).toMatch(/^"[^"]+-br"$/);

    const revalidated = await request(port, "/assets/app-abc123.js", {
      "accept-encoding": "gzip, br",
      "if-none-match": String(br.headers.etag),
    });
    expect(revalidated.status).toBe(304);

    const identity = await request(port, "/assets/app-abc123.js");
    expect(identity.headers["content-encoding"]).toBeUndefined();
    expect(identity.body.toString("utf-8")).toBe(ASSET);
  });

  it("compresses an asset without siblings per request, and serves ranges as identity", { timeout: 30_000 }, async () => {
    const port = await boot(await makeApp());
    const legacy = await request(port, "/assets/legacy-abc123.js", { "accept-encoding": "gzip" });
    expect(legacy.headers["content-encoding"]).toBe("gzip");
    expect(decode(legacy)).toContain("legacy");

    const range = await request(port, "/assets/app-abc123.js", { range: "bytes=0-9" });
    expect(range.status).toBe(206);
    expect(range.body.toString("utf-8")).toBe(ASSET.slice(0, 10));
  });
});

describe("static files under a changing build output", () => {
  it("serves the new bytes and length after a file is rebuilt in place", async () => {
    const root = await makeApp({ precompress: false });
    const port = await boot(root);
    const file = path.join(root, "dist/assets/legacy-abc123.js");
    const before = await request(port, "/assets/legacy-abc123.js");
    expect(before.status).toBe(200);
    // Shorter content, as a build run in place under the live server writes.
    await fs.writeFile(file, "export const short = 1;\n");
    const after = await request(port, "/assets/legacy-abc123.js");
    expect(after.status).toBe(200);
    expect(after.body.toString("utf-8")).toBe("export const short = 1;\n");
    expect(Number(after.headers["content-length"])).toBe(after.body.byteLength);
    expect(after.headers.etag).not.toBe(before.headers.etag);
  });

  it("changes the ETag when content changes but size and mtime do not", async () => {
    const root = await makeApp({ precompress: false });
    const port = await boot(root);
    const file = path.join(root, "dist/assets/legacy-abc123.js");
    const fixed = new Date("2020-01-01T00:00:00Z");
    await fs.writeFile(file, "export const v = '1.2.3';\n");
    await fs.utimes(file, fixed, fixed);
    const first = await request(port, "/assets/legacy-abc123.js");
    // Reproducible builds: same size, same mtime, different bytes. Linux
    // reuses the inode at once, so only ctime tells the files apart; file
    // timestamps tick coarsely (a few ms), and a real rebuild comes later.
    await new Promise((resolve) => setTimeout(resolve, 50));
    await fs.rm(file);
    await fs.writeFile(file, "export const v = '1.2.4';\n");
    await fs.utimes(file, fixed, fixed);
    const second = await request(port, "/assets/legacy-abc123.js");
    expect(second.body.toString("utf-8")).toContain("1.2.4");
    expect(second.headers.etag).not.toBe(first.headers.etag);
  });

  it("answers a Range request with identity bytes that match Content-Range", async () => {
    const root = await makeApp();
    const port = await boot(root);
    const response = await request(port, "/assets/app-abc123.js", { range: "bytes=0-99", "accept-encoding": "br, gzip" });
    expect(response.status).toBe(206);
    expect(response.headers["content-encoding"]).toBeUndefined();
    expect(response.headers["content-range"]).toMatch(/^bytes 0-99\//);
    expect(response.body.byteLength).toBe(100);
    expect(response.body.toString("utf-8")).toBe(ASSET.slice(0, 100));
    expect(response.headers["cache-control"]).toBe("public, max-age=31536000, immutable");
  });
});
