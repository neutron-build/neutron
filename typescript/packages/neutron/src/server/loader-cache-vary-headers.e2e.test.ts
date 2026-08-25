import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { decodeSerializedPayload } from "../core/serialization.js";
import { createServer } from "./index.js";

/**
 * The loader-data cache keyed entries on URL alone. Cookie/Authorization
 * requests are excluded from caching entirely, but any OTHER header a loader
 * personalizes on (Accept-Language being the canonical one) was not part of
 * the key — one user's localized loader output served to everyone under
 * `loaderMaxAge`.
 */

let fixtureRoot = "";
let closeServer: (() => Promise<void>) | null = null;
let baseUrl = "";

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

const PAGE = `
import { h } from "preact";
let loadCount = 0;
export const config = { mode: "app", cache: { loaderMaxAge: 120 } };
export async function loader({ request }) {
  loadCount += 1;
  return { loadCount, lang: request.headers.get("accept-language") };
}
export default function Page({ data }) {
  return h("div", null, data.loadCount + ":" + data.lang);
}
`;

async function getLoaderData(url: string, acceptLanguage: string): Promise<{ loadCount: number; lang: string | null }> {
  const res = await fetch(url, {
    headers: { Accept: "application/json", "Accept-Language": acceptLanguage },
  });
  expect(res.status).toBe(200);
  const payload = decodeSerializedPayload<Record<string, unknown>>(await res.json());
  const first = Object.values(payload)[0] as { loadCount: number; lang: string | null } | undefined;
  if (!first || typeof first.loadCount !== "number") {
    throw new Error(`unexpected payload for ${url}: ${JSON.stringify(payload)}`);
  }
  return first;
}

beforeAll(async () => {
  fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-loader-vary-"));
  await fs.mkdir(path.join(fixtureRoot, "src", "routes", "localized"), { recursive: true });
  await fs.writeFile(path.join(fixtureRoot, "src", "routes", "localized", "index.ts"), PAGE, "utf-8");

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

describe("loader-data cache varies on Accept-Language", () => {
  it("header-personalized loader output is not shared across locales", { timeout: 30_000 }, async () => {
    const url = `${baseUrl}/localized`;

    const french = await getLoaderData(url, "fr");
    expect(french.lang).toBe("fr");

    const german = await getLoaderData(url, "de");
    expect(german.lang).toBe("de");
    expect(german.loadCount).toBe(french.loadCount + 1);
  });

  it("same-locale repeat requests still hit the cache", { timeout: 30_000 }, async () => {
    const url = `${baseUrl}/localized`;

    const first = await getLoaderData(url, "en");
    const second = await getLoaderData(url, "en");

    expect(second.lang).toBe("en");
    expect(second.loadCount).toBe(first.loadCount);
  });
});
