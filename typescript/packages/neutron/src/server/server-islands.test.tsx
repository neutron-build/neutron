import { describe, expect, it } from "vitest";
import { h } from "preact";
import { renderToString } from "preact-render-to-string";
import { ServerIsland, clearIslandRegistry, handleIslandRequest } from "./server-islands.js";

/**
 * The island registry is module-global and the island endpoint serves
 * first-fetch-wins. The only thing standing between another visitor and this
 * run's SSR data used to be the island URL itself — 32 bits of entropy plus a
 * predictable counter. The render now binds each island to a per-render token
 * that exists ONLY in the owning page's HTML: holding the island id (from a
 * log, a referrer, a guess) no longer fetches the content.
 */

function renderIslandPage(secret: string): string {
  return renderToString(
    h(ServerIsland, {
      fallback: h("div", null, "loading"),
      children: h("div", null, secret),
    })
  );
}

function extractIslandUrl(html: string): URL {
  const match = html.match(/\/__neutron_island\/[^"']+/);
  if (!match) throw new Error("island fetch URL not found in rendered page");
  return new URL(`http://localhost${match[0].replace(/\\u0026/g, "&")}`);
}

describe("server islands capability binding", () => {
  it("denies the island to a requester holding only the island id", async () => {
    clearIslandRegistry();
    const html = renderIslandPage("visitor-a-session-data");

    const url = extractIslandUrl(html);
    const islandId = decodeURIComponent(url.pathname.split("/").pop()!);

    // The attacker knows the id (and even a wrong/stolen-but-different token).
    await expect(handleIslandRequest(islandId)).resolves.toBeNull();
    await expect(handleIslandRequest(islandId, "forged-token")).resolves.toBeNull();

    // The owning page's browser holds the real token from the same HTML.
    const token = url.searchParams.get("t")!;
    expect(token).toBeTruthy();
    const content = await handleIslandRequest(islandId, token);
    expect(content).toContain("visitor-a-session-data");

    // One-shot: even the legitimate token is consumed by the first fetch.
    await expect(handleIslandRequest(islandId, token)).resolves.toBeNull();
  });

  it("does not leak the island token outside the fetch URL", async () => {
    clearIslandRegistry();
    const html = renderIslandPage("secret-payload");
    // The token may appear in the script's fetch URL, but never as an
    // attribute on the served DOM (where scrapers and extensions read it).
    expect(html).not.toMatch(/data-island-token|token="/);
    const url = extractIslandUrl(html);
    expect(url.searchParams.get("t")).toMatch(/^[0-9a-f]{32}$/);
  });
});
