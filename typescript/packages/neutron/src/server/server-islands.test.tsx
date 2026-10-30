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

// Preact-side counterpart of type-fixtures/react-server-island/: ServerIsland
// now returns an `any` element so React-typed JSX can use it too. This file
// is compiled by the package build (jsxImportSource: preact), so the JSX
// below pins that preact DX, runtime behavior and prop checking did not
// regress — including the @ts-expect-error, which fails the build if a
// mistyped prop stops being rejected.
describe("ServerIsland (preact JSX)", () => {
  it("still rejects mistyped props (props stay fully typed)", () => {
    // @ts-expect-error `id` must be a string, not a number
    return <ServerIsland id={42}>payload</ServerIsland>;
  });

  it("renders the fallback shell from JSX and serves the children to the token holder", async () => {
    clearIslandRegistry();
    const html = renderToString(
      <ServerIsland id="jsx-cart" fallback={<p role="status">Loading cart…</p>}>
        <div data-cart-payload>jsx-rendered island content</div>
      </ServerIsland>
    );

    // The static shell: the placeholder sits where the widget will land.
    expect(html).toContain('id="jsx-cart"');
    expect(html).toContain("data-server-island");
    expect(html).toContain("Loading cart…");

    // The deferred half: the same page's token fetches the real children.
    const url = extractIslandUrl(html);
    expect(url.pathname).toContain("jsx-cart");
    const token = url.searchParams.get("t")!;
    const content = await handleIslandRequest("jsx-cart", token);
    expect(content).toContain("jsx-rendered island content");
  });
});
