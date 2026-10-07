import { describe, it, expect } from "vitest";

import { resolveClientTier } from "./render-app-route.js";
import {
  renderSpeculationRules,
  renderStaticLinkSpeculationRules,
} from "./speculation-rules.js";
import { parseRouteFacts } from "./manifest.js";

type R = { config: { mode: "static" | "app"; hydrate?: boolean } };
const staticRoute: R = { config: { mode: "static" } };
const appRoute: R = { config: { mode: "app" } };

describe("resolveClientTier", () => {
  // The defect this replaces: the old expression was
  // `allRoutes.every((r) => r.config.hydrate !== false)`, and `hydrate` is
  // undefined on every un-annotated route. One ordinary layout — the default
  // state of every layout — therefore forced the full router onto every page
  // in the app, static ones included.
  it("does not ship the router for a static page under an ordinary layout", () => {
    expect(resolveClientTier([staticRoute, staticRoute])).toBe("static");
  });

  it("ships the router when any route in the chain is an app route", () => {
    expect(resolveClientTier([staticRoute, appRoute])).toBe("full");
  });

  it("honours an explicit opt-out anywhere in the chain", () => {
    expect(resolveClientTier([{ config: { mode: "app", hydrate: false } }, appRoute])).toBe("none");
    // A layout that wants no JS cannot have JS reintroduced by its child.
    expect(resolveClientTier([{ config: { mode: "static", hydrate: false } }, appRoute])).toBe("none");
  });

  // The one genuine preference that is not derivable: a statically served page
  // that still wants client-side navigation, to keep scroll position, open
  // menus or playing media across clicks.
  it("lets a static route opt up to the router", () => {
    expect(resolveClientTier([staticRoute, { config: { mode: "static", hydrate: true } }])).toBe("full");
  });

  it("an empty chain is treated as needing the router", () => {
    // Nothing is known, so the conservative answer wins.
    expect(resolveClientTier([])).toBe("static");
  });
});

describe("parseRouteFacts", () => {
  it("detects the declaration forms a loader is written in", () => {
    expect(parseRouteFacts("export async function loader() {}").hasLoader).toBe(true);
    expect(parseRouteFacts("export const loader = async () => {};").hasLoader).toBe(true);
    expect(parseRouteFacts("export function loader() {}").hasLoader).toBe(true);
    expect(parseRouteFacts("const loader = () => {};\nexport { loader };").hasLoader).toBe(true);
    expect(parseRouteFacts("export { getData as loader };").hasLoader).toBe(true);
  });

  it("reports no loader for a page that has none", () => {
    expect(parseRouteFacts("export default function Page() { return null; }").hasLoader).toBe(false);
  });

  // Detection must not fire on an unrelated identifier that merely contains
  // the word, or every route would be marked as needing data.
  it("does not fire on a similarly named local", () => {
    expect(parseRouteFacts("const loaderStyles = {};\nexport default () => null;").hasLoader).toBe(false);
    expect(parseRouteFacts("export const loaderData = 1;").hasLoader).toBe(false);
  });

  // `action` feeds OpenAPI generation, which runs at boot against route
  // source — same detection family, same failure modes.
  it("detects the declaration forms an action is written in", () => {
    expect(parseRouteFacts("export async function action() {}").hasAction).toBe(true);
    expect(parseRouteFacts("export const action = async () => {};").hasAction).toBe(true);
    expect(parseRouteFacts("const action = () => {};\nexport { action };").hasAction).toBe(true);
    expect(parseRouteFacts("export default function Page() { return null; }").hasAction).toBe(false);
    expect(parseRouteFacts("export const actionable = 1;").hasAction).toBe(false);
  });

  // NA-04: the regex version recognized exactly one spelling per export and
  // reported every other valid one as ABSENT — including the spellings a
  // gated page plausibly uses. A false `hasMiddleware: false` lets the
  // static-serving fast path skip the route's gate entirely.
  it("does not miss middleware because it is not the first declarator", () => {
    const src = `export const marker = 1, middleware = [requireLogin];\nexport default Page;`;
    expect(parseRouteFacts(src).hasMiddleware).toBe(true);
  });

  it("detects a re-export list that carries middleware", () => {
    const src = `export { requireLogin as middleware } from "./guard";\nexport default Page;`;
    expect(parseRouteFacts(src).hasMiddleware).toBe(true);
  });

  it("treats an unresolved star re-export as potentially gated", () => {
    const src = `export * from "./guard";\nexport default Page;`;
    const facts = parseRouteFacts(src);
    expect(facts.hasMiddleware).toBe(true);
    expect(facts.hasLoader).toBe(true);
    expect(facts.hasAction).toBe(true);
  });

  it("a type-only star re-export does not imply runtime middleware", () => {
    const src = `export type * from "./guard";\nexport default Page;`;
    expect(parseRouteFacts(src).hasMiddleware).toBe(false);
  });

  it("type-only exports do not imply runtime middleware", () => {
    const src = `export type { Middleware } from "./guard";\nexport default Page;`;
    expect(parseRouteFacts(src).hasMiddleware).toBe(false);
  });

  it("detects middleware through destructured exports", () => {
    const src = `export const { middleware, other } = makeGuards();`;
    expect(parseRouteFacts(src).hasMiddleware).toBe(true);
  });

  it("detects a string-literal export name", () => {
    const src = `const guard = [];\nexport { guard as "middleware" };`;
    expect(parseRouteFacts(src).hasMiddleware).toBe(true);
  });

  it("an unparseable module is conservatively gated", () => {
    const facts = parseRouteFacts("export const {{{");
    expect(facts.hasMiddleware).toBe(true);
    expect(facts.hasLoader).toBe(true);
  });

  it("comments between tokens do not hide the export", () => {
    const src = `export const /* tricky */ middleware = [];`;
    expect(parseRouteFacts(src).hasMiddleware).toBe(true);
  });
});

describe("renderSpeculationRules", () => {
  it("prerenders same-origin links on intent, not eagerly", () => {
    const html = renderSpeculationRules();
    expect(html).toContain('type="speculationrules"');
    const json = JSON.parse(html.replace(/^<script[^>]*>/, "").replace(/<\/script>$/, ""));
    expect(json.prerender[0].eagerness).toBe("moderate");
    expect(json.prerender[0].where.and).toContainEqual({ href_matches: "/*" });
  });

  // Prerendering executes the target page, so the opt-out that governs the JS
  // prefetcher has to govern this too — one attribute, both mechanisms.
  it("excludes links marked as not prefetchable", () => {
    const html = renderSpeculationRules();
    expect(html).toContain("data-neutron-prefetch=false");
  });

  it("excludes downloads and new-tab links", () => {
    const html = renderSpeculationRules();
    expect(html).toContain("[download]");
    expect(html).toContain('target=');
  });

  it("takes caller-supplied exclusions", () => {
    const html = renderSpeculationRules({ exclude: ["/logout"] });
    const json = JSON.parse(html.replace(/^<script[^>]*>/, "").replace(/<\/script>$/, ""));
    expect(json.prerender[0].where.and).toContainEqual({ not: { href_matches: "/logout" } });
  });

  // Without the nonce a nonce-based CSP drops the tag and navigation silently
  // stops being instant — a failure with no error anywhere.
  it("carries a CSP nonce", () => {
    expect(renderSpeculationRules({}, "abc123")).toContain('nonce="abc123"');
  });

  it("cannot break out of the script element", () => {
    const html = renderSpeculationRules({ exclude: ["</script><script>alert(1)</script>"] });
    expect(html).not.toContain("</script><script>");
    expect(html.match(/<\/script>/g)).toHaveLength(1);
  });
});

describe("renderStaticLinkSpeculationRules", () => {
  // An app-tier document must not speculate app routes: prerendering one runs
  // its loaders and middleware for a page nobody asked for. Scoping to the
  // attribute the router applies is what prevents that.
  it("only speculates links the router marked as static", () => {
    const html = renderStaticLinkSpeculationRules();
    const json = JSON.parse(html.replace(/^<script[^>]*>/, "").replace(/<\/script>$/, ""));
    expect(json.prerender[0].where.and).toContainEqual({
      selector_matches: "[data-neutron-static]",
    });
    expect(json.prerender[0].eagerness).toBe("moderate");
  });

  it("still honours the per-link opt-out", () => {
    expect(renderStaticLinkSpeculationRules()).toContain("data-neutron-prefetch=false");
  });

  it("carries a CSP nonce", () => {
    expect(renderStaticLinkSpeculationRules(undefined, "n0nce")).toContain('nonce="n0nce"');
  });
});
