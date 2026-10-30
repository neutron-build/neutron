import { describe, it, expect } from "vitest";
import { h } from "preact";
import { renderToString } from "preact-render-to-string";
import { Island } from "./island.js";

function Inner() {
  return h("span", null, "inner");
}

function Outer() {
  return h("div", null, h(Island, { component: Inner, client: "load" }));
}

describe("<Island>", () => {
  it("renders a hydration marker around a top-level island", () => {
    const html = renderToString(h(Island, { component: Inner, client: "load", __src: "/i.tsx" }));
    expect(html).toMatch(/^<neutron-island [^>]*data-src="\/i.tsx"[^>]*><span>inner<\/span><\/neutron-island>$/);
  });

  // An <Island> inside another island is rendered inline: the outer island
  // owns the subtree, and a second marker would be hydrated a second time.
  it("renders a nested island inline, without its own marker", () => {
    const html = renderToString(h(Island, { component: Outer, client: "load" }));
    expect(html.match(/<neutron-island/g)?.length).toBe(1);
    expect(html).toContain("<div><span>inner</span></div>");
  });
});
