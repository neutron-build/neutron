import { describe, it, expect } from "vitest";
import { h } from "preact";
import { useState, useEffect, useRef } from "preact/hooks";
import { renderToString } from "preact-render-to-string";

// Regression guard for the "Cannot read properties of undefined (reading '__H')"
// crash. It appears when `preact/hooks` and `preact-render-to-string` resolve to
// two different physical preact instances during SSR: the hooks dispatcher (set
// via options.__r) is never bridged, so useState/useEffect run with an undefined
// current component and `r.__H` throws.
//
// Every real Neutron SSR path forces a single instance
// (neutron-cli build.ts / dev.ts via resolve.dedupe + ssr.noExternal, plugin.ts
// via ssrLoadModule, edge/worker via alias + noExternal). This test proves hooks
// actually dispatch under renderToString so a future regression that re-splits
// the instance fails loudly here instead of only at app build time.
describe("SSR with hooks (dual-preact-instance regression)", () => {
  it("renders a useState component without crashing", () => {
    const Counter = () => {
      const [n] = useState(7);
      return h("button", null, `count: ${n}`);
    };
    const html = renderToString(h(Counter, {}));
    expect(html).toContain("count: 7");
  });

  it("renders useEffect + useRef components without a dispatcher crash", () => {
    const WithHooks = () => {
      const ref = useRef<number>(3);
      const [label] = useState("ok");
      useEffect(() => {}, []); // no-op in SSR, but must not throw
      return h("div", { "data-x": String(ref.current) }, label);
    };
    const html = renderToString(h(WithHooks, {}));
    expect(html).toContain("ok");
    expect(html).toContain('data-x="3"');
  });

  it("renders a hook component nested inside a layout wrapper", () => {
    // Mirrors the reported repro: a layout rendering a useState component.
    const Toggle = () => {
      const [x] = useState(0);
      return h("button", null, x);
    };
    const Layout = ({ children }: { children: unknown }) =>
      h("div", { id: "app" }, h(Toggle, {}), children as never);
    const html = renderToString(
      h(Layout, { children: h("main", null, "page") })
    );
    expect(html).toContain("<button>0</button>");
    expect(html).toContain("<main>page</main>");
  });
});
