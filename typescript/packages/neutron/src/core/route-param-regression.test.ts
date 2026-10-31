import { describe, it, expect } from "vitest";
import { createRouter } from "./router.js";
import { bindPathParams, parsePath } from "./route-path.js";
import { findNotFoundMatch } from "./manifest.js";
import { generateRouteTypesDeclaration } from "./route-typegen.js";
import type { Route } from "./types.js";

function page(path: string, id = path): Route {
  return {
    id: `route:${id}`,
    path,
    file: `/project/src/routes${id}/page.tsx`,
    params: [],
    config: { mode: "static" },
    hasLoader: false,
    hasMiddleware: false,
    hasAction: false,
    parentId: null,
    isLayout: false,
  };
}

function notFound(path: string, id = path): Route {
  return { ...page(path, id), isNotFound: true };
}

// NA-06: the trie shares one dynamic edge between `/users/:id` and
// `/users/:slug/edit`, and the shared edge used to carry the FIRST inserter's
// name — so the deeper route matched with the wrong parameter bound, and
// flipping the insertion order flipped which API broke.
describe("shared dynamic trie edges bind the matched route's parameter names", () => {
  const orders: string[][] = [
    ["/users/:id", "/users/:slug/edit"],
    ["/users/:slug/edit", "/users/:id"],
  ];

  for (const order of orders) {
    it(`binds :slug for /users/alice/edit (insertion ${order.join(" then ")})`, () => {
      const router = createRouter();
      for (const p of order) router.insert(page(p));

      const match = router.match("/users/alice/edit");
      expect(match).not.toBeNull();
      expect(match!.route.path).toBe("/users/:slug/edit");
      expect(match!.params).toEqual({ slug: "alice" });
    });

    it(`binds :id for /users/alice (insertion ${order.join(" then ")})`, () => {
      const router = createRouter();
      for (const p of order) router.insert(page(p));

      const match = router.match("/users/alice");
      expect(match).not.toBeNull();
      expect(match!.route.path).toBe("/users/:id");
      expect(match!.params).toEqual({ id: "alice" });
    });
  }

  it("two descendant branches with different names both bind correctly", () => {
    const router = createRouter();
    router.insert(page("/users/:id/posts/:postId"));
    router.insert(page("/users/:userId/comments/:commentId"));

    expect(router.match("/users/7/posts/9")!.params).toEqual({ id: "7", postId: "9" });
    expect(router.match("/users/7/comments/4")!.params).toEqual({ userId: "7", commentId: "4" });
  });

  it("suffixed and catch-all parameters keep their suffix semantics", () => {
    const router = createRouter();
    router.insert(page("/docs/*slug"));
    router.insert(page("/docs/*slug.md"));
    router.insert(page("/docs/:id.json"));

    // Wildcard suffix selection is unchanged: deeper .md wins over the bare
    // catch-all, and the param suffix binds its own name.
    const md = router.match("/docs/guide/intro.md");
    expect(md!.route.path).toBe("/docs/*slug.md");
    expect(md!.params).toEqual({ slug: "guide/intro" });

    const bare = router.match("/docs/guide/intro");
    expect(bare!.route.path).toBe("/docs/*slug");
    expect(bare!.params).toEqual({ slug: "guide/intro" });

    const json = router.match("/docs/7.json");
    expect(json!.route.path).toBe("/docs/:id.json");
    expect(json!.params).toEqual({ id: "7" });
  });

  it("static branches win over dynamic siblings and backtrack correctly", () => {
    const router = createRouter();
    router.insert(page("/settings/:section"));
    router.insert(page("/settings/profile/edit"));

    const match = router.match("/settings/profile/edit");
    expect(match!.route.path).toBe("/settings/profile/edit");
    const dyn = router.match("/settings/privacy");
    expect(dyn!.route.path).toBe("/settings/:section");
    expect(dyn!.params).toEqual({ section: "privacy" });
  });

  it("duplicate parameter names are rejected at insertion", () => {
    const router = createRouter();
    expect(() => router.insert(page("/a/:x/b/:x"))).toThrow(/Duplicate route parameter/);
  });

  it("params map has a null prototype", () => {
    const router = createRouter();
    router.insert(page("/w/:__proto__"));
    const match = router.match("/w/payload");
    expect(Object.getPrototypeOf(match!.params)).toBeNull();
    expect((match!.params as Record<string, string>)["__proto__"]).toBe("payload");
  });
});

// NA-07: a not-found page under a dynamic directory used to be unreachable —
// the scope `/org/:orgId` was compared as a literal prefix — and even a
// matched scope arrived with `params: {}`.
describe("dynamic not-found scopes match structurally and receive parameters", () => {
  const routes: Route[] = [
    page("/", "root"),
    notFound("/", "root-nf"),
    notFound("/org/:orgId", "org-nf"),
    notFound("/org/:orgId/settings/:section", "org-settings-nf"),
  ];

  it("binds orgId for a miss under /org/acme", () => {
    const found = findNotFoundMatch(routes, "/org/acme/missing");
    expect(found).not.toBeNull();
    expect(found!.route.path).toBe("/org/:orgId");
    expect(found!.params).toEqual({ orgId: "acme" });
  });

  it("a deeper scope wins over a shallower one", () => {
    const found = findNotFoundMatch(routes, "/org/acme/settings/x/missing");
    expect(found!.route.path).toBe("/org/:orgId/settings/:section");
    expect(found!.params).toEqual({ orgId: "acme", section: "x" });
  });

  it("root fallback still catches everything", () => {
    expect(findNotFoundMatch(routes, "/unrelated")!.route.path).toBe("/");
  });

  it("specificity is structural, not name-length based", () => {
    // Two same-shape scopes: the winner must not depend on parameter-name
    // length, and must not depend on the order candidates were discovered in.
    const a = notFound("/a/:x", "x-scope");
    const b = notFound("/a/:longerParameterName", "z-scope");
    const forward = findNotFoundMatch([a, b], "/a/whatever/missing");
    const reverse = findNotFoundMatch([b, a], "/a/whatever/missing");
    expect(forward!.route.id).toBe(reverse!.route.id);
    expect(forward!.route.id).not.toBe(
      "chosen-by-name-length:longerParameterName"
    );
  });

  it("a static scope beats a dynamic scope of the same depth", () => {
    const dyn = notFound("/admin/:section", "admin-dyn");
    const stat = notFound("/admin/billing", "admin-static");
    const found = findNotFoundMatch([dyn, stat], "/admin/billing/nope");
    expect(found!.route.id).toBe("route:admin-static");
  });

  it("matchNotFound returns the params through the router", () => {
    const router = createRouter();
    for (const r of routes) router.insert(r);
    const match = router.matchNotFound("/org/acme/missing");
    expect(match).not.toBeNull();
    expect(match!.params).toEqual({ orgId: "acme" });
    expect(match!.layouts.every((l) => l.isLayout)).toBe(true);
  });

  it("a not-found page does not make its scope navigable", () => {
    const router = createRouter();
    for (const r of routes) router.insert(r);
    expect(router.match("/org/acme")).toBeNull();
  });
});

describe("bindPathParams", () => {
  it("rejects non-matching static segments", () => {
    expect(bindPathParams("/a/b", ["a", "c"])).toBeNull();
  });

  it("requires exact consumption without prefix", () => {
    expect(bindPathParams("/a", ["a", "b"])).toBeNull();
    expect(bindPathParams("/a", ["a"])).toEqual({});
  });

  it("honors suffixes on plain params", () => {
    expect(bindPathParams("/x/:id.json", ["x", "7.json"])).toEqual({ id: "7" });
    expect(bindPathParams("/x/:id.json", ["x", "7"])).toBeNull();
  });
});

// NA-13: static paths with special characters must produce VALID literals,
// dynamic tokens keep their suffixes, and not-found pages are not advertised
// as navigable.
describe("route type declarations", () => {
  it("escapes special characters in static paths", () => {
    const decl = generateRouteTypesDeclaration([page('/say"hi')]);
    // The declaration must parse: assert the emitted literal is JSON-escaped.
    expect(decl).toContain(`| ${JSON.stringify('/say"hi')}`);
  });

  it("emits a parseable declaration for a pathological static route", () => {
    const decl = generateRouteTypesDeclaration([
      page('/q`uote$back\\slash'),
      page("/plain"),
    ]);
    expect(decl).toContain(JSON.stringify("/plain"));
    // Backtick/dollar/backslash in a STATIC path are inside a JSON string.
    expect(decl).toContain(JSON.stringify('/q`uote$back\\slash'));
  });

  it("keeps literal suffixes on dynamic tokens", () => {
    const decl = generateRouteTypesDeclaration([page("/docs/:id.json"), page("/files/*path.md")]);
    expect(decl).toContain("`/docs/" + "${string}" + ".json`");
    expect(decl).toContain("`/files/" + "${string}" + ".md`");
  });

  it("excludes not-found pages and layouts from the navigable set", () => {
    const decl = generateRouteTypesDeclaration([
      page("/real"),
      notFound("/", "nf"),
      { ...page("/", "layout"), isLayout: true },
    ]);
    expect(decl).toContain(JSON.stringify("/real"));
    expect(decl).not.toContain("| \"/\"");
  });

  it("root path renders as a plain string literal", () => {
    const decl = generateRouteTypesDeclaration([page("/")]);
    expect(decl).toContain('| "/"');
  });
});

// Sanity: the declaration we generate actually parses as TypeScript.
describe("generated declarations parse", () => {
  it("parses a pathological set", async () => {
    const { parse } = await import("@babel/parser");
    const decl = generateRouteTypesDeclaration([
      page('/say"hi'),
      page("/docs/:id.json"),
      page("/files/*path.md"),
      page("/"),
    ]);
    expect(() => parse(decl, { sourceType: "module", plugins: ["typescript"] })).not.toThrow();
  });
});
