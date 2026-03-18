import { describe, it, expect } from "vitest";
import { createRouter } from "../core/router.js";
import type { Route } from "../core/types.js";

function createTestRoute(path: string, id?: string): Route {
  const segments = path.split("/").filter(Boolean);
  const params: string[] = [];
  let patternStr = "^";

  for (const seg of segments) {
    if (seg === "*") {
      params.push("*");
      patternStr += "/(.*)";
    } else if (seg.startsWith(":")) {
      params.push(seg.slice(1));
      patternStr += "/([^/]+)";
    } else {
      patternStr += "/" + seg.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    }
  }
  patternStr += "$";

  return {
    id: id || `route:${path}`,
    path,
    file: `/src/routes${path === "/" ? "/index" : path}.tsx`,
    pattern: new RegExp(patternStr || "^/$"),
    params,
    config: { mode: "static" },
    parentId: null,
  };
}

describe("router", () => {
  it("matches static routes", () => {
    const router = createRouter();
    router.insert(createTestRoute("/about"));
    router.insert(createTestRoute("/"));

    expect(router.match("/")?.route.path).toBe("/");
    expect(router.match("/about")?.route.path).toBe("/about");
    expect(router.match("/notfound")).toBeNull();
  });

  it("matches dynamic params", () => {
    const router = createRouter();
    router.insert(createTestRoute("/users/:id"));

    const result = router.match("/users/123");
    expect(result?.route.path).toBe("/users/:id");
    expect(result?.params.id).toBe("123");
  });

  it("matches catch-all routes", () => {
    const router = createRouter();
    router.insert(createTestRoute("/docs/*"));

    const result = router.match("/docs/guide/getting-started");
    expect(result?.route.path).toBe("/docs/*");
    expect(result?.params["*"]).toBe("guide/getting-started");
  });

  it("prioritizes static over dynamic", () => {
    const router = createRouter();
    router.insert(createTestRoute("/users/:id"));
    router.insert(createTestRoute("/users/me"));

    expect(router.match("/users/me")?.route.path).toBe("/users/me");
    expect(router.match("/users/123")?.route.path).toBe("/users/:id");
  });

  it("prioritizes dynamic over catch-all", () => {
    const router = createRouter();
    router.insert(createTestRoute("/docs/*"));
    router.insert(createTestRoute("/docs/:id"));

    expect(router.match("/docs/intro")?.route.path).toBe("/docs/:id");
    expect(router.match("/docs/guide/intro")?.route.path).toBe("/docs/*");
  });

  it("returns all routes", () => {
    const router = createRouter();
    router.insert(createTestRoute("/"));
    router.insert(createTestRoute("/about"));
    router.insert(createTestRoute("/users/:id"));

    expect(router.getRoutes()).toHaveLength(3);
  });

  it("uses correct param name when two routes share trie node with different param names", () => {
    const router = createRouter();
    router.insert(createTestRoute("/users/:userId"));
    router.insert(createTestRoute("/posts/:postId"));

    const userResult = router.match("/users/42");
    expect(userResult?.params.userId).toBe("42");

    const postResult = router.match("/posts/99");
    expect(postResult?.params.postId).toBe("99");
  });

  it("builds layout chain in root-to-child order", () => {
    const router = createRouter();
    const rootLayout: Route = {
      id: "layout:root",
      path: "/",
      file: "/src/routes/_layout.tsx",
      pattern: /^\/$/,
      params: [],
      config: { mode: "static" },
      parentId: null,
    };
    const childRoute: Route = {
      id: "route:/about",
      path: "/about",
      file: "/src/routes/about.tsx",
      pattern: /^\/about$/,
      params: [],
      config: { mode: "static" },
      parentId: "layout:root",
    };
    router.insert(rootLayout);
    router.insert(childRoute);

    const result = router.match("/about");
    expect(result?.layouts).toHaveLength(1);
    expect(result?.layouts[0].id).toBe("layout:root");
  });
});
