import { describe, expect, it } from "vitest";
import type { Route } from "../core/types.js";
import {
  buildOpenApiSpec,
  resourceRouteOperationId,
  routePathToOpenApiPath,
  swaggerDocsHtml,
} from "./openapi.js";

function route(overrides: Partial<Route> & { path: string }): Route {
  return {
    id: `route:${overrides.path}`,
    file: `/src/routes${overrides.path}.tsx`,
    params: [],
    config: { mode: "app" },
    hasLoader: false,
    hasAction: false,
    parentId: null,
    ...overrides,
  };
}

describe("routePathToOpenApiPath", () => {
  it("converts :param and *splat segments to OpenAPI templates", () => {
    expect(routePathToOpenApiPath("/api/items")).toBe("/api/items");
    expect(routePathToOpenApiPath("/api/users/:id")).toBe("/api/users/{id}");
    expect(routePathToOpenApiPath("/docs/*slug")).toBe("/docs/{slug}");
    expect(routePathToOpenApiPath("/docs/*slug.md")).toBe("/docs/{slug}.md");
  });
});

describe("buildOpenApiSpec", () => {
  it("declares OpenAPI 3.1 with info from the options", () => {
    const spec = buildOpenApiSpec([], { title: "Test API", version: "1.2.3" });
    expect(spec.openapi).toBe("3.1.0");
    expect(spec.info).toEqual({ title: "Test API", version: "1.2.3" });
    expect(spec.paths).toEqual({});
  });

  it("documents GET per loader, POST per action, with path params", () => {
    const spec = buildOpenApiSpec(
      [
        route({ path: "/api/items", hasLoader: true, hasAction: true }),
        route({ path: "/api/users/:id", hasLoader: true, params: ["id"] }),
        route({ path: "/health", hasLoader: true, config: { mode: "static" } }),
        route({ path: "/hidden", hasLoader: true, hasAction: true, isLayout: true }),
        route({ path: "/gone", hasLoader: true, isNotFound: true }),
        route({ path: "/page-only", hasLoader: false, hasAction: false }),
      ],
      { title: "Test API", version: "9.9.9" }
    );

    const paths = spec.paths as Record<string, Record<string, unknown>>;
    expect(Object.keys(paths).sort()).toEqual(["/api/items", "/api/users/{id}"]);
    expect(Object.keys(paths["/api/items"]).sort()).toEqual(["get", "post"]);
    expect(Object.keys(paths["/api/users/{id}"])).toEqual(["get"]);

    const get = paths["/api/users/{id}"].get as {
      parameters: Array<{ name: string; in: string; required: boolean }>;
      responses: Record<string, unknown>;
    };
    expect(get.parameters).toEqual([
      { name: "id", in: "path", required: true, schema: { type: "string" } },
    ]);
    // Contract §4: error responses reference the shared RFC 7807 schema.
    expect(get.responses.default).toEqual({
      description: "RFC 7807 problem",
      content: {
        "application/problem+json": {
          schema: { $ref: "#/components/schemas/ProblemDetail" },
        },
      },
    });
  });

  it("references a ProblemDetail schema with the §2 required fields", () => {
    const spec = buildOpenApiSpec([route({ path: "/api/items", hasLoader: true })], {
      title: "T",
      version: "1",
    });
    const schemas = (spec.components as { schemas: Record<string, unknown> }).schemas;
    expect(schemas.ProblemDetail).toMatchObject({
      required: ["type", "title", "status", "detail"],
    });
  });

  it("merges user paths over generated ones and schemas by name", () => {
    const spec = buildOpenApiSpec([route({ path: "/api/items", hasLoader: true })], {
      title: "T",
      version: "1",
      paths: {
        "/api/items": {
          post: {
            requestBody: {
              content: { "application/json": { schema: { type: "object" } } },
            },
          },
        },
      },
      components: {
        schemas: { Item: { type: "object" } },
        securitySchemes: { bearer: { type: "http", scheme: "bearer" } },
      },
    });

    const paths = spec.paths as Record<string, Record<string, unknown>>;
    // User POST is added alongside the generated GET.
    expect(Object.keys(paths["/api/items"]).sort()).toEqual(["get", "post"]);
    expect(paths["/api/items"].post).toEqual({
      requestBody: { content: { "application/json": { schema: { type: "object" } } } },
    });

    const components = spec.components as {
      schemas: Record<string, unknown>;
      securitySchemes: Record<string, unknown>;
    };
    expect(Object.keys(components.schemas).sort()).toEqual(["Item", "ProblemDetail"]);
    expect(components.securitySchemes).toEqual({ bearer: { type: "http", scheme: "bearer" } });
  });
});

describe("buildOpenApiSpec: resource routes", () => {
  it("documents one operation per exported method handler, with path params, in every mode", () => {
    const spec = buildOpenApiSpec(
      [
        // config.mode defaults to "static" for handler-only modules; they are
        // endpoints in every mode (never prerendered), so the mode filter
        // that applies to pages must not drop them.
        route({
          path: "/api/users/:id",
          params: ["id"],
          config: { mode: "static" },
          hasMethodHandlers: true,
          hasDefaultExport: false,
          resourceMethods: ["GET", "POST"],
        }),
        route({
          path: "/rss.xml",
          hasMethodHandlers: true,
          hasDefaultExport: false,
          resourceMethods: ["GET"],
        }),
        // Same path shape as a page: layouts/not-found never documented.
        route({
          path: "/api/gone",
          hasMethodHandlers: true,
          hasDefaultExport: false,
          resourceMethods: ["GET"],
          isNotFound: true,
        }),
      ],
      { title: "Test API", version: "9.9.9" }
    );

    const paths = spec.paths as Record<string, Record<string, any>>;
    expect(Object.keys(paths).sort()).toEqual(["/api/users/{id}", "/rss.xml"]);

    const item = paths["/api/users/{id}"];
    expect(Object.keys(item).sort()).toEqual(["get", "post"]);

    const get = item.get;
    expect(get.operationId).toBe("get-api-users-by-id");
    expect(get.parameters).toEqual([
      { name: "id", in: "path", required: true, schema: { type: "string" } },
    ]);
    // Minimal honest body: a generic object placeholder, no fabricated schema.
    expect(get.responses["200"].content["application/json"].schema).toEqual({
      type: "object",
      additionalProperties: true,
    });
    expect(get.responses.default).toEqual({
      description: "RFC 7807 problem",
      content: {
        "application/problem+json": {
          schema: { $ref: "#/components/schemas/ProblemDetail" },
        },
      },
    });
    // GET carries no request body — nothing declared one.
    expect(get.requestBody).toBeUndefined();

    const post = item.post;
    expect(post.operationId).toBe("post-api-users-by-id");
    expect(post.requestBody).toEqual({
      required: false,
      content: { "application/json": { schema: { type: "object", additionalProperties: true } } },
    });

    expect(paths["/rss.xml"].get.operationId).toBe("get-rss-xml");
  });

  it("fabricates no operations when method facts are unavailable", () => {
    // A synthesized route table that classifies a resource route without
    // listing its methods: nothing is invented — an undocumented endpoint
    // beats an invented one.
    const spec = buildOpenApiSpec(
      [
        route({
          path: "/api/ghost",
          hasMethodHandlers: true,
          hasDefaultExport: false,
        }),
      ],
      { title: "T", version: "1" }
    );
    expect(spec.paths).toEqual({});
  });

  it("leaves page-route documentation unchanged", () => {
    const spec = buildOpenApiSpec(
      [
        // A page with a loader stays the loader-shaped GET operation — no
        // operationId, no body placeholders — even if method-ish facts exist.
        route({ path: "/item", hasLoader: true, hasAction: true }),
        // Static pages stay undocumented (prerendered, not API surface).
        route({ path: "/about", hasLoader: true, config: { mode: "static" } }),
      ],
      { title: "T", version: "1" }
    );

    const paths = spec.paths as Record<string, Record<string, any>>;
    expect(Object.keys(paths)).toEqual(["/item"]);
    expect(Object.keys(paths["/item"]).sort()).toEqual(["get", "post"]);
    expect(paths["/item"].get.operationId).toBeUndefined();
    expect(paths["/item"].get.requestBody).toBeUndefined();
    expect(paths["/item"].get.responses).toEqual({
      default: {
        description: "RFC 7807 problem",
        content: {
          "application/problem+json": {
            schema: { $ref: "#/components/schemas/ProblemDetail" },
          },
        },
      },
    });
  });

  it("lets a user-supplied operation replace a generated resource one", () => {
    const spec = buildOpenApiSpec(
      [
        route({
          path: "/rss.xml",
          hasMethodHandlers: true,
          hasDefaultExport: false,
          resourceMethods: ["GET"],
        }),
      ],
      {
        title: "T",
        version: "1",
        paths: {
          "/rss.xml": {
            get: {
              responses: {
                "200": {
                  description: "RSS 2.0 feed",
                  content: { "application/xml": { schema: { type: "string" } } },
                },
              },
            },
          },
        },
      }
    );

    const item = (spec.paths as Record<string, Record<string, any>>)["/rss.xml"];
    expect(item.get.responses["200"].description).toBe("RSS 2.0 feed");
  });
});

describe("resourceRouteOperationId", () => {
  it("derives a deterministic id from method + path template", () => {
    expect(resourceRouteOperationId("GET", "/api/time")).toBe("get-api-time");
    expect(resourceRouteOperationId("POST", "/rss.xml")).toBe("post-rss-xml");
    expect(resourceRouteOperationId("GET", "/api/users/{id}")).toBe("get-api-users-by-id");
    expect(resourceRouteOperationId("GET", "/docs/{slug}.md")).toBe("get-docs-by-slug-md");
  });
});

describe("swaggerDocsHtml", () => {
  it("points Swagger UI at /openapi.json and escapes the title", () => {
    const html = swaggerDocsHtml('Test <script>"API"');
    expect(html).toContain('url: "/openapi.json"');
    expect(html).toContain("Test &lt;script&gt;&quot;API&quot;");
    expect(html).not.toContain("<script>Test");
  });
});
