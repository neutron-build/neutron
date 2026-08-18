import { describe, expect, it } from "vitest";
import type { Route } from "../core/types.js";
import {
  buildOpenApiSpec,
  routePathToOpenApiPath,
  swaggerDocsHtml,
} from "./openapi.js";

function route(overrides: Partial<Route> & { path: string }): Route {
  return {
    id: `route:${overrides.path}`,
    file: `/src/routes${overrides.path}.tsx`,
    pattern: /^$/,
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

describe("swaggerDocsHtml", () => {
  it("points Swagger UI at /openapi.json and escapes the title", () => {
    const html = swaggerDocsHtml('Test <script>"API"');
    expect(html).toContain('url: "/openapi.json"');
    expect(html).toContain("Test &lt;script&gt;&quot;API&quot;");
    expect(html).not.toContain("<script>Test");
  });
});
