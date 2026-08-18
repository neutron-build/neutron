/**
 * OpenAPI 3.1 spec generation (FRAMEWORK_CONTRACT.md §4).
 *
 * The spec is generated from the discovered route table, the same way the
 * client build derives its facts: statically, from route source. TS loaders
 * and actions return untyped runtime values, so request/response schemas
 * cannot be inferred the way Go and Python infer them from handler types —
 * per-operation enrichment (request bodies, response schemas, summaries)
 * is merged in via the `paths`/`components` options on the server's `openapi`
 * setting, which override the generated operations key-by-key.
 */
import { escapeHtml } from "../core/escape.js";
import type { Route } from "../core/types.js";

export interface NeutronOpenApiOptions {
  /** Spec `info.title`; also shown on the /docs page. */
  title: string;
  /** Spec `info.version`. Defaults to the server's `version` option. */
  version?: string;
  description?: string;
  /**
   * Path-item fragments merged over the generated ones (per operation key),
   * e.g. `{ "/api/items": { post: { requestBody: {...} } } }`. Supplied
   * operations win over generated ones.
   */
  paths?: Record<string, Record<string, unknown>>;
  /** Merged over the generated `components` (schemas are merged by name). */
  components?: Record<string, unknown>;
}

/** Shared RFC 7807 error schema — same shape as the Go and Python SDKs' specs. */
const PROBLEM_DETAIL_SCHEMA = {
  type: "object",
  properties: {
    type: { type: "string" },
    title: { type: "string" },
    status: { type: "integer" },
    detail: { type: "string" },
    instance: { type: "string" },
    errors: {
      type: "array",
      items: {
        type: "object",
        properties: {
          field: { type: "string" },
          message: { type: "string" },
        },
      },
    },
  },
  required: ["type", "title", "status", "detail"],
};

function problemResponse(): Record<string, unknown> {
  return {
    description: "RFC 7807 problem",
    content: {
      "application/problem+json": {
        schema: { $ref: "#/components/schemas/ProblemDetail" },
      },
    },
  };
}

/** `:id` and `*slug` (with optional literal suffix) → `{id}` / `{slug}`. */
export function routePathToOpenApiPath(routePath: string): string {
  const segments = routePath.split("/").map((segment) => {
    if (segment.startsWith(":") || segment.startsWith("*")) {
      const rest = segment.slice(1);
      const dot = rest.indexOf(".");
      if (dot === -1) return `{${rest}}`;
      return `{${rest.slice(0, dot)}}${rest.slice(dot)}`;
    }
    return segment;
  });
  return segments.join("/");
}

/**
 * Build the spec. `routes` is the discovered table; only live app-mode routes
 * are documented — static pages are prerendered documents, not API surface.
 */
export function buildOpenApiSpec(
  routes: Route[],
  options: NeutronOpenApiOptions
): Record<string, unknown> {
  const paths: Record<string, Record<string, unknown>> = {};

  for (const route of routes) {
    if (route.isLayout || route.isNotFound) continue;
    if (route.config.mode !== "app") continue;

    const pathItem: Record<string, unknown> = {};
    const parameters = route.params.map((name) => ({
      name,
      in: "path",
      required: true,
      schema: { type: "string" },
    }));

    if (route.hasLoader) {
      pathItem.get = { parameters, responses: { default: problemResponse() } };
    }
    if (route.hasAction) {
      pathItem.post = { parameters, responses: { default: problemResponse() } };
    }
    if (Object.keys(pathItem).length === 0) continue;

    paths[routePathToOpenApiPath(route.path)] = pathItem;
  }

  // Operation-level merge: a user-supplied operation replaces the generated
  // one at the same key, while generated siblings survive (a user POST for
  // /api/items does not erase the generated GET).
  for (const [path, userItem] of Object.entries(options.paths ?? {})) {
    const existing = paths[path];
    if (existing) {
      Object.assign(existing, userItem);
    } else {
      paths[path] = userItem;
    }
  }

  const info: Record<string, unknown> = {
    title: options.title,
    version: options.version,
  };
  if (options.description !== undefined) {
    info.description = options.description;
  }

  const components: Record<string, unknown> = {
    schemas: { ProblemDetail: PROBLEM_DETAIL_SCHEMA },
  };
  if (options.components) {
    for (const [key, value] of Object.entries(options.components)) {
      if (key === "schemas" && value && typeof value === "object") {
        components.schemas = { ...(components.schemas as object), ...value };
      } else {
        components[key] = value;
      }
    }
  }

  return {
    openapi: "3.1.0",
    info,
    paths,
    components,
  };
}

/** Minimal Swagger UI page pointing at /openapi.json, as the Python SDK serves. */
export function swaggerDocsHtml(title: string): string {
  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>${escapeHtml(title)} — API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>SwaggerUIBundle({url: "/openapi.json", dom_id: "#swagger-ui"})</script>
</body>
</html>`;
}
