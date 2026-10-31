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
 *
 * Resource routes are documented from the same table: one operation per
 * exported method handler (`route.resourceMethods`, derived at discovery —
 * the generator cannot import the module). Bodies are generic object
 * placeholders for the same untyped-handler reason.
 */
import { escapeHtml } from "../core/escape.js";
import type { Route } from "../core/types.js";
import {
  isResourceRoute,
  RESOURCE_ROUTE_METHODS,
  type ResourceRouteMethod,
} from "../core/resource-routes.js";

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

/** Methods whose handlers conventionally read a request body. */
const METHODS_WITH_REQUEST_BODY = new Set<ResourceRouteMethod>(["POST", "PUT", "PATCH"]);

/** Generic body placeholder — handlers are untyped at the route-table level. */
function genericBodySchema(): Record<string, unknown> {
  return { type: "object", additionalProperties: true };
}

/**
 * Deterministic operation id from method + (template) path:
 * `GET /api/users/:id` → `get-api-users-by-id`, `POST /rss.xml` →
 * `post-rss-xml`. Param segments contribute `by-<name>`; anything the URL
 * allows but an operation id shouldn't (dots, suffixes) collapses to `-`.
 */
export function resourceRouteOperationId(method: string, openApiPath: string): string {
  const segments = openApiPath
    .split("/")
    .filter(Boolean)
    .map((segment) => {
      const param = /^\{([^}]+)\}(.*)$/.exec(segment);
      if (param) return `by-${param[1]}${param[2]}`;
      return segment;
    });
  return [method.toLowerCase(), ...segments]
    .join("-")
    .replace(/[^a-zA-Z0-9-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

/**
 * One operation for one exported method handler. Deliberately minimal and
 * honest: the generator reads the route table (it cannot import the module),
 * and TS handlers are untyped, so bodies are generic object placeholders —
 * `summary`/`requestBody`/response schemas can be enriched per operation via
 * the `paths` option, which replaces the generated operation key-by-key.
 */
function resourceRouteOperation(
  route: Route,
  method: ResourceRouteMethod,
  parameters: Array<Record<string, unknown>>
): Record<string, unknown> {
  const operation: Record<string, unknown> = {
    operationId: resourceRouteOperationId(method, routePathToOpenApiPath(route.path)),
    summary: `${method} ${route.path} (resource route)`,
    parameters,
    responses: {
      "200": {
        description:
          "The handler's response. Placeholder: a handler may return any Response; " +
          "a non-Response return is serialized as JSON 200.",
        content: { "application/json": { schema: genericBodySchema() } },
      },
      default: problemResponse(),
    },
  };
  if (METHODS_WITH_REQUEST_BODY.has(method)) {
    operation.requestBody = {
      required: false,
      content: { "application/json": { schema: genericBodySchema() } },
    };
  }
  return operation;
}

/**
 * Build the spec. `routes` is the discovered table; only live app-mode routes
 * are documented — static pages are prerendered documents, not API surface.
 * Resource routes are the exception to the mode filter: they are endpoints in
 * every mode (never prerendered; the server dispatches them ahead of any
 * static path), so a resource route is documented whatever its `config.mode`.
 */
export function buildOpenApiSpec(
  routes: Route[],
  options: NeutronOpenApiOptions
): Record<string, unknown> {
  const paths: Record<string, Record<string, unknown>> = {};

  for (const route of routes) {
    if (route.isLayout || route.isNotFound) continue;

    const parameters = route.params.map((name) => ({
      name,
      in: "path",
      required: true,
      schema: { type: "string" },
    }));

    if (isResourceRoute(route)) {
      // One operation per exported method handler, from the route-table
      // facts. No facts (`resourceMethods` absent) → no fabricated
      // operations: an undocumented endpoint beats an invented one.
      const methods = (route.resourceMethods ?? []).filter((method) =>
        (RESOURCE_ROUTE_METHODS as readonly string[]).includes(method)
      );
      const pathItem: Record<string, unknown> = {};
      for (const method of methods) {
        pathItem[method.toLowerCase()] = resourceRouteOperation(route, method, parameters);
      }
      if (Object.keys(pathItem).length > 0) {
        paths[routePathToOpenApiPath(route.path)] = pathItem;
      }
      continue;
    }

    if (route.config.mode !== "app") continue;

    const pathItem: Record<string, unknown> = {};

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

/**
 * The document the server serves at /openapi.json for its `openapi` setting.
 * Shared by `start` and the dev server so both serve the same spec.
 */
export function serverOpenApiSpec(
  routes: Route[],
  openapi: NeutronOpenApiOptions,
  serverVersion: string
): Record<string, unknown> {
  return buildOpenApiSpec(routes, {
    title: openapi.title,
    version: openapi.version ?? serverVersion,
    description: openapi.description,
    paths: openapi.paths,
    components: openapi.components,
  });
}

/**
 * True when the app defines its own /openapi.json or /docs route. The
 * built-ins are then both suppressed so the user route wins, as with /health.
 */
export function appDefinesSpecRoute(routes: readonly Route[]): boolean {
  return routes.some(
    (route) =>
      (route.path === "/openapi.json" || route.path === "/docs") &&
      !route.file.includes("_layout")
  );
}
