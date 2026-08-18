import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createServer, type NeutronServer } from "./index.js";

// FRAMEWORK_CONTRACT.md §2/§4 over real HTTP: an SSR app whose routes throw
// taxonomy problems, validate a POST body, and enable the OpenAPI surface.
// Route files import "@neutron-build/core" the way a real app does; the
// fixture lives inside the package so Node's self-reference resolves it to
// the built dist (build before test, as CI does).

async function getFreePort(): Promise<number> {
  return await new Promise((resolve, reject) => {
    const socket = net.createServer();
    socket.listen(0, "127.0.0.1", () => {
      const address = socket.address();
      if (!address || typeof address === "string") {
        reject(new Error("Failed to resolve free port"));
        return;
      }
      const { port } = address;
      socket.close((error) => (error ? reject(error) : resolve(port)));
    });
    socket.on("error", reject);
  });
}

async function writeFixtureApp(rootDir: string): Promise<void> {
  await fs.mkdir(path.join(rootDir, "src", "routes", "api"), { recursive: true });
  await fs.mkdir(path.join(rootDir, "src", "routes", "errors"), { recursive: true });

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "api", "items.ts"),
    `import { z, json, validateJsonBody } from "@neutron-build/core";

export const config = { mode: "app" };

const NewItem = z.object({
  name: z.string().min(1),
  price: z.number().gte(0),
});

export async function loader() {
  return json([{ id: 1, name: "fixture-item", price: 1 }]);
}

export async function action({ request }: { request: Request }) {
  const item = await validateJsonBody(request, NewItem);
  return json({ id: 1, ...item }, 201);
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "errors", "[code].ts"),
    `import { badRequest, conflict, forbidden, internalError, notFoundError, rateLimited, unauthorized } from "@neutron-build/core";

export const config = { mode: "app" };

const FORCERS: Record<string, () => never> = {
  "bad-request": () => { throw badRequest("forced bad-request"); },
  unauthorized: () => { throw unauthorized("forced unauthorized"); },
  forbidden: () => { throw forbidden("forced forbidden"); },
  "not-found": () => { throw notFoundError("forced not-found"); },
  conflict: () => { throw conflict("forced conflict"); },
  "rate-limited": () => { throw rateLimited("forced rate-limited"); },
  internal: () => { throw internalError("forced internal"); },
};

export async function loader({ params }: { params: Record<string, string> }) {
  const force = FORCERS[params.code];
  if (!force) {
    throw notFoundError("no forced error for " + params.code);
  }
  force();
}
`,
    "utf-8"
  );

  await fs.writeFile(
    path.join(rootDir, "src", "routes", "protected.ts"),
    `import { unauthorized } from "@neutron-build/core";

export const config = { mode: "app" };

export async function middleware(_request: Request, _context: unknown, next: () => Promise<Response>) {
  throw unauthorized("route middleware says no");
}

export async function loader() {
  return { secret: true };
}
`,
    "utf-8"
  );
}

describe("contract problems and OpenAPI (e2e)", () => {
  let running: NeutronServer | null = null;
  let root = "";

  afterEach(async () => {
    if (running) {
      await running.close();
      running = null;
    }
    if (root) {
      await fs.rm(root, { recursive: true, force: true });
      root = "";
    }
  });

  it("serves thrown taxonomy problems as RFC 7807 problem+json", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-problems-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      distDir: root,
      compress: false,
      openapi: { title: "Fixture API", version: "9.9.9" },
    });
    const base = `http://127.0.0.1:${port}`;

    const cases: Array<[string, number, string, string]> = [
      ["bad-request", 400, "bad-request", "Bad Request"],
      ["unauthorized", 401, "unauthorized", "Unauthorized"],
      ["forbidden", 403, "forbidden", "Forbidden"],
      ["not-found", 404, "not-found", "Not Found"],
      ["conflict", 409, "conflict", "Conflict"],
      ["rate-limited", 429, "rate-limited", "Rate Limited"],
      ["internal", 500, "internal", "Internal Server Error"],
    ];

    for (const [code, status, suffix, title] of cases) {
      const res = await fetch(`${base}/errors/${code}`);
      expect(res.status, code).toBe(status);
      expect(res.headers.get("content-type"), code).toContain("application/problem+json");
      const body = (await res.json()) as Record<string, unknown>;
      expect(body, code).toEqual({
        type: `https://neutron.dev/errors/${suffix}`,
        title,
        status,
        detail: `forced ${code}`,
        instance: `/errors/${code}`,
      });
      // Middleware ordering intact: the problem response still carries the id.
      expect(res.headers.get("x-request-id")).toBeTruthy();
    }
  });

  it("returns 422 problem+json with errors[] for an invalid POST body", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-problems-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      distDir: root,
      compress: false,
    });
    const base = `http://127.0.0.1:${port}`;

    const res = await fetch(`${base}/api/items`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "", price: -1 }),
    });
    expect(res.status).toBe(422);
    expect(res.headers.get("content-type")).toContain("application/problem+json");
    const body = (await res.json()) as {
      type: string;
      title: string;
      status: number;
      detail: string;
      errors: Array<{ field: string; message: string; value?: unknown }>;
    };
    expect(body.type).toBe("https://neutron.dev/errors/validation");
    expect(body.title).toBe("Validation Failed");
    expect(body.status).toBe(422);
    expect(body.detail).toBe("Request body failed validation");
    expect(body.errors.map((e) => e.field)).toEqual(["name", "price"]);
    expect(body.errors.every((e) => typeof e.message === "string")).toBe(true);

    // The happy path still works through the same action.
    const ok = await fetch(`${base}/api/items`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "x", price: 1 }),
    });
    expect(ok.status).toBe(201);
  });

  it("converts a ProblemError thrown from route middleware", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-problems-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      distDir: root,
      compress: false,
    });
    const base = `http://127.0.0.1:${port}`;

    const res = await fetch(`${base}/protected`);
    expect(res.status).toBe(401);
    expect(res.headers.get("content-type")).toContain("application/problem+json");
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.type).toBe("https://neutron.dev/errors/unauthorized");
    expect(body.detail).toBe("route middleware says no");
  });

  it("serves an OpenAPI 3.1 document and /docs from the route tree", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-problems-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      distDir: root,
      compress: false,
      openapi: { title: "Fixture API", version: "9.9.9" },
    });
    const base = `http://127.0.0.1:${port}`;

    const res = await fetch(`${base}/openapi.json`);
    expect(res.status).toBe(200);
    const spec = (await res.json()) as {
      openapi: string;
      info: Record<string, string>;
      paths: Record<string, Record<string, unknown>>;
      components: { schemas: Record<string, unknown> };
    };
    expect(spec.openapi).toBe("3.1.0");
    expect(spec.info).toEqual({ title: "Fixture API", version: "9.9.9" });
    expect(Object.keys(spec.paths["/api/items"]).sort()).toEqual(["get", "post"]);
    expect(Object.keys(spec.paths["/errors/{code}"])).toEqual(["get"]);
    expect(spec.paths["/protected"]).toBeDefined();
    expect(spec.components.schemas.ProblemDetail).toBeDefined();

    const docs = await fetch(`${base}/docs`);
    expect(docs.status).toBe(200);
    expect(docs.headers.get("content-type")).toContain("text/html");
    expect(await docs.text()).toContain("/openapi.json");
  });

  it("omits the spec surface when the openapi option is absent", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-problems-"));
    await writeFixtureApp(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      distDir: root,
      compress: false,
    });
    const base = `http://127.0.0.1:${port}`;
    expect((await fetch(`${base}/openapi.json`)).status).toBe(404);
    expect((await fetch(`${base}/docs`)).status).toBe(404);
  });
});
