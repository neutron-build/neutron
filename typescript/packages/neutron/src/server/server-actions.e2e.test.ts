import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as net from "node:net";
import { afterAll, describe, expect, it, vi } from "vitest";
import { stringify } from "devalue";
import { createServer as createViteServer } from "vite";
import { createServer, type NeutronErrorEvent } from "./index.js";
import { neutronPlugin } from "../vite/plugin.js";
import { ServerActionRegistry, serverActionId } from "../vite/server-actions.js";
import { resolvePreactSsr, vitePreactAliases } from "../core/preact-ssr.js";
import { callServerAction, ServerActionError } from "../client/server-actions.js";
import { SERVER_ACTIONS_MANIFEST } from "../core/server-action-protocol.js";

/**
 * Server Actions end to end: a "use server" module imported by client code
 * is invoked through POST /_neutron/action — in the dev server (registered
 * when Vite transforms it for the client) and in createServer on the Vite
 * runtime (registered by the build's manifest). The client half is the real
 * one, with fetch pointed at the server under test.
 */

const closers: Array<() => Promise<void>> = [];
const roots: string[] = [];

afterAll(async () => {
  await Promise.all(closers.map((close) => close()));
  for (const root of roots) {
    await fs.rm(root, { recursive: true, force: true });
  }
});

async function getFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const socket = net.createServer();
    socket.listen(0, "127.0.0.1", () => {
      const address = socket.address();
      if (!address || typeof address === "string") {
        reject(new Error("Failed to resolve test port"));
        return;
      }
      const { port } = address;
      socket.close((error) => (error ? reject(error) : resolve(port)));
    });
    socket.on("error", reject);
  });
}

const ACTIONS = "src/actions.ts";
const ACTION_NAMES = ["echo", "submit", "fail", "crash", "go", "touch", "whoami", "badResult", "plain"];

const FIXTURE: Record<string, string> = {
  [ACTIONS]: `"use server";
import { ProblemError, redirect, getActionContext } from "@neutron-build/core";

export async function echo(value: unknown) {
  return { value, at: new Date(0), map: new Map([["k", 10n]]), set: new Set(["s"]), none: undefined };
}
export async function submit(label: string, form: FormData) {
  const file = form.get("file") as File | null;
  return { label, name: form.get("name"), file: file ? { name: file.name, text: await file.text() } : null };
}
export async function fail() {
  throw new ProblemError(409, "conflict", "Conflict", "already exists");
}
export async function crash() {
  throw new Error("secret-db-password");
}
export async function go() {
  throw redirect("/done?from=action");
}
export async function touch() {
  const { responseHeaders } = getActionContext();
  responseHeaders.set("x-neutron-invalidate", "/todos");
  responseHeaders.append("set-cookie", "a=1; Path=/");
  responseHeaders.append("set-cookie", "b=2; Path=/");
  return "touched";
}
export async function whoami() {
  return getActionContext().context.user ?? null;
}
export async function badResult() {
  return { fn: () => 1 };
}
export const plain = async () => "plain";
`,
  "src/middleware.ts": `
export default async function middleware(request, context, next) {
  if (request.headers.get("x-deny") === "1") return new Response("gated", { status: 401 });
  context.user = request.headers.get("x-user");
  const response = await next();
  response.headers.set("x-global", "ran");
  return response;
}
`,
  // A server-side import of the action module gets the real function.
  "src/routes/index.tsx": `
import { h } from "preact";
import { plain } from "../actions";
export const config = { mode: "app" };
export async function loader() { return { direct: await plain() }; }
export default function Page({ data }) { return h("p", null, "direct=" + data.direct); }
`,
};

async function writeFixture(prefix: string): Promise<string> {
  // Inside the package dir so the Vite SSR runtime resolves node_modules and
  // "@neutron-build/core" self-references this package.
  const root = await fs.mkdtemp(path.join(process.cwd(), prefix));
  roots.push(root);
  for (const [file, body] of Object.entries(FIXTURE)) {
    await fs.mkdir(path.dirname(path.join(root, file)), { recursive: true });
    await fs.writeFile(path.join(root, file), body, "utf-8");
  }
  return root;
}

const id = (name: string) => serverActionId(ACTIONS, name);

interface Booted {
  base: string;
  errors: NeutronErrorEvent[];
}

async function bootStart(root: string): Promise<Booted> {
  // What `neutron-ts build` writes for a client graph that reached the module.
  const registry = new ServerActionRegistry();
  registry.set(ACTIONS, ACTION_NAMES);
  await fs.mkdir(path.join(root, "dist"), { recursive: true });
  await fs.writeFile(path.join(root, "dist", SERVER_ACTIONS_MANIFEST), JSON.stringify(registry.toManifest()));
  const errors: NeutronErrorEvent[] = [];
  const port = await getFreePort();
  const running = await createServer({
    host: "127.0.0.1",
    port,
    rootDir: root,
    compress: false,
    serverBundle: false,
    serverActions: { bodySizeLimit: 4096 },
    hooks: { onError: (event) => void errors.push(event) },
  });
  closers.push(running.close);
  expect(running.moduleSource).toBe("vite");
  return { base: `http://127.0.0.1:${port}`, errors };
}

async function bootDev(root: string): Promise<Booted> {
  const port = await getFreePort();
  const preactSsr = resolvePreactSsr(root);
  const vite = await createViteServer({
    configFile: false,
    root,
    logLevel: "silent",
    plugins: [
      neutronPlugin({
        routesDir: path.join(root, "src", "routes"),
        rootDir: root,
        serverActions: { bodySizeLimit: 4096 },
      }),
    ],
    resolve: { alias: vitePreactAliases(preactSsr) },
    ssr: { noExternal: preactSsr.noExternal },
    server: { host: "127.0.0.1", port, strictPort: true, hmr: false },
  });
  await vite.listen();
  closers.push(() => vite.close());
  const base = `http://127.0.0.1:${port}`;
  // Nothing is callable until the client graph has seen the module.
  const before = await rawCall(base, id("plain"), []);
  expect(before.status).toBe(404);
  const stub = await (await fetch(`${base}/${ACTIONS}`)).text();
  expect(stub).toContain("createServerActionReference");
  expect(stub).not.toContain("secret-db-password");
  return { base, errors: [] };
}

function rawCall(
  base: string,
  actionId: string,
  args: unknown[],
  headers: Record<string, string> = {},
  body?: BodyInit
): Promise<Response> {
  return fetch(`${base}/_neutron/action`, {
    method: "POST",
    headers: {
      "X-Neutron-Action": actionId,
      "Content-Type": "application/json",
      "X-Neutron-Data": "true",
      ...headers,
    },
    body: body ?? stringify(args),
    redirect: "manual",
  });
}

/** The real client stub, with its relative fetch aimed at `base`. */
async function clientCall(base: string, name: string, args: unknown[]): Promise<unknown> {
  const realFetch = globalThis.fetch;
  vi.stubGlobal("fetch", (input: RequestInfo | URL, init?: RequestInit) =>
    realFetch(typeof input === "string" && input.startsWith("/") ? base + input : input, init)
  );
  try {
    return await callServerAction(id(name), name, args);
  } finally {
    vi.unstubAllGlobals();
  }
}

async function expectContract(booted: Booted, mode: "dev" | "start"): Promise<void> {
  const { base } = booted;

  // Plain server-side call: the route's loader imported the real module.
  const page = await fetch(base + "/", { headers: { Accept: "text/html" } });
  expect(await page.text()).toContain("direct=plain");

  // Serializer round trip through the real client.
  expect(await clientCall(base, "echo", [{ when: new Date(5), tags: new Set(["a"]), n: 2n }])).toEqual({
    value: { when: new Date(5), tags: new Set(["a"]), n: 2n },
    at: new Date(0),
    map: new Map([["k", 10n]]),
    set: new Set(["s"]),
    none: undefined,
  });

  // FormData (with a File) as an argument next to plain data.
  const form = new FormData();
  form.append("name", "Ada");
  form.append("file", new File(["file-body"], "note.txt", { type: "text/plain" }));
  expect(await clientCall(base, "submit", ["label-1", form])).toEqual({
    label: "label-1",
    name: "Ada",
    file: { name: "note.txt", text: "file-body" },
  });

  // An unserializable argument fails on the client, before any request.
  await expect(clientCall(base, "echo", [{ fn: () => 1 }])).rejects.toThrow(
    /Server action "echo": an argument cannot be serialized/
  );

  // A thrown ProblemError answers with its own status and problem.
  const failed = (await clientCall(base, "fail", []).catch((error: unknown) => error)) as ServerActionError;
  expect(failed).toBeInstanceOf(ServerActionError);
  expect(failed.status).toBe(409);
  expect(failed.problem).toMatchObject({ title: "Conflict", detail: "already exists", status: 409 });

  // Anything else: a generic 500 that never echoes the message.
  const crashed = await rawCall(base, id("crash"), []);
  expect(crashed.status).toBe(500);
  expect(crashed.headers.get("content-type")).toBe("application/problem+json");
  const crashBody = await crashed.text();
  expect(crashBody).not.toContain("secret-db-password");
  expect(JSON.parse(crashBody)).toMatchObject({ status: 500, title: "Internal Server Error" });
  if (mode === "start") {
    const reported = booted.errors.find((event) => event.error.message === "secret-db-password");
    expect(reported?.source).toBe("action");
  }

  // An unserializable return value is a server-side failure, reported likewise.
  const badResult = await rawCall(base, id("badResult"), []);
  expect(badResult.status).toBe(500);
  if (mode === "start") {
    expect(booted.errors.some((event) => /badResult" returned a value the serializer cannot encode/.test(event.error.message))).toBe(true);
  }

  // A redirect is signalled through the data protocol, not followed.
  const redirected = await rawCall(base, id("go"), []);
  expect(redirected.status).toBe(204);
  expect(redirected.headers.get("x-neutron-redirect")).toBe("/done?from=action");

  // Response headers set through getActionContext(): invalidate + cookies.
  const touched = await rawCall(base, id("touch"), []);
  expect(touched.status).toBe(200);
  expect(touched.headers.get("x-neutron-invalidate")).toBe("/todos");
  expect(touched.headers.getSetCookie()).toEqual(["a=1; Path=/", "b=2; Path=/"]);
  expect(touched.headers.get("cache-control")).toBe("no-store");

  // Global middleware: it runs, its context reaches the action, and it gates.
  const who = await rawCall(base, id("whoami"), [], { "x-user": "ada" });
  expect(who.headers.get("x-global")).toBe("ran");
  expect(await who.text()).toBe(stringify("ada"));
  const gated = await rawCall(base, id("whoami"), [], { "x-deny": "1" });
  expect(gated.status).toBe(401);
  expect(await gated.text()).toBe("gated");

  // CSRF: a cross-site browser request is refused, by Sec-Fetch-Site or Origin.
  const crossSite = await rawCall(base, id("touch"), [], {
    Origin: "https://evil.example",
    "Sec-Fetch-Site": "cross-site",
  });
  expect(crossSite.status).toBe(403);
  expect(crossSite.headers.get("x-neutron-invalidate")).toBeNull();
  const crossOrigin = await rawCall(base, id("touch"), [], { Origin: "https://evil.example" });
  expect(crossOrigin.status).toBe(403);
  const sameOrigin = await rawCall(base, id("plain"), [], { Origin: base, "Sec-Fetch-Site": "same-origin" });
  expect(sameOrigin.status).toBe(200);

  // Unknown ids, missing ids, other methods, malformed and oversized bodies.
  const unknown = await rawCall(base, "0".repeat(40), []);
  expect(unknown.status).toBe(404);
  expect(await unknown.json()).toMatchObject({ status: 404, title: "Not Found" });
  const missing = await fetch(`${base}/_neutron/action`, { method: "POST", body: "[]" });
  expect(missing.status).toBe(400);
  const get = await fetch(`${base}/_neutron/action`);
  expect(get.status).toBe(405);
  expect(get.headers.get("allow")).toBe("POST");
  const malformed = await rawCall(base, id("plain"), [], {}, "{not devalue");
  expect(malformed.status).toBe(400);
  const oversized = await rawCall(base, id("echo"), ["x".repeat(8000)]);
  expect(oversized.status).toBe(413);
}

describe("server actions", () => {
  it("createServer on the Vite runtime (dist manifest)", { timeout: 60_000 }, async () => {
    const root = await writeFixture(".tmp-neutron-actions-start-");
    await expectContract(await bootStart(root), "start");
  });

  it("dev server (modules the client graph has seen)", { timeout: 60_000 }, async () => {
    const root = await writeFixture(".tmp-neutron-actions-dev-");
    // Dev reports failing actions on the terminal (and overlay).
    const logged = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      await expectContract(await bootDev(root), "dev");
      expect(logged.mock.calls.some(([label, error]) => label === "Server action error:" && /secret-db-password/.test(String(error)))).toBe(true);
    } finally {
      logged.mockRestore();
    }
  });
});
