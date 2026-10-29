import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createServer, type NeutronServer } from "./index.js";

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

// A throwaway root with no src/routes — works for every mode (discovery is
// skipped in non-ssr modes; ssr mode serves /health from the empty route set).
async function emptyRoot(): Promise<string> {
  return await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-cors-"));
}

/** Every Access-Control-* header on the response, lowercased name → value. */
function corsHeaders(response: Response): Record<string, string> {
  const found: Record<string, string> = {};
  response.headers.forEach((value, key) => {
    if (key.startsWith("access-control-")) {
      found[key] = value;
    }
  });
  return found;
}

function varyTokens(response: Response): string[] {
  return (response.headers.get("vary") ?? "")
    .split(",")
    .map((token) => token.trim().toLowerCase())
    .filter(Boolean);
}

const ALLOWED_ORIGIN = "https://app.example.com";

describe("cors defaults (no cors option)", () => {
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

  it.each(["ssr", "api", "raw"] as const)(
    "%s mode: no CORS headers on responses when Origin is sent",
    async (mode) => {
      root = await emptyRoot();
      const port = await getFreePort();
      running = await createServer({
        mode,
        host: "127.0.0.1",
        port,
        rootDir: root,
        compress: false,
      });
      const base = `http://127.0.0.1:${port}`;

      const response = await fetch(`${base}/health`, {
        headers: { Origin: ALLOWED_ORIGIN },
      });
      expect(response.status).toBe(200);
      expect(corsHeaders(response)).toEqual({});
      expect(varyTokens(response)).not.toContain("origin");
    }
  );

  it("api mode: preflight OPTIONS is not short-circuited and gets no CORS headers", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "api",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });
    const base = `http://127.0.0.1:${port}`;

    const response = await fetch(`${base}/health`, {
      method: "OPTIONS",
      headers: {
        Origin: ALLOWED_ORIGIN,
        "Access-Control-Request-Method": "POST",
      },
    });
    // Not the CORS preflight 204 — the request falls through to the normal
    // router (404/405 depending on mode), carrying no CORS headers.
    expect(response.status).not.toBe(204);
    expect(corsHeaders(response)).toEqual({});
  });

  it("security headers stay on by default when CORS is off", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "api",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });

    const response = await fetch(`http://127.0.0.1:${port}/health`);
    expect(response.headers.get("X-Frame-Options")).toBe("DENY");
    expect(response.headers.get("X-Content-Type-Options")).toBe("nosniff");
    expect(response.headers.get("Referrer-Policy")).toBe(
      "strict-origin-when-cross-origin"
    );
  });

  it("cors: false (explicit opt-out) also sends no CORS headers", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "api",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      cors: false,
    });

    const response = await fetch(`http://127.0.0.1:${port}/health`, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    expect(corsHeaders(response)).toEqual({});
    expect(varyTokens(response)).not.toContain("origin");
  });
});

describe("cors opt-in (cors option provided)", () => {
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

  it("allowlist echoes an allowed origin exactly and denies others (api mode)", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "api",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      cors: { origin: [ALLOWED_ORIGIN], credentials: true },
    });
    const base = `http://127.0.0.1:${port}`;

    const allowed = await fetch(`${base}/health`, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    expect(allowed.headers.get("Access-Control-Allow-Origin")).toBe(ALLOWED_ORIGIN);
    expect(allowed.headers.get("Access-Control-Allow-Credentials")).toBe("true");
    expect(varyTokens(allowed)).toContain("origin");

    // Disallowed origin: no allow header, but Vary: Origin still declared
    // (TS-30) so a shared cache cannot reuse the rejection cross-origin.
    const denied = await fetch(`${base}/health`, {
      headers: { Origin: "https://evil.example" },
    });
    expect(denied.headers.get("Access-Control-Allow-Origin")).toBeNull();
    expect(denied.headers.get("Access-Control-Allow-Credentials")).toBeNull();
    expect(varyTokens(denied)).toContain("origin");
  });

  it("allowlist + credentials keeps Accept-Encoding in Vary alongside Origin", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "api",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: true,
      cors: { origin: [ALLOWED_ORIGIN], credentials: true },
    });

    const response = await fetch(`http://127.0.0.1:${port}/health`, {
      headers: { Origin: ALLOWED_ORIGIN },
    });
    const tokens = varyTokens(response);
    expect(tokens).toContain("origin");
    expect(tokens).toContain("accept-encoding");
  });

  it("preflight from an allowed origin: 204 with methods, headers, max-age", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "api",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      cors: { origin: [ALLOWED_ORIGIN], methods: ["GET", "POST"] },
    });

    const response = await fetch(`http://127.0.0.1:${port}/health`, {
      method: "OPTIONS",
      headers: {
        Origin: ALLOWED_ORIGIN,
        "Access-Control-Request-Method": "POST",
      },
    });
    expect(response.status).toBe(204);
    expect(response.headers.get("Access-Control-Allow-Origin")).toBe(ALLOWED_ORIGIN);
    const methods = response.headers.get("Access-Control-Allow-Methods") ?? "";
    expect(methods).toContain("GET");
    expect(methods).toContain("POST");
    expect(response.headers.get("Access-Control-Allow-Headers")).toContain("Content-Type");
    expect(response.headers.get("Access-Control-Max-Age")).toBe("600");
    expect(varyTokens(response)).toContain("origin");
  });

  it("preflight from a disallowed origin: 403, no allow header, Vary: Origin", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "api",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      cors: { origin: [ALLOWED_ORIGIN] },
    });

    const response = await fetch(`http://127.0.0.1:${port}/health`, {
      method: "OPTIONS",
      headers: {
        Origin: "https://evil.example",
        "Access-Control-Request-Method": "POST",
      },
    });
    expect(response.status).toBe(403);
    expect(response.headers.get("Access-Control-Allow-Origin")).toBeNull();
    expect(varyTokens(response)).toContain("origin");
  });

  it.each(["ssr", "raw"] as const)(
    "%s mode: configured allowlist applies identically",
    async (mode) => {
      root = await emptyRoot();
      const port = await getFreePort();
      running = await createServer({
        mode,
        host: "127.0.0.1",
        port,
        rootDir: root,
        compress: false,
        cors: { origin: [ALLOWED_ORIGIN], credentials: true },
      });

      const allowed = await fetch(`http://127.0.0.1:${port}/health`, {
        headers: { Origin: ALLOWED_ORIGIN },
      });
      expect(allowed.headers.get("Access-Control-Allow-Origin")).toBe(ALLOWED_ORIGIN);

      const denied = await fetch(`http://127.0.0.1:${port}/health`, {
        headers: { Origin: "https://evil.example" },
      });
      expect(denied.headers.get("Access-Control-Allow-Origin")).toBeNull();
    }
  );

  it("credentials with a wildcard origin refuses to construct the server", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    await expect(
      createServer({
        mode: "api",
        host: "127.0.0.1",
        port,
        rootDir: root,
        compress: false,
        cors: { origin: "*", credentials: true },
      })
    ).rejects.toThrow("credentials cannot be used with origin");
    // createServer rejected before listening — nothing to close.
    running = null;
  });
});
