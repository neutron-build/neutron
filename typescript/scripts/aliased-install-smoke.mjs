import { spawn, spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import process from "node:process";
import { setTimeout as delay } from "node:timers/promises";

/**
 * Aliased-install smoke (S77).
 *
 * Starts `neutron dev` for apps/aliased-smoke — an app whose
 * `@neutron-build/core` import specifiers are redirected by a Vite
 * `resolve.alias` to the local package checkout (the same shape as a `file:`
 * install or a pnpm `link:`) — and verifies the dev server survives Vite's
 * dependency optimizer when it meets the aliased copy.
 *
 * Fails loudly: if the dev server process exits at any point, the failure is
 * reported immediately with the captured process output instead of polling
 * until timeout.
 */

const APP_FILTER = "@neutron/aliased-smoke";
const PORT = 3131;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const READINESS_TIMEOUT_MS = 60000;
const MAX_MODULES = 40;

const childOutput = { stdout: "", stderr: "" };

function capture(stream, key) {
  stream.setEncoding("utf-8");
  stream.on("data", (chunk) => {
    const merged = childOutput[key] + chunk;
    childOutput[key] = merged.length > 64 * 1024 ? merged.slice(-64 * 1024) : merged;
  });
}

function tailOutput() {
  const parts = [];
  if (childOutput.stdout.trim()) {
    parts.push(`--- child stdout (tail) ---\n${childOutput.stdout.trim()}`);
  }
  if (childOutput.stderr.trim()) {
    parts.push(`--- child stderr (tail) ---\n${childOutput.stderr.trim()}`);
  }
  return parts.join("\n") || "(no captured output)";
}

function startDevServer() {
  const child = spawn(
    "pnpm",
    [
      "--filter",
      APP_FILTER,
      "exec",
      "neutron-ts",
      "dev",
      "--host",
      "127.0.0.1",
      "--port",
      String(PORT),
    ],
    {
      cwd: process.cwd(),
      detached: process.platform !== "win32",
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env },
    }
  );
  capture(child.stdout, "stdout");
  capture(child.stderr, "stderr");
  return child;
}

function stopProcessTree(child) {
  if (!child || child.exitCode !== null || child.pid === undefined) {
    return;
  }

  if (process.platform === "win32") {
    spawnSync("taskkill", ["/pid", String(child.pid), "/t", "/f"], { stdio: "ignore" });
    return;
  }

  try {
    process.kill(-child.pid, "SIGTERM");
  } catch {
    // Group already gone.
  }
  const end = Date.now() + 5000;
  const exited = new Promise((resolve) => child.once("exit", resolve));
  const timer = delay(end - Date.now()).then(() => {
    try {
      process.kill(-child.pid, "SIGKILL");
    } catch {
      // Group already gone.
    }
  });
  return Promise.race([exited, timer]).finally(() => void 0);
}

async function waitForUrl(url, child, timeoutMs = READINESS_TIMEOUT_MS) {
  const end = Date.now() + timeoutMs;
  const exited = new Promise(
    (resolve) => child.once("exit", (code, signal) => resolve({ code, signal }))
  );
  while (Date.now() < end) {
    const exit = await Promise.race([
      exited.then((info) => info),
      delay(300).then(() => null),
    ]);
    if (exit) {
      throw new Error(
        `Dev server exited early (code=${exit.code} signal=${exit.signal}) while waiting for ${url}.\n${tailOutput()}`
      );
    }
    try {
      const res = await fetch(url, { redirect: "manual" });
      if (res.status < 500) {
        return;
      }
    } catch {
      // Keep polling.
    }
  }

  throw new Error(`Dev server did not become ready within ${timeoutMs}ms: ${url}\n${tailOutput()}`);
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function extractModuleUrls(moduleUrl, body) {
  const urls = new Set();
  const pattern = /(?:from|import)\s*["']([^"'\n]+)["']/g;
  let match;
  while ((match = pattern.exec(body)) !== null) {
    const spec = match[1];
    if (!spec.startsWith(".") && !spec.startsWith("/")) continue;
    if (spec.startsWith("data:")) continue;
    urls.add(new URL(spec, new URL(moduleUrl, ORIGIN)).pathname);
  }
  return [...urls];
}

async function fetchText(pathname, what) {
  const res = await fetch(`${ORIGIN}${pathname}`, { redirect: "manual" });
  const body = await res.text();
  assert(
    res.status === 200,
    `[aliased] GET ${pathname} (${what}) returned ${res.status}:\n${body.slice(0, 4000)}`
  );
  return body;
}

async function main() {
  // Cold start every time: a warm dep-optimizer cache could satisfy requests
  // without running esbuild, which is exactly the code under test.
  const viteCache = path.join(process.cwd(), "apps", "aliased-smoke", "node_modules", ".vite");
  fs.rmSync(viteCache, { recursive: true, force: true });

  console.log(`[aliased] Starting dev server for ${APP_FILTER} (${ORIGIN})`);
  const child = startDevServer();

  try {
    await waitForUrl(`${ORIGIN}/`, child);

    const html = await fetchText("/", "home HTML");
    assert(html.includes("aliased-smoke"), "[aliased] Home HTML missing expected content");
    assert(
      html.includes("Aliased Counter") || html.includes("neutron-island"),
      "[aliased] Home HTML missing island markup"
    );

    // Walk the client module graph the way a browser would (bounded): the
    // hydration entry and its rewritten imports, which is where Vite's
    // import-analysis and dep optimizer meet the aliased package.
    const entry = await fetchText("/src/main.tsx", "hydration entry");
    const modules = extractModuleUrls("/src/main.tsx", entry).slice(0, MAX_MODULES);
    console.log(`[aliased] Walking ${modules.length} client modules from /src/main.tsx`);
    for (const moduleUrl of modules) {
      await fetchText(moduleUrl, "client module");
    }

    console.log("[aliased] Aliased-install dev smoke passed.");
  } finally {
    await stopProcessTree(child);
    await delay(300);
  }
}

main().catch((error) => {
  console.error("\n[aliased] Aliased-install dev smoke FAILED.");
  console.error(error);
  process.exit(1);
});
