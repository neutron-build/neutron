// Boots the generated deploy preset artifacts from apps/playground and makes
// real requests against them, so a preset that builds but cannot serve turns
// CI red.
//
// Coverage per preset (kept honest, not uniform):
//   - docker:     booted as a real process (node dist/server.mjs). Readiness
//                 poll is bounded and fails loudly if the process exits; real
//                 HTTP requests assert status + body; a SIGTERM drain test
//                 proves an in-flight request completes after SIGTERM and the
//                 process exits 0 through the drain path.
//   - vercel:     the generated api/__neutron.mjs handler is a Node-style
//                 (req, res) function; it is invoked in-process with real
//                 request objects. Real requests flow through the generated
//                 artifact + runtime bundle, but no Vercel platform (routing,
//                 filesystem handling) is emulated. No vercel CLI exists in
//                 this workspace and none is added.
//   - cloudflare: STATIC CHECKS ONLY, labelled [static] below. The workers
//                 runtime bundle (dist/server/worker/entry.js) is mixed-module
//                 output (ESM import/export statements plus a top-level
//                 require("preact")); it can only execute after wrangler
//                 rebundles it, and wrangler/miniflare are not dependencies
//                 of this workspace. We do not fake a boot.
import { spawn, spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as net from "node:net";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const PLAYGROUND_DIR = path.resolve("apps/playground");
const DIST_DIR = path.join(PLAYGROUND_DIR, "dist");
const DOCKER_PORT = Number(process.env.DEPLOY_PRESET_BOOT_PORT || 3137);

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function buildPreset(preset) {
  console.log(`\n[boot] Building preset: ${preset}`);
  const result = spawnSync("pnpm", ["--dir", PLAYGROUND_DIR, "run", `build:${preset}`], {
    stdio: "inherit",
    env: process.env,
  });
  if (result.status !== 0) {
    throw new Error(`build:${preset} failed with exit code ${result.status}`);
  }
}

async function verifyVercel() {
  const handlerPath = path.join(DIST_DIR, "api", "__neutron.mjs");
  assert(fs.existsSync(handlerPath), "vercel: dist/api/__neutron.mjs missing after build");
  const handler = (await import(pathToFileURL(handlerPath))).default;
  assert(typeof handler === "function", "vercel: api/__neutron.mjs default export is not a function");

  const call = async (url) => {
    const chunks = [];
    const headers = {};
    const res = {
      statusCode: 200,
      setHeader: (key, value) => {
        headers[key.toLowerCase()] = value;
      },
      write: (chunk) => {
        chunks.push(Buffer.from(chunk));
      },
      end: (chunk) => {
        if (chunk) chunks.push(Buffer.from(chunk));
      },
    };
    await handler({ method: "GET", url, headers: { host: "localhost" } }, res);
    return { status: res.statusCode, body: Buffer.concat(chunks).toString(), headers };
  };

  const cache = await call("/api/cache");
  assert(cache.status === 200, `vercel: GET /api/cache via generated handler returned ${cache.status}`);
  assert(
    (cache.headers["content-type"] || "").includes("application/json"),
    `vercel: /api/cache content-type mismatch: ${cache.headers["content-type"]}`
  );
  assert(cache.body.includes(`"ok":true`), `vercel: /api/cache body missing ok:true: ${cache.body.slice(0, 80)}`);

  const compute = await call("/compute");
  assert(compute.status === 200, `vercel: GET /compute via generated handler returned ${compute.status}`);
  assert(compute.body.includes("value="), "vercel: /compute SSR body missing rendered loader value");

  console.log("[boot] vercel: invoked generated handler (in-process) — /api/cache 200 JSON, /compute 200 SSR");
}

function verifyCloudflareStatic() {
  const checks = [];
  const workerPath = path.join(DIST_DIR, "_worker.js");
  const entryPath = path.join(DIST_DIR, "server", "worker", "entry.js");
  assert(fs.existsSync(workerPath), "cloudflare [static]: dist/_worker.js missing after build");
  assert(fs.existsSync(entryPath), "cloudflare [static]: dist/server/worker/entry.js missing after build");

  const wranglerRaw = fs.readFileSync(path.join(DIST_DIR, "wrangler.json"), "utf-8");
  let wrangler;
  try {
    wrangler = JSON.parse(wranglerRaw);
  } catch (error) {
    throw new Error(`cloudflare [static]: wrangler.json is not valid JSON: ${error.message}`);
  }
  checks.push("wrangler.json parses");
  assert(wrangler.main === "./_worker.js", `cloudflare [static]: wrangler.json main should be ./_worker.js, got ${wrangler.main}`);
  assert(wrangler.assets?.binding === "ASSETS", "cloudflare [static]: wrangler.json missing assets.binding === ASSETS");
  assert(wrangler.assets?.directory === ".", "cloudflare [static]: wrangler.json assets.directory should be .");
  checks.push("wrangler.json main/assets contract holds");

  const workerSource = fs.readFileSync(workerPath, "utf-8");
  assert(/export\s+default/.test(workerSource), "cloudflare [static]: _worker.js missing default export");
  assert(workerSource.includes("env.ASSETS.fetch"), "cloudflare [static]: _worker.js missing asset-first env.ASSETS.fetch");
  const importMatch = workerSource.match(/from\s+"(\.\/server\/worker\/entry\.js)"/);
  assert(importMatch !== null, "cloudflare [static]: _worker.js does not import ./server/worker/entry.js");
  checks.push("_worker.js exports fetch handler, asset-first, entry import path exists on disk");

  // Cross-artifact consistency: if the worker bundle references node: builtins,
  // wrangler.json must enable nodejs_compat (mirrors the adapter's own logic).
  const entrySource = fs.readFileSync(entryPath, "utf-8");
  const nodeCompatRequired = /(?:^|[^a-zA-Z0-9_])(?:node:[a-z]+|require\s*\()/.test(entrySource);
  const flags = Array.isArray(wrangler.compatibility_flags) ? wrangler.compatibility_flags : [];
  if (nodeCompatRequired) {
    assert(
      flags.includes("nodejs_compat"),
      "cloudflare [static]: worker bundle uses node builtins/require but wrangler.json lacks nodejs_compat"
    );
    checks.push("worker bundle needs node compat and wrangler.json declares nodejs_compat");
  } else {
    checks.push("worker bundle needs no node compat flag");
  }

  console.log("[boot] cloudflare [static-only]: " + checks.join("; "));
  console.log(
    "[boot] cloudflare: NOT booted — workers bundle is mixed-module (ESM + top-level require()) " +
      "and only executes after wrangler rebundling; wrangler/miniflare are not workspace dependencies."
  );
}

async function verifyDocker() {
  const serverPath = path.join(DIST_DIR, "server.mjs");
  assert(fs.existsSync(serverPath), "docker: dist/server.mjs missing after build");

  const child = spawn("node", [serverPath], {
    cwd: DIST_DIR,
    env: { ...process.env, PORT: String(DOCKER_PORT) },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let serverOutput = "";
  child.stdout.on("data", (chunk) => {
    serverOutput += chunk;
  });
  child.stderr.on("data", (chunk) => {
    serverOutput += chunk;
  });
  let childExit = null;
  child.on("exit", (code) => {
    childExit = code;
  });

  const origin = `http://127.0.0.1:${DOCKER_PORT}`;
  try {
    const readyDeadline = Date.now() + 30_000;
    while (Date.now() < readyDeadline) {
      if (childExit !== null) {
        throw new Error(`docker: server exited before listening (code ${childExit}). Output:\n${serverOutput}`);
      }
      try {
        const res = await fetch(`${origin}/`);
        if (res.status < 500) break;
      } catch {
        // keep polling
      }
      await delay(250);
      if (Date.now() >= readyDeadline) {
        throw new Error(`docker: server did not listen within 30s. Output:\n${serverOutput}`);
      }
    }

    const home = await fetch(`${origin}/`);
    assert(home.status === 200, `docker: GET / returned ${home.status}`);
    const homeBody = await home.text();
    assert(homeBody.includes("<!DOCTYPE html"), "docker: GET / body is not HTML");

    const cache = await fetch(`${origin}/api/cache`);
    assert(cache.status === 200, `docker: GET /api/cache returned ${cache.status}`);
    assert(
      (cache.headers.get("content-type") || "").includes("application/json"),
      `docker: /api/cache content-type mismatch: ${cache.headers.get("content-type")}`
    );
    const cacheBody = await cache.json();
    assert(cacheBody?.ok === true, `docker: /api/cache body missing ok:true: ${JSON.stringify(cacheBody).slice(0, 80)}`);
    console.log(`[boot] docker: booted ${path.relative(process.cwd(), serverPath)} — GET / 200 HTML, GET /api/cache 200 JSON`);

    // SIGTERM drain: send a request with incomplete headers (the server cannot
    // have responded yet — asserted below), SIGTERM the process, then finish
    // the request and require the full response plus a clean exit 0.
    const sock = net.connect(DOCKER_PORT, "127.0.0.1");
    await new Promise((resolve, reject) => {
      sock.once("connect", resolve);
      sock.once("error", reject);
    });
    sock.write(`GET /api/cache HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n`);
    await delay(400);
    assert(sock.bytesRead === 0, "docker drain: received bytes before request was complete (test is not measuring in-flight drain)");
    child.kill("SIGTERM");
    await delay(300);
    sock.write(`\r\n`);
    const raw = await new Promise((resolve, reject) => {
      const chunks = [];
      const timer = setTimeout(() => reject(new Error("docker drain: no response within 10s after SIGTERM")), 10_000);
      sock.on("data", (chunk) => chunks.push(chunk));
      sock.once("close", () => {
        clearTimeout(timer);
        resolve(Buffer.concat(chunks).toString());
      });
      sock.once("error", (error) => {
        clearTimeout(timer);
        reject(error);
      });
    });
    assert(raw.startsWith("HTTP/1.1 200"), `docker drain: expected 200 after SIGTERM, got: ${raw.slice(0, 40)}`);
    assert(raw.includes(`"ok":true`), "docker drain: response body after SIGTERM missing ok:true");
    sock.destroy();

    const exit = await new Promise((resolve) => {
      if (childExit !== null) return resolve(childExit);
      child.once("exit", resolve);
      setTimeout(() => resolve(null), 25_000);
    });
    assert(exit === 0, `docker drain: expected exit code 0 after drain, got ${exit}. Output:\n${serverOutput}`);
    assert(serverOutput.includes("draining"), `docker drain: expected drain log, got:\n${serverOutput}`);
    console.log("[boot] docker: SIGTERM drain observed — in-flight request completed post-SIGTERM, process exited 0");
  } finally {
    if (childExit === null) {
      child.kill("SIGKILL");
    }
  }
}

async function main() {
  buildPreset("vercel");
  await verifyVercel();

  buildPreset("cloudflare");
  verifyCloudflareStatic();

  buildPreset("docker");
  await verifyDocker();

  console.log("\n[boot] Deploy preset boot smoke passed.");
  console.log("[boot] Coverage: docker=booted as a process (HTTP + SIGTERM drain); vercel=generated handler invoked in-process; cloudflare=static checks only (see labels above).");
}

main().catch((error) => {
  console.error("\n[boot] Deploy preset boot smoke FAILED.");
  console.error(error);
  process.exit(1);
});
