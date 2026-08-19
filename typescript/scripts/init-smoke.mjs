import { spawn, spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import process from "node:process";
import { setTimeout as delay } from "node:timers/promises";

/**
 * Init smoke (S76).
 *
 * Proves `neutron-ts init` produces a RUNNING app, not just files: scaffolds
 * every template via the real CLI binary into examples/ (a workspace glob, so
 * the scaffold's `workspace:*` deps link the local checkout), installs, boots
 * the default template's dev server, and asserts a real SSR response.
 *
 * Fails loudly: if the dev server process exits at any point, the failure is
 * reported immediately with the captured output instead of polling to timeout.
 * The pnpm lockfile is backed up and restored, and a final install prunes the
 * temporary projects, so the working tree is left as it was found.
 */

const TEMPLATES = ["basic", "marketing", "app", "full", "docs"];
const BOOT_TEMPLATE = "basic";
const PORT = 3141;
const ORIGIN = `http://127.0.0.1:${PORT}`;
const READINESS_TIMEOUT_MS = 60000;
const INSTALL_TIMEOUT_MS = 300000;

const workspaceRoot = process.cwd();
const lockfilePath = path.join(workspaceRoot, "pnpm-lock.yaml");
const lockfileBackup = fs.readFileSync(lockfilePath);
const projectDirs = [];
let lockfileRestored = false;

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

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: workspaceRoot,
    encoding: "utf-8",
    ...options,
  });
  if (result.status !== 0) {
    const out = [result.stdout, result.stderr].filter(Boolean).join("\n");
    throw new Error(`Command failed (${result.status}): ${command} ${args.join(" ")}\n${out}`);
  }
  return result;
}

function restoreLockfile() {
  if (lockfileRestored) {
    return;
  }
  lockfileRestored = true;
  fs.writeFileSync(lockfilePath, lockfileBackup);
}

function cleanup() {
  for (const dir of projectDirs) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
  restoreLockfile();
  // Prune the removed importers from node_modules so the workspace is exactly
  // as the committed lockfile describes. Runs after lockfile restore; failure
  // here means a dirty tree, so it must fail the smoke.
  run("pnpm", ["install", "--no-frozen-lockfile", "--reporter", "append-only"], {
    timeout: INSTALL_TIMEOUT_MS,
  });
}

function startDevServer(projectName) {
  const child = spawn(
    "pnpm",
    [
      "--filter",
      projectName,
      "exec",
      "neutron-ts",
      "dev",
      "--host",
      "127.0.0.1",
      "--port",
      String(PORT),
    ],
    {
      cwd: workspaceRoot,
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

function check(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function fetchText(pathname, what) {
  const res = await fetch(`${ORIGIN}${pathname}`, { redirect: "manual" });
  const body = await res.text();
  check(
    res.status === 200,
    `[init] GET ${pathname} (${what}) returned ${res.status}:\n${body.slice(0, 4000)}`
  );
  return body;
}

function assertNoRawTokens(rootDir, template) {
  const visit = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const entryPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        visit(entryPath);
        continue;
      }
      if (entry.name.endsWith(".md") || entry.name === ".gitignore") {
        continue;
      }
      const content = fs.readFileSync(entryPath, "utf8");
      check(
        !/__[A-Z0-9_]+__/.test(content),
        `[init] raw template token left behind (${template}): ${entryPath}`
      );
    }
  };
  visit(rootDir);
}

function scaffold(template) {
  const projectName = `init-smoke-${template}`;
  const projectDir = path.join(workspaceRoot, "examples", projectName);

  fs.rmSync(projectDir, { recursive: true, force: true });
  projectDirs.push(projectDir);

  console.log(`[init] neutron-ts init examples/${projectName} --template ${template}`);
  run("node", [
    path.join("packages", "neutron-cli", "dist", "index.js"),
    "init",
    `examples/${projectName}`,
    "--template",
    template,
  ]);

  const pkgPath = path.join(projectDir, "package.json");
  check(fs.existsSync(pkgPath), `[init] scaffold wrote no package.json (${template})`);
  const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  check(pkg.name === projectName, `[init] package name token wrong (${template}): ${pkg.name}`);
  check(
    pkg.dependencies["@neutron-build/core"] === "workspace:*",
    `[init] expected workspace:* deps inside the monorepo, got ${pkg.dependencies["@neutron-build/core"]} (${template})`
  );
  assertNoRawTokens(projectDir, template);
  return projectName;
}

async function main() {
  const projectName = scaffold(BOOT_TEMPLATE);
  for (const template of TEMPLATES.filter((name) => name !== BOOT_TEMPLATE)) {
    scaffold(template);
  }

  console.log("[init] Installing scaffolded projects into the workspace");
  run("pnpm", ["install", "--no-frozen-lockfile", "--reporter", "append-only"], {
    timeout: INSTALL_TIMEOUT_MS,
  });

  console.log(`[init] Starting dev server for ${projectName} (${ORIGIN})`);
  const child = startDevServer(projectName);

  try {
    await waitForUrl(`${ORIGIN}/`, child);

    // The basic home route's loader returns the project name as its title, so
    // a rendered <h2> with the scaffold's name proves: template copied, tokens
    // substituted, loader executed, SSR responded.
    const home = await fetchText("/", "home HTML");
    check(home.includes(projectName), `[init] Home HTML missing project name "${projectName}"`);
    check(
      home.includes("Static route generated at"),
      "[init] Home HTML missing basic template content"
    );

    const user = await fetchText("/users/42", "dynamic route");
    check(
      user.includes("42") || user.toLowerCase().includes("user"),
      "[init] /users/42 response missing expected content"
    );

    console.log("[init] Init smoke passed: neutron-ts init produces a running app.");
  } finally {
    await stopProcessTree(child);
    await delay(300);
    cleanup();
  }
}

main().catch((error) => {
  console.error("\n[init] Init smoke FAILED.");
  console.error(error);
  try {
    cleanup();
  } catch (cleanupError) {
    console.error("[init] Cleanup after failure also failed — tree may be dirty:");
    console.error(cleanupError);
  }
  process.exit(1);
});
