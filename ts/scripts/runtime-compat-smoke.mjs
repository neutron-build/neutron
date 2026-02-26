import { spawn, spawnSync } from "node:child_process";
import process from "node:process";
import { setTimeout as delay } from "node:timers/promises";

const CASES = [
  { runtime: "preact", port: 3111 },
  { runtime: "react-compat", port: 3112 },
];

const PLAYGROUND_FILTER = "@neutron/playground";

async function runPnpm(args, env = {}) {
  await new Promise((resolve, reject) => {
    const child = spawn(toShellCommand(["pnpm", ...args]), {
      cwd: process.cwd(),
      shell: true,
      stdio: "inherit",
      env: { ...process.env, ...env },
    });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Command failed (${code}): pnpm ${args.join(" ")}`));
      }
    });
  });
}

function startPnpm(args, env = {}) {
  return spawn(toShellCommand(["pnpm", ...args]), {
    cwd: process.cwd(),
    shell: true,
    stdio: "ignore",
    env: { ...process.env, ...env },
  });
}

function stopProcessTree(child) {
  if (!child || child.exitCode !== null || child.pid === undefined) {
    return;
  }

  if (process.platform === "win32") {
    spawnSync("taskkill", ["/pid", String(child.pid), "/t", "/f"], { stdio: "ignore" });
    return;
  }

  child.kill("SIGTERM");
}

async function waitForUrl(url, timeoutMs = 60000) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    try {
      const res = await fetch(url, { redirect: "manual" });
      if (res.status < 500) {
        return;
      }
    } catch {
      // Keep polling.
    }
    await delay(300);
  }

  throw new Error(`Server did not become ready: ${url}`);
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function verifyRuntimeCase(runtime, port) {
  const env = {
    NODE_ENV: "production",
    NEUTRON_RUNTIME: runtime,
  };

  console.log(`\n[compat] Building ${PLAYGROUND_FILTER} with runtime=${runtime}`);
  await runPnpm(["--filter", PLAYGROUND_FILTER, "run", "build"], env);

  console.log(`[compat] Starting server (runtime=${runtime}, port=${port})`);
  const child = startPnpm(
    [
      "--filter",
      PLAYGROUND_FILTER,
      "exec",
      "neutron",
      "start",
      "--host",
      "127.0.0.1",
      "--port",
      String(port),
    ],
    env
  );

  const origin = `http://127.0.0.1:${port}`;
  try {
    await waitForUrl(`${origin}/`);

    const homeHtml = await fetch(`${origin}/`);
    assert(homeHtml.status === 200, `[${runtime}] GET / returned ${homeHtml.status}`);
    assert(
      (await homeHtml.text()).includes("Welcome to Neutron!"),
      `[${runtime}] GET / missing expected content`
    );

    const appHtml = await fetch(`${origin}/users/1`);
    assert(appHtml.status === 200, `[${runtime}] GET /users/1 returned ${appHtml.status}`);
    assert(
      (await appHtml.text()).includes("User:"),
      `[${runtime}] GET /users/1 missing expected content`
    );

    const appJson = await fetch(`${origin}/users/1`, {
      headers: { Accept: "application/json" },
    });
    assert(
      appJson.status === 200,
      `[${runtime}] GET /users/1 JSON returned ${appJson.status}`
    );

    const contentType = appJson.headers.get("content-type") || "";
    assert(
      contentType.includes("application/json"),
      `[${runtime}] JSON content-type mismatch: ${contentType}`
    );

    const payload = await appJson.json();
    assert(
      typeof payload?.__neutron_serialized__ === "string",
      `[${runtime}] JSON payload missing serialized envelope`
    );

    console.log(`[compat] Passed runtime=${runtime}`);
  } finally {
    stopProcessTree(child);
    await delay(1200);
  }
}

function toShellCommand(args) {
  return args.map(quoteArg).join(" ");
}

function quoteArg(arg) {
  if (/^[a-zA-Z0-9_./:@=+-]+$/.test(arg)) {
    return arg;
  }
  return `"${String(arg).replaceAll('"', '\\"')}"`;
}

async function main() {
  for (const testCase of CASES) {
    await verifyRuntimeCase(testCase.runtime, testCase.port);
  }
  console.log("\n[compat] Runtime compatibility smoke checks passed.");
}

main().catch((error) => {
  console.error("\n[compat] Runtime compatibility smoke checks failed.");
  console.error(error);
  process.exit(1);
});
