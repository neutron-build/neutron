import { spawn, spawnSync } from "node:child_process";
import process from "node:process";
import { setTimeout as delay } from "node:timers/promises";

const PLAYGROUND_FILTER = "@neutron/playground";
const CASES = [
  {
    name: "memory",
    port: 3121,
    env: {
      NEUTRON_DATA_PROFILE: "memory",
    },
    assertions: [
      { path: "/todos", includes: "Data profile: <code>memory</code>" },
      { path: "/todos", includes: "Queue: <code>memory</code>" },
      { path: "/protected", includes: "Session backend profile: <code>memory</code>" },
      { path: "/admin", includes: "Data profile: <code>memory</code>" },
    ],
  },
  {
    name: "production",
    port: 3122,
    env: {
      NEUTRON_DATA_PROFILE: "production",
    },
    assertions: [
      { path: "/todos", includes: "Data profile: <code>production</code>" },
      { path: "/protected", includes: "Session backend profile: <code>production</code>" },
      { path: "/admin", includes: "Data profile: <code>production</code>" },
    ],
  },
];

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
    await delay(250);
  }

  throw new Error(`Server did not become ready: ${url}`);
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function verifyCase(testCase) {
  const env = {
    NODE_ENV: "production",
    ...testCase.env,
  };

  console.log(`\n[data] Starting profile=${testCase.name}`);
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
      String(testCase.port),
    ],
    env
  );

  const origin = `http://127.0.0.1:${testCase.port}`;

  try {
    await waitForUrl(`${origin}/`);
    for (const check of testCase.assertions) {
      const response = await fetch(`${origin}${check.path}`);
      assert(
        response.status === 200,
        `[${testCase.name}] GET ${check.path} returned ${response.status}`
      );
      const html = await response.text();
      assert(
        html.includes(check.includes),
        `[${testCase.name}] GET ${check.path} missing "${check.includes}"`
      );
    }
    console.log(`[data] Passed profile=${testCase.name}`);
  } finally {
    stopProcessTree(child);
    await delay(1200);
  }
}

function shouldRunProductionCase() {
  if (process.env.NEUTRON_DATA_RUN_PRODUCTION_SMOKE !== "1") {
    console.log(
      "[data] Skipping production profile smoke. Set NEUTRON_DATA_RUN_PRODUCTION_SMOKE=1 to enable."
    );
    return false;
  }

  const redisUrl = process.env.DRAGONFLY_URL || process.env.REDIS_URL;
  if (!redisUrl) {
    console.log(
      "[data] Skipping production profile smoke. DRAGONFLY_URL or REDIS_URL is required."
    );
    return false;
  }

  return true;
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
  console.log("\n[data] Building playground before smoke checks");
  await runPnpm(["--filter", PLAYGROUND_FILTER, "run", "build"], {
    NODE_ENV: "production",
  });

  await verifyCase(CASES[0]);

  if (shouldRunProductionCase()) {
    await verifyCase(CASES[1]);
  }

  console.log("\n[data] Data profile smoke checks passed.");
}

main().catch((error) => {
  console.error("\n[data] Data profile smoke checks failed.");
  console.error(error);
  process.exit(1);
});
