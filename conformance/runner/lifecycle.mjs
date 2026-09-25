// Process-level contract dimensions: §6 configuration and §8 graceful shutdown.
//
// Everything in contract.mjs speaks HTTP to a server someone else started.
// These two cannot: whether an SDK honours NEUTRON_PORT is decided by how the
// process is STARTED, and whether it shuts down gracefully by what the process
// does when SIGNALLED. So this module owns the boot as well as the assertion.
//
// Both were blind spots until 2026-09-23. The suite stayed green while Go and
// TypeScript ignored NEUTRON_PORT and Python never exited on SIGTERM, because
// every adapter pinned its port with a variable of its own (`PORT`) and no
// dimension ever signalled a server and watched what happened. The runner's
// teardown did send SIGTERM — and then SIGKILLed whatever was left 1.5s later
// without looking at the exit status, which turned the defect into a cleanup.
//
// Constants come from contract-ir.json (`config`, `shutdown`), which
// validate-ir.mjs keeps in agreement with FRAMEWORK_CONTRACT.md.

import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

const IR = JSON.parse(
  fs.readFileSync(
    path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "contract-ir.json"),
    "utf8",
  ),
);
const CFG = IR.config;
const SHUT = IR.shutdown;
const HOST_VAR = `${CFG.prefix}_HOST`;
const PORT_VAR = `${CFG.prefix}_PORT`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

// The environment an app is booted with.
//
//   contract — only NEUTRON_HOST / NEUTRON_PORT address the app. Every
//              adapter-specific addressing variable is removed, including
//              ones inherited from the caller's shell, so an app that only
//              listens where it should because PORT happened to be set
//              cannot pass.
//   adapter  — the SDK's descriptor-declared `portEnv`/`hostEnv`, for an SDK
//              that does not read NEUTRON_PORT yet. Only ever used after
//              `config.env` has failed, so the rest of the contract can
//              still be measured; the failure itself stays in the matrix.
function bootEnv(sdk, port, mode) {
  const env = { ...process.env };
  for (const v of [...CFG.adapterVars, sdk.portEnv, sdk.hostEnv]) if (v) delete env[v];
  delete env[HOST_VAR];
  delete env[PORT_VAR];
  if (mode === "contract") {
    env[HOST_VAR] = CFG.probeHost;
    env[PORT_VAR] = String(port);
  } else {
    env[sdk.portEnv] = String(port);
    if (sdk.hostEnv) env[sdk.hostEnv] = CFG.probeHost;
  }
  return env;
}

// Boot the SDK's conformance app. The spawned process must BE the server — a
// wrapper (`go run`, `npx`, a shell script that does not exec) would take the
// SIGTERM the shutdown dimension sends and report its own exit status, not
// the SDK's. Descriptors launch built binaries or the interpreter directly.
export async function boot(sdk, mode) {
  const port = await freePort();
  const { command, args, cwd } = sdk.cmd();
  const child = spawn(command, args, {
    cwd,
    env: bootEnv(sdk, port, mode),
    stdio: ["ignore", "ignore", "inherit"],
  });
  const h = {
    child,
    port,
    base: `http://127.0.0.1:${port}`,
    command,
    exited: false,
    exit: null, // { code, signal, at }
    spawnError: null,
  };
  h.exitPromise = new Promise((resolve) => {
    // A spawn failure — a missing binary, a non-executable file — arrives on
    // the child's 'error' event, NOT as a throw from spawn(). Without this
    // listener it is an unhandled error that kills the runner outright.
    child.on("error", (e) => {
      h.spawnError = e;
      h.exited = true;
      resolve();
    });
    child.on("exit", (code, signal) => {
      h.exited = true;
      h.exit = { code, signal, at: Date.now() };
      resolve();
    });
  });
  return h;
}

export function spawnNote(h) {
  if (!h.spawnError) return null;
  return h.spawnError.code === "ENOENT"
    ? `could not start "${h.command}" — no such file. Was it built? (--no-build skips the build)`
    : `could not start "${h.command}": ${h.spawnError.message}`;
}

// Wait until GET /health answers 200, the process dies, or the timeout passes.
export async function waitHealthy(h, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline && !h.exited) {
    const r = await request(h.base + "/health", 1000);
    if (r.status === 200) return true;
    await sleep(250);
  }
  return false;
}

// Tear down whatever is left. SIGTERM first, then SIGKILL.
export async function stop(h, graceMs = 1500) {
  if (h.exited) return;
  h.child.kill("SIGTERM");
  await Promise.race([h.exitPromise, sleep(graceMs)]);
  if (!h.exited) {
    h.child.kill("SIGKILL");
    await h.exitPromise;
  }
}

// One HTTP GET on its own fresh connection (no keep-alive pool), so "a new
// connection" means exactly that. Resolves { status, body } or { error }.
function request(url, timeoutMs) {
  return new Promise((resolve) => {
    let done = false;
    const finish = (v) => {
      if (!done) {
        done = true;
        resolve(v);
      }
    };
    const req = http.get(url, { agent: false, headers: { connection: "close" } }, (res) => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", (c) => (body += c));
      res.on("end", () => finish({ status: res.statusCode, body }));
      res.on("error", (e) => finish({ error: e.code || e.message }));
    });
    req.on("error", (e) => finish({ error: e.code || e.message }));
    req.setTimeout(timeoutMs, () => {
      req.destroy();
      finish({ error: "TIMEOUT" });
    });
  });
}

// --- config.env (§6) -------------------------------------------------------

export async function checkConfigEnv(h, timeoutMs) {
  const ok = await waitHealthy(h, timeoutMs);
  if (ok) {
    return {
      dim: "config.env",
      status: "pass",
      detail: `booted with only ${HOST_VAR}=${CFG.probeHost} ${PORT_VAR}=${h.port}; /health answered there`,
    };
  }
  const why = spawnNote(h)
    ? spawnNote(h)
    : h.exited
      ? `the process exited (code ${h.exit?.code}, signal ${h.exit?.signal})`
      : `nothing answered GET /health within ${timeoutMs / 1000}s`;
  return {
    dim: "config.env",
    status: "fail",
    detail:
      `booted with only ${HOST_VAR}/${PORT_VAR} (no adapter-specific variable) and ` +
      `${why} — the SDK does not listen where ${PORT_VAR}=${h.port} says`,
  };
}

// --- shutdown.sigterm (§8) -------------------------------------------------
//
// With the app idle and healthy: start a request to the slow route, send
// SIGTERM to the server process while it is in flight, open a new connection
// shortly after, and watch the process. Pass requires all three:
//
//   1. the in-flight request completes 200            (§8.3 drain)
//   2. the new connection is refused, or answered 503 (§8.2 stop accepting)
//   3. the process exits with status 0 within the drain plus a margin (§8.6)
//
// A connection the kernel queued but the app never served (reset or closed
// without a response when the process exits) counts as refused: the app did
// not accept it. Only a non-503 response means the server took new work.
export async function checkShutdown(h, bootTimeoutMs) {
  const dim = "shutdown.sigterm";
  if (!(await waitHealthy(h, bootTimeoutMs))) {
    return { dim, status: "fail", detail: "the app did not become healthy for the shutdown probe" };
  }

  const exitDeadlineMs = SHUT.slowMs + SHUT.exitMarginMs;
  const inflight = request(h.base + SHUT.slowPath, SHUT.slowMs + exitDeadlineMs);
  await sleep(SHUT.signalAfterMs);
  if (h.exited) {
    return { dim, status: "fail", detail: "the process died before it was signalled" };
  }
  h.child.kill(SHUT.probeSignal);
  const signalledAt = Date.now();
  await sleep(SHUT.probeAfterSignalMs);
  const fresh = request(h.base + IR.health.path, exitDeadlineMs);

  const [slow, probe] = await Promise.all([inflight, fresh]);
  const remaining = signalledAt + exitDeadlineMs - Date.now();
  if (!h.exited && remaining > 0) await Promise.race([h.exitPromise, sleep(remaining)]);

  const problems = [];
  const facts = [];

  if (slow.status === 200) facts.push(`in-flight ${SHUT.slowPath} completed 200`);
  else if (slow.status === 404) problems.push(`no ${SHUT.slowPath} route in the conformance app (404)`);
  else
    problems.push(
      `in-flight ${SHUT.slowPath} was not drained: ${slow.error ? slow.error : "status " + slow.status}`,
    );

  if (probe.error) facts.push(`new connection not served (${probe.error})`);
  else if (SHUT.newConnectionStatuses.includes(probe.status))
    facts.push(`new connection answered ${probe.status}`);
  else problems.push(`a new connection after ${SHUT.probeSignal} was served ${probe.status}`);

  if (!h.exited) {
    problems.push(
      `still running ${(exitDeadlineMs / 1000).toFixed(1)}s after ${SHUT.probeSignal} ` +
        `(drain ${SHUT.slowMs}ms + margin ${SHUT.exitMarginMs}ms)`,
    );
  } else if (h.exit.signal) {
    problems.push(`died by signal ${h.exit.signal} instead of exiting (${SHUT.probeSignal} not caught)`);
  } else if (h.exit.code !== SHUT.exitStatus) {
    problems.push(`exited with status ${h.exit.code}, want ${SHUT.exitStatus}`);
  } else {
    facts.push(`exited ${h.exit.code} after ${((h.exit.at - signalledAt) / 1000).toFixed(2)}s`);
  }

  return problems.length
    ? { dim, status: "fail", detail: problems.join("; ") }
    : { dim, status: "pass", detail: facts.join("; ") };
}
