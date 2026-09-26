// Shared by embed-gate.mjs and onboarding.mjs: run a CLI binary's embedded
// Studio in isolation and open it in headless Chrome.
//
// The Studio gets a temporary home (HOME and USERPROFILE: no access to the
// user's saved connections), a temporary working directory (no project
// neutron.toml) and an empty PATH, so it cannot launch a browser of its own.

import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import os from "node:os";
import path from "node:path";
import { chromium } from "playwright-core";

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export function freePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

// Starts `<cli> studio --port <free port>` and resolves once it serves /.
// Returns { base, home, log(), stop() }.
export async function startStudio(cli) {
  const home = mkdtempSync(path.join(os.tmpdir(), "studio-home-"));
  const env = { HOME: home, USERPROFILE: home, PATH: "" };
  // Windows sockets need the system root in the environment.
  for (const k of ["SystemRoot", "SYSTEMROOT", "windir"]) if (process.env[k]) env[k] = process.env[k];
  const port = await freePort();
  const child = spawn(path.resolve(cli), ["studio", "--port", String(port)], { cwd: home, env, stdio: ["ignore", "pipe", "pipe"] });
  let output = "";
  child.stdout.on("data", (d) => (output += d));
  child.stderr.on("data", (d) => (output += d));
  const stop = async () => {
    if (child.exitCode === null) {
      child.kill();
      await new Promise((r) => child.once("exit", r));
    }
    rmSync(home, { recursive: true, force: true });
  };
  const base = `http://127.0.0.1:${port}`;
  for (let i = 0; ; i++) {
    try {
      if ((await fetch(`${base}/`)).ok) break;
    } catch {}
    if (i > 150 || child.exitCode !== null) {
      await stop();
      throw new Error(`studio did not start (exit ${child.exitCode}):\n${output}`);
    }
    await sleep(100);
  }
  return { base, home, log: () => output, stop };
}

export async function launchChrome() {
  return chromium.launch(process.env.CHROME_PATH ? { executablePath: process.env.CHROME_PATH } : { channel: "chrome" });
}
