// Per-SDK boot descriptors for the conformance runner.
//
// Each descriptor declares how to (optionally) build and how to start a canonical
// "conformance app" for that SDK, plus the env var used to pin the port. The
// runner picks a free ephemeral port, boots the server, waits for /health, runs
// the contract, and tears the process down.
//
// `available()` returns null if the SDK can be booted in this environment, or a
// string reason if it cannot (missing toolchain, needs a build step, etc.).

import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const CONF = path.resolve(HERE, "..");
const REPO = path.resolve(CONF, "..");

function have(bin) {
  // `go version` / `cargo --version` both exit 0 when the toolchain is present;
  // we only care that the binary is resolvable and runnable.
  const arg = bin === "go" ? "version" : "--version";
  const r = spawnSync(bin, [arg], { stdio: "ignore" });
  return r.error == null && r.status === 0;
}

const GO_APP = path.join(CONF, "adapters/go/conformance-app");
const GO_BIN = path.join(CONF, ".build/conf-go-app");
const RUST_BIN = path.join(REPO, "rust/target/release/examples/conformance_app");
const PY_APP = path.join(CONF, "adapters/python/conformance_app.py");

function pythonBin() {
  for (const c of ["python3", "python"]) {
    const r = spawnSync(c, ["--version"], { stdio: "ignore" });
    if (r.error == null && r.status === 0) return c;
  }
  return null;
}

function pythonDepsOk(py) {
  const r = spawnSync(py, ["-c", "import starlette, pydantic, uvicorn"], { stdio: "ignore" });
  return r.error == null && r.status === 0;
}

export const SDKS = [
  {
    name: "go",
    portEnv: "PORT",
    hostEnv: "HOST",
    // Build ahead of time so boot is instant and deterministic.
    build() {
      fs.mkdirSync(path.dirname(GO_BIN), { recursive: true });
      const r = spawnSync("go", ["-C", GO_APP, "build", "-o", GO_BIN, "."], {
        stdio: "inherit",
        env: { ...process.env, GOFLAGS: "-mod=mod" },
      });
      if (r.status !== 0) throw new Error("go build failed");
    },
    cmd() {
      return { command: GO_BIN, args: [] };
    },
    available() {
      if (!have("go")) return "go toolchain not found";
      return null;
    },
  },
  {
    name: "rust",
    portEnv: "PORT",
    hostEnv: "HOST",
    build() {
      const r = spawnSync(
        "cargo",
        ["build", "--release", "--example", "conformance_app", "--manifest-path", path.join(REPO, "rust/crates/neutron/Cargo.toml")],
        { stdio: "inherit" },
      );
      if (r.status !== 0) throw new Error("cargo build failed");
    },
    cmd() {
      return { command: RUST_BIN, args: [] };
    },
    available() {
      if (!have("cargo")) return "cargo toolchain not found";
      return null;
    },
  },
  {
    name: "python",
    portEnv: "PORT",
    hostEnv: "HOST",
    build() {},
    cmd() {
      return { command: pythonBin() || "python3", args: [PY_APP] };
    },
    available() {
      const py = pythonBin();
      if (!py) return "python interpreter not found";
      if (!pythonDepsOk(py)) return "python deps missing (pip install starlette pydantic uvicorn)";
      return null;
    },
  },
];
