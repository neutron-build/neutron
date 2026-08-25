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
const TS_APP = path.join(CONF, "adapters/typescript/conformance_app.mjs");
const TS_DIST = path.join(REPO, "typescript/packages/neutron/dist/server/index.js");
const EX_APP = path.join(CONF, "adapters/elixir/conformance_app.exs");
const ZIG_APP = path.join(CONF, "adapters/zig");
const ZIG_PREFIX = path.join(CONF, ".build/conf-zig");
const ZIG_BIN = path.join(ZIG_PREFIX, "bin", "conformance-app");
// The SDK pins Zig 0.15 (build.zig.zon documents why): plain `zig` on PATH
// may be 0.16, which cannot compile the SDK. Prefer the brew keg, else
// whatever `zig` resolves to — same resolution as live/executors/zig/run.sh.
const ZIG_PINNED = "/opt/homebrew/opt/zig@0.15/bin/zig";

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

// Resolve a Zig 0.15.x toolchain: the pinned brew keg when present, else
// `zig` on PATH — but only if it is actually 0.15.x. Returns null when no
// usable toolchain exists, with the reason.
function zig15() {
  const candidates = [ZIG_PINNED, "zig"];
  for (const bin of candidates) {
    const r = spawnSync(bin, ["version"], { encoding: "utf8" });
    if (r.error != null || r.status !== 0) continue;
    const v = (r.stdout || "").trim();
    if (v.startsWith("0.15.")) return { bin, version: v };
    return { bin: null, version: v };
  }
  return { bin: null, version: null };
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
    portEnv: "NEUTRON_PORT",
    hostEnv: "NEUTRON_HOST",
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
  {
    // Web/SSR meta-framework (Hono). Implements the whole contract surface as
    // of S81 (2026-08-18): health, request-id, CORS and compression, plus RFC
    // 7807 problems, typed validation and OpenAPI 3.1. Those last three were
    // "skip-by-design for an SSR framework" here until it was checked --
    // FRAMEWORK_CONTRACT.md §2 grants no SSR exemption, so the design was
    // self-exempting from a MUST rather than scoping one.
    name: "ts",
    portEnv: "PORT",
    hostEnv: "HOST",
    build() {},
    cmd() {
      return { command: process.execPath, args: [TS_APP] };
    },
    available() {
      if (!fs.existsSync(TS_DIST)) {
        return "TS package not built (run: pnpm --filter @neutron-build/core build)";
      }
      return null;
    },
  },
  {
    name: "elixir",
    portEnv: "PORT",
    hostEnv: "HOST",
    // Mix.install compiles the path dependency on first run and caches it by
    // lockfile hash, so the build step is a no-op and the first boot is slow.
    // The runner's health wait covers it.
    build() {},
    cmd() {
      return { command: "elixir", args: [EX_APP] };
    },
    available() {
      if (!have("elixir")) return "elixir toolchain not found";
      if (!have("mix")) return "mix not found";
      return null;
    },
  },
  {
    name: "zig",
    portEnv: "NEUTRON_PORT",
    hostEnv: "NEUTRON_HOST",
    build() {
      const { bin } = zig15();
      const r = spawnSync(
        bin,
        [
          "build",
          "--cache-dir", path.join(CONF, ".build/zig-cache"),
          "--global-cache-dir", path.join(CONF, ".build/zig-global-cache"),
          "-p", ZIG_PREFIX,
        ],
        { stdio: "inherit", cwd: ZIG_APP },
      );
      if (r.status !== 0) throw new Error("zig build failed");
    },
    cmd() {
      return { command: ZIG_BIN, args: [] };
    },
    available() {
      const { bin, version } = zig15();
      if (bin) return null;
      if (version) {
        return `zig ${version} found, but the SDK pins 0.15.x (0.16 cannot compile it; see zig/build.zig.zon)`;
      }
      return "zig 0.15.x toolchain not found (expected /opt/homebrew/opt/zig@0.15/bin/zig or zig on PATH)";
    },
  },
];
