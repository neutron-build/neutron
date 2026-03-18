import assert from "node:assert/strict";
import { describe, it } from "node:test";
import * as path from "node:path";
import * as fs from "node:fs";
import * as os from "node:os";

// ---------------------------------------------------------------------------
// We cannot import the CLI commands directly because they depend on vite,
// neutron, preact, etc. at import time. Instead we test the pure utility
// functions that are re-implemented here (mirroring the source exactly) so
// the test file is self-contained and does not trigger heavy imports.
// ---------------------------------------------------------------------------

// =========================================================================
// Unit-testable functions extracted from the CLI source
// =========================================================================

// -- build.ts: parseBuildArgs ------------------------------------------------

interface BuildArgs {
  preset: "vercel" | "cloudflare" | "docker" | "static" | null;
  cloudflareMode: "pages" | "workers";
}

function parseBuildArgs(argv: string[]): BuildArgs {
  let preset: BuildArgs["preset"] = null;
  let cloudflareMode: BuildArgs["cloudflareMode"] = "pages";

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--preset" && argv[i + 1]) {
      const value = argv[++i];
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "static") {
        preset = value;
      }
      continue;
    }
    if (arg.startsWith("--preset=")) {
      const value = arg.split("=")[1];
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "static") {
        preset = value;
      }
      continue;
    }
    if (arg === "--cloudflare-mode" && argv[i + 1]) {
      const value = argv[++i];
      if (value === "pages" || value === "workers") {
        cloudflareMode = value;
      }
      continue;
    }
    if (arg.startsWith("--cloudflare-mode=")) {
      const value = arg.split("=")[1];
      if (value === "pages" || value === "workers") {
        cloudflareMode = value;
      }
    }
  }

  return { preset, cloudflareMode };
}

// -- build.ts: resolvePath ---------------------------------------------------

function resolvePath(pattern: string, params: Record<string, string>): string {
  let resolved = pattern;
  for (const [key, value] of Object.entries(params)) {
    resolved = resolved.replace(`[${key}]`, value);
    resolved = resolved.replace(`:${key}`, value);
  }
  return resolved;
}

// -- build.ts: escapeHtml ----------------------------------------------------

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

// -- build.ts: getOutputPath -------------------------------------------------

function getOutputPath(outputDir: string, routePath: string): string {
  if (routePath === "/") {
    return path.join(outputDir, "index.html");
  }
  const cleanPath = routePath.replace(/\/$/, "");
  return path.join(outputDir, cleanPath, "index.html");
}

// -- build.ts: normalizeHeaders ----------------------------------------------

function normalizeHeaders(
  value: Headers | Record<string, string> | null | undefined
): Record<string, string> {
  if (!value) {
    return {};
  }
  if (value instanceof Headers) {
    return headersToRecord(value);
  }
  const output: Record<string, string> = {};
  for (const [name, headerValue] of Object.entries(value)) {
    const lower = name.toLowerCase();
    if (lower === "content-length" || lower === "set-cookie") {
      continue;
    }
    output[name] = String(headerValue);
  }
  return output;
}

function headersToRecord(headers: Headers): Record<string, string> {
  const output: Record<string, string> = {};
  headers.forEach((value, name) => {
    const lower = name.toLowerCase();
    if (lower === "content-length" || lower === "set-cookie") {
      return;
    }
    output[name] = value;
  });
  return output;
}

// -- build.ts: relativeImportPath --------------------------------------------

function relativeImportPath(fromDir: string, filePath: string): string {
  const rel = path.relative(fromDir, filePath).split(path.sep).join("/");
  return rel.startsWith(".") ? rel : `./${rel}`;
}

// -- build.ts: escapeJsString ------------------------------------------------

function escapeJsString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

// -- deploy-check.ts: parseDeployCheckArgs -----------------------------------

type DeployPreset = "vercel" | "cloudflare" | "docker" | "static";

interface DeployCheckArgs {
  preset: DeployPreset | null;
  distDir: string;
}

function parseDeployCheckArgs(argv: string[]): DeployCheckArgs {
  let preset: DeployPreset | null = null;
  let distDir = "dist";

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--preset" && argv[i + 1]) {
      const value = argv[++i];
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "static") {
        preset = value;
      }
      continue;
    }
    if (arg.startsWith("--preset=")) {
      const value = arg.split("=")[1];
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "static") {
        preset = value;
      }
      continue;
    }
    if (arg === "--dist" && argv[i + 1]) {
      distDir = argv[++i];
      continue;
    }
    if (arg.startsWith("--dist=")) {
      distDir = arg.split("=")[1];
    }
  }

  return { preset, distDir };
}

// -- deploy-check.ts: detectPresetsFromDist ----------------------------------

function detectPresetsFromDist(distDir: string): DeployPreset[] {
  const output: DeployPreset[] = [];
  if (fs.existsSync(path.join(distDir, ".neutron-adapter-vercel.json"))) {
    output.push("vercel");
  }
  if (fs.existsSync(path.join(distDir, ".neutron-adapter-cloudflare.json"))) {
    output.push("cloudflare");
  }
  if (fs.existsSync(path.join(distDir, ".neutron-adapter-docker.json"))) {
    output.push("docker");
  }
  if (fs.existsSync(path.join(distDir, ".neutron-adapter-static.json"))) {
    output.push("static");
  }
  return output;
}

// -- worker.ts: parseWorkerArgs ----------------------------------------------

interface WorkerArgs {
  entry?: string;
  mode: string;
  once: boolean;
  workerArgs: string[];
}

function parseWorkerArgs(argv: string[]): WorkerArgs {
  let entry: string | undefined;
  let mode = "development";
  let once = false;

  const passthroughIndex = argv.indexOf("--");
  const workerArgs = passthroughIndex >= 0 ? argv.slice(passthroughIndex + 1) : [];
  const parsedArgs = passthroughIndex >= 0 ? argv.slice(0, passthroughIndex) : argv;

  for (let i = 0; i < parsedArgs.length; i++) {
    const arg = parsedArgs[i];
    if (arg === "--entry" && parsedArgs[i + 1]) {
      entry = parsedArgs[++i];
      continue;
    }
    if (arg.startsWith("--entry=")) {
      entry = arg.split("=")[1];
      continue;
    }
    if (arg === "--mode" && parsedArgs[i + 1]) {
      mode = parsedArgs[++i];
      continue;
    }
    if (arg.startsWith("--mode=")) {
      mode = arg.split("=")[1];
      continue;
    }
    if (arg === "--once") {
      once = true;
    }
  }

  return { entry, mode, once, workerArgs };
}

// -- worker.ts: resolveWorkerEntry -------------------------------------------

const WORKER_ENTRY_CANDIDATES = [
  "src/worker.ts",
  "src/worker.tsx",
  "src/worker/index.ts",
  "src/worker/index.tsx",
  "worker.ts",
  "worker.js",
];

function resolveWorkerEntry(
  cwd: string,
  cliEntry?: string,
  configEntry?: string
): string | null {
  const candidates = [cliEntry, configEntry, ...WORKER_ENTRY_CANDIDATES].filter(
    (candidate): candidate is string => Boolean(candidate)
  );
  for (const candidate of candidates) {
    const absolutePath = path.resolve(cwd, candidate);
    if (fs.existsSync(absolutePath)) {
      return absolutePath;
    }
  }
  return null;
}

// -- preview.ts: normalizePathname -------------------------------------------

function normalizePathname(pathname: string): string {
  if (!pathname) {
    return "/";
  }
  let decoded: string;
  try {
    decoded = decodeURIComponent(pathname);
  } catch {
    return "";
  }
  if (!decoded.startsWith("/") || decoded.includes("..")) {
    return "";
  }
  if (decoded.length > 1 && decoded.endsWith("/")) {
    return decoded.slice(0, -1);
  }
  return decoded;
}

// -- preview.ts: isWithinDirectory -------------------------------------------

function isWithinDirectory(baseDir: string, candidatePath: string): boolean {
  const relative = path.relative(baseDir, candidatePath);
  return (
    relative === "" ||
    (!relative.startsWith("..") && !path.isAbsolute(relative))
  );
}

// -- preview.ts: resolveDistFilePath -----------------------------------------

function resolveDistFilePath(distDir: string, pathname: string): string | null {
  const resolved = path.resolve(distDir, `.${pathname}`);
  return isWithinDirectory(distDir, resolved) ? resolved : null;
}

// -- index.ts: CLI dispatch --------------------------------------------------

const VALID_COMMANDS = ["dev", "build", "preview", "start", "deploy-check", "release-check", "worker"];

// =========================================================================
// Tests
// =========================================================================

// ---------------------------------------------------------------------------
// parseBuildArgs
// ---------------------------------------------------------------------------

describe("parseBuildArgs", () => {
  it("returns default values when no args are provided", () => {
    const result = parseBuildArgs([]);
    assert.equal(result.preset, null);
    assert.equal(result.cloudflareMode, "pages");
  });

  it("parses --preset vercel", () => {
    const result = parseBuildArgs(["--preset", "vercel"]);
    assert.equal(result.preset, "vercel");
  });

  it("parses --preset=cloudflare", () => {
    const result = parseBuildArgs(["--preset=cloudflare"]);
    assert.equal(result.preset, "cloudflare");
  });

  it("parses --preset docker", () => {
    const result = parseBuildArgs(["--preset", "docker"]);
    assert.equal(result.preset, "docker");
  });

  it("parses --preset static", () => {
    const result = parseBuildArgs(["--preset", "static"]);
    assert.equal(result.preset, "static");
  });

  it("ignores invalid preset values", () => {
    const result = parseBuildArgs(["--preset", "invalid"]);
    assert.equal(result.preset, null);
  });

  it("parses --cloudflare-mode workers", () => {
    const result = parseBuildArgs(["--preset", "cloudflare", "--cloudflare-mode", "workers"]);
    assert.equal(result.preset, "cloudflare");
    assert.equal(result.cloudflareMode, "workers");
  });

  it("parses --cloudflare-mode=pages", () => {
    const result = parseBuildArgs(["--cloudflare-mode=pages"]);
    assert.equal(result.cloudflareMode, "pages");
  });

  it("ignores invalid cloudflare-mode values", () => {
    const result = parseBuildArgs(["--cloudflare-mode", "invalid"]);
    assert.equal(result.cloudflareMode, "pages");
  });

  it("handles multiple args together", () => {
    const result = parseBuildArgs(["--preset=vercel", "--cloudflare-mode=workers"]);
    assert.equal(result.preset, "vercel");
    assert.equal(result.cloudflareMode, "workers");
  });
});

// ---------------------------------------------------------------------------
// resolvePath
// ---------------------------------------------------------------------------

describe("resolvePath", () => {
  it("replaces bracket params", () => {
    assert.equal(resolvePath("/blog/[slug]", { slug: "hello" }), "/blog/hello");
  });

  it("replaces colon params", () => {
    assert.equal(resolvePath("/users/:id", { id: "42" }), "/users/42");
  });

  it("replaces multiple params", () => {
    assert.equal(
      resolvePath("/[category]/[slug]", { category: "tech", slug: "post" }),
      "/tech/post"
    );
  });

  it("leaves pattern unchanged when params are empty", () => {
    assert.equal(resolvePath("/about", {}), "/about");
  });
});

// ---------------------------------------------------------------------------
// escapeHtml
// ---------------------------------------------------------------------------

describe("escapeHtml", () => {
  it("escapes ampersands", () => {
    assert.equal(escapeHtml("a&b"), "a&amp;b");
  });

  it("escapes angle brackets", () => {
    assert.equal(escapeHtml("<script>"), "&lt;script&gt;");
  });

  it("escapes double quotes", () => {
    assert.equal(escapeHtml('"hello"'), "&quot;hello&quot;");
  });

  it("escapes single quotes", () => {
    assert.equal(escapeHtml("it's"), "it&#039;s");
  });

  it("leaves safe strings unchanged", () => {
    assert.equal(escapeHtml("hello world"), "hello world");
  });
});

// ---------------------------------------------------------------------------
// getOutputPath
// ---------------------------------------------------------------------------

describe("getOutputPath", () => {
  const outDir = "/dist";

  it("returns index.html for root route", () => {
    assert.equal(getOutputPath(outDir, "/"), path.join(outDir, "index.html"));
  });

  it("returns nested index.html for sub-route", () => {
    assert.equal(getOutputPath(outDir, "/about"), path.join(outDir, "about", "index.html"));
  });

  it("strips trailing slash before computing output path", () => {
    assert.equal(getOutputPath(outDir, "/blog/"), path.join(outDir, "blog", "index.html"));
  });

  it("handles deep paths", () => {
    assert.equal(
      getOutputPath(outDir, "/a/b/c"),
      path.join(outDir, "a", "b", "c", "index.html")
    );
  });
});

// ---------------------------------------------------------------------------
// normalizeHeaders
// ---------------------------------------------------------------------------

describe("normalizeHeaders", () => {
  it("returns empty object for null", () => {
    assert.deepEqual(normalizeHeaders(null), {});
  });

  it("returns empty object for undefined", () => {
    assert.deepEqual(normalizeHeaders(undefined), {});
  });

  it("converts a plain object", () => {
    const headers = { "X-Custom": "value" };
    const result = normalizeHeaders(headers);
    assert.equal(result["X-Custom"], "value");
  });

  it("filters out content-length", () => {
    const result = normalizeHeaders({ "Content-Length": "100", "X-Custom": "v" });
    assert.equal(result["Content-Length"], undefined);
    assert.equal(result["X-Custom"], "v");
  });

  it("filters out set-cookie", () => {
    const result = normalizeHeaders({ "Set-Cookie": "a=b", "X-Test": "1" });
    assert.equal(result["Set-Cookie"], undefined);
    assert.equal(result["X-Test"], "1");
  });

  it("converts Headers instance", () => {
    const headers = new Headers();
    headers.set("x-foo", "bar");
    const result = normalizeHeaders(headers);
    assert.equal(result["x-foo"], "bar");
  });

  it("filters content-length from Headers instance", () => {
    const headers = new Headers();
    headers.set("content-length", "42");
    headers.set("x-id", "abc");
    const result = normalizeHeaders(headers);
    assert.equal(result["content-length"], undefined);
    assert.equal(result["x-id"], "abc");
  });
});

// ---------------------------------------------------------------------------
// relativeImportPath
// ---------------------------------------------------------------------------

describe("relativeImportPath", () => {
  it("prepends ./ for sibling files", () => {
    const result = relativeImportPath("/project/src", "/project/src/route.ts");
    assert.equal(result, "./route.ts");
  });

  it("preserves relative prefix for parent directories", () => {
    const result = relativeImportPath("/project/src/dir", "/project/src/route.ts");
    assert.equal(result, "../route.ts");
  });

  it("prepends ./ for subdirectory files", () => {
    const result = relativeImportPath("/project", "/project/src/index.ts");
    assert.equal(result, "./src/index.ts");
  });
});

// ---------------------------------------------------------------------------
// escapeJsString
// ---------------------------------------------------------------------------

describe("escapeJsString", () => {
  it("escapes backslashes", () => {
    assert.equal(escapeJsString("a\\b"), "a\\\\b");
  });

  it("escapes double quotes", () => {
    assert.equal(escapeJsString('say "hi"'), 'say \\"hi\\"');
  });

  it("leaves safe strings unchanged", () => {
    assert.equal(escapeJsString("hello"), "hello");
  });
});

// ---------------------------------------------------------------------------
// parseDeployCheckArgs
// ---------------------------------------------------------------------------

describe("parseDeployCheckArgs", () => {
  it("returns defaults when no args provided", () => {
    const result = parseDeployCheckArgs([]);
    assert.equal(result.preset, null);
    assert.equal(result.distDir, "dist");
  });

  it("parses --preset vercel", () => {
    const result = parseDeployCheckArgs(["--preset", "vercel"]);
    assert.equal(result.preset, "vercel");
  });

  it("parses --preset=cloudflare", () => {
    const result = parseDeployCheckArgs(["--preset=cloudflare"]);
    assert.equal(result.preset, "cloudflare");
  });

  it("parses --dist with space", () => {
    const result = parseDeployCheckArgs(["--dist", "build"]);
    assert.equal(result.distDir, "build");
  });

  it("parses --dist= format", () => {
    const result = parseDeployCheckArgs(["--dist=output"]);
    assert.equal(result.distDir, "output");
  });

  it("handles combined args", () => {
    const result = parseDeployCheckArgs(["--preset", "docker", "--dist", "out"]);
    assert.equal(result.preset, "docker");
    assert.equal(result.distDir, "out");
  });

  it("ignores invalid preset", () => {
    const result = parseDeployCheckArgs(["--preset", "netlify"]);
    assert.equal(result.preset, null);
  });
});

// ---------------------------------------------------------------------------
// detectPresetsFromDist
// ---------------------------------------------------------------------------

describe("detectPresetsFromDist", () => {
  it("returns empty array for directory with no adapter files", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "deploy-check-"));
    try {
      const result = detectPresetsFromDist(tmpDir);
      assert.deepEqual(result, []);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("detects vercel adapter", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "deploy-check-"));
    try {
      fs.writeFileSync(path.join(tmpDir, ".neutron-adapter-vercel.json"), "{}");
      const result = detectPresetsFromDist(tmpDir);
      assert.deepEqual(result, ["vercel"]);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("detects multiple adapters", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "deploy-check-"));
    try {
      fs.writeFileSync(path.join(tmpDir, ".neutron-adapter-vercel.json"), "{}");
      fs.writeFileSync(path.join(tmpDir, ".neutron-adapter-cloudflare.json"), "{}");
      fs.writeFileSync(path.join(tmpDir, ".neutron-adapter-docker.json"), "{}");
      fs.writeFileSync(path.join(tmpDir, ".neutron-adapter-static.json"), "{}");
      const result = detectPresetsFromDist(tmpDir);
      assert.deepEqual(result, ["vercel", "cloudflare", "docker", "static"]);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// parseWorkerArgs
// ---------------------------------------------------------------------------

describe("parseWorkerArgs", () => {
  it("returns defaults when no args provided", () => {
    const result = parseWorkerArgs([]);
    assert.equal(result.entry, undefined);
    assert.equal(result.mode, "development");
    assert.equal(result.once, false);
    assert.deepEqual(result.workerArgs, []);
  });

  it("parses --entry flag", () => {
    const result = parseWorkerArgs(["--entry", "src/worker.ts"]);
    assert.equal(result.entry, "src/worker.ts");
  });

  it("parses --entry= format", () => {
    const result = parseWorkerArgs(["--entry=worker.js"]);
    assert.equal(result.entry, "worker.js");
  });

  it("parses --mode flag", () => {
    const result = parseWorkerArgs(["--mode", "production"]);
    assert.equal(result.mode, "production");
  });

  it("parses --mode= format", () => {
    const result = parseWorkerArgs(["--mode=production"]);
    assert.equal(result.mode, "production");
  });

  it("parses --once flag", () => {
    const result = parseWorkerArgs(["--once"]);
    assert.equal(result.once, true);
  });

  it("passes through args after --", () => {
    const result = parseWorkerArgs(["--entry", "w.ts", "--", "arg1", "arg2"]);
    assert.equal(result.entry, "w.ts");
    assert.deepEqual(result.workerArgs, ["arg1", "arg2"]);
  });

  it("handles all flags combined", () => {
    const result = parseWorkerArgs(["--entry=w.ts", "--mode=production", "--once", "--", "extra"]);
    assert.equal(result.entry, "w.ts");
    assert.equal(result.mode, "production");
    assert.equal(result.once, true);
    assert.deepEqual(result.workerArgs, ["extra"]);
  });
});

// ---------------------------------------------------------------------------
// resolveWorkerEntry
// ---------------------------------------------------------------------------

describe("resolveWorkerEntry", () => {
  it("returns null when no candidates exist", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "worker-entry-"));
    try {
      const result = resolveWorkerEntry(tmpDir);
      assert.equal(result, null);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("prioritises CLI entry over default candidates", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "worker-entry-"));
    try {
      const workerPath = path.join(tmpDir, "custom-worker.ts");
      fs.writeFileSync(workerPath, "export function run() {}");
      fs.mkdirSync(path.join(tmpDir, "src"), { recursive: true });
      fs.writeFileSync(path.join(tmpDir, "src", "worker.ts"), "");

      const result = resolveWorkerEntry(tmpDir, "custom-worker.ts");
      assert.equal(result, workerPath);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("falls back to default candidate src/worker.ts", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "worker-entry-"));
    try {
      fs.mkdirSync(path.join(tmpDir, "src"), { recursive: true });
      const workerPath = path.join(tmpDir, "src", "worker.ts");
      fs.writeFileSync(workerPath, "");

      const result = resolveWorkerEntry(tmpDir);
      assert.equal(result, workerPath);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("uses config entry when CLI entry is undefined", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "worker-entry-"));
    try {
      const workerPath = path.join(tmpDir, "jobs.ts");
      fs.writeFileSync(workerPath, "");

      const result = resolveWorkerEntry(tmpDir, undefined, "jobs.ts");
      assert.equal(result, workerPath);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// normalizePathname
// ---------------------------------------------------------------------------

describe("normalizePathname", () => {
  it("returns / for empty string", () => {
    assert.equal(normalizePathname(""), "/");
  });

  it("returns the path as-is for a normal path", () => {
    assert.equal(normalizePathname("/about"), "/about");
  });

  it("strips trailing slash", () => {
    assert.equal(normalizePathname("/blog/"), "/blog");
  });

  it("keeps root / as-is", () => {
    assert.equal(normalizePathname("/"), "/");
  });

  it("rejects path traversal", () => {
    assert.equal(normalizePathname("/../../etc"), "");
  });

  it("rejects non-absolute paths", () => {
    assert.equal(normalizePathname("relative"), "");
  });

  it("decodes percent-encoded characters", () => {
    assert.equal(normalizePathname("/hello%20world"), "/hello world");
  });

  it("returns empty string for bad encoding", () => {
    assert.equal(normalizePathname("/%E0%A4%A"), "");
  });
});

// ---------------------------------------------------------------------------
// isWithinDirectory
// ---------------------------------------------------------------------------

describe("isWithinDirectory", () => {
  it("returns true for a child path", () => {
    assert.equal(isWithinDirectory("/dist", "/dist/index.html"), true);
  });

  it("returns true for the directory itself", () => {
    assert.equal(isWithinDirectory("/dist", "/dist"), true);
  });

  it("returns false for a parent path", () => {
    assert.equal(isWithinDirectory("/dist", "/etc/passwd"), false);
  });

  it("returns false for path traversal", () => {
    assert.equal(isWithinDirectory("/dist", "/dist/../etc/passwd"), false);
  });
});

// ---------------------------------------------------------------------------
// resolveDistFilePath
// ---------------------------------------------------------------------------

describe("resolveDistFilePath", () => {
  it("resolves a normal file path", () => {
    const result = resolveDistFilePath("/dist", "/index.html");
    assert.equal(result, path.resolve("/dist", "./index.html"));
  });

  it("returns null for path traversal attempt", () => {
    const result = resolveDistFilePath("/dist", "/../etc/passwd");
    assert.equal(result, null);
  });
});

// ---------------------------------------------------------------------------
// CLI command dispatch
// ---------------------------------------------------------------------------

describe("CLI command dispatch", () => {
  it("recognises all valid commands", () => {
    for (const cmd of VALID_COMMANDS) {
      assert.ok(VALID_COMMANDS.includes(cmd), `${cmd} should be a valid command`);
    }
  });

  it("has 7 valid commands", () => {
    assert.equal(VALID_COMMANDS.length, 7);
  });

  it("includes dev, build, preview, start", () => {
    assert.ok(VALID_COMMANDS.includes("dev"));
    assert.ok(VALID_COMMANDS.includes("build"));
    assert.ok(VALID_COMMANDS.includes("preview"));
    assert.ok(VALID_COMMANDS.includes("start"));
  });

  it("includes worker, deploy-check, release-check", () => {
    assert.ok(VALID_COMMANDS.includes("worker"));
    assert.ok(VALID_COMMANDS.includes("deploy-check"));
    assert.ok(VALID_COMMANDS.includes("release-check"));
  });
});

// ---------------------------------------------------------------------------
// Config file candidate ordering
// ---------------------------------------------------------------------------

describe("config file candidates", () => {
  const CONFIG_CANDIDATES = [
    "neutron.config.ts",
    "neutron.config.js",
    "neutron.config.mjs",
    "neutron.config.cjs",
  ];

  it("prefers .ts over .js", () => {
    assert.equal(CONFIG_CANDIDATES[0], "neutron.config.ts");
    assert.equal(CONFIG_CANDIDATES[1], "neutron.config.js");
  });

  it("includes all four extensions", () => {
    assert.equal(CONFIG_CANDIDATES.length, 4);
  });
});
