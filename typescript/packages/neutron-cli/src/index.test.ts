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
  preset: "vercel" | "cloudflare" | "docker" | "netlify" | "static" | null;
  cloudflareMode: "pages" | "workers";
}

function parseBuildArgs(argv: string[]): BuildArgs {
  let preset: BuildArgs["preset"] = null;
  let cloudflareMode: BuildArgs["cloudflareMode"] = "pages";

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--preset" && argv[i + 1]) {
      const value = argv[++i];
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "netlify" || value === "static") {
        preset = value;
      }
      continue;
    }
    if (arg.startsWith("--preset=")) {
      const value = arg.split("=")[1];
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "netlify" || value === "static") {
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

// -- build.ts: resolvePath — now imported from ./commands/static-paths.js --

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

type DeployPreset = "vercel" | "cloudflare" | "docker" | "netlify" | "static";

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
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "netlify" || value === "static") {
        preset = value;
      }
      continue;
    }
    if (arg.startsWith("--preset=")) {
      const value = arg.split("=")[1];
      if (value === "vercel" || value === "cloudflare" || value === "docker" || value === "netlify" || value === "static") {
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
  if (fs.existsSync(path.join(distDir, ".neutron-adapter-netlify.json"))) {
    output.push("netlify");
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

const VALID_COMMANDS = ["dev", "build", "preview", "start", "deploy-check", "release-check", "worker", "init"];

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

  it("parses --preset netlify", () => {
    const result = parseBuildArgs(["--preset", "netlify"]);
    assert.equal(result.preset, "netlify");
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

  it("parses --preset netlify", () => {
    const result = parseDeployCheckArgs(["--preset", "netlify"]);
    assert.equal(result.preset, "netlify");
  });

  it("ignores invalid preset", () => {
    const result = parseDeployCheckArgs(["--preset", "deno"]);
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
      fs.writeFileSync(path.join(tmpDir, ".neutron-adapter-netlify.json"), "{}");
      fs.writeFileSync(path.join(tmpDir, ".neutron-adapter-static.json"), "{}");
      const result = detectPresetsFromDist(tmpDir);
      assert.deepEqual(result, ["vercel", "cloudflare", "docker", "netlify", "static"]);
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

  it("has 8 valid commands", () => {
    assert.equal(VALID_COMMANDS.length, 8);
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

  it("includes init", () => {
    assert.ok(VALID_COMMANDS.includes("init"));
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

// ---------------------------------------------------------------------------
// Static build path traversal — imported from source, not mirrored.
//
// getStaticPaths params are untrusted (a CMS can return any slug) and are
// substituted verbatim into the output-path join. The guard below is what the
// static writer applies before every param-derived write; these tests exercise
// the real module (split out of build.ts, which imports vite at load time).
// ---------------------------------------------------------------------------

import { resolvePath, isUnsafeResolvedPath } from "./commands/static-paths.js";

describe("static build path traversal (getStaticPaths params)", () => {
  it("flags a named param carrying parent-directory traversal", () => {
    const resolved = resolvePath("/blog/[slug]", { slug: "../../evil" });
    assert.equal(resolved, "/blog/../../evil");
    assert.equal(isUnsafeResolvedPath(resolved), true);
  });

  it("flags a splat param carrying traversal", () => {
    const resolved = resolvePath("/docs/*", { "*": "a/../../../etc/passwd" });
    assert.equal(isUnsafeResolvedPath(resolved), true);
  });

  it("flags backslash traversal (Windows separator form)", () => {
    assert.equal(isUnsafeResolvedPath("/blog/..\\evil"), true);
  });

  it("flags non-absolute resolved paths", () => {
    assert.equal(isUnsafeResolvedPath("blog/hello"), true);
  });

  it("allows normal slugs and multi-segment splats", () => {
    assert.equal(isUnsafeResolvedPath(resolvePath("/blog/[slug]", { slug: "hello" })), false);
    assert.equal(isUnsafeResolvedPath(resolvePath("/docs/*", { "*": "a/b/c" })), false);
    assert.equal(isUnsafeResolvedPath(resolvePath("/", {})), false);
  });

  it("allows legal dot-containing slugs the runtime guard would reject", () => {
    // Segment-precise: "a..b" and "v1.2..3" are legal path segments, not "..".
    assert.equal(isUnsafeResolvedPath("/a..b"), false);
    assert.equal(isUnsafeResolvedPath("/v1.2..3/x"), false);
  });
});

// ---------------------------------------------------------------------------
// Client-entry resolution — imported from source, not mirrored.
//
// The rest of this file re-implements the functions it tests. That is exactly
// how the bug below survived: a mirrored copy can pass while the real function
// is broken. `client-entry.ts` was split out of build.ts (which imports vite at
// load time) so the real implementation can be exercised here.
// ---------------------------------------------------------------------------

import { extractClientEntryScriptSrc } from "./client-entry.js";

// ---------------------------------------------------------------------------
// Gated static routes (A-020) — imported from source, not mirrored.
//
// The production build refuses `mode: "static"` + middleware exactly as
// core's renderStatic does. `gated-static.ts` is split out of build.ts
// (which imports vite at load time) for the same reason static-paths.ts is.
// ---------------------------------------------------------------------------

import { findGatedStaticRoutes, gatedStaticRoutesError } from "./commands/gated-static.js";
import type { Route } from "@neutron-build/core";
import { decodeSerializedPayload } from "@neutron-build/core";

/** Minimal Route factory — only the fields the gate reads. */
function gateRoute(overrides: {
  path: string;
  id?: string;
  file?: string;
  mode?: "static" | "app";
  parentId?: string | null;
  hasMiddleware?: boolean;
  hasMethodHandlers?: boolean;
  hasDefaultExport?: boolean;
}): Route {
  return {
    id: overrides.id ?? `route:${overrides.path}`,
    path: overrides.path,
    file: overrides.file ?? `/src/routes${overrides.path}.tsx`,
    params: [],
    config: { mode: overrides.mode ?? "static" },
    parentId: overrides.parentId ?? null,
    hasMiddleware: overrides.hasMiddleware,
    hasMethodHandlers: overrides.hasMethodHandlers ?? false,
    hasDefaultExport: overrides.hasDefaultExport ?? true,
  };
}

describe("findGatedStaticRoutes (A-020 build-side gate)", () => {
  it("flags a static route gated by its own middleware, naming the file", () => {
    const gated = findGatedStaticRoutes([
      gateRoute({ path: "/secret", file: "/src/routes/secret.tsx", hasMiddleware: true }),
    ]);
    assert.equal(gated.length, 1);
    assert.equal(gated[0].route.path, "/secret");
    assert.equal(gated[0].gate.id, gated[0].route.id);
    assert.match(gatedStaticRoutesError(gated), /\/secret — \/src\/routes\/secret\.tsx exports `middleware`/);
    assert.match(gatedStaticRoutesError(gated), /mode: "static"/);
  });

  it("flags a static route gated by an ancestor layout, naming the layout", () => {
    const gated = findGatedStaticRoutes([
      gateRoute({
        id: "route:admin/_layout.tsx",
        path: "/admin",
        file: "/src/routes/admin/_layout.tsx",
        hasMiddleware: true,
      }),
      gateRoute({
        path: "/admin/report",
        file: "/src/routes/admin/report.tsx",
        parentId: "route:admin/_layout.tsx",
      }),
    ]);
    assert.equal(gated.length, 1);
    assert.equal(gated[0].route.path, "/admin/report");
    assert.equal(gated[0].gate.file, "/src/routes/admin/_layout.tsx");
    assert.match(gatedStaticRoutesError(gated), /\/admin\/report — its layout/);
  });

  it("does not flag ungated static routes or gated app routes", () => {
    assert.deepEqual(
      findGatedStaticRoutes([
        gateRoute({ path: "/public" }),
        gateRoute({ path: "/live", mode: "app", hasMiddleware: true }),
      ]),
      []
    );
  });

  it("does not flag gated resource routes — they are never prerendered, gates run per request", () => {
    assert.deepEqual(
      findGatedStaticRoutes([
        gateRoute({
          path: "/api/stats",
          file: "/src/routes/api/stats.ts",
          hasMiddleware: true,
          hasMethodHandlers: true,
          hasDefaultExport: false,
        }),
      ]),
      []
    );
  });
});

// `init.ts` is imported from source for the same reason client-entry.ts is:
// it pulls in no build tooling (only the create-neutron scaffold library), so
// the real parseInitArgs can be exercised instead of a mirror.
import { parseInitArgs } from "./commands/init.js";

describe("parseInitArgs", () => {
  it("returns default options when no args provided", () => {
    const result = parseInitArgs([]);
    assert.ok(result);
    assert.equal(result.targetDir, "neutron-app");
    assert.equal(result.template, "basic");
    assert.equal(result.runtime, "preact");
  });

  it("parses project name as positional arg", () => {
    const result = parseInitArgs(["my-app"]);
    assert.ok(result);
    assert.equal(result.targetDir, "my-app");
  });

  it("parses --template flag with space", () => {
    const result = parseInitArgs(["--template", "full"]);
    assert.ok(result);
    assert.equal(result.template, "full");
  });

  it("parses --template= format", () => {
    const result = parseInitArgs(["--template=marketing"]);
    assert.ok(result);
    assert.equal(result.template, "marketing");
  });

  it("returns null for unsupported template", () => {
    const result = parseInitArgs(["--template", "invalid"]);
    assert.equal(result, null);
  });

  it("returns null for unsupported template in = format", () => {
    const result = parseInitArgs(["--template=nonexistent"]);
    assert.equal(result, null);
  });

  it("parses --runtime react-compat", () => {
    const result = parseInitArgs(["--runtime", "react-compat"]);
    assert.ok(result);
    assert.equal(result.runtime, "react-compat");
  });

  it("returns null for unsupported runtime", () => {
    const result = parseInitArgs(["--runtime", "solid"]);
    assert.equal(result, null);
  });

  it("handles all options combined", () => {
    const result = parseInitArgs(["my-project", "--template", "docs", "--runtime", "react-compat"]);
    assert.ok(result);
    assert.equal(result.targetDir, "my-project");
    assert.equal(result.template, "docs");
    assert.equal(result.runtime, "react-compat");
  });
});

// ---------------------------------------------------------------------------
// Preview bind address — imported from source, not mirrored.
//
// `neutron preview` must bind loopback by default; the network is an explicit
// opt-in via --host or server.host in neutron.config. The resolution logic is
// split into src/lib/preview-host.ts (preview.ts imports vite at load time) so
// the real implementation is exercised here.
// ---------------------------------------------------------------------------

import {
  DEFAULT_PREVIEW_HOST,
  isLoopbackHost,
  parsePreviewArgs,
  resolveNetworkHost,
  resolvePreviewHost,
} from "./lib/preview-host.js";

describe("parsePreviewArgs", () => {
  it("returns no host when no args are provided", () => {
    const result = parsePreviewArgs([]);
    assert.equal(result.host, undefined);
  });

  it("parses --host with space", () => {
    const result = parsePreviewArgs(["--host", "0.0.0.0"]);
    assert.equal(result.host, "0.0.0.0");
  });

  it("parses --host= format", () => {
    const result = parsePreviewArgs(["--host=192.168.1.10"]);
    assert.equal(result.host, "192.168.1.10");
  });

  it("ignores unrelated flags", () => {
    const result = parsePreviewArgs(["--port", "4173"]);
    assert.equal(result.host, undefined);
  });
});

describe("resolvePreviewHost", () => {
  it("binds loopback by default (no flag, no config host)", () => {
    const host = resolvePreviewHost(undefined, undefined);
    assert.equal(host, "127.0.0.1");
    assert.equal(isLoopbackHost(host), true);
  });

  it("defaults to a loopback address", () => {
    assert.equal(isLoopbackHost(DEFAULT_PREVIEW_HOST), true);
  });

  it("opts in to a network bind via --host", () => {
    const host = resolvePreviewHost("0.0.0.0", undefined);
    assert.equal(host, "0.0.0.0");
    assert.equal(isLoopbackHost(host), false);
  });

  it("opts in to a network bind via config server.host", () => {
    const host = resolvePreviewHost(undefined, "10.0.0.5");
    assert.equal(host, "10.0.0.5");
  });

  it("lets --host override config server.host", () => {
    const host = resolvePreviewHost("192.168.1.10", "10.0.0.5");
    assert.equal(host, "192.168.1.10");
  });
});

describe("isLoopbackHost", () => {
  it("classifies 127.0.0.1 and the 127.0.0.0/8 range as loopback", () => {
    assert.equal(isLoopbackHost("127.0.0.1"), true);
    assert.equal(isLoopbackHost("127.0.0.8"), true);
  });

  it("classifies localhost and ::1 as loopback", () => {
    assert.equal(isLoopbackHost("localhost"), true);
    assert.equal(isLoopbackHost("::1"), true);
  });

  it("classifies wildcard and LAN addresses as non-loopback", () => {
    assert.equal(isLoopbackHost("0.0.0.0"), false);
    assert.equal(isLoopbackHost("::"), false);
    assert.equal(isLoopbackHost("192.168.1.5"), false);
  });
});

describe("resolveNetworkHost", () => {
  it("returns null (no network URL) when bound to loopback", () => {
    assert.equal(resolveNetworkHost("127.0.0.1"), null);
    assert.equal(resolveNetworkHost("localhost"), null);
  });

  it("returns the bound address itself for an explicit non-loopback host", () => {
    assert.equal(resolveNetworkHost("10.1.2.3"), "10.1.2.3");
  });

  it("resolves a LAN address for the wildcard bind", () => {
    const result = resolveNetworkHost("0.0.0.0");
    assert.notEqual(result, "0.0.0.0");
    assert.equal(result === null || isLoopbackHost(result) === false, true);
  });
});

function scratchDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), "neutron-client-entry-"));
}

function writeManifest(dir: string, manifest: unknown): void {
  const viteDir = path.join(dir, ".vite");
  fs.mkdirSync(viteDir, { recursive: true });
  fs.writeFileSync(path.join(viteDir, "manifest.json"), JSON.stringify(manifest));
}

describe("extractClientEntryScriptSrc", () => {
  // The live failure this exists for. covely.io served
  // `const o={mode:"app"};export{o as config};` — 42 bytes — as its entire
  // client runtime, because `src/routes/index.tsx` emits `assets/index-<hash>.js`
  // just like the real entry, and the old glob sorted and took the last one.
  // The app never hydrated, no link was marked static, the speculation rules
  // scoped to those marks matched nothing, and every navigation was a cold full
  // page load. Nothing errored, because the page is server-rendered.
  it("does not mistake a route chunk for the client entry", () => {
    const dir = scratchDir();
    fs.mkdirSync(path.join(dir, "assets"), { recursive: true });
    // Hash chosen so the ROUTE chunk sorts last — what the old code returned.
    fs.writeFileSync(path.join(dir, "assets/index-Aaaaaaaa.js"), "// real runtime");
    fs.writeFileSync(path.join(dir, "assets/index-Zzzzzzzz.js"), "const o={mode:\"app\"};");
    writeManifest(dir, {
      "src/main.tsx": { file: "assets/index-Aaaaaaaa.js", isEntry: true },
      "src/routes/index.tsx": { file: "assets/index-Zzzzzzzz.js", isEntry: true },
    });

    assert.equal(extractClientEntryScriptSrc(dir), "/assets/index-Aaaaaaaa.js");
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("ignores a route chunk carrying the client-route query", () => {
    const dir = scratchDir();
    fs.mkdirSync(path.join(dir, "assets"), { recursive: true });
    writeManifest(dir, {
      "src/main.tsx": { file: "assets/index-real.js", isEntry: true },
      "src/routes/index.tsx?neutron-client-route": {
        file: "assets/index-zzzz.js",
        isEntry: true,
      },
    });

    assert.equal(extractClientEntryScriptSrc(dir), "/assets/index-real.js");
    fs.rmSync(dir, { recursive: true, force: true });
  });

  // Without a manifest the glob is all there is, but it must still not return a
  // chunk that no manifest vouched for as an entry when one is available.
  it("falls back to the filename glob when no manifest exists", () => {
    const dir = scratchDir();
    fs.mkdirSync(path.join(dir, "assets"), { recursive: true });
    fs.writeFileSync(path.join(dir, "assets/index-abc123.js"), "// runtime");

    assert.equal(extractClientEntryScriptSrc(dir), "/assets/index-abc123.js");
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("returns null when there is nothing to load", () => {
    const dir = scratchDir();
    assert.equal(extractClientEntryScriptSrc(dir), null);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  // A non-JS entry (the html itself) must not be returned as a module script.
  it("skips non-JS entries", () => {
    const dir = scratchDir();
    fs.mkdirSync(path.join(dir, "assets"), { recursive: true });
    fs.writeFileSync(path.join(dir, "assets/index-abc123.js"), "// runtime");
    writeManifest(dir, {
      "index.html": { file: "index.html", isEntry: true },
      "src/main.tsx": { file: "assets/index-abc123.js", isEntry: true },
    });

    assert.equal(extractClientEntryScriptSrc(dir), "/assets/index-abc123.js");
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// Shared config loader — imported from source.
//
// dev/build/start/preview/worker all load neutron.config through this one
// function. It only imports vite (no build tooling at load time beyond that),
// so the real implementation is exercised here against fixture config files.
// ---------------------------------------------------------------------------

import { loadNeutronConfig } from "./lib/config.js";

describe("loadNeutronConfig (shared command loader)", () => {
  function loaderDir(): string {
    return fs.mkdtempSync(path.join(os.tmpdir(), "neutron-loader-"));
  }

  it("returns an empty config when no config file exists", async () => {
    const dir = loaderDir();
    try {
      const config = await loadNeutronConfig(dir);
      assert.deepEqual(config, {});
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("loads object configs with the fields the commands read", async () => {
    const dir = loaderDir();
    fs.writeFileSync(
      path.join(dir, "neutron.config.mjs"),
      [
        "export default {",
        '  server: { port: 4321, host: "10.0.0.5" },',
        '  routes: { redirects: [{ source: "/old", destination: "/new" }] },',
        '  worker: { entry: "jobs.ts" },',
        "}",
      ].join("\n")
    );
    try {
      const config = await loadNeutronConfig(dir);
      assert.equal(config.server?.port, 4321);
      assert.equal(config.server?.host, "10.0.0.5");
      assert.equal(config.worker?.entry, "jobs.ts");
      assert.equal(config.routes?.redirects?.[0]?.source, "/old");
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("passes command and mode through to function configs", async () => {
    const dir = loaderDir();
    fs.writeFileSync(
      path.join(dir, "neutron.config.mjs"),
      'export default ({ command, mode }) => ({ runtime: command + ":" + mode });'
    );
    try {
      const serve = await loadNeutronConfig(dir, { mode: "development" });
      assert.equal((serve as Record<string, unknown>).runtime, "serve:development");
      const build = await loadNeutronConfig(dir, { command: "build" });
      assert.equal((build as Record<string, unknown>).runtime, "build:production");
      const serveProd = await loadNeutronConfig(dir);
      assert.equal((serveProd as Record<string, unknown>).runtime, "serve:production");
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("prefers .ts over the other config candidates", async () => {
    const dir = loaderDir();
    fs.writeFileSync(path.join(dir, "neutron.config.mjs"), "export default { adapter: 'netlify' };");
    fs.writeFileSync(path.join(dir, "neutron.config.ts"), "export default { adapter: 'vercel' };");
    try {
      const config = await loadNeutronConfig(dir);
      assert.equal(config.adapter, "vercel");
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Port/host resolution (FRAMEWORK_CONTRACT.md §6): flag > env > config > default
// ---------------------------------------------------------------------------

import { resolveListenAddress } from "./lib/listen.js";

describe("resolveListenAddress", () => {
  const base = { argv: [] as string[], env: {} as Record<string, string | undefined> };

  it("defaults to 3000 and the caller's default host", () => {
    assert.deepEqual(resolveListenAddress({ ...base, defaultHost: "0.0.0.0" }), {
      port: 3000,
      host: "0.0.0.0",
      portExplicit: false,
    });
    assert.equal(resolveListenAddress(base).host, undefined);
  });

  it("config beats the default", () => {
    const r = resolveListenAddress({ ...base, config: { port: 4321, host: "10.0.0.5" }, defaultHost: "0.0.0.0" });
    assert.deepEqual(r, { port: 4321, host: "10.0.0.5", portExplicit: true });
  });

  it("NEUTRON_PORT / NEUTRON_HOST beat config", () => {
    const r = resolveListenAddress({
      ...base,
      env: { NEUTRON_PORT: "5555", NEUTRON_HOST: "127.0.0.1" },
      config: { port: 4321, host: "10.0.0.5" },
    });
    assert.deepEqual(r, { port: 5555, host: "127.0.0.1", portExplicit: true });
  });

  it("flags beat the environment (both flag spellings)", () => {
    const env = { NEUTRON_PORT: "5555", NEUTRON_HOST: "127.0.0.1" };
    assert.deepEqual(
      resolveListenAddress({ ...base, argv: ["--port", "6000", "--host", "::1"], env }),
      { port: 6000, host: "::1", portExplicit: true }
    );
    assert.deepEqual(
      resolveListenAddress({ ...base, argv: ["--port=6001", "--host=localhost"], env }),
      { port: 6001, host: "localhost", portExplicit: true }
    );
  });

  it("empty env vars count as unset", () => {
    const r = resolveListenAddress({ ...base, env: { NEUTRON_PORT: "", NEUTRON_HOST: " " }, config: { port: 4321 } });
    assert.equal(r.port, 4321);
    assert.equal(r.host, undefined);
  });

  for (const bad of ["abc", "0", "65536", "80a", "-1", "3.5", "1e3"]) {
    it(`rejects NEUTRON_PORT=${JSON.stringify(bad)} instead of falling back`, () => {
      assert.throws(
        () => resolveListenAddress({ ...base, env: { NEUTRON_PORT: bad }, config: { port: 4321 } }),
        (error: Error) => error.message.includes(`Invalid NEUTRON_PORT "${bad}"`) && error.message.includes("1 and 65535")
      );
    });
  }

  it("rejects an invalid --port the same way", () => {
    assert.throws(() => resolveListenAddress({ ...base, argv: ["--port", "nope"] }), /Invalid --port "nope"/);
  });

  it("accepts the range edges", () => {
    assert.equal(resolveListenAddress({ ...base, env: { NEUTRON_PORT: "1" } }).port, 1);
    assert.equal(resolveListenAddress({ ...base, env: { NEUTRON_PORT: "65535" } }).port, 65535);
  });
});

// ---------------------------------------------------------------------------
// `neutron-ts dev` / `start` as real processes: §6 env binding, §7 /health in
// dev, §8 graceful drain on SIGTERM in dev.
// ---------------------------------------------------------------------------

import { spawn, type ChildProcess } from "node:child_process";
import * as net from "node:net";
import { fileURLToPath, pathToFileURL } from "node:url";

const CLI_PACKAGE_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CLI_BIN = path.join(CLI_PACKAGE_DIR, "bin", "neutron-ts.mjs");

const SLOW_ROUTE = `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  await new Promise((resolve) => setTimeout(resolve, 2000));
  return { done: true };
}
export default function Page({ data }) {
  return h("div", null, "slow-done=" + String(data.done));
}
`;

function makeCliFixture(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-cli-proc-"));
  fs.mkdirSync(path.join(dir, "src", "routes"), { recursive: true });
  fs.writeFileSync(path.join(dir, "src", "routes", "slow.tsx"), SLOW_ROUTE);
  // Resolve preact / @neutron-build/core through the CLI package's own deps.
  fs.symlinkSync(path.join(CLI_PACKAGE_DIR, "node_modules"), path.join(dir, "node_modules"), "dir");
  return dir;
}

async function freePort(): Promise<number> {
  return await new Promise((resolve, reject) => {
    const socket = net.createServer();
    socket.listen(0, "127.0.0.1", () => {
      const address = socket.address();
      if (!address || typeof address === "string") return reject(new Error("no port"));
      const { port } = address;
      socket.close(() => resolve(port));
    });
    socket.on("error", reject);
  });
}

interface CliProcess {
  child: ChildProcess;
  output: () => string;
  exited: Promise<number | null>;
}

function runCli(command: string, cwd: string, env: Record<string, string>, args: string[] = []): CliProcess {
  const child = spawn(process.execPath, [CLI_BIN, command, ...args], {
    cwd,
    env: { ...process.env, ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout!.on("data", (chunk) => (output += chunk));
  child.stderr!.on("data", (chunk) => (output += chunk));
  const exited = new Promise<number | null>((resolve) => child.on("exit", (code) => resolve(code)));
  return { child, output: () => output, exited };
}

async function waitForHealth(url: string, proc: CliProcess, timeoutMs = 30_000): Promise<Response> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (proc.child.exitCode !== null) {
      throw new Error(`process exited early (${proc.child.exitCode}):\n${proc.output()}`);
    }
    try {
      return await fetch(url);
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 150));
    }
  }
  throw new Error(`timed out waiting for ${url}:\n${proc.output()}`);
}

describe("neutron-ts dev (process)", () => {
  it("binds NEUTRON_PORT, serves /health, and drains an in-flight request on SIGTERM", { timeout: 90_000 }, async () => {
    const dir = makeCliFixture();
    const port = await freePort();
    const proc = runCli("dev", dir, { NEUTRON_PORT: String(port), NEUTRON_HOST: "127.0.0.1" });
    try {
      const health = await waitForHealth(`http://127.0.0.1:${port}/health`, proc);
      assert.equal(health.status, 200);
      assert.deepEqual(await health.json(), { status: "ok", nucleus: "unconfigured", version: "0.1.0" });

      // Warm the route once so the timed request measures the loader, not Vite's first compile.
      await (await fetch(`http://127.0.0.1:${port}/slow`)).text();

      const inFlight = fetch(`http://127.0.0.1:${port}/slow`).then(async (res) => ({
        status: res.status,
        body: await res.text(),
      }));
      await new Promise((resolve) => setTimeout(resolve, 400));
      proc.child.kill("SIGTERM");

      const result = await inFlight;
      assert.equal(result.status, 200);
      assert.match(result.body, /slow-done=true/);
      assert.equal(await proc.exited, 0, proc.output());
    } finally {
      proc.child.kill("SIGKILL");
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("fails fast with a clear error on an invalid NEUTRON_PORT", { timeout: 60_000 }, async () => {
    const dir = makeCliFixture();
    const proc = runCli("dev", dir, { NEUTRON_PORT: "not-a-port" });
    try {
      assert.equal(await proc.exited, 1);
      assert.match(proc.output(), /neutron-ts dev: Invalid NEUTRON_PORT "not-a-port"/);
    } finally {
      proc.child.kill("SIGKILL");
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("neutron-ts start (process)", () => {
  it("binds NEUTRON_PORT, and --port overrides it", { timeout: 90_000 }, async () => {
    const dir = makeCliFixture();
    const envPort = await freePort();
    const flagPort = await freePort();
    const fromEnv = runCli("start", dir, { NEUTRON_PORT: String(envPort), NEUTRON_HOST: "127.0.0.1" });
    const fromFlag = runCli("start", dir, { NEUTRON_PORT: String(envPort), NEUTRON_HOST: "127.0.0.1" }, [
      "--port",
      String(flagPort),
    ]);
    try {
      const envHealth = await waitForHealth(`http://127.0.0.1:${envPort}/health`, fromEnv);
      assert.equal(envHealth.status, 200);
      const flagHealth = await waitForHealth(`http://127.0.0.1:${flagPort}/health`, fromFlag);
      assert.equal(flagHealth.status, 200);

      fromEnv.child.kill("SIGTERM");
      fromFlag.child.kill("SIGTERM");
      assert.equal(await fromEnv.exited, 0, fromEnv.output());
      assert.equal(await fromFlag.exited, 0, fromFlag.output());
    } finally {
      fromEnv.child.kill("SIGKILL");
      fromFlag.child.kill("SIGKILL");
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("fails fast with a clear error on an out-of-range NEUTRON_PORT", { timeout: 60_000 }, async () => {
    const dir = makeCliFixture();
    const proc = runCli("start", dir, { NEUTRON_PORT: "70000" });
    try {
      assert.equal(await proc.exited, 1);
      assert.match(proc.output(), /neutron-ts start: Invalid NEUTRON_PORT "70000"/);
    } finally {
      proc.child.kill("SIGKILL");
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// -- public-files.ts: the public/ list the SSR server serves root URLs from --
// Split out of build.ts (vite at load time) for the same reason as
// static-paths.ts. The server serves ONLY these paths from the dist root, so
// the Vite manifest, build metadata and preset server bundles stay private.

import { writePublicFilesMetadata } from "./commands/public-files.js";

describe("writePublicFilesMetadata", () => {
  it("lists every copied public/ file (nested) and none of the build output", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-public-files-"));
    try {
      const publicDir = path.join(root, "public");
      const dist = path.join(root, "dist");
      fs.mkdirSync(path.join(publicDir, "icons"), { recursive: true });
      fs.writeFileSync(path.join(publicDir, "favicon.svg"), "<svg/>");
      fs.writeFileSync(path.join(publicDir, "icons", "logo.png"), "png");
      fs.mkdirSync(path.join(dist, "icons"), { recursive: true });
      fs.mkdirSync(path.join(dist, ".vite"), { recursive: true });
      fs.mkdirSync(path.join(dist, "server", "node"), { recursive: true });
      fs.writeFileSync(path.join(dist, "favicon.svg"), "<svg/>");
      fs.writeFileSync(path.join(dist, "icons", "logo.png"), "png");
      fs.writeFileSync(path.join(dist, ".vite", "manifest.json"), "{}");
      fs.writeFileSync(path.join(dist, ".neutron-client-entry.json"), "{}");
      fs.writeFileSync(path.join(dist, "server", "node", "index.mjs"), "");

      writePublicFilesMetadata(dist, publicDir);
      const written = JSON.parse(
        fs.readFileSync(path.join(dist, ".neutron-public-files.json"), "utf-8")
      );
      assert.deepEqual(written, { files: ["/favicon.svg", "/icons/logo.png"] });
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("follows symlinked files and directories in public/, as Vite's copy does", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-public-files-"));
    try {
      const shared = path.join(root, "shared");
      const publicDir = path.join(root, "public");
      const dist = path.join(root, "dist");
      fs.mkdirSync(path.join(shared, "img"), { recursive: true });
      fs.writeFileSync(path.join(shared, "img", "hero.jpg"), "jpg");
      fs.writeFileSync(path.join(shared, "robots.txt"), "");
      fs.mkdirSync(publicDir, { recursive: true });
      fs.symlinkSync(path.join(shared, "img"), path.join(publicDir, "img"));
      fs.symlinkSync(path.join(shared, "robots.txt"), path.join(publicDir, "robots.txt"));
      fs.mkdirSync(path.join(dist, "img"), { recursive: true });
      fs.writeFileSync(path.join(dist, "img", "hero.jpg"), "jpg");
      fs.writeFileSync(path.join(dist, "robots.txt"), "");

      writePublicFilesMetadata(dist, publicDir);
      const written = JSON.parse(
        fs.readFileSync(path.join(dist, ".neutron-public-files.json"), "utf-8")
      );
      assert.deepEqual(written, { files: ["/img/hero.jpg", "/robots.txt"] });
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("writes an empty list when there is no public/ dir (the list still governs)", () => {
    const dist = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-public-files-"));
    try {
      writePublicFilesMetadata(dist, null);
      const written = JSON.parse(
        fs.readFileSync(path.join(dist, ".neutron-public-files.json"), "utf-8")
      );
      assert.deepEqual(written, { files: [] });
    } finally {
      fs.rmSync(dist, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// `neutron-ts build` as a real process.
// ---------------------------------------------------------------------------

// node_modules links every package the CLI resolves, so `files` can add fake
// packages under node_modules/ next to them.
function makeBuildFixture(files: Record<string, string | Buffer>): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-cli-build-"));
  const cliModules = path.join(CLI_PACKAGE_DIR, "node_modules");
  fs.mkdirSync(path.join(dir, "node_modules"));
  for (const entry of fs.readdirSync(cliModules)) {
    if (entry.startsWith(".")) continue;
    fs.symlinkSync(path.join(cliModules, entry), path.join(dir, "node_modules", entry), "dir");
  }
  for (const [name, source] of Object.entries(files)) {
    fs.mkdirSync(path.join(dir, path.dirname(name)), { recursive: true });
    fs.writeFileSync(path.join(dir, name), source);
  }
  return dir;
}

/**
 * The client build walks core's root entry (Link, hooks). It must not carry
 * Vite, esbuild, shiki, … — not even as orphan dynamic-import chunks the
 * browser never loads, which still ship in dist/assets.
 */
function assertNoServerCodeInClientBuild(dir: string): void {
  const manifest = JSON.parse(fs.readFileSync(path.join(dir, "dist", ".vite", "manifest.json"), "utf-8")) as Record<
    string,
    unknown
  >;
  const serverModules = Object.keys(manifest).filter((key) =>
    /node_modules\/(\.pnpm\/[^/]+\/node_modules\/)?(vite|esbuild|rolldown|postcss|jiti|tsx|lightningcss|shiki|@shikijs\/[^/]+|typescript|@mdx-js\/mdx|ws)\//.test(key)
  );
  assert.deepEqual(serverModules, [], "server-only modules in the client build");
  const assetsDir = path.join(dir, "dist", "assets");
  for (const file of fs.readdirSync(assetsDir).filter((name) => name.endsWith(".js"))) {
    const code = fs.readFileSync(path.join(assetsDir, file), "utf-8");
    for (const marker of ["createServerModuleRunner", "vite:build-import-analysis", "createHighlighterCore"]) {
      assert.ok(!code.includes(marker), `${file} carries server code (${marker})`);
    }
  }
}

async function runBuild(dir: string): Promise<string> {
  const proc = runCli("build", dir, {});
  const code = await proc.exited;
  assert.equal(code, 0, proc.output());
  return proc.output();
}

const LOCATION_PAGE = `
import { h } from "preact";
import { useLocation } from "@neutron-build/core";
export const config = { mode: "static" };
export default function Page() {
  return h("p", { id: "page" }, "page-path=" + useLocation().pathname);
}
`;

describe("neutron-ts build (process): prerendered router context", () => {
  it("prerenders each static page with its own useLocation().pathname", { timeout: 180_000 }, async () => {
    const dir = makeBuildFixture({
      "src/routes/_layout.tsx": `
import { h } from "preact";
import { useLocation } from "@neutron-build/core";
export default function Layout({ children }) {
  return h("main", null, h("span", null, "layout-path=" + useLocation().pathname), children);
}
`,
      "src/routes/index.tsx": LOCATION_PAGE,
      "src/routes/about.tsx": LOCATION_PAGE,
      "src/routes/posts/[slug].tsx": `${LOCATION_PAGE}
export function getStaticPaths() { return [{ params: { slug: "hello" } }]; }
`,
    });
    try {
      await runBuild(dir);
      const read = (rel: string) => fs.readFileSync(path.join(dir, "dist", rel), "utf-8");
      for (const [file, expected] of [
        ["index.html", "/"],
        ["about/index.html", "/about"],
        ["posts/hello/index.html", "/posts/hello"],
      ] as const) {
        const html = read(file);
        assert.ok(html.includes(`page-path=${expected}<`), `${file}: ${html}`);
        assert.ok(html.includes(`layout-path=${expected}<`), `${file}: ${html}`);
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("neutron-ts build (process): react-compat prerender", () => {
  it("prerenders a static page that imports core under react-compat", { timeout: 180_000 }, async () => {
    // react-compat processes every dependency through the prerender runner;
    // core's CommonJS-only deps (acorn-jsx under @mdx-js/mdx) must stay
    // native or the page dies with "require is not defined".
    const dir = makeBuildFixture({
      "package.json": JSON.stringify({ name: "react-compat-prerender", type: "module" }),
      "neutron.config.ts": `export default { runtime: "react-compat" };\n`,
      "src/routes/index.tsx": `
import { useMemo } from "react";
import { useLocation } from "@neutron-build/core";
export const config = { mode: "static" };
export default function Page() {
  const path = useLocation().pathname;
  return <p>{useMemo(() => "compat-path=" + path, [path])}</p>;
}
`,
    });
    try {
      await runBuild(dir);
      const html = fs.readFileSync(path.join(dir, "dist", "index.html"), "utf-8");
      assert.ok(html.includes("compat-path=/<"), html);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("neutron-ts build + start (process): prebuilt server bundle", () => {
  it("serves every route kind from dist without constructing a Vite server", { timeout: 240_000 }, async () => {
    const dir = makeBuildFixture({
      "package.json": JSON.stringify({ name: "server-bundle-fixture", type: "module" }),
      "neutron.config.ts": `export default { runtime: "react-compat" };\n`,
      "index.html": '<!doctype html><html><body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body></html>\n',
      "src/main.tsx": `import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";
registerRoutes(routes);
void init();
`,
      "src/middleware.ts": `
export default async function middleware(request, context, next) {
  if (new URL(request.url).pathname.startsWith("/private")) return new Response("denied", { status: 401 });
  const response = await next();
  response.headers.set("x-global", "ran");
  return response;
}
`,
      "src/routes/_layout.tsx": `
import { useLocation } from "@neutron-build/core";
export default function Layout({ children }) {
  return <main><span>layout-path={useLocation().pathname}</span>{children}</main>;
}
`,
      "src/routes/index.tsx": `
import { useState, useMemo, use, createContext, Suspense } from "react";
export const config = { mode: "app" };
export async function loader() { return { n: 41 }; }
const Tone = createContext("tone-default");
const later = new Promise((resolve) => setTimeout(() => resolve("use-later"), 20));
function Later() { return <i>{use(later)}</i>; }
function ToneReader() { return <b>{use(Tone)}</b>; }
export default function Home({ data }) {
  const [count] = useState(data.n + 1);
  return <Tone.Provider value="use-context"><p>{useMemo(() => "hooks-count=" + count, [count])}</p><ToneReader /><Suspense fallback="wait"><Later /></Suspense></Tone.Provider>;
}
`,
      "src/routes/about.tsx": `
export const config = { mode: "static" };
export default function About() { return <p>about-static</p>; }
`,
      "src/routes/not-found.tsx": `
export const config = { mode: "app" };
export default function NotFound() { return <p>custom-missing</p>; }
`,
      "src/routes/api/meta.ts": `
export function GET() { return Response.json({ url: import.meta.url }); }
`,
      // Records every import of "vite" from core's server module: the only
      // one is createSsrServer's, so the file stays empty unless a Vite
      // server is constructed.
      "probe/register.mjs": `import { register } from "node:module";
register(new URL("./hooks.mjs", import.meta.url));
`,
      "probe/hooks.mjs": `import { appendFileSync } from "node:fs";
export async function resolve(specifier, context, next) {
  if (specifier === "vite" && context.parentURL?.endsWith("/dist/server/index.js")) {
    appendFileSync(process.env.NEUTRON_VITE_PROBE, context.parentURL + "\\n");
  }
  return next(specifier, context);
}
`,
    });
    const probeLog = path.join(dir, "probe", "vite-imports.log");
    fs.writeFileSync(probeLog, "");
    const startEnv = {
      NODE_OPTIONS: `--import ${path.join(dir, "probe", "register.mjs")}`,
      NEUTRON_VITE_PROBE: probeLog,
      NEUTRON_HOST: "127.0.0.1",
    };
    const startAndProbe = async () => {
      const port = await freePort();
      const proc = runCli("start", dir, { ...startEnv, NEUTRON_PORT: String(port) });
      try {
        await waitForHealth(`http://127.0.0.1:${port}/health`, proc);
        const get = async (p: string) => {
          const res = await fetch(`http://127.0.0.1:${port}${p}`, { headers: { accept: "text/html" } });
          return { status: res.status, global: res.headers.get("x-global"), body: await res.text() };
        };
        return {
          home: await get("/"),
          about: await get("/about"),
          meta: await get("/api/meta"),
          missing: await get("/nope"),
          denied: await get("/private/x"),
          output: proc.output(),
        };
      } finally {
        proc.child.kill("SIGTERM");
        await proc.exited;
      }
    };
    try {
      await runBuild(dir);
      assertNoServerCodeInClientBuild(dir);
      const manifest = path.join(dir, "dist", ".neutron-server-bundle.json");
      assert.deepEqual(JSON.parse(fs.readFileSync(manifest, "utf-8")), {
        format: 1,
        entry: "server/start/entry.js",
        runtime: "react-compat",
      });

      const bundled = await startAndProbe();
      assert.match(bundled.output, /Modules: prebuilt server bundle/, bundled.output);
      assert.equal(fs.readFileSync(probeLog, "utf-8"), "", "a Vite server was constructed");
      assert.equal(bundled.home.status, 200);
      assert.ok(bundled.home.body.includes("hooks-count=42"), bundled.home.body);
      // React 19 use() from "react" (react-compat): a context, and a promise
      // through Suspense in the streamed document.
      assert.ok(bundled.home.body.includes("<b>use-context</b>"), bundled.home.body);
      assert.ok(bundled.home.body.includes("<i>use-later</i>"), bundled.home.body);
      assert.ok(bundled.home.body.includes("layout-path=/<"), bundled.home.body);
      assert.equal(bundled.home.global, "ran");
      assert.equal(bundled.about.status, 200);
      assert.ok(bundled.about.body.includes("about-static"), bundled.about.body);
      assert.equal(bundled.meta.status, 200);
      assert.equal(bundled.meta.global, "ran");
      // import.meta.url still names the source module, as under Vite and Node.
      assert.deepEqual(JSON.parse(bundled.meta.body), {
        url: pathToFileURL(fs.realpathSync(path.join(dir, "src", "routes", "api", "meta.ts"))).href,
      });
      assert.equal(bundled.missing.status, 404);
      assert.ok(bundled.missing.body.includes("custom-missing"), bundled.missing.body);
      assert.equal(bundled.denied.status, 401);

      // Without the manifest the same dist runs on the Vite runtime — and the
      // probe sees it, so its silence above is meaningful.
      fs.rmSync(manifest);
      const viaVite = await startAndProbe();
      assert.match(viaVite.output, /Modules: transformed through Vite/, viaVite.output);
      assert.notEqual(fs.readFileSync(probeLog, "utf-8"), "");
      assert.ok(viaVite.home.body.includes("hooks-count=42"), viaVite.home.body);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  // pnpm keeps core's own dependencies (shiki, @mdx-js/mdx, zod, …) as
  // siblings of core's REAL directory. With resolve.preserveSymlinks the
  // resolver stays on the linked path, from which those siblings' own deps
  // (shiki → @shikijs/*) are invisible, so bundling them failed and start
  // fell back to Vite. They stay external, resolved from core's real path.
  it("builds with preserveSymlinks + pnpm-linked core and keeps core's own deps out of the bundle", { timeout: 240_000 }, async () => {
    const dir = makeBuildFixture({
      "package.json": JSON.stringify({ name: "server-bundle-symlinks-fixture", type: "module" }),
      "neutron.config.ts": `export default { runtime: "react-compat" };\n`,
      "vite.config.ts": `export default { resolve: { preserveSymlinks: true } };\n`,
      "index.html": '<!doctype html><html><body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body></html>\n',
      "src/main.tsx": `import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";
registerRoutes(routes);
void init();
`,
      "src/routes/index.tsx": `
import { useState } from "react";
import { useLocation } from "@neutron-build/core";
export const config = { mode: "app" };
export default function Home() {
  const [n] = useState(7);
  return <p>{"linked-core-" + n + "-" + useLocation().pathname}</p>;
}
`,
      // z is core's zod: not resolvable from this app root, so it can only
      // load through the bundle's importer-resolved alias.
      "src/routes/api/zod.ts": `
import { z } from "@neutron-build/core";
export function GET() { return Response.json({ parsed: z.object({ a: z.string() }).parse({ a: "zod-ok" }).a }); }
`,
    });
    try {
      const output = await runBuild(dir);
      assert.doesNotMatch(output, /Server bundle failed to build/, output);
      const manifest = path.join(dir, "dist", ".neutron-server-bundle.json");
      assert.ok(fs.existsSync(manifest), output);

      assertNoServerCodeInClientBuild(dir);

      const outDir = path.join(dir, "dist", "server", "start");
      const code = fs
        .readdirSync(outDir)
        .filter((file) => file.endsWith(".js"))
        .map((file) => fs.readFileSync(path.join(outDir, file), "utf-8"))
        .join("\n");
      for (const marker of ["createHighlighterCore", "ZodError"]) {
        assert.ok(!code.includes(marker), `core dependency code (${marker}) was bundled`);
      }
      // Imported by core, resolved from core's real directory (not from this
      // app root, which cannot see them).
      for (const alias of ["shiki", "mdx-js__mdx", "zod"]) {
        assert.match(code, new RegExp(`"@neutron-external/${alias}-[0-9a-f]{8}"`));
      }
      assert.ok(code.length < 2_000_000, `server bundle is ${code.length} bytes`);

      const port = await freePort();
      const proc = runCli("start", dir, { NEUTRON_HOST: "127.0.0.1", NEUTRON_PORT: String(port) });
      try {
        await waitForHealth(`http://127.0.0.1:${port}/health`, proc);
        assert.match(proc.output(), /Modules: prebuilt server bundle/, proc.output());
        const home = await fetch(`http://127.0.0.1:${port}/`, { headers: { accept: "text/html" } });
        const html = await home.text();
        assert.equal(home.status, 200, html);
        assert.ok(html.includes("linked-core-7-/"), html);
        const zod = await fetch(`http://127.0.0.1:${port}/api/zod`);
        assert.deepEqual(await zod.json(), { parsed: "zod-ok" });
      } finally {
        proc.child.kill("SIGTERM");
        await proc.exited;
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  // F-P4-1: mounts and the WebSocket handler declared in src/server.entry.ts
  // are compiled into the start bundle, so a module they import and a route
  // imports is one instance (a host-built mount loaded a second copy).
  it("start: src/server.entry.ts mounts, routes and websocket share one module instance from the bundle", { timeout: 240_000 }, async () => {
    const dir = makeBuildFixture({
      "package.json": JSON.stringify({ name: "server-entry-fixture", type: "module" }),
      "index.html": '<!doctype html><html><body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body></html>\n',
      "src/main.tsx": `import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";
registerRoutes(routes);
void init();
`,
      "src/lib/counter.ts": "let n = 0;\nexport const next = () => ++n;\n",
      "src/server.entry.ts": `
import { Hono } from "hono";
import { defineServerEntry } from "@neutron-build/core";
import { next } from "./lib/counter";
const api = new Hono();
api.get("/count", (c) => c.json({ n: next() }));
export default defineServerEntry({
  mount: { "/api": api },
  websocket: { path: "/ws", attach(wss) { wss.on("connection", (socket) => socket.send(String(next()))); } },
});
`,
      "src/routes/page.tsx": `
import { h } from "preact";
import { next } from "../lib/counter";
export const config = { mode: "app" };
export async function loader() { return { n: next() }; }
export default function Page({ data }) { return h("p", null, "n=" + data.n); }
`,
    });
    // The app's own hono (core's copy, linked in like every other package here).
    fs.symlinkSync(
      fs.realpathSync(path.join(CLI_PACKAGE_DIR, "node_modules", "@neutron-build", "core", "node_modules", "hono")),
      path.join(dir, "node_modules", "hono"),
      "dir"
    );
    try {
      const output = await runBuild(dir);
      assert.doesNotMatch(output, /Server bundle failed to build/, output);
      const port = await freePort();
      const proc = runCli("start", dir, { NEUTRON_HOST: "127.0.0.1", NEUTRON_PORT: String(port) });
      try {
        await waitForHealth(`http://127.0.0.1:${port}/health`, proc);
        assert.match(proc.output(), /Modules: prebuilt server bundle/, proc.output());
        const base = `http://127.0.0.1:${port}`;
        const first = await (await fetch(`${base}/api/count`)).json();
        const page = await (await fetch(`${base}/page`, { headers: { accept: "text/html" } })).text();
        const second = await (await fetch(`${base}/api/count`)).json();
        // Two copies of the counter would count 1, 1, 2.
        assert.deepEqual(first, { n: 1 }, proc.output());
        assert.ok(page.includes("n=2"), page);
        assert.deepEqual(second, { n: 3 });
        const message = await new Promise<string>((resolve, reject) => {
          const socket = new WebSocket(`ws://127.0.0.1:${port}/ws`);
          const timer = setTimeout(() => reject(new Error("no websocket message")), 10_000);
          socket.onmessage = (event) => {
            clearTimeout(timer);
            socket.close();
            resolve(String(event.data));
          };
          socket.onerror = () => {
            clearTimeout(timer);
            reject(new Error("websocket failed"));
          };
        });
        assert.equal(message, "4");
      } finally {
        proc.child.kill("SIGTERM");
        await proc.exited;
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  // The docker image context is dist/ alone (no node_modules beside it).
  // With pnpm-linked core + preserveSymlinks the docker runtime bundle failed
  // to build (gray-matter's section-matter is invisible from core's linked
  // path), and a bundle that kept core's deps external would have had nothing
  // to load them from in the image. The build now keeps them external, as
  // start does, and ships them into dist/node_modules as real directories.
  it("docker preset: builds with preserveSymlinks + pnpm-linked core and the context runs alone", { timeout: 240_000 }, async () => {
    const dir = makeBuildFixture({
      "package.json": JSON.stringify({ name: "docker-symlinks-fixture", type: "module" }),
      "neutron.config.ts": `export default { runtime: "react-compat" };\n`,
      "vite.config.ts": `export default { resolve: { preserveSymlinks: true } };\n`,
      "index.html": '<!doctype html><html><body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body></html>\n',
      "src/main.tsx": `import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";
registerRoutes(routes);
void init();
`,
      // z is core's zod: resolvable only from core's real directory, so in
      // the image it can only come from the shipped dependency set.
      "src/routes/probe.tsx": `
import { useState } from "react";
import { useLocation, z } from "@neutron-build/core";
export const config = { mode: "app" };
export async function loader() { return { parsed: z.object({ a: z.string() }).parse({ a: "zod-ok" }).a }; }
export default function Probe({ data }) {
  const [n] = useState(7);
  return <p>{"docker-" + n + "-" + useLocation().pathname + "-" + data.parsed}</p>;
}
`,
    });
    const context = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-docker-context-"));
    try {
      const build = runCli("build", dir, {}, ["--preset", "docker"]);
      assert.equal(await build.exited, 0, build.output());
      const dist = path.join(dir, "dist");

      // The dependency set is in the context, as real directories.
      assert.ok(fs.existsSync(path.join(dist, "node_modules", "section-matter", "package.json")), build.output());
      const runtimeDir = path.join(dist, "server", "node");
      const aliases = fs.readdirSync(path.join(runtimeDir, "node_modules", "@neutron-external"));
      for (const alias of ["gray-matter", "zod"]) {
        assert.ok(aliases.some((name) => name.startsWith(`${alias}-`)), aliases.join(", "));
      }
      const symlinks: string[] = [];
      const walk = (current: string) => {
        for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
          const full = path.join(current, entry.name);
          if (entry.isSymbolicLink()) symlinks.push(full);
          else if (entry.isDirectory()) walk(full);
        }
      };
      walk(path.join(dist, "node_modules"));
      walk(runtimeDir);
      assert.deepEqual(symlinks, []);
      // And it is not excluded from the image.
      const dockerignore = fs.readFileSync(path.join(dist, ".dockerignore"), "utf-8");
      assert.ok(!dockerignore.split("\n").includes("node_modules"), dockerignore);

      // The context alone, outside any tree with node_modules, serves the page.
      fs.cpSync(dist, context, { recursive: true });
      const port = await freePort();
      const child = spawn(process.execPath, ["server.mjs"], {
        cwd: context,
        env: { ...process.env, PORT: String(port), NODE_PATH: "" },
        stdio: ["ignore", "pipe", "pipe"],
      });
      let output = "";
      child.stdout!.on("data", (chunk) => (output += chunk));
      child.stderr!.on("data", (chunk) => (output += chunk));
      const exited = new Promise((resolve) => child.on("exit", resolve));
      try {
        const deadline = Date.now() + 30_000;
        let html = "";
        while (Date.now() < deadline) {
          assert.equal(child.exitCode, null, output);
          try {
            const res = await fetch(`http://127.0.0.1:${port}/probe`, { headers: { accept: "text/html" } });
            html = await res.text();
            assert.equal(res.status, 200, html + output);
            break;
          } catch (error) {
            if (error instanceof assert.AssertionError) throw error;
            await new Promise((resolve) => setTimeout(resolve, 150));
          }
        }
        assert.ok(html.includes("docker-7-/probe-zod-ok"), html + output);
      } finally {
        child.kill("SIGTERM");
        await exited;
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
      fs.rmSync(context, { recursive: true, force: true });
    }
  });

  // Rolldown writes the chunks before it reports a resolve error; the dead
  // tree then sat under the app root, where the Vite runtime start falls
  // back to scans it (Tailwind source detection: +120 MB boot RSS on the
  // Fylun port).
  it("leaves no partial dist/server/start behind when the bundle fails", { timeout: 240_000 }, async () => {
    const dir = makeBuildFixture({
      "package.json": JSON.stringify({ name: "server-bundle-failure-fixture", type: "module" }),
      "neutron.config.ts": `export default { runtime: "react-compat" };\n`,
      "index.html": '<!doctype html><html><body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body></html>\n',
      "src/main.tsx": `import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";
registerRoutes(routes);
void init();
`,
      "src/routes/index.tsx": `
export const config = { mode: "app" };
export default function Home() { return <p>fallback-home</p>; }
`,
      // Resource routes are in no client build or prerender: only the server
      // bundle resolves this import.
      "src/routes/api/broken.ts": `
import missing from "neutron-missing-package-xyz";
export function GET() { return Response.json({ v: String(missing) }); }
`,
    });
    try {
      const output = await runBuild(dir);
      assert.match(output, /Server bundle failed to build/, output);
      assert.ok(!fs.existsSync(path.join(dir, "dist", ".neutron-server-bundle.json")));
      assert.ok(!fs.existsSync(path.join(dir, "dist", "server", "start")), "partial server bundle left in dist");

      const port = await freePort();
      const proc = runCli("start", dir, { NEUTRON_HOST: "127.0.0.1", NEUTRON_PORT: String(port) });
      try {
        await waitForHealth(`http://127.0.0.1:${port}/health`, proc);
        assert.match(proc.output(), /Modules: transformed through Vite/, proc.output());
        const home = await fetch(`http://127.0.0.1:${port}/`, { headers: { accept: "text/html" } });
        assert.ok((await home.text()).includes("fallback-home"));
      } finally {
        proc.child.kill("SIGTERM");
        await proc.exited;
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("neutron-ts build (process): static CSS pass", () => {
  it("does not bundle server-only dependencies, and still collects dependency CSS", { timeout: 180_000 }, async () => {
    const dir = makeBuildFixture({
      "package.json": JSON.stringify({ name: "css-pass-fixture", type: "module" }),
      // Node loads this fine (the addon is only required when called); a
      // browser bundle has to load the binary and cannot.
      "node_modules/server-native/package.json": JSON.stringify({ name: "server-native", main: "index.js" }),
      "node_modules/server-native/index.js": `exports.load = () => require("./out/addon");\n`,
      "node_modules/server-native/out/addon.node": Buffer.from([0x7f, 0x45, 0x4c, 0x46, 0xff, 0xfe, 0x00, 0x80]),
      "node_modules/ui-kit/package.json": JSON.stringify({ name: "ui-kit", main: "index.js" }),
      "node_modules/ui-kit/index.js": "exports.x = 1;\n",
      "node_modules/ui-kit/style.css": ".from-ui-kit { color: blue; }\n",
      "node_modules/css-main/package.json": JSON.stringify({ name: "css-main", main: "index.css" }),
      "node_modules/css-main/index.css": ".from-css-main { color: green; }\n",
      "src/routes/index.tsx": `
import { h } from "preact";
import { load } from "server-native";
import "ui-kit/style.css";
import "css-main";
import "./page.css";
export const config = { mode: "static" };
export default function Page() {
  return h("p", null, "loader is " + typeof load);
}
`,
      "src/routes/page.css": ".from-page { color: red; }\n",
      "src/routes/api/native.ts": `
import { load } from "server-native";
export function GET() { return Response.json({ kind: typeof load }); }
`,
    });
    try {
      await runBuild(dir);
      assert.ok(
        fs.readFileSync(path.join(dir, "dist", "index.html"), "utf-8").includes("loader is function")
      );
      const assets = path.join(dir, "dist", "assets");
      const css = fs
        .readdirSync(assets)
        .filter((f) => f.endsWith(".css"))
        .map((f) => fs.readFileSync(path.join(assets, f), "utf-8"))
        .join("\n");
      for (const rule of [".from-page", ".from-ui-kit", ".from-css-main"]) {
        assert.ok(css.includes(rule), `${rule} missing from emitted CSS:\n${css}`);
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("neutron-ts build (process): static CSS assets", () => {
  it("emits url() fonts as hashed files instead of inlining them", { timeout: 180_000 }, async () => {
    // Over the 4 KiB default inline limit, like any real font face.
    const font = Buffer.alloc(16 * 1024, 7);
    const dir = makeBuildFixture({
      "src/routes/index.tsx": `
import { h } from "preact";
import "./fonts.css";
export const config = { mode: "static" };
export default function Page() { return h("p", null, "fonts"); }
`,
      "src/routes/fonts.css":
        '@font-face { font-family: "Probe"; src: url("./probe.woff2") format("woff2"); }\n' +
        "p { font-family: Probe; }\n",
      "src/routes/probe.woff2": font,
    });
    try {
      await runBuild(dir);
      const assets = path.join(dir, "dist", "assets");
      const files = fs.readdirSync(assets);
      const fontFile = files.find((f) => /^probe-[\w-]+\.woff2$/.test(f));
      assert.ok(fontFile, `no hashed woff2 emitted: ${files.join(", ")}`);
      assert.deepEqual(fs.readFileSync(path.join(assets, fontFile)), font);
      const css = files
        .filter((f) => f.endsWith(".css"))
        .map((f) => fs.readFileSync(path.join(assets, f), "utf-8"))
        .join("\n");
      assert.ok(css.includes(`url(/assets/${fontFile})`), css);
      assert.ok(!css.includes("data:font"), "font inlined as a data: URI");
      // The pass's JS is deleted, so it must not be recorded as a client entry.
      assert.equal(fs.existsSync(path.join(dir, "dist", ".neutron-client-entry.json")), false);
      assert.ok(
        fs.readFileSync(path.join(dir, "dist", "index.html"), "utf-8").includes('rel="stylesheet"')
      );
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Per-route CSS (F-P3-1): a page links only what its route graph imports.
// ---------------------------------------------------------------------------

const ROUTE_CSS_FILES: Record<string, string> = {
  "src/styles/root.css": "body { --from-root: 1; }\n",
  "src/styles/shared.css": ".from-shared { color: green; }\n",
  "src/styles/shop.css": ":root { --theme: shop; }\n",
  "src/styles/docs.css": ":root { --theme: docs; }\n",
  "src/Shared.tsx": `import { h } from "preact";
import "./styles/shared.css";
export function Shared() { return h("b", { class: "from-shared" }, "shared"); }
`,
  "src/routes/_layout.tsx": `import { h } from "preact";
import "../styles/root.css";
export default function Layout({ children }) { return h("main", null, children); }
`,
  "src/routes/about.tsx": `import { h } from "preact";
export const config = { mode: "static" };
export default function About() { return h("p", null, "about-page"); }
`,
};

function sectionRoute(name: string, mode: "app" | "static"): string {
  return `import { h } from "preact";
import "../styles/${name}.css";
import { Shared } from "../Shared";
export const config = { mode: "${mode}" };
export default function Page() { return h("p", null, "${name}-page", h(Shared, null)); }
`;
}

function linkedCss(dir: string, hrefs: string[]): string {
  return hrefs.map((href) => fs.readFileSync(path.join(dir, "dist", href), "utf-8")).join("\n");
}

function stylesheetLinks(html: string): string[] {
  return [...html.matchAll(/<link rel="stylesheet" href="([^"]+)">/g)].map((match) => match[1]);
}

function assertSectionCss(css: string, section: "shop" | "docs" | null, where: string): void {
  assert.ok(css.includes("--from-root"), `${where}: root layout CSS missing:\n${css}`);
  for (const other of ["shop", "docs"]) {
    assert.equal(new RegExp(`--theme:\\s*${other}`).test(css), other === section, `${where}: ${other} theme CSS:\n${css}`);
  }
  assert.equal(css.includes(".from-shared"), section !== null, `${where}: shared CSS:\n${css}`);
}

describe("neutron-ts build (process): per-route CSS", () => {
  it("links each app and static page only to its own route graph's stylesheets", { timeout: 240_000 }, async () => {
    const dir = makeBuildFixture({
      ...ROUTE_CSS_FILES,
      "package.json": JSON.stringify({ name: "route-css-fixture", type: "module" }),
      "index.html": '<!doctype html><html><body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body></html>\n',
      "src/main.tsx": `import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";
registerRoutes(routes);
void init();
`,
      "src/routes/shop.tsx": sectionRoute("shop", "app"),
      "src/routes/docs.tsx": sectionRoute("docs", "app"),
    });
    try {
      await runBuild(dir);
      const about = fs.readFileSync(path.join(dir, "dist", "about", "index.html"), "utf-8");
      assertSectionCss(linkedCss(dir, stylesheetLinks(about)), null, "static /about");

      const port = await freePort();
      const proc = runCli("start", dir, { NEUTRON_PORT: String(port), NEUTRON_HOST: "127.0.0.1" });
      try {
        await waitForHealth(`http://127.0.0.1:${port}/health`, proc);
        for (const section of ["shop", "docs"] as const) {
          const html = await (
            await fetch(`http://127.0.0.1:${port}/${section}`, { headers: { accept: "text/html" } })
          ).text();
          assert.ok(html.includes(`${section}-page`), html);
          const hrefs = stylesheetLinks(html);
          assertSectionCss(linkedCss(dir, hrefs), section, `SSR /${section}`);
          // Client navigation loads the same list before it swaps the page.
          const data = decodeSerializedPayload<{ __css__: string[] }>(
            await (await fetch(`http://127.0.0.1:${port}/${section}`, { headers: { accept: "application/json" } })).json()
          );
          assert.deepEqual(data.__css__, hrefs);
        }
      } finally {
        proc.child.kill("SIGTERM");
        await proc.exited;
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("splits a static-only site's CSS per page", { timeout: 180_000 }, async () => {
    const dir = makeBuildFixture({
      ...ROUTE_CSS_FILES,
      "src/routes/shop.tsx": sectionRoute("shop", "static"),
      "src/routes/docs.tsx": sectionRoute("docs", "static"),
    });
    try {
      await runBuild(dir);
      for (const [page, section] of [["about", null], ["shop", "shop"], ["docs", "docs"]] as const) {
        const html = fs.readFileSync(path.join(dir, "dist", page, "index.html"), "utf-8");
        assertSectionCss(linkedCss(dir, stylesheetLinks(html)), section, `static /${page}`);
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Static-tier routes stay out of the client build (F-P3-8).
// ---------------------------------------------------------------------------

describe("neutron-ts build (process): static-tier routes in an app", () => {
  it("keeps static pages' modules out of the client bundle and still links their CSS", { timeout: 240_000 }, async () => {
    const dir = makeBuildFixture({
      "package.json": JSON.stringify({ name: "static-tier-fixture", type: "module" }),
      "index.html": '<!doctype html><html><body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body></html>\n',
      "src/main.tsx": `import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";
registerRoutes(routes);
void init();
`,
      // Bundleable for Node only: a browser build has to load the binary.
      "node_modules/server-native/package.json": JSON.stringify({ name: "server-native", main: "index.js" }),
      "node_modules/server-native/index.js": `exports.load = () => require("./out/addon");\n`,
      "node_modules/server-native/out/addon.node": Buffer.from([0x7f, 0x45, 0x4c, 0x46, 0xff, 0xfe, 0x00, 0x80]),
      "src/marketing.ts": `export const MARKETING_COPY = "static-tier-marker-" + "x".repeat(64);\n`,
      "src/routes/_layout.tsx": `import { h } from "preact";
import "./layout.css";
export default function Layout({ children }) { return h("main", null, children); }
`,
      "src/routes/layout.css": ".from-layout { color: red; }\n",
      "src/routes/legal.tsx": `import { h } from "preact";
import { load } from "server-native";
import { MARKETING_COPY } from "../marketing";
import "./legal.css";
export const config = { mode: "static" };
export default function Legal() { return h("p", null, "legal:" + typeof load + ":" + MARKETING_COPY.length); }
`,
      "src/routes/legal.css": ".from-legal { color: blue; }\n",
      "src/routes/app.tsx": `import { h } from "preact";
export const config = { mode: "app" };
export default function App() { return h("p", null, "app-page"); }
`,
    });
    try {
      await runBuild(dir);
      const manifest = JSON.parse(fs.readFileSync(path.join(dir, "dist", ".vite", "manifest.json"), "utf-8"));
      assert.ok(manifest["src/routes/app.tsx?neutron-client-route"], Object.keys(manifest).join(", "));
      assert.equal(manifest["src/routes/legal.tsx?neutron-client-route"], undefined, "static route in the client build");
      const assets = path.join(dir, "dist", "assets");
      for (const file of fs.readdirSync(assets).filter((name) => name.endsWith(".js"))) {
        assert.ok(!fs.readFileSync(path.join(assets, file), "utf-8").includes("static-tier-marker"), file);
      }
      const html = fs.readFileSync(path.join(dir, "dist", "legal", "index.html"), "utf-8");
      assert.ok(html.includes("legal:function:83"), html);
      const css = linkedCss(dir, stylesheetLinks(html));
      assert.ok(css.includes(".from-layout") && css.includes(".from-legal"), css);
      // The layout's stylesheet is the same file for the static and the app page.
      const routeCss = JSON.parse(fs.readFileSync(path.join(dir, "dist", ".neutron-route-css.json"), "utf-8"));
      const layoutHref = (routeCss.routes["route:app.tsx"] as string[])[0];
      assert.equal((routeCss.routes["route:legal.tsx"] as string[])[0], layoutHref);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// CommonJS dependencies under react-compat (F-P3-5), on every SSR path.
// ---------------------------------------------------------------------------

const CJS_PAGE = (mode: "static" | "app") => `import { useStore } from "store-shim";
import extend from "plain-cjs";
export const config = { mode: "${mode}" };
export default function Page() {
  return <p>{"cjs-" + "${mode}=" + useStore(() => "s") + ":" + Object.keys(extend({ a: 1 }, { b: 2 })).join("")}</p>;
}
`;

describe("neutron-ts (process): CommonJS dependencies under react-compat", () => {
  it("load in the prerender, start (bundle and Vite runtime) and dev", { timeout: 300_000 }, async () => {
    const dir = makeBuildFixture({
      "package.json": JSON.stringify({ name: "cjs-fixture", type: "module" }),
      "neutron.config.ts": `export default { runtime: "react-compat" };\n`,
      "index.html": '<!doctype html><html><body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body></html>\n',
      "src/main.tsx": `import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";
registerRoutes(routes);
void init();
`,
      // The shape of use-sync-external-store: CommonJS that requires react.
      "node_modules/store-shim/package.json": JSON.stringify({ name: "store-shim", main: "index.js", peerDependencies: { react: "*" } }),
      "node_modules/store-shim/index.js":
        "'use strict';\nif (process.env.NODE_ENV === 'production') {\n  module.exports = require('./cjs/shim.production.js');\n} else {\n  module.exports = require('./cjs/shim.development.js');\n}\n",
      "node_modules/store-shim/cjs/shim.production.js":
        "'use strict';\nvar React = require('react');\nexports.useStore = function (get) { return React.useState(get)[0] + typeof React.useSyncExternalStore; };\n",
      "node_modules/store-shim/cjs/shim.development.js":
        "'use strict';\nvar React = require('react');\nexports.useStore = function (get) { return React.useState(get)[0] + typeof React.useSyncExternalStore; };\n",
      // The shape of extend / highlight.js: plain CommonJS.
      "node_modules/plain-cjs/package.json": JSON.stringify({ name: "plain-cjs", main: "index.js" }),
      "node_modules/plain-cjs/index.js": "module.exports = function extend(a, b) { return Object.assign({}, a, b); };\n",
      "src/routes/index.tsx": CJS_PAGE("app"),
      "src/routes/about.tsx": CJS_PAGE("static"),
    });
    const expectApp = "cjs-app=sfunction:ab";
    const serve = async (command: "start" | "dev") => {
      const port = await freePort();
      const proc = runCli(command, dir, { NEUTRON_PORT: String(port), NEUTRON_HOST: "127.0.0.1" });
      try {
        await waitForHealth(`http://127.0.0.1:${port}/health`, proc, 60_000);
        const res = await fetch(`http://127.0.0.1:${port}/`, { headers: { accept: "text/html" } });
        return { status: res.status, body: await res.text(), output: proc.output() };
      } finally {
        proc.child.kill("SIGTERM");
        await proc.exited;
      }
    };
    try {
      await runBuild(dir);
      const about = fs.readFileSync(path.join(dir, "dist", "about", "index.html"), "utf-8");
      assert.ok(about.includes("cjs-static=sfunction:ab"), about);

      const bundled = await serve("start");
      assert.match(bundled.output, /Modules: prebuilt server bundle/, bundled.output);
      assert.ok(bundled.body.includes(expectApp), bundled.body);

      fs.rmSync(path.join(dir, "dist", ".neutron-server-bundle.json"));
      const viaVite = await serve("start");
      assert.match(viaVite.output, /Modules: transformed through Vite/, viaVite.output);
      assert.ok(viaVite.body.includes(expectApp), viaVite.body);

      const dev = await serve("dev");
      assert.equal(dev.status, 200, dev.body);
      assert.ok(dev.body.includes(expectApp), dev.body);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// Next apps read public configuration as process.env.NEXT_PUBLIC_*. Client
// builds turned process.env into {} and exposed only import.meta.env.VITE_*,
// so every such read was empty. neutron.config env.publicPrefixes inlines
// prefixed variables in the client and server bundles; nothing else ships.
describe("neutron-ts build (process): env.publicPrefixes", () => {
  it("inlines prefixed variables into client and server output, never a secret", { timeout: 240_000 }, async () => {
    const dir = makeBuildFixture({
      "package.json": JSON.stringify({ name: "public-env-fixture", type: "module" }),
      "neutron.config.ts": `export default { env: { publicPrefixes: ["NEXT_PUBLIC_"] } };\n`,
      "index.html": '<!doctype html><html><body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body></html>\n',
      "src/main.tsx": `import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";
registerRoutes(routes);
(globalThis as any).__publicEnv = [
  process.env.NEXT_PUBLIC_APP_URL,
  import.meta.env.NEXT_PUBLIC_APP_URL,
  process.env.NEXT_PUBLIC_UNSET ?? "client-fallback",
  typeof process === "undefined" ? "" : process.env.FYLUN_SECRET_KEY,
];
void init();
`,
      "src/routes/page.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() { return { url: process.env.NEXT_PUBLIC_APP_URL, secret: typeof process.env.FYLUN_SECRET_KEY }; }
export default function Page({ data }) { return h("p", null, "url=" + data.url); }
`,
    });
    try {
      const proc = runCli("build", dir, {
        NEXT_PUBLIC_APP_URL: "https://public-env.example",
        FYLUN_SECRET_KEY: "secret-value-must-not-ship",
      });
      assert.equal(await proc.exited, 0, proc.output());
      const files = (root: string): string[] =>
        fs.readdirSync(root, { recursive: true, withFileTypes: true })
          .filter((entry) => entry.isFile() && /\.(m?js|html)$/.test(entry.name))
          .map((entry) => path.join(entry.parentPath, entry.name));
      const dist = path.join(dir, "dist");
      const serverDir = path.join(dist, "server");
      const all = files(dist);
      const client = all.filter((file) => !file.startsWith(serverDir + path.sep));
      const server = all.filter((file) => file.startsWith(serverDir + path.sep));
      assert.ok(server.length > 0, "no server bundle");
      const read = (list: string[]) => list.map((file) => fs.readFileSync(file, "utf-8")).join("\n");
      const clientCode = read(client);
      const serverCode = read(server);
      // Inlined as a string literal (the minifier may pick any quote).
      const inlined = /["'`]https:\/\/public-env\.example["'`]/;
      assert.match(clientCode, inlined, "client output lacks the public value");
      assert.ok(clientCode.includes("client-fallback"));
      assert.doesNotMatch(clientCode, /process\.env\.NEXT_PUBLIC_APP_URL/);
      assert.match(serverCode, inlined, "server bundle lacks the public value");
      assert.ok(serverCode.includes("process.env.FYLUN_SECRET_KEY"), "the server's secret read must stay a runtime read");
      assert.doesNotMatch(clientCode + serverCode, /secret-value-must-not-ship/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// Server Actions (F-P3-4): a "use server" module imported by client code
// ships as stubs; the server registers exactly the modules the client graph
// reached, under ids fixed at build time. One react-compat build with the
// docker preset carries all three servers that answer them: `start` from the
// prebuilt bundle, the docker image's server.mjs, and the generated runtime
// the vercel/netlify/cloudflare handlers import.
import { serializeTransportData, deserializeTransportData } from "@neutron-build/core";

const ACTIONS_APP: Record<string, string> = {
  "package.json": JSON.stringify({ name: "server-actions-fixture", type: "module" }),
  "neutron.config.ts": `export default { runtime: "react-compat", server: { serverActions: { bodySizeLimit: 4096 } } };\n`,
  "index.html": '<!doctype html><html><body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body></html>\n',
  "src/main.tsx": `import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";
registerRoutes(routes);
void init();
`,
  "src/actions.ts": `"use server";
import { redirect, getActionContext } from "@neutron-build/core";
const SECRET = "server-only-secret-marker";
export async function add(a: number, b: number) {
  return { sum: a + b, secretLength: SECRET.length, at: new Date(0) };
}
export async function save(form: FormData) {
  getActionContext().responseHeaders.set("x-neutron-invalidate", "self");
  return { title: form.get("title"), user: getActionContext().context.user ?? null };
}
export async function leave() {
  throw redirect("/bye");
}
`,
  "src/middleware.ts": `
export default async function middleware(request, context, next) {
  if (request.headers.get("x-deny") === "1") return new Response("gated", { status: 401 });
  context.user = request.headers.get("x-user");
  return next();
}
`,
  "src/routes/index.tsx": `
import { useState } from "react";
import { add, save } from "../actions";
export const config = { mode: "app" };
export default function Home() {
  const [result, setResult] = useState("");
  return (
    <form onSubmit={async (event) => {
      event.preventDefault();
      const saved = await save(new FormData(event.currentTarget));
      setResult(JSON.stringify({ saved, added: await add(1, 2) }));
    }}>
      <input name="title" />
      <button>save</button>
      <output>{result || "actions-home"}</output>
    </form>
  );
}
`,
};

type ActionCall = (actionId: string, body: BodyInit, headers?: Record<string, string>) => Promise<Response>;

function actionRequest(base: string, actionId: string, body: BodyInit, headers: Record<string, string> = {}): Request {
  return new Request(`${base}/_neutron/action`, {
    method: "POST",
    headers: {
      "X-Neutron-Action": actionId,
      "X-Neutron-Data": "true",
      ...(typeof body === "string" ? { "Content-Type": "application/json" } : {}),
      ...headers,
    },
    body,
    redirect: "manual",
  });
}

/** The action contract every server answers the same way. */
async function expectActionsServed(call: ActionCall, ids: Record<string, string>): Promise<void> {
  const added = await call(ids.add, serializeTransportData([2, 3]));
  assert.equal(added.status, 200);
  assert.deepEqual(deserializeTransportData(await added.text()), { sum: 5, secretLength: 25, at: new Date(0) });

  // A FormData argument, multipart as the client stub sends it.
  const form = new FormData();
  form.append("$neutron_args", '[[1],["FormData",2],"0"]');
  form.append("$f0:title", "hello");
  const saved = await call(ids.save, form, { "x-user": "ada", Referer: "http://127.0.0.1/" });
  assert.equal(saved.status, 200);
  assert.deepEqual(deserializeTransportData(await saved.text()), { title: "hello", user: "ada" });
  assert.equal(saved.headers.get("x-neutron-invalidate"), "self");

  const gated = await call(ids.add, serializeTransportData([1, 1]), { "x-deny": "1" });
  assert.equal(gated.status, 401);
  const redirected = await call(ids.leave, serializeTransportData([]));
  assert.equal(redirected.status, 204);
  assert.equal(redirected.headers.get("x-neutron-redirect"), "/bye");
  const crossSite = await call(ids.add, serializeTransportData([1, 1]), {
    Origin: "https://evil.example",
    "Sec-Fetch-Site": "cross-site",
  });
  assert.equal(crossSite.status, 403);
  const unknown = await call("f".repeat(40), serializeTransportData([]));
  assert.equal(unknown.status, 404);
  const oversized = await call(ids.add, serializeTransportData(["x".repeat(8000), 1]));
  assert.equal(oversized.status, 413);
}

describe("neutron-ts build (process): server actions", () => {
  it("registers what the client reached, ships stubs, and start/docker/the adapter runtime serve it", { timeout: 360_000 }, async () => {
    const dir = makeBuildFixture(ACTIONS_APP);
    const context = fs.mkdtempSync(path.join(os.tmpdir(), "neutron-actions-docker-"));
    try {
      const build = runCli("build", dir, {}, ["--preset", "docker"]);
      assert.equal(await build.exited, 0, build.output());
      const dist = path.join(dir, "dist");

      const manifest = JSON.parse(fs.readFileSync(path.join(dist, ".neutron-server-actions.json"), "utf-8"));
      const ids: Record<string, string> = {};
      for (const [id, entry] of Object.entries(manifest.actions as Record<string, { file: string; export: string }>)) {
        assert.equal(entry.file, "src/actions.ts");
        ids[entry.export] = id;
      }
      assert.deepEqual(Object.keys(ids).sort(), ["add", "leave", "save"]);

      // The browser gets stubs bound to those ids, never the module.
      const clientCode = fs
        .readdirSync(path.join(dist, "assets"))
        .filter((name) => name.endsWith(".js"))
        .map((name) => fs.readFileSync(path.join(dist, "assets", name), "utf-8"))
        .join("\n");
      assert.ok(clientCode.includes(ids.add) && clientCode.includes(ids.save), "client stubs missing");
      assert.ok(!clientCode.includes("server-only-secret-marker"), "the action module shipped to the browser");
      assertNoServerCodeInClientBuild(dir);

      // start, from the prebuilt bundle.
      const port = await freePort();
      const proc = runCli("start", dir, { NEUTRON_HOST: "127.0.0.1", NEUTRON_PORT: String(port) });
      try {
        await waitForHealth(`http://127.0.0.1:${port}/health`, proc);
        assert.match(proc.output(), /Modules: prebuilt server bundle/, proc.output());
        const base = `http://127.0.0.1:${port}`;
        const home = await fetch(base + "/", { headers: { accept: "text/html" } });
        assert.ok((await home.text()).includes("actions-home"));
        await expectActionsServed((id, body, headers) => fetch(actionRequest(base, id, body, headers)), ids);
      } finally {
        proc.child.kill("SIGTERM");
        await proc.exited;
      }

      // The adapter runtime (what the vercel/netlify/cloudflare handlers call).
      const runtime = (await import(pathToFileURL(path.join(dist, "server", "node", "entry.js")).href)) as {
        handleNeutronRequest: (request: Request) => Promise<Response>;
      };
      await expectActionsServed(
        (id, body, headers) => runtime.handleNeutronRequest(actionRequest("http://127.0.0.1", id, body, headers)),
        ids
      );

      // The docker image context, alone.
      fs.cpSync(dist, context, { recursive: true });
      const dockerPort = await freePort();
      const child = spawn(process.execPath, ["server.mjs"], {
        cwd: context,
        env: { ...process.env, PORT: String(dockerPort), NODE_PATH: "" },
        stdio: ["ignore", "pipe", "pipe"],
      });
      let output = "";
      child.stdout!.on("data", (chunk) => (output += chunk));
      child.stderr!.on("data", (chunk) => (output += chunk));
      const exited = new Promise((resolve) => child.on("exit", resolve));
      try {
        const base = `http://127.0.0.1:${dockerPort}`;
        const deadline = Date.now() + 30_000;
        while (Date.now() < deadline) {
          assert.equal(child.exitCode, null, output);
          try {
            await fetch(base + "/health");
            break;
          } catch {
            await new Promise((resolve) => setTimeout(resolve, 150));
          }
        }
        await expectActionsServed((id, body, headers) => fetch(actionRequest(base, id, body, headers)), ids);
      } finally {
        child.kill("SIGTERM");
        await exited;
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
      fs.rmSync(context, { recursive: true, force: true });
    }
  });

  it("fails the build on an export that is not an async function, and on inline \"use server\"", { timeout: 240_000 }, async () => {
    const base = {
      "package.json": JSON.stringify({ name: "server-actions-bad-fixture", type: "module" }),
      "index.html": ACTIONS_APP["index.html"],
      "src/main.tsx": ACTIONS_APP["src/main.tsx"],
    };
    const badExport = makeBuildFixture({
      ...base,
      "src/actions.ts": `"use server";\nexport async function ok() { return 1; }\nexport function notAsync() { return 2; }\n`,
      "src/routes/index.tsx": `import { h } from "preact";\nimport { ok, notAsync } from "../actions";\nexport const config = { mode: "app" };\nexport default function Home() { return h("button", { onClick: () => ok() + notAsync() }, "x"); }\n`,
    });
    const inline = makeBuildFixture({
      ...base,
      "src/routes/index.tsx": `import { h } from "preact";\nexport const config = { mode: "app" };\nexport default function Home() {\n  async function save() {\n    "use server";\n    return 1;\n  }\n  return h("button", { onClick: save }, "x");\n}\n`,
    });
    try {
      const first = runCli("build", badExport, {});
      assert.notEqual(await first.exited, 0, first.output());
      assert.match(first.output(), /src\/actions\.ts:3: export "notAsync" is not async/, first.output());
      const second = runCli("build", inline, {});
      assert.notEqual(await second.exited, 0, second.output());
      assert.match(second.output(), /src\/routes\/index\.tsx:4: inline "use server" functions/, second.output());
    } finally {
      fs.rmSync(badExport, { recursive: true, force: true });
      fs.rmSync(inline, { recursive: true, force: true });
    }
  });
});

// The vercel/netlify/cloudflare/docker runtime sent 3 of start's 5 security
// defaults, ignored server.securityHeaders, and served no mounts. It now
// applies routes.headers then the defaults exactly as start does, and serves
// src/server.entry.ts mounts with the global middleware ahead of them.
describe("neutron-ts build (process): adapter runtime parity", () => {
  const app = (entry: string): Record<string, string> => ({
    "package.json": JSON.stringify({ name: "adapter-parity-fixture", type: "module" }),
    "neutron.config.ts": `export default {
  server: { securityHeaders: { headers: { "Permissions-Policy": "camera=()" } } },
  routes: {
    headers: [
      { source: "/embed", headers: { "X-Frame-Options": null, "Cross-Origin-Opener-Policy": "same-origin" } },
    ],
  },
};
`,
    "index.html": '<!doctype html><html><body><div id="app"></div><script type="module" src="/src/main.tsx"></script></body></html>\n',
    "src/main.tsx": `import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";
registerRoutes(routes);
void init();
`,
    "src/middleware.ts": `
export default async function middleware(request, context, next) {
  if (request.headers.get("x-deny") === "1") return new Response("gated", { status: 401 });
  const response = await next();
  response.headers.set("x-global", (response.headers.get("x-global") || "") + "ran");
  return response;
}
`,
    "src/server.entry.ts": entry,
    "src/routes/embed.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export default function Embed() { return h("p", null, "embed-page"); }
`,
    "src/routes/api/fallback.tsx": `
import { h } from "preact";
export const config = { mode: "app" };
export default function Fallback() { return h("p", null, "fell-through"); }
`,
  });

  it("vercel preset: start's security headers and the server entry's mounts", { timeout: 240_000 }, async () => {
    const dir = makeBuildFixture(
      app(`
import { Hono } from "hono";
const api = new Hono();
api.get("/hello", (c) => c.json({ hello: "mount" }));
const open = new Hono();
open.get("/ping", (c) => c.text("pong"));
export default () => ({ mount: { "/api": api, "/open": { app: open, globalMiddleware: false } } });
`)
    );
    // The app's own hono (core's copy, linked in like every other package here).
    fs.symlinkSync(
      fs.realpathSync(path.join(CLI_PACKAGE_DIR, "node_modules", "@neutron-build", "core", "node_modules", "hono")),
      path.join(dir, "node_modules", "hono"),
      "dir"
    );
    try {
      const build = runCli("build", dir, {}, ["--preset", "vercel"]);
      assert.equal(await build.exited, 0, build.output());
      const dist = path.join(dir, "dist");
      assert.ok(fs.existsSync(path.join(dist, "api", "__neutron.mjs")));
      const runtime = (await import(pathToFileURL(path.join(dist, "server", "node", "entry.js")).href)) as {
        handleNeutronRequest: (request: Request) => Promise<Response>;
      };
      const get = (p: string, headers: Record<string, string> = {}) =>
        runtime.handleNeutronRequest(new Request(`http://127.0.0.1${p}`, { headers: { accept: "text/html", ...headers } }));

      const defaults = {
        "x-content-type-options": "nosniff",
        "x-frame-options": "DENY",
        "referrer-policy": "strict-origin-when-cross-origin",
        "cross-origin-opener-policy": "same-origin-allow-popups",
        "cross-origin-resource-policy": "same-origin",
        "permissions-policy": "camera=()",
      };
      const mounted = await get("/api/hello");
      assert.equal(mounted.status, 200);
      assert.deepEqual(await mounted.json(), { hello: "mount" });
      assert.equal(mounted.headers.get("x-global"), "ran");
      for (const [name, value] of Object.entries(defaults)) assert.equal(mounted.headers.get(name), value, name);

      // A rule overrides one default and nulls another, on its path only.
      const embed = await get("/embed");
      assert.equal(embed.status, 200);
      assert.ok((await embed.text()).includes("embed-page"));
      assert.equal(embed.headers.get("x-frame-options"), null);
      assert.equal(embed.headers.get("cross-origin-opener-policy"), "same-origin");
      assert.equal(embed.headers.get("x-content-type-options"), "nosniff");

      // Global middleware gates the mount; an opted-out mount skips it.
      assert.equal((await get("/api/hello", { "x-deny": "1" })).status, 401);
      const open = await get("/open/ping", { "x-deny": "1" });
      assert.equal(open.status, 200);
      assert.equal(await open.text(), "pong");
      assert.equal(open.headers.get("x-global"), null);

      // Unmatched by the sub-app: the routes answer, the chain ran once.
      const fellThrough = await get("/api/fallback");
      assert.equal(fellThrough.status, 200);
      assert.ok((await fellThrough.text()).includes("fell-through"));
      assert.equal(fellThrough.headers.get("x-global"), "ran");
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("fails an edge build whose server entry declares a websocket", { timeout: 240_000 }, async () => {
    const dir = makeBuildFixture(
      app(`
export default { websocket: { path: "/ws", attach(wss) { wss.on("connection", () => {}); } } };
`)
    );
    try {
      const build = runCli("build", dir, {}, ["--preset", "netlify"]);
      assert.notEqual(await build.exited, 0, build.output());
      assert.match(build.output(), /The netlify adapter cannot serve the websocket src\/server\.entry\.ts declares/, build.output());
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
