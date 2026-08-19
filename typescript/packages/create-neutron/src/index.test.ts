import assert from "node:assert/strict";
import { describe, it } from "node:test";
import * as path from "node:path";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import { fileURLToPath } from "node:url";
import { scaffoldProject } from "./scaffold.js";

// ---------------------------------------------------------------------------
// Unit-testable functions extracted from create-neutron/src/scaffold.ts
// (the scaffolding library `index.ts` and `neutron-ts init` both call).
// These mirror the source exactly so tests are self-contained.
// ---------------------------------------------------------------------------

type RuntimeMode = "preact" | "react-compat";
type TemplateName = "basic" | "marketing" | "app" | "full" | "docs";

const TEMPLATE_NAMES: TemplateName[] = ["basic", "marketing", "app", "full", "docs"];

interface CliOptions {
  targetDir: string;
  template: TemplateName;
  runtime: RuntimeMode;
}

function parseArgs(argv: string[]): CliOptions | null {
  const positional: string[] = [];
  let template: TemplateName = "basic";
  let runtime: RuntimeMode = "preact";

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg) {
      continue;
    }

    if (arg === "--template" && argv[i + 1]) {
      const candidate = argv[++i];
      if (isTemplateName(candidate)) {
        template = candidate;
      } else {
        return null;
      }
      continue;
    }

    if (arg.startsWith("--template=")) {
      const candidate = arg.split("=")[1];
      if (isTemplateName(candidate)) {
        template = candidate;
      } else {
        return null;
      }
      continue;
    }

    if (arg === "--runtime" && argv[i + 1]) {
      const candidate = argv[++i];
      if (candidate === "preact" || candidate === "react-compat") {
        runtime = candidate;
      } else {
        return null;
      }
      continue;
    }

    if (arg.startsWith("--runtime=")) {
      const candidate = arg.split("=")[1];
      if (candidate === "preact" || candidate === "react-compat") {
        runtime = candidate;
      } else {
        return null;
      }
      continue;
    }

    positional.push(arg);
  }

  const targetDir = positional[0] || "neutron-app";
  return { targetDir, template, runtime };
}

function isTemplateName(value: string): value is TemplateName {
  return TEMPLATE_NAMES.includes(value as TemplateName);
}

function toPackageName(input: string): string {
  const normalized = input
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || "neutron-app";
}

function resolveOutputName(inputName: string): string {
  if (inputName === "_gitignore") {
    return ".gitignore";
  }
  return inputName;
}

function applyTokens(source: string, tokens: Record<string, string>): string {
  let output = source;
  for (const [key, value] of Object.entries(tokens)) {
    output = output.replaceAll(`__${key}__`, value);
  }
  return output;
}

async function ensureTargetDirectory(targetDir: string): Promise<void> {
  if (!fs.existsSync(targetDir)) {
    await fsp.mkdir(targetDir, { recursive: true });
    return;
  }
  const files = await fsp.readdir(targetDir);
  if (files.length > 0) {
    throw new Error(`Target directory is not empty: ${targetDir}`);
  }
}

async function copyDirectory(
  sourceDir: string,
  targetDir: string,
  tokens: Record<string, string>
): Promise<void> {
  await fsp.mkdir(targetDir, { recursive: true });
  const entries = await fsp.readdir(sourceDir, { withFileTypes: true });

  for (const entry of entries) {
    const sourcePath = path.join(sourceDir, entry.name);
    const outputName = resolveOutputName(entry.name);
    const outputPath = path.join(targetDir, outputName);

    if (entry.isDirectory()) {
      await copyDirectory(sourcePath, outputPath, tokens);
      continue;
    }

    const source = await fsp.readFile(sourcePath, "utf-8");
    const rendered = applyTokens(source, tokens);
    await fsp.writeFile(outputPath, rendered, "utf-8");
  }
}

function findWorkspaceRoot(startDir: string): string | null {
  let current = path.resolve(startDir);
  while (true) {
    const hasWorkspaceConfig = fs.existsSync(path.join(current, "pnpm-workspace.yaml"));
    const hasNeutronPackage = fs.existsSync(path.join(current, "packages", "neutron"));
    const hasNeutronCliPackage = fs.existsSync(path.join(current, "packages", "neutron-cli"));
    if (hasWorkspaceConfig && hasNeutronPackage && hasNeutronCliPackage) {
      return current;
    }
    const parent = path.dirname(current);
    if (parent === current) {
      return null;
    }
    current = parent;
  }
}

function resolveDependencyVersions(targetDir: string): {
  neutron: string;
  neutronCli: string;
} {
  const workspaceRoot = findWorkspaceRoot(path.dirname(targetDir));
  if (!workspaceRoot) {
    return { neutron: "latest", neutronCli: "latest" };
  }
  return { neutron: "workspace:*", neutronCli: "workspace:*" };
}

// =========================================================================
// Tests
// =========================================================================

// ---------------------------------------------------------------------------
// parseArgs
// ---------------------------------------------------------------------------

describe("parseArgs", () => {
  it("returns default options when no args provided", () => {
    const result = parseArgs([]);
    assert.ok(result);
    assert.equal(result.targetDir, "neutron-app");
    assert.equal(result.template, "basic");
    assert.equal(result.runtime, "preact");
  });

  it("parses project name as positional arg", () => {
    const result = parseArgs(["my-app"]);
    assert.ok(result);
    assert.equal(result.targetDir, "my-app");
  });

  it("parses --template flag with space", () => {
    const result = parseArgs(["--template", "full"]);
    assert.ok(result);
    assert.equal(result.template, "full");
  });

  it("parses --template= format", () => {
    const result = parseArgs(["--template=marketing"]);
    assert.ok(result);
    assert.equal(result.template, "marketing");
  });

  it("parses --template app", () => {
    const result = parseArgs(["--template", "app"]);
    assert.ok(result);
    assert.equal(result.template, "app");
  });

  it("returns null for unsupported template", () => {
    const result = parseArgs(["--template", "invalid"]);
    assert.equal(result, null);
  });

  it("returns null for unsupported template in = format", () => {
    const result = parseArgs(["--template=nonexistent"]);
    assert.equal(result, null);
  });

  it("parses --runtime preact", () => {
    const result = parseArgs(["--runtime", "preact"]);
    assert.ok(result);
    assert.equal(result.runtime, "preact");
  });

  it("parses --runtime react-compat", () => {
    const result = parseArgs(["--runtime", "react-compat"]);
    assert.ok(result);
    assert.equal(result.runtime, "react-compat");
  });

  it("parses --runtime= format", () => {
    const result = parseArgs(["--runtime=react-compat"]);
    assert.ok(result);
    assert.equal(result.runtime, "react-compat");
  });

  it("returns null for unsupported runtime", () => {
    const result = parseArgs(["--runtime", "solid"]);
    assert.equal(result, null);
  });

  it("returns null for unsupported runtime in = format", () => {
    const result = parseArgs(["--runtime=vue"]);
    assert.equal(result, null);
  });

  it("handles all options combined", () => {
    const result = parseArgs(["my-project", "--template", "full", "--runtime", "react-compat"]);
    assert.ok(result);
    assert.equal(result.targetDir, "my-project");
    assert.equal(result.template, "full");
    assert.equal(result.runtime, "react-compat");
  });

  it("handles --help as target dir", () => {
    const result = parseArgs(["--help"]);
    assert.ok(result);
    assert.equal(result.targetDir, "--help");
  });
});

// ---------------------------------------------------------------------------
// isTemplateName
// ---------------------------------------------------------------------------

describe("isTemplateName", () => {
  it("returns true for valid template names", () => {
    assert.equal(isTemplateName("basic"), true);
    assert.equal(isTemplateName("marketing"), true);
    assert.equal(isTemplateName("app"), true);
    assert.equal(isTemplateName("full"), true);
  });

  it("returns false for invalid names", () => {
    assert.equal(isTemplateName(""), false);
    assert.equal(isTemplateName("custom"), false);
    assert.equal(isTemplateName("minimal"), false);
  });
});

// ---------------------------------------------------------------------------
// toPackageName
// ---------------------------------------------------------------------------

describe("toPackageName", () => {
  it("lowercases the input", () => {
    assert.equal(toPackageName("MyApp"), "myapp");
  });

  it("replaces invalid characters with hyphens", () => {
    assert.equal(toPackageName("my app!"), "my-app");
    assert.equal(toPackageName("Hello World"), "hello-world");
  });

  it("strips leading and trailing hyphens", () => {
    assert.equal(toPackageName("---test---"), "test");
  });

  it("handles already valid names", () => {
    assert.equal(toPackageName("my-app"), "my-app");
  });

  it("returns neutron-app for empty result", () => {
    assert.equal(toPackageName("!!!"), "neutron-app");
  });

  it("keeps numbers", () => {
    assert.equal(toPackageName("app123"), "app123");
  });
});

// ---------------------------------------------------------------------------
// resolveOutputName
// ---------------------------------------------------------------------------

describe("resolveOutputName", () => {
  it("maps _gitignore to .gitignore", () => {
    assert.equal(resolveOutputName("_gitignore"), ".gitignore");
  });

  it("preserves normal filenames", () => {
    assert.equal(resolveOutputName("index.html"), "index.html");
    assert.equal(resolveOutputName("package.json"), "package.json");
    assert.equal(resolveOutputName("tsconfig.json"), "tsconfig.json");
  });
});

// ---------------------------------------------------------------------------
// applyTokens
// ---------------------------------------------------------------------------

describe("applyTokens", () => {
  it("replaces __TOKEN__ patterns", () => {
    const result = applyTokens('{ "name": "__PACKAGE_NAME__" }', {
      PACKAGE_NAME: "my-app",
    });
    assert.equal(result, '{ "name": "my-app" }');
  });

  it("replaces multiple tokens", () => {
    const result = applyTokens("__A__ and __B__", { A: "1", B: "2" });
    assert.equal(result, "1 and 2");
  });

  it("replaces all occurrences", () => {
    const result = applyTokens("__X__ + __X__", { X: "val" });
    assert.equal(result, "val + val");
  });

  it("leaves unmatched tokens as-is", () => {
    const result = applyTokens("__UNKNOWN__", {});
    assert.equal(result, "__UNKNOWN__");
  });

  it("handles empty tokens", () => {
    const result = applyTokens("hello", {});
    assert.equal(result, "hello");
  });
});

// ---------------------------------------------------------------------------
// ensureTargetDirectory
// ---------------------------------------------------------------------------

describe("ensureTargetDirectory", () => {
  it("creates directory if it does not exist", async () => {
    const tmpDir = path.join(os.tmpdir(), `cn-test-${Date.now()}-new`);
    try {
      await ensureTargetDirectory(tmpDir);
      assert.ok(fs.existsSync(tmpDir));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("succeeds if directory exists but is empty", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "cn-test-empty-"));
    try {
      await ensureTargetDirectory(tmpDir);
      assert.ok(true);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("throws if directory is not empty", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "cn-test-notempty-"));
    try {
      fs.writeFileSync(path.join(tmpDir, "file.txt"), "hi");
      await assert.rejects(
        () => ensureTargetDirectory(tmpDir),
        (err: Error) => {
          assert.ok(err.message.includes("not empty"));
          return true;
        }
      );
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// copyDirectory
// ---------------------------------------------------------------------------

describe("copyDirectory", () => {
  it("copies files from source to target with token substitution", async () => {
    const sourceDir = fs.mkdtempSync(path.join(os.tmpdir(), "cn-src-"));
    const targetDir = path.join(os.tmpdir(), `cn-tgt-${Date.now()}`);

    try {
      fs.writeFileSync(
        path.join(sourceDir, "package.json"),
        '{ "name": "__PACKAGE_NAME__" }'
      );
      fs.writeFileSync(path.join(sourceDir, "_gitignore"), "node_modules");

      await copyDirectory(sourceDir, targetDir, { PACKAGE_NAME: "test-proj" });

      // Check package.json was written with token replaced
      const pkg = fs.readFileSync(path.join(targetDir, "package.json"), "utf-8");
      assert.equal(pkg, '{ "name": "test-proj" }');

      // Check _gitignore was renamed to .gitignore
      assert.ok(fs.existsSync(path.join(targetDir, ".gitignore")));
      const gitignore = fs.readFileSync(path.join(targetDir, ".gitignore"), "utf-8");
      assert.equal(gitignore, "node_modules");
    } finally {
      fs.rmSync(sourceDir, { recursive: true, force: true });
      fs.rmSync(targetDir, { recursive: true, force: true });
    }
  });

  it("recursively copies subdirectories", async () => {
    const sourceDir = fs.mkdtempSync(path.join(os.tmpdir(), "cn-src-sub-"));
    const targetDir = path.join(os.tmpdir(), `cn-tgt-sub-${Date.now()}`);

    try {
      const subDir = path.join(sourceDir, "src");
      fs.mkdirSync(subDir, { recursive: true });
      fs.writeFileSync(path.join(subDir, "main.tsx"), "export default function() {}");

      await copyDirectory(sourceDir, targetDir, {});

      assert.ok(fs.existsSync(path.join(targetDir, "src", "main.tsx")));
    } finally {
      fs.rmSync(sourceDir, { recursive: true, force: true });
      fs.rmSync(targetDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// findWorkspaceRoot
// ---------------------------------------------------------------------------

describe("findWorkspaceRoot", () => {
  it("returns null when no workspace root is found", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "cn-ws-"));
    try {
      const result = findWorkspaceRoot(tmpDir);
      assert.equal(result, null);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("finds workspace root when all markers exist", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "cn-ws-root-"));
    try {
      fs.writeFileSync(path.join(tmpDir, "pnpm-workspace.yaml"), "");
      fs.mkdirSync(path.join(tmpDir, "packages", "neutron"), { recursive: true });
      fs.mkdirSync(path.join(tmpDir, "packages", "neutron-cli"), { recursive: true });
      const childDir = path.join(tmpDir, "child");
      fs.mkdirSync(childDir, { recursive: true });

      const result = findWorkspaceRoot(childDir);
      assert.equal(result, tmpDir);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// resolveDependencyVersions
// ---------------------------------------------------------------------------

describe("resolveDependencyVersions", () => {
  it("returns latest when outside a workspace", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "cn-deps-"));
    try {
      const targetDir = path.join(tmpDir, "my-app");
      fs.mkdirSync(targetDir, { recursive: true });
      const result = resolveDependencyVersions(targetDir);
      assert.equal(result.neutron, "latest");
      assert.equal(result.neutronCli, "latest");
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("returns workspace:* when inside a workspace", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "cn-deps-ws-"));
    try {
      fs.writeFileSync(path.join(tmpDir, "pnpm-workspace.yaml"), "");
      fs.mkdirSync(path.join(tmpDir, "packages", "neutron"), { recursive: true });
      fs.mkdirSync(path.join(tmpDir, "packages", "neutron-cli"), { recursive: true });
      const targetDir = path.join(tmpDir, "my-app");
      fs.mkdirSync(targetDir, { recursive: true });

      const result = resolveDependencyVersions(targetDir);
      assert.equal(result.neutron, "workspace:*");
      assert.equal(result.neutronCli, "workspace:*");
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Template names
// ---------------------------------------------------------------------------

describe("TEMPLATE_NAMES", () => {
  it("contains exactly 5 templates", () => {
    assert.equal(TEMPLATE_NAMES.length, 5);
  });

  it("includes basic, marketing, app, full, and docs", () => {
    assert.ok(TEMPLATE_NAMES.includes("basic"));
    assert.ok(TEMPLATE_NAMES.includes("marketing"));
    assert.ok(TEMPLATE_NAMES.includes("app"));
    assert.ok(TEMPLATE_NAMES.includes("full"));
    assert.ok(TEMPLATE_NAMES.includes("docs"));
  });
});

// ---------------------------------------------------------------------------
// Template package.json integrity — reads the REAL template files on disk
// (not a mirror). Guards the class of bug where the published scaffold shipped
// `neutron` scripts while the bin is `neutron-ts`, so a fresh `pnpm dev`/`build`
// failed with "neutron: command not found".
// ---------------------------------------------------------------------------

describe("template package.json integrity", () => {
  const testDir = path.dirname(fileURLToPath(import.meta.url));
  const templatesDir = path.join(testDir, "..", "templates");

  it("has a directory for every name in TEMPLATE_NAMES", () => {
    for (const name of TEMPLATE_NAMES) {
      const pkgPath = path.join(templatesDir, name, "package.json");
      assert.ok(fs.existsSync(pkgPath), `missing template package.json: ${name}`);
    }
  });

  it("invokes the neutron-ts bin in scripts (never a bare `neutron`)", () => {
    for (const name of TEMPLATE_NAMES) {
      const pkg = JSON.parse(
        fs.readFileSync(path.join(templatesDir, name, "package.json"), "utf8"),
      );
      const scripts: Record<string, string> = pkg.scripts ?? {};
      for (const [scriptName, body] of Object.entries(scripts)) {
        // Any invocation of the dev CLI must be `neutron-ts`, not `neutron`.
        const callsBareNeutron = /(^|\s|&&|;)neutron(\s|$)/.test(body);
        assert.ok(
          !callsBareNeutron,
          `template ${name} script "${scriptName}" calls bare \`neutron\` (bin is \`neutron-ts\`): ${body}`,
        );
        if (/neutron/.test(body)) {
          assert.ok(
            /neutron-ts/.test(body),
            `template ${name} script "${scriptName}" should call \`neutron-ts\`: ${body}`,
          );
        }
      }
    }
  });

  it("declares the Neutron deps and preact-render-to-string", () => {
    for (const name of TEMPLATE_NAMES) {
      const pkg = JSON.parse(
        fs.readFileSync(path.join(templatesDir, name, "package.json"), "utf8"),
      );
      const deps: Record<string, string> = pkg.dependencies ?? {};
      assert.ok(deps["@neutron-build/core"], `${name} missing @neutron-build/core`);
      assert.ok(deps["@neutron-build/cli"], `${name} missing @neutron-build/cli`);
      assert.ok(
        deps["preact-render-to-string"],
        `${name} missing preact-render-to-string`,
      );
    }
  });

  it("named catch-all routes use the named param key, not bare `*`", () => {
    // A `[...slug]` file's catch-all param is named `slug` (per the router).
    // Using `params["*"]` / `{ "*": ... }` leaves the literal name glued to the
    // resolved path (e.g. `/docs/installationslug`). Only a bare `[...]` file
    // may use the `*` key. Guard every named catch-all template route.
    const routeFiles: string[] = [];
    const walk = (dir: string) => {
      if (!fs.existsSync(dir)) return;
      for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
        const p = path.join(dir, e.name);
        if (e.isDirectory()) walk(p);
        else if (/\[\.\.\.[^\]]+\]\.tsx$/.test(e.name)) routeFiles.push(p);
      }
    };
    for (const name of TEMPLATE_NAMES) {
      walk(path.join(templatesDir, name, "src", "routes"));
    }
    for (const file of routeFiles) {
      const src = fs.readFileSync(file, "utf8");
      assert.ok(
        !/params\s*\[\s*["']\*["']\s*\]/.test(src) && !/\{\s*["']\*["']\s*:/.test(src),
        `named catch-all route uses bare "*" param key (should be the named param): ${file}`,
      );
    }
  });
});

// ---------------------------------------------------------------------------
// Template DX guards — keep the scaffolded project TypeScript-clean and friendly.
// ---------------------------------------------------------------------------

describe("template DX files", () => {
  const testDir2 = path.dirname(fileURLToPath(import.meta.url));
  const templatesDir2 = path.join(testDir2, "..", "templates");

  it("ships a README.md in every template", () => {
    for (const name of TEMPLATE_NAMES) {
      assert.ok(
        fs.existsSync(path.join(templatesDir2, name, "README.md")),
        `template ${name} is missing README.md`,
      );
    }
  });

  it("ships src/neutron-env.d.ts declaring the routes virtual module", () => {
    for (const name of TEMPLATE_NAMES) {
      const p = path.join(templatesDir2, name, "src", "neutron-env.d.ts");
      assert.ok(fs.existsSync(p), `template ${name} is missing src/neutron-env.d.ts`);
      const src = fs.readFileSync(p, "utf8");
      assert.ok(
        /declare module ["']virtual:neutron\/routes["']/.test(src),
        `template ${name} env.d.ts does not declare virtual:neutron/routes`,
      );
    }
  });

  it("tsconfig include picks up the generated .neutron-*.d.ts files", () => {
    // TypeScript's wildcard include skips dot-prefixed files, so the generated
    // route/content type files must be globbed explicitly or tsc ignores them.
    for (const name of TEMPLATE_NAMES) {
      const tsconfig = JSON.parse(
        fs.readFileSync(path.join(templatesDir2, name, "tsconfig.json"), "utf8"),
      );
      const include: string[] = tsconfig.include ?? [];
      assert.ok(
        include.some((g) => g.includes(".neutron-")),
        `template ${name} tsconfig.include must glob .neutron-*.d.ts (has: ${JSON.stringify(include)})`,
      );
    }
  });

  it("layouts type children as ComponentChildren, not unknown", () => {
    const layoutFiles: string[] = [];
    const walk = (dir: string) => {
      if (!fs.existsSync(dir)) return;
      for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
        const p = path.join(dir, e.name);
        if (e.isDirectory()) walk(p);
        else if (e.name === "_layout.tsx") layoutFiles.push(p);
      }
    };
    for (const name of TEMPLATE_NAMES) walk(path.join(templatesDir2, name, "src", "routes"));
    assert.ok(layoutFiles.length > 0, "expected at least one _layout.tsx across templates");
    for (const file of layoutFiles) {
      const src = fs.readFileSync(file, "utf8");
      assert.ok(!/children\?:\s*unknown/.test(src), `layout types children as unknown: ${file}`);
    }
  });
});

// ---------------------------------------------------------------------------
// detectPackageManager — "Next steps" commands should match the PM the user ran.
// ---------------------------------------------------------------------------

describe("detectPackageManager", () => {
  function detectPackageManager(): string {
    const ua = process.env.npm_config_user_agent || "";
    if (ua.startsWith("pnpm")) return "pnpm";
    if (ua.startsWith("yarn")) return "yarn";
    if (ua.startsWith("bun")) return "bun";
    return "npm";
  }

  const cases: Array<[string, string]> = [
    ["npm/10.2.3 node/v22.0.0", "npm"],
    ["pnpm/8.15.0 node/v22.0.0", "pnpm"],
    ["yarn/1.22.19", "yarn"],
    ["bun/1.1.0", "bun"],
    ["", "npm"],
  ];

  for (const [ua, expected] of cases) {
    it(`maps "${ua.slice(0, 8)}" -> ${expected}`, () => {
      const prev = process.env.npm_config_user_agent;
      process.env.npm_config_user_agent = ua;
      try {
        assert.equal(detectPackageManager(), expected);
      } finally {
        if (prev === undefined) delete process.env.npm_config_user_agent;
        else process.env.npm_config_user_agent = prev;
      }
    });
  }
});

// ---------------------------------------------------------------------------
// scaffoldProject — the REAL export, not a mirror. This is the exact library
// path `neutron-ts init` calls, so it must actually scaffold. Scratch dirs are
// created inside the package (not os.tmpdir) and removed in finally blocks.
// ---------------------------------------------------------------------------

describe("scaffoldProject (real export)", () => {
  function scratchPath(name: string): string {
    return path.join(process.cwd(), `.scaffold-scratch-${name}-${process.pid}-${Date.now()}`);
  }

  function assertNoRawTokens(rootDir: string, context: string): void {
    const visit = (dir: string) => {
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
        assert.ok(
          !/__[A-Z0-9_]+__/.test(content),
          `raw template token left behind in ${context}: ${entryPath}`,
        );
      }
    };
    visit(rootDir);
  }

  it("scaffolds the basic template with every token applied", async () => {
    const targetDir = scratchPath("basic");
    try {
      const result = await scaffoldProject({
        targetDir,
        template: "basic",
        runtime: "preact",
      });

      assert.equal(result.projectName, path.basename(targetDir));
      assert.equal(result.targetDir, targetDir);
      assert.equal(result.absoluteDir, path.resolve(process.cwd(), targetDir));
      assert.equal(result.template, "basic");
      assert.equal(result.runtime, "preact");

      const pkg = JSON.parse(fs.readFileSync(path.join(targetDir, "package.json"), "utf8"));
      assert.equal(pkg.name, result.packageName);
      // Scaffolding inside this workspace links the local packages.
      assert.equal(pkg.dependencies["@neutron-build/core"], "workspace:*");
      assert.equal(pkg.dependencies["@neutron-build/cli"], "workspace:*");

      // The basic home route renders __PROJECT_NAME__ as its title.
      const home = fs.readFileSync(path.join(targetDir, "src", "routes", "index.tsx"), "utf8");
      assert.ok(home.includes(`"${result.projectName}"`));
      assert.ok(fs.existsSync(path.join(targetDir, ".gitignore")));
      const config = fs.readFileSync(path.join(targetDir, "neutron.config.ts"), "utf8");
      assert.ok(config.includes('"preact"'));

      assertNoRawTokens(targetDir, "basic");
    } finally {
      fs.rmSync(targetDir, { recursive: true, force: true });
    }
  });

  it("scaffolds every template with the runtime token applied", async () => {
    for (const template of ["marketing", "app", "full", "docs"] as const) {
      const targetDir = scratchPath(template);
      try {
        const result = await scaffoldProject({
          targetDir,
          template,
          runtime: "react-compat",
        });
        assert.equal(result.template, template);
        assert.equal(result.runtime, "react-compat");
        assertNoRawTokens(targetDir, template);
        if (template !== "docs") {
          const config = fs.readFileSync(path.join(targetDir, "neutron.config.ts"), "utf8");
          assert.ok(config.includes('"react-compat"'), `${template} runtime token not applied`);
        }
      } finally {
        fs.rmSync(targetDir, { recursive: true, force: true });
      }
    }
  });

  it("refuses to scaffold into a non-empty directory", async () => {
    const targetDir = scratchPath("notempty");
    try {
      fs.mkdirSync(targetDir, { recursive: true });
      fs.writeFileSync(path.join(targetDir, "file.txt"), "hi");
      await assert.rejects(
        () => scaffoldProject({ targetDir, template: "basic", runtime: "preact" }),
        /not empty/,
      );
    } finally {
      fs.rmSync(targetDir, { recursive: true, force: true });
    }
  });
});
