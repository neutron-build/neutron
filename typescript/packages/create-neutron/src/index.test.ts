import assert from "node:assert/strict";
import { describe, it } from "node:test";
import * as path from "node:path";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as os from "node:os";

// ---------------------------------------------------------------------------
// Unit-testable functions extracted from create-neutron/src/index.ts
// These mirror the source exactly so tests are self-contained.
// ---------------------------------------------------------------------------

type RuntimeMode = "preact" | "react-compat";
type TemplateName = "basic" | "marketing" | "app" | "full";

const TEMPLATE_NAMES: TemplateName[] = ["basic", "marketing", "app", "full"];

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
  it("contains exactly 4 templates", () => {
    assert.equal(TEMPLATE_NAMES.length, 4);
  });

  it("includes basic, marketing, app, and full", () => {
    assert.ok(TEMPLATE_NAMES.includes("basic"));
    assert.ok(TEMPLATE_NAMES.includes("marketing"));
    assert.ok(TEMPLATE_NAMES.includes("app"));
    assert.ok(TEMPLATE_NAMES.includes("full"));
  });
});
