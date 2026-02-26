import * as fs from "node:fs";
import * as path from "node:path";
import { execFileSync } from "node:child_process";

const workspaceRoot = process.cwd();
const snapshotPath = path.join(workspaceRoot, ".turbo-ls-normalized.json");
const writeMode = process.argv.includes("--write");

const requiredPackages = new Set([
  "neutron",
  "neutron-cli",
  "create-neutron",
  "neutron-data",
]);

run();

function run() {
  const current = normalizeWorkspaceGraph(getTurboGraph());

  if (writeMode) {
    fs.writeFileSync(snapshotPath, `${JSON.stringify(current, null, 2)}\n`, "utf8");
    console.log(`[workspace-graph] Wrote snapshot: ${toRelative(snapshotPath)}`);
    return;
  }

  if (!fs.existsSync(snapshotPath)) {
    console.error(`[workspace-graph] Missing snapshot file: ${toRelative(snapshotPath)}`);
    console.error('[workspace-graph] Run "pnpm run ci:workspace:snapshot" to create it.');
    process.exit(1);
  }

  const expected = normalizeWorkspaceGraph(readJson(snapshotPath));
  const problems = [];

  const expectedSet = new Set(expected.packages.items.map((item) => packageKey(item)));
  const currentSet = new Set(current.packages.items.map((item) => packageKey(item)));

  const missing = [...expectedSet].filter((key) => !currentSet.has(key)).sort();
  const unexpected = [...currentSet].filter((key) => !expectedSet.has(key)).sort();

  if (missing.length > 0) {
    problems.push(`Missing package entries:\n${missing.map((value) => `  - ${value}`).join("\n")}`);
  }

  if (unexpected.length > 0) {
    problems.push(`Unexpected package entries:\n${unexpected.map((value) => `  - ${value}`).join("\n")}`);
  }

  for (const pkgName of requiredPackages) {
    if (!current.packages.items.some((item) => item.name === pkgName)) {
      problems.push(`Required workspace package missing from turbo graph: ${pkgName}`);
    }
  }

  if (problems.length > 0) {
    console.error("\n[workspace-graph] Workspace graph drift detected:");
    for (const problem of problems) {
      console.error(`- ${problem}`);
    }
    console.error('\n[workspace-graph] If changes are intentional, run "pnpm run ci:workspace:snapshot".');
    process.exit(1);
  }

  console.log("[workspace-graph] Workspace graph matches snapshot.");
  console.log(`[workspace-graph] Checked ${current.packages.count} packages.`);
}

function getTurboGraph() {
  const turboRun = runTurboCommand();
  if (!turboRun.ok) {
    if (isNonPortableSpawnError(turboRun.error)) {
      console.warn("[workspace-graph] turbo execution unavailable in this environment; using workspace manifest fallback.");
      return scanWorkspacePackages();
    }
    const stderr = String(turboRun.error?.stderr || "").trim();
    const message = stderr || turboRun.error?.message || "Unknown error while running turbo.";
    console.error(`[workspace-graph] Failed to run turbo ls: ${message}`);
    process.exit(1);
  }

  const jsonText = extractJson(turboRun.stdout);
  if (!jsonText) {
    console.error("[workspace-graph] turbo ls did not return valid JSON output.");
    process.exit(1);
  }

  try {
    return JSON.parse(jsonText);
  } catch {
    console.error("[workspace-graph] Failed to parse turbo ls JSON output.");
    process.exit(1);
  }
}

function runTurboCommand() {
  const localTurboEntrypoint = path.join(workspaceRoot, "node_modules", "turbo", "bin", "turbo");
  const commands = [
    { bin: process.execPath, args: [localTurboEntrypoint, "ls", "--output=json"] },
    { bin: "turbo", args: ["ls", "--output=json"] },
  ];

  for (const command of commands) {
    try {
      const stdout = execFileSync(command.bin, command.args, {
        cwd: workspaceRoot,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
      });
      return { ok: true, stdout };
    } catch (error) {
      if (!isCommandMissing(error)) {
        return { ok: false, error };
      }
    }
  }

  return { ok: false, error: new Error("turbo command not found") };
}

function scanWorkspacePackages() {
  const packageFiles = [];
  for (const pattern of readWorkspacePackagePatterns()) {
    if (!pattern.endsWith("/*")) {
      continue;
    }
    const folder = pattern.slice(0, -2);
    const absoluteFolder = path.join(workspaceRoot, folder);
    if (!fs.existsSync(absoluteFolder)) {
      continue;
    }
    for (const entry of fs.readdirSync(absoluteFolder, { withFileTypes: true })) {
      if (!entry.isDirectory()) {
        continue;
      }
      const manifestPath = path.join(absoluteFolder, entry.name, "package.json");
      if (fs.existsSync(manifestPath)) {
        packageFiles.push(manifestPath);
      }
    }
  }

  const items = packageFiles
    .map((filePath) => {
      const json = readJson(filePath);
      if (!json || typeof json.name !== "string" || json.name.length === 0) {
        return null;
      }
      return {
        name: json.name,
        path: normalizePath(path.relative(workspaceRoot, path.dirname(filePath))),
      };
    })
    .filter(Boolean);

  return {
    packageManager: "pnpm",
    packages: {
      count: items.length,
      items,
    },
  };
}

function readWorkspacePackagePatterns() {
  const workspaceFile = path.join(workspaceRoot, "pnpm-workspace.yaml");
  if (!fs.existsSync(workspaceFile)) {
    return [];
  }

  const lines = fs.readFileSync(workspaceFile, "utf8").split(/\r?\n/);
  const patterns = [];

  for (const line of lines) {
    const match = line.match(/^\s*-\s*"([^"]+)"\s*$/);
    if (match) {
      patterns.push(match[1]);
    }
  }

  return patterns;
}

function isCommandMissing(error) {
  return error?.code === "ENOENT" || /not recognized/i.test(String(error?.message || ""));
}

function isNonPortableSpawnError(error) {
  const code = String(error?.code || "");
  return code === "EPERM" || code === "EACCES" || code === "ENOENT";
}

function extractJson(text) {
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start < 0 || end < 0 || end <= start) {
    return "";
  }
  return text.slice(start, end + 1).trim();
}

function normalizeWorkspaceGraph(raw) {
  const items = Array.isArray(raw?.packages?.items) ? raw.packages.items : [];
  const normalizedItems = items
    .map((item) => ({
      name: typeof item?.name === "string" ? item.name : "",
      path: typeof item?.path === "string" ? normalizePath(item.path) : "",
    }))
    .filter((item) => item.name.length > 0 && item.path.length > 0)
    .sort((a, b) => {
      if (a.name === b.name) {
        return a.path.localeCompare(b.path);
      }
      return a.name.localeCompare(b.name);
    });

  return {
    packageManager: typeof raw?.packageManager === "string" ? raw.packageManager : "",
    packages: {
      count: normalizedItems.length,
      items: normalizedItems,
    },
  };
}

function readJson(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    console.error(`[workspace-graph] Failed to parse ${toRelative(filePath)}.`);
    process.exit(1);
  }
}

function packageKey(item) {
  return `${item.name} (${item.path})`;
}

function normalizePath(value) {
  return value.replaceAll("\\", "/");
}

function toRelative(filePath) {
  return path.relative(workspaceRoot, filePath) || filePath;
}
