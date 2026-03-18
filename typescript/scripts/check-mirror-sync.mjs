import * as fs from "node:fs";
import * as path from "node:path";
import { createHash } from "node:crypto";

const workspaceRoot = process.cwd();
const primaryDir = path.join(workspaceRoot, "packages", "neutron");
const mirrorDir = path.resolve(workspaceRoot, "..", "packages", "neutron");
const ignoredFiles = new Set(["pnpm-lock.yaml"]);
const skippedDirs = new Set(["dist", "node_modules", ".git", ".turbo", "coverage"]);

run();

function run() {
  if (!fs.existsSync(primaryDir)) {
    console.error(`[mirror-sync] Primary package not found: ${toRelative(primaryDir)}`);
    process.exit(1);
  }

  if (!fs.existsSync(mirrorDir)) {
    console.log("[mirror-sync] External mirror not present; skipping mirror sync check.");
    return;
  }

  const primaryFiles = collectFiles(primaryDir);
  const mirrorFiles = collectFiles(mirrorDir);

  const primarySet = new Set(primaryFiles);
  const mirrorSet = new Set(mirrorFiles);
  const errors = [];

  for (const file of primaryFiles) {
    if (!mirrorSet.has(file)) {
      errors.push(`Missing in mirror: ${file}`);
    }
  }

  for (const file of mirrorFiles) {
    if (!primarySet.has(file)) {
      errors.push(`Only in mirror: ${file}`);
    }
  }

  for (const file of primaryFiles) {
    if (!mirrorSet.has(file)) {
      continue;
    }
    const primaryHash = hashFile(path.join(primaryDir, file));
    const mirrorHash = hashFile(path.join(mirrorDir, file));
    if (primaryHash !== mirrorHash) {
      errors.push(`Content mismatch: ${file}`);
    }
  }

  if (errors.length > 0) {
    console.error("\n[mirror-sync] Detected drift between primary and mirror package directories:");
    for (const error of errors) {
      console.error(`- ${error}`);
    }
    process.exit(1);
  }

  console.log(
    `[mirror-sync] Primary and mirror package directories are in sync (${primaryFiles.length} files).`
  );
}

function collectFiles(rootDir) {
  const files = [];
  walk(rootDir, "", files);
  return files.sort((left, right) => left.localeCompare(right));
}

function walk(baseDir, relativeDir, out) {
  const absoluteDir = path.join(baseDir, relativeDir);
  let entries = [];
  try {
    entries = fs.readdirSync(absoluteDir, { withFileTypes: true });
  } catch {
    return;
  }

  for (const entry of entries) {
    if (entry.isDirectory()) {
      if (!skippedDirs.has(entry.name)) {
        walk(baseDir, path.join(relativeDir, entry.name), out);
      }
      continue;
    }
    if (!entry.isFile()) {
      continue;
    }
    if (ignoredFiles.has(entry.name)) {
      continue;
    }
    out.push(normalizePath(path.join(relativeDir, entry.name)));
  }
}

function hashFile(filePath) {
  const data = fs.readFileSync(filePath);
  return createHash("sha256").update(data).digest("hex");
}

function normalizePath(filePath) {
  return filePath.split(path.sep).join("/");
}

function toRelative(filePath) {
  return path.relative(workspaceRoot, filePath) || filePath;
}
