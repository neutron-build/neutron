import * as fs from "node:fs";
import * as path from "node:path";
import process from "node:process";

const taskName = process.argv[2];

if (!taskName) {
  console.error("[tasks] Missing task name argument. Example: node scripts/check-workspace-tasks.mjs test");
  process.exit(1);
}

const workspaceRoot = process.cwd();
const packagesDir = path.join(workspaceRoot, "packages");

if (!fs.existsSync(packagesDir)) {
  console.error(`[tasks] Workspace packages directory not found: ${packagesDir}`);
  process.exit(1);
}

const packageJsonFiles = fs
  .readdirSync(packagesDir, { withFileTypes: true })
  .filter((entry) => entry.isDirectory())
  .map((entry) => path.join(packagesDir, entry.name, "package.json"))
  .filter((file) => fs.existsSync(file));

const missingTaskPackages = [];

for (const file of packageJsonFiles) {
  let manifest;
  try {
    manifest = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    console.error(`[tasks] Failed to parse ${toRelative(file)}.`);
    process.exit(1);
  }

  const scripts = manifest.scripts || {};
  if (!Object.prototype.hasOwnProperty.call(scripts, taskName)) {
    missingTaskPackages.push({
      name: manifest.name || toRelative(path.dirname(file)),
      path: toRelative(file),
    });
  }
}

if (missingTaskPackages.length > 0) {
  console.error(`[tasks] Missing "${taskName}" script in workspace packages:`);
  for (const pkg of missingTaskPackages) {
    console.error(`- ${pkg.name} (${pkg.path})`);
  }
  process.exit(1);
}

console.log(`[tasks] All workspace packages define "${taskName}".`);

function toRelative(filePath) {
  return path.relative(workspaceRoot, filePath) || filePath;
}
