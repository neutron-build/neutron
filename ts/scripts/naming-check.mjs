import * as fs from "node:fs";
import * as path from "node:path";
import process from "node:process";

const workspaceRoot = process.cwd();
const repoRoot = resolveRepoRoot(workspaceRoot);

const errors = [];

run();

function run() {
  const npmPackageFiles = collectUniqueFiles([
    path.join(workspaceRoot, "packages"),
    path.join(repoRoot, "packages"),
  ], "package.json");
  validateNpmPackages(npmPackageFiles);

  const cargoFiles = collectUniqueFiles([path.join(repoRoot, "rust")], "Cargo.toml");
  validateCargoPackages(cargoFiles);

  const mojoProjectFiles = collectUniqueFiles([path.join(repoRoot, "mojo")], "mojoproject.toml");
  validateMojoProjects(mojoProjectFiles);

  if (errors.length > 0) {
    console.error("\n[naming] Naming checks failed:");
    for (const error of errors) {
      console.error(`- ${error}`);
    }
    process.exit(1);
  }

  console.log("[naming] Naming checks passed.");
  console.log(
    `[naming] Checked ${npmPackageFiles.length} npm packages, ${cargoFiles.length} cargo crates, ${mojoProjectFiles.length} mojo projects.`
  );
}

function resolveRepoRoot(cwd) {
  const parent = path.resolve(cwd, "..");
  if (fs.existsSync(path.join(parent, "ARCHITECTURE.md"))) {
    return parent;
  }
  return cwd;
}

function collectUniqueFiles(roots, basename) {
  const out = [];
  const seen = new Set();
  for (const root of roots) {
    if (!fs.existsSync(root)) {
      continue;
    }
    for (const file of walkForBasename(root, basename)) {
      const real = safeRealpath(file);
      if (real && seen.has(real)) {
        continue;
      }
      if (real) {
        seen.add(real);
      } else {
        seen.add(file);
      }
      out.push(file);
    }
  }
  return out;
}

function *walkForBasename(root, basename) {
  const skipDirs = new Set(["node_modules", ".git", ".turbo", "dist", "build", "target", "coverage"]);
  const stack = [root];

  while (stack.length > 0) {
    const current = stack.pop();
    let entries = [];
    try {
      entries = fs.readdirSync(current, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        if (!skipDirs.has(entry.name)) {
          stack.push(fullPath);
        }
        continue;
      }
      if (entry.isFile() && entry.name === basename) {
        yield fullPath;
      }
    }
  }
}

function validateNpmPackages(files) {
  const allowedExact = new Set([
    "neutron",
    "neutron-cli",
    "create-neutron",
    "neutron-data",
    "neutron-monorepo",
  ]);

  for (const file of files) {
    const json = readJson(file);
    if (!json || typeof json.name !== "string") {
      continue;
    }

    const rel = toRelative(file);
    const name = json.name;

    if (name === "__PACKAGE_NAME__") {
      continue;
    }

    const valid =
      allowedExact.has(name) ||
      name.startsWith("neutron-") ||
      name.startsWith("@neutron/") ||
      name.startsWith("@nucleus/");

    if (!valid) {
      errors.push(`${rel}: npm package name "${name}" does not follow Neutron/Nucleus naming prefixes.`);
    }

    validateLayerMixing(name, rel);
  }
}

function validateCargoPackages(files) {
  for (const file of files) {
    const name = readTomlName(file);
    if (!name) {
      continue;
    }
    const rel = toRelative(file);
    const lowerRel = rel.toLowerCase();

    if (lowerRel.includes(`${path.sep}rust${path.sep}neutron${path.sep}`) && !name.startsWith("neutron")) {
      errors.push(`${rel}: cargo crate "${name}" should use the neutron prefix in rust/neutron.`);
    }

    if (lowerRel.includes(`${path.sep}rust${path.sep}nucleus${path.sep}`) && !name.startsWith("nucleus")) {
      errors.push(`${rel}: cargo crate "${name}" should use the nucleus prefix in rust/nucleus.`);
    }

    validateLayerMixing(name, rel);
  }
}

function validateMojoProjects(files) {
  for (const file of files) {
    const name = readTomlName(file);
    if (!name) {
      continue;
    }
    const rel = toRelative(file);

    const valid = name === "neutron-mojo" || name.startsWith("neutron-mojo-") || name.startsWith("nucleus-");
    if (!valid) {
      errors.push(
        `${rel}: mojo project "${name}" should use "neutron-mojo-*" or "nucleus-*" naming.`
      );
    }

    validateLayerMixing(name, rel);
  }
}

function validateLayerMixing(name, rel) {
  const lower = name.toLowerCase();
  const implementationTokens = ["typescript", "rust", "zig", "mojo"];
  const foundImplTokens = implementationTokens.filter((token) => lower.includes(token));

  if (lower.includes("neutron") && lower.includes("nucleus")) {
    errors.push(`${rel}: name "${name}" mixes platform and subsystem labels.`);
  }

  if (foundImplTokens.length > 1) {
    errors.push(`${rel}: name "${name}" mixes multiple implementation labels (${foundImplTokens.join(", ")}).`);
  }

  if (lower.includes("nucleus") && foundImplTokens.length > 0) {
    errors.push(`${rel}: name "${name}" mixes subsystem and implementation labels.`);
  }
}

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    errors.push(`${toRelative(file)}: failed to parse JSON.`);
    return null;
  }
}

function readTomlName(file) {
  const text = readText(file);
  if (!text) {
    return "";
  }
  const line = text.split(/\r?\n/).find((row) => /^\s*name\s*=\s*"/.test(row));
  if (!line) {
    return "";
  }
  const match = line.match(/^\s*name\s*=\s*"([^"]+)"/);
  return match ? match[1] : "";
}

function readText(file) {
  try {
    return fs.readFileSync(file, "utf8");
  } catch {
    errors.push(`${toRelative(file)}: failed to read file.`);
    return "";
  }
}

function toRelative(file) {
  return path.relative(repoRoot, file) || file;
}

function safeRealpath(file) {
  try {
    return fs.realpathSync(file);
  } catch {
    return "";
  }
}
