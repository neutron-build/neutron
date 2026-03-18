import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import process from "node:process";

const frameworkPackageDir = resolveFrameworkPackageDir();

const steps = [
  { label: "Naming checks", command: ["pnpm", "run", "ci:naming"] },
  { label: "Workspace graph checks", command: ["pnpm", "run", "ci:workspace"] },
  { label: "Mirror sync checks", command: ["pnpm", "run", "ci:mirror-sync"] },
  { label: "Build packages", command: ["pnpm", "-r", "build"] },
  { label: "Framework tests", command: ["pnpm", "--dir", frameworkPackageDir, "test"] },
  { label: "Runtime compatibility smoke", command: ["pnpm", "run", "ci:runtime-compat"] },
  { label: "Deploy preset checks", command: ["pnpm", "run", "ci:deploy-presets"] },
];

if (process.env.RELEASE_CHECK_SKIP_BENCH !== "1") {
  const publishGrade = process.env.RELEASE_CHECK_PUBLISH_BENCH === "1";
  steps.push({
    label: publishGrade ? "Benchmark publish-grade protocol" : "Benchmark smoke gate",
    command: ["pnpm", "run", publishGrade ? "ci:bench:publish" : "ci:bench:smoke"],
  });
}

async function run() {
  for (const step of steps) {
    console.log(`\n=== ${step.label} ===`);
    await runCommand(step.command);
  }
  console.log("\nRelease check passed.");
}

function runCommand(command) {
  return new Promise((resolve, reject) => {
    const [bin, ...args] = command;
    const child = spawn(bin, args, {
      cwd: process.cwd(),
      stdio: "inherit",
      shell: process.platform === "win32",
      env: process.env,
    });

    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`Command failed (${code}): ${command.join(" ")}`));
    });
  });
}

function resolveFrameworkPackageDir() {
  const candidates = [
    path.resolve(process.cwd(), "packages", "neutron"),
    path.resolve(process.cwd(), "..", "packages", "neutron"),
  ];

  for (const candidate of candidates) {
    if (!fs.existsSync(path.join(candidate, "package.json"))) {
      continue;
    }
    try {
      const resolved = fs.realpathSync(candidate);
      const relative = path.relative(process.cwd(), resolved);
      return relative || ".";
    } catch {
      return candidate;
    }
  }

  return "packages/neutron";
}

run().catch((error) => {
  console.error("\nRelease check failed.");
  console.error(error);
  process.exit(1);
});
