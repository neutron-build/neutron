import * as fs from "node:fs";
import * as path from "node:path";

type DeployPreset = "vercel" | "cloudflare" | "docker" | "static";

interface DeployCheckArgs {
  preset: DeployPreset | null;
  distDir: string;
}

interface AdapterMetadata {
  routes?: {
    app?: number;
  };
  compression?: {
    enabled?: boolean;
    files?: number;
  };
}

export async function deployCheck(): Promise<void> {
  const cwd = process.cwd();
  const args = parseDeployCheckArgs(process.argv.slice(3));
  const distDir = path.resolve(cwd, args.distDir);

  if (!fs.existsSync(distDir)) {
    console.error(`Dist directory not found: ${distDir}`);
    process.exit(1);
  }

  const presets = args.preset ? [args.preset] : detectPresetsFromDist(distDir);
  if (presets.length === 0) {
    console.error(
      "No deployment preset detected. Pass --preset vercel|cloudflare|docker|static or run a preset build first."
    );
    process.exit(1);
  }

  let hasFailures = false;
  for (const preset of presets) {
    const failures = runChecksForPreset(preset, distDir);
    if (failures.length > 0) {
      hasFailures = true;
      console.error(`\n[deploy-check] ${preset}: FAILED`);
      for (const failure of failures) {
        console.error(`  - ${failure}`);
      }
    } else {
      console.log(`[deploy-check] ${preset}: OK`);
    }
  }

  if (hasFailures) {
    process.exit(1);
  }
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

function runChecksForPreset(preset: DeployPreset, distDir: string): string[] {
  const failures: string[] = [];

  const requireFile = (relativePath: string): void => {
    const fullPath = path.join(distDir, relativePath);
    if (!fs.existsSync(fullPath)) {
      failures.push(`Missing file: ${relativePath}`);
    }
  };

  requireFile("index.html");

  if (preset === "vercel") {
    requireFile("vercel.json");
    const metadata = readMetadata(path.join(distDir, ".neutron-adapter-vercel.json"));
    if ((metadata.routes?.app || 0) > 0) {
      requireFile("api/__neutron.mjs");
      requireFile("server/node/entry.js");
    }
  }

  if (preset === "cloudflare") {
    requireFile("wrangler.json");
    const metadata = readMetadata(path.join(distDir, ".neutron-adapter-cloudflare.json"));
    if ((metadata.routes?.app || 0) > 0) {
      requireFile("_worker.js");
      requireFile("server/worker/entry.js");
    }
  }

  if (preset === "docker") {
    requireFile("Dockerfile");
    requireFile("server.mjs");
    const metadata = readMetadata(path.join(distDir, ".neutron-adapter-docker.json"));
    if ((metadata.routes?.app || 0) > 0) {
      requireFile("server/node/entry.js");
    }
  }

  if (preset === "static") {
    requireFile("_headers");
    requireFile(".neutron-static-policy.json");
    const metadataPath = path.join(distDir, ".neutron-adapter-static.json");
    requireFile(".neutron-adapter-static.json");
    const metadata = readMetadata(metadataPath);
    if (metadata.compression?.enabled) {
      const compressedCount = countFilesBySuffix(distDir, [".br", ".gz"]);
      if (compressedCount === 0) {
        failures.push(
          "Static compression enabled but no .br/.gz files found in dist output."
        );
      }
    }
  }

  return failures;
}

function readMetadata(metadataPath: string): AdapterMetadata {
  if (!fs.existsSync(metadataPath)) {
    return {};
  }
  try {
    const raw = fs.readFileSync(metadataPath, "utf-8");
    return JSON.parse(raw) as AdapterMetadata;
  } catch {
    return {};
  }
}

function countFilesBySuffix(dir: string, suffixes: string[]): number {
  let count = 0;
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      count += countFilesBySuffix(fullPath, suffixes);
      continue;
    }
    if (!entry.isFile()) {
      continue;
    }
    if (suffixes.some((suffix) => entry.name.endsWith(suffix))) {
      count += 1;
    }
  }

  return count;
}
