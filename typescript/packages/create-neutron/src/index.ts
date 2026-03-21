#!/usr/bin/env node

import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

type RuntimeMode = "preact" | "react-compat";
type TemplateName = "basic" | "marketing" | "app" | "full" | "docs";

const TEMPLATE_NAMES: TemplateName[] = ["basic", "marketing", "app", "full", "docs"];

interface CliOptions {
  targetDir: string;
  template: TemplateName;
  runtime: RuntimeMode;
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));

  if (!options) {
    printUsage();
    process.exit(1);
  }

  if (options.targetDir === "--help" || options.targetDir === "-h") {
    printUsage();
    return;
  }

  const absoluteTargetDir = path.resolve(process.cwd(), options.targetDir);
  const packageName = toPackageName(path.basename(absoluteTargetDir));
  const dependencyVersions = resolveDependencyVersions(absoluteTargetDir);

  await ensureTargetDirectory(absoluteTargetDir);
  await copyTemplate(options.template, absoluteTargetDir, {
    PROJECT_NAME: path.basename(absoluteTargetDir),
    PACKAGE_NAME: packageName,
    RUNTIME: options.runtime,
    NEUTRON_VERSION: dependencyVersions.neutron,
    NEUTRON_CLI_VERSION: dependencyVersions.neutronCli,
  });

  printSuccess(path.basename(absoluteTargetDir), options.template, options.runtime);
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
        console.error(`Unsupported template: ${candidate}`);
        return null;
      }
      continue;
    }

    if (arg.startsWith("--template=")) {
      const candidate = arg.split("=")[1];
      if (isTemplateName(candidate)) {
        template = candidate;
      } else {
        console.error(`Unsupported template: ${candidate}`);
        return null;
      }
      continue;
    }

    if (arg === "--runtime" && argv[i + 1]) {
      const candidate = argv[++i];
      if (candidate === "preact" || candidate === "react-compat") {
        runtime = candidate;
      } else {
        console.error(`Unsupported runtime: ${candidate}`);
        return null;
      }
      continue;
    }

    if (arg.startsWith("--runtime=")) {
      const candidate = arg.split("=")[1];
      if (candidate === "preact" || candidate === "react-compat") {
        runtime = candidate;
      } else {
        console.error(`Unsupported runtime: ${candidate}`);
        return null;
      }
      continue;
    }

    positional.push(arg);
  }

  const targetDir = positional[0] || "neutron-app";
  return { targetDir, template, runtime };
}

function printUsage(): void {
  console.log(`Usage:
  create-neutron [project-name] [options]

Options:
  --template <name>     Template to use (default: basic)
                        ${TEMPLATE_NAMES.join(" | ")}
  --runtime <mode>      Runtime mode: preact | react-compat (default: preact)
  -h, --help            Show this help message`);
}

function printSuccess(projectName: string, template: TemplateName, runtime: RuntimeMode): void {
  console.log(`\nCreated ${projectName} (template: ${template}, runtime: ${runtime})\n`);
  console.log("Next steps:");
  console.log(`  cd ${projectName}`);
  console.log("  pnpm install");
  console.log("  pnpm dev\n");
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

async function copyTemplate(
  template: TemplateName,
  targetDir: string,
  tokens: Record<string, string>
): Promise<void> {
  const templateRoot = getTemplateRoot();
  const sourceDir = path.join(templateRoot, template);

  if (!fs.existsSync(sourceDir)) {
    throw new Error(`Template not found: ${template}`);
  }

  await copyDirectory(sourceDir, targetDir, tokens);
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

function toPackageName(input: string): string {
  const normalized = input
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/^-+|-+$/g, "");

  return normalized || "neutron-app";
}

function isTemplateName(value: string): value is TemplateName {
  return TEMPLATE_NAMES.includes(value as TemplateName);
}

function resolveDependencyVersions(targetDir: string): {
  neutron: string;
  neutronCli: string;
} {
  const workspaceRoot = findWorkspaceRoot(path.dirname(targetDir));
  if (!workspaceRoot) {
    return {
      neutron: "latest",
      neutronCli: "latest",
    };
  }

  return {
    neutron: "workspace:*",
    neutronCli: "workspace:*",
  };
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

function getTemplateRoot(): string {
  const currentFile = fileURLToPath(import.meta.url);
  const currentDir = path.dirname(currentFile);
  return path.resolve(currentDir, "..", "templates");
}

main().catch((error) => {
  console.error("\nFailed to create Neutron app.");
  console.error(error);
  process.exit(1);
});
