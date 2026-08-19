import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

export type RuntimeMode = "preact" | "react-compat";
export type TemplateName = "basic" | "marketing" | "app" | "full" | "docs";

export const TEMPLATE_NAMES: TemplateName[] = ["basic", "marketing", "app", "full", "docs"];

export interface ScaffoldOptions {
  targetDir: string;
  template: TemplateName;
  runtime: RuntimeMode;
}

export interface ScaffoldResult {
  targetDir: string;
  absoluteDir: string;
  projectName: string;
  packageName: string;
  template: TemplateName;
  runtime: RuntimeMode;
}

export async function scaffoldProject(options: ScaffoldOptions): Promise<ScaffoldResult> {
  const absoluteTargetDir = path.resolve(process.cwd(), options.targetDir);
  const projectName = path.basename(absoluteTargetDir);
  const packageName = toPackageName(projectName);
  const dependencyVersions = resolveDependencyVersions(absoluteTargetDir);

  await ensureTargetDirectory(absoluteTargetDir);
  await copyTemplate(options.template, absoluteTargetDir, {
    PROJECT_NAME: projectName,
    PACKAGE_NAME: packageName,
    RUNTIME: options.runtime,
    NEUTRON_VERSION: dependencyVersions.neutron,
    NEUTRON_CLI_VERSION: dependencyVersions.neutronCli,
  });

  return {
    targetDir: options.targetDir,
    absoluteDir: absoluteTargetDir,
    projectName,
    packageName,
    template: options.template,
    runtime: options.runtime,
  };
}

export function printScaffoldSuccess(result: ScaffoldResult): void {
  const pm = detectPackageManager();
  const install = pm === "yarn" ? "yarn" : `${pm} install`;
  const dev = pm === "npm" ? "npm run dev" : `${pm} dev`;
  console.log(`\nCreated ${result.projectName} (template: ${result.template}, runtime: ${result.runtime})\n`);
  console.log("Next steps:");
  console.log(`  cd ${result.targetDir}`);
  console.log(`  ${install}`);
  console.log(`  ${dev}\n`);
}

function detectPackageManager(): string {
  const ua = process.env.npm_config_user_agent || "";
  if (ua.startsWith("pnpm")) return "pnpm";
  if (ua.startsWith("yarn")) return "yarn";
  if (ua.startsWith("bun")) return "bun";
  return "npm";
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

export function isTemplateName(value: string): value is TemplateName {
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
