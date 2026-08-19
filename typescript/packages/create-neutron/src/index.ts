#!/usr/bin/env node

import {
  TEMPLATE_NAMES,
  isTemplateName,
  printScaffoldSuccess,
  scaffoldProject,
  type RuntimeMode,
  type TemplateName,
} from "./scaffold.js";

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

  const result = await scaffoldProject(options);
  printScaffoldSuccess(result);
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

main().catch((error) => {
  console.error("\nFailed to create Neutron app.");
  console.error(error);
  process.exit(1);
});
