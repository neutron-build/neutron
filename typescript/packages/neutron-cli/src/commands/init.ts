import {
  TEMPLATE_NAMES,
  isTemplateName,
  printScaffoldSuccess,
  scaffoldProject,
  type RuntimeMode,
  type TemplateName,
} from "create-neutron";

interface InitOptions {
  targetDir: string;
  template: TemplateName;
  runtime: RuntimeMode;
}

export function parseInitArgs(argv: string[]): InitOptions | null {
  // Same flag shape as `create-neutron`: positional target dir, --template and
  // --runtime in both ` ` and `=` forms, hard fail on unsupported values.
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

export function printInitUsage(): void {
  console.log(`Usage:
  neutron-ts init [project-name] [options]

Options:
  --template <name>     Template to use (default: basic)
                        ${TEMPLATE_NAMES.join(" | ")}
  --runtime <mode>      Runtime mode: preact | react-compat (default: preact)
  -h, --help            Show this help message`);
}

export async function init(): Promise<void> {
  const args = process.argv.slice(3);

  if (args.includes("-h") || args.includes("--help")) {
    printInitUsage();
    return;
  }

  const options = parseInitArgs(args);
  if (!options) {
    printInitUsage();
    process.exit(1);
  }

  const result = await scaffoldProject(options);
  printScaffoldSuccess(result);
}
