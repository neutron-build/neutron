import * as fs from "node:fs";
import * as path from "node:path";
import {
  createServer,
  loadConfigFromFile,
  loadEnv,
  mergeConfig,
} from "vite";
import {
  resolveRuntime,
  resolveRuntimeAliases,
  resolveRuntimeNoExternal,
  type NeutronConfig,
} from "neutron";

interface WorkerRunContext {
  mode: string;
  args: string[];
  signal: AbortSignal;
  log: (message: string) => void;
}

type WorkerRunner = (
  context: WorkerRunContext
) => unknown | void | Promise<unknown | void>;

interface WorkerArgs {
  entry?: string;
  mode: string;
  once: boolean;
  workerArgs: string[];
}

const WORKER_ENTRY_CANDIDATES = [
  "src/worker.ts",
  "src/worker.tsx",
  "src/worker/index.ts",
  "src/worker/index.tsx",
  "worker.ts",
  "worker.js",
];

export async function worker(): Promise<void> {
  const cwd = process.cwd();
  const args = parseWorkerArgs(process.argv.slice(3));
  applyEnv(cwd, args.mode);
  const neutronConfig = await loadNeutronConfig(cwd, args.mode);
  const entryPath = resolveWorkerEntry(cwd, args.entry, neutronConfig.worker?.entry);

  if (!entryPath) {
    console.error(
      "Worker entry not found. Provide --entry <path>, set worker.entry in neutron.config, or create src/worker.ts."
    );
    process.exit(1);
  }

  const runtime = resolveRuntime(neutronConfig);
  const runtimeAliases = resolveRuntimeAliases(runtime);
  const runtimeNoExternal = resolveRuntimeNoExternal(runtime);
  const loadedConfig = await loadConfigFromFile(
    { command: "serve", mode: args.mode },
    undefined,
    cwd
  );
  const userConfig = loadedConfig?.config || {};

  const viteServer = await createServer(
    mergeConfig(userConfig, {
      configFile: false,
      root: cwd,
      ...(runtimeAliases ? { resolve: { alias: runtimeAliases } } : {}),
      ...(runtimeNoExternal.length > 0 ? { ssr: { noExternal: runtimeNoExternal } } : {}),
      server: {
        middlewareMode: true,
        hmr: false,
        ws: false,
      },
      optimizeDeps: {
        disabled: true,
        noDiscovery: true,
        entries: [],
      },
      appType: "custom",
      logLevel: "error",
    })
  );

  const abortController = new AbortController();
  let teardown: (() => Promise<void> | void) | undefined;
  let shuttingDown = false;

  const shutdown = async (): Promise<void> => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    abortController.abort();
    try {
      await teardown?.();
    } finally {
      await viteServer.close();
    }
  };

  process.on("SIGTERM", () => {
    void shutdown().finally(() => process.exit(0));
  });
  process.on("SIGINT", () => {
    void shutdown().finally(() => process.exit(0));
  });

  try {
    const module = await viteServer.ssrLoadModule(entryPath);
    const run = resolveWorkerRunner(module, entryPath);

    const result = await run({
      mode: args.mode,
      args: args.workerArgs,
      signal: abortController.signal,
      log: (message: string) => {
        console.log(`[worker] ${message}`);
      },
    });

    if (typeof result === "function") {
      teardown = result as () => Promise<void> | void;
    }

    if (args.once) {
      await shutdown();
      return;
    }

    console.log(`\n  Neutron worker running:\n`);
    console.log(`  Entry: ${path.relative(cwd, entryPath)}\n`);
    console.log(`  Press Ctrl+C to stop\n`);

    await new Promise<void>(() => {
      // Keep process alive until a shutdown signal is received.
    });
  } catch (error) {
    await shutdown();
    throw error;
  }
}

function resolveWorkerRunner(module: Record<string, unknown>, entryPath: string): WorkerRunner {
  const candidate = module.run || module.default;
  if (typeof candidate !== "function") {
    throw new Error(
      `Worker module "${entryPath}" must export "run" (or default) as a function.`
    );
  }
  return candidate as WorkerRunner;
}

function parseWorkerArgs(argv: string[]): WorkerArgs {
  let entry: string | undefined;
  let mode = "development";
  let once = false;

  const passthroughIndex = argv.indexOf("--");
  const workerArgs = passthroughIndex >= 0 ? argv.slice(passthroughIndex + 1) : [];
  const parsedArgs = passthroughIndex >= 0 ? argv.slice(0, passthroughIndex) : argv;

  for (let i = 0; i < parsedArgs.length; i++) {
    const arg = parsedArgs[i];
    if (arg === "--entry" && parsedArgs[i + 1]) {
      entry = parsedArgs[++i];
      continue;
    }
    if (arg.startsWith("--entry=")) {
      entry = arg.split("=")[1];
      continue;
    }
    if (arg === "--mode" && parsedArgs[i + 1]) {
      mode = parsedArgs[++i];
      continue;
    }
    if (arg.startsWith("--mode=")) {
      mode = arg.split("=")[1];
      continue;
    }
    if (arg === "--once") {
      once = true;
    }
  }

  return {
    entry,
    mode,
    once,
    workerArgs,
  };
}

function resolveWorkerEntry(
  cwd: string,
  cliEntry?: string,
  configEntry?: string
): string | null {
  const candidates = [cliEntry, configEntry, ...WORKER_ENTRY_CANDIDATES].filter(
    (candidate): candidate is string => Boolean(candidate)
  );

  for (const candidate of candidates) {
    const absolutePath = path.resolve(cwd, candidate);
    if (fs.existsSync(absolutePath)) {
      return absolutePath;
    }
  }

  return null;
}

async function loadNeutronConfig(cwd: string, mode: string): Promise<NeutronConfig> {
  const candidates = [
    "neutron.config.ts",
    "neutron.config.js",
    "neutron.config.mjs",
    "neutron.config.cjs",
  ];

  for (const file of candidates) {
    const fullPath = path.resolve(cwd, file);
    if (!fs.existsSync(fullPath)) {
      continue;
    }

    const loaded = await loadConfigFromFile({ command: "serve", mode }, fullPath, cwd);
    if (loaded?.config) {
      return loaded.config as NeutronConfig;
    }
  }

  return {};
}

function applyEnv(cwd: string, mode: string): void {
  const env = loadEnv(mode, cwd, "");
  for (const [key, value] of Object.entries(env)) {
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}
