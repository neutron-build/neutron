import { spawn } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve, sep } from "node:path";

/** RFC 7807 problem shape, matching the rest of the ecosystem. */
export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail: string;
}

export class AgentError extends Error {
  readonly problem: ProblemDetails;
  constructor(problem: ProblemDetails, options?: { cause?: unknown }) {
    super(problem.detail, options?.cause === undefined ? undefined : { cause: options.cause });
    this.name = "AgentError";
    this.problem = problem;
  }
}

export function problemFromStatus(status: number, detail: string): ProblemDetails {
  const known: Record<number, { suffix: string; title: string }> = {
    400: { suffix: "bad-request", title: "Bad Request" },
    404: { suffix: "not-found", title: "Not Found" },
    500: { suffix: "internal", title: "Internal Server Error" },
  };
  const normalized = known[status] ? status : status >= 500 ? 500 : 400;
  const { suffix, title } = known[normalized]!;
  return { type: `https://neutron.dev/errors/${suffix}`, title, status: normalized, detail };
}

export interface ExecOptions {
  /** Working directory relative to the executor's root. */
  cwd?: string;
  env?: Record<string, string>;
  /** Kill the command after this many milliseconds (default 120000). */
  timeoutMs?: number;
  /** Cap on captured stdout+stderr bytes (default 1 MiB; excess is truncated). */
  maxOutputBytes?: number;
}

export interface ExecResult {
  exitCode: number;
  stdout: string;
  stderr: string;
  /** True when the command was killed by the timeout. */
  timedOut: boolean;
  /** True when output was truncated by maxOutputBytes. */
  truncated: boolean;
}

/**
 * THE execution contract (per the platform plan): agents act on compute
 * through exactly this interface. LocalExecutor covers dev/test and
 * trusted personal use; the Teploy sandbox daemon's HTTP client
 * (SandboxExecutor) implements the same contract — neither side blocks
 * the other.
 */
export interface AgentExecutor {
  exec(command: string, options?: ExecOptions): Promise<ExecResult>;
  putFile(path: string, data: Uint8Array | string): Promise<void>;
  getFile(path: string): Promise<Uint8Array>;
  destroy(): Promise<void>;
}

export interface LocalExecutorOptions {
  /** Root directory all paths and cwd resolve inside (required — no implicit CWD). */
  root: string;
  /** Shell used for exec (default /bin/sh -c). */
  shell?: string;
  env?: Record<string, string>;
  /**
   * Env var NAMES stripped from the inherited environment before every
   * exec — secret scoping for agent workloads sharing the host process
   * (an agent's `env` must not read the operator's tokens). Explicit
   * `env` values (constructor or per-exec) still win over the strip.
   *
   * Fail-safe default: a curated list of well-known credential variables is
   * stripped even when this option is omitted. Pass `[]` to restore full
   * inheritance.
   */
  envDenylist?: string[];
}

/**
 * Well-known credential-carrying env var names stripped by default. Not a
 * sandbox — but a model-chosen shell command must not read the operator's
 * tokens just because it shares the host process.
 */
const DEFAULT_SECRET_ENV_DENYLIST = [
  "ANTHROPIC_API_KEY",
  "OPENAI_API_KEY",
  "GEMINI_API_KEY",
  "GOOGLE_API_KEY",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "AWS_SESSION_TOKEN",
  "AZURE_CLIENT_SECRET",
  "GITHUB_TOKEN",
  "GH_TOKEN",
  "GITLAB_TOKEN",
  "FORGEJO_TOKEN",
  "NPM_TOKEN",
  "PYPI_TOKEN",
  "STRIPE_SECRET_KEY",
  "DATABASE_URL",
  "REDIS_URL",
];

/**
 * Runs commands as child processes under a root directory. Path arguments
 * are confined to the root (a path escaping it is a 400) — honest local
 * isolation, not a sandbox: commands themselves can still do whatever the
 * user running them can. Use the Teploy sandbox for untrusted work.
 */
export class LocalExecutor implements AgentExecutor {
  #root: string;
  #shell: string;
  #env?: Record<string, string>;
  #envDenylist: string[];
  #destroyed = false;

  constructor(options: LocalExecutorOptions) {
    this.#root = resolve(options.root);
    this.#shell = options.shell ?? "/bin/sh";
    if (options.env !== undefined) this.#env = options.env;
    this.#envDenylist = options.envDenylist ?? DEFAULT_SECRET_ENV_DENYLIST;
  }

  #execEnv(overrides?: Record<string, string>): NodeJS.ProcessEnv {
    const env: NodeJS.ProcessEnv = { ...process.env };
    for (const name of this.#envDenylist) delete env[name];
    return { ...env, ...this.#env, ...overrides };
  }

  #resolveInsideRoot(path: string): string {
    const full = resolve(this.#root, path);
    if (full !== this.#root && !full.startsWith(this.#root + sep)) {
      throw new AgentError(problemFromStatus(400, `Path escapes the executor root: ${path}`));
    }
    return full;
  }

  #assertLive(): void {
    if (this.#destroyed) {
      throw new AgentError(problemFromStatus(400, "Executor was destroyed."));
    }
  }

  async exec(command: string, options: ExecOptions = {}): Promise<ExecResult> {
    this.#assertLive();
    const cwd = options.cwd !== undefined ? this.#resolveInsideRoot(options.cwd) : this.#root;
    const timeoutMs = options.timeoutMs ?? 120_000;
    const maxBytes = options.maxOutputBytes ?? 1_048_576;

    return new Promise<ExecResult>((resolvePromise, rejectPromise) => {
      // detached: the shell becomes its own process group leader, so the
      // timeout can SIGKILL the WHOLE group. Killing only the shell pid
      // orphans background grandchildren while the executor reports
      // timedOut: true.
      const child = spawn(this.#shell, ["-c", command], {
        cwd,
        env: this.#execEnv(options.env),
        stdio: ["ignore", "pipe", "pipe"],
        detached: true,
      });

      let stdout = "";
      let stderr = "";
      let truncated = false;
      let timedOut = false;
      const capture = (current: string, chunk: Buffer): string => {
        if (current.length >= maxBytes) {
          truncated = true;
          return current;
        }
        const next = current + chunk.toString("utf8");
        if (next.length > maxBytes) {
          truncated = true;
          return next.slice(0, maxBytes);
        }
        return next;
      };
      child.stdout.on("data", (chunk: Buffer) => (stdout = capture(stdout, chunk)));
      child.stderr.on("data", (chunk: Buffer) => (stderr = capture(stderr, chunk)));

      const timer = setTimeout(() => {
        timedOut = true;
        if (child.pid !== undefined) {
          try {
            // Negative pid = the whole process group (see detached above).
            process.kill(-child.pid, "SIGKILL");
          } catch {
            child.kill("SIGKILL");
          }
        } else {
          child.kill("SIGKILL");
        }
      }, timeoutMs);
      timer.unref?.();

      child.on("error", (error) => {
        clearTimeout(timer);
        rejectPromise(new AgentError(problemFromStatus(500, `exec failed: ${error.message}`), { cause: error }));
      });
      child.on("close", (code) => {
        clearTimeout(timer);
        resolvePromise({ exitCode: code ?? -1, stdout, stderr, timedOut, truncated });
      });
    });
  }

  async putFile(path: string, data: Uint8Array | string): Promise<void> {
    this.#assertLive();
    const full = this.#resolveInsideRoot(path);
    await mkdir(dirname(full), { recursive: true });
    await writeFile(full, data);
  }

  async getFile(path: string): Promise<Uint8Array> {
    this.#assertLive();
    const full = this.#resolveInsideRoot(path);
    try {
      return new Uint8Array(await readFile(full));
    } catch (cause) {
      throw new AgentError(problemFromStatus(404, `No such file: ${path}`), { cause });
    }
  }

  async destroy(): Promise<void> {
    this.#destroyed = true;
  }
}
