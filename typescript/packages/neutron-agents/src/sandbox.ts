import { AgentError, problemFromStatus } from "./executor.js";
import type { AgentExecutor, ExecOptions, ExecResult } from "./executor.js";

/**
 * Client for the teploy-sandbox daemon (contract:
 * teploy-platform/_internal/SANDBOX_DESIGN.md). Both sides build against
 * this wire shape; the daemon need not exist for agents to be authored.
 *
 * Exec streams SSE frames the client defines as the contract:
 *   event: stdout | stderr   data: <utf8 chunk>
 *   event: exit              data: {"exitCode": n, "timedOut": bool}
 */
export interface SandboxExecutorOptions {
  /** Daemon base URL (loopback or teploy network — never public). */
  baseURL: string;
  /** Bearer token minted at daemon start. */
  token: string;
  /**
   * Deadline for each HTTP round trip to the daemon (default 300s; exec
   * requests get at least their own timeout plus 60s of slack). Without a
   * deadline a hung daemon hangs exec/putFile/destroy forever — the
   * daemon-side `timeoutSec` only helps when the daemon is alive enough to
   * enforce it.
   */
  requestTimeoutMs?: number;
  /** Custom transport; also the test seam. */
  fetch?: typeof globalThis.fetch;
}

export interface SandboxCreateOptions {
  image: string;
  env?: Record<string, string>;
  /** Runs are reaped after this many seconds regardless (default daemon-side 30min). */
  ttlSec?: number;
  limits?: { memoryMb?: number; cpus?: number; pids?: number };
  /** Default "none"; "egress" joins the NAT'd egress-only bridge. */
  network?: "none" | "egress";
}

export class SandboxExecutor implements AgentExecutor {
  readonly runId: string;
  #options: SandboxExecutorOptions;

  private constructor(runId: string, options: SandboxExecutorOptions) {
    this.runId = runId;
    this.#options = options;
  }

  /** Create a fresh sandboxed run and attach to it. */
  static async start(options: SandboxExecutorOptions & { create: SandboxCreateOptions }): Promise<SandboxExecutor> {
    const response = await request(options, "POST", "/v1/runs", JSON.stringify(options.create), {
      "content-type": "application/json",
    });
    const body = (await response.json()) as { id?: string };
    if (typeof body.id !== "string") {
      throw new AgentError(problemFromStatus(500, "Sandbox daemon returned no run id."));
    }
    return new SandboxExecutor(body.id, options);
  }

  /** Attach to an existing run (e.g. resumed from a snapshot or a prior turn). */
  static attach(runId: string, options: SandboxExecutorOptions): SandboxExecutor {
    return new SandboxExecutor(runId, options);
  }

  /**
   * Commit this run's filesystem to a snapshot image. The image survives
   * the run (and the daemon's TTL reaper) — start a later run from it
   * via `SandboxExecutor.start({ create: { image } })`. Delete it with
   * `SandboxExecutor.deleteSnapshot` when done.
   */
  async snapshot(): Promise<string> {
    const response = await request(this.#options, "POST", `/v1/runs/${this.runId}/snapshot`);
    const body = (await response.json()) as { image?: string };
    if (typeof body.image !== "string") {
      throw new AgentError(problemFromStatus(500, "Sandbox daemon returned no snapshot image."));
    }
    return body.image;
  }

  /** Delete a snapshot image (static — the run that made it may be long gone). */
  static async deleteSnapshot(options: SandboxExecutorOptions & { image: string }): Promise<void> {
    await request(options, "DELETE", `/v1/snapshots?image=${encodeURIComponent(options.image)}`);
  }

  async exec(command: string, options: ExecOptions = {}): Promise<ExecResult> {
    const timeoutSec = Math.ceil((options.timeoutMs ?? 120_000) / 1000);
    // The exec response streams for the whole command, so its deadline must
    // cover the daemon-enforced timeout plus slack — unless the caller set
    // an explicit requestTimeoutMs, which then binds.
    const requestTimeoutMs =
      this.#options.requestTimeoutMs ??
      Math.max(300_000, (options.timeoutMs ?? 120_000) + 60_000);
    const response = await request(
      this.#options,
      "POST",
      `/v1/runs/${this.runId}/exec`,
      JSON.stringify({ cmd: command, timeoutSec, ...(options.cwd !== undefined ? { cwd: options.cwd } : {}) }),
      { "content-type": "application/json" },
      requestTimeoutMs,
    );
    if (response.body === null) {
      throw new AgentError(problemFromStatus(500, "Sandbox exec response had no body."));
    }

    const maxBytes = options.maxOutputBytes ?? 1_048_576;
    let stdout = "";
    let stderr = "";
    let truncated = false;
    let exitCode = -1;
    let timedOut = false;
    const cap = (current: string, chunk: string): string => {
      if (current.length >= maxBytes) {
        truncated = true;
        return current;
      }
      const next = current + chunk;
      if (next.length > maxBytes) {
        truncated = true;
        return next.slice(0, maxBytes);
      }
      return next;
    };

    for await (const frame of parseSSE(response.body)) {
      if (frame.event === "stdout") stdout = cap(stdout, frame.data);
      else if (frame.event === "stderr") stderr = cap(stderr, frame.data);
      else if (frame.event === "exit") {
        let info: { exitCode?: number; timedOut?: boolean };
        try {
          info = JSON.parse(frame.data) as { exitCode?: number; timedOut?: boolean };
        } catch (cause) {
          throw new AgentError(problemFromStatus(500, "Sandbox daemon sent a malformed exit frame."), { cause });
        }
        exitCode = info.exitCode ?? -1;
        timedOut = info.timedOut === true;
      }
    }
    return { exitCode, stdout, stderr, timedOut, truncated };
  }

  async putFile(path: string, data: Uint8Array | string): Promise<void> {
    await request(this.#options, "PUT", `/v1/runs/${this.runId}/files/${encodePath(path)}`, data, {
      "content-type": "application/octet-stream",
    });
  }

  async getFile(path: string): Promise<Uint8Array> {
    const response = await request(this.#options, "GET", `/v1/runs/${this.runId}/files/${encodePath(path)}`);
    return new Uint8Array(await response.arrayBuffer());
  }

  async destroy(): Promise<void> {
    await request(this.#options, "DELETE", `/v1/runs/${this.runId}`);
  }
}

function encodePath(path: string): string {
  if (path.split("/").some((segment) => segment === "..")) {
    throw new AgentError(problemFromStatus(400, `Path escapes the sandbox workdir: ${path}`));
  }
  return path.split("/").map(encodeURIComponent).join("/");
}

const DEFAULT_REQUEST_TIMEOUT_MS = 300_000;

async function request(
  options: SandboxExecutorOptions,
  method: string,
  path: string,
  body?: BodyInit,
  headers: Record<string, string> = {},
  timeoutMs: number = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
): Promise<Response> {
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeoutError = new Error(`Sandbox request timed out after ${timeoutMs}ms.`);
  timeoutError.name = "TimeoutError";
  let response: Response;
  try {
    // Race the deadline rather than relying on the fetch implementation
    // honoring the abort signal — a hung (or stubbed) transport must not
    // hang the executor forever.
    response = await Promise.race([
      fetchImpl(`${options.baseURL.replace(/\/+$/, "")}${path}`, {
        method,
        headers: { authorization: `Bearer ${options.token}`, ...headers },
        ...(body !== undefined ? { body } : {}),
        signal: controller.signal,
      }),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          controller.abort();
          reject(timeoutError);
        }, timeoutMs);
      }),
    ]);
  } catch (cause) {
    const aborted = cause instanceof Error && (cause.name === "TimeoutError" || cause.name === "AbortError");
    if (aborted) {
      throw new AgentError(
        problemFromStatus(500, `Sandbox request timed out after ${timeoutMs}ms.`),
        { cause },
      );
    }
    const message = cause instanceof Error ? cause.message : String(cause);
    throw new AgentError(problemFromStatus(500, `Sandbox request failed: ${message}`), { cause });
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
  if (!response.ok) {
    let detail = `Sandbox request failed with status ${response.status}.`;
    try {
      const problem = (await response.json()) as { detail?: string };
      if (typeof problem.detail === "string") detail = problem.detail;
    } catch {
      // non-JSON body
    }
    throw new AgentError(problemFromStatus(response.status, detail));
  }
  return response;
}

interface SSEFrame {
  event: string;
  data: string;
}

async function* parseSSE(body: ReadableStream<Uint8Array>): AsyncGenerator<SSEFrame> {
  const decoder = new TextDecoder();
  const reader = body.getReader();
  let buffer = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let boundary: number;
      while ((boundary = buffer.indexOf("\n\n")) !== -1) {
        const frame = parseFrame(buffer.slice(0, boundary));
        buffer = buffer.slice(boundary + 2);
        if (frame !== null) yield frame;
      }
    }
    const trailing = parseFrame(buffer);
    if (trailing !== null) yield trailing;
  } finally {
    // Covers both exits: a consumer that unwinds mid-stream (exec's frame
    // handling throwing, or a future early break) and a stream error.
    // releaseLock alone leaves the daemon connection streaming into the
    // void; cancel() through the reader reaches the underlying source even
    // while locked, and is a no-op once drained.
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

function parseFrame(block: string): SSEFrame | null {
  let event = "";
  const data: string[] = [];
  for (const line of block.split("\n")) {
    if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
  }
  if (event === "" && data.length === 0) return null;
  return { event, data: data.join("\n") };
}
