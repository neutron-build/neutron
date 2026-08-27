import { spawn as nodeSpawn } from "node:child_process";

import { AIError, problemFromStatus } from "../errors.js";
import { deferred } from "../internal/deferred.js";
import { abandonmentSettler, drainEvents } from "../internal/run-to-promise.js";
import type { Usage } from "../types.js";
import type { AgentEvent, AgentHarness, AgentResult, AgentResultStatus, AgentRun, AgentRunOptions } from "./index.js";

/** Minimal process surface the adapter needs; the test seam. */
export interface SpawnedProcess {
  stdout: AsyncIterable<Uint8Array | string>;
  stderr?: AsyncIterable<Uint8Array | string>;
  kill(): void;
  /** Resolves with the exit code. */
  exited: Promise<number>;
}

export type SpawnFn = (
  command: string,
  args: string[],
  options: { cwd?: string; env?: Record<string, string> },
) => SpawnedProcess;

export interface ClaudeCodeSettings {
  /** Defaults to "claude" on PATH. */
  executable?: string;
  permissionMode?: "default" | "acceptEdits" | "bypassPermissions" | "plan";
  allowedTools?: string[];
  appendSystemPrompt?: string;
  env?: Record<string, string>;
  /** Custom process launcher; the test seam. */
  spawn?: SpawnFn;
}

/**
 * Claude Code behind the AgentHarness interface, via headless mode
 * (`claude -p --output-format stream-json --verbose`). Sessions map to
 * `--resume`; approvals are governed by permissionMode (the CLI has no
 * per-call approval channel in headless mode, so onApprovalRequest and
 * toolApprovals are not consulted here).
 */
export function claudeCode(settings: ClaudeCodeSettings = {}): AgentHarness {
  return {
    name: "claude-code",
    run(options: AgentRunOptions): AgentRun {
      if (options.prompt === undefined || options.prompt === "") {
        throw new AIError(problemFromStatus(400, "Claude Code runs require a `prompt`."));
      }
      return new ClaudeCodeRun(settings, options);
    },
  };
}

interface StreamJsonEvent {
  type?: string;
  subtype?: string;
  session_id?: string;
  message?: {
    content?: Array<{
      type?: string;
      text?: string;
      id?: string;
      name?: string;
      input?: unknown;
      tool_use_id?: string;
      content?: unknown;
      is_error?: boolean;
    }>;
  };
  result?: string;
  total_cost_usd?: number;
  usage?: { input_tokens?: number; output_tokens?: number };
  is_error?: boolean;
}

class ClaudeCodeRun implements AgentRun {
  #settings: ClaudeCodeSettings;
  #options: AgentRunOptions;
  #consumed = false;
  #stopped = false;
  #child: SpawnedProcess | null = null;
  #resultDeferred = deferred<AgentResult>();
  #settleAbandoned = abandonmentSettler(
    [this.#resultDeferred],
    // Killing the child is a no-op once it exited; on abandonment it is the
    // only thing that stops the subprocess from leaking.
    () => this.#child?.kill(),
  );

  constructor(settings: ClaudeCodeSettings, options: AgentRunOptions) {
    this.#settings = settings;
    this.#options = options;
    options.abortSignal?.addEventListener("abort", () => this.stop());
  }

  #buildArgs(): string[] {
    const options = this.#options;
    const settings = this.#settings;
    const args = ["-p", options.prompt!, "--output-format", "stream-json", "--verbose"];
    if (options.sessionId !== undefined) args.push("--resume", options.sessionId);
    if (options.model !== undefined) args.push("--model", options.model);
    if (settings.permissionMode !== undefined) args.push("--permission-mode", settings.permissionMode);
    if (settings.allowedTools !== undefined && settings.allowedTools.length > 0) {
      args.push("--allowed-tools", settings.allowedTools.join(","));
    }
    if (settings.appendSystemPrompt !== undefined) {
      args.push("--append-system-prompt", settings.appendSystemPrompt);
    }
    return args;
  }

  #start(): AsyncGenerator<AgentEvent, void, undefined> {
    if (this.#consumed) {
      throw new AIError(problemFromStatus(400, "This run's events were already consumed."));
    }
    this.#consumed = true;
    return this.#iterate();
  }

  async *#iterate(): AsyncGenerator<AgentEvent, void, undefined> {
    const spawnFn = this.#settings.spawn ?? defaultSpawn;
    const spawnOptions: { cwd?: string; env?: Record<string, string> } = {};
    if (this.#options.cwd !== undefined) spawnOptions.cwd = this.#options.cwd;
    if (this.#settings.env !== undefined) spawnOptions.env = this.#settings.env;

    let output = "";
    let sessionId: string | undefined;
    let finalEvent: StreamJsonEvent | undefined;
    let stderrTail = "";
    const toolNames = new Map<string, string>();

    try {
      const child = spawnFn(this.#settings.executable ?? "claude", this.#buildArgs(), spawnOptions);
      this.#child = child;
      if (this.#stopped) child.kill();

      if (child.stderr !== undefined) {
        const stderr = child.stderr;
        void (async () => {
          const decoder = new TextDecoder();
          for await (const chunk of stderr) {
            const text = typeof chunk === "string" ? chunk : decoder.decode(chunk, { stream: true });
            stderrTail = (stderrTail + text).slice(-2000);
          }
        })().catch(() => {});
      }

      const decoder = new TextDecoder();
      let buffer = "";
      for await (const chunk of child.stdout) {
        buffer += typeof chunk === "string" ? chunk : decoder.decode(chunk, { stream: true });
        let newline: number;
        while ((newline = buffer.indexOf("\n")) !== -1) {
          const line = buffer.slice(0, newline).trim();
          buffer = buffer.slice(newline + 1);
          if (line === "") continue;
          let event: StreamJsonEvent;
          try {
            event = JSON.parse(line) as StreamJsonEvent;
          } catch {
            continue; // non-JSON noise on stdout
          }

          if (event.session_id !== undefined && sessionId === undefined) {
            sessionId = event.session_id;
            yield { type: "session", sessionId };
          }
          if (event.type === "assistant") {
            for (const block of event.message?.content ?? []) {
              if (block.type === "text" && block.text !== undefined && block.text !== "") {
                output += block.text;
                yield { type: "text-delta", text: block.text };
              } else if (block.type === "tool_use") {
                const toolCallId = block.id ?? "";
                const toolName = block.name ?? "";
                toolNames.set(toolCallId, toolName);
                yield { type: "tool-start", toolCallId, toolName, input: block.input ?? {} };
              }
            }
          } else if (event.type === "user") {
            for (const block of event.message?.content ?? []) {
              if (block.type === "tool_result") {
                const toolCallId = block.tool_use_id ?? "";
                const toolEnd: AgentEvent = {
                  type: "tool-end",
                  toolCallId,
                  toolName: toolNames.get(toolCallId) ?? "",
                  output: block.content ?? null,
                };
                if (block.is_error === true) toolEnd.isError = true;
                yield toolEnd;
              }
            }
          } else if (event.type === "result") {
            finalEvent = event;
          }
        }
      }

      const exitCode = await child.exited;
      if (this.#stopped) {
        yield { type: "finish", status: "cancelled" };
        this.#resultDeferred.resolve(this.#buildResult("cancelled", output, sessionId, finalEvent));
        return;
      }

      let status: AgentResultStatus;
      if (finalEvent !== undefined) {
        status = finalEvent.subtype === "success" && finalEvent.is_error !== true ? "completed" : "error";
      } else {
        status = exitCode === 0 ? "completed" : "error";
      }
      const result = this.#buildResult(status, output, sessionId, finalEvent);
      if (status === "error" && result.error === undefined) {
        result.error = problemFromStatus(
          500,
          stderrTail !== "" ? `Claude Code exited with code ${exitCode}: ${stderrTail}` : `Claude Code exited with code ${exitCode}.`,
        );
      }
      yield { type: "finish", status };
      this.#resultDeferred.resolve(result);
    } catch (error) {
      if (this.#stopped) {
        yield { type: "finish", status: "cancelled" };
        this.#resultDeferred.resolve(this.#buildResult("cancelled", output, sessionId, finalEvent));
        return;
      }
      const problem =
        error instanceof AIError
          ? error.problem
          : problemFromStatus(500, error instanceof Error ? error.message : String(error));
      yield { type: "finish", status: "error" };
      const result = this.#buildResult("error", output, sessionId, finalEvent);
      result.error = problem;
      result.raw = error;
      this.#resultDeferred.resolve(result);
    } finally {
      // A consumer that breaks out of `events` abandons the generator at a
      // yield: neither the resolve block nor the catch above runs, the result
      // deferred stays pending forever, and the subprocess leaks. The shared
      // settler kills the child (a no-op once it exited) and settles the
      // deferred; on the normal and error paths it is already settled.
      this.#settleAbandoned();
    }
  }

  #buildResult(
    status: AgentResultStatus,
    output: string,
    sessionId: string | undefined,
    finalEvent: StreamJsonEvent | undefined,
  ): AgentResult {
    const result: AgentResult = {
      status,
      output: finalEvent?.result !== undefined && finalEvent.result !== "" ? finalEvent.result : output,
    };
    if (sessionId !== undefined) result.sessionId = sessionId;
    if (finalEvent?.usage !== undefined) {
      const inputTokens = finalEvent.usage.input_tokens ?? 0;
      const outputTokens = finalEvent.usage.output_tokens ?? 0;
      const usage: Usage = { inputTokens, outputTokens, totalTokens: inputTokens + outputTokens };
      result.usage = usage;
    }
    if (finalEvent?.total_cost_usd !== undefined) result.costUSD = finalEvent.total_cost_usd;
    if (finalEvent !== undefined && result.status === "error" && finalEvent.result !== undefined) {
      result.error = problemFromStatus(500, finalEvent.result);
    }
    if (finalEvent !== undefined) result.raw = finalEvent;
    return result;
  }

  #drain(): void {
    if (this.#consumed) return;
    drainEvents(this.#start());
  }

  get events(): AsyncIterable<AgentEvent> {
    return this.#start();
  }

  get result(): Promise<AgentResult> {
    this.#drain();
    return this.#resultDeferred.promise;
  }

  stop(): void {
    this.#stopped = true;
    this.#child?.kill();
  }
}

function defaultSpawn(command: string, args: string[], options: { cwd?: string; env?: Record<string, string> }): SpawnedProcess {
  const child = nodeSpawn(command, args, {
    ...(options.cwd !== undefined ? { cwd: options.cwd } : {}),
    env: options.env !== undefined ? { ...process.env, ...options.env } : process.env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  return {
    stdout: child.stdout as AsyncIterable<Uint8Array>,
    stderr: child.stderr as AsyncIterable<Uint8Array>,
    kill: () => {
      child.kill("SIGTERM");
    },
    exited: new Promise<number>((resolve) => {
      child.on("close", (code) => resolve(code ?? 0));
      child.on("error", () => resolve(-1));
    }),
  };
}
