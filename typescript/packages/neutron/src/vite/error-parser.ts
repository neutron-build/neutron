import * as fs from "node:fs";
import * as path from "node:path";

export interface NeutronErrorPayload {
  id: string;
  message: string;
  type: 'loader' | 'action' | 'render' | 'middleware' | 'hydration' | 'unknown';
  timestamp: number;
  file?: string;
  fileRelative?: string;
  line?: number;
  column?: number;
  codeFrame?: {
    lines: Array<{ number: number; text: string; highlight: boolean }>;
  };
  stack?: string;
  hint?: string;
  routeId?: string;
  requestUrl?: string;
}

const STACK_FRAME_RE = /at\s+(?:.*?\s+)?\(?(\/[^:]+|[A-Z]:\\[^:]+):(\d+):(\d+)\)?/;

function generateId(): string {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

function parseStackLocation(stack: string): { file: string; line: number; column: number } | null {
  const lines = stack.split("\n");
  for (const line of lines) {
    if (line.includes("node_modules")) continue;
    const match = STACK_FRAME_RE.exec(line);
    if (match) {
      return {
        file: match[1],
        line: parseInt(match[2], 10),
        column: parseInt(match[3], 10),
      };
    }
  }
  return null;
}

function buildCodeFrame(
  file: string,
  errorLine: number,
  contextLines: number = 5
): NeutronErrorPayload["codeFrame"] | undefined {
  let source: string;
  try {
    source = fs.readFileSync(file, "utf-8");
  } catch {
    return undefined;
  }

  const sourceLines = source.split("\n");
  const start = Math.max(0, errorLine - 1 - contextLines);
  const end = Math.min(sourceLines.length, errorLine + contextLines);

  const lines: Array<{ number: number; text: string; highlight: boolean }> = [];
  for (let i = start; i < end; i++) {
    lines.push({
      number: i + 1,
      text: sourceLines[i],
      highlight: i + 1 === errorLine,
    });
  }

  return { lines };
}

function generateHint(message: string): string | undefined {
  if (message.includes("is not a function")) {
    return "Check export names. Loaders must be named 'loader', actions must be named 'action'.";
  }
  if (message.includes("Cannot read properties of undefined") || message.includes("Cannot read property")) {
    return "Your loader might be returning undefined. Ensure the loader returns a value or object.";
  }
  if (message.includes("is not defined")) {
    return "Make sure all imports are correct and the variable is in scope.";
  }
  if (message.includes("Failed to fetch") || message.includes("ECONNREFUSED")) {
    return "A network request failed. Check that external services are running.";
  }
  if (message.includes("Unexpected token") || message.includes("SyntaxError")) {
    return "There is a syntax error in your code. Check for missing brackets or invalid syntax.";
  }
  return undefined;
}

export function parseError(
  error: Error,
  type: NeutronErrorPayload["type"],
  rootDir: string,
  routeId?: string,
  requestUrl?: string
): NeutronErrorPayload {
  const id = generateId();
  const message = error.message || String(error);
  const stack = error.stack || undefined;
  const timestamp = Date.now();

  let file: string | undefined;
  let fileRelative: string | undefined;
  let line: number | undefined;
  let column: number | undefined;
  let codeFrame: NeutronErrorPayload["codeFrame"] | undefined;

  if (stack) {
    const loc = parseStackLocation(stack);
    if (loc) {
      file = loc.file;
      line = loc.line;
      column = loc.column;
      fileRelative = path.relative(rootDir, file);
      codeFrame = buildCodeFrame(file, line);
    }
  }

  const hint = generateHint(message);

  return {
    id,
    message,
    type,
    timestamp,
    file,
    fileRelative,
    line,
    column,
    codeFrame,
    stack,
    hint,
    routeId,
    requestUrl,
  };
}
