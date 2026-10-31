import * as fs from "node:fs";
import * as path from "node:path";
import type { IncomingMessage } from "node:http";
import type { WebSocketServer } from "ws";
import type { NeutronMount, NeutronMountApp } from "../server/index.js";

/**
 * The app's optional server entry: <rootDir>/src/server.entry.{ts,tsx,js,mjs}.
 *
 * Long-lived server objects that a custom `server.ts` would otherwise build
 * BEFORE `createServer` — mounted sub-apps (tRPC, REST), the WebSocket
 * handler, anything they hold (a Prisma client, an auth instance, rate
 * limiters) — are declared here instead. The file is loaded through the same
 * module graph as the routes: from the server bundle under `neutron-ts start`
 * (it is compiled into it), through the Vite SSR runtime when there is no
 * bundle, and through Vite in `neutron-ts dev`. A module imported by a mount
 * and by a route module is therefore ONE instance. A host that built its
 * mounts itself got a second copy of that graph next to the bundle's.
 *
 * Not `src/server.ts`: that is the conventional name of an app's own custom
 * server, which calls `createServer` and must not be imported by it.
 */
export const SERVER_ENTRY_CANDIDATES = [
  "src/server.entry.ts",
  "src/server.entry.tsx",
  "src/server.entry.js",
  "src/server.entry.mjs",
];

export interface NeutronServerEntryWebSocket {
  /** Only upgrades whose pathname equals this are accepted (unset = any path). */
  path?: string;
  /** Fail-closed pre-upgrade authorization, as `createServer({ websocket: { authorize } })`. */
  authorize?: (request: IncomingMessage) => boolean | Promise<boolean>;
  /** Receives the server's `WebSocketServer` once, to attach `connection` handlers. */
  attach: (wss: WebSocketServer) => void | Promise<void>;
}

export interface NeutronServerEntry {
  /** Mounted sub-apps, merged with `createServer({ mount })` (a prefix may be declared once). */
  mount?: Record<string, NeutronMountApp | NeutronMount>;
  /** Enables the WebSocket server (if `createServer` did not) and attaches the handler. */
  websocket?: NeutronServerEntryWebSocket;
  /** Called on shutdown, after in-flight requests have drained. */
  close?: () => void | Promise<void>;
}

/** The default export of a server entry: the entry, or a (possibly async) factory for it. */
export type NeutronServerEntryExport =
  | NeutronServerEntry
  | (() => NeutronServerEntry | Promise<NeutronServerEntry>);

/** Typed identity helper for `export default defineServerEntry({ ... })`. */
export function defineServerEntry<T extends NeutronServerEntryExport>(entry: T): T {
  return entry;
}

/** Absolute path of the app's server entry file, or null (a plain fs check). */
export function findServerEntryFile(rootDir: string): string | null {
  for (const rel of SERVER_ENTRY_CANDIDATES) {
    const abs = path.join(rootDir, rel);
    if (fs.existsSync(abs)) {
      return abs;
    }
  }
  return null;
}

/**
 * Resolve a loaded server-entry module to its entry. A present file that does
 * not export a usable entry is a startup error: losing the mounts (and the
 * auth they carry) to a typo must not look like a working server.
 */
export async function resolveServerEntryModule(
  mod: unknown,
  absolutePath: string
): Promise<NeutronServerEntry> {
  const name = path.basename(absolutePath);
  const exported = (mod as { default?: unknown } | null)?.default;
  const entry = typeof exported === "function" ? await (exported as () => unknown)() : exported;
  if (!entry || typeof entry !== "object") {
    throw new TypeError(
      `Server entry ${name}: expected a default export of an object (or a function returning ` +
        `one) with \`mount\`, \`websocket\` and/or \`close\` (got ${typeof entry}).`
    );
  }
  const { mount, websocket, close } = entry as NeutronServerEntry;
  if (mount !== undefined && (mount === null || typeof mount !== "object")) {
    throw new TypeError(`Server entry ${name}: \`mount\` must be an object of prefix → Hono app.`);
  }
  if (websocket !== undefined && typeof websocket?.attach !== "function") {
    throw new TypeError(`Server entry ${name}: \`websocket.attach\` must be a function.`);
  }
  if (close !== undefined && typeof close !== "function") {
    throw new TypeError(`Server entry ${name}: \`close\` must be a function.`);
  }
  return entry as NeutronServerEntry;
}

export async function loadServerEntry(
  loadModule: (id: string) => Promise<unknown>,
  absolutePath: string
): Promise<NeutronServerEntry> {
  let mod: unknown;
  try {
    mod = await loadModule(absolutePath);
  } catch (error) {
    throw new Error(`Failed to load server entry ${path.basename(absolutePath)}: ${String(error)}`);
  }
  return resolveServerEntryModule(mod, absolutePath);
}
