import { createHash, createHmac, randomBytes, scrypt as scryptCallback, timingSafeEqual } from "node:crypto";
import { promisify } from "node:util";
import { db } from "./store.js";

const scrypt = promisify(scryptCallback) as (
  password: string,
  salt: Buffer,
  length: number,
  options: { N: number; r: number; p: number; maxmem: number },
) => Promise<Buffer>;

const COOKIE = "n_session";
const SESSION_DAYS = 30;
const SCRYPT = { N: 2 ** 15, r: 8, p: 1, maxmem: 128 * 1024 * 1024 };
const KEY_LENGTH = 32;

// Passwords

// Dots, not `$`: this value travels through .env files and shell quoting.
export async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(16);
  const key = await scrypt(password, salt, KEY_LENGTH, SCRYPT);
  return `scrypt.${SCRYPT.N}.${SCRYPT.r}.${SCRYPT.p}.${salt.toString("base64url")}.${key.toString("base64url")}`;
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const [scheme, N, r, p, salt, key] = stored.split(".");
  if (scheme !== "scrypt" || !key) return false;
  const expected = Buffer.from(key, "base64url");
  const actual = await scrypt(password, Buffer.from(salt, "base64url"), expected.length, {
    N: Number(N),
    r: Number(r),
    p: Number(p),
    maxmem: SCRYPT.maxmem,
  });
  return timingSafeEqual(actual, expected);
}

export const passwordHash = () => process.env.CMS_PASSWORD_HASH || null;

const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");

// Sessions. Binding each to the password means changing it signs everything out.

export function createSession(hash: string): string {
  const token = randomBytes(32).toString("base64url");
  const expires = new Date(Date.now() + SESSION_DAYS * 86_400_000).toISOString();
  db().prepare("DELETE FROM sessions WHERE expires_at < ?").run(new Date().toISOString());
  db().prepare("INSERT INTO sessions (token_hash, password_tag, expires_at) VALUES (?, ?, ?)").run(sha256(token), sha256(hash), expires);
  return token;
}

function cookieValues(request: Request): string[] {
  const values: string[] = [];
  for (const part of (request.headers.get("cookie") ?? "").split(";")) {
    const [name, value] = part.trim().split("=");
    if (name === COOKIE && value) values.push(value);
  }
  return values.slice(0, 8);
}

/** A browser can send several cookies of this name; any one that is valid counts. */
export function hasSession(request: Request): boolean {
  const hash = passwordHash();
  const tokens = cookieValues(request);
  if (!hash || tokens.length === 0) return false;
  const find = db().prepare("SELECT 1 FROM sessions WHERE token_hash = ? AND password_tag = ? AND expires_at > ?");
  const now = new Date().toISOString();
  return tokens.some((token) => Boolean(find.get(sha256(token), sha256(hash), now)));
}

// Drafts of designed pages open in the Edit site frame, on the site's own address, where the
// session cookie (scoped to the admin) never reaches. A short-lived signed key in the address does.
const PREVIEW_KEY = randomBytes(32);
const PREVIEW_HOURS = 12;
const previewMac = (slug: string, expires: string) => createHmac("sha256", PREVIEW_KEY).update(`${slug}\n${expires}`).digest("base64url");

export function previewToken(slug: string): string {
  const expires = String(Date.now() + PREVIEW_HOURS * 3_600_000);
  return `${expires}.${previewMac(slug, expires)}`;
}

export function isPreview(slug: string, token: string | null): boolean {
  const [expires, mac] = (token ?? "").split(".");
  if (!expires || !mac || !(Number(expires) > Date.now())) return false;
  const expected = Buffer.from(previewMac(slug, expires));
  const given = Buffer.from(mac);
  return given.length === expected.length && timingSafeEqual(given, expected);
}

export function endSession(request: Request): void {
  const remove = db().prepare("DELETE FROM sessions WHERE token_hash = ?");
  for (const token of cookieValues(request)) remove.run(sha256(token));
}

export function sessionCookie(token: string | null, path = "/n-admin"): string {
  const secure = process.env.NODE_ENV === "production" || process.env.SITE_URL?.startsWith("https:") ? "; Secure" : "";
  const age = token ? SESSION_DAYS * 86_400 : 0;
  return `${COOKIE}=${token ?? ""}; Path=${path}; HttpOnly; SameSite=Strict; Max-Age=${age}${secure}`;
}

// Throttle. Per client, so an attacker cannot lock the owner out. Counted before
// the slow hash, so a burst cannot run ahead of it.

const WINDOW_MS = 15 * 60_000;
const MAX_ATTEMPTS = 5;
const MAX_CLIENTS = 10_000;
const attempts = new Map<string, { count: number; resetAt: number }>();

// Neutron records each request's socket address here (core's server/peer.ts). Looked up per request,
// because this module can load before Neutron creates the registry; read without importing core.
const PEERS = Symbol.for("@neutron-build/core/transport-peer");
const peerOf = (request: Request) =>
  (globalThis as unknown as Record<symbol, WeakMap<Request, { remoteAddress: string }> | undefined>)[PEERS]?.get(request)?.remoteAddress;

const isLocalAddress = (address: string) =>
  /^(127\.|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|::1$|fc|fd|::ffff:(127|10|192\.168|172\.(1[6-9]|2\d|3[01]))\.)/i.test(address);

/** IPv6 hosts get a /64 each, so a single network can't mint new addresses to dodge the count. */
function bucket(address: string): string {
  const plain = address.replace(/^::ffff:/i, "");
  if (!plain.includes(":")) return plain;
  return plain.split(":").slice(0, 4).join(":") + "::/64";
}

/**
 * The connection's own address. A forwarded address is believed only when the connection comes
 * from a proxy on this machine or network, such as Caddy, which replaces what the client sent.
 */
export function clientAddress(request: Request): string {
  const forwarded = request.headers.get("x-forwarded-for")?.split(",").at(-1)?.trim();
  const peer = peerOf(request);
  if (peer && !isLocalAddress(peer)) return bucket(peer);
  return bucket(forwarded || peer || "unknown");
}

export function takeAttempt(client: string, at = Date.now()): boolean {
  if (!attempts.has(client) && attempts.size >= MAX_CLIENTS) {
    for (const [key, entry] of attempts) if (entry.resetAt <= at) attempts.delete(key);
    // Still full: drop the oldest entry, never everything, so a flood can't unlock everyone at once.
    if (attempts.size >= MAX_CLIENTS) attempts.delete(attempts.keys().next().value!);
  }
  const entry = attempts.get(client);
  if (!entry || entry.resetAt <= at) {
    attempts.set(client, { count: 1, resetAt: at + WINDOW_MS });
    return true;
  }
  entry.count += 1;
  return entry.count <= MAX_ATTEMPTS;
}

// At most two password checks run at once, and a short queue waits behind them. Each check is about
// 70 ms of CPU, so this bounds what any number of login attempts can cost the server.
const MAX_CHECKS = 2;
const MAX_WAITING = 16;
let running = 0;
const waiting: (() => void)[] = [];

export async function withPasswordCheck<T>(work: () => Promise<T>): Promise<T | "busy"> {
  if (running >= MAX_CHECKS) {
    if (waiting.length >= MAX_WAITING) return "busy";
    await new Promise<void>((resolve) => waiting.push(resolve));
  }
  running += 1;
  try {
    return await work();
  } finally {
    running -= 1;
    waiting.shift()?.();
  }
}

export const clearAttempts = (client: string) => {
  attempts.delete(client);
};

/** Reads at most `limit` bytes, however the body is framed. */
export async function readLimited(request: Request, limit: number): Promise<string | null> {
  if (!request.body) return "";
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.length;
    if (size > limit) {
      await reader.cancel();
      return null;
    }
    chunks.push(value);
  }
  return Buffer.concat(chunks).toString("utf8");
}
