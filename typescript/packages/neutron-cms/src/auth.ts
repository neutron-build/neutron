import { createHash, randomBytes, scrypt as scryptCallback, timingSafeEqual } from "node:crypto";
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

function cookieValue(request: Request): string | null {
  for (const part of (request.headers.get("cookie") ?? "").split(";")) {
    const [name, value] = part.trim().split("=");
    if (name === COOKIE && value) return value;
  }
  return null;
}

export function hasSession(request: Request): boolean {
  const hash = passwordHash();
  const token = cookieValue(request);
  if (!hash || !token) return false;
  return Boolean(
    db()
      .prepare("SELECT 1 FROM sessions WHERE token_hash = ? AND password_tag = ? AND expires_at > ?")
      .get(sha256(token), sha256(hash), new Date().toISOString()),
  );
}

export function endSession(request: Request): void {
  const token = cookieValue(request);
  if (token) db().prepare("DELETE FROM sessions WHERE token_hash = ?").run(sha256(token));
}

export function sessionCookie(token: string | null, path = "/n-admin"): string {
  const secure = process.env.NODE_ENV === "production" ? "; Secure" : "";
  const age = token ? SESSION_DAYS * 86_400 : 0;
  return `${COOKIE}=${token ?? ""}; Path=${path}; HttpOnly; SameSite=Strict; Max-Age=${age}${secure}`;
}

// Throttle. Per client, so an attacker cannot lock the owner out. Counted before
// the slow hash, so a burst cannot run ahead of it.

const WINDOW_MS = 15 * 60_000;
const MAX_ATTEMPTS = 5;
const attempts = new Map<string, { count: number; resetAt: number }>();

/** Caddy replaces any client-sent X-Forwarded-For, so its last hop is the real peer. */
export function clientAddress(request: Request): string {
  return request.headers.get("x-forwarded-for")?.split(",").at(-1)?.trim() || "local";
}

export function takeAttempt(client: string, at = Date.now()): boolean {
  if (attempts.size > 10_000) {
    for (const [key, entry] of attempts) if (entry.resetAt <= at) attempts.delete(key);
    if (attempts.size > 10_000) attempts.clear();
  }
  const entry = attempts.get(client);
  if (!entry || entry.resetAt <= at) {
    attempts.set(client, { count: 1, resetAt: at + WINDOW_MS });
    return true;
  }
  entry.count += 1;
  return entry.count <= MAX_ATTEMPTS;
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
