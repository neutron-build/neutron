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

function transaction<T>(work: () => T): T {
  db().exec("BEGIN IMMEDIATE");
  try {
    const result = work();
    db().exec("COMMIT");
    return result;
  } catch (error) {
    db().exec("ROLLBACK");
    throw error;
  }
}

// Accounts. The owner logs in with CMS_PASSWORD_HASH, or with a password set in the admin against
// it. Everyone else is a person, with a name and a password of their own.

export type Account = { kind: "owner" } | { kind: "person"; id: number; name: string };
export interface Person {
  id: number;
  name: string;
  created_at: string;
}

export const MIN_PASSWORD = 12;
/** Long enough for any passphrase, short enough that refusing one costs no hashing. */
export const MAX_PASSWORD = 200;

/** As strong as the command line's. Shown once, never stored. */
export const generatePassword = () => randomBytes(24).toString("base64url");

const envTag = (env: string) => sha256(`neutron-cms owner\n${env}`);

/**
 * The hash the owner's password is checked against now. A password set in the admin counts only while
 * CMS_PASSWORD_HASH is the one it was set against: setting a new one from the command line replaces
 * it, and it is removed, so the command line is always the way back in.
 */
export function ownerHash(): string | null {
  const env = passwordHash();
  if (!env) return null;
  const row = db().prepare("SELECT env_tag, password_hash FROM owner_password WHERE id = 1").get() as { env_tag: string; password_hash: string } | undefined;
  if (!row) return env;
  if (row.env_tag === envTag(env)) return row.password_hash;
  db().prepare("DELETE FROM owner_password WHERE env_tag <> ?").run(envTag(env));
  return env;
}

/**
 * A login name, trimmed and in lower case, or null when it isn't one: letters, digits, dot, dash and
 * underscore, or an email address. Nothing that looks like another character, and nothing to escape.
 */
export function loginName(raw: string): string | null {
  const name = raw.trim().toLowerCase();
  if (name.length > 100) return null;
  return /^[a-z0-9][a-z0-9._-]{0,63}$/.test(name) || /^[a-z0-9._+-]+@[a-z0-9-]+(\.[a-z0-9-]+)+$/.test(name) ? name : null;
}

export const people = () => db().prepare("SELECT id, name, created_at FROM people ORDER BY name").all() as unknown as Person[];
export const personById = (id: number) => db().prepare("SELECT id, name, created_at FROM people WHERE id = ?").get(id) as Person | undefined;
export const hasPeople = () => Boolean(db().prepare("SELECT 1 FROM people LIMIT 1").get());

// Shaped like a real hash, so checking a name nobody has costs the same as checking one somebody has.
// What it says is never used.
const DECOY = `scrypt.${SCRYPT.N}.${SCRYPT.r}.${SCRYPT.p}.${"A".repeat(22)}.${"A".repeat(43)}`;

/**
 * Who this name and password belong to. No name is the owner. Exactly one password check runs either
 * way, so an unknown name and a wrong password take the same time and give the same answer.
 */
export async function checkLogin(rawName: string, password: string): Promise<{ account: Account; hash: string } | null> {
  if (!rawName.trim()) {
    const hash = ownerHash();
    return hash && (await verifyPassword(password, hash)) ? { account: { kind: "owner" }, hash } : null;
  }
  const name = loginName(rawName);
  const person = name ? (db().prepare("SELECT id, name, password_hash FROM people WHERE name = ?").get(name) as { id: number; name: string; password_hash: string } | undefined) : undefined;
  const verified = await verifyPassword(password, person?.password_hash ?? DECOY);
  return person && verified ? { account: { kind: "person", id: person.id, name: person.name }, hash: person.password_hash } : null;
}

/** Checks the password of an account that is logged in. */
export async function checkPassword(account: Account, password: string): Promise<boolean> {
  const hash = account.kind === "owner" ? ownerHash() : (db().prepare("SELECT password_hash FROM people WHERE id = ?").get(account.id) as { password_hash: string } | undefined)?.password_hash;
  return verifyPassword(password, hash ?? DECOY).then((verified) => Boolean(hash) && verified);
}

export function addPerson(name: string, hash: string): number | "taken" {
  const result = db().prepare("INSERT INTO people (name, password_hash, created_at) VALUES (?, ?, ?) ON CONFLICT (name) DO NOTHING").run(name, hash, new Date().toISOString());
  return result.changes ? Number(result.lastInsertRowid) : "taken";
}

/** Removes a person and ends their sessions at once. */
export function removePerson(id: number): boolean {
  return transaction(() => {
    db().prepare("DELETE FROM sessions WHERE person_id = ?").run(id);
    const removed = db().prepare("DELETE FROM people WHERE id = ?").run(id).changes > 0;
    if (removed) rotatePreviewKey();
    return removed;
  });
}

/**
 * A new password for an account. Its other sessions end; `keep`, the session making the change, stays
 * logged in. Draft preview links end too.
 */
export function setPassword(account: Account, hash: string, keep?: string): boolean {
  return transaction(() => {
    const env = passwordHash();
    if (!env) return false;
    if (account.kind === "owner") {
      db().prepare("INSERT INTO owner_password (id, env_tag, password_hash, set_at) VALUES (1, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET env_tag = excluded.env_tag, password_hash = excluded.password_hash, set_at = excluded.set_at")
        .run(envTag(env), hash, new Date().toISOString());
      db().prepare("DELETE FROM sessions WHERE person_id IS NULL AND token_hash <> ?").run(keep ?? "");
      if (keep) db().prepare("UPDATE sessions SET password_tag = ? WHERE token_hash = ? AND person_id IS NULL").run(sessionTag(hash), keep);
    } else {
      if (!db().prepare("UPDATE people SET password_hash = ? WHERE id = ?").run(hash, account.id).changes) return false;
      db().prepare("DELETE FROM sessions WHERE person_id = ? AND token_hash <> ?").run(account.id, keep ?? "");
      if (keep) db().prepare("UPDATE sessions SET password_tag = ? WHERE token_hash = ? AND person_id = ?").run(sessionTag(hash, env), keep, account.id);
    }
    rotatePreviewKey();
    return true;
  });
}

// Sessions. Each carries a tag of the password it was made with, so a new password ends it. A person's
// tag takes in CMS_PASSWORD_HASH too, so a new one from the command line still signs out everyone.
// The owner's tag is the bare hash, as it always was, so sessions from before people existed carry on.

const sessionTag = (hash: string, env?: string) => (env === undefined ? sha256(hash) : sha256(`neutron-cms person\n${env}\n${hash}`));

/** `hash` is the password hash just checked: the owner's, or with `person`, that person's. */
export function createSession(hash: string, person?: number): string {
  const token = randomBytes(32).toString("base64url");
  const expires = new Date(Date.now() + SESSION_DAYS * 86_400_000).toISOString();
  const tag = person === undefined ? sessionTag(hash) : sessionTag(hash, passwordHash() ?? "");
  db().prepare("DELETE FROM sessions WHERE expires_at < ?").run(new Date().toISOString());
  db().prepare("INSERT INTO sessions (token_hash, password_tag, expires_at, person_id) VALUES (?, ?, ?, ?)").run(sha256(token), tag, expires, person ?? null);
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

/**
 * Who is logged in, and the session that says so. A browser can send several cookies of this name;
 * the first valid one counts. Nothing is logged in while CMS_PASSWORD_HASH is unset.
 */
export function sessionOf(request: Request): { account: Account; session: string } | null {
  const env = passwordHash();
  const tokens = cookieValues(request);
  if (!env || tokens.length === 0) return null;
  const owner = ownerHash()!;
  const find = db().prepare(
    `SELECT sessions.person_id, sessions.password_tag, people.name, people.password_hash FROM sessions
     LEFT JOIN people ON people.id = sessions.person_id WHERE sessions.token_hash = ? AND sessions.expires_at > ?`,
  );
  const now = new Date().toISOString();
  for (const token of tokens) {
    const session = sha256(token);
    const row = find.get(session, now) as { person_id: number | null; password_tag: string; name: string | null; password_hash: string | null } | undefined;
    if (!row) continue;
    if (row.person_id === null) {
      if (row.password_tag === sessionTag(owner)) return { account: { kind: "owner" }, session };
    } else if (row.password_hash !== null && row.name !== null && row.password_tag === sessionTag(row.password_hash, env)) {
      return { account: { kind: "person", id: row.person_id, name: row.name }, session };
    }
  }
  return null;
}

export const hasSession = (request: Request): boolean => sessionOf(request) !== null;

// Drafts of designed pages open in the Edit site frame, on the site's own address, where the
// session cookie (scoped to the admin) never reaches. A short-lived signed key in the address does.
// The key comes from CMS_PASSWORD_HASH and a random value kept in the database: every instance
// agrees and it survives a deploy, a stolen database alone can't make one, and a new password from
// the command line ends every link. The random value is replaced whenever a password changes or a
// person is removed or given a new password, which ends every link made before.
export type PreviewScope = "page" | "design";
const PREVIEW_HOURS = 2;

function previewSecret(): string {
  const read = () => (db().prepare("SELECT value FROM auth_keys WHERE name = 'preview'").get() as { value: string } | undefined)?.value;
  const found = read();
  if (found) return found;
  db().prepare("INSERT INTO auth_keys (name, value) VALUES ('preview', ?) ON CONFLICT (name) DO NOTHING").run(randomBytes(32).toString("base64url"));
  return read()!;
}

function rotatePreviewKey(): void {
  db().prepare("INSERT INTO auth_keys (name, value) VALUES ('preview', ?) ON CONFLICT (name) DO UPDATE SET value = excluded.value").run(randomBytes(32).toString("base64url"));
}

const previewMac = (scope: PreviewScope, name: string, expires: string) =>
  createHmac("sha256", sha256(`neutron-cms preview\n${passwordHash()}\n${previewSecret()}`)).update(`${scope}\n${name}\n${expires}`).digest("base64url");

export function previewToken(scope: PreviewScope, name: string): string {
  const expires = String(Date.now() + PREVIEW_HOURS * 3_600_000);
  return `${expires}.${previewMac(scope, name, expires)}`;
}

export function isPreview(scope: PreviewScope, name: string, token: string | null): boolean {
  const [expires, mac] = (token ?? "").split(".");
  if (!passwordHash() || !expires || !mac || !(Number(expires) > Date.now())) return false;
  const expected = Buffer.from(previewMac(scope, name, expires));
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
