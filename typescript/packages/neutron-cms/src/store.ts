import { existsSync, mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";

export type Kind = "post" | "page";

export interface Post {
  id: number;
  slug: string | null;
  title: string;
  description: string;
  label: string;
  kind: Kind;
  /** A design's key when the page has its own design, otherwise empty. */
  layout: string;
  body: string;
  published_at: string | null;
  updated_at: string;
}

export type Item = Record<string, string>;
export type PostFields = Pick<Post, "title" | "description" | "body" | "slug" | "published_at"> & { label?: string; kind?: Kind };
export type SaveResult =
  | { ok: true; post: Post }
  | { ok: false; error: "stale" | "slug-taken" | "slug-reserved" | "missing" };

// Additive only: during a deploy the previous release still runs against this file.
const MIGRATIONS = [
  `CREATE TABLE posts (
     id INTEGER PRIMARY KEY,
     slug TEXT UNIQUE,
     title TEXT NOT NULL DEFAULT '',
     description TEXT NOT NULL DEFAULT '',
     body TEXT NOT NULL DEFAULT '',
     published_at TEXT,
     updated_at TEXT NOT NULL
   );
   CREATE INDEX posts_published ON posts(published_at);
   CREATE TABLE items (
     id INTEGER PRIMARY KEY,
     list TEXT NOT NULL,
     position INTEGER NOT NULL,
     data TEXT NOT NULL
   );
   CREATE INDEX items_list ON items(list, position);
   CREATE TABLE texts (key TEXT PRIMARY KEY, value TEXT NOT NULL);
   CREATE TABLE sessions (
     token_hash TEXT PRIMARY KEY,
     password_tag TEXT NOT NULL,
     expires_at TEXT NOT NULL
   );
   CREATE TABLE posts_old (
     id INTEGER PRIMARY KEY,
     post_id INTEGER NOT NULL,
     title TEXT NOT NULL,
     body TEXT NOT NULL,
     saved_at TEXT NOT NULL
   );
   CREATE TRIGGER posts_keep_update AFTER UPDATE OF body ON posts WHEN old.body <> new.body BEGIN
     INSERT INTO posts_old (post_id, title, body, saved_at) VALUES (old.id, old.title, old.body, old.updated_at);
     DELETE FROM posts_old WHERE post_id = old.id AND id NOT IN
       (SELECT id FROM posts_old WHERE post_id = old.id ORDER BY id DESC LIMIT 50);
   END;
   CREATE TRIGGER posts_keep_delete BEFORE DELETE ON posts BEGIN
     INSERT INTO posts_old (post_id, title, body, saved_at) VALUES (old.id, old.title, old.body, old.updated_at);
   END;`,
  `ALTER TABLE posts ADD COLUMN label TEXT NOT NULL DEFAULT ''`,
  `ALTER TABLE posts ADD COLUMN kind TEXT NOT NULL DEFAULT 'post'`,
  `ALTER TABLE posts ADD COLUMN layout TEXT NOT NULL DEFAULT ''`,
];

const store = globalThis as typeof globalThis & { __cmsDb?: DatabaseSync };

export function dataDir(): string {
  const configured = process.env.CMS_DATA_DIR;
  if (configured) return resolve(configured);
  if (process.env.NODE_ENV === "production") throw new Error("CMS_DATA_DIR is not set");
  return resolve("data");
}

export const dbPath = () => join(dataDir(), "site.db");

/** Production only opens a database that already exists, so a lost volume fails loudly. */
export function db(options: { create?: boolean } = {}): DatabaseSync {
  if (store.__cmsDb) return store.__cmsDb;
  const path = dbPath();
  // A configured folder is normally a mounted volume, and an empty one usually means it wasn't mounted.
  const mayCreate = options.create || (process.env.NODE_ENV !== "production" && !process.env.CMS_DATA_DIR);
  if (!existsSync(path)) {
    if (!mayCreate) throw new Error(`No database at ${path}. Is the volume mounted?`);
    mkdirSync(dataDir(), { recursive: true });
  }
  const database = new DatabaseSync(path, { timeout: 5000 });
  database.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;");
  migrate(database);
  store.__cmsDb = database;
  return database;
}

export function closeDb(): void {
  store.__cmsDb?.close();
  store.__cmsDb = undefined;
}

function migrate(database: DatabaseSync): void {
  const version = () => (database.prepare("PRAGMA user_version").get() as { user_version: number }).user_version;
  if (version() >= MIGRATIONS.length) return;
  database.exec("BEGIN IMMEDIATE");
  try {
    for (let next = version(); next < MIGRATIONS.length; next++) {
      database.exec(MIGRATIONS[next]);
      database.exec(`PRAGMA user_version = ${next + 1}`);
    }
    database.exec("COMMIT");
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  }
}

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

const now = () => new Date().toISOString();

// Posts

export const isLive = (post: Pick<Post, "published_at">, at = now()) =>
  post.published_at !== null && post.published_at <= at;

export const allPosts = (kind: Kind = "post") =>
  db().prepare("SELECT * FROM posts WHERE kind = ? ORDER BY published_at IS NOT NULL, published_at DESC, updated_at DESC").all(kind) as unknown as Post[];

export const livePosts = (kind: Kind = "post") =>
  db().prepare("SELECT * FROM posts WHERE kind = ? AND published_at <= ? ORDER BY published_at DESC").all(kind, now()) as unknown as Post[];

export const getPost = (id: number) =>
  db().prepare("SELECT * FROM posts WHERE id = ?").get(id) as Post | undefined;

export const livePostBySlug = (slug: string, kind: Kind = "post") =>
  db().prepare("SELECT * FROM posts WHERE slug = ? AND kind = ? AND published_at <= ?").get(slug, kind, now()) as Post | undefined;

export const designPage = (key: string) =>
  db().prepare("SELECT * FROM posts WHERE kind = 'page' AND layout = ?").get(key) as Post | undefined;

/** A page or post at this address, published or not. */
export const postBySlug = (slug: string, kind: Kind = "post") =>
  db().prepare("SELECT * FROM posts WHERE slug = ? AND kind = ?").get(slug, kind) as Post | undefined;

/**
 * Every design has a page: a draft is made for one that has none. A page already at the design's
 * address, such as one imported from an older site, becomes the design's page and keeps its state.
 */
export function ensureDesigns(given: Record<string, string | { title: string; published?: boolean; permanent?: boolean }>): void {
  const designs = Object.fromEntries(Object.entries(given).map(([key, value]) => [key, typeof value === "string" ? { title: value } : value])) as Record<string, { title: string; published?: boolean; permanent?: boolean }>;
  const keys = Object.keys(designs);
  if (!keys.length) return;
  const byLayout = db().prepare("SELECT id FROM posts WHERE kind = 'page' AND layout = ?");
  transaction(() => {
    for (const key of keys) {
      if (designs[key].permanent) db().prepare("UPDATE posts SET published_at = ? WHERE kind = 'page' AND layout = ? AND (published_at IS NULL OR published_at > ?)").run(now(), key, now());
      if (byLayout.get(key)) continue;
      const atAddress = postBySlug(key, "page") ?? (db().prepare("SELECT * FROM posts WHERE slug = ?").get(key) as Post | undefined);
      const live = designs[key].published || designs[key].permanent ? now() : null;
      if (atAddress?.kind === "page") db().prepare("UPDATE posts SET layout = ? WHERE id = ?").run(key, atAddress.id);
      else db().prepare("INSERT INTO posts (slug, title, kind, layout, published_at, updated_at) VALUES (?, ?, 'page', ?, ?, ?)").run(atAddress ? null : key, designs[key].title, key, live, now());
    }
  });
}

export function createPost(fields: Partial<PostFields> & { updated_at?: string } = {}): number {
  const result = db()
    .prepare("INSERT INTO posts (slug, title, description, label, body, published_at, updated_at, kind) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
    .run(fields.slug ?? null, fields.title ?? "", fields.description ?? "", fields.label ?? "", fields.body ?? "", fields.published_at ?? null, fields.updated_at ?? now(), fields.kind ?? "post");
  return Number(result.lastInsertRowid);
}

export function slugify(title: string): string {
  return title
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80)
    .replace(/-+$/, "");
}

/**
 * Saves only when `revision` still matches, so a stale tab cannot overwrite newer text.
 * A live post keeps its slug, because changing it would break every link to it.
 */
export function savePost(id: number, fields: PostFields, revision: string, reservedSlugs: string[] = []): SaveResult {
  return transaction<SaveResult>(() => {
    const current = getPost(id);
    if (!current) return { ok: false, error: "missing" };
    if (current.updated_at !== revision) return { ok: false, error: "stale" };

    let slug = isLive(current) ? current.slug : slugify(fields.slug ?? "") || null;
    if (!slug && fields.published_at) slug = slugify(fields.title) || `post-${id}`;
    if (slug && reservedSlugs.includes(slug)) return { ok: false, error: "slug-reserved" };
    if (slug && db().prepare("SELECT 1 FROM posts WHERE slug = ? AND id <> ?").get(slug, id)) {
      return { ok: false, error: "slug-taken" };
    }

    db()
      .prepare("UPDATE posts SET slug = ?, title = ?, description = ?, label = ?, body = ?, published_at = ?, updated_at = ? WHERE id = ?")
      .run(slug, fields.title, fields.description, fields.label ?? current.label, fields.body.replace(/\r\n?/g, "\n"), fields.published_at, now(), id);
    return { ok: true, post: getPost(id)! };
  });
}

export const deletePost = (id: number) => {
  db().prepare("DELETE FROM posts WHERE id = ?").run(id);
};

// Lists

export const listItems = (list: string): Item[] =>
  (db().prepare("SELECT data FROM items WHERE list = ? ORDER BY position").all(list) as { data: string }[]).map(
    (row) => JSON.parse(row.data) as Item,
  );

export const listRevision = (list: string) =>
  createHash("sha256").update(JSON.stringify(listItems(list))).digest("hex").slice(0, 16);

/** Replaces a whole list at once, which is what makes reordering free. */
export function replaceItems(list: string, items: Item[], revision?: string): { ok: boolean } {
  return transaction(() => {
    if (revision !== undefined && revision !== listRevision(list)) return { ok: false };
    db().prepare("DELETE FROM items WHERE list = ?").run(list);
    const insert = db().prepare("INSERT INTO items (list, position, data) VALUES (?, ?, ?)");
    items.forEach((item, position) => insert.run(list, position, JSON.stringify(item)));
    return { ok: true };
  });
}

/** Saves one page's edits together. If any list changed elsewhere, nothing is written. */
export function saveEdits(texts: Record<string, string>, lists: Record<string, { revision: string; items: Item[] }>): { ok: true } | { ok: false; stale: string } {
  return transaction(() => {
    for (const [key, list] of Object.entries(lists)) {
      if (list.revision !== listRevision(key)) return { ok: false, stale: key };
    }
    const upsert = db().prepare("INSERT INTO texts (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value");
    for (const [key, value] of Object.entries(texts)) upsert.run(key, value);
    const insert = db().prepare("INSERT INTO items (list, position, data) VALUES (?, ?, ?)");
    for (const [key, list] of Object.entries(lists)) {
      db().prepare("DELETE FROM items WHERE list = ?").run(key);
      list.items.forEach((item, position) => insert.run(key, position, JSON.stringify(item)));
    }
    return { ok: true };
  });
}

/** Dated items in order, undated ones last. */
export const sortByDate = (items: Item[], field: string) =>
  [...items].sort((a, b) => (a[field] || "9999").localeCompare(b[field] || "9999"));

export const today = (timeZone = "UTC") => new Date().toLocaleDateString("en-CA", { timeZone });

/** Keeps an event up through its own day. Past items stay in the admin. */
export const upcoming = (items: Item[], field = "date", from = today()) =>
  items.filter((item) => !item[field] || item[field] >= from);

// Texts

export function allTexts(): Record<string, string> {
  const rows = db().prepare("SELECT key, value FROM texts").all() as { key: string; value: string }[];
  return Object.fromEntries(rows.map((row) => [row.key, row.value]));
}

export function setTexts(values: Record<string, string>): void {
  transaction(() => {
    const upsert = db().prepare("INSERT INTO texts (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value");
    for (const [key, value] of Object.entries(values)) upsert.run(key, value);
  });
}

// Health and backup

export function contentCount(): number {
  const row = db().prepare("SELECT (SELECT count(*) FROM posts) + (SELECT count(*) FROM items) AS n").get() as { n: number };
  return row.n;
}

/** A consistent single-file copy. Copying a live WAL database by hand is not one. */
export function backupTo(path: string): void {
  db().prepare("VACUUM INTO ?").run(path);
}
