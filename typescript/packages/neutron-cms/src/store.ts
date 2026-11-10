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
  // The whole post is kept, so it can come back as it was. Old versions go along under the negative of its trash id.
  // A draft autosaves as it's typed, so it keeps a version every ten minutes at most; a published post keeps every update.
  // Versions of posts deleted before there was a Trash move to post 0, so a new post can't inherit them through a reused id.
  `CREATE TABLE posts_trash (
     id INTEGER PRIMARY KEY,
     slug TEXT,
     title TEXT NOT NULL,
     description TEXT NOT NULL,
     label TEXT NOT NULL,
     kind TEXT NOT NULL,
     layout TEXT NOT NULL,
     body TEXT NOT NULL,
     published_at TEXT,
     updated_at TEXT NOT NULL,
     deleted_at TEXT NOT NULL
   );
   DROP TRIGGER posts_keep_update;
   CREATE TRIGGER posts_keep_update AFTER UPDATE ON posts
   WHEN (old.body <> new.body OR old.title <> new.title)
     AND (old.published_at IS NOT NULL OR NOT EXISTS (
       SELECT 1 FROM posts_old WHERE post_id = old.id AND saved_at > strftime('%Y-%m-%dT%H:%M:%fZ', old.updated_at, '-10 minutes')))
   BEGIN
     INSERT INTO posts_old (post_id, title, body, saved_at) VALUES (old.id, old.title, old.body, old.updated_at);
     DELETE FROM posts_old WHERE post_id = old.id AND id NOT IN
       (SELECT id FROM posts_old WHERE post_id = old.id ORDER BY id DESC LIMIT 50);
   END;
   UPDATE posts_old SET post_id = 0 WHERE post_id NOT IN (SELECT id FROM posts);`,
  // Each save of lists and texts keeps what it replaced, as JSON: { texts: { key: old or null }, lists: { key: [items] } }.
  `CREATE TABLE saves (
     id INTEGER PRIMARY KEY,
     saved_at TEXT NOT NULL,
     before TEXT NOT NULL
   );`,
  // People who log in with their own name and password. Names are stored trimmed and lower case, so
  // they are unique however they are typed. AUTOINCREMENT: a removed person's id is never reused.
  // A session with no person_id is the owner's, as every session made before this was.
  // owner_password is a password the owner set in the admin, valid only while CMS_PASSWORD_HASH is
  // the one it was set against (env_tag). auth_keys holds the random part of the preview key.
  `CREATE TABLE people (
     id INTEGER PRIMARY KEY AUTOINCREMENT,
     name TEXT NOT NULL UNIQUE,
     password_hash TEXT NOT NULL,
     created_at TEXT NOT NULL
   );
   ALTER TABLE sessions ADD COLUMN person_id INTEGER;
   CREATE INDEX sessions_person ON sessions(person_id);
   CREATE TABLE owner_password (
     id INTEGER PRIMARY KEY CHECK (id = 1),
     env_tag TEXT NOT NULL,
     password_hash TEXT NOT NULL,
     set_at TEXT NOT NULL
   );
   CREATE TABLE auth_keys (name TEXT PRIMARY KEY, value TEXT NOT NULL);`,
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

/** Posts or pages whose title or text contains `query`, in the order of allPosts, without their text. */
export function findPosts(kind: Kind, query = ""): Omit<Post, "body">[] {
  const columns = "id, slug, title, description, label, kind, layout, published_at, updated_at";
  const order = "ORDER BY published_at IS NOT NULL, published_at DESC, updated_at DESC";
  if (!query) return db().prepare(`SELECT ${columns} FROM posts WHERE kind = ? ${order}`).all(kind) as unknown as Omit<Post, "body">[];
  // Typed % and _ are looked for as themselves.
  const like = `%${query.replace(/[\\%_]/g, "\\$&")}%`;
  return db()
    .prepare(`SELECT ${columns} FROM posts WHERE kind = ? AND (title LIKE ? ESCAPE '\\' OR body LIKE ? ESCAPE '\\') ${order}`)
    .all(kind, like, like) as unknown as Omit<Post, "body">[];
}

export const postCount = (kind: Kind = "post") => (db().prepare("SELECT count(*) AS n FROM posts WHERE kind = ?").get(kind) as { n: number }).n;

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
 * Every design has a page: a draft is made for one that has none (published, for one that starts so).
 * A page already at the design's address, such as one imported from an older site, becomes the
 * design's page and keeps its state. A permanent design's page is always published. Writes only
 * when something is missing, so reads stay reads.
 */
export function ensureDesigns(given: Record<string, string | { title: string; published?: boolean; permanent?: boolean }>): void {
  const designs = Object.fromEntries(Object.entries(given).map(([key, value]) => [key, typeof value === "string" ? { title: value } : value])) as Record<string, { title: string; published?: boolean; permanent?: boolean }>;
  const byLayout = db().prepare("SELECT * FROM posts WHERE kind = 'page' AND layout = ?");
  const needs = Object.entries(designs).filter(([key, design]) => {
    const page = byLayout.get(key) as unknown as Post | undefined;
    return !page || (design.permanent && !isLive(page));
  });
  if (!needs.length) return;
  transaction(() => {
    for (const [key, design] of needs) {
      let page = byLayout.get(key) as unknown as Post | undefined;
      if (!page) {
        const atAddress = db().prepare("SELECT * FROM posts WHERE slug = ?").get(key) as Post | undefined;
        if (atAddress?.kind === "page") {
          db().prepare("UPDATE posts SET layout = ? WHERE id = ?").run(key, atAddress.id);
        } else {
          // A post at this address keeps it; the design's page has none until the post moves.
          const live = design.published || design.permanent ? now() : null;
          db().prepare("INSERT INTO posts (slug, title, kind, layout, published_at, updated_at) VALUES (?, ?, 'page', ?, ?, ?)").run(atAddress ? null : key, design.title, key, live, now());
        }
        page = byLayout.get(key) as unknown as Post;
      }
      if (design.permanent && !isLive(page)) db().prepare("UPDATE posts SET published_at = ? WHERE id = ?").run(now(), page.id);
    }
  });
}

/** "New post" makes a draft at once; one left empty for an hour was never wanted. */
export function removeAbandonedDrafts(): void {
  const hourAgo = new Date(Date.now() - 3_600_000).toISOString();
  db().prepare("DELETE FROM posts WHERE published_at IS NULL AND layout = '' AND title = '' AND body = '' AND description = '' AND updated_at < ?").run(hourAgo);
  db().exec("DELETE FROM posts_old WHERE title = '' AND body = '' AND post_id NOT IN (SELECT id FROM posts)");
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
/** Now, or a moment later than `previous`: a revision must change on every save, even two in the same millisecond. */
function after(previous: string): string {
  const stamp = now();
  return stamp > previous ? stamp : new Date(Date.parse(previous) + 1).toISOString();
}

export function savePost(id: number, fields: PostFields, revision: string, reservedSlugs: string[] = []): SaveResult {
  return transaction<SaveResult>(() => {
    const current = getPost(id);
    if (!current) return { ok: false, error: "missing" };
    if (current.updated_at !== revision) return { ok: false, error: "stale" };

    const taken = (candidate: string) =>
      reservedSlugs.includes(candidate) || Boolean(db().prepare("SELECT 1 FROM posts WHERE slug = ? AND id <> ?").get(candidate, id));
    let slug = isLive(current) ? current.slug : slugify(fields.slug ?? "") || null;
    if (!slug && fields.published_at) {
      // An address made from the title steps aside, as WordPress does: "notes", then "notes-2".
      const base = slugify(fields.title) || `post-${id}`;
      slug = base;
      for (let n = 2; taken(slug); n++) slug = `${base}-${n}`;
    }
    // One chosen by hand is never changed behind the owner's back.
    if (slug && reservedSlugs.includes(slug)) return { ok: false, error: "slug-reserved" };
    if (slug && taken(slug)) return { ok: false, error: "slug-taken" };

    db()
      .prepare("UPDATE posts SET slug = ?, title = ?, description = ?, label = ?, body = ?, published_at = ?, updated_at = ? WHERE id = ?")
      .run(slug, fields.title, fields.description, fields.label ?? current.label, fields.body.replace(/\r\n?/g, "\n"), fields.published_at, after(current.updated_at), id);
    return { ok: true, post: getPost(id)! };
  });
}

// Versions

export interface Version {
  id: number;
  title: string;
  body: string;
  saved_at: string;
}

/** Earlier versions, newest first. A draft's first, empty state and anything identical to the post now are not worth showing. */
export function versionsOf(post: Pick<Post, "id" | "title" | "body">): Version[] {
  const rows = db().prepare("SELECT id, title, body, saved_at FROM posts_old WHERE post_id = ? AND (title <> '' OR body <> '') ORDER BY id DESC").all(post.id) as unknown as Version[];
  return rows.filter((row) => row.title !== post.title || row.body !== post.body);
}

export type RestoreResult = { ok: true } | { ok: false; error: "stale" | "missing" };

/** Puts an old version's title and text back. What it replaces becomes a version, so this can be undone too. */
export function restoreVersion(id: number, versionId: number, revision: string): RestoreResult {
  return transaction<RestoreResult>(() => {
    const current = getPost(id);
    const version = db().prepare("SELECT title, body FROM posts_old WHERE id = ? AND post_id = ?").get(versionId, id) as Pick<Version, "title" | "body"> | undefined;
    if (!current || !version) return { ok: false, error: "missing" };
    if (current.updated_at !== revision) return { ok: false, error: "stale" };
    // Published posts always keep a version; a draft might be inside its ten minutes, and this one must be kept.
    if (current.published_at === null) db().prepare("INSERT INTO posts_old (post_id, title, body, saved_at) VALUES (?, ?, ?, ?)").run(id, current.title, current.body, current.updated_at);
    db().prepare("UPDATE posts SET title = ?, body = ?, updated_at = ? WHERE id = ?").run(version.title, version.body, after(current.updated_at), id);
    return { ok: true };
  });
}

// Trash

export interface Trashed {
  id: number;
  title: string;
  kind: Kind;
  deleted_at: string;
}

const TRASH_DAYS = 30;

/** Takes a post off the site and into the Trash, with its address and dates. Returns its place there. */
export function trashPost(id: number): number | null {
  return transaction(() => {
    if (!getPost(id)) return null;
    const trashed = db().prepare(
      `INSERT INTO posts_trash (slug, title, description, label, kind, layout, body, published_at, updated_at, deleted_at)
       SELECT slug, title, description, label, kind, layout, body, published_at, updated_at, ? FROM posts WHERE id = ?`,
    ).run(now(), id);
    const trashId = Number(trashed.lastInsertRowid);
    db().prepare("DELETE FROM posts WHERE id = ?").run(id);
    // The delete trigger's copy would only repeat what the Trash now holds.
    db().prepare("DELETE FROM posts_old WHERE id = (SELECT max(id) FROM posts_old WHERE post_id = ?)").run(id);
    db().prepare("UPDATE posts_old SET post_id = ? WHERE post_id = ?").run(-trashId, id);
    return trashId;
  });
}

export const trashed = () =>
  db().prepare("SELECT id, title, kind, deleted_at FROM posts_trash ORDER BY deleted_at DESC, id DESC").all() as unknown as Trashed[];

export const inTrash = (id: number) =>
  db().prepare("SELECT id, title, kind, deleted_at FROM posts_trash WHERE id = ?").get(id) as Trashed | undefined;

export const trashCount = () => (db().prepare("SELECT count(*) AS n FROM posts_trash").get() as { n: number }).n;

/**
 * Brings a post back as it was. If something else has taken its address or the site now reserves it,
 * it comes back as a draft with no address, because publishing it there would break the other page.
 */
export function restoreFromTrash(trashId: number, reservedSlugs: string[] = []): { id: number; addressLost: boolean } | null {
  return transaction(() => {
    const row = db().prepare("SELECT * FROM posts_trash WHERE id = ?").get(trashId) as (Post & { deleted_at: string }) | undefined;
    if (!row) return null;
    const addressLost = row.slug !== null && (reservedSlugs.includes(row.slug) || Boolean(db().prepare("SELECT 1 FROM posts WHERE slug = ?").get(row.slug)));
    const restored = db().prepare("INSERT INTO posts (slug, title, description, label, kind, layout, body, published_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").run(
      addressLost ? null : row.slug, row.title, row.description, row.label, row.kind, row.layout, row.body, addressLost ? null : row.published_at, row.updated_at,
    );
    const id = Number(restored.lastInsertRowid);
    db().prepare("UPDATE posts_old SET post_id = ? WHERE post_id = ?").run(id, -trashId);
    db().prepare("DELETE FROM posts_trash WHERE id = ?").run(trashId);
    return { id, addressLost };
  });
}

export function deleteForever(trashId: number): void {
  transaction(() => {
    db().prepare("DELETE FROM posts_old WHERE post_id = ?").run(-trashId);
    db().prepare("DELETE FROM posts_trash WHERE id = ?").run(trashId);
  });
}

export function emptyOldTrash(): void {
  const cutoff = new Date(Date.now() - TRASH_DAYS * 86_400_000).toISOString();
  for (const { id } of db().prepare("SELECT id FROM posts_trash WHERE deleted_at < ?").all(cutoff) as { id: number }[]) deleteForever(id);
}

// Lists

export const listItems = (list: string): Item[] =>
  (db().prepare("SELECT data FROM items WHERE list = ? ORDER BY position").all(list) as { data: string }[]).map(
    (row) => JSON.parse(row.data) as Item,
  );

export const listRevision = (list: string) =>
  createHash("sha256").update(JSON.stringify(listItems(list))).digest("hex").slice(0, 16);

function writeList(list: string, items: Item[]): void {
  db().prepare("DELETE FROM items WHERE list = ?").run(list);
  const insert = db().prepare("INSERT INTO items (list, position, data) VALUES (?, ?, ?)");
  items.forEach((item, position) => insert.run(list, position, JSON.stringify(item)));
}

/** Replaces a whole list at once, which is what makes reordering free. */
export function replaceItems(list: string, items: Item[], revision?: string): { ok: boolean } {
  return transaction(() => {
    if (revision !== undefined && revision !== listRevision(list)) return { ok: false };
    keepBefore({}, { [list]: items });
    writeList(list, items);
    return { ok: true };
  });
}

/** Saves one page's edits together. If any list changed elsewhere, nothing is written. */
export function saveEdits(texts: Record<string, string>, lists: Record<string, { revision: string; items: Item[] }>): { ok: true } | { ok: false; stale: string } {
  return transaction(() => {
    for (const [key, list] of Object.entries(lists)) {
      if (list.revision !== listRevision(key)) return { ok: false, stale: key };
    }
    keepBefore(texts, Object.fromEntries(Object.entries(lists).map(([key, list]) => [key, list.items])));
    for (const [key, value] of Object.entries(texts)) writeText(key, value);
    for (const [key, list] of Object.entries(lists)) writeList(key, list.items);
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

const textNow = (key: string) =>
  (db().prepare("SELECT value FROM texts WHERE key = ?").get(key) as { value: string } | undefined)?.value ?? null;

/** A text that was never saved reads as empty, so null takes it back to never saved. */
function writeText(key: string, value: string | null): void {
  if (value === null) db().prepare("DELETE FROM texts WHERE key = ?").run(key);
  else db().prepare("INSERT INTO texts (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value").run(key, value);
}

export function setTexts(values: Record<string, string>): void {
  transaction(() => {
    keepBefore(values, {});
    for (const [key, value] of Object.entries(values)) writeText(key, value);
  });
}

// History of lists and texts

export interface Snapshot {
  /** null: the text had never been saved. */
  texts: Record<string, string | null>;
  lists: Record<string, Item[]>;
}

export interface Save {
  id: number;
  saved_at: string;
  /** What this save replaced, only for the keys it changed. */
  before: Snapshot;
}

const SAVES_KEPT = 50;

/**
 * Keeps what a save is about to replace, for the keys it actually changes. Runs inside the save's
 * transaction, before it writes. Returns the new save's id, or null when nothing changes.
 */
function keepBefore(texts: Record<string, string | null>, lists: Record<string, Item[]>): number | null {
  const before: Snapshot = { texts: {}, lists: {} };
  for (const [key, value] of Object.entries(texts)) {
    const old = textNow(key);
    if ((old ?? "") !== (value ?? "")) before.texts[key] = old;
  }
  for (const [key, items] of Object.entries(lists)) {
    const old = listItems(key);
    if (JSON.stringify(old) !== JSON.stringify(items)) before.lists[key] = old;
  }
  if (!Object.keys(before.texts).length && !Object.keys(before.lists).length) return null;
  const result = db().prepare("INSERT INTO saves (saved_at, before) VALUES (?, ?)").run(now(), JSON.stringify(before));
  db().prepare("DELETE FROM saves WHERE id NOT IN (SELECT id FROM saves ORDER BY id DESC LIMIT ?)").run(SAVES_KEPT);
  return Number(result.lastInsertRowid);
}

function parseSave(row: { id: number; saved_at: string; before: string }): Save {
  const parsed = JSON.parse(row.before) as Partial<Snapshot>;
  return { id: row.id, saved_at: row.saved_at, before: { texts: parsed.texts ?? {}, lists: parsed.lists ?? {} } };
}

/** Saves of lists and texts, newest first. */
export const saves = (): Save[] =>
  (db().prepare("SELECT id, saved_at, before FROM saves ORDER BY id DESC").all() as { id: number; saved_at: string; before: string }[]).map(parseSave);

export const saveCount = () => (db().prepare("SELECT count(*) AS n FROM saves").get() as { n: number }).n;

/** Today's value of every key a snapshot names. */
export function valuesNow(keys: { texts: string[]; lists: string[] }): Snapshot {
  return {
    texts: Object.fromEntries(keys.texts.map((key) => [key, textNow(key)])),
    lists: Object.fromEntries(keys.lists.map((key) => [key, listItems(key)])),
  };
}

/**
 * Puts back what a save replaced, for the keys the site still declares. What it replaces is kept
 * as a new save first, so a restore can be undone too. Returns that save's id, null when nothing changed.
 */
export function restoreSave(id: number, declared: { texts: string[]; lists: string[] }): { ok: true; save: number | null } | { ok: false } {
  return transaction(() => {
    const row = db().prepare("SELECT id, saved_at, before FROM saves WHERE id = ?").get(id) as { id: number; saved_at: string; before: string } | undefined;
    if (!row) return { ok: false as const };
    const { before } = parseSave(row);
    const texts = Object.fromEntries(Object.entries(before.texts).filter(([key]) => declared.texts.includes(key)));
    const lists = Object.fromEntries(Object.entries(before.lists).filter(([key]) => declared.lists.includes(key)));
    const save = keepBefore(texts, lists);
    for (const [key, value] of Object.entries(texts)) writeText(key, value);
    for (const [key, items] of Object.entries(lists)) writeList(key, items);
    return { ok: true as const, save };
  });
}

// Where uploaded images are used

export interface ImageUse {
  posts: Map<number, Pick<Post, "id" | "title" | "kind">>;
  lists: Set<string>;
  texts: Set<string>;
  /** Only in what an undo could bring back. */
  kept: Set<"trash" | "versions" | "history">;
}

// An upload's file name, wherever it sits in an address. A thumbnail counts as its image.
const IMAGE_NAME = /(?<![a-f0-9])([a-f0-9]{32}(?:-\d{1,5}x\d{1,5})?)(?:-t)?\.(jpg|png|webp|gif)/g;

/** Every image named in the content, by file name: six queries, however many images there are. */
export function imageUses(): Map<string, ImageUse> {
  const uses = new Map<string, ImageUse>();
  const scan = (text: string, record: (use: ImageUse) => void) => {
    for (const match of text.matchAll(IMAGE_NAME)) {
      const name = `${match[1]}.${match[2]}`;
      let use = uses.get(name);
      if (!use) uses.set(name, (use = { posts: new Map(), lists: new Set(), texts: new Set(), kept: new Set() }));
      record(use);
    }
  };
  const rows = <T>(sql: string) => db().prepare(sql).all() as unknown as T[];
  for (const post of rows<Pick<Post, "id" | "title" | "kind" | "body" | "description">>("SELECT id, title, kind, body, description FROM posts")) {
    scan(`${post.body}\n${post.description}`, (use) => use.posts.set(post.id, { id: post.id, title: post.title, kind: post.kind }));
  }
  for (const item of rows<{ list: string; data: string }>("SELECT list, data FROM items")) scan(item.data, (use) => use.lists.add(item.list));
  for (const text of rows<{ key: string; value: string }>("SELECT key, value FROM texts")) scan(text.value, (use) => use.texts.add(text.key));
  for (const row of rows<{ body: string; description: string }>("SELECT body, description FROM posts_trash")) scan(`${row.body}\n${row.description}`, (use) => use.kept.add("trash"));
  for (const row of rows<{ body: string }>("SELECT body FROM posts_old")) scan(row.body, (use) => use.kept.add("versions"));
  for (const row of rows<{ before: string }>("SELECT before FROM saves")) scan(row.before, (use) => use.kept.add("history"));
  return uses;
}

export const inCurrentUse = (use: ImageUse | undefined) => Boolean(use && (use.posts.size || use.lists.size || use.texts.size));

// Health and backup

export function contentCount(): number {
  const row = db().prepare("SELECT (SELECT count(*) FROM posts) + (SELECT count(*) FROM items) AS n").get() as { n: number };
  return row.n;
}

/** A consistent single-file copy. Copying a live WAL database by hand is not one. */
export function backupTo(path: string): void {
  db().prepare("VACUUM INTO ?").run(path);
}
