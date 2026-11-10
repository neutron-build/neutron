import assert from "node:assert/strict";
import { mkdtempSync, rmSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, beforeEach, test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import {
  clearAttempts, clientAddress, createSession, endSession, hasSession, hashPassword,
  readLimited, sessionCookie, takeAttempt, verifyPassword,
} from "./auth.js";
import {
  allPosts, backupTo, closeDb, contentCount, createPost, db, trashPost, getPost, isLive, listItems,
  listRevision, livePostBySlug, livePosts, replaceItems, restoreSave, saveEdits, savePost, saves, setTexts, slugify, sortByDate, upcoming,
} from "./store.js";
import { isPhotoList } from "./config.js";
import { imageSize, photoAttributes, thumbUrl } from "./images.js";
import { saveImage, serveImage, sniffImage } from "./media.js";
import { firstImage, renderMarkdown, summarize } from "./markdown.js";

let directory: string;
const iso = (offset: number) => new Date(Date.now() + offset).toISOString();
const fields = (over = {}) => ({ title: "Hello", description: "", body: "Text", slug: null, published_at: null, ...over });

before(() => {
  directory = mkdtempSync(join(tmpdir(), "cms-"));
  process.env.CMS_DATA_DIR = directory;
  db({ create: true });
});
beforeEach(() => {
  for (const table of ["posts", "items", "texts", "sessions", "posts_old", "saves"]) db().exec(`DELETE FROM ${table}`);
});
after(() => {
  closeDb();
  rmSync(directory, { recursive: true, force: true });
});

test("production refuses to create a database, so a lost volume fails loudly", () => {
  closeDb();
  const previous = { env: process.env.NODE_ENV, dir: process.env.CMS_DATA_DIR };
  process.env.NODE_ENV = "production";
  process.env.CMS_DATA_DIR = join(directory, "absent");
  assert.throws(() => db(), /Is the volume mounted/);
  delete process.env.CMS_DATA_DIR;
  assert.throws(() => db(), /CMS_DATA_DIR is not set/);
  process.env.NODE_ENV = previous.env;
  process.env.CMS_DATA_DIR = previous.dir;
});

test("published_at alone decides draft, scheduled and live", () => {
  createPost({ title: "draft", slug: "draft" });
  createPost({ title: "soon", slug: "soon", published_at: iso(60_000) });
  createPost({ title: "live", slug: "live", published_at: iso(-60_000) });
  assert.deepEqual(livePosts().map((post) => post.slug), ["live"]);
  assert.equal(livePostBySlug("draft"), undefined);
  assert.equal(livePostBySlug("soon"), undefined);
  assert.equal(livePostBySlug("live")?.title, "live");
  assert.equal(allPosts().length, 3);
});

test("a stale revision cannot overwrite newer text", () => {
  const id = createPost({ updated_at: iso(-1000) });
  const stale = getPost(id)!.updated_at;
  assert.equal(savePost(id, fields({ body: "new" }), stale).ok, true);
  assert.deepEqual(savePost(id, fields({ body: "old tab" }), stale), { ok: false, error: "stale" });
  assert.equal(getPost(id)!.body, "new");
});

test("slugs: derived on publish, frozen once live, reserved and taken refused", () => {
  const id = createPost();
  const rev = () => getPost(id)!.updated_at;
  assert.equal(slugify("Crème Brûlée: A Story!"), "creme-brulee-a-story");

  const draft = savePost(id, fields({ title: "A Title" }), rev());
  assert.equal(draft.ok && draft.post.slug, null);

  const published = savePost(id, fields({ title: "A Title", published_at: iso(-1000) }), rev());
  assert.equal(published.ok && published.post.slug, "a-title");

  const renamed = savePost(id, fields({ slug: "other", published_at: iso(-1000) }), rev());
  assert.equal(renamed.ok && renamed.post.slug, "a-title");

  const other = createPost();
  const otherRev = getPost(other)!.updated_at;
  assert.deepEqual(savePost(other, fields({ slug: "a-title" }), otherRev), { ok: false, error: "slug-taken" });
  assert.deepEqual(savePost(other, fields({ slug: "gpg" }), otherRev, ["gpg"]), { ok: false, error: "slug-reserved" });
});

test("a scheduled post can still change its slug", () => {
  const id = createPost({ slug: "first", published_at: iso(60_000) });
  const saved = savePost(id, fields({ slug: "second", published_at: iso(60_000) }), getPost(id)!.updated_at);
  assert.equal(saved.ok && saved.post.slug, "second");
});

test("old bodies stay recoverable, including after delete", () => {
  const id = createPost({ title: "T", body: "one", updated_at: iso(-5000) });
  savePost(id, fields({ title: "T", body: "two" }), getPost(id)!.updated_at);
  const trashId = trashPost(id)!;
  assert.equal(getPost(id), undefined);
  const old = db().prepare("SELECT body FROM posts_old WHERE post_id = ? ORDER BY id").all(-trashId) as { body: string }[];
  assert.deepEqual(old.map((row) => row.body), ["one"], "the last text is in the Trash, the earlier ones go with it");
  assert.equal((db().prepare("SELECT body FROM posts_trash WHERE id = ?").get(trashId) as { body: string }).body, "two");
});

test("old bodies are capped at 50 per post", () => {
  const id = createPost({ body: "0", published_at: iso(-200_000), updated_at: iso(-100_000) });
  for (let n = 1; n <= 60; n++) db().prepare("UPDATE posts SET body = ? WHERE id = ?").run(String(n), id);
  const row = db().prepare("SELECT count(*) AS n, min(body + 0) AS low FROM posts_old WHERE post_id = ?").get(id) as { n: number; low: number };
  assert.equal(row.n, 50);
  assert.equal(row.low, 10);
});

test("lists replace whole, keep order and refuse a stale revision", () => {
  replaceItems("sites", [{ label: "a" }, { label: "b" }]);
  const revision = listRevision("sites");
  assert.equal(replaceItems("sites", [{ label: "b" }, { label: "a" }], revision).ok, true);
  assert.deepEqual(listItems("sites").map((item) => item.label), ["b", "a"]);
  assert.equal(replaceItems("sites", [], revision).ok, false);
  assert.equal(listItems("sites").length, 2);
  assert.equal(contentCount(), 2);
});

test("backup is a complete, independent database", () => {
  createPost({ title: "kept", slug: "kept", published_at: iso(-1000) });
  const path = join(directory, "backup.db");
  backupTo(path);
  const copy = new DatabaseSync(path);
  assert.equal((copy.prepare("SELECT title FROM posts").get() as { title: string }).title, "kept");
  copy.close();
});

test("passwords verify and reject; the hash survives .env quoting", async () => {
  const hash = await hashPassword("correct horse");
  assert.doesNotMatch(hash, /[$\s'"]/);
  assert.equal(await verifyPassword("correct horse", hash), true);
  assert.equal(await verifyPassword("wrong", hash), false);
  assert.equal(await verifyPassword("x", "garbage"), false);
});

test("sessions expire with logout and with a password change", async () => {
  const hash = await hashPassword("one");
  process.env.CMS_PASSWORD_HASH = hash;
  const token = createSession(hash);
  const request = new Request("http://x/", { headers: { cookie: `a=b; n_session=${token}` } });
  assert.equal(hasSession(request), true);
  assert.equal(hasSession(new Request("http://x/", { headers: { cookie: "n_session=forged" } })), false);

  process.env.CMS_PASSWORD_HASH = await hashPassword("two");
  assert.equal(hasSession(request), false);
  process.env.CMS_PASSWORD_HASH = hash;
  assert.equal(hasSession(request), true);

  endSession(request);
  assert.equal(hasSession(request), false);
  assert.match(sessionCookie("t"), /HttpOnly; SameSite=Strict/);
  delete process.env.CMS_PASSWORD_HASH;
});

test("throttle allows five tries per client, then recovers", () => {
  const at = 1_000_000;
  for (let n = 0; n < 5; n++) assert.equal(takeAttempt("a", at), true);
  assert.equal(takeAttempt("a", at), false);
  assert.equal(takeAttempt("b", at), true);
  assert.equal(takeAttempt("a", at + 16 * 60_000), true);
  clearAttempts("a");
  clearAttempts("b");
});

test("client address is the proxy's last hop", () => {
  const request = new Request("http://x/", { headers: { "x-forwarded-for": "6.6.6.6, 1.2.3.4" } });
  assert.equal(clientAddress(request), "1.2.3.4");
  assert.equal(clientAddress(new Request("http://x/")), "unknown");
});

test("readLimited stops at the limit", async () => {
  const body = (size: number) => new Request("http://x/", { method: "POST", body: "x".repeat(size) });
  assert.equal(await readLimited(body(10), 10), "x".repeat(10));
  assert.equal(await readLimited(body(11), 10), null);
});

test("images: sniffed by content, content-addressed, SVG refused", async () => {
  const png = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3]);
  assert.equal(sniffImage(png), "png");
  assert.equal(sniffImage(new TextEncoder().encode('<svg onload="alert(1)">')), null);
  assert.equal(saveImage(new TextEncoder().encode("<html>")), null);

  const url = saveImage(png)!;
  assert.equal(saveImage(png), url);
  assert.equal(readdirSync(join(directory, "media")).filter((name) => name.endsWith(".tmp")).length, 0);

  const served = await serveImage(url.replace("/media/", ""));
  assert.equal(served.headers.get("content-type"), "image/png");
  assert.equal((await serveImage("../site.db")).status, 404);
  assert.equal((await serveImage("deadbeef.png")).status, 404);
});

test("markdown: raw HTML kept, images lazy, summary and first image", () => {
  const html = renderMarkdown('one\ntwo\n\n<iframe src="https://open.spotify.com/x"></iframe>\n\n![a](/i.png)');
  assert.match(html, /one<br>/);
  assert.match(html, /<iframe src="https:\/\/open\.spotify\.com/);
  assert.match(html, /loading="lazy"/);
  assert.match(renderMarkdown('![a "b" <c>](/i.png)'), /alt="a &quot;b&quot; &lt;c>"/);
  assert.equal(firstImage("x ![a](/one.png) ![b](/two.png)"), "/one.png");
  assert.equal(summarize({ description: " set ", body: "x" }), "set");
  assert.equal(summarize({ description: "", body: "**bold** text\n\n<iframe src=x></iframe>" }), "bold text");
  assert.ok(summarize({ description: "", body: "word ".repeat(100) }).length <= 160);
  assert.equal(isLive({ published_at: null }), false);
});

test("a browser's CRLF line endings do not change the stored body", () => {
  const id = createPost({ title: "Lines", body: "one\ntwo" });
  const result = savePost(id, { title: "Lines", description: "", body: "one\r\ntwo", slug: null, published_at: null }, getPost(id)!.updated_at);
  assert.equal(result.ok && result.post.body, "one\ntwo");
});

test("image names carry their size; a thumbnail exists only above its width", async () => {
  const png = (seed: number) => new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, seed]);
  const large = saveImage(png(10), { width: 2000, height: 1000, thumb: png(11) })!;
  assert.deepEqual(imageSize(large), { width: 2000, height: 1000 });
  assert.equal((await serveImage(thumbUrl(large).replace("/media/", ""))).status, 200);
  assert.equal(imageSize(saveImage(png(12), { width: 2000, height: 1000 })!), null);
  assert.equal(imageSize(saveImage(png(13), { width: 2000, height: 1000, thumb: new Uint8Array([0xff, 0xd8, 0xff, 1]) })!), null);
  assert.equal(thumbUrl(saveImage(png(14), { width: 800, height: 600 })!).includes("-t."), false);
  assert.equal((await serveImage("deadbeef-1x1.png")).status, 404);
  assert.equal((await serveImage("../" + large.replace("/media/", ""))).status, 404);
});

test("photo attributes offer both sizes, and older images pass through", () => {
  const attributes = photoAttributes("/media/a-2000x1000.webp", "50vw");
  assert.equal(attributes.src, "/media/a-2000x1000-t.webp");
  assert.equal(attributes.width, 2000);
  assert.equal(attributes.sizes, "50vw");
  assert.match(attributes.srcset!, /-t\.webp 800w, \/media\/a-2000x1000\.webp 2000w/);
  assert.deepEqual(photoAttributes("/img/old.jpg"), { src: "/img/old.jpg" });
  assert.deepEqual(photoAttributes("/media/a-400x300.webp"), { src: "/media/a-400x300.webp", width: 400, height: 300 });
});

test("events sort by date and stay up through their own day", () => {
  const items = [{ title: "c", date: "2026-12-01" }, { title: "none", date: "" }, { title: "a", date: "2026-01-01" }, { title: "b", date: "2026-06-15" }];
  assert.deepEqual(sortByDate(items, "date").map((item) => item.title), ["a", "b", "c", "none"]);
  assert.deepEqual(upcoming(items, "date", "2026-06-15").map((item) => item.title), ["c", "none", "b"]);
  assert.deepEqual(upcoming(items, "date", "2026-06-16").map((item) => item.title), ["c", "none"]);
});

test("a list of mostly pictures is a photo list", () => {
  const field = (type: "text" | "image" = "text") => ({ label: "L", type, required: false });
  assert.equal(isPhotoList({ label: "G", ownPage: false, fields: { image: field("image"), caption: field() } }), true);
  assert.equal(isPhotoList({ label: "P", ownPage: false, fields: { image: field("image"), label: field(), url: field() } }), false);
  assert.equal(isPhotoList({ label: "L", ownPage: false, fields: { label: field(), url: field() } }), false);
});

test("post images carry their size and offer the thumbnail", () => {
  const sized = renderMarkdown("![A](/media/a-2000x1000.webp)");
  assert.match(sized, /src="\/media\/a-2000x1000-t\.webp"/);
  assert.match(sized, /srcset="\/media\/a-2000x1000-t\.webp 800w, \/media\/a-2000x1000\.webp 2000w"/);
  assert.match(sized, /width="2000" height="1000"/);
  assert.match(renderMarkdown("![B](/media/b-600x400.webp)"), /src="\/media\/b-600x400\.webp" alt="B" width="600" height="400"/);
  assert.match(renderMarkdown("![C](/img/old.jpg)"), /^<p><img src="\/img\/old\.jpg" alt="C" loading="lazy"/);
});

test("lists and texts keep their last 50 saves", () => {
  for (let n = 1; n <= 60; n++) replaceItems("menu", [{ label: String(n) }]);
  const kept = saves();
  assert.equal(kept.length, 50);
  assert.deepEqual(kept[0].before.lists.menu, [{ label: "59" }], "newest first");
  assert.deepEqual(kept[49].before.lists.menu, [{ label: "10" }], "the oldest go first");
});

test("restoring a save writes only declared keys, and keeps what it replaced", () => {
  setTexts({ a: "one", b: "x" });
  saveEdits({ a: "two" }, { menu: { revision: listRevision("menu"), items: [{ label: "m" }] } });
  const id = saves()[0].id;
  assert.deepEqual(saves()[0].before, { texts: { a: "one" }, lists: { menu: [] } });
  const result = restoreSave(id, { texts: ["a"], lists: [] });
  assert.deepEqual(result, { ok: true, save: saves()[0].id });
  assert.deepEqual(saves()[0].before, { texts: { a: "two" }, lists: {} }, "the undeclared list is neither written nor kept");
  assert.deepEqual(listItems("menu"), [{ label: "m" }]);
  assert.deepEqual(restoreSave(id, { texts: ["a"], lists: [] }), { ok: true, save: null }, "nothing left to change keeps nothing");
  assert.deepEqual(restoreSave(9999, { texts: [], lists: [] }), { ok: false });
});

test("a text that was never saved goes back to never saved", () => {
  setTexts({ fresh: "typed" });
  assert.deepEqual(saves()[0].before.texts, { fresh: null });
  restoreSave(saves()[0].id, { texts: ["fresh"], lists: [] });
  assert.equal(db().prepare("SELECT 1 FROM texts WHERE key = 'fresh'").get(), undefined);
});

test("the history table is added after the existing migrations", () => {
  const version = (db().prepare("PRAGMA user_version").get() as { user_version: number }).user_version;
  assert.equal(version, 6);
});
