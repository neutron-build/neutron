import assert from "node:assert/strict";
import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, beforeEach, test } from "node:test";
import { hashPassword } from "./auth.js";
import { defineCms } from "./index.js";
import { closeDb, createPost, db } from "./store.js";

const SITE = "https://site.test";
const PASSWORD = "a-long-enough-test-password";
let directory: string;

const cms = defineCms({
  reservedSlugs: ["about"],
  texts: { tagline: "text" },
  lists: {
    gallery: { fields: { image: { type: "image", required: true }, caption: "text" } },
    events: { sortBy: "date", fields: { title: { type: "text", required: true }, date: { type: "date", required: true }, url: "url" } },
    links: { fields: { label: { type: "text", required: true }, url: { type: "url", required: true } } },
  },
});

/** Calls the handler the way a route file does: the router's decoded path, no server. */
function call(path: string, init: RequestInit & { cookie?: string; origin?: string | null } = {}) {
  const { cookie, origin, ...rest } = init;
  const headers = new Headers(rest.headers);
  if (cookie) headers.set("cookie", cookie);
  const unsafe = rest.method && rest.method !== "GET";
  if (unsafe && origin !== null) headers.set("origin", origin ?? SITE);
  const url = new URL(path, SITE);
  const segments = url.pathname.replace(/^\/n-admin\/?/, "");
  const request = new Request(url, { ...rest, headers });
  return (unsafe ? cms.POST : cms.GET)({ request, params: segments ? { path: segments } : {} });
}

const form = (fields: Record<string, string | string[]>) => {
  const body = new URLSearchParams();
  for (const [key, value] of Object.entries(fields)) for (const one of [value].flat()) body.append(key, one);
  return body;
};

async function logIn() {
  const response = await call("/n-admin/login", { method: "POST", body: form({ password: PASSWORD }), headers: { "x-forwarded-for": `10.0.0.${Math.floor(Math.random() * 250)}` } });
  assert.equal(response.status, 303);
  return response.headers.get("set-cookie")!.split(";")[0];
}

const png = (seed: number) => new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, seed]);

function send(cookie: string, bytes: Uint8Array, fields: Record<string, string | File> = {}) {
  const body = new FormData();
  body.set("file", new File([bytes as BlobPart], "x", { type: "image/png" }));
  for (const [key, value] of Object.entries(fields)) body.set(key, value);
  return call("/n-admin/upload", { method: "POST", body, cookie, headers: { "x-cms": "1" } });
}

const revisionOf = (html: string) => html.match(/name="revision" value="([^"]+)"/)![1];

before(async () => {
  directory = mkdtempSync(join(tmpdir(), "cms-handler-"));
  process.env.CMS_DATA_DIR = directory;
  process.env.SITE_URL = SITE;
  process.env.CMS_PASSWORD_HASH = await hashPassword(PASSWORD);
});
beforeEach(() => {
  for (const table of ["posts", "items", "texts", "posts_old"]) db().exec(`DELETE FROM ${table}`);
});
after(() => {
  closeDb();
  rmSync(directory, { recursive: true, force: true });
});

test("every admin address is gated; only login, styles and images are public", async () => {
  for (const path of ["/n-admin", "/n-admin/lists/gallery", "/n-admin/lists/text", "/n-admin/posts/1", "/n-admin/admin.js", "/n-admin/login/x", "/n-admin/media/a/b", "/n-admin/nope"]) {
    const response = await call(path);
    assert.equal(response.status, 303, path);
    assert.equal(response.headers.get("location"), "/n-admin/login", path);
  }
  assert.equal((await call("/n-admin/login")).status, 200);
  assert.equal((await call("/n-admin/admin.css")).status, 200);
  assert.equal((await call("/n-admin/media/deadbeef.png")).status, 404);
});

test("script requests get a status, never a redirect to a login page", async () => {
  const response = await call("/n-admin/upload", { method: "POST", headers: { "x-cms": "1" } });
  assert.equal(response.status, 401);
});

test("writes need the site's origin, even when signed in", async () => {
  const cookie = await logIn();
  assert.equal((await call("/n-admin", { method: "POST", cookie, origin: "https://evil.test" })).status, 403);
  assert.equal((await call("/n-admin", { method: "POST", cookie, origin: null })).status, 403);
  assert.equal((await call("/n-admin/login", { method: "POST", origin: "https://evil.test", body: form({ password: PASSWORD }) })).status, 403);
  assert.equal((await call("/n-admin", { method: "POST", cookie })).status, 303);
});

test("a wrong password is refused and repeated tries are throttled", async () => {
  const headers = { "x-forwarded-for": "10.9.9.9" };
  const attempt = () => call("/n-admin/login", { method: "POST", body: form({ password: "wrong" }), headers }).then((r) => r.text());
  assert.match(await attempt(), /Wrong password/);
  for (let index = 0; index < 4; index++) await attempt();
  assert.match(await attempt(), /Too many attempts/);
});

test("logging out ends the session on the server", async () => {
  const cookie = await logIn();
  assert.equal((await call("/n-admin", { cookie })).status, 200);
  const out = await call("/n-admin/logout", { method: "POST", cookie });
  assert.match(out.headers.get("set-cookie")!, /Path=\/n-admin/);
  assert.equal((await call("/n-admin", { cookie })).status, 303);
});

test("admin pages forbid caching, framing and indexing", async () => {
  const response = await call("/n-admin", { cookie: await logIn() });
  assert.equal(response.headers.get("cache-control"), "no-store");
  assert.match(response.headers.get("content-security-policy")!, /frame-ancestors 'none'/);
  assert.equal(response.headers.get("x-robots-tag"), "noindex");
});

test("the menu follows the config", async () => {
  const html = await (await call("/n-admin", { cookie: await logIn() })).text();
  for (const label of ["Posts", "Gallery", "Events", "Links", "Text"]) assert.match(html, new RegExp(`>${label}</a>`));
});

test("a post goes from draft to live, and reserved or taken URLs are refused", async () => {
  const cookie = await logIn();
  const created = await call("/n-admin", { method: "POST", cookie });
  const path = created.headers.get("location")!;
  const revision = revisionOf(await (await call(path, { cookie })).text());
  assert.equal(cms.posts().length, 0);

  const save = (fields: Record<string, string>, rev = revision) => call(path, { method: "POST", cookie, body: form({ title: "Hello", body: "Text", revision: rev, ...fields }) });
  assert.match(await (await save({ intent: "save", slug: "about" })).text(), /belongs to another page/);
  assert.match(await (await save({ intent: "save", slug: "n-admin" })).text(), /belongs to another page/);
  assert.equal((await save({ intent: "publish", slug: "hello" })).status, 303);
  assert.equal(cms.post("hello")?.title, "Hello");
  assert.match(await (await save({ intent: "save" })).text(), /changed elsewhere/);
});

test("a stale autosave answers with a status", async () => {
  const cookie = await logIn();
  const id = createPost({ title: "x", body: "y" });
  const response = await call(`/n-admin/posts/${id}`, { method: "POST", cookie, headers: { "x-cms": "1" }, body: form({ title: "x", body: "z", revision: "old" }) });
  assert.equal(response.status, 409);
  assert.equal((await response.json()).error, "stale");
});

test("a list saves in order and a refused save keeps what was typed", async () => {
  const cookie = await logIn();
  const page = await (await call("/n-admin/lists/links", { cookie })).text();
  const revision = revisionOf(page);
  const save = (rows: Record<string, string[]>, extra = {}) => call("/n-admin/lists/links", { method: "POST", cookie, body: form({ revision, ...rows, ...extra }) });

  assert.match(await (await save({ "f.label": ["keep me"], "f.url": [""] })).text(), /Every row needs label and url[\s\S]*value="keep me"/);
  assert.match(await (await save({})).text(), /was not cleared/);
  assert.equal((await save({ "f.label": ["b", "a"], "f.url": ["https://b.test", "https://a.test"] })).status, 303);
  assert.deepEqual(cms.list("links").map((item) => item.label), ["b", "a"]);
  assert.match(await (await save({ "f.label": ["c"], "f.url": ["https://c.test"] })).text(), /changed elsewhere/);
  assert.deepEqual(cms.list("links").map((item) => item.label), ["b", "a"]);
});

test("a dated list sorts itself, refuses bad dates and drops past items from upcoming", async () => {
  const cookie = await logIn();
  const revision = revisionOf(await (await call("/n-admin/lists/events", { cookie })).text());
  const save = (titles: string[], dates: string[]) => call("/n-admin/lists/events", { method: "POST", cookie, body: form({ revision, "f.title": titles, "f.date": dates, "f.url": titles.map(() => "") }) });
  assert.match(await (await save(["Bad"], ["2026-02-31"])).text(), /must be a real date/);
  const day = (offset: number) => new Date(Date.now() + offset * 86_400_000).toISOString().slice(0, 10);
  assert.equal((await save(["Later", "Past", "Today"], [day(30), day(-3), day(0)])).status, 303);
  assert.deepEqual(cms.list("events").map((item) => item.title), ["Past", "Today", "Later"]);
  assert.deepEqual(cms.upcoming("events").map((item) => item.title), ["Today", "Later"]);
});

test("the list page offers bulk photos on photo lists and no arrows on sorted ones", async () => {
  const cookie = await logIn();
  assert.match(await (await call("/n-admin/lists/gallery", { cookie })).text(), /data-row="add-photos"/);
  assert.doesNotMatch(await (await call("/n-admin/lists/events", { cookie })).text(), /data-row="up"/);
  assert.equal((await call("/n-admin/lists/nope", { cookie })).status, 404);
  assert.equal((await call("/n-admin/lists/constructor", { cookie })).status, 404);
});

test("text fields save and read back", async () => {
  const cookie = await logIn();
  assert.equal((await call("/n-admin/lists/text", { method: "POST", cookie, body: form({ tagline: " Hello " }) })).status, 303);
  assert.equal(cms.text("tagline"), "Hello");
  assert.match(await (await call("/n-admin/lists/text", { cookie })).text(), /value="Hello"/);
});

test("uploads land under the mount, keep their thumbnail and are served to anyone", async () => {
  const cookie = await logIn();
  const sized = await (await send(cookie, png(1), { width: "2000", height: "1000", thumb: new File([png(2) as BlobPart], "t", { type: "image/png" }) })).json();
  assert.match(sized.url, /^\/n-admin\/media\/[a-f0-9]{32}-2000x1000\.png$/);
  assert.match(sized.thumb, /-2000x1000-t\.png$/);
  for (const url of [sized.url, sized.thumb]) {
    const served = await call(url);
    assert.equal(served.status, 200);
    assert.match(served.headers.get("cache-control")!, /immutable/);
  }
  assert.match((await (await send(cookie, png(3), { width: "2000", height: "1000" })).json()).url, /\/[a-f0-9]{32}\.png$/);
  assert.match((await (await send(cookie, png(4), { width: "-5", height: "1e3" })).json()).url, /\/[a-f0-9]{32}\.png$/);
  assert.ok(readdirSync(join(directory, "media")).length >= 4);
});

test("SVG, HTML and oversized uploads are refused", async () => {
  const cookie = await logIn();
  assert.equal((await send(cookie, new TextEncoder().encode("<svg onload=alert(1)>"))).status, 415);
  assert.equal((await send(cookie, new TextEncoder().encode("<html>"))).status, 415);
  assert.equal((await send(cookie, new Uint8Array(13 * 1024 * 1024))).status, 413);
  for (const name of ["..%2Fsite.db", "site.db", "../site.db"]) assert.equal((await call(`/n-admin/media/${name}`)).status === 200, false, name);
});

test("a wrong config stops at startup", () => {
  assert.throws(() => defineCms({ lists: { text: { fields: { a: "text" } } } }), /cannot be called "text"/);
  assert.throws(() => defineCms({ lists: { a: { sortBy: "b", fields: { b: "text" } } } }), /not a date field/);
  assert.throws(() => defineCms({ lists: { a: { fields: {} } } }), /has no fields/);
});

test("a site with no blog hides posts and opens on its first list", async () => {
  const noBlog = defineCms({ posts: false, lists: { gallery: { fields: { image: "image" } } } });
  const cookie = await logIn();
  const request = new Request(`${SITE}/n-admin`, { headers: { cookie } });
  const response = await noBlog.GET({ request });
  assert.equal(response.headers.get("location"), "/n-admin/lists/gallery");
  const page = await (await noBlog.GET({ request: new Request(`${SITE}/n-admin/lists/gallery`, { headers: { cookie } }), params: { path: "lists/gallery" } })).text();
  assert.doesNotMatch(page, />Posts<\/a>/);
  assert.equal((await noBlog.GET({ request: new Request(`${SITE}/n-admin/posts/1`, { headers: { cookie } }), params: { path: "posts/1" } })).status, 404);
});

test("the admin can live at another path", async () => {
  const moved = defineCms({ path: "/edit/" });
  const cookie = await logIn();
  const page = await (await moved.GET({ request: new Request(`${SITE}/edit`, { headers: { cookie } }) })).text();
  assert.match(page, /href="\/edit\/admin\.css"/);
  assert.match(page, /src="\/edit\/admin\.js"/);
  assert.match(await moved.GET({ request: new Request(`${SITE}/edit`) }).then((r) => r.headers.get("location")!), /^\/edit\/login$/);
});

test("ready is true on an empty database, so a new site can deploy, and false when the database is gone", () => {
  assert.equal(cms.ready(), true);
  closeDb();
  const { CMS_DATA_DIR, NODE_ENV } = process.env;
  process.env.CMS_DATA_DIR = join(directory, "lost-volume");
  process.env.NODE_ENV = "production";
  try {
    assert.equal(cms.ready(), false);
  } finally {
    process.env.CMS_DATA_DIR = CMS_DATA_DIR;
    process.env.NODE_ENV = NODE_ENV;
  }
});
