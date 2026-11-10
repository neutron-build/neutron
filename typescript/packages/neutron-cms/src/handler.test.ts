import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, beforeEach, test } from "node:test";
import { createHash } from "node:crypto";
import { hashPassword, isPreview, previewToken } from "./auth.js";
import { defineCms } from "./index.js";
import { closeDb, createPost, db, listRevision, replaceItems, restoreVersion, saveEdits, saves, setTexts, trashPost, type Item } from "./store.js";

const SITE = "https://site.test";
const PASSWORD = "a-long-enough-test-password";
let directory: string;

const cms = defineCms({
  pages: true,
  reservedSlugs: ["about"],
  texts: { tagline: "text" },
  onPage: ["links", "text"],
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
  db({ create: true });
  process.env.SITE_URL = SITE;
  process.env.CMS_PASSWORD_HASH = await hashPassword(PASSWORD);
});
beforeEach(() => {
  for (const table of ["posts", "items", "texts", "posts_old", "saves", "people", "owner_password"]) db().exec(`DELETE FROM ${table}`);
});
after(() => {
  closeDb();
  rmSync(directory, { recursive: true, force: true });
});

test("every admin address is gated; only login, styles and images are public", async () => {
  for (const path of ["/n-admin/edit", "/n-admin/edit/data", "/n-admin/edit.js", "/n-admin", "/n-admin/lists/gallery", "/n-admin/lists/text", "/n-admin/posts/1", "/n-admin/admin.js", "/n-admin/login/x", "/n-admin/media/a/b", "/n-admin/nope"]) {
    const response = await call(path);
    assert.equal(response.status, 303, path);
    assert.match(response.headers.get("location")!, /^\/n-admin\/login(\?next=%2Fn-admin[^/]*)?$/, path);
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
  for (const label of ["Posts", "Edit site", "Gallery", "Events"]) assert.match(html, new RegExp(`>${label}</a>`));
  for (const label of ["Links", "Text"]) assert.doesNotMatch(html, new RegExp(`>${label}</a>`));
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

  assert.match(await (await save({ "f.label": ["keep me"], "f.url": [""] })).text(), /add URL\.[\s\S]*value="keep me"/);
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

test("form text keeps plain line ends, as the editors save them", async () => {
  const cookie = await logIn();
  await call("/n-admin/lists/text", { method: "POST", cookie, body: form({ tagline: "one\r\ntwo" }) });
  assert.equal(cms.text("tagline"), "one\ntwo");
  const revision = revisionOf(await (await call("/n-admin/lists/links", { cookie })).text());
  await call("/n-admin/lists/links", { method: "POST", cookie, body: form({ revision, "f.label": ["a\r\nb"], "f.url": ["https://a.test"] }) });
  assert.equal(cms.list("links")[0].label, "a\nb");
});

test("an upload that arrives empty says so, so the page can try again", async () => {
  const cookie = await logIn();
  const response = await send(cookie, new Uint8Array(0));
  assert.equal(response.status, 400);
  assert.equal((await response.json()).empty, true);
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
  assert.match(page, /href="\/edit\/admin\.css\?v=[a-f0-9]{12}"/);
  assert.match(page, /src="\/edit\/admin\.js\?v=[a-f0-9]{12}"/);
  assert.match(await moved.GET({ request: new Request(`${SITE}/edit`) }).then((r) => r.headers.get("location")!), /^\/edit\/login$/);
});

test("the tab icon is the site's: its favicon by default, or the one named in icon", async () => {
  const login = (site: { GET: typeof cms.GET }) => site.GET({ request: new Request(`${SITE}/n-admin/login`), params: { path: "login" } }).then((r) => r.text());
  assert.doesNotMatch(await login(cms), /rel="icon"/);
  assert.match(await login(defineCms({ icon: "/img/icon.png" })), /<link rel="icon" href="\/img\/icon\.png"/);
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

const edit = (cookie: string, body: unknown) =>
  call("/n-admin/edit/save", { method: "POST", cookie, headers: { "x-cms": "1" }, body: typeof body === "string" ? body : JSON.stringify(body) });

test("the editor page, script and data are there once signed in", async () => {
  const cookie = await logIn();
  const page = await (await call("/n-admin/edit", { cookie })).text();
  assert.match(page, /data-edit-frame/);
  assert.match(page, /src="\/n-admin\/edit\.js\?v=[a-f0-9]{12}"/);
  assert.match(await (await call("/n-admin/edit.js", { cookie })).text(), /data-edit-save/);
  const data = await (await call("/n-admin/edit/data", { cookie })).json();
  assert.equal(data.lists.links.fields.url.type, "url");
  assert.equal(data.textFields.tagline.type, "text");
  assert.equal((await call("/n-admin/edit/save", { method: "POST", headers: { "x-cms": "1" }, body: "{}" })).status, 401);
  assert.equal((await call("/n-admin/edit/save", { method: "POST", cookie, origin: "https://evil.test", body: "{}" })).status, 403);
});

test("a page's edits save together, keeping only declared fields", async () => {
  const cookie = await logIn();
  const { lists } = await (await call("/n-admin/edit/data", { cookie })).json();
  const response = await edit(cookie, {
    texts: { tagline: " Hi " },
    lists: { links: { revision: lists.links.revision, items: [{ label: " One ", url: "https://a.test", injected: "x", label2: 5 }] } },
  });
  assert.equal(response.status, 200);
  assert.equal(cms.text("tagline"), "Hi");
  assert.deepEqual(cms.list("links"), [{ label: "One", url: "https://a.test" }]);
});

test("a stale or invalid edit writes nothing at all", async () => {
  const cookie = await logIn();
  const { lists } = await (await call("/n-admin/edit/data", { cookie })).json();
  assert.equal((await edit(cookie, { texts: { tagline: "first" }, lists: { links: { revision: lists.links.revision, items: [{ label: "a", url: "https://a.test" }] } } })).status, 200);

  const stale = await edit(cookie, { texts: { tagline: "second" }, lists: { links: { revision: lists.links.revision, items: [{ label: "b", url: "https://b.test" }] } } });
  assert.equal(stale.status, 409);
  assert.equal(cms.text("tagline"), "first");
  assert.equal(cms.list("links")[0].label, "a");

  const fresh = (await (await call("/n-admin/edit/data", { cookie })).json()).lists.links.revision;
  assert.equal((await edit(cookie, { texts: { tagline: "third" }, lists: { links: { revision: fresh, items: [{ label: "", url: "" }] } } })).status, 422);
  assert.equal(cms.text("tagline"), "first");

  assert.equal((await edit(cookie, { texts: { nope: "x" } })).status, 422);
  assert.equal((await edit(cookie, { lists: { nope: { items: [] } } })).status, 422);
  assert.equal((await edit(cookie, { lists: { constructor: { items: [] } } })).status, 422);
  assert.equal((await edit(cookie, "not json")).status, 400);
});

test("onPage must name real lists, and a site with no posts opens the editor", async () => {
  assert.throws(() => defineCms({ onPage: ["nope"] }), /not a list/);
  const onlyPage = defineCms({ posts: false, onPage: ["a"], lists: { a: { fields: { b: "text" } } } });
  const cookie = await logIn();
  const response = await onlyPage.GET({ request: new Request(`${SITE}/n-admin`, { headers: { cookie } }) });
  assert.equal(response.headers.get("location"), "/n-admin/edit");
});

test("the stylesheet's address changes with its content, so an old cached copy is never used", async () => {
  const page = await (await call("/n-admin/login")).text();
  const address = page.match(/href="(\/n-admin\/admin\.css\?v=[a-f0-9]{12})"/)![1];
  const current = await call(address);
  assert.match(current.headers.get("cache-control")!, /immutable/);
  assert.match(await current.text(), /\.edit-frame/);
  assert.equal((await call("/n-admin/admin.css")).headers.get("cache-control"), "no-store");
  assert.equal((await call("/n-admin/admin.css?v=old")).headers.get("cache-control"), "no-store");
});

test("the editor's back link never returns to the editor", async () => {
  const cookie = await logIn();
  const open = async (site: { GET: (args: { request: Request; params?: Record<string, string> }) => Promise<Response> }) => (await site.GET({ request: new Request(`${SITE}/n-admin/edit`, { headers: { cookie } }), params: { path: "edit" } })).text();
  assert.match(await open(cms), /class="back" href="\/n-admin"/);
  const lists = { a: { fields: { b: "text" } }, c: { label: "Gallery", fields: { d: "text" } } } as const;
  assert.match(await open(defineCms({ posts: false, onPage: ["a"], lists })), /class="back" href="\/n-admin\/lists\/c"/);
  assert.doesNotMatch(await open(defineCms({ posts: false, onPage: ["a", "c"], lists })), /class="back"/);
});

test("a planted second session cookie can't sign the owner out", async () => {
  const cookie = await logIn();
  assert.equal((await call("/n-admin", { cookie: `n_session=planted; ${cookie}` })).status, 200);
  assert.equal((await call("/n-admin", { cookie: `${cookie}; n_session=planted` })).status, 200);
});

test("the throttle believes forwarded addresses only from a local proxy, and groups IPv6 by /64", async () => {
  const { clientAddress } = await import("./auth.js");
  const peers = new WeakMap<Request, { remoteAddress: string }>();
  (globalThis as Record<symbol, unknown>)[Symbol.for("@neutron-build/core/transport-peer")] ??= peers;
  const registry = (globalThis as Record<symbol, WeakMap<Request, { remoteAddress: string }>>)[Symbol.for("@neutron-build/core/transport-peer")];
  const at = (peer: string, forwarded?: string) => {
    const request = new Request(SITE, { headers: forwarded ? { "x-forwarded-for": forwarded } : {} });
    registry.set(request, { remoteAddress: peer });
    return clientAddress(request);
  };
  assert.equal(at("203.0.113.9", "1.2.3.4"), "203.0.113.9");
  assert.equal(at("172.18.0.2", "198.51.100.7"), "198.51.100.7");
  assert.equal(at("::ffff:127.0.0.1", "198.51.100.8"), "198.51.100.8");
  assert.equal(at("2001:db8:1:2:aaaa::1"), "2001:db8:1:2::/64");
  assert.equal(at("2001:db8:1:2:bbbb::9"), "2001:db8:1:2::/64");
});

test("a full throttle drops its oldest entry, not everyone's", async () => {
  const { takeAttempt } = await import("./auth.js");
  const at = 1_000_000;
  for (let index = 0; index < 6; index++) takeAttempt("locked-out", at);
  assert.equal(takeAttempt("locked-out", at), false);
  for (let index = 0; index < 10_000; index++) takeAttempt(`flood-${index}`, at);
  assert.equal(takeAttempt("flood-9999", at), true);
});

test("images stream with their length", async () => {
  const cookie = await logIn();
  const { url } = await (await send(cookie, png(42))).json();
  const served = await call(url);
  assert.equal(served.headers.get("content-length"), "9");
  assert.equal((await served.arrayBuffer()).byteLength, 9);
});

test("without SITE_URL only a localhost origin is accepted, and https sites get Secure cookies", async () => {
  const { sessionCookie } = await import("./auth.js");
  const saved = process.env.SITE_URL;
  try {
    process.env.SITE_URL = "https://site.test";
    assert.match(sessionCookie("t"), /; Secure$/);
    delete process.env.SITE_URL;
    const cookie = await (async () => {
      process.env.SITE_URL = SITE;
      const value = await logIn();
      delete process.env.SITE_URL;
      return value;
    })();
    const write = (host: string) => cms.POST({ request: new Request(`http://${host}/n-admin`, { method: "POST", headers: { cookie, origin: `http://${host}` } }) });
    assert.equal((await write("evil.test")).status, 403);
    assert.equal((await write("localhost:3000")).status, 303);
  } finally {
    process.env.SITE_URL = saved;
  }
});

test("an empty configured data folder is never filled with a new database", async () => {
  const { DatabaseSync } = await import("node:sqlite");
  void DatabaseSync;
  closeDb();
  const saved = process.env.CMS_DATA_DIR;
  process.env.CMS_DATA_DIR = join(directory, "unmounted");
  try {
    assert.equal(cms.ready(), false);
  } finally {
    closeDb();
    process.env.CMS_DATA_DIR = saved;
  }
});

test("pages are kept apart from the blog and share its editor", async () => {
  const cookie = await logIn();
  const created = await call("/n-admin/pages", { method: "POST", cookie });
  const path = created.headers.get("location")!;
  assert.match(path, /^\/n-admin\/posts\/\d+$/);
  const editor = await (await call(path, { cookie })).text();
  assert.match(editor, /href="\/n-admin\/pages"/);
  const revision = editor.match(/name="revision" value="([^"]+)"/)![1];
  assert.equal((await call(path, { method: "POST", cookie, body: form({ intent: "publish", title: "About me", body: "Hi", slug: "about-me", revision }) })).status, 303);
  assert.equal(cms.page("about-me")?.title, "About me");
  assert.equal(cms.post("about-me"), undefined);
  assert.deepEqual(cms.posts().map((post) => post.slug), []);
  assert.deepEqual(cms.pages().map((page) => page.slug), ["about-me"]);
  const list = await (await call("/n-admin/pages", { cookie })).text();
  assert.match(list, />About me</);
  assert.match(list, />New page</);
  assert.doesNotMatch(await (await call("/n-admin", { cookie })).text(), />About me</);
  assert.match(list, />Pages<\/a>/);
});

test("a site without pages has no Pages section", async () => {
  const noPages = defineCms({});
  const cookie = await logIn();
  const response = await noPages.GET({ request: new Request(`${SITE}/n-admin/pages`, { headers: { cookie } }), params: { path: "pages" } });
  assert.equal(response.status, 404);
});

test("the preview resolves the site's own image paths", async () => {
  const cookie = await logIn();
  const response = await call("/n-admin/preview", { method: "POST", cookie, body: form({ title: "T", body: "![](/img/a.jpg)" }) });
  const html = await response.text();
  assert.match(html, /src="https:\/\/site\.test\/img\/a\.jpg"/);
  assert.doesNotMatch(html, /<base/);
  const sized = await (await call("/n-admin/preview", { method: "POST", cookie, body: form({ title: "T", body: "![](/media/a-2000x1000.webp)\n\n[x](/about/) [y](https://other.test/z)" }) })).text();
  assert.match(sized, /srcset="https:\/\/site\.test\/media\/a-2000x1000-t\.webp 800w, https:\/\/site\.test\/media\/a-2000x1000\.webp 2000w"/);
  assert.match(sized, /href="https:\/\/site\.test\/about\/"/);
  assert.match(sized, /href="https:\/\/other\.test\/z"/);
});

test("admin scripts are versioned, cached only when current, and kept behind the login", async () => {
  const cookie = await logIn();
  const created = await call("/n-admin", { method: "POST", cookie });
  const editor = await (await call(created.headers.get("location")!, { cookie })).text();
  const src = editor.match(/src="(\/n-admin\/editor\.js\?v=[a-f0-9]{12})"/)![1];
  const current = await call(src, { cookie });
  assert.equal(current.headers.get("cache-control"), "private, max-age=31536000, immutable");
  assert.match(await current.text(), /cmsUpload/);
  assert.equal((await call("/n-admin/editor.js?v=old", { cookie })).headers.get("cache-control"), "no-store");
  assert.equal((await call(src)).status, 303);
  assert.match(editor, /data-toggle-markdown/);
});

test("a designed page is a draft until published, and its draft shows only through Edit site", async () => {
  const designed = defineCms({ designs: { portfolio: "Portfolio" }, onPage: ["text"], texts: { tagline: "text" } });
  const cookie = await logIn();
  const call2 = (path: string, init: RequestInit = {}) => {
    const url = new URL(path, SITE);
    const unsafe = init.method && init.method !== "GET";
    const headers = new Headers({ cookie, ...(unsafe ? { origin: SITE } : {}) });
    return (unsafe ? designed.POST : designed.GET)({ request: new Request(url, { ...init, headers }), params: { path: url.pathname.replace(/^\/n-admin\/?/, "") } });
  };
  const list = await (await call2("/n-admin/pages")).text();
  assert.match(list, />Portfolio</);
  assert.match(list, />draft</);
  const id = list.match(/href="\/n-admin\/posts\/(\d+)">Portfolio</)![1];
  const editor = await (await call2(`/n-admin/posts/${id}`)).text();
  assert.match(editor, /data-edit-frame/, "a designed page opens as itself");
  assert.match(editor, /value="publish"/);
  assert.doesNotMatch(editor, /data-toggle-preview/);

  assert.equal(designed.page("portfolio"), undefined);
  assert.equal(designed.page("portfolio", new Request(`${SITE}/portfolio/`)), undefined);
  assert.equal(designed.page("portfolio", new Request(`${SITE}/portfolio/?cms-preview=1.forged`)), undefined);
  assert.equal((await call2(`/n-admin/edit?page=${id}`)).headers.get("location"), `/n-admin/posts/${id}`);
  const edit = editor;
  const src = edit.match(/data-edit-frame[^>]*|src="([^"]+)"[^>]*data-edit-frame/)![0].match(/src="([^"]+)"/)![1].replace(/&amp;/g, "&");
  assert.match(src, /^\/portfolio\?cms-preview=/);
  assert.equal(designed.page("portfolio", new Request(SITE + src))?.layout, "portfolio");
  assert.equal(designed.page("other", new Request(SITE + src.replace("/portfolio", "/other"))), undefined);

  const revision = editor.match(/name="revision" value="([^"]+)"/)![1];
  assert.equal((await call2(`/n-admin/posts/${id}`, { method: "POST", body: new URLSearchParams({ intent: "publish", title: "Portfolio", body: "ignored", slug: "portfolio", revision }) })).status, 303);
  assert.equal(designed.page("portfolio")?.layout, "portfolio");
  assert.equal(designed.page("portfolio")?.body, "");
  assert.deepEqual(designed.pages().map((page) => page.slug), ["portfolio"]);
});

test("a page imported at a design's address becomes that design's page", () => {
  const id = createPost({ kind: "page", slug: "gallery", title: "Gallery", body: "old text" });
  const designed = defineCms({ designs: { gallery: "Gallery" } });
  assert.equal(designed.pages().length, 0);
  const row = db().prepare("SELECT id, layout, body FROM posts WHERE slug = 'gallery'").get() as { id: number; layout: string; body: string };
  assert.deepEqual({ ...row }, { id, layout: "gallery", body: "old text" });
  assert.equal((db().prepare("SELECT COUNT(*) AS n FROM posts WHERE layout = 'gallery'").get() as { n: number }).n, 1);
});

test("a list edited on the site can keep its own admin page", async () => {
  const menu = defineCms({ onPage: ["menu"], lists: { menu: { label: "Menu links", ownPage: true, fields: { label: "text", url: "url" } } } });
  const cookie = await logIn();
  const page = await (await menu.GET({ request: new Request(`${SITE}/n-admin/lists/menu`, { headers: { cookie } }), params: { path: "lists/menu" } })).text();
  assert.match(page, />Menu links<\/a>/);
  assert.match(page, /<span>Label<\/span>/);
});

test("a homepage design is always published, answers only at its own address, and can't be taken down", async () => {
  const site = defineCms({ reservedSlugs: ["gpg"], designs: { home: { title: "Home", url: "/", permanent: true }, gpg: { title: "GPG", url: "/gpg/", published: true }, extra: "Extra" } });
  const cookie = await logIn();
  const call2 = (path: string, init: RequestInit = {}) => {
    const url = new URL(path, SITE);
    const unsafe = init.method && init.method !== "GET";
    const headers = new Headers({ cookie, ...(unsafe ? { origin: SITE } : {}) });
    return (unsafe ? site.POST : site.GET)({ request: new Request(url, { ...init, headers }), params: { path: url.pathname.replace(/^\/n-admin\/?/, "") } });
  };
  const list = await (await call2("/n-admin/pages")).text();
  assert.match(list, />Home<\/a><\/td><td class="status live">live</);
  assert.match(list, />GPG<\/a><\/td><td class="status live">live</);
  assert.match(list, />Extra<\/a><\/td><td class="status draft">draft</);
  assert.equal(site.design("home")?.title, "Home");
  assert.equal(site.page("home"), undefined, "the homepage doesn't also answer at /home");
  assert.equal(site.design("gpg")?.title, "GPG");
  assert.equal(site.design("extra"), undefined);

  const id = list.match(/href="\/n-admin\/posts\/(\d+)">Home</)![1];
  const editor = await (await call2(`/n-admin/posts/${id}`)).text();
  assert.doesNotMatch(editor, /value="unpublish"/);
  assert.doesNotMatch(editor, /value="delete"/);
  const revision = "any"; // the homepage has no publish controls at all
  await call2(`/n-admin/posts/${id}`, { method: "POST", body: new URLSearchParams({ intent: "unpublish", revision }) });
  await call2(`/n-admin/posts/${id}`, { method: "POST", body: new URLSearchParams({ intent: "delete" }) });
  assert.equal(site.design("home")?.title, "Home");

  const extra = list.match(/href="\/n-admin\/posts\/(\d+)">Extra</)![1];
  await call2(`/n-admin/posts/${extra}`, { method: "POST", body: new URLSearchParams({ intent: "delete" }) });
  assert.match(await (await call2(`/n-admin/pages`)).text(), />Extra<\/a>/, "a design's page can't be thrown away");

  const gpg = list.match(/href="\/n-admin\/posts\/(\d+)">GPG</)![1];
  const gpgEditor = await (await call2(`/n-admin/posts/${gpg}`)).text();
  assert.match(gpgEditor, /value="unpublish"/);
  const gpgRevision = gpgEditor.match(/name="revision" value="([^"]+)"/)![1];
  await call2(`/n-admin/posts/${gpg}`, { method: "POST", body: new URLSearchParams({ intent: "unpublish", revision: gpgRevision }) });
  assert.equal(site.design("gpg"), undefined, "a reserved address doesn't stop a design being unpublished");
  assert.equal(site.design("gpg") === undefined && (await site.GET({ request: new Request(`${SITE}/n-admin/pages`, { headers: { cookie } }), params: { path: "pages" } })).status, 200);
  const edit = await (await call2(`/n-admin/posts/${gpg}`)).text();
  assert.match(edit, /value="publish"/);
  const src = edit.match(/src="([^"]+)"[^>]*data-edit-frame/)![1].replace(/&amp;/g, "&");
  assert.match(src, /^\/gpg\/\?cms-preview=/);
  assert.equal(site.design("gpg", new Request(SITE + src))?.title, "GPG");
});

test("a refused save can't be pressed again to overwrite the newer text", async () => {
  const cookie = await logIn();
  const path = (await call("/n-admin", { method: "POST", cookie })).headers.get("location")!;
  const opened = (await (await call(path, { cookie })).text()).match(/name="revision" value="([^"]+)"/)![1];
  // Another tab saves first.
  assert.equal((await call(path, { method: "POST", cookie, body: form({ intent: "save", title: "B", body: "newer", revision: opened }) })).status, 303);
  // This tab's save is refused, and the page it gets back must still carry the old revision.
  const refused = await (await call(path, { method: "POST", cookie, body: form({ intent: "save", title: "A", body: "older", revision: opened }) })).text();
  assert.match(refused, /changed elsewhere|Changed elsewhere|no longer/i);
  const again = refused.match(/name="revision" value="([^"]+)"/)![1];
  assert.equal(again, opened);
  await call(path, { method: "POST", cookie, body: form({ intent: "save", title: "A", body: "older", revision: again }) });
  const id = Number(path.split("/").pop());
  assert.equal((db().prepare("SELECT body FROM posts WHERE id = ?").get(id) as { body: string }).body, "newer");
});

test("typed links are completed, and a bad one names its row", async () => {
  const cookie = await logIn();
  const page = await (await call("/n-admin/lists/links", { cookie })).text();
  const revision = page.match(/name="revision" value="([^"]*)"/)![1];
  const save = (labels: string[], urls: string[]) => {
    const body = new URLSearchParams({ revision });
    labels.forEach((label, i) => { body.append("f.label", label); body.append("f.url", urls[i]); });
    return call("/n-admin/lists/links", { method: "POST", cookie, body });
  };
  assert.match(await (await save(["a", "b"], ["https://a.test", "fylun dot ai"])).text(), /Row 2: &quot;fylun dot ai&quot; isn't a link/);
  assert.match(await (await save(["a"], ["javascript:alert(1)"])).text(), /isn.{1,6}t a link/);
  assert.equal((await save(["a", "b", "c"], ["fylun.ai", "me@site.com", "/about/"])).status, 303);
  assert.deepEqual(cms.list("links").map((item) => item.url), ["https://fylun.ai", "mailto:me@site.com", "/about/"]);
});

test("after a lapsed session, logging in returns to the page you were on, and only inside the admin", async () => {
  const away = await call("/n-admin/posts/1");
  assert.equal(away.headers.get("location"), "/n-admin/login?next=%2Fn-admin%2Fposts%2F1");
  assert.match(await (await call("/n-admin/login?next=%2Fn-admin%2Fposts%2F1")).text(), /name="next" value="\/n-admin\/posts\/1"/);
  assert.doesNotMatch(await (await call("/n-admin/login?next=https%3A%2F%2Fevil.test")).text(), /name="next"/);
  assert.doesNotMatch(await (await call("/n-admin/login?next=%2F%2Fevil.test")).text(), /name="next"/);
});

test("a title-made address steps aside for one in use; publishing needs a title", async () => {
  const cookie = await logIn();
  const open = async () => {
    const path = (await call("/n-admin", { method: "POST", cookie })).headers.get("location")!;
    const revision = (await (await call(path, { cookie })).text()).match(/name="revision" value="([^"]+)"/)![1];
    return { path, revision };
  };
  const first = await open();
  await call(first.path, { method: "POST", cookie, body: form({ intent: "publish", title: "Notes", body: "x", revision: first.revision }) });
  const second = await open();
  assert.equal((await call(second.path, { method: "POST", cookie, body: form({ intent: "publish", title: "Notes", body: "y", revision: second.revision }) })).status, 303);
  assert.ok(cms.post("notes-2"));
  const third = await open();
  assert.match(await (await call(third.path, { method: "POST", cookie, body: form({ intent: "publish", title: "", body: "z", revision: third.revision }) })).text(), /Add a title before publishing/);
});

test("a deleted post goes to the Trash, and comes back as it was with its versions", async () => {
  const cookie = await logIn();
  const id = createPost({ title: "Keep me", body: "first", slug: "keep-me", published_at: "2020-01-02T00:00:00.000Z" });
  const open = await (await call(`/n-admin/posts/${id}`, { cookie })).text();
  await call(`/n-admin/posts/${id}`, { method: "POST", cookie, body: form({ intent: "save", title: "Keep me", body: "second", slug: "keep-me", date: "2020-01-02T00:00:00.000Z", revision: revisionOf(open), label: "", description: "" }) });
  assert.equal(cms.post("keep-me")?.body, "second");

  const deleted = await call(`/n-admin/posts/${id}`, { method: "POST", cookie, body: form({ intent: "delete" }) });
  const after = deleted.headers.get("location")!;
  const trashId = after.match(/deleted=(\d+)/)![1];
  assert.equal(cms.post("keep-me"), undefined, "off the site at once");
  assert.match(await (await call(after, { cookie })).text(), /&quot;Keep me&quot; moved to Trash|"Keep me" moved to Trash/);
  assert.match(await (await call("/n-admin/trash", { cookie })).text(), /Keep me/);

  const restored = await call(`/n-admin/trash/${trashId}`, { method: "POST", cookie, body: form({}) });
  const newId = restored.headers.get("location")!.match(/posts\/(\d+)$/)![1];
  const back = cms.post("keep-me");
  assert.equal(back?.body, "second");
  assert.equal(back?.published_at, "2020-01-02T00:00:00.000Z", "keeps its date");
  assert.match(await (await call(`/n-admin/posts/${newId}/versions`, { cookie })).text(), /1 words/);
  assert.doesNotMatch(await (await call("/n-admin/trash", { cookie })).text(), /Keep me/);
});

test("a post whose address was taken while in the Trash comes back as a draft", async () => {
  const cookie = await logIn();
  const id = createPost({ title: "Old", body: "x", slug: "shared", published_at: "2020-01-01T00:00:00.000Z" });
  const deleted = await call(`/n-admin/posts/${id}`, { method: "POST", cookie, body: form({ intent: "delete" }) });
  const trashId = deleted.headers.get("location")!.match(/deleted=(\d+)/)![1];
  createPost({ title: "New", body: "y", slug: "shared", published_at: "2020-01-01T00:00:00.000Z" });
  const restored = await call(`/n-admin/trash/${trashId}`, { method: "POST", cookie, body: form({}) });
  const location = restored.headers.get("location")!;
  assert.match(location, /address-lost/);
  assert.equal(cms.post("shared")?.title, "New");
  const editor = await (await call(location, { cookie })).text();
  assert.match(editor, /came back as a draft/);
  assert.match(editor, /class="status draft"/);
});

test("delete forever takes the post and its versions, and a new post can't inherit them", async () => {
  const cookie = await logIn();
  const id = createPost({ title: "Gone", body: "one", published_at: "2020-01-01T00:00:00.000Z" });
  db().prepare("UPDATE posts SET body = 'two' WHERE id = ?").run(id);
  const deleted = await call(`/n-admin/posts/${id}`, { method: "POST", cookie, body: form({ intent: "delete" }) });
  const trashId = deleted.headers.get("location")!.match(/deleted=(\d+)/)![1];
  const fresh = createPost({ title: "Fresh", body: "new" });
  assert.equal(fresh, id, "sqlite reuses the id, which is the case that matters");
  assert.match(await (await call(`/n-admin/posts/${fresh}/versions`, { cookie })).text(), /No earlier versions/);
  await call(`/n-admin/trash/${trashId}`, { method: "POST", cookie, body: form({ intent: "forever" }) });
  assert.equal((db().prepare("SELECT count(*) AS n FROM posts_old WHERE body = 'one'").get() as { n: number }).n, 0);
  assert.doesNotMatch(await (await call("/n-admin/trash", { cookie })).text(), /Gone/);
});

test("the Trash forgets things after 30 days, and needs a login", async () => {
  const cookie = await logIn();
  const id = createPost({ title: "Ancient", body: "x" });
  await call(`/n-admin/posts/${id}`, { method: "POST", cookie, body: form({ intent: "delete" }) });
  db().prepare("UPDATE posts_trash SET deleted_at = '2020-01-01T00:00:00.000Z'").run();
  assert.match(await (await call("/n-admin/trash", { cookie })).text(), /The Trash is empty/);
  assert.equal((await call("/n-admin/trash")).status, 303);
  assert.equal((await call("/n-admin/trash/1", { method: "POST", body: form({}) })).status, 303);
});

test("restoring a version brings it back, and what it replaced can be restored too", async () => {
  const cookie = await logIn();
  const id = createPost({ title: "One", body: "<script>alert(1)</script> old text", published_at: "2020-01-01T00:00:00.000Z" });
  db().prepare("UPDATE posts SET title = 'Two', body = 'new text', updated_at = '2021-01-01T00:00:00.000Z' WHERE id = ?").run(id);
  const page = await (await call(`/n-admin/posts/${id}/versions`, { cookie })).text();
  assert.match(page, /sandbox="allow-same-origin"/);
  assert.doesNotMatch(page, /<script>alert/);
  const version = page.match(/name="version" value="(\d+)"/)![1];

  const stale = await call(`/n-admin/posts/${id}/versions`, { method: "POST", cookie, body: form({ version, revision: "old" }) });
  assert.equal(stale.status, 409);
  assert.match(await stale.text(), /changed since you opened/);

  const done = await call(`/n-admin/posts/${id}/versions`, { method: "POST", cookie, body: form({ version, revision: "2021-01-01T00:00:00.000Z" }) });
  assert.equal(done.status, 303);
  const row = db().prepare("SELECT title, body FROM posts WHERE id = ?").get(id) as { title: string; body: string };
  assert.equal(row.title, "One");
  assert.match(row.body, /old text/);
  assert.match(await (await call(`/n-admin/posts/${id}/versions`, { cookie })).text(), /Two, 2 words/);

  const other = createPost({ title: "Other", body: "x" });
  assert.equal((await call(`/n-admin/posts/${other}/versions`, { method: "POST", cookie, body: form({ version, revision: "any" }) })).status, 409, "a version belongs to its own post");
});

test("a draft keeps a version every ten minutes, but restoring always keeps what it replaced", () => {
  const id = createPost({ title: "Draft", body: "a", updated_at: "2026-01-01T00:00:00.000Z" });
  const edit = (body: string, at: string) => db().prepare("UPDATE posts SET body = ?, updated_at = ? WHERE id = ?").run(body, at, id);
  const count = () => (db().prepare("SELECT count(*) AS n FROM posts_old WHERE post_id = ?").get(id) as { n: number }).n;
  edit("ab", "2026-01-01T00:00:02.000Z");
  edit("abc", "2026-01-01T00:00:04.000Z");
  edit("abcd", "2026-01-01T00:00:06.000Z");
  assert.equal(count(), 1, "typing doesn't flood the history");
  edit("abcde", "2026-01-01T00:11:00.000Z");
  edit("abcdef", "2026-01-01T00:12:00.000Z");
  assert.equal(count(), 2);
  const version = (db().prepare("SELECT id FROM posts_old WHERE post_id = ? ORDER BY id LIMIT 1").get(id) as { id: number }).id;
  assert.deepEqual(restoreVersion(id, version, "2026-01-01T00:12:00.000Z"), { ok: true });
  assert.equal(count(), 3, "the text it replaced is kept even inside the ten minutes");
});

test("Move to Trash doesn't ask first, because it can be undone", async () => {
  const cookie = await logIn();
  const id = createPost({ title: "Plain", body: "x", kind: "page" });
  const editor = await (await call(`/n-admin/posts/${id}`, { cookie })).text();
  assert.match(editor, /value="delete"/);
  assert.doesNotMatch(editor, /data-confirm="Delete/, "Trash can be undone, so it doesn't ask first");
});

const savesCount = () => saves().length;

test("every way of saving lists and texts keeps what it replaced, and only what changed", async () => {
  const cookie = await logIn();
  const revision = revisionOf(await (await call("/n-admin/lists/links", { cookie })).text());
  const saveLinks = (rev: string) => call("/n-admin/lists/links", { method: "POST", cookie, body: form({ revision: rev, "f.label": ["a"], "f.url": ["https://a.test"] }) });
  assert.equal((await saveLinks(revision)).status, 303);
  assert.equal(savesCount(), 1);
  assert.deepEqual(saves()[0].before, { texts: {}, lists: { links: [] } });
  assert.equal((await saveLinks(revisionOf(await (await call("/n-admin/lists/links", { cookie })).text()))).status, 303);
  assert.equal(savesCount(), 1, "the same list again keeps nothing");

  await call("/n-admin/lists/text", { method: "POST", cookie, body: form({ tagline: "" }) });
  assert.equal(savesCount(), 1, "an empty text that was never saved hasn't changed");
  await call("/n-admin/lists/text", { method: "POST", cookie, body: form({ tagline: "Hello" }) });
  assert.deepEqual(saves()[0].before, { texts: { tagline: "" }, lists: {} });
  await call("/n-admin/lists/text", { method: "POST", cookie, body: form({ tagline: "Hello" }) });
  assert.equal(savesCount(), 2);

  const { lists } = await (await call("/n-admin/edit/data", { cookie })).json();
  assert.equal((await edit(cookie, { texts: { tagline: "Hi" }, lists: { links: { revision: lists.links.revision, items: [{ label: "a", url: "https://a.test" }] } } })).status, 200);
  assert.deepEqual(saves()[0].before, { texts: { tagline: "Hello" }, lists: {} }, "the unchanged list is left out");
  const fresh = (await (await call("/n-admin/edit/data", { cookie })).json()).lists.links.revision;
  await edit(cookie, { texts: { tagline: "Hi" }, lists: { links: { revision: fresh, items: [{ label: "a", url: "https://a.test" }] } } });
  assert.equal(savesCount(), 3, "a click-to-edit save that changes nothing keeps nothing");
});

test("History lists saves newest first in plain words, and filters to one list or the texts", async () => {
  const cookie = await logIn();
  replaceItems("gallery", [{ image: "/n-admin/media/a.png", caption: "A" }, { image: "/n-admin/media/b.png", caption: "B" }, { image: "/n-admin/media/c.png", caption: "C" }]);
  replaceItems("gallery", [{ image: "/n-admin/media/a.png", caption: "A" }]);
  setTexts({ tagline: "Hello" });
  const page = await (await call("/n-admin/history", { cookie })).text();
  const order = ["Tagline", "Gallery: 2 photos removed", "Gallery: 3 photos added"].map((text) => page.indexOf(text));
  assert.ok(order.every((at) => at > 0), page);
  assert.deepEqual([...order].sort((a, b) => a - b), order, "newest first");

  const gallery = await (await call("/n-admin/history?only=gallery", { cookie })).text();
  assert.match(gallery, /Showing saves of Gallery/);
  assert.doesNotMatch(gallery, /<small>Tagline<\/small>/);
  const texts = await (await call("/n-admin/history?only=text", { cookie })).text();
  assert.match(texts, /<small>Tagline<\/small>/);
  assert.doesNotMatch(texts, /photos/);
  assert.match(await (await call("/n-admin/history?only=events", { cookie })).text(), /Nothing to go back to yet/);

  // Selecting the second save shows the three photos it replaced, as thumbnails.
  const id = saves()[1].id;
  const detail = await (await call(`/n-admin/history?s=${id}`, { cookie })).text();
  assert.equal(detail.match(/<img src="\/n-admin\/media\/[abc]\.png"/g)?.length, 3);
  assert.match(detail, /3 photos then, 1 now/);
  assert.equal(detail.match(/Not there now/g)?.length, 2);
});

test("Restore puts a save back, and the restore can itself be undone", async () => {
  const cookie = await logIn();
  setTexts({ tagline: "One" });
  replaceItems("links", [{ label: "a", url: "https://a.test" }]);
  const wrong = saveEditsLike({ tagline: "Oops" }, { links: [] });
  assert.equal(cms.text("tagline"), "Oops");

  const restored = await call(`/n-admin/history/${wrong}?only=links`, { method: "POST", cookie, body: form({}) });
  assert.equal(restored.status, 303);
  const location = restored.headers.get("location")!;
  assert.match(location, /^\/n-admin\/history\?only=links&restored=\d+&s=\d+$/);
  assert.equal(cms.text("tagline"), "One");
  assert.deepEqual(cms.list("links"), [{ label: "a", url: "https://a.test" }]);
  const after = await (await call(location, { cookie })).text();
  assert.match(after, /Restored\./);
  assert.match(after, /Nothing to restore: this is how it is now/);

  const undo = location.match(/restored=(\d+)/)![1];
  assert.equal((await call(`/n-admin/history/${undo}`, { method: "POST", cookie, body: form({}) })).status, 303);
  assert.equal(cms.text("tagline"), "Oops");
  assert.deepEqual(cms.list("links"), []);
  assert.equal(savesCount(), 5);

  assert.equal((await call("/n-admin/history/99999", { method: "POST", cookie, body: form({}) })).headers.get("location"), "/n-admin/history");
});

/** One click-to-edit save, returning its save id. */
function saveEditsLike(texts: Record<string, string>, lists: Record<string, Item[]>) {
  const fresh = Object.fromEntries(Object.entries(lists).map(([key, items]) => [key, { revision: listRevision(key), items }]));
  assert.equal(saveEdits(texts, fresh).ok, true);
  return saves()[0].id;
}

test("a save naming a list or text the site no longer declares is labelled, and never written back", async () => {
  const cookie = await logIn();
  db().prepare("INSERT INTO saves (saved_at, before) VALUES (?, ?)").run("2026-01-01T00:00:00.000Z", JSON.stringify({ texts: { motto: "Old motto", tagline: "Kept" }, lists: { menu: [{ label: "Home" }] } }));
  const id = saves()[0].id;
  const page = await (await call("/n-admin/history", { cookie })).text();
  assert.match(page, /menu \(no longer on the site\)/);
  assert.match(page, /motto \(no longer on the site\)/);
  assert.match(page, /This list is no longer on the site/);
  assert.equal((await call(`/n-admin/history/${id}`, { method: "POST", cookie, body: form({}) })).status, 303);
  assert.equal(cms.text("tagline"), "Kept");
  assert.equal(cms.texts().motto, undefined);
  assert.deepEqual(cms.list("menu" as "links"), []);

  db().exec("DELETE FROM saves");
  db().prepare("INSERT INTO saves (saved_at, before) VALUES (?, ?)").run("2026-01-01T00:00:00.000Z", JSON.stringify({ texts: { motto: "x" }, lists: {} }));
  assert.match(await (await call("/n-admin/history", { cookie })).text(), /Nothing to restore/);
});

test("History needs a login, writes need the site's origin, and its links show only when there is something to go back to", async () => {
  assert.equal((await call("/n-admin/history")).status, 303);
  setTexts({ tagline: "x" });
  const id = saves()[0].id;
  assert.equal((await call(`/n-admin/history/${id}`, { method: "POST", body: form({}) })).status, 303);
  const cookie = await logIn();
  assert.equal((await call(`/n-admin/history/${id}`, { method: "POST", cookie, origin: "https://evil.test", body: form({}) })).status, 403);
  assert.equal((await call(`/n-admin/history/${id}`, { cookie })).status, 404, "restoring is a POST");
  assert.equal(cms.text("tagline"), "x");

  assert.doesNotMatch(await (await call("/n-admin/lists/gallery", { cookie })).text(), /history\?only=gallery/);
  assert.match(await (await call("/n-admin/lists/text", { cookie })).text(), /href="\/n-admin\/history\?only=text"/);
  assert.match(await (await call("/n-admin/edit", { cookie })).text(), /<a class="button quiet" href="\/n-admin\/history" data-edit-history="true">/);
  db().exec("DELETE FROM saves");
  assert.match(await (await call("/n-admin/edit", { cookie })).text(), /data-edit-history="true" hidden/);
});

// Media

const hex = (n: number) => n.toString(16).padStart(32, "0");
/** An upload as media.ts writes it, added `days` ago; a wide one gets its thumbnail. */
function stored(n: number, { days = 2, sized = true } = {}) {
  const media = join(directory, "media");
  mkdirSync(media, { recursive: true });
  const name = sized ? `${hex(n)}-2000x1000.png` : `${hex(n)}.png`;
  const at = new Date(Date.now() - days * 86_400_000 - n * 1000);
  for (const file of sized ? [name, name.replace(".png", "-t.png")] : [name]) {
    writeFileSync(join(media, file), png(n));
    utimesSync(join(media, file), at, at);
  }
  return name;
}
const exists = (name: string) => existsSync(join(directory, "media", name));
function freshMedia() {
  rmSync(join(directory, "media"), { recursive: true, force: true });
  db().exec("DELETE FROM posts_trash");
}

test("Media lists every upload newest first, with its size and where it is used", async () => {
  freshMedia();
  const cookie = await logIn();
  const [inPost, inPage, inList, inText, thumbOnly, inTrash, inVersion, inHistory, unused] = [1, 2, 3, 4, 5, 6, 7, 8, 9].map((n) => stored(n));
  const old = stored(10, { sized: false });
  const url = (name: string) => `/n-admin/media/${name}`;

  const post = createPost({ title: "My Stack", body: `Hi ![](${url(inPost)})` });
  createPost({ title: "About", kind: "page", body: `<img src="https://site.test${url(inPage)}">` });
  replaceItems("gallery", [{ image: url(inList), caption: "" }]);
  setTexts({ tagline: `See ${url(inText)}` });
  createPost({ title: "Small", body: `![](${url(thumbOnly.replace(".png", "-t.png"))})` });
  trashPost(createPost({ title: "Gone", body: `![](${url(inTrash)})` }));
  const versioned = createPost({ title: "V", body: `![](${url(inVersion)})`, published_at: "2020-01-01T00:00:00.000Z" });
  db().prepare("UPDATE posts SET body = 'new' WHERE id = ?").run(versioned);
  replaceItems("events", [{ title: "Show", date: "2030-01-01", url: url(inHistory) }]);
  replaceItems("events", []);

  const html = await (await call("/n-admin/media", { cookie })).text();
  assert.match(html, />Media<\/a>/, "in the menu");
  const order = [...html.matchAll(/name="name" value="([^"]+)"|data-copy="\/n-admin\/media\/([^"]+)"/g)].map((m) => m[1] ?? m[2]);
  assert.deepEqual([...new Set(order)], [inPost, inPage, inList, inText, thumbOnly, inTrash, inVersion, inHistory, unused, old]);
  assert.doesNotMatch(html, /-t\.png" data-copy|data-copy="[^"]*-t\.png/, "thumbnails are not listed apart");
  assert.match(html, /2000 × 1000, 1 KB/);
  assert.match(html, new RegExp(`<a href="/n-admin/posts/${post}">In the post My Stack</a>`));
  assert.match(html, /In the page About<\/a>/);
  assert.match(html, /<a href="\/n-admin\/lists\/gallery">In Gallery<\/a>/);
  assert.match(html, /<a href="\/n-admin\/edit">In the text Tagline<\/a>/, "texts edited on the page link there");
  assert.match(html, /In the post Small<\/a>/, "a thumbnail counts as its image");
  assert.match(html, /Kept for undo in a post in the Trash\./);
  assert.match(html, /Kept for undo in an old version of a post\./);
  assert.match(html, /Kept for undo in list and text history\./);
  // Only images not in use can be deleted.
  const deletable = [...html.matchAll(/name="name" value="([^"]+)"/g)].map((m) => m[1]);
  assert.deepEqual(deletable, [inTrash, inVersion, inHistory, unused, old]);
  assert.match(html, /only kept for undo in a post in the Trash, so restoring that would show a broken image/);

  const filtered = await (await call("/n-admin/media?unused=1", { cookie })).text();
  assert.match(filtered, /Unused \(5\)/);
  assert.doesNotMatch(filtered, new RegExp(inPost));
  assert.match(filtered, /Delete all unused \(2\)/, "kept-for-undo images are left out");
  assert.match(filtered, /Delete 2 unused images for good\?/);
});

test("the image chooser gets the newest images, and an empty library shows nothing to choose", async () => {
  freshMedia();
  const cookie = await logIn();
  assert.deepEqual(await (await call("/n-admin/media", { cookie, headers: { "x-cms": "1" } })).json(), { images: [] });
  const name = stored(1);
  const { images } = await (await call("/n-admin/media", { cookie, headers: { "x-cms": "1" } })).json();
  assert.deepEqual(images, [{ url: `/n-admin/media/${name}`, thumb: `/n-admin/media/${name.replace(".png", "-t.png")}` }]);
});

test("Media pages through many images, 60 at a time", async () => {
  freshMedia();
  const cookie = await logIn();
  for (let n = 1; n <= 61; n++) stored(n, { sized: false });
  const first = await (await call("/n-admin/media", { cookie })).text();
  assert.equal(first.match(/data-copy=/g)?.length, 60);
  assert.match(first, /Page 1 of 2/);
  assert.match(first, /href="\/n-admin\/media\?page=2">Older/);
  const second = await (await call("/n-admin/media?page=2", { cookie })).text();
  assert.equal(second.match(/data-copy=/g)?.length, 1);
  assert.match(second, new RegExp(hex(61)));
});

test("deleting an image takes its thumbnail, and an image in use is never deleted", async () => {
  freshMedia();
  const cookie = await logIn();
  const used = stored(1);
  const kept = stored(2);
  const spare = stored(3);
  replaceItems("gallery", [{ image: `/n-admin/media/${used}`, caption: "" }]);
  trashPost(createPost({ title: "Gone", body: `![](/n-admin/media/${kept})` }));
  const remove = (name: string) => call("/n-admin/media/delete", { method: "POST", cookie, body: form({ name }) });

  const refused = await remove(used);
  assert.equal(refused.status, 409);
  assert.match(await refused.text(), /That image is in use/);
  assert.ok(exists(used) && exists(used.replace(".png", "-t.png")));

  const done = await remove(spare);
  assert.equal(done.status, 303);
  assert.equal(done.headers.get("location"), "/n-admin/media?deleted=1");
  assert.ok(!exists(spare) && !exists(spare.replace(".png", "-t.png")), "the thumbnail goes too");
  assert.match(await (await call("/n-admin/media?deleted=1", { cookie })).text(), /1 image deleted\./);

  assert.equal((await remove(kept)).status, 303, "kept only for undo can go, after its warning");
  assert.ok(!exists(kept));
  assert.equal((await remove(kept)).status, 404, "already gone");
});

test("Delete all unused leaves images in use, kept for undo, or added in the last day", async () => {
  freshMedia();
  const cookie = await logIn();
  const used = stored(1);
  const kept = stored(2);
  const fresh = stored(3, { days: 0 });
  const unused = [stored(4), stored(5, { sized: false })];
  createPost({ title: "Live", body: `![](/n-admin/media/${used})` });
  db().prepare("INSERT INTO posts_old (post_id, title, body, saved_at) VALUES (0, 'x', ?, '2020-01-01T00:00:00.000Z')").run(`![](/n-admin/media/${kept})`);

  const response = await call("/n-admin/media/delete-unused", { method: "POST", cookie, body: form({}) });
  assert.equal(response.headers.get("location"), "/n-admin/media?unused=1&deleted=2");
  assert.ok(exists(used) && exists(kept) && exists(fresh));
  for (const name of unused) assert.ok(!exists(name));
  assert.deepEqual(readdirSync(join(directory, "media")).sort(), [used, used.replace(".png", "-t.png"), kept, kept.replace(".png", "-t.png"), fresh, fresh.replace(".png", "-t.png")].sort());
});

test("only names media.ts could have written are deleted; nothing else in the data folder is reachable", async () => {
  freshMedia();
  const cookie = await logIn();
  const name = stored(1);
  writeFileSync(join(directory, "media", "notes.txt"), "x");
  for (const bad of ["../site.db", "..%2Fsite.db", "/etc/passwd", `../media/${name}`, `${name}/..`, name.replace(".png", "-t.png"), name.toUpperCase(), `${hex(1)}.svg`, "notes.txt", "", `${name}\0`]) {
    const response = await call("/n-admin/media/delete", { method: "POST", cookie, body: form({ name: bad }) });
    assert.equal(response.status, 404, bad);
  }
  assert.ok(existsSync(join(directory, "site.db")));
  assert.ok(exists(name) && exists(name.replace(".png", "-t.png")) && exists("notes.txt"));
  assert.doesNotMatch(await (await call("/n-admin/media", { cookie })).text(), /notes\.txt/, "only uploads are listed");
});

test("Media needs a login, and its writes need the site's origin", async () => {
  freshMedia();
  const name = stored(1);
  assert.equal((await call("/n-admin/media")).status, 303);
  assert.equal((await call("/n-admin/media", { headers: { "x-cms": "1" } })).status, 401);
  assert.equal((await call("/n-admin/media/delete", { method: "POST", body: form({ name }) })).status, 303);
  assert.equal((await call("/n-admin/media/delete-unused", { method: "POST", body: form({}) })).status, 303);
  const cookie = await logIn();
  assert.equal((await call("/n-admin/media/delete", { method: "POST", cookie, origin: "https://evil.test", body: form({ name }) })).status, 403);
  assert.equal((await call("/n-admin/media/delete-unused", { method: "POST", cookie, origin: null, body: form({}) })).status, 403);
  assert.equal((await call("/n-admin/media/delete")).status, 404, "a GET only ever serves an image");
  assert.ok(exists(name));
  assert.equal((await call(`/n-admin/media/${name}`)).status, 200, "images stay public");
});

// People and passwords

let lastAddress = 0;
const freshAddress = () => `10.1.${Math.floor(++lastAddress / 250)}.${lastAddress % 250}`;
const logInAs = (fields: Record<string, string>, address = freshAddress()) =>
  call("/n-admin/login", { method: "POST", body: form(fields), headers: { "x-forwarded-for": address } });
const cookieOf = (response: Response) => {
  assert.equal(response.status, 303);
  return response.headers.get("set-cookie")!.split(";")[0];
};
const shownPassword = (html: string) => html.match(/<dt>Password<\/dt><dd><code>([^<]+)<\/code>/)?.[1];
const personId = (name: string) => (db().prepare("SELECT id FROM people WHERE name = ?").get(name) as { id: number } | undefined)?.id;
const signedIn = async (cookie: string) => (await call("/n-admin/account", { cookie })).status === 200;

async function addPerson(owner: string, name: string) {
  const response = await call("/n-admin/people", { method: "POST", cookie: owner, body: form({ name }) });
  assert.equal(response.status, 200);
  const password = shownPassword(await response.text());
  assert.ok(password);
  return password;
}

function changePassword(cookie: string, current: string, password: string, repeat = password) {
  return call("/n-admin/account", { method: "POST", cookie, body: form({ current, password, repeat }), headers: { "x-forwarded-for": freshAddress() } });
}

/** Runs with another CMS_PASSWORD_HASH, as if the owner had run `neutron-cms password` again. */
async function withEnvPassword(password: string, work: () => Promise<void>) {
  const saved = process.env.CMS_PASSWORD_HASH;
  process.env.CMS_PASSWORD_HASH = await hashPassword(password);
  try {
    await work();
  } finally {
    process.env.CMS_PASSWORD_HASH = saved;
  }
}

test("with nobody else, the login asks only for the password, and the owner sees People and Account", async () => {
  const login = await (await call("/n-admin/login")).text();
  assert.doesNotMatch(login, /name="name"/);
  assert.match(login, /name="password"/);
  assert.match(await (await logInAs({ password: "wrong" })).text(), /Wrong password\./);
  const owner = cookieOf(await logInAs({ password: PASSWORD }));
  const html = await (await call("/n-admin", { cookie: owner })).text();
  assert.match(html, />People<\/a>/);
  assert.match(html, />Account<\/a>/);
  assert.match(await (await call("/n-admin/account", { cookie: owner })).text(), /You are the owner[\s\S]*npx neutron-cms password/);
});

test("the owner adds a person, sees the password once, and the login then asks for a name", async () => {
  const owner = await logIn();
  const password = await addPerson(owner, "  Sam.Smith@Example.com ");
  assert.match(password, /^[A-Za-z0-9_-]{32}$/);
  assert.ok(personId("sam.smith@example.com"), "stored trimmed and in lower case");
  const list = await (await call("/n-admin/people", { cookie: owner })).text();
  assert.match(list, /sam\.smith@example\.com/);
  assert.ok(!list.includes(password), "shown once");
  assert.equal(db().prepare("SELECT 1 FROM people WHERE password_hash LIKE ?").get(`%${password}%`), undefined);

  assert.match(await (await call("/n-admin/login")).text(), /name="name"[\s\S]*Leave empty if you are the owner/);
  const sam = cookieOf(await logInAs({ name: "SAM.smith@example.com", password }));
  const html = await (await call("/n-admin", { cookie: sam })).text();
  assert.match(html, />Account<\/a>/);
  assert.doesNotMatch(html, />People<\/a>/);
  assert.match(await (await call("/n-admin/account", { cookie: sam })).text(), /logged in as <strong>sam\.smith@example\.com<\/strong>/);
  // The owner's habit doesn't change: no name, the owner's password.
  assert.equal(await signedIn(cookieOf(await logInAs({ name: "", password: PASSWORD }))), true);
  assert.equal(await signedIn(cookieOf(await logInAs({ password: PASSWORD }))), true);
  assert.equal((await logInAs({ name: "sam.smith@example.com", password: PASSWORD })).status, 200, "a person's name with the owner's password is wrong");
});

test("names have one simple shape and are unique however they are typed", async () => {
  const owner = await logIn();
  for (const bad of ["", "   ", "<b>x</b>", "a b", "-sam", "s\u0430m", "x".repeat(101), "a@b", "sam@@x.com"]) {
    const response = await call("/n-admin/people", { method: "POST", cookie: owner, body: form({ name: bad }) });
    assert.equal(response.status, 422, bad);
    assert.match(await response.text(), /Use letters, numbers/);
  }
  await addPerson(owner, "sam_1");
  const again = await call("/n-admin/people", { method: "POST", cookie: owner, body: form({ name: " SAM_1" }) });
  assert.equal(again.status, 409);
  assert.match(await again.text(), /Someone already has that name/);
  assert.equal((db().prepare("SELECT count(*) AS n FROM people").get() as { n: number }).n, 1);
});

test("an unknown name and a wrong password get the same reply and the same work", async () => {
  const owner = await logIn();
  const password = await addPerson(owner, "sam");
  const reply = async (fields: Record<string, string>) => {
    const started = performance.now();
    const response = await logInAs(fields);
    const html = (await response.text()).replace(/ value="[^"]*"/g, "");
    return { status: response.status, html, ms: performance.now() - started };
  };
  const wrong = await reply({ name: "sam", password: `${password}x` });
  const unknown = await reply({ name: "nobody", password });
  const malformed = await reply({ name: "no body!", password });
  assert.match(wrong.html, /Wrong name or password\./);
  for (const other of [unknown, malformed]) {
    assert.equal(other.status, wrong.status);
    assert.equal(other.html, wrong.html);
    // One password check either way: without it an unknown name would answer in about a millisecond.
    assert.ok(other.ms > wrong.ms * 0.4, `${other.ms} ms against ${wrong.ms} ms`);
  }
  assert.match(await (await logInAs({ password: "wrong" })).text(), /Wrong name or password\./, "the owner's wrong password reads the same");
});

test("the login throttle still applies with names", async () => {
  const owner = await logIn();
  const password = await addPerson(owner, "sam");
  const address = "10.8.8.8";
  for (let index = 0; index < 5; index++) await logInAs({ name: index % 2 ? "nobody" : "sam", password: "wrong" }, address);
  const blocked = await logInAs({ name: "sam", password }, address);
  assert.equal(blocked.status, 200);
  assert.match(await blocked.text(), /Too many attempts/);
});

test("only the owner manages people, checked on the server", async () => {
  const owner = await logIn();
  await addPerson(owner, "sam");
  const alex = cookieOf(await logInAs({ name: "alex", password: await addPerson(owner, "alex") }));
  assert.doesNotMatch(await (await call("/n-admin", { cookie: alex })).text(), />People<\/a>/);
  const self = personId("alex")!;
  for (const [path, body] of [
    ["/n-admin/people", undefined],
    ["/n-admin/people", { name: "mallory" }],
    [`/n-admin/people/${personId("sam")}/remove`, {}],
    [`/n-admin/people/${personId("sam")}/reset`, {}],
    [`/n-admin/people/${self}/remove`, {}],
  ] as const) {
    const response = body ? await call(path, { method: "POST", cookie: alex, body: form(body) }) : await call(path, { cookie: alex });
    assert.equal(response.status, 404, path);
    assert.ok(!shownPassword(await response.text()));
  }
  assert.equal((await call("/n-admin/people", { method: "POST", cookie: owner, origin: "https://evil.test", body: form({ name: "mallory" }) })).status, 403);
  assert.equal((await call(`/n-admin/people/${self}/remove`, { method: "POST", cookie: owner, origin: null, body: form({}) })).status, 403);
  assert.equal((await call("/n-admin/account", { method: "POST", cookie: alex, origin: "https://evil.test", body: form({ current: "x", password: "y".repeat(12), repeat: "y".repeat(12) }) })).status, 403);
  assert.equal((await call(`/n-admin/people/${self}/remove`, { cookie: owner })).status, 404, "actions are POST only");
  assert.equal((await call("/n-admin/people", { method: "POST", body: form({ name: "mallory" }) })).status, 303, "signed out");
  assert.equal(personId("mallory"), undefined);
  assert.ok(personId("sam") && personId("alex"));
  assert.equal(await signedIn(alex), true, "and refusing them changed nothing");
});

test("removing a person ends their sessions at once and their password stops working", async () => {
  const owner = await logIn();
  const password = await addPerson(owner, "sam");
  const phone = cookieOf(await logInAs({ name: "sam", password }));
  const laptop = cookieOf(await logInAs({ name: "sam", password }));
  const id = personId("sam")!;
  const response = await call(`/n-admin/people/${id}/remove`, { method: "POST", cookie: owner, body: form({}) });
  assert.equal(response.headers.get("location"), "/n-admin/people?removed=1");
  assert.equal(await signedIn(phone), false);
  assert.equal(await signedIn(laptop), false);
  assert.equal((db().prepare("SELECT count(*) AS n FROM sessions WHERE person_id = ?").get(id) as { n: number }).n, 0);
  assert.equal((await logInAs({ name: "sam", password })).status, 200);
  // Someone new with the same name is a new account; nothing of the old one carries over.
  const again = await addPerson(owner, "sam");
  assert.notEqual(personId("sam"), id);
  assert.equal(await signedIn(phone), false);
  assert.equal(await signedIn(cookieOf(await logInAs({ name: "sam", password: again }))), true);
  assert.equal(await signedIn(owner), true);
});

test("a new password from the owner ends that person's sessions, and only the new one works", async () => {
  const owner = await logIn();
  const old = await addPerson(owner, "sam");
  const alex = cookieOf(await logInAs({ name: "alex", password: await addPerson(owner, "alex") }));
  const sam = cookieOf(await logInAs({ name: "sam", password: old }));
  const response = await call(`/n-admin/people/${personId("sam")}/reset`, { method: "POST", cookie: owner, body: form({}) });
  const html = await response.text();
  assert.match(html, /sam has a new password and is logged out/);
  const fresh = shownPassword(html)!;
  assert.notEqual(fresh, old);
  assert.equal(await signedIn(sam), false);
  assert.equal(await signedIn(alex), true, "only that person");
  assert.equal((await logInAs({ name: "sam", password: old })).status, 200);
  assert.equal(await signedIn(cookieOf(await logInAs({ name: "sam", password: fresh }))), true);
  assert.equal((await call("/n-admin/people/99999/reset", { method: "POST", cookie: owner, body: form({}) })).status, 303);
});

test("changing your own password: its rules, a wrong current password, and only this session kept", async () => {
  const owner = await logIn();
  const old = await addPerson(owner, "sam");
  const here = cookieOf(await logInAs({ name: "sam", password: old }));
  const elsewhere = cookieOf(await logInAs({ name: "sam", password: old }));
  const next = "correct horse battery";
  const refused = async (response: Response, message: RegExp) => {
    assert.match(await response.text(), message);
    assert.equal(await signedIn(elsewhere), true);
  };
  await refused(await changePassword(here, "", next), /Type your current password/);
  await refused(await changePassword(here, old, "short"), /at least 12 characters/);
  await refused(await changePassword(here, old, "x".repeat(201)), /at most 200 characters/);
  await refused(await changePassword(here, old, next, `${next}!`), /don.{1,6}t match/);
  await refused(await changePassword(here, `${old}x`, next), /current password is wrong/);

  const changed = await changePassword(here, old, next);
  assert.equal(changed.headers.get("location"), "/n-admin/account?changed=1");
  assert.equal(changed.headers.get("set-cookie"), null, "this session carries on as it is");
  assert.match(await (await call("/n-admin/account?changed=1", { cookie: here })).text(), /Password changed/);
  assert.equal(await signedIn(elsewhere), false);
  assert.equal(await signedIn(owner), true, "other accounts are untouched");
  assert.equal((await logInAs({ name: "sam", password: old })).status, 200);
  assert.equal(await signedIn(cookieOf(await logInAs({ name: "sam", password: next }))), true);
});

test("wrong current passwords are throttled, from any address", async () => {
  const owner = await logIn();
  const password = await addPerson(owner, "sam");
  const sam = cookieOf(await logInAs({ name: "sam", password }));
  for (let index = 0; index < 5; index++) assert.match(await (await changePassword(sam, "wrong", "a new long password")).text(), /current password is wrong/);
  const blocked = await changePassword(sam, password, "a new long password");
  assert.equal(blocked.status, 429);
  assert.match(await blocked.text(), /Too many attempts/);
  assert.equal(await signedIn(cookieOf(await logInAs({ name: "sam", password }))), true, "the password didn't change");
});

test("the owner's password set here works until CMS_PASSWORD_HASH changes, then it is removed", async () => {
  const owner = await logIn();
  const elsewhere = await logIn();
  const next = "the owner's own new password";
  assert.equal((await changePassword(owner, PASSWORD, next)).status, 303);
  assert.equal(await signedIn(owner), true);
  assert.equal(await signedIn(elsewhere), false);
  assert.equal((await logInAs({ password: PASSWORD })).status, 200, "the environment's password stops working");
  assert.equal(await signedIn(cookieOf(await logInAs({ password: next }))), true);
  const stored = db().prepare("SELECT password_hash FROM owner_password").get() as { password_hash: string };
  assert.ok(!stored.password_hash.includes(next) && stored.password_hash.startsWith("scrypt."));

  // Forgot it: the owner runs `neutron-cms password` and sets the new hash.
  await withEnvPassword("from the command line", async () => {
    assert.equal(await signedIn(owner), false, "a new CMS_PASSWORD_HASH signs the owner out");
    assert.equal((await logInAs({ password: next })).status, 200, "the password set here no longer counts");
    assert.equal(db().prepare("SELECT 1 FROM owner_password").get(), undefined, "and is removed");
    assert.equal(await signedIn(cookieOf(await logInAs({ password: "from the command line" }))), true);
  });
  // Even with the old hash back, the removed password stays gone.
  assert.equal((await logInAs({ password: next })).status, 200);
  assert.equal(await signedIn(cookieOf(await logInAs({ password: PASSWORD }))), true);
});

test("a new CMS_PASSWORD_HASH signs out people too", async () => {
  const owner = await logIn();
  const password = await addPerson(owner, "sam");
  const sam = cookieOf(await logInAs({ name: "sam", password }));
  await withEnvPassword("another", async () => {
    assert.equal(await signedIn(sam), false);
    assert.equal(await signedIn(cookieOf(await logInAs({ name: "sam", password }))), true, "their password still works");
  });
});

test("draft preview links end when a password changes, a person goes or the command line sets a new one", async () => {
  const owner = await logIn();
  const password = await addPerson(owner, "sam");
  const sam = cookieOf(await logInAs({ name: "sam", password }));
  let link = previewToken("page", "draft");
  assert.equal(isPreview("page", "draft", link), true);
  assert.equal(isPreview("page", "other", link), false);

  assert.equal((await changePassword(sam, password, "sam's own new password")).status, 303);
  assert.equal(isPreview("page", "draft", link), false);
  link = previewToken("page", "draft");
  assert.equal(isPreview("page", "draft", link), true, "a new link works");

  await call(`/n-admin/people/${personId("sam")}/remove`, { method: "POST", cookie: owner, body: form({}) });
  assert.equal(isPreview("page", "draft", link), false);
  link = previewToken("page", "draft");
  await withEnvPassword("another", async () => {
    assert.equal(isPreview("page", "draft", link), false);
  });
  assert.equal(isPreview("page", "draft", link), true, "the key is stable otherwise, so every instance agrees");
});

test("a session made before people existed is the owner's", async () => {
  const token = "made-by-the-previous-release";
  const sha = (value: string) => createHash("sha256").update(value).digest("hex");
  db().prepare("INSERT INTO sessions (token_hash, password_tag, expires_at) VALUES (?, ?, ?)").run(sha(token), sha(process.env.CMS_PASSWORD_HASH!), new Date(Date.now() + 60_000).toISOString());
  const cookie = `n_session=${token}`;
  assert.equal((await call("/n-admin/people", { cookie })).status, 200);
  // A forged tag, or one row claiming to be a person who doesn't exist, is nobody.
  db().prepare("INSERT INTO sessions (token_hash, password_tag, expires_at, person_id) VALUES (?, ?, ?, 4242)").run(sha("ghost"), sha(process.env.CMS_PASSWORD_HASH!), new Date(Date.now() + 60_000).toISOString());
  assert.equal(await signedIn("n_session=ghost"), false);
});

const rowsOf = (html: string) => [...html.matchAll(/<td><a href="\/n-admin\/posts\/\d+">([^<]*)<\/a>/g)].map((match) => match[1]);

test("Posts offers a search past ten, by title and text, with LIKE wildcards taken as typed", async () => {
  const cookie = await logIn();
  for (let n = 1; n <= 10; n++) createPost({ title: `Post ${n}`, body: "plain words" });
  assert.doesNotMatch(await (await call("/n-admin", { cookie })).text(), /name="q"/, "ten rows need no search");
  createPost({ title: "Spring notes", body: "On 50% off and snake_case" });
  createPost({ title: "Other", body: "mentions SPRING in the text" });
  const all = await (await call("/n-admin", { cookie })).text();
  assert.match(all, /<form class="search" method="get" action="\/n-admin" role="search">/);

  assert.deepEqual(rowsOf(await (await call("/n-admin?q=spring", { cookie })).text()).sort(), ["Other", "Spring notes"]);
  assert.deepEqual(rowsOf(await (await call("/n-admin?q=50%25", { cookie })).text()), ["Spring notes"]);
  assert.deepEqual(rowsOf(await (await call("/n-admin?q=e_c", { cookie })).text()), ["Spring notes"]);
  assert.deepEqual(rowsOf(await (await call("/n-admin?q=%25", { cookie })).text()), ["Spring notes"]);
  const none = await (await call("/n-admin?q=_", { cookie })).text();
  assert.deepEqual(rowsOf(none), ["Spring notes"], "an underscore matches only an underscore");
  const nothing = await (await call(`/n-admin?q=${encodeURIComponent("<b>x</b>")}`, { cookie })).text();
  assert.match(nothing, /Nothing matches &quot;&lt;b>x&lt;\/b>&quot;\./);
  assert.match(nothing, /value="&lt;b>x&lt;\/b>"/);
  assert.match(nothing, /href="\/n-admin">Show all/);
});

test("a search shows with few rows while a query is active, and keeps the Undo banner and Trash link", async () => {
  const cookie = await logIn();
  const id = createPost({ title: "Gone", body: "x" });
  createPost({ title: "Kept", body: "x" });
  const trashId = trashPost(id);
  const page = await (await call(`/n-admin?q=kept&deleted=${trashId}`, { cookie })).text();
  assert.match(page, /name="q"/);
  assert.deepEqual(rowsOf(page), ["Kept"]);
  assert.match(page, /&quot;Gone&quot; moved to Trash\./);
  assert.match(page, new RegExp(`action="/n-admin/trash/${trashId}"`));
  assert.match(page, /href="\/n-admin\/trash">Trash \(1\)/);
  assert.doesNotMatch(await (await call("/n-admin", { cookie })).text(), /name="q"/);
});

test("Posts and Pages page through 50 at a time, keeping the query", async () => {
  const cookie = await logIn();
  for (let n = 1; n <= 51; n++) createPost({ title: `Note ${n}`, published_at: new Date(Date.UTC(2026, 0, 1, 0, n)).toISOString() });
  createPost({ title: "Other", published_at: "2025-01-01T00:00:00.000Z" });
  const first = await (await call("/n-admin", { cookie })).text();
  assert.equal(rowsOf(first).length, 50);
  assert.match(first, /Page 1 of 2/);
  assert.match(first, /href="\/n-admin\?page=2">Older/);
  const found = await (await call("/n-admin?q=note", { cookie })).text();
  assert.equal(rowsOf(found).length, 50);
  assert.match(found, /href="\/n-admin\?q=note&amp;page=2">Older/);
  const second = await (await call("/n-admin?q=note&page=2", { cookie })).text();
  assert.deepEqual(rowsOf(second), ["Note 1"]);
  assert.match(second, /href="\/n-admin\?q=note">Newer/);
  assert.deepEqual(rowsOf(await (await call("/n-admin?q=note&page=9", { cookie })).text()), ["Note 1"], "past the end shows the last page");

  for (let n = 1; n <= 12; n++) createPost({ title: `Page ${n}`, kind: "page" });
  const pages = await (await call("/n-admin/pages?q=page%2012", { cookie })).text();
  assert.match(pages, /action="\/n-admin\/pages"/);
  assert.deepEqual(rowsOf(pages), ["Page 12"]);
});

test("the Text page wires its image fields to the upload script", async () => {
  const image = defineCms({ texts: { hero: "image" }, lists: {} });
  const cookie = await logIn();
  const request = new Request(`${SITE}/n-admin/lists/text`, { headers: { cookie } });
  const page = await (await image.GET({ request, params: { path: "lists/text" } })).text();
  assert.match(page, /<form class="text-form" method="post">[\s\S]*class="image-field"[\s\S]*data-row="upload"[\s\S]*data-state/);
  const script = await (await call("/n-admin/admin.js", { cookie })).text();
  assert.match(script, /\$\$\("\.text-form"\)[\s\S]*button\[data-row=upload\]/);
});

test("restoring a save writes only the fields the list declares now", async () => {
  const cookie = await logIn();
  db().prepare("INSERT INTO saves (saved_at, before) VALUES (?, ?)").run(
    "2026-01-01T00:00:00.000Z",
    JSON.stringify({ texts: {}, lists: { links: [{ label: "Old", url: "https://old.test", icon: "star" }, { label: "No url" }] } }),
  );
  const id = saves()[0].id;
  assert.match(await (await call("/n-admin/history", { cookie })).text(), />Restore<\/button>/);
  assert.equal((await call(`/n-admin/history/${id}`, { method: "POST", cookie, body: form({}) })).status, 303);
  assert.deepEqual(cms.list("links"), [{ label: "Old", url: "https://old.test" }, { label: "No url", url: "" }]);
  assert.doesNotMatch(await (await call(`/n-admin/history?s=${id}`, { cookie })).text(), />Restore<\/button>/, "restored as the list declares it, there is nothing left to restore");
});
