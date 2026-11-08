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
  for (const table of ["posts", "items", "texts", "posts_old"]) db().exec(`DELETE FROM ${table}`);
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
