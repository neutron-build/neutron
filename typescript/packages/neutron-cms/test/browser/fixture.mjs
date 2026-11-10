// A tiny site around the built handler: node:http turns each request into a Request, the admin
// answers under /n-admin, and a few plain pages show CMS content with the click-to-edit attributes.
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { deflateSync, crc32 } from "node:zlib";
import { hashPassword } from "../../dist/auth.js";
import { defineCms, renderMarkdown } from "../../dist/index.js";
import { closeDb, createPost, db, replaceItems, savePost, setTexts } from "../../dist/store.js";

export const OWNER_PASSWORD = "owner-password-for-tests";

export const cms = defineCms({
  pages: true,
  designs: { portfolio: "Portfolio" },
  reservedSlugs: ["about"],
  texts: { tagline: "text", intro: "textarea", hero: { type: "image", label: "Hero image" } },
  onPage: ["team", "press", "shots"],
  lists: {
    links: { fields: { label: { type: "text", required: true }, url: { type: "url", required: true } } },
    gallery: { fields: { image: { type: "image", required: true }, caption: "text" } },
    events: { sortBy: "date", fields: { title: { type: "text", required: true }, date: { type: "date", required: true } } },
    team: { fields: { name: "text", photo: "image", site: "url" } },
    press: { fields: { outlet: "text", link: "url" } },
    shots: { fields: { image: "image", caption: "text" } },
  },
});

const escape = (text) => String(text ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);

const site = (title, body) => new Response(
  `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${escape(title)}</title>`
  + `<style>body{font-family:sans-serif;max-width:720px;margin:0 auto;padding:24px}img{max-width:100%;height:auto}.team{display:flex;gap:16px;list-style:none;padding:0}.team li{width:150px}.team img{width:150px;height:150px;object-fit:cover}</style>${body}`,
  { headers: { "Content-Type": "text/html; charset=utf-8", "X-Frame-Options": "SAMEORIGIN" } },
);

/** The site's own pages. */
function sitePage(request) {
  const url = new URL(request.url);
  const path = url.pathname.replace(/\/$/, "") || "/";
  if (path === "/") {
    const texts = cms.texts();
    const team = cms.list("team");
    return site("Home", `
      <h1 data-cms-text="tagline">${escape(texts.tagline)}</h1>
      <img data-cms-text="hero" ${texts.hero ? `src="${escape(texts.hero)}"` : ""} alt="Hero" width="200">
      <ul class="team">${team.map((person, index) => `
        <li data-cms-item="team.${index}">
          <a href="${escape(person.site || "#")}"><img data-cms="photo" ${person.photo ? `src="${escape(person.photo)}"` : ""} alt=""></a>
          <h3 data-cms="name">${escape(person.name)}</h3>
        </li>`).join("")}
      </ul>
      <ul class="press" data-cms-list="press">${cms.list("press").map((entry, index) => `<li data-cms-item="press.${index}"><span data-cms="outlet">${escape(entry.outlet)}</span></li>`).join("")}</ul>
      <div class="shots" data-cms-list="shots">${cms.list("shots").map((shot, index) => `<figure data-cms-item="shots.${index}"><img data-cms="image" src="${escape(shot.image)}" alt="" width="120"></figure>`).join("")}</div>
      <p><a href="/portfolio">Portfolio</a> <a href="https://example.com/">Elsewhere</a></p>`);
  }
  if (path === "/portfolio") {
    const page = cms.page("portfolio", request);
    if (!page || page.layout !== "portfolio") return new Response("Not found", { status: 404 });
    return site(page.title, `<h1>${escape(page.title)}</h1><p data-cms-text="intro">${escape(cms.text("intro"))}</p>`);
  }
  const post = cms.post(path.slice(1));
  if (post) return site(post.title, `<h1>${escape(post.title)}</h1><article>${renderMarkdown(post.body)}</article>`);
  const page = cms.page(path.slice(1), request);
  if (page) return site(page.title, `<h1>${escape(page.title)}</h1><article>${renderMarkdown(page.body)}</article>`);
  return new Response("Not found", { status: 404 });
}

async function answer(request) {
  const url = new URL(request.url);
  if (url.pathname === "/n-admin" || url.pathname.startsWith("/n-admin/")) {
    const path = decodeURIComponent(url.pathname.slice("/n-admin".length).replace(/^\//, ""));
    const handler = request.method === "GET" || request.method === "HEAD" ? cms.GET : cms.POST;
    return handler({ request, params: path ? { path } : {} });
  }
  return sitePage(request);
}

// Images: real PNGs the browser can decode, each a different colour so each is its own upload.

function chunk(type, data) {
  const out = Buffer.alloc(12 + data.length);
  out.writeUInt32BE(data.length, 0);
  out.write(type, 4, "ascii");
  data.copy(out, 8);
  out.writeUInt32BE(crc32(Buffer.concat([Buffer.from(type, "ascii"), data])), 8 + data.length);
  return out;
}

export function png(width, height, [r, g, b]) {
  const header = Buffer.alloc(13);
  header.writeUInt32BE(width, 0);
  header.writeUInt32BE(height, 4);
  header.set([8, 2, 0, 0, 0], 8);
  const row = Buffer.alloc(1 + width * 3);
  for (let x = 0; x < width; x++) row.set([r, g, b], 1 + x * 3);
  const raw = Buffer.concat(Array.from({ length: height }, () => row));
  return Buffer.concat([Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), chunk("IHDR", header), chunk("IDAT", deflateSync(raw)), chunk("IEND", Buffer.alloc(0))]);
}

let colour = 0;
/** A new image file on disk, unlike any other made this run. */
export function imageFile(dir, name = `photo-${colour + 1}.png`, width = 320, height = 240) {
  colour += 1;
  const path = join(dir, name);
  writeFileSync(path, png(width, height, [(colour * 53) % 256, (colour * 101) % 256, (colour * 197) % 256]));
  return path;
}

// Content for the run.

const IFRAME = '<iframe src="https://player.example.com/embed/42" width="640" height="360" title="A player" allowfullscreen></iframe>';
export const SAMPLES = {
  untouched: "# A heading\n\nSome *emphasis*, **strong**, `code` and a [link](https://example.com).\n\n- one\n- two\n  - nested\n\n1. first\n2. second\n\n> A quote\n\n```js\nconst x = 1;\n```\n\n---\n\n![Alt text](https://example.com/a.png)\n\nLast line with a hard  \nbreak.",
  iframe: `Before the player.\n\n${IFRAME}\n\nAfter the player.`,
};

export function seed() {
  setTexts({ tagline: "Hello from the site", intro: "About the portfolio", hero: "" });
  replaceItems("team", [{ name: "Ada", photo: "", site: "https://ada.example.com" }, { name: "Grace", photo: "", site: "" }]);
  replaceItems("links", [{ label: "Home", url: "https://example.com" }]);
  replaceItems("events", [{ title: "Launch", date: "2030-01-02" }]);
  const day = Date.parse("2024-01-01T00:00:00Z");
  for (let n = 1; n <= 55; n++) {
    const id = createPost();
    savePost(id, { title: `Seed post ${n}`, description: "", body: `Body of seed ${n}${n === 7 ? " with a needle in it" : ""}.`, slug: `seed-${n}`, published_at: new Date(day + n * 86_400_000).toISOString() }, db().prepare("SELECT updated_at FROM posts WHERE id = ?").get(id).updated_at);
  }
  const published = (title, slug, body) => {
    const id = createPost();
    savePost(id, { title, description: "", body, slug, published_at: new Date(day + 100 * 86_400_000).toISOString() }, db().prepare("SELECT updated_at FROM posts WHERE id = ?").get(id).updated_at);
    return id;
  };
  const ids = { untouched: published("Untouched", "untouched", SAMPLES.untouched), iframe: published("With a player", "with-a-player", SAMPLES.iframe) };
  db().exec("DELETE FROM saves; DELETE FROM posts_old;");
  return ids;
}

/** A fresh data folder and server. `stop` removes both. */
export async function startFixture() {
  const dir = mkdtempSync(join(tmpdir(), "cms-browser-"));
  const files = join(dir, "files");
  mkdirSync(files);
  closeDb();
  process.env.CMS_DATA_DIR = join(dir, "data");
  process.env.NODE_ENV = "test";
  process.env.CMS_PASSWORD_HASH = await hashPassword(OWNER_PASSWORD);
  db({ create: true });

  const server = createServer(async (req, res) => {
    try {
      const chunks = [];
      for await (const part of req) chunks.push(part);
      const headers = new Headers();
      for (const [name, value] of Object.entries(req.headers)) headers.set(name, Array.isArray(value) ? value.join(", ") : value);
      const method = req.method ?? "GET";
      const request = new Request(new URL(req.url, process.env.SITE_URL), { method, headers, body: method === "GET" || method === "HEAD" ? undefined : Buffer.concat(chunks) });
      const response = await answer(request);
      const out = {};
      response.headers.forEach((value, name) => { if (name !== "set-cookie") out[name] = value; });
      const cookies = response.headers.getSetCookie();
      if (cookies.length) out["set-cookie"] = cookies;
      res.writeHead(response.status, out);
      res.end(Buffer.from(await response.arrayBuffer()));
    } catch (error) {
      console.error(error);
      res.writeHead(500).end(String(error));
    }
  });
  await new Promise((done) => server.listen(0, "127.0.0.1", done));
  const origin = `http://127.0.0.1:${server.address().port}`;
  process.env.SITE_URL = origin;
  const ids = seed();
  return {
    origin,
    files,
    ids,
    async stop() {
      server.closeAllConnections();
      await new Promise((done) => server.close(done));
      closeDb();
      rmSync(dir, { recursive: true, force: true });
    },
  };
}
