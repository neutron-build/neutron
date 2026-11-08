import type { VNode } from "preact";
import { render } from "preact-render-to-string";
import { isAsset, serveAsset } from "./assets.js";
import {
  clearAttempts, clientAddress, createSession, endSession, hasSession, passwordHash, previewToken, readLimited, sessionCookie,
  takeAttempt, verifyPassword, withPasswordCheck,
} from "./auth.js";
import { TEXT, type ResolvedConfig, type ResolvedList } from "./config.js";
import { thumbUrl } from "./images.js";
import { renderMarkdown } from "./markdown.js";
import { MAX_UPLOAD_BYTES, saveImage, serveImage } from "./media.js";
import { Document, EditorPage, EditPage, ListPage, LoginPage, NotFoundPage, PostsPage, TextPage, type Status } from "./pages.js";
import {
  allPosts, allTexts, createPost, deletePost, ensureDesigns, getPost, isLive, listItems, listRevision, replaceItems, saveEdits, savePost, setTexts,
  sortByDate, type Item, type Post,
} from "./store.js";

const CSP = [
  "default-src 'self'",
  "script-src 'self'",
  "style-src 'self' 'unsafe-inline'",
  "img-src 'self' https: data: blob:",
  "font-src 'self'",
  "frame-src 'self' https:",
  "connect-src 'self'",
  "object-src 'none'",
  "base-uri 'none'",
  "form-action 'self'",
  "frame-ancestors 'none'",
].join("; ");

const SAVE_ERRORS: Record<string, string> = {
  stale: "This post changed elsewhere since you opened it. Copy your text, then reload.",
  "slug-taken": "Another post already uses that URL.",
  "slug-reserved": "That URL belongs to another page on the site.",
  missing: "This post no longer exists.",
};


const redirect = (location: string, cookie?: string) =>
  new Response(null, { status: 303, headers: { Location: location, ...(cookie ? { "Set-Cookie": cookie } : {}) } });

const escapeHtml = (text: string) =>
  text.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]!);

const isDate = (value: string) => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T00:00:00Z`);
  return !Number.isNaN(date.getTime()) && date.toISOString().startsWith(value);
};

function parseDate(value: string): string | null {
  if (!value) return null;
  const date = new Date(/(Z|[+-]\d\d:\d\d)$/.test(value) ? value : `${value}Z`);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

/** The first thing wrong with these rows, in words the editor can act on. */
function problemWith(list: ResolvedList, items: Item[]): string | null {
  const fields = Object.entries(list.fields);
  const required = fields.filter(([, field]) => field.required);
  if (items.some((item) => required.some(([key]) => !item[key]))) {
    return `Every row needs ${required.map(([, field]) => field.label.toLowerCase()).join(" and ")}.`;
  }
  for (const [key, field] of fields) {
    if (field.type === "date" && items.some((item) => item[key] && !isDate(item[key]))) return `${field.label} must be a real date.`;
  }
  return null;
}

// `request.url` is http:// behind a proxy, so the expected origin comes from configuration.
function isSameOrigin(request: Request): boolean {
  const origin = request.headers.get("origin");
  if (!origin) return false;
  const own = new URL(request.url);
  const local = own.hostname === "localhost" || own.hostname === "127.0.0.1" || own.hostname === "[::1]";
  const expected = process.env.SITE_URL ? new URL(process.env.SITE_URL).origin : process.env.NODE_ENV !== "production" && local ? own.origin : null;
  return origin === expected;
}

const statusOf = (post: Post, at?: string): Status => (!post.published_at ? "draft" : isLive(post, at) ? "live" : "scheduled");

export interface RouteArgs {
  request: Request;
  params?: Record<string, string | undefined>;
}

/**
 * Every admin address is answered here, and the route file only hands over the matched path.
 * The router has already decoded it, so an encoded address reaches the same gate as a plain one.
 */
export function createHandler(config: ResolvedConfig) {
  const admin = config.path;

  const page = (title: string, body: VNode, status = 200) =>
    new Response(`<!doctype html>${render(<Document config={config} title={title}>{body}</Document>)}`, {
      status,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });

  const notFound = () => page("Not found", <NotFoundPage config={config} />, 404);
  const isScript = (request: Request) => request.headers.has("x-cms");

  function postsPage(kind: "post" | "page" = "post") {
    if (kind === "page") ensureDesigns(config.designs);
    const at = new Date().toISOString();
    const posts = allPosts(kind).map((post) => ({
      id: post.id,
      title: post.title,
      date: post.published_at ?? post.updated_at,
      status: statusOf(post, at),
    }));
    return page(kind === "page" ? "Pages" : "Posts", <PostsPage config={config} posts={posts} kind={kind} />);
  }

  function home(request: Request) {
    if (config.posts) {
      if (request.method === "POST") return redirect(`${admin}/posts/${createPost()}`);
      return postsPage();
    }
    if (config.onPage.length) return redirect(`${admin}/edit`);
    const first = Object.keys(config.lists)[0] ?? (Object.keys(config.texts).length ? TEXT : null);
    return first ? redirect(`${admin}/lists/${first}`) : page("Nothing to edit", <NotFoundPage config={config} />);
  }

  async function login(request: Request) {
    const hash = passwordHash();
    if (request.method !== "POST") {
      if (hasSession(request)) return redirect(admin);
      return page("Log in", <LoginPage config={config} configured={Boolean(hash)} />);
    }
    const refuse = (error: string, status = 200) => page("Log in", <LoginPage config={config} configured={Boolean(hash)} error={error} />, status);
    if (!hash) return refuse("No password is set.");

    const client = clientAddress(request);
    if (!takeAttempt(client)) return refuse("Too many attempts. Try again in 15 minutes.");

    const raw = await readLimited(request, 4096);
    const password = raw === null ? null : new URLSearchParams(raw).get("password");
    if (!password) return refuse("Wrong password.");
    const verified = await withPasswordCheck(() => verifyPassword(password, hash));
    if (verified === "busy") return refuse("The server is busy. Try again in a moment.", 503);
    if (!verified) return refuse("Wrong password.");

    clearAttempts(client);
    return redirect(admin, sessionCookie(createSession(hash), admin));
  }

  async function upload(request: Request) {
    const tooLarge = () => Response.json({ error: "That image is over 12 MB." }, { status: 413 });
    if (Number(request.headers.get("content-length") ?? 0) > MAX_UPLOAD_BYTES + 4096) return tooLarge();
    const form = await request.formData();
    const file = form.get("file");
    const thumb = form.get("thumb");
    if (!(file instanceof File)) return Response.json({ error: "No file." }, { status: 400 });
    if (file.size + (thumb instanceof File ? thumb.size : 0) > MAX_UPLOAD_BYTES) return tooLarge();

    const name = saveImage(new Uint8Array(await file.arrayBuffer()), {
      width: Number(form.get("width")),
      height: Number(form.get("height")),
      thumb: thumb instanceof File ? new Uint8Array(await thumb.arrayBuffer()) : undefined,
    });
    if (!name) return Response.json({ error: "Use a JPEG, PNG, WebP or GIF." }, { status: 415 });
    const url = `${admin}/media/${name}`;
    return Response.json({ url, thumb: thumbUrl(url) });
  }

  async function preview(request: Request) {
    const form = await request.formData();
    const title = escapeHtml(String(form.get("title") ?? ""));
    // The preview frame has no address of its own, and the admin forbids <base>, so the site's own
    // paths are written out in full.
    const site = process.env.SITE_URL ? new URL(process.env.SITE_URL).origin : new URL(request.url).origin;
    const body = renderMarkdown(String(form.get("body") ?? ""))
      .replace(/\b(src|href|poster)="\/(?!\/)/g, `$1="${site}/`)
      .replace(/\bsrcset="([^"]*)"/g, (_, set: string) => `srcset="${set.replace(/(^|,\s*)\/(?!\/)/g, `$1${site}/`)}"`);
    return new Response(
      `<!doctype html><meta charset="utf-8"><style>html{box-sizing:border-box}*{box-sizing:inherit}body{margin:0;font-family:sans-serif}img{max-width:100%;height:auto}main{width:min(768px,100%);margin:0 auto;padding:40px 24px 96px}h1:first-child{margin:0 0 16px;font-size:40px;line-height:1.2}${config.previewCss}</style><main><h1>${title}</h1><div class="prose">${body}</div></main>`,
      { headers: { "Content-Type": "text/html; charset=utf-8" } },
    );
  }

  const designOf = (post: Post) => (post.kind === "page" && Object.hasOwn(config.designs, post.layout) ? config.designs[post.layout] : undefined);
  const isDesign = (post: Post) => designOf(post) !== undefined;

  /** The site, or with `?page=` one designed page, drafts included, framed for editing. */
  function editSite(request: Request) {
    const id = Number(new URL(request.url).searchParams.get("page"));
    const designed = id ? getPost(id) : undefined;
    const design = designed && designOf(designed);
    if (designed && design && (design.url || designed.slug)) {
      const address = design.url ?? config.pageUrl(designed.slug!);
      const src = isLive(designed) ? address : `${address}?cms-preview=${encodeURIComponent(previewToken(design.url ? designed.layout : designed.slug!))}`;
      return page(`Edit ${designed.title || "page"}`, <EditPage config={config} src={src} back={{ href: `${admin}/posts/${designed.id}`, label: designed.title || "Page" }} />);
    }
    return page("Edit site", <EditPage config={config} />);
  }

  async function editor(request: Request, id: number) {
    const post = getPost(id);
    if (!post || (post.kind === "page" ? !config.pages : !config.posts)) return notFound();
    const saved = new URL(request.url).searchParams.has("saved");
    if (request.method !== "POST") return page(post.title || "Untitled", <EditorPage config={config} post={post} status={statusOf(post)} saved={saved} />);
    // A designed page keeps its body, which its design doesn't show, whatever the form sends.

    const form = await request.formData();
    const read = (name: string) => String(form.get(name) ?? "");
    const intent = read("intent") || "save";
    const script = isScript(request);

    const design = designOf(post);
    if (design?.permanent && (intent === "delete" || intent === "unpublish")) return redirect(`${admin}/posts/${id}`);
    if (intent === "delete") {
      deletePost(id);
      return redirect(post.kind === "page" ? `${admin}/pages` : admin);
    }

    const date = parseDate(read("date") || read("date_local"));
    const published_at =
      intent === "publish" ? (date ?? new Date().toISOString()) :
      intent === "unpublish" ? null :
      post.published_at === null ? null : (date ?? post.published_at);

    const fields = {
      title: read("title").trim(),
      description: read("description").trim(),
      label: read("label").trim(),
      body: isDesign(post) ? post.body : read("body"),
      slug: design?.url ? (post.slug ?? "") : read("slug"),
      published_at,
    };

    // A design owns its address, even one the site reserves for its own routes.
    const result = savePost(id, fields, read("revision"), design ? [] : config.reservedSlugs);
    if (!result.ok) {
      const message = SAVE_ERRORS[result.error];
      if (script) return Response.json({ ok: false, error: result.error, message }, { status: 409 });
      return page(post.title || "Untitled", <EditorPage config={config} post={post} status={statusOf(post)} saved={false} error={message} fields={fields} />);
    }
    if (script) return Response.json({ ok: true, revision: result.post.updated_at, slug: result.post.slug });
    return redirect(`${admin}/posts/${id}?saved=1`);
  }

  async function list(request: Request, key: string) {
    const saved = () => redirect(`${admin}/lists/${key}?saved=1`);
    const showsSaved = new URL(request.url).searchParams.has("saved");

    if (key === TEXT) {
      if (Object.keys(config.texts).length === 0) return notFound();
      if (request.method !== "POST") return page("Text", <TextPage config={config} texts={allTexts()} saved={showsSaved} />);
      const form = await request.formData();
      setTexts(Object.fromEntries(Object.keys(config.texts).map((name) => [name, String(form.get(name) ?? "").trim()])));
      return saved();
    }

    const definition = Object.hasOwn(config.lists, key) ? config.lists[key] : undefined;
    if (!definition) return notFound();
    const show = (items: Item[], error?: string) =>
      page(definition.label, <ListPage config={config} listKey={key} items={items} revision={listRevision(key)} saved={showsSaved && !error} error={error} />);
    if (request.method !== "POST") return show(listItems(key));

    const form = await request.formData();
    const names = Object.keys(definition.fields);
    const columns = names.map((name) => form.getAll(`f.${name}`).map((value) => String(value).trim()));
    const rows = columns[0].map((_, index) => Object.fromEntries(names.map((name, column) => [name, columns[column][index] ?? ""])) as Item);
    const filled = rows.filter((row) => Object.values(row).some(Boolean));
    const items = definition.sortBy ? sortByDate(filled, definition.sortBy) : filled;

    // A refused save shows what was submitted, so no work is lost.
    const problem = problemWith(definition, items);
    if (problem) return show(items, problem);
    if (items.length === 0 && form.get("confirm_empty") !== "1") return show(items, `${definition.label} was not cleared. Confirm to remove everything.`);
    if (!replaceItems(key, items, String(form.get("revision") ?? "")).ok) return show(items, `${definition.label} changed elsewhere. Copy anything you need, then reload.`);
    return saved();
  }

  /** Everything the editor needs. Only signed-in requests reach it. */
  function editData() {
    return Response.json({
      textFields: config.texts,
      lists: Object.fromEntries(Object.entries(config.lists).map(([key, list]) => [key, {
        label: list.label,
        fields: list.fields,
        sortBy: list.sortBy,
        items: listItems(key),
        revision: listRevision(key),
      }])),
    });
  }

  async function editSave(request: Request) {
    const refuse = (error: string, status = 422) => Response.json({ error }, { status });
    let body: { texts?: unknown; lists?: unknown };
    try {
      body = await request.json();
    } catch {
      return refuse("That save could not be read.", 400);
    }
    const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);

    const texts: Record<string, string> = {};
    for (const [name, value] of Object.entries(isRecord(body.texts) ? body.texts : {})) {
      if (!Object.hasOwn(config.texts, name) || typeof value !== "string") return refuse(`${name} is not a text field.`);
      texts[name] = value.trim();
    }

    const lists: Record<string, { revision: string; items: Item[] }> = {};
    for (const [key, value] of Object.entries(isRecord(body.lists) ? body.lists : {})) {
      const definition = Object.hasOwn(config.lists, key) ? config.lists[key] : undefined;
      if (!definition || !isRecord(value) || !Array.isArray(value.items)) return refuse(`${key} is not a list.`);
      // Only declared fields are kept, whatever the request carried.
      const items = value.items.map((item: unknown) =>
        Object.fromEntries(Object.keys(definition.fields).map((name) => {
          const field = isRecord(item) ? item[name] : "";
          return [name, typeof field === "string" ? field.trim() : ""];
        })),
      );
      const problem = problemWith(definition, items);
      if (problem) return refuse(`${definition.label}: ${problem}`);
      lists[key] = { revision: String(value.revision ?? ""), items: definition.sortBy ? sortByDate(items, definition.sortBy) : items };
    }

    const result = saveEdits(texts, lists);
    if (!result.ok) return refuse(`${config.lists[result.stale].label} changed elsewhere. Reload, then make your edit again.`, 409);
    return Response.json({ ok: true });
  }

  async function route(request: Request, segments: string[]): Promise<Response> {
    const [first, second, ...rest] = segments;
    const post = request.method === "POST";
    if (first === undefined) return home(request);
    if (first === "login" && !second) return login(request);
    if (first === "logout" && !second && post) {
      endSession(request);
      return redirect(`${admin}/login`, sessionCookie(null, admin));
    }
    if (isAsset(first) && !second) return serveAsset(first, request);
    if (first === "edit" && !second && !post && (config.onPage.length || Object.keys(config.designs).length)) return editSite(request);
    if (first === "edit" && second === "data" && rest.length === 0 && !post) return editData();
    if (first === "edit" && second === "save" && rest.length === 0 && post) return editSave(request);
    if (first === "upload" && !second && post) return upload(request);
    if (first === "preview" && !second && post) return preview(request);
    if (first === "pages" && config.pages && !second) return post ? redirect(`${admin}/posts/${createPost({ kind: "page" })}`) : postsPage("page");
    if (first === "posts" && (config.posts || config.pages) && second && /^\d+$/.test(second) && rest.length === 0) return editor(request, Number(second));
    if (first === "lists" && second && rest.length === 0) return list(request, second);
    return notFound();
  }

  return async function handle({ request, params }: RouteArgs): Promise<Response> {
    const segments = (params?.path ?? "").split("/").filter(Boolean);
    const unsafe = request.method !== "GET" && request.method !== "HEAD";

    // The only things a visitor may fetch.
    if (!unsafe && segments[0] === "media" && segments.length === 2) return serveImage(segments[1]);
    if (!unsafe && segments[0] === "admin.css" && segments.length === 1) return serveAsset("admin.css", request);

    if (unsafe && !isSameOrigin(request)) return new Response("Forbidden", { status: 403 });

    // Only an exact /login is exempt, so anything unusual stays gated.
    const isLogin = segments.length === 1 && segments[0] === "login";
    if (!isLogin && !hasSession(request)) {
      if (isScript(request)) return Response.json({ error: "signed-out" }, { status: 401 });
      return redirect(`${admin}/login`);
    }

    const response = await route(request, segments);
    const headers = new Headers(response.headers);
    // Versioned scripts say how long they keep; everything else behind the login is never stored.
    if (!isAsset(segments[0]) || segments.length !== 1) headers.set("Cache-Control", "no-store");
    headers.set("Content-Security-Policy", CSP);
    headers.set("X-Robots-Tag", "noindex");
    return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
  };
}
