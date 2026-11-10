import { AsyncLocalStorage } from "node:async_hooks";
import type { VNode } from "preact";
import { render } from "preact-render-to-string";
import { isAsset, serveAsset } from "./assets.js";
import {
  type Account, addPerson, checkLogin, checkPassword, clearAttempts, clientAddress, createSession, endSession, generatePassword, hashPassword, hasPeople, hasSession,
  loginName, MAX_PASSWORD, MIN_PASSWORD, passwordHash, people, personById, previewToken, readLimited, removePerson, sessionCookie, sessionOf, setPassword,
  takeAttempt, withPasswordCheck,
} from "./auth.js";
import { TEXT, type ResolvedConfig, type ResolvedList } from "./config.js";
import { thumbUrl } from "./images.js";
import { renderMarkdown } from "./markdown.js";
import { isSafeUrl } from "./safe-url.js";
import { deleteImage, isImageName, listImages, MAX_UPLOAD_BYTES, saveImage, serveImage, type StoredImage } from "./media.js";
import { AccountPage, Document, EditorPage, PeoplePage, Viewer, EditPage, type DesignedPage, HistoryPage, type HistoryEntry, ListPage, LoginPage, type MediaEntry, MediaPage, NotFoundPage, PostsPage, TextPage, TrashPage, VersionsPage, type Status } from "./pages.js";
import {
  allPosts, allTexts, createPost, findPosts, postCount, deleteForever, emptyOldTrash, ensureDesigns, imageUses, inCurrentUse, inTrash, removeAbandonedDrafts, getPost, isLive, listItems, listRevision, replaceItems, restoreFromTrash, restoreSave, restoreVersion, saveCount, saveEdits, savePost, saves, setTexts,
  sortByDate, trashCount, trashed, trashPost, valuesNow, versionsOf, type Item, type Post, type Save, type Snapshot,
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
/** A link typed the way people type them, "fylun.ai" or "me@site.com", made into one that works. */
function completeUrl(value: string): string {
  if (/^[^\s@/:]+@[^\s@/]+\.[a-z]{2,}$/i.test(value)) return `mailto:${value}`;
  if (/^(www\.)?[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}([/?#]\S*)?$/i.test(value)) return `https://${value}`;
  return value;
}

const isLinkTarget = (value: string) => /^(https?:\/\/[^\s/]+\S*|mailto:\S+|tel:\S+|[/#?]\S*)$/i.test(value) && isSafeUrl(value);

/** Completes typed links in place, then names the first row that can't be saved, and why. */
function problemWith(list: ResolvedList, items: Item[]): string | null {
  const fields = Object.entries(list.fields);
  for (const item of items) {
    for (const [key, field] of fields) if (field.type === "url" && item[key]) item[key] = completeUrl(item[key]);
  }
  for (const [index, item] of items.entries()) {
    const row = items.length > 1 ? `Row ${index + 1}: ` : "";
    const missing = fields.filter(([key, field]) => field.required && !item[key]).map(([, field]) => field.label);
    if (missing.length) return `${row}add ${missing.join(" and ")}.`;
    for (const [key, field] of fields) {
      const value = item[key];
      if (!value) continue;
      if (field.type === "date" && !isDate(value)) return `${row}${field.label} must be a real date.`;
      if (field.type === "url" && !isLinkTarget(value)) return `${row}"${value}" isn't a link. Use a web address such as https://example.com, or a path such as /about/.`;
      if (field.type === "image" && !isSafeUrl(value)) return `${row}${field.label} isn't an image address.`;
    }
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

const statusOf = (post: Pick<Post, "published_at">, at?: string): Status => (!post.published_at ? "draft" : isLive(post, at) ? "live" : "scheduled");

/** Who is logged in, for the request being answered. */
const viewer = new AsyncLocalStorage<{ account: Account; session: string }>();

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
    new Response(`<!doctype html>${render(<Viewer.Provider value={viewer.getStore()?.account ?? null}><Document config={config} title={title}>{body}</Document></Viewer.Provider>)}`, {
      status,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });

  const notFound = () => page("Not found", <NotFoundPage config={config} />, 404);
  const isScript = (request: Request) => request.headers.has("x-cms");

  const POSTS_PAGE = 50;

  function postsPage(request: Request, kind: "post" | "page" = "post") {
    if (kind === "page") ensureDesigns(config.designs);
    removeAbandonedDrafts();
    emptyOldTrash();
    const search = new URL(request.url).searchParams;
    const deletedId = Number(search.get("deleted"));
    const deleted = deletedId ? inTrash(deletedId) : undefined;
    const query = (search.get("q") ?? "").trim().slice(0, 200);
    const at = new Date().toISOString();
    // Pages that are always there, such as the homepage, lead the list.
    const first = (post: Pick<Post, "kind" | "layout">) => (kind === "page" && designOf(post)?.permanent ? 0 : 1);
    const found = findPosts(kind, query).sort((a, b) => first(a) - first(b));
    const pages = Math.max(1, Math.ceil(found.length / POSTS_PAGE));
    const current = Math.min(pages, Math.max(1, Math.floor(Number(search.get("page")) || 1)));
    const posts = found.slice((current - 1) * POSTS_PAGE, current * POSTS_PAGE).map((post) => ({
      id: post.id,
      title: post.title,
      date: post.published_at ?? post.updated_at,
      status: statusOf(post, at),
    }));
    return page(kind === "page" ? "Pages" : "Posts", (
      <PostsPage
        config={config} posts={posts} kind={kind} deleted={deleted} trash={trashCount()}
        query={query} total={query ? postCount(kind) : found.length} page={current} pageCount={pages}
      />
    ));
  }

  function home(request: Request) {
    if (config.posts) {
      if (request.method === "POST") return redirect(`${admin}/posts/${createPost()}`);
      return postsPage(request);
    }
    if (config.onPage.length) return redirect(`${admin}/edit`);
    const first = Object.keys(config.lists)[0] ?? (Object.keys(config.texts).length ? TEXT : null);
    return first ? redirect(`${admin}/lists/${first}`) : page("Nothing to edit", <NotFoundPage config={config} />);
  }

  /** Where to go after logging in: only somewhere inside the admin, never another site. */
  const backTo = (next: string | null) =>
    next && next.startsWith(`${admin}/`) && !next.startsWith(`${admin}/login`) && !/[\\\s]|\/\//.test(next) ? next : null;

  async function login(request: Request) {
    const hash = passwordHash();
    const named = hash ? hasPeople() : false;
    if (request.method !== "POST") {
      const next = backTo(new URL(request.url).searchParams.get("next"));
      if (hasSession(request)) return redirect(next ?? admin);
      return page("Log in", <LoginPage config={config} configured={Boolean(hash)} named={named} next={next} />);
    }
    let next: string | null = null;
    let name = "";
    const refuse = (error: string, status = 200) => page("Log in", <LoginPage config={config} configured={Boolean(hash)} named={named} name={name} error={error} next={next} />, status);
    if (!hash) return refuse("No password is set.");
    // One answer for an unknown name and a wrong password.
    const wrong = named ? "Wrong name or password." : "Wrong password.";

    const client = clientAddress(request);
    if (!takeAttempt(client)) return refuse("Too many attempts. Try again in 15 minutes.");

    const raw = await readLimited(request, 4096);
    const fields = new URLSearchParams(raw ?? "");
    next = backTo(fields.get("next"));
    const typed = fields.get("name") ?? "";
    name = typed.trim().slice(0, 100);
    const password = raw === null ? null : fields.get("password");
    if (!password) return refuse(wrong);
    const found = await withPasswordCheck(() => checkLogin(typed, password));
    if (found === "busy") return refuse("The server is busy. Try again in a moment.", 503);
    if (!found) return refuse(wrong);

    clearAttempts(client);
    const token = found.account.kind === "owner" ? createSession(found.hash) : createSession(found.hash, found.account.id);
    return redirect(next ?? admin, sessionCookie(token, admin));
  }

  // Account and people

  async function account(request: Request) {
    const { account: who, session } = viewer.getStore()!;
    const show = (error?: string, status = 200) =>
      page("Account", <AccountPage config={config} account={who} changed={new URL(request.url).searchParams.has("changed") && !error} error={error} />, status);
    if (request.method !== "POST") return show();

    const raw = await readLimited(request, 4096);
    if (raw === null) return show("That was too long.", 413);
    const fields = new URLSearchParams(raw);
    const current = fields.get("current") ?? "";
    const next = fields.get("password") ?? "";
    if (!current) return show("Type your current password.");
    if (next.length < MIN_PASSWORD) return show(`Use at least ${MIN_PASSWORD} characters for the new password.`);
    if (next.length > MAX_PASSWORD) return show(`Use at most ${MAX_PASSWORD} characters for the new password.`);
    if (next !== (fields.get("repeat") ?? "")) return show("The two new passwords don't match.");

    // Counted per client and per account, so a stolen session can't guess at the password from many addresses.
    const keys = [`change ${clientAddress(request)}`, `change ${who.kind === "owner" ? "owner" : `person ${who.id}`}`];
    if (!keys.map((key) => takeAttempt(key)).every(Boolean)) return show("Too many attempts. Try again in 15 minutes.", 429);
    const verified = await withPasswordCheck(() => checkPassword(who, current));
    if (verified === "busy") return show("The server is busy. Try again in a moment.", 503);
    if (!verified) return show("Your current password is wrong.");
    keys.forEach(clearAttempts);

    const hash = await withPasswordCheck(() => hashPassword(next));
    if (hash === "busy") return show("The server is busy. Try again in a moment.", 503);
    if (!setPassword(who, hash, session)) return show("Your account no longer exists.", 409);
    return redirect(`${admin}/account?changed=1`);
  }

  /** Only the owner manages people. Checked here on every request, not by hiding links. */
  async function peopleScreen(request: Request, id?: number, action?: string) {
    if (viewer.getStore()?.account.kind !== "owner") return notFound();
    const show = (extra: { error?: string; given?: { name: string; password: string; added: boolean }; removed?: boolean } = {}, status = 200) =>
      page("People", <PeoplePage config={config} people={people()} {...extra} />, status);
    if (request.method !== "POST") return show({ removed: new URL(request.url).searchParams.has("removed") });

    const fields = new URLSearchParams((await readLimited(request, 4096)) ?? "");
    const give = async (person: { name: string }, added: boolean, store: (hash: string) => boolean) => {
      const password = generatePassword();
      const hash = await withPasswordCheck(() => hashPassword(password));
      if (hash === "busy") return show({ error: "The server is busy. Try again in a moment." }, 503);
      if (!store(hash)) return show({ error: added ? "Someone already has that name." : "That person was removed." }, 409);
      // Shown on this page only, never in an address, and never stored.
      return show({ given: { name: person.name, password, added } });
    };

    if (id === undefined) {
      const name = loginName(fields.get("name") ?? "");
      if (!name) return show({ error: "Use letters, numbers, dots, dashes or underscores for the name, or an email address." }, 422);
      return give({ name }, true, (hash) => addPerson(name, hash) !== "taken");
    }
    const person = personById(id);
    if (!person) return redirect(`${admin}/people`);
    if (action === "remove") {
      removePerson(id);
      return redirect(`${admin}/people?removed=1`);
    }
    return give(person, false, (hash) => setPassword({ kind: "person", id, name: person.name }, hash));
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

  /** A post's text as a whole page, for a frame in the admin. */
  function previewDocument(request: Request, rawTitle: string, markdown: string) {
    const title = escapeHtml(rawTitle);
    // The preview frame has no address of its own, and the admin forbids <base>, so the site's own
    // paths are written out in full.
    const site = process.env.SITE_URL ? new URL(process.env.SITE_URL).origin : new URL(request.url).origin;
    const body = renderMarkdown(markdown)
      .replace(/\b(src|href|poster)="\/(?!\/)/g, `$1="${site}/`)
      .replace(/\bsrcset="([^"]*)"/g, (_, set: string) => `srcset="${set.replace(/(^|,\s*)\/(?!\/)/g, `$1${site}/`)}"`);
    return `<!doctype html><meta charset="utf-8"><style>html{box-sizing:border-box}*{box-sizing:inherit}body{margin:0;font-family:sans-serif}img{max-width:100%;height:auto}main{width:min(768px,100%);margin:0 auto;padding:40px 24px 96px}h1:first-child{margin:0 0 16px;font-size:40px;line-height:1.2}${config.previewCss}</style><main><h1>${title}</h1><div class="prose">${body}</div></main>`;
  }

  async function preview(request: Request) {
    const form = await request.formData();
    return new Response(previewDocument(request, String(form.get("title") ?? ""), String(form.get("body") ?? "")), { headers: { "Content-Type": "text/html; charset=utf-8" } });
  }

  const designOf = (post: Pick<Post, "kind" | "layout">) => (post.kind === "page" && Object.hasOwn(config.designs, post.layout) ? config.designs[post.layout] : undefined);
  const isDesign = (post: Post) => designOf(post) !== undefined;

  /** The site, framed for editing. `?page=` was the old way to open a designed page; it now opens as itself. */
  function editSite(request: Request) {
    const id = Number(new URL(request.url).searchParams.get("page"));
    if (id) return redirect(`${admin}/posts/${id}`);
    return page("Edit site", <EditPage config={config} history={saveCount() > 0} />);
  }

  /** A designed page opens as itself, drafts included, ready to click and edit, and is published from the same bar. */
  function designedPage(post: Post) {
    const design = designOf(post)!;
    const address = design.url ?? (post.slug ? config.pageUrl(post.slug) : null);
    if (!address) {
      const taken = allPosts("post").find((other) => other.slug === post.layout);
      return page(post.title || "Page", (
        <EditPage config={config} back={{ href: `${admin}/pages`, label: "Pages" }} notice={`This page belongs at /${post.layout}/, but the post "${taken?.title || "Untitled"}" is using that address. Change that post's URL in its Settings, then come back.`} />
      ));
    }
    const src = isLive(post) ? address : `${address}?cms-preview=${encodeURIComponent((design.url ? previewToken("design", post.layout) : previewToken("page", post.slug!)))}`;
    const state: DesignedPage = { id: post.id, status: statusOf(post), revision: post.updated_at, permanent: design.permanent };
    return page(post.title || "Page", <EditPage config={config} src={src} back={{ href: `${admin}/pages`, label: "Pages" }} page={state} history={saveCount() > 0} />);
  }

  const editorPage = (post: Post, rest: { saved?: boolean; error?: string; fields?: Partial<Post>; revision?: string } = {}) =>
    page(post.title || "Untitled", <EditorPage config={config} post={post} status={statusOf(post)} saved={rest.saved ?? false} error={rest.error} fields={rest.fields} revision={rest.revision} versions={versionsOf(post).length} />);

  const editable = (post: Post | undefined): post is Post => Boolean(post) && (post!.kind === "page" ? config.pages : config.posts);

  async function editor(request: Request, id: number) {
    const post = getPost(id);
    if (!editable(post)) return notFound();
    const search = new URL(request.url).searchParams;
    if (request.method !== "POST") {
      if (isDesign(post)) return designedPage(post);
      const error = search.has("address-lost") ? "Its old address is now used by something else, so it came back as a draft. Choose a new URL to publish it." : undefined;
      return editorPage(post, { saved: search.has("saved"), error });
    }

    const form = await request.formData();
    const read = (name: string) => String(form.get(name) ?? "");
    const intent = read("intent") || "save";
    const script = isScript(request);

    const design = designOf(post);
    // A design's page is part of the site's code, so it can be taken offline but not thrown away.
    if ((design && intent === "delete") || (design?.permanent && intent === "unpublish")) return redirect(`${admin}/posts/${id}`);
    if (intent === "delete") {
      const trashId = trashPost(id);
      return redirect(`${post.kind === "page" ? `${admin}/pages` : admin}?deleted=${trashId}`);
    }

    const date = parseDate(read("date") || read("date_local"));
    const published_at =
      design?.permanent ? (isLive(post) ? post.published_at : new Date().toISOString()) :
      intent === "publish" ? (date ?? new Date().toISOString()) :
      intent === "unpublish" ? null :
      post.published_at === null ? null : (date ?? post.published_at);

    // A designed page is only published or unpublished from its bar; everything else stays as it is.
    const fields = design
      ? { title: post.title, description: post.description, label: post.label, body: post.body, slug: post.slug ?? "", published_at }
      : { title: read("title").trim(), description: read("description").trim(), label: read("label").trim(), body: read("body"), slug: read("slug"), published_at };

    if (intent === "publish" && !design && !fields.title) {
      return editorPage(post, { error: "Add a title before publishing.", fields, revision: read("revision") });
    }

    // A design owns its address, even one the site reserves for its own routes.
    const result = savePost(id, fields, read("revision"), design ? [] : config.reservedSlugs);
    if (!result.ok) {
      const message = SAVE_ERRORS[result.error];
      if (script) return Response.json({ ok: false, error: result.error, message }, { status: 409 });
      if (design) return redirect(`${admin}/posts/${id}`);
      // Keep the revision this form was opened at: the current one would let a second press overwrite the newer text.
      return editorPage(post, { error: message, fields, revision: read("revision") });
    }
    if (script) return Response.json({ ok: true, revision: result.post.updated_at, slug: result.post.slug });
    return redirect(`${admin}/posts/${id}?saved=1`);
  }

  async function versions(request: Request, id: number) {
    const post = getPost(id);
    if (!editable(post) || isDesign(post)) return notFound();
    const show = (error?: string, status = 200) => {
      const all = versionsOf(post);
      const wanted = Number(new URL(request.url).searchParams.get("v"));
      const selected = all.find((version) => version.id === wanted) ?? all[0];
      const doc = selected ? previewDocument(request, selected.title, selected.body) : "";
      return page(`Versions of ${post.title || "Untitled"}`, <VersionsPage config={config} post={post} versions={all} selected={selected} document={doc} error={error} />, status);
    };
    if (request.method !== "POST") return show();
    const form = await request.formData();
    const result = restoreVersion(id, Number(form.get("version")), String(form.get("revision") ?? ""));
    if (result.ok) return redirect(`${admin}/posts/${id}?saved=1`);
    return show(result.error === "stale" ? "This post changed since you opened this page. Look at the list again, then restore." : "That version is no longer kept.", 409);
  }

  function trashPage() {
    emptyOldTrash();
    return page("Trash", <TrashPage config={config} items={trashed()} />);
  }

  async function trashAction(request: Request, id: number) {
    const item = inTrash(id);
    if (!item) return redirect(`${admin}/trash`);
    const form = await request.formData();
    if (form.get("intent") === "forever") {
      deleteForever(id);
      return redirect(`${admin}/trash`);
    }
    const restored = restoreFromTrash(id, config.reservedSlugs);
    return redirect(`${admin}/posts/${restored!.id}${restored!.addressLost ? "?address-lost=1" : ""}`);
  }

  async function list(request: Request, key: string) {
    const saved = () => redirect(`${admin}/lists/${key}?saved=1`);
    const showsSaved = new URL(request.url).searchParams.has("saved");

    if (key === TEXT) {
      if (Object.keys(config.texts).length === 0) return notFound();
      if (request.method !== "POST") return page("Text", <TextPage config={config} texts={allTexts()} saved={showsSaved} history={hasHistory(TEXT)} />);
      const form = await request.formData();
      setTexts(Object.fromEntries(Object.keys(config.texts).map((name) => [name, String(form.get(name) ?? "").trim()])));
      return saved();
    }

    const definition = Object.hasOwn(config.lists, key) ? config.lists[key] : undefined;
    if (!definition) return notFound();
    // A refused save keeps the revision it was made from, so pressing Save again can't overwrite a newer list.
    const show = (items: Item[], error?: string, revision = listRevision(key)) =>
      page(definition.label, <ListPage config={config} listKey={key} items={items} revision={revision} saved={showsSaved && !error} error={error} history={hasHistory(key)} />);
    if (request.method !== "POST") return show(listItems(key));

    const form = await request.formData();
    const names = Object.keys(definition.fields);
    const columns = names.map((name) => form.getAll(`f.${name}`).map((value) => String(value).trim()));
    const rows = columns[0].map((_, index) => Object.fromEntries(names.map((name, column) => [name, columns[column][index] ?? ""])) as Item);
    const filled = rows.filter((row) => Object.values(row).some(Boolean));
    const items = definition.sortBy ? sortByDate(filled, definition.sortBy) : filled;

    // A refused save shows what was submitted, so no work is lost.
    const submitted = String(form.get("revision") ?? "");
    const problem = problemWith(definition, items);
    if (problem) return show(items, problem, submitted);
    if (items.length === 0 && form.get("confirm_empty") !== "1") return show(items, `${definition.label} was not cleared. Confirm to remove everything.`, submitted);
    if (!replaceItems(key, items, submitted).ok) return show(items, `${definition.label} changed elsewhere. Copy anything you need, then reload.`, submitted);
    return saved();
  }

  const declared = () => ({ texts: Object.keys(config.texts), lists: Object.keys(config.lists) });
  /** A save touches a list by its key, or the text fields as a whole. */
  const touches = (save: Save, only: string) => (only === TEXT ? Object.keys(save.before.texts).length > 0 : Object.hasOwn(save.before.lists, only));
  const hasHistory = (only: string) => saves().some((save) => touches(save, only));

  /** Each save with the values it left behind: the next newer save's "before", or today's value. */
  function withAfter(all: Save[]): HistoryEntry[] {
    const keys = { texts: new Set<string>(), lists: new Set<string>() };
    for (const save of all) {
      Object.keys(save.before.texts).forEach((key) => keys.texts.add(key));
      Object.keys(save.before.lists).forEach((key) => keys.lists.add(key));
    }
    const running = valuesNow({ texts: [...keys.texts], lists: [...keys.lists] });
    const now: Snapshot = { texts: { ...running.texts }, lists: { ...running.lists } };
    return all.map((save) => {
      const after: Snapshot = { texts: {}, lists: {} };
      for (const [key, value] of Object.entries(save.before.texts)) {
        after.texts[key] = running.texts[key];
        running.texts[key] = value;
      }
      for (const [key, items] of Object.entries(save.before.lists)) {
        after.lists[key] = running.lists[key];
        running.lists[key] = items;
      }
      return { save, after, now };
    });
  }

  async function history(request: Request, id?: number) {
    const search = new URL(request.url).searchParams;
    const wanted = search.get("only") ?? "";
    const only = wanted === TEXT || Object.hasOwn(config.lists, wanted) ? wanted : "";
    const here = (extra: string) => `${admin}/history${only || extra ? "?" : ""}${[only ? `only=${only}` : "", extra].filter(Boolean).join("&")}`;
    if (id !== undefined) {
      const result = restoreSave(id, declared());
      if (!result.ok) return redirect(here(""));
      return redirect(here(`${result.save ? `restored=${result.save}&` : ""}s=${id}`));
    }
    const entries = withAfter(saves()).filter((entry) => !only || touches(entry.save, only));
    const selected = entries.find((entry) => entry.save.id === Number(search.get("s"))) ?? entries[0];
    const restored = Number(search.get("restored")) || undefined;
    return page("History", <HistoryPage config={config} entries={entries} selected={selected} only={only} restored={restored} />);
  }

  // Media

  const MEDIA_PAGE = 60;
  /** An image added this recently may be in work not saved yet, so "Delete all unused" leaves it. */
  const FRESH_MS = 86_400_000;
  const KEPT_WORDS = { trash: "a post in the Trash", versions: "an old version of a post", history: "list and text history" } as const;
  const mediaUrl = (name: string) => `${admin}/media/${name}`;

  /** Where each image is used, in words with links. */
  function describeImages(images: StoredImage[]): MediaEntry[] {
    const uses = imageUses();
    const listHref = (key: string) => {
      const list = Object.hasOwn(config.lists, key) ? config.lists[key] : undefined;
      if (!list) return undefined;
      return list.ownPage || !config.onPage.includes(key) ? `${admin}/lists/${key}` : `${admin}/edit`;
    };
    return images.map((image) => {
      const use = uses.get(image.name);
      const places = [
        ...[...(use?.posts.values() ?? [])].map((post) => ({ label: `In the ${post.kind} ${post.title || "Untitled"}`, href: `${admin}/posts/${post.id}` })),
        ...[...(use?.lists ?? [])].map((key) => ({
          label: `In ${Object.hasOwn(config.lists, key) ? config.lists[key].label : `${key} (no longer on the site)`}`,
          href: listHref(key),
        })),
        ...[...(use?.texts ?? [])].map((key) => ({
          label: `In the text ${Object.hasOwn(config.texts, key) ? config.texts[key].label : key}`,
          href: Object.hasOwn(config.texts, key) ? (config.onPage.includes(TEXT) ? `${admin}/edit` : `${admin}/lists/${TEXT}`) : undefined,
        })),
      ];
      const kept = places.length ? [] : [...(use?.kept ?? [])].map((reason) => KEPT_WORDS[reason]);
      return { ...image, url: mediaUrl(image.name), thumb: thumbUrl(mediaUrl(image.name)), places, kept };
    });
  }

  const deletableNow = (entries: MediaEntry[], at = Date.now()) =>
    entries.filter((entry) => !entry.places.length && !entry.kept.length && at - Date.parse(entry.added) > FRESH_MS);

  function mediaPage(request: Request, error?: string, status = 200) {
    const search = new URL(request.url).searchParams;
    const unused = search.has("unused");
    const all = describeImages(listImages());
    const shown = unused ? all.filter((entry) => !entry.places.length) : all;
    const pages = Math.max(1, Math.ceil(shown.length / MEDIA_PAGE));
    const at = Math.min(pages, Math.max(1, Math.floor(Number(search.get("page")) || 1)));
    const deleted = Number(search.get("deleted")) || 0;
    const added = Number(search.get("added")) || 0;
    const notice = deleted ? `${deleted === 1 ? "1 image" : `${deleted} images`} deleted.` : added ? `${added === 1 ? "1 image" : `${added} images`} added.` : undefined;
    return page("Media", (
      <MediaPage
        config={config} images={shown.slice((at - 1) * MEDIA_PAGE, at * MEDIA_PAGE)} total={all.length} unusedCount={all.filter((entry) => !entry.places.length).length}
        unused={unused} page={at} pages={pages} deletable={deletableNow(all).length} notice={notice} error={error}
      />
    ), status);
  }

  /** The newest images, for choosing one in an editor. */
  const recentImages = () =>
    Response.json({ images: listImages().slice(0, 30).map((image) => ({ url: mediaUrl(image.name), thumb: thumbUrl(mediaUrl(image.name)) })) });

  async function mediaDelete(request: Request) {
    const form = await request.formData();
    const name = String(form.get("name") ?? "");
    const back = form.get("unused") ? "&unused=1" : "";
    if (!isImageName(name)) return notFound();
    // Checked again here, not trusted from the page: it may have been put to use since.
    if (inCurrentUse(imageUses().get(name))) return mediaPage(request, "That image is in use, so it was not deleted. Take it out of the places listed first.", 409);
    if (!deleteImage(name)) return notFound();
    return redirect(`${admin}/media?deleted=1${back}`);
  }

  function mediaDeleteUnused() {
    const going = deletableNow(describeImages(listImages()));
    for (const entry of going) deleteImage(entry.name);
    return redirect(`${admin}/media?unused=1&deleted=${going.length}`);
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
    if (first === "pages" && config.pages && !second) return post ? redirect(`${admin}/posts/${createPost({ kind: "page" })}`) : postsPage(request, "page");
    if (first === "posts" && (config.posts || config.pages) && second && /^\d+$/.test(second) && rest.length === 0) return editor(request, Number(second));
    if (first === "posts" && (config.posts || config.pages) && second && /^\d+$/.test(second) && rest.length === 1 && rest[0] === "versions") return versions(request, Number(second));
    if (first === "trash" && (config.posts || config.pages) && !second && !post) return trashPage();
    if (first === "trash" && (config.posts || config.pages) && second && /^\d+$/.test(second) && rest.length === 0 && post) return trashAction(request, Number(second));
    if (first === "lists" && second && rest.length === 0) return list(request, second);
    if (first === "history" && !second && !post) return history(request);
    if (first === "media" && !second && !post) return isScript(request) ? recentImages() : mediaPage(request);
    if (first === "media" && second === "delete" && rest.length === 0 && post) return mediaDelete(request);
    if (first === "media" && second === "delete-unused" && rest.length === 0 && post) return mediaDeleteUnused();
    if (first === "history" && second && /^\d+$/.test(second) && rest.length === 0 && post) return history(request, Number(second));
    if (first === "account" && !second) return account(request);
    if (first === "people" && !second) return peopleScreen(request);
    if (first === "people" && second && /^\d+$/.test(second) && rest.length === 1 && (rest[0] === "remove" || rest[0] === "reset") && post) return peopleScreen(request, Number(second), rest[0]);
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
    const signedIn = isLogin ? null : sessionOf(request);
    if (!isLogin && !signedIn) {
      if (isScript(request)) return Response.json({ error: "signed-out" }, { status: 401 });
      // Back to the same page after logging in.
      const here = new URL(request.url);
      const next = request.method === "GET" && segments.length ? `?next=${encodeURIComponent(here.pathname + here.search)}` : "";
      return redirect(`${admin}/login${next}`);
    }

    const response = signedIn ? await viewer.run(signedIn, () => route(request, segments)) : await route(request, segments);
    const headers = new Headers(response.headers);
    // Versioned scripts say how long they keep; everything else behind the login is never stored.
    if (!isAsset(segments[0]) || segments.length !== 1) headers.set("Cache-Control", "no-store");
    headers.set("Content-Security-Policy", CSP);
    headers.set("X-Robots-Tag", "noindex");
    return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
  };
}
