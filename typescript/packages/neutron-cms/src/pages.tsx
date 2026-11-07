import type { ComponentChildren } from "preact";
import { assetUrl } from "./assets.js";
import { isPhotoList, TEXT, type ResolvedConfig, type ResolvedField, type ResolvedList } from "./config.js";
import { thumbUrl } from "./images.js";
import type { Item, Post } from "./store.js";

export const STATUS_LABEL = { draft: "Draft", live: "Live", scheduled: "Scheduled" } as const;
export type Status = keyof typeof STATUS_LABEL;

const DATE = new Intl.DateTimeFormat("en", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" });

export function Document(props: { config: ResolvedConfig; title: string; children: ComponentChildren }) {
  return (
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="robots" content="noindex" />
        <title>{`${props.title} - Neutron CMS`}</title>
        <link rel="icon" href="data:," />
        <link rel="stylesheet" href={assetUrl(props.config.path, "admin.css")} />
      </head>
      <body>{props.children}</body>
    </html>
  );
}

function Shell(props: { config: ResolvedConfig; active?: string; children: ComponentChildren }) {
  const { config, active } = props;
  const admin = config.path;
  const current = (name: string) => (active === name ? "page" : undefined);
  return (
    <>
      <header class="top">
        <a class="name" href={admin}>Neutron CMS</a>
        {config.posts ? <a href={admin} aria-current={current("posts")}>Posts</a> : null}
        {config.pages ? <a href={`${admin}/pages`} aria-current={current("pages")}>Pages</a> : null}
        {config.onPage.length ? <a href={`${admin}/edit`} aria-current={current("edit")}>Edit site</a> : null}
        {Object.entries(config.lists).filter(([key, list]) => list.ownPage || !config.onPage.includes(key)).map(([key, list]) => (
          <a key={key} href={`${admin}/lists/${key}`} aria-current={current(key)}>{list.label}</a>
        ))}
        {Object.keys(config.texts).length && !config.onPage.includes(TEXT) ? <a href={`${admin}/lists/${TEXT}`} aria-current={current(TEXT)}>Text</a> : null}
        <span class="spacer" />
        <a href="/" target="_blank" rel="noreferrer">View site</a>
        <form method="post" action={`${admin}/logout`}>
          <button class="link" type="submit">Log out</button>
        </form>
      </header>
      {props.children}
      <script defer src={assetUrl(admin, "admin.js")} />
    </>
  );
}

export function NotFoundPage(props: { config: ResolvedConfig }) {
  return (
    <Shell config={props.config}>
      <main class="page"><p class="muted">Not found.</p></main>
    </Shell>
  );
}

export function LoginPage(props: { config: ResolvedConfig; configured: boolean; error?: string }) {
  return (
    <main class="login">
      <h1>Log in</h1>
      {props.configured ? (
        <form method="post" action={`${props.config.path}/login`}>
          <label class="field">
            <span>Password</span>
            <input class="input" type="password" name="password" autocomplete="current-password" required autofocus />
          </label>
          {props.error ? <p class="error" role="alert">{props.error}</p> : null}
          <button class="button primary" type="submit">Log in</button>
        </form>
      ) : (
        <p>No password is set. Run <code>npx neutron-cms password</code>.</p>
      )}
    </main>
  );
}

export interface PostRow {
  id: number;
  title: string;
  date: string;
  status: Status;
}

export function PostsPage(props: { config: ResolvedConfig; posts: PostRow[]; kind?: "post" | "page" }) {
  const admin = props.config.path;
  const pages = props.kind === "page";
  return (
    <Shell config={props.config} active={pages ? "pages" : "posts"}>
      <main class="page narrow">
        <div class="page-head">
          <h1>{pages ? "Pages" : "Posts"}</h1>
          <form method="post" action={pages ? `${admin}/pages` : admin}>
            <button class="button primary" type="submit">{pages ? "New page" : "New post"}</button>
          </form>
        </div>
        {props.posts.length === 0 ? (
          <p class="muted">{pages ? "No pages yet." : "No posts yet."}</p>
        ) : (
          <table class="posts">
            <tbody>
              {props.posts.map((post) => (
                <tr key={post.id}>
                  <td><a href={`${admin}/posts/${post.id}`}>{post.title || "Untitled"}</a></td>
                  <td class="status">{post.status === "live" ? "" : post.status}</td>
                  <td class="date"><time dateTime={post.date}>{DATE.format(new Date(post.date))}</time></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </main>
    </Shell>
  );
}

export function EditorPage(props: {
  config: ResolvedConfig;
  post: Post;
  status: Status;
  saved: boolean;
  error?: string;
  fields?: Partial<Post>;
}) {
  const { config, post, status, saved, error } = props;
  const values = { ...post, ...props.fields };
  const live = status === "live";
  const admin = config.path;
  const FORM = "post-form";
  const design = post.kind === "page" && Object.hasOwn(config.designs, post.layout);
  const url = post.kind === "page" ? config.pageUrl : config.postUrl;
  return (
    <>
      <header class="top editor-top">
        <a class="back" href={post.kind === "page" ? `${admin}/pages` : admin} aria-label={post.kind === "page" ? "Back to pages" : "Back to posts"}>{"\u2190"}<span class="on-wide">&nbsp;{post.kind === "page" ? "Pages" : "Posts"}</span></a>
        <span class={`status ${status}`}>{STATUS_LABEL[status]}</span>
        <span class="state" data-state aria-live="polite">{saved ? "Saved" : ""}</span>
        <span class="spacer" />
        {design ? null : <button class="button quiet" type="button" data-add-image aria-label="Add image"><span class="on-wide">Add image</span><span class="on-phone">Image</span></button>}
        <button class="button quiet" type="button" data-toggle-meta aria-expanded={error ? "true" : "false"} aria-controls="post-settings">Settings</button>
        {design ? null : <button class="button" type="button" data-toggle-preview aria-pressed="false">Preview</button>}
        {status === "draft" ? (
          <button class="button primary" type="submit" form={FORM} name="intent" value="publish" data-publish>Publish</button>
        ) : (
          <button class="button primary" type="submit" form={FORM} name="intent" value="save" data-save>Update</button>
        )}
      </header>

      <form class="editor" id={FORM} method="post" noValidate data-editor={post.id} data-design={design ? "" : undefined} data-status={status} data-meta-open={error ? "" : undefined}>
        <input type="hidden" name="revision" value={post.updated_at} />
        <input type="hidden" name="date" value={post.published_at ?? ""} />
        {error ? <p class="notice error" role="alert">{error}</p> : null}
        <div class="notice" data-restore hidden>
          This browser has unsaved text for this post.
          <button class="button" type="button" data-restore-yes>Restore</button>
          <button class="button" type="button" data-restore-no>Discard</button>
        </div>

        <main class="writing">
          <textarea class="title" name="title" rows={1} placeholder="Add title" aria-label="Title" autofocus={!post.title}>{values.title}</textarea>
          {design ? (
            <div class="design-note">
              <p>This page has its own design. Its words and pictures are edited on the page itself, and nothing shows to visitors until you publish.</p>
              <a class="button primary" href={`${admin}/edit?page=${post.id}`}>Edit on the page</a>
              <textarea name="body" hidden aria-hidden="true" />
            </div>
          ) : (
            <>
              <p class="markdown-note" data-markdown-note hidden />
              <textarea class="body" name="body" placeholder="Start writing in Markdown. Images can be pasted or dropped here." aria-label="Body">{values.body}</textarea>
            </>
          )}
        </main>
        {/* Same origin so the site's images load past its Cross-Origin-Resource-Policy; scripts stay off. */}
        <iframe class="preview" title="Preview" sandbox="allow-same-origin" data-preview hidden />

        <aside class="meta" id="post-settings" aria-label="Post settings">
          <label class="field">
            <span>URL</span>
            <input class="input" name="slug" value={values.slug ?? ""} disabled={live} placeholder="from-the-title" spellcheck={false} autocapitalize="off" />
            {live ? <small>Fixed while live, so links keep working. Unpublish to change it.</small> : null}
          </label>
          <label class="field">
            <span>{status === "draft" ? "Publish on" : "Published on"}</span>
            <input class="input" type="datetime-local" name="date_local" value={post.published_at?.slice(0, 16) ?? ""} />
            {status === "draft" ? <small>Empty publishes now. A future date schedules it.</small> : null}
          </label>
          <label class="field">
            <span>Description</span>
            <textarea class="input" name="description" placeholder="Defaults to the opening text">{values.description}</textarea>
          </label>
          <label class="field">
            <span>Short label</span>
            <input class="input" name="label" value={values.label} placeholder="Defaults to the title" />
          </label>
          {design ? null : <button class="button" type="button" data-toggle-markdown>Edit as Markdown</button>}
          <footer>
            {live && post.slug ? <a href={url(post.slug)} target="_blank" rel="noreferrer">{post.kind === "page" ? "View page" : "View post"}</a> : null}
            {status === "draft" ? null : (
              <button class="link" type="submit" name="intent" value="unpublish" data-confirm={live ? "Take this post offline?" : undefined}>{live ? "Unpublish" : "Unschedule"}</button>
            )}
            <button class="link" type="submit" name="intent" value="delete" data-confirm="Delete this post?" formnovalidate>Delete</button>
          </footer>
        </aside>
      </form>
      <script defer src={assetUrl(admin, "admin.js")} />
      <script defer src={assetUrl(admin, "editor.js")} />
    </>
  );
}

function Input(props: { name: string; field: ResolvedField; value?: string }) {
  const { name, field, value } = props;
  if (field.type === "textarea") return <textarea class="input" name={name} placeholder={field.label} aria-label={field.label}>{value}</textarea>;
  if (field.type === "date") return <input class="input" type="date" name={name} value={value} aria-label={field.label} />;
  if (field.type === "image") {
    return (
      <div class="image-field">
        <img src={value ? thumbUrl(value) : ""} alt="" draggable={false} loading="lazy" />
        <input type="hidden" name={name} value={value} />
        <button class="button" type="button" data-row="upload">{value ? "Replace" : "Upload"}</button>
      </div>
    );
  }
  return <input class="input" name={name} inputMode={field.type === "url" ? "url" : undefined} value={value} placeholder={field.label} aria-label={field.label} />;
}

function Row(props: { list: ResolvedList; item?: Item }) {
  const entries = Object.entries(props.list.fields);
  const grid = isPhotoList(props.list);
  const sorted = Boolean(props.list.sortBy);
  return (
    <div class={entries.length > 2 ? "item stacked" : "item"}>
      <div class="item-fields">
        {entries.map(([key, field]) => grid
          ? <Input key={key} name={`f.${key}`} field={field} value={props.item?.[key]} />
          : (
            <label class="field" key={key}>
              <span>{field.label}</span>
              <Input name={`f.${key}`} field={field} value={props.item?.[key]} />
            </label>
          ))}
      </div>
      <div class="item-tools">
        {sorted ? null : <button class="link" type="button" data-row="up" aria-label="Move earlier">{grid ? "←" : "↑"}</button>}
        {sorted ? null : <button class="link" type="button" data-row="down" aria-label="Move later">{grid ? "→" : "↓"}</button>}
        <button class="link" type="button" data-row="remove">Remove</button>
      </div>
    </div>
  );
}

export function ListPage(props: {
  config: ResolvedConfig;
  listKey: string;
  items: Item[];
  revision: string;
  saved: boolean;
  error?: string;
}) {
  const { config, listKey, items, revision, saved, error } = props;
  const list = config.lists[listKey];
  const photos = isPhotoList(list);
  return (
    <Shell config={config} active={listKey}>
      <main class={photos ? "page" : "page narrow"}>
        <div class="page-head"><h1>{list.label}</h1></div>
        <form class="list-form" method="post" data-list>
          <input type="hidden" name="revision" value={revision} />
          <input type="hidden" name="confirm_empty" value="" />
          {error ? <p class="error" role="alert">{error}</p> : null}
          <div class={photos ? "rows photos" : "rows"} data-sorted={list.sortBy ? "" : undefined}>
            {items.map((item, index) => <Row key={index} list={list} item={item} />)}
          </div>
          <template><Row list={list} /></template>
          <div class="save">
            <button class="button primary" type="submit">Save</button>
            {photos
              ? <button class="button" type="button" data-row="add-photos">Add photos</button>
              : <button class="button" type="button" data-row="add">Add</button>}
            <span class="muted" data-state role="status">{saved ? "Saved" : photos ? "Or drop photos on this page" : ""}</span>
            <span class="spacer" />
            <button class="link" type="button" data-discard hidden>Discard changes</button>
          </div>
        </form>
      </main>
    </Shell>
  );
}

export function TextPage(props: { config: ResolvedConfig; texts: Record<string, string>; saved: boolean }) {
  const { config, texts, saved } = props;
  return (
    <Shell config={config} active={TEXT}>
      <main class="page narrow">
        <div class="page-head"><h1>Text</h1></div>
        <form class="text-form" method="post">
          {Object.entries(config.texts).map(([name, field]) => (
            <label class="field" key={name}>
              <span>{field.label}</span>
              <Input name={name} field={field} value={texts[name]} />
            </label>
          ))}
          <div class="save">
            <button class="button primary" type="submit">Save</button>
            {saved ? <span class="muted" role="status">Saved</span> : null}
          </div>
        </form>
      </main>
    </Shell>
  );
}

export function EditPage(props: { config: ResolvedConfig; src?: string; back?: { href: string; label: string } }) {
  const { config } = props;
  const admin = config.path;
  // A site with no blog opens on this page, so going back to its home would land here again.
  const otherList = Object.entries(config.lists).find(([key]) => !config.onPage.includes(key));
  const back = props.back ? props.back : config.posts ? { href: admin, label: "Posts" } : otherList ? { href: `${admin}/lists/${otherList[0]}`, label: otherList[1].label } : null;
  return (
    <>
      <header class="top editor-top edit-top">
        {back ? <a class="back" href={back.href} aria-label={`Back to ${back.label}`}>{"\u2190"}<span class="on-wide">&nbsp;{back.label}</span></a> : null}
        <span class="state" data-edit-state role="status" />
        <span class="spacer" />
        <div class="edit-tools" data-edit-tools hidden />
        {props.src ? null : <a class="button quiet on-wide" href="/" target="_blank" rel="noreferrer">View site</a>}
        <button class="button" type="button" data-edit-discard disabled>Discard</button>
        <button class="button primary" type="button" data-edit-save disabled>Save</button>
      </header>
      <iframe class="edit-frame" title="Your site" src={props.src ?? "/"} data-edit-frame />
      <script defer src={assetUrl(admin, "admin.js")} />
      <script defer src={assetUrl(admin, "edit.js")} />
    </>
  );
}
