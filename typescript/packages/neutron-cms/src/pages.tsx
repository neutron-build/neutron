import { createContext, type ComponentChildren } from "preact";
import { useContext } from "preact/hooks";
import { assetUrl } from "./assets.js";
import { MAX_PASSWORD, MIN_PASSWORD, type Account, type Person } from "./auth.js";
import { isPhotoList, TEXT, type ResolvedConfig, type ResolvedField, type ResolvedList } from "./config.js";
import { thumbUrl } from "./images.js";
import { isSafeUrl } from "./safe-url.js";
import type { StoredImage } from "./media.js";
import { withFields, type Item, type Post, type Save, type Snapshot, type Trashed, type Version } from "./store.js";

export const STATUS_LABEL = { draft: "Draft", live: "Live", scheduled: "Scheduled" } as const;
export type Status = keyof typeof STATUS_LABEL;

// Dates in the list are the site's dates, in its time zone, as the public pages show them.
const listDate = (config: ResolvedConfig, iso: string) =>
  new Intl.DateTimeFormat("en", { month: "short", day: "numeric", year: "numeric", timeZone: config.timeZone }).format(new Date(iso));

const fullDate = (config: ResolvedConfig, iso: string) =>
  new Intl.DateTimeFormat("en", { month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit", timeZone: config.timeZone }).format(new Date(iso));

const wordCount = (text: string) => text.trim().split(/\s+/).filter(Boolean).length;

export function Document(props: { config: ResolvedConfig; title: string; children: ComponentChildren }) {
  return (
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="robots" content="noindex" />
        <title>{`${props.title} - Neutron CMS`}</title>
        {props.config.icon ? <link rel="icon" href={props.config.icon} /> : null}
        <link rel="stylesheet" href={assetUrl(props.config.path, "admin.css")} />
      </head>
      <body>{props.children}</body>
    </html>
  );
}

/** Who is logged in, for the header. */
export const Viewer = createContext<Account | null>(null);

function Shell(props: { config: ResolvedConfig; active?: string; children: ComponentChildren }) {
  const { config, active } = props;
  const admin = config.path;
  const viewer = useContext(Viewer);
  const current = (name: string) => (active === name ? "page" : undefined);
  return (
    <>
      <header class="top">
        <a class="name" href={admin}>Neutron CMS</a>
        <nav aria-label="Sections">
        {config.posts ? <a href={admin} aria-current={current("posts")}>Posts</a> : null}
        {config.pages ? <a href={`${admin}/pages`} aria-current={current("pages")}>Pages</a> : null}
        {config.onPage.length && !Object.keys(config.designs).length ? <a href={`${admin}/edit`} aria-current={current("edit")}>Edit site</a> : null}
        {Object.entries(config.lists).filter(([key, list]) => list.ownPage || !config.onPage.includes(key)).map(([key, list]) => (
          <a key={key} href={`${admin}/lists/${key}`} aria-current={current(key)}>{list.label}</a>
        ))}
        {Object.keys(config.texts).length && !config.onPage.includes(TEXT) ? <a href={`${admin}/lists/${TEXT}`} aria-current={current(TEXT)}>Text</a> : null}
        <a href={`${admin}/media`} aria-current={current("media")}>Media</a>
        </nav>
        <span class="spacer" />
        <a href="/" target="_blank" rel="noreferrer">View site</a>
        {viewer ? (
          <details class="menu">
            <summary aria-current={active === "account" || active === "people" ? "page" : undefined}>{viewer.kind === "person" ? viewer.name : "Owner"}</summary>
            <div class="menu-items">
              {viewer.kind === "owner" ? <a href={`${admin}/people`} aria-current={current("people")}>People</a> : null}
              <a href={`${admin}/account`} aria-current={current("account")}>Account</a>
              <form method="post" action={`${admin}/logout`}>
                <button type="submit">Log out</button>
              </form>
            </div>
          </details>
        ) : (
          <form method="post" action={`${admin}/logout`}>
            <button class="link" type="submit">Log out</button>
          </form>
        )}
      </header>
      {props.children}
      <script defer src={assetUrl(admin, "admin.js")} />
    </>
  );
}

export function NotFoundPage(props: { config: ResolvedConfig }) {
  return (
    <Shell config={props.config}>
      <main class="page"><p class="empty-state">Not found.</p></main>
    </Shell>
  );
}

export function LoginPage(props: { config: ResolvedConfig; configured: boolean; named?: boolean; name?: string; error?: string; next?: string | null }) {
  return (
    <main class="login">
      <h1>Log in</h1>
      {props.next && !props.error ? <p class="muted">Your session ended. Log in to pick up where you were. Unsaved text is kept in this browser.</p> : null}
      {props.configured ? (
        <form method="post" action={`${props.config.path}/login`}>
          {props.next ? <input type="hidden" name="next" value={props.next} /> : null}
          {props.named ? (
            <label class="field">
              <span>Name</span>
              <input class="input" name="name" value={props.name ?? ""} autocomplete="username" autocapitalize="off" spellcheck={false} autofocus />
              <small>Leave empty if you are the owner.</small>
            </label>
          ) : null}
          <label class="field">
            <span>Password</span>
            <input class="input" type="password" name="password" autocomplete="current-password" required autofocus={!props.named} />
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

export function AccountPage(props: { config: ResolvedConfig; account: Account; changed: boolean; error?: string }) {
  const { config, account } = props;
  const owner = account.kind === "owner";
  return (
    <Shell config={config} active="account">
      <main class="page narrow">
        <div class="page-head"><h1>Account</h1></div>
        <p class="muted account-who">{owner ? "You are the owner." : <>You are logged in as <strong>{account.name}</strong>.</>}</p>
        {props.changed ? <p class="undo" role="status">Password changed. You are still logged in here, and logged out everywhere else.</p> : null}
        <form class="account-form" method="post">
          {owner ? null : <input type="hidden" name="username" value={account.name} autocomplete="username" />}
          <h2>Change password</h2>
          <label class="field">
            <span>Current password</span>
            <input class="input" type="password" name="current" autocomplete="current-password" required />
          </label>
          <label class="field">
            <span>New password</span>
            <input class="input" type="password" name="password" autocomplete="new-password" minLength={MIN_PASSWORD} maxLength={MAX_PASSWORD} required />
            <small>At least {MIN_PASSWORD} characters. A few unrelated words make a good one.</small>
          </label>
          <label class="field">
            <span>New password again</span>
            <input class="input" type="password" name="repeat" autocomplete="new-password" minLength={MIN_PASSWORD} maxLength={MAX_PASSWORD} required />
          </label>
          {props.error ? <p class="error" role="alert">{props.error}</p> : null}
          <button class="button primary" type="submit">Change password</button>
          <p class="muted account-note">
            {owner
              ? <>Changing it logs you out on your other devices. Forgot it? Whoever runs the site can run <code>npx neutron-cms password</code> and set the new <code>CMS_PASSWORD_HASH</code>. That replaces the password set here.</>
              : "Changing it logs you out on your other devices. Forgot it? Ask the owner for a new one."}
          </p>
        </form>
      </main>
    </Shell>
  );
}

export function PeoplePage(props: {
  config: ResolvedConfig;
  people: Person[];
  /** A password just made, shown this once. */
  given?: { name: string; password: string; added: boolean };
  removed?: boolean;
  error?: string;
}) {
  const { config, given } = props;
  const admin = config.path;
  return (
    <Shell config={config} active="people">
      <main class="page narrow">
        <div class="page-head"><h1>People</h1></div>
        <p class="muted people-intro">People you add log in with their own name and password. They can edit everything, but only you can add or remove people.</p>
        {given ? (
          <div class="given" role="status">
            <p>{given.added ? `${given.name} is added.` : `${given.name} has a new password and is logged out.`} Give them these. The password is shown only now.</p>
            <dl>
              <dt>Name</dt><dd><code>{given.name}</code></dd>
              <dt>Password</dt><dd><code>{given.password}</code></dd>
            </dl>
            <button class="button" type="button" data-copy-text={given.password}>Copy password</button>
          </div>
        ) : null}
        {props.removed ? <p class="undo" role="status">Removed, and logged out.</p> : null}
        <form class="people-add" method="post" action={`${admin}/people`}>
          <label class="field">
            <span>Name</span>
            <input class="input" name="name" required maxLength={100} placeholder="sam or sam@example.com" autocomplete="off" autocapitalize="off" spellcheck={false} />
            <small>Letters, numbers, dots, dashes and underscores, or an email address.</small>
          </label>
          <button class="button primary" type="submit">Add person</button>
        </form>
        {props.error ? <p class="error people-error" role="alert">{props.error}</p> : null}
        {props.people.length === 0 ? (
          <p class="empty-state">Only you can log in now.</p>
        ) : (
          <table class="posts people">
            <tbody>
              {props.people.map((person) => (
                <tr key={person.id}>
                  <td><span class="person-name">{person.name}</span><small class="muted">Added {listDate(config, person.created_at)}</small></td>
                  <td class="actions"><div>
                    <form method="post" action={`${admin}/people/${person.id}/reset`}>
                      <button class="link" type="submit" data-confirm={`Give ${person.name} a new password? The old one stops working and they are logged out.`}>New password</button>
                    </form>
                    <form method="post" action={`${admin}/people/${person.id}/remove`}>
                      <button class="link danger" type="submit" data-confirm={`Remove ${person.name}? They are logged out and can't log in again.`}>Remove</button>
                    </form>
                  </div></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </main>
    </Shell>
  );
}

export interface PostRow {
  id: number;
  title: string;
  tags?: string[];
  date: string;
  status: Status;
}

export function PostsPage(props: {
  config: ResolvedConfig;
  posts: PostRow[];
  kind?: "post" | "page";
  deleted?: Trashed;
  trash?: number;
  /** What was searched for, if anything. */
  query?: string;
  /** How many there are in all, which decides whether searching is offered. */
  total?: number;
  page?: number;
  pageCount?: number;
}) {
  const admin = props.config.path;
  const pages = props.kind === "page";
  const { query = "", page = 1, pageCount = 1 } = props;
  const base = pages ? `${admin}/pages` : admin;
  const here = (at: number) => `${base}${[query ? `q=${encodeURIComponent(query)}` : "", at > 1 ? `page=${at}` : ""].filter(Boolean).join("&").replace(/^./, "?$&")}`;
  return (
    <Shell config={props.config} active={pages ? "pages" : "posts"}>
      <main class="page narrow">
        <div class="page-head">
          <h1>{pages ? "Pages" : "Posts"}</h1>
          <form method="post" action={pages ? `${admin}/pages` : admin}>
            <button class="button primary" type="submit">{pages ? "New page" : "New post"}</button>
          </form>
        </div>
        {props.deleted ? (
          <form class="undo" method="post" action={`${admin}/trash/${props.deleted.id}`} role="status">
            <span>"{props.deleted.title || "Untitled"}" moved to Trash.</span>
            <button class="button" type="submit">Undo</button>
          </form>
        ) : null}
        {(props.total ?? 0) > 10 || query ? (
          <form class="search" method="get" action={base} role="search">
            <input class="input" type="search" name="q" value={query} placeholder={pages ? "Find a page" : "Find a post"} aria-label={pages ? "Find a page" : "Find a post"} />
            <button class="button" type="submit">Find</button>
            {query ? <a href={base}>Show all</a> : null}
          </form>
        ) : null}
        {props.posts.length === 0 ? (
          <p class="empty-state">{query ? `Nothing matches "${query}".` : pages ? "No pages yet." : "No posts yet."}</p>
        ) : (
          <table class="posts">
            <tbody>
              {props.posts.map((post) => (
                <tr key={post.id}>
                  <td>
                    <a href={`${admin}/posts/${post.id}`}>{post.title || "Untitled"}</a>
                    {post.tags?.length ? <span class="post-tags">{post.tags.map((tag) => <a key={tag} href={`${base}?q=${encodeURIComponent(tag)}`}>{tag}</a>)}</span> : null}
                  </td>
                  <td class={`status ${post.status}`}>{post.status}</td>
                  <td class="date"><time dateTime={post.date}>{listDate(props.config, post.date)}</time></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {pageCount > 1 ? (
          <nav class="pager" aria-label="Pages">
            {page > 1 ? <a href={here(page - 1)}>Newer</a> : <span />}
            <span class="muted">Page {page} of {pageCount}</span>
            {page < pageCount ? <a href={here(page + 1)}>Older</a> : <span />}
          </nav>
        ) : null}
        {props.trash ? <p class="trash-link"><a href={`${admin}/trash`}>Trash ({props.trash})</a></p> : null}
      </main>
    </Shell>
  );
}

export function TrashPage(props: { config: ResolvedConfig; items: Trashed[] }) {
  const { config, items } = props;
  const admin = config.path;
  return (
    <Shell config={config}>
      <main class="page narrow">
        <div class="page-head"><h1>Trash</h1></div>
        {items.length === 0 ? <p class="empty-state">The Trash is empty.</p> : (
          <>
            <table class="posts trash">
              <tbody>
                {items.map((item) => (
                  <tr key={item.id}>
                    <td><span class="trash-title">{item.title || "Untitled"}</span><small class="muted">{item.kind === "page" ? "Page" : "Post"}, deleted {listDate(config, item.deleted_at)}</small></td>
                    <td class="actions">
                      <form method="post" action={`${admin}/trash/${item.id}`}>
                        <button class="link danger" type="submit" name="intent" value="forever" data-confirm={`Delete "${item.title || "Untitled"}" for good? This can't be undone.`}>Delete forever</button>
                        <button class="button" type="submit" name="intent" value="restore">Restore</button>
                      </form>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p class="trash-link muted">Items are deleted for good 30 days after they land here.</p>
          </>
        )}
      </main>
    </Shell>
  );
}

export function VersionsPage(props: { config: ResolvedConfig; post: Post; versions: Version[]; selected?: Version; document: string; error?: string }) {
  const { config, post, versions, selected, error } = props;
  const admin = config.path;
  const here = `${admin}/posts/${post.id}`;
  return (
    <>
      <header class="top editor-top">
        <a class="back" href={here} aria-label="Back to the editor">{"\u2190"}<span class="on-wide">&nbsp;Editor</span></a>
        <span class="state">{post.title || "Untitled"}</span>
        <span class="spacer" />
        {selected ? (
          <form method="post">
            <input type="hidden" name="version" value={selected.id} />
            <input type="hidden" name="revision" value={post.updated_at} />
            <button class="button primary" type="submit">Restore this version</button>
          </form>
        ) : null}
      </header>
      {error ? <p class="notice error" role="alert">{error}</p> : null}
      {selected ? (
        <div class="versions">
          <nav class="version-list" aria-label="Versions">
            {versions.map((version) => (
              <a key={version.id} href={`${here}/versions?v=${version.id}`} aria-current={version.id === selected.id ? "true" : undefined}>
                <time dateTime={version.saved_at}>{fullDate(config, version.saved_at)}</time>
                <small>{version.title !== post.title ? `${version.title || "Untitled"}, ` : ""}{wordCount(version.body)} words</small>
              </a>
            ))}
          </nav>
          <iframe class="version-preview" title="This version" sandbox="allow-same-origin" srcdoc={props.document} />
        </div>
      ) : (
        <main class="page narrow"><p class="empty-state">No earlier versions yet. One is kept when you update a published post, and every ten minutes while you write a draft.</p></main>
      )}
    </>
  );
}

export function EditorPage(props: {
  config: ResolvedConfig;
  post: Post;
  status: Status;
  saved: boolean;
  error?: string;
  fields?: Partial<Post>;
  /** The revision the form was opened at, after a refused save. */
  revision?: string;
  /** How many earlier versions there are to go back to. */
  versions?: number;
}) {
  const { config, post, status, saved, error } = props;
  const values = { ...post, ...props.fields };
  const live = status === "live";
  const admin = config.path;
  const FORM = "post-form";
  const url = post.kind === "page" ? config.pageUrl : config.postUrl;
  return (
    <>
      <header class="top editor-top">
        <a class="back" href={post.kind === "page" ? `${admin}/pages` : admin} aria-label={post.kind === "page" ? "Back to pages" : "Back to posts"}>{"\u2190"}<span class="on-wide">&nbsp;{post.kind === "page" ? "Pages" : "Posts"}</span></a>
        <span class={`status ${status}`}>{STATUS_LABEL[status]}</span>
        <span class="state" data-state aria-live="polite">{saved ? "Saved" : ""}</span>
        <span class="spacer" />
        <button class="button quiet wide-add" type="button" data-add-image>Add image</button>
        <button class="button quiet wide-add" type="button" data-add-file>Add file</button>
        <button class="button quiet wide-add" type="button" data-add-embed>Embed</button>
        <button class="button quiet phone-add" type="button" data-add-menu>Add</button>
        <button class="button quiet" type="button" data-toggle-meta aria-expanded={error ? "true" : "false"} aria-controls="post-settings">Settings</button>
        <button class="button" type="button" data-toggle-preview aria-pressed="false">Preview</button>
        {status !== "draft" ? (
          <button class="button quiet on-wide" type="submit" form={FORM} name="intent" value="unpublish" data-confirm={live ? `Take this ${post.kind} offline? You can publish it again any time.` : undefined}>{live ? "Unpublish" : "Unschedule"}</button>
        ) : null}
        {status === "draft" ? (
          <button class="button primary" type="submit" form={FORM} name="intent" value="publish" data-publish>Publish</button>
        ) : (
          <button class="button primary" type="submit" form={FORM} name="intent" value="save" data-save>Update</button>
        )}
      </header>

      <form class="editor" id={FORM} method="post" noValidate data-editor={post.id} data-status={status} data-meta-open={error ? "" : undefined}>
        <input type="hidden" name="revision" value={props.revision ?? post.updated_at} />
        <input type="hidden" name="date" value={post.published_at ?? ""} />

        <div class="notice" data-restore hidden>
          This browser has unsaved text for this post.
          <button class="button" type="button" data-restore-yes>Restore</button>
          <button class="button" type="button" data-restore-no>Discard</button>
        </div>

        <main class="writing">
          <textarea class="title" name="title" rows={1} placeholder="Add title" aria-label="Title" autofocus={!post.title}>{values.title}</textarea>
          <p class="markdown-note" data-markdown-note hidden />
          <textarea class="body" name="body" placeholder="Start writing in Markdown. Images can be pasted or dropped here." aria-label="Body">{values.body}</textarea>
        </main>
        {/* Same origin so the site's images load past its Cross-Origin-Resource-Policy; scripts stay off. */}
        <iframe class="preview" title="Preview" sandbox="allow-same-origin" data-preview hidden />

        <aside class="meta" id="post-settings" aria-label="Post settings">
          {/* Here, not above the text: the settings open on an error, and on a phone they cover the page. */}
          {error ? <p class="error" role="alert">{error}</p> : null}
          <label class="field">
            <span>URL</span>
            <input class="input" name="slug" value={values.slug ?? ""} disabled={live} placeholder="from-the-title" spellcheck={false} autocapitalize="off" />
            {live ? <small>Fixed while live, so links keep working. Unpublish to change it.</small> : null}
          </label>
          <label class="field">
            <span>{status === "draft" ? "Publish on" : "Published on"}</span>
            <input class="input" type="datetime-local" name="date_local" value={post.published_at?.slice(0, 16) ?? ""} />
            <small>{status === "draft" ? "Empty publishes now. A future date schedules it. " : ""}In this browser's time zone.</small>
          </label>
          <label class="field">
            <span>Description</span>
            <textarea class="input" name="description" placeholder="Defaults to the opening text">{values.description}</textarea>
          </label>
          <label class="field">
            <span>Short label</span>
            <input class="input" name="label" value={values.label} placeholder="Defaults to the title" />
          </label>
          {post.kind === "post" ? (
            <label class="field">
              <span>Tags</span>
              <input class="input" name="tags" value={values.tags} placeholder="Games, Music" autocapitalize="off" />
              <small>Separate with commas.</small>
            </label>
          ) : null}
          <button class="button" type="button" data-toggle-markdown>Edit as Markdown</button>
          <footer>
            {live && post.slug ? <a href={url(post.slug)} target="_blank" rel="noreferrer">{post.kind === "page" ? "View page" : "View post"}</a> : null}
            {props.versions ? <a href={`${admin}/posts/${post.id}/versions`}>Versions ({props.versions})</a> : null}
            {status !== "draft" ? (
              <button class="link on-phone" type="submit" name="intent" value="unpublish" data-confirm={live ? `Take this ${post.kind} offline? You can publish it again any time.` : undefined}>{live ? "Unpublish" : "Unschedule"}</button>
            ) : null}
            <button class="link danger" type="submit" name="intent" value="delete" formnovalidate>Move to Trash</button>
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
  if (field.type === "file") {
    return (
      <div class="file-field">
        <input class="input" name={name} inputMode="url" value={value} placeholder="Upload, or paste a link" aria-label={field.label} spellcheck={false} />
        <button class="button" type="button" data-row="file">Upload</button>
      </div>
    );
  }
  return <input class="input" name={name} inputMode={field.type === "url" ? "url" : undefined} value={value} placeholder={field.label} aria-label={field.label} />;
}

/** A field with its name. An image or file field isn't a label: that would name its button after the field and send clicks on the picture to it. */
function Labelled(props: { field: ResolvedField; children: ComponentChildren }) {
  const { field } = props;
  return field.type === "image" || field.type === "file"
    ? <div class="field" role="group" aria-label={field.label}><span>{field.label}</span>{props.children}</div>
    : <label class="field"><span>{field.label}</span>{props.children}</label>;
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
            <Labelled key={key} field={field}>
              <Input name={`f.${key}`} field={field} value={props.item?.[key]} />
            </Labelled>
          ))}
      </div>
      <div class="item-tools">
        {sorted ? null : <button class="link" type="button" data-row="up" aria-label="Move earlier">{grid ? "←" : "↑"}</button>}
        {sorted ? null : <button class="link" type="button" data-row="down" aria-label="Move later">{grid ? "→" : "↓"}</button>}
        <button class="link danger" type="button" data-row="remove">Remove</button>
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
  /** Whether there is an earlier save of this list to go back to. */
  history?: boolean;
}) {
  const { config, listKey, items, revision, saved, error } = props;
  const list = config.lists[listKey];
  const photos = isPhotoList(list);
  return (
    <Shell config={config} active={listKey}>
      <main class={photos ? "page" : "page narrow"}>
        <div class="page-head"><h1>{list.label}</h1>{props.history ? <HistoryLink config={config} only={listKey} /> : null}</div>
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

function HistoryLink(props: { config: ResolvedConfig; only: string }) {
  return <a class="head-link" href={`${props.config.path}/history?only=${props.only}`}>History</a>;
}

export function TextPage(props: { config: ResolvedConfig; texts: Record<string, string>; saved: boolean; history?: boolean }) {
  const { config, texts, saved } = props;
  return (
    <Shell config={config} active={TEXT}>
      <main class="page narrow">
        <div class="page-head"><h1>Text</h1>{props.history ? <HistoryLink config={config} only={TEXT} /> : null}</div>
        <form class="text-form" method="post">
          {Object.entries(config.texts).map(([name, field]) => (
            <Labelled key={name} field={field}>
              <Input name={name} field={field} value={texts[name]} />
            </Labelled>
          ))}
          <div class="save">
            <button class="button primary" type="submit">Save</button>
            <span class="muted" data-state role="status">{saved ? "Saved" : ""}</span>
          </div>
        </form>
      </main>
    </Shell>
  );
}

/** A designed page's state, when Edit site is showing one: it's published from the same bar. */
export interface DesignedPage {
  id: number;
  status: Status;
  revision: string;
  permanent: boolean;
}

export function EditPage(props: { config: ResolvedConfig; src?: string; back?: { href: string; label: string }; page?: DesignedPage; notice?: string; history?: boolean }) {
  const { config } = props;
  const admin = config.path;
  // A site with no blog opens on this page, so going back to its home would land here again.
  const otherList = Object.entries(config.lists).find(([key]) => !config.onPage.includes(key));
  const back = props.back ? props.back : config.posts ? { href: admin, label: "Posts" } : otherList ? { href: `${admin}/lists/${otherList[0]}`, label: otherList[1].label } : null;
  return (
    <>
      <header class="top editor-top edit-top">
        {back ? <a class="back" href={back.href} aria-label={`Back to ${back.label}`}>{"\u2190"}<span class="on-wide">&nbsp;{back.label}</span></a> : null}
        {props.page ? <span class={`status ${props.page.status}`}>{STATUS_LABEL[props.page.status]}</span> : null}
        <span class="state" data-edit-state role="status" />
        <span class="spacer" />
        <div class="edit-tools" data-edit-tools hidden />
        {props.src ? null : <a class="button quiet on-wide" href="/" target="_blank" rel="noreferrer">View site</a>}
        <a class="button quiet" href={`${admin}/history`} data-edit-history hidden={!props.history}>History</a>
        <button class="button" type="button" data-edit-discard disabled>Discard</button>
        <button class="button primary" type="button" data-edit-save disabled>Save</button>
        {props.page && !(props.page.permanent && props.page.status === "live") ? (
          <form class="publish-form" method="post" action={`${admin}/posts/${props.page.id}`}>
            <input type="hidden" name="revision" value={props.page.revision} />
            {props.page.status === "draft" ? (
              <button class="button" type="submit" name="intent" value="publish" data-publish>Publish</button>
            ) : (
              <button class="button quiet" type="submit" name="intent" value="unpublish" data-confirm="Take this page offline? You can publish it again any time.">{props.page.status === "live" ? "Unpublish" : "Unschedule"}</button>
            )}
          </form>
        ) : null}
      </header>
      {props.notice ? <main class="page narrow"><p class="notice error" role="alert">{props.notice}</p></main> : <iframe class="edit-frame" title="Your site" data-src={props.src ?? "/"} data-edit-frame />}
      <script defer src={assetUrl(admin, "admin.js")} />
      {props.notice ? null : <script defer src={assetUrl(admin, "edit.js")} />}
    </>
  );
}

/** A save of lists and texts, with what it left behind and what is there today. */
export interface HistoryEntry {
  save: Save;
  after: Snapshot;
  now: Snapshot;
}

const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;
const sameItems = (a: Item, b: Item) => JSON.stringify(a) === JSON.stringify(b);
const listName = (config: ResolvedConfig, key: string) => (Object.hasOwn(config.lists, key) ? config.lists[key].label : `${key} (no longer on the site)`);
const textName = (config: ResolvedConfig, key: string) => (Object.hasOwn(config.texts, key) ? config.texts[key].label : `${key} (no longer on the site)`);

/** In words: "Gallery: 2 photos removed, Tagline". */
function describe(config: ResolvedConfig, entry: HistoryEntry): string {
  const parts: string[] = [];
  for (const [key, before] of Object.entries(entry.save.before.lists)) {
    const known = Object.hasOwn(config.lists, key);
    const noun = known && isPhotoList(config.lists[key]) ? "photo" : "item";
    const change = (entry.after.lists[key]?.length ?? before.length) - before.length;
    parts.push(change > 0 ? `${listName(config, key)}: ${plural(change, noun)} added` : change < 0 ? `${listName(config, key)}: ${plural(-change, noun)} removed` : listName(config, key));
  }
  const texts = Object.keys(entry.save.before.texts).map((key) => textName(config, key));
  parts.push(...(texts.length > 3 ? [...texts.slice(0, 2), `${texts.length - 2} more texts`] : texts));
  return parts.join(", ");
}

function SavedRow(props: { list?: ResolvedList; item: Item; note?: string }) {
  const { list, item } = props;
  const fields = list ? Object.entries(list.fields) : Object.keys(item).map((key) => [key, undefined] as const);
  const images = fields.filter(([key, field]) => field?.type === "image" && item[key] && isSafeUrl(item[key]));
  const words = fields.filter(([key, field]) => field?.type !== "image" && item[key]).map(([key]) => item[key]);
  return (
    <li class={props.note ? "changed" : undefined}>
      {images.map(([key]) => <img key={key} src={thumbUrl(item[key])} alt="" loading="lazy" />)}
      <span>{words.join(" \u00b7 ") || (images.length ? "" : "Empty")}</span>
      {props.note ? <small>{props.note}</small> : null}
    </li>
  );
}

function SavedList(props: { config: ResolvedConfig; listKey: string; before: Item[]; now?: Item[] }) {
  const { config, listKey, before } = props;
  const list = Object.hasOwn(config.lists, listKey) ? config.lists[listKey] : undefined;
  const now = props.now ?? [];
  const added = now.filter((item) => !before.some((old) => sameItems(old, item)));
  const same = before.length === now.length && before.every((item, index) => sameItems(item, now[index]));
  const noun = list && isPhotoList(list) ? "photo" : "item";
  return (
    <section>
      <h2>{listName(config, listKey)}</h2>
      <p class="muted">
        {!list ? "This list is no longer on the site, so it isn't restored. " : null}
        {same ? "Same as now." : `${plural(before.length, noun)} then, ${now.length} now.`}
      </p>
      {before.length ? (
        <ul class="history-rows">
          {before.map((item, index) => <SavedRow key={index} list={list} item={item} note={list && !now.some((other) => sameItems(other, item)) ? "Not there now" : undefined} />)}
        </ul>
      ) : <p class="history-value muted">Empty</p>}
      {list && added.length ? (
        <>
          <h3>Added since</h3>
          <ul class="history-rows">{added.map((item, index) => <SavedRow key={index} list={list} item={item} />)}</ul>
        </>
      ) : null}
    </section>
  );
}

function SavedText(props: { config: ResolvedConfig; textKey: string; before: string | null; now: string | null }) {
  const { config, textKey } = props;
  const known = Object.hasOwn(config.texts, textKey);
  const before = props.before ?? "";
  const now = props.now ?? "";
  return (
    <section>
      <h2>{textName(config, textKey)}</h2>
      <p class={before ? "history-value" : "history-value muted"}>{before || "Empty"}</p>
      <p class="muted">
        {!known ? "This text is no longer on the site, so it isn't restored." : before === now ? "Same as now." : <>Now: <span class="history-now">{now || "empty"}</span></>}
      </p>
    </section>
  );
}

export function HistoryPage(props: { config: ResolvedConfig; entries: HistoryEntry[]; selected?: HistoryEntry; only: string; restored?: number }) {
  const { config, entries, selected, only } = props;
  const admin = config.path;
  const query = (extra: string) => {
    const parts = [only ? `only=${only}` : "", extra].filter(Boolean);
    return parts.length ? `?${parts.join("&")}` : "";
  };
  const onlyName = only === TEXT ? "Text" : only ? listName(config, only) : "";
  // Only what the site still declares is put back, and only if it differs from now.
  const restorable = selected && (
    Object.entries(selected.save.before.lists).some(([key, items]) => Object.hasOwn(config.lists, key) && JSON.stringify(withFields(items, Object.keys(config.lists[key].fields))) !== JSON.stringify(selected.now.lists[key])) ||
    Object.entries(selected.save.before.texts).some(([key, value]) => Object.hasOwn(config.texts, key) && (value ?? "") !== (selected.now.texts[key] ?? ""))
  );
  const listKeys = selected ? Object.keys(selected.save.before.lists) : [];
  const textKeys = selected ? Object.keys(selected.save.before.texts) : [];
  return (
    <Shell config={config}>
      <main class="page">
        <div class="page-head">
          <h1>History</h1>
          {only ? <a class="head-link" href={`${admin}/history`}>Show everything</a> : null}
        </div>
        {props.restored ? (
          <form class="undo" method="post" action={`${admin}/history/${props.restored}${query("")}`} role="status">
            <span>Restored.</span>
            <button class="button" type="submit">Undo</button>
          </form>
        ) : null}
        {only ? <p class="muted history-filter">Showing saves of {onlyName}.</p> : null}
        {!selected ? (
          <p class="empty-state">Nothing to go back to yet. Every save of {only ? onlyName : "a list or text"} is kept here, the last 50.</p>
        ) : (
          <div class="history">
            <nav class="history-list" aria-label="Saves">
              {entries.map((entry) => (
                <a key={entry.save.id} href={`${admin}/history${query(`s=${entry.save.id}`)}`} aria-current={entry === selected ? "true" : undefined}>
                  <time dateTime={entry.save.saved_at}>{fullDate(config, entry.save.saved_at)}</time>
                  <small>{describe(config, entry)}</small>
                </a>
              ))}
            </nav>
            <div class="history-detail">
              <div class="history-head">
                <div>
                  <h2>Before the save of {fullDate(config, selected.save.saved_at)}</h2>
                  <p class="muted">{restorable ? "Restore puts these back as they were. Everything else stays as it is." : "Nothing to restore: this is how it is now."}</p>
                </div>
                {restorable ? (
                  <form method="post" action={`${admin}/history/${selected.save.id}${query("")}`}>
                    <button class="button primary" type="submit">Restore</button>
                  </form>
                ) : null}
              </div>
              {listKeys.map((key) => <SavedList key={key} config={config} listKey={key} before={selected.save.before.lists[key]} now={selected.now.lists[key]} />)}
              {textKeys.map((key) => <SavedText key={key} config={config} textKey={key} before={selected.save.before.texts[key]} now={selected.now.texts[key]} />)}
            </div>
          </div>
        )}
      </main>
    </Shell>
  );
}

export interface MediaEntry extends StoredImage {
  /** A name the owner gave it. */
  title?: string;
  url: string;
  thumb: string;
  /** Where it is used now, in words, with a link where there is one. */
  places: { label: string; href?: string }[];
  /** When it is not used now: what an undo could bring it back in. */
  kept: string[];
}

const fileSize = (bytes: number) => (bytes < 1_000_000 ? `${Math.max(1, Math.round(bytes / 1000))} KB` : `${(bytes / 1_000_000).toFixed(1)} MB`);

export function MediaPage(props: {
  config: ResolvedConfig;
  images: MediaEntry[];
  total: number;
  unusedCount: number;
  unused: boolean;
  /** What was searched for, if anything. */
  query?: string;
  page: number;
  pages: number;
  /** How many "Delete all unused" would remove. */
  deletable: number;
  notice?: string;
  error?: string;
}) {
  const { config, images, unused, page, pages } = props;
  const admin = config.path;
  const query = props.query ?? "";
  const where = [unused ? "unused=1" : "", query ? `q=${encodeURIComponent(query)}` : ""].filter(Boolean).join("&");
  const here = (at: number) => `${admin}/media${[where, at > 1 ? `page=${at}` : ""].filter(Boolean).join("&").replace(/^./, "?$&")}`;
  return (
    <Shell config={config} active="media">
      <main class="page" data-media>
        <div class="page-head">
          <h1>Media</h1>
          <button class="button primary" type="button" data-media-upload>Upload</button>
        </div>
        {props.notice ? <p class="undo" role="status">{props.notice}</p> : null}
        {props.error ? <p class="notice error" role="alert">{props.error}</p> : null}
        <div class="media-bar">
          <nav class="media-filter" aria-label="Show">
            <a href={`${admin}/media`} aria-current={unused ? undefined : "page"}>All ({props.total})</a>
            <a href={`${admin}/media?unused=1`} aria-current={unused ? "page" : undefined}>Unused ({props.unusedCount})</a>
          </nav>
          <span class="muted" data-state role="status">Or drop images and files on this page</span>
          <span class="spacer" />
          {props.total > 12 || query ? (
            <form class="search" method="get" action={`${admin}/media`} role="search">
              {unused ? <input type="hidden" name="unused" value="1" /> : null}
              <input class="input" type="search" name="q" value={query} placeholder="Find by name or where used" aria-label="Find in Media" />
              <button class="button" type="submit">Find</button>
            </form>
          ) : null}
          {unused && props.deletable ? (
            <form method="post" action={`${admin}/media/delete-unused`}>
              <button class="button" type="submit" data-confirm={`Delete ${props.deletable === 1 ? "1 unused file" : `${props.deletable} unused files`} for good? Those kept for undo, and any added in the last day, stay. This can't be undone.`}>
                Delete all unused ({props.deletable})
              </button>
            </form>
          ) : null}
        </div>
        {unused && props.unusedCount > props.deletable ? <p class="muted media-note">"Delete all unused" leaves anything kept for undo, and any added in the last day, in case they are in work not saved yet.</p> : null}
        {images.length === 0 ? (
          <p class="empty-state">{query ? `Nothing matches "${query}".` : unused ? "Nothing unused." : "Nothing here yet. Upload images or files, or drop them on this page."}</p>
        ) : (
          <ul class="media-grid">
            {images.map((image) => (
              <li key={image.name}>
                {image.file ? (
                  <a class="media-thumb media-file" href={image.url}><span>{image.file.slice(image.file.lastIndexOf(".") + 1)}</span></a>
                ) : (
                  <a class="media-thumb" href={image.url} target="_blank" rel="noreferrer"><img src={image.thumb} alt="" loading="lazy" /></a>
                )}
                {image.title || image.file ? <p class="media-name">{image.title || image.file}</p> : null}
                <p class="media-size">{image.width ? `${image.width} \u00d7 ${image.height}, ` : ""}{fileSize(image.bytes)}</p>
                <ul class="media-uses">
                  {image.places.map((place) => <li key={place.label}>{place.href ? <a href={place.href}>{place.label}</a> : place.label}</li>)}
                  {!image.places.length ? <li class="muted">{image.kept.length ? `Not used. Kept for undo in ${image.kept.join(" and ")}.` : "Not used"}</li> : null}
                </ul>
                <div class="media-actions">
                  <button class="link" type="button" data-copy={image.url}>Copy address</button>
                  <form method="post" action={`${admin}/media/rename`}>
                    <input type="hidden" name="name" value={image.name} />
                    <input type="hidden" name="back" value={[where, page > 1 ? `page=${page}` : ""].filter(Boolean).join("&")} />
                    <input type="hidden" name="title" value={image.title ?? ""} />
                    <button class="link" type="submit" data-rename={image.title || image.file?.replace(/\.\w+$/, "") || ""}>{image.title ? "Rename" : "Name"}</button>
                  </form>
                  {image.places.length ? null : (
                    <form method="post" action={`${admin}/media/delete`}>
                      <input type="hidden" name="name" value={image.name} />
                      {unused ? <input type="hidden" name="unused" value="1" /> : null}
                      <button class="link danger" type="submit" data-confirm={image.kept.length
                        ? `Delete this ${image.file ? "file" : "image"} for good? It is only kept for undo in ${image.kept.join(" and ")}, so restoring that would show a broken ${image.file ? "link" : "image"}. This can't be undone.`
                        : `Delete this ${image.file ? "file" : "image"} for good? This can't be undone.`}>Delete</button>
                    </form>
                  )}
                </div>
              </li>
            ))}
          </ul>
        )}
        {pages > 1 ? (
          <nav class="pager" aria-label="Pages">
            {page > 1 ? <a href={here(page - 1)}>Newer</a> : <span />}
            <span class="muted">Page {page} of {pages}</span>
            {page < pages ? <a href={here(page + 1)}>Older</a> : <span />}
          </nav>
        ) : null}
      </main>
    </Shell>
  );
}
