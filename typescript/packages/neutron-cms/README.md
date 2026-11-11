# @neutron-build/cms

A small CMS for Neutron sites: posts, photo galleries, events and any other list, edited at `/n-admin`.

It stores everything in one SQLite file and one folder, needs no database server, and sends visitors no JavaScript. Content can be edited by clicking it on the page. Layout stays in your code, so there is no page builder.

Requires Node 22.16 or newer. Run the site with `neutron-ts start`, because the `docker` preset drops the route type this uses (as of Neutron 0.5.4).

## Set up

```bash
npm install @neutron-build/cms
npx neutron-cms password   # prints a password and the hash to set as CMS_PASSWORD_HASH
```

Declare what can be edited in `src/cms.ts`:

```ts
import { defineCms } from "@neutron-build/cms";

export default defineCms({
  lists: {
    gallery: { fields: { image: "image", caption: "text" } },
    events: { sortBy: "date", fields: { title: "text", date: "date", location: "text" } },
    links: { fields: { label: "text", url: "url" } },
  },
});
```

Add the admin's two route files, `src/routes/n-admin/index.ts` and `src/routes/n-admin/[...path].ts`, both with:

```ts
import cms from "../../cms.ts";

export const config = { mode: "app" };
export const { GET, POST } = cms;
```

Log in at `/n-admin` with that password. It belongs to the owner.

## People and passwords

The owner can let others edit without sharing a password. **People** in the admin adds a person by name (letters, numbers, dots, dashes and underscores, or an email address; upper and lower case are the same) and shows a strong password once, to pass on. Once anyone is added, the login asks for a name too; the owner leaves it empty, as before. People can edit all content but not manage people. The owner can give a person a new password or remove them, and either logs them out at once.

Everyone can change their own password under **Account**: at least 12 characters, at most 200. It logs that account out on other devices and keeps the current one.

The owner's password changed there is kept in the database and counts only while `CMS_PASSWORD_HASH` stays the same. If the owner forgets it, run `npx neutron-cms password` and set the new hash: that password works, the one set in the admin is removed, and everyone is logged out. A person who forgets theirs asks the owner for a new one.

## Show content

Read content in a loader:

```tsx
import { Photo } from "@neutron-build/cms/photo";
import cms from "../cms.ts";

export const config = { mode: "app", hydrate: false };

export async function loader() {
  return { photos: cms.list("gallery"), events: cms.upcoming("events") };
}

export default function Page({ data }) {
  return data.photos.map((photo) => <Photo src={photo.image} alt={photo.caption} sizes="33vw" />);
}
```

| Call | Returns |
|---|---|
| `cms.posts()` | Published posts, newest first |
| `cms.post(slug)` | One published post |
| `cms.pages()`, `cms.page(slug, request?)` | Published pages; with the request, a designed draft opened from the admin too |
| `cms.design(key, request?)` | The page of a design with a fixed address, such as a homepage; undefined while unpublished, so answer 404 |
| `cms.list(key)` | A list's items, in saved order |
| `cms.upcoming(key)` | A dated list through today |
| `cms.text(key)`, `cms.texts()` | One text field, or all of them at once |
| `cms.ready()` | False if the database is missing |

Post bodies are Markdown, written in a visual editor: headings, lists, links and images show as they will look, a toolbar appears over selected text, typed Markdown such as `## ` or `**bold**` turns into formatting, and clicking an image asks for its description. The editor reads with the same parser the site renders with and refuses any round trip that would change the page, so HTML and anything else it can't draw stays as an editable source block; a post it can't show exactly opens as Markdown. Settings has a switch to write in Markdown instead. `renderMarkdown`, `summarize` and `firstImage` are exported for showing them; links and images that could run code (`javascript:` and the like) lose their link, and `renderMarkdown(text, { html: false })` shows raw HTML as text, for Markdown you don't control. Your site chooses the post URLs, so post pages, feeds and sitemaps are ordinary routes.

**Embed** in the editor's bar takes a YouTube, Vimeo or Spotify link, or an `<iframe>` from any https site, and shows the player in the post; a player link pasted alone on an empty line becomes one too, with Keep as link to undo it. It is stored as plain iframe HTML. Your site's own Content-Security-Policy decides which players visitors see, so if you set one, allow those hosts in `frame-src`, for example `frame-src 'self' https://www.youtube-nocookie.com https://player.vimeo.com https://open.spotify.com`.

Posts and Pages offer a search, by title and text, once there are more than ten, and show 50 at a time.

Uploaded images live in `media/` and are served at `/n-admin/media/`. **Media** in the admin shows them all, newest first, with where each is used, a filter for unused ones, upload by drop, Copy address and Delete. An image in a post, page, list or text can't be deleted; one only in the Trash, an old version or list history can, with a warning. "Add image" in the editor, and image fields, offer the newest images to reuse as well as Upload. Images that aren't uploads, such as files in your `public/` folder, aren't listed.

**Files.** Media also takes PDF, ZIP, 7z, gz and tar, audio (MP3, M4A, WAV, OGG, FLAC), video (MP4, WebM, MOV), text, CSV, JSON, EPUB, Word, Excel and PowerPoint files, up to 100 MB each. They upload in small parts, so no server limit needs raising. "Add file" in the editor uploads one, or picks one already there, and links the selected words to it (or adds a link under the file's name). A file is always served as a download, never opened as a page of your site. On the site the link is `<a class="file" download>`, so your CSS can show it as a button: style `.file`.

`<Photo>` serves a thumbnail or the full image to suit the screen, reserves space so the page doesn't jump, and loads lazily. Set `height: auto` on it if your CSS sets its width.

## Click to edit

Mark what's on the page, list it in `onPage`, and a new **Edit site** page opens your real site with those parts editable. Those lists leave the menu. Words are typed in place, images are clicked, and anything the page doesn't show, such as a link's address, appears in a small toolbar along with Add, Remove and move.

```ts
defineCms({
  texts: { tagline: "text" },
  lists: { team: { fields: { name: "text", photo: "image", site: "url" } } },
  onPage: ["team", "text"],
});
```

```tsx
<p data-cms-text="tagline">{tagline}</p>
<ul>
  {team.map((person, index) => (
    <li data-cms-item={`team.${index}`}>
      <a href={person.site}><Photo data-cms="photo" src={person.photo} alt={person.name} /></a>
      <h3 data-cms="name">{person.name}</h3>
    </li>
  ))}
</ul>
```

- `data-cms-text` marks a text field, `data-cms-item` marks one list item by its index in `cms.list()`, and `data-cms` marks a field inside that item.
- `data-cms-list="team"` on the element that holds a list's items lets an empty list be filled: while it is empty, it shows an Add button that adds the first item, asking for the image first in a photo list. Once the item is saved, the page shows it in your own markup.
- A marked element should hold only that field's text.
- The editor loads your site in a frame, and Neutron forbids framing by default. Allow your own site to frame itself in `neutron.config.ts`, and use `frame-ancestors 'self'` if you set a Content-Security-Policy:

  ```ts
  routes: { headers: [{ source: "/:path*", headers: { "X-Frame-Options": "SAMEORIGIN" } }] }
  ```

- Visitors get the same page as ever. The attributes do nothing, and the editing script only runs in the admin.
- Use `hydrate: false` on these pages, so a client render can't overwrite an edit in progress.
- It changes content, not layout. A filtered list, such as one from `cms.upcoming`, can edit its fields here but not add or move items, so edit those on their list page.

## Fields and lists

Field types are `text`, `textarea`, `url`, `image` and `date`. A field is a type, or `{ type, label, required }` when it needs more. Labels default to the key.

- **Photo lists:** a list with an image and at most one other field is a grid. Drop files anywhere on the page and drag to reorder.
- **Dated lists:** `sortBy` keeps a list in date order. `cms.upcoming` keeps an item up through its own day, using `timeZone`.
- **Text fields:** `texts: { tagline: "text" }` adds a Text page for one-off values.

## Options

| Option | Default | Purpose |
|---|---|---|
| `path` | `/n-admin` | Where the route files are |
| `posts` | `true` | Set `false` on a site with no blog |
| `pages` | `false`, or `true` when `designs` is set | Standalone pages, edited like posts and kept out of the blog |
| `designs` | `{}` | Pages with their own design, by key: a title, or `{ title, url?, published?, permanent? }`. Each is listed under Pages and opens framed for click-to-edit, with Publish in the same bar. `url` fixes its address (a homepage at `/`); `permanent` keeps it always published |
| `postUrl` | `/<slug>` | Where a post lives, for the admin's links |
| `pageUrl` | same as `postUrl` | Where a page lives |
| `icon` | your site's `/favicon.ico`, if it has one | The admin's browser tab icon, as a URL such as `/img/icon.png` |
| `reservedSlugs` | none | URLs a post may not take, usually your other pages |
| `previewCss` | none | Your post styles as a string, so previews match the site |
| `timeZone` | `UTC` | When a date has passed |
| `onPage` | none | Lists edited by clicking them on the page. Use `"text"` for the text fields |

A list also takes `label`, its name in the admin, and `ownPage: true`, which keeps its page in the admin menu even when it is edited on the site. A post with a future "Publish on" date is scheduled: it appears on the site when that time comes, with nothing to run. `@neutron-build/cms/store` exports the storage functions for seed and migration scripts.

## Environment

| Variable | Purpose |
|---|---|
| `CMS_PASSWORD_HASH` | The owner's password, from `npx neutron-cms password`. Changing it logs everyone out and undoes an owner password set in the admin. Without it, nobody can log in |
| `SITE_URL` | Your public origin. Required in production, because admin saves must come from it |
| `CMS_DATA_DIR` | Where `site.db` and `media/` live. Required in production, normally a mounted volume |

## Production data

Everything is in `CMS_DATA_DIR`. Production never creates the database, so a lost volume fails loudly instead of showing an empty site:

```bash
npx neutron-cms init             # once, on the new volume
npx neutron-cms backup out.db    # a consistent copy of the database; copy media/ alongside it
```

Serve `cms.ready()` from a health route to stop a deploy that has lost its data. Each post keeps its last 50 versions, restorable from Settings, Versions: every update of a published post, and one every ten minutes while a draft is written. Deleting moves a post to the Trash, with an Undo, for 30 days. Every save of a list or text field keeps what it changed, the last 50 saves in all, in the `saves` table; History, linked from each list, the Text page and Edit site, shows them and restores one, and a restore can itself be undone.

## Testing

`pnpm run test` runs the unit tests. `pnpm run test:browser` drives the admin in a real browser: it starts a small site around the handler on a free local port, with a fresh data folder that is removed afterwards, and runs every scenario in Chrome and then WebKit, headless, one browser at a time. A scenario fails on an uncaught page error, a console error from the admin, or a screen that scrolls sideways.

It uses `playwright-core`, which downloads no browsers: Chrome must be installed, and WebKit must already be in Playwright's cache (`npx playwright-core install webkit`). Add `-- chrome` or `-- webkit` for one browser, `-- --grep media` for matching scenarios, and set `SHOTS=folder` to keep the screenshots of the newer screens, light and dark, at desktop and phone size.

## Limits

The admin trusts your public site. Any script running on the site's own address, including an analytics or embed script, runs with the owner's login when the owner visits. That is normal for a CMS on the same domain, WordPress included. For real separation, serve the admin from its own subdomain.


- Two kinds of account: the owner, and people who can edit everything but not manage people.
- Draft preview links from Edit site last two hours, and end when any password changes or a person is removed.
- Uploads are limited to 2000px on the long side, and the original is not kept.
- Images are not deleted when content stops using them. Delete them from Media; "Delete all unused" leaves those kept for undo and any added in the last day.
- If you disallow `/n-admin` in `robots.txt`, add `Allow: /n-admin/media/` so search engines can see your images.
- Set `SITE_URL` to the address people actually use, or every save is refused.

## License

MIT
