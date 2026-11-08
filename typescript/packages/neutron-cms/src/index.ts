import { resolveConfig, type CmsConfig, type ListDefinition } from "./config.js";
import { createHandler, type RouteArgs } from "./handler.js";
import { isPreview } from "./auth.js";
import { allTexts, contentCount, designPage, ensureDesigns, isLive, listItems, livePostBySlug, livePosts, postBySlug, today, upcoming, type Item, type Post } from "./store.js";

export type { CmsConfig, Design, Field, FieldType, ListDefinition } from "./config.js";
export type { Item, Post } from "./store.js";
export { firstImage, renderMarkdown, summarize } from "./markdown.js";

/**
 * Everything a site needs from the CMS. Declare what can be edited, hand `GET` and `POST`
 * to the admin's route files, and read content in your loaders.
 */
export function defineCms<Lists extends Record<string, ListDefinition> = Record<string, ListDefinition>>(options: CmsConfig<Lists> = {}) {
  const config = resolveConfig(options);
  const handle = createHandler(config);
  let designsReady = false;
  const withDesigns = () => {
    if (!designsReady) ensureDesigns(config.designs);
    designsReady = true;
  };
  const dateField = (key: string) =>
    config.lists[key]?.sortBy ?? Object.entries(config.lists[key]?.fields ?? {}).find(([, field]) => field.type === "date")?.[0] ?? "date";

  return {
    GET: (args: RouteArgs) => handle(args),
    POST: (args: RouteArgs) => handle(args),

    /** Published posts, newest first. */
    posts: (): Post[] => livePosts(),
    /** One published post, or undefined. */
    post: (slug: string): Post | undefined => livePostBySlug(slug),
    /** Published pages at their own slugs, newest first. Designs with a fixed address are left out; ask `design(key)` for those. */
    pages: (): Post[] => {
      withDesigns();
      return livePosts("page").filter((page) => !(Object.hasOwn(config.designs, page.layout) && config.designs[page.layout].url));
    },
    /**
     * One published page, or undefined. Pass the request and a draft opened from the admin's
     * Edit site shows too, so a designed page can be edited before it goes live.
     */
    page: (slug: string, request?: Request): Post | undefined => {
      withDesigns();
      const found = livePostBySlug(slug, "page") ?? (request && isPreview("page", slug, new URL(request.url).searchParams.get("cms-preview")) ? postBySlug(slug, "page") : undefined);
      // A design with its own address answers only there.
      return found && config.designs[found.layout]?.url && Object.hasOwn(config.designs, found.layout) ? undefined : found;
    },
    /**
     * The page of a design with a fixed address, such as a homepage, when published or opened from
     * Edit site; otherwise undefined, and the route should answer 404.
     */
    design: (key: string, request?: Request): Post | undefined => {
      withDesigns();
      const found = designPage(key);
      if (!found) return undefined;
      if (isLive(found)) return found;
      return request && isPreview("design", key, new URL(request.url).searchParams.get("cms-preview")) ? found : undefined;
    },
    /** A list's items, in their saved order. */
    list: (key: keyof Lists & string): Item[] => listItems(key),
    /** A dated list's items through today, with undated ones kept. */
    upcoming: (key: keyof Lists & string): Item[] => upcoming(listItems(key), dateField(key), today(config.timeZone)),
    /** A text field's value, or an empty string. */
    text: (key: string): string => allTexts()[key] ?? "",
    /** Every text field at once, for a page that shows many. */
    texts: (): Record<string, string> => allTexts(),
    /** False when the database is missing, as it is on a lost volume. Use it as a deploy health check. */
    ready(): boolean {
      try {
        return contentCount() >= 0;
      } catch {
        return false;
      }
    },
  };
}
