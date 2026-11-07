import { resolveConfig, type CmsConfig, type ListDefinition } from "./config.js";
import { createHandler, type RouteArgs } from "./handler.js";
import { allTexts, contentCount, listItems, livePostBySlug, livePosts, today, upcoming, type Item, type Post } from "./store.js";

export type { CmsConfig, Field, FieldType, ListDefinition } from "./config.js";
export type { Item, Post } from "./store.js";
export { firstImage, renderMarkdown, summarize } from "./markdown.js";

/**
 * Everything a site needs from the CMS. Declare what can be edited, hand `GET` and `POST`
 * to the admin's route files, and read content in your loaders.
 */
export function defineCms<Lists extends Record<string, ListDefinition> = Record<string, ListDefinition>>(options: CmsConfig<Lists> = {}) {
  const config = resolveConfig(options);
  const handle = createHandler(config);
  const dateField = (key: string) =>
    config.lists[key]?.sortBy ?? Object.entries(config.lists[key]?.fields ?? {}).find(([, field]) => field.type === "date")?.[0] ?? "date";

  return {
    GET: (args: RouteArgs) => handle(args),
    POST: (args: RouteArgs) => handle(args),

    /** Published posts, newest first. */
    posts: (): Post[] => livePosts(),
    /** One published post, or undefined. */
    post: (slug: string): Post | undefined => livePostBySlug(slug),
    /** A list's items, in their saved order. */
    list: (key: keyof Lists & string): Item[] => listItems(key),
    /** A dated list's items through today, with undated ones kept. */
    upcoming: (key: keyof Lists & string): Item[] => upcoming(listItems(key), dateField(key), today(config.timeZone)),
    /** A text field's value, or an empty string. */
    text: (key: string): string => allTexts()[key] ?? "",
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
