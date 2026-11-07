export type FieldType = "text" | "textarea" | "url" | "image" | "date";

export interface Field {
  type?: FieldType;
  /** Defaults to the field's key, capitalized. */
  label?: string;
  required?: boolean;
}

export interface ListDefinition {
  /** Defaults to the list's key, capitalized. */
  label?: string;
  /** Each is a type, such as `"image"`, or a full field. */
  fields: Record<string, FieldType | Field>;
  /** A date field to keep the list in order by, instead of arranging it by hand. */
  sortBy?: string;
}

export interface CmsConfig<Lists extends Record<string, ListDefinition> = Record<string, ListDefinition>> {
  /** Where the route files are. Default `/n-admin`. */
  path?: string;
  /** Set false on a site with no blog. */
  posts?: boolean;
  /** Where a post lives on the site, for the admin's links. Default `/<slug>`. */
  postUrl?: (slug: string) => string;
  /** URLs a post may not take because a page already answers them. */
  reservedSlugs?: string[];
  /** Your post styles as a string, so previews match the site. */
  previewCss?: string;
  /** Decides when a date has passed, for example "America/Phoenix". Default UTC. */
  timeZone?: string;
  lists?: Lists;
  texts?: Record<string, FieldType | Field>;
  /** Lists edited by clicking them on the page, so they leave the menu. Use "text" for the text fields. */
  onPage?: string[];
}

export type ResolvedField = Required<Pick<Field, "label" | "type">> & { required: boolean };

export interface ResolvedList {
  label: string;
  fields: Record<string, ResolvedField>;
  sortBy?: string;
}

export interface ResolvedConfig {
  path: string;
  posts: boolean;
  postUrl: (slug: string) => string;
  reservedSlugs: string[];
  previewCss: string;
  timeZone: string;
  lists: Record<string, ResolvedList>;
  texts: Record<string, ResolvedField>;
  onPage: string[];
}

/** The address of the page that edits text fields, so a list cannot use it. */
export const TEXT = "text";

const title = (key: string) => key[0].toUpperCase() + key.slice(1);

function resolveField(key: string, field: FieldType | Field): ResolvedField {
  const full = typeof field === "string" ? { type: field } : field;
  return { label: full.label ?? title(key), type: full.type ?? "text", required: full.required ?? false };
}

const resolveFields = (fields: Record<string, FieldType | Field>) =>
  Object.fromEntries(Object.entries(fields).map(([key, field]) => [key, resolveField(key, field)]));

/** A wrong config stops the site at startup, not when someone opens the page. */
export function resolveConfig(config: CmsConfig): ResolvedConfig {
  const path = (config.path ?? "/n-admin").replace(/\/+$/, "");
  const lists = Object.fromEntries(
    Object.entries(config.lists ?? {}).map(([key, list]) => {
      if (key === TEXT) throw new Error(`neutron-cms: a list cannot be called "${TEXT}"`);
      const fields = resolveFields(list.fields);
      if (Object.keys(fields).length === 0) throw new Error(`neutron-cms: list "${key}" has no fields`);
      if (list.sortBy && fields[list.sortBy]?.type !== "date") {
        throw new Error(`neutron-cms: list "${key}" sorts by "${list.sortBy}", which is not a date field`);
      }
      return [key, { label: list.label ?? title(key), fields, sortBy: list.sortBy }];
    }),
  );
  const onPage = config.onPage ?? [];
  for (const key of onPage) {
    if (key !== TEXT && !Object.hasOwn(lists, key)) throw new Error(`neutron-cms: onPage names "${key}", which is not a list`);
  }
  const firstSegment = path.split("/").filter(Boolean)[0];
  return {
    path,
    posts: config.posts ?? true,
    postUrl: config.postUrl ?? ((slug) => `/${slug}`),
    reservedSlugs: [...new Set([...(firstSegment ? [firstSegment] : []), ...(config.reservedSlugs ?? [])])],
    previewCss: config.previewCss ?? "",
    timeZone: config.timeZone ?? "UTC",
    lists,
    texts: resolveFields(config.texts ?? {}),
    onPage,
  };
}

/** A list that is mostly pictures is edited as a grid and filled by dropping files on it. */
export function isPhotoList(list: ResolvedList): boolean {
  const fields = Object.values(list.fields);
  return fields.length <= 2 && fields.some((field) => field.type === "image");
}
