import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import { evaluate } from "@mdx-js/mdx";
import matter from "gray-matter";
import { Marked } from "marked";
import type { NeutronMarkdownConfig } from "../config.js";
import { markedShikiExtension } from "./syntax-highlight.js";
import { sanitizeHtml } from "./sanitize.js";

export { sanitizeHtml } from "./sanitize.js";
export type { SanitizeOptions } from "./sanitize.js";
import YAML from "yaml";
import { h } from "preact";
import type * as preact from "preact";
import * as preactJsxRuntime from "preact/jsx-runtime";
import { renderToString } from "preact-render-to-string";
import {
  ZodArray,
  ZodBoolean,
  ZodDate,
  ZodDefault,
  ZodEffects,
  ZodEnum,
  ZodLiteral,
  ZodNullable,
  ZodNumber,
  ZodObject,
  ZodOptional,
  ZodString,
  ZodTypeAny,
  ZodUnion,
  z,
} from "zod";

export { z };

// SECURITY: Validate against prototype pollution
function hasPrototypePollution(obj: any, visited = new WeakSet()): boolean {
  if (!obj || typeof obj !== "object") return false;

  // Prevent infinite recursion on circular references
  if (visited.has(obj)) return false;
  visited.add(obj);

  // Check current level
  if (
    obj.hasOwnProperty("__proto__") ||
    obj.hasOwnProperty("constructor") ||
    obj.hasOwnProperty("prototype")
  ) {
    return true;
  }

  // Recursively check nested objects and arrays
  for (const key in obj) {
    if (obj.hasOwnProperty(key)) {
      const value = obj[key];
      if (value && typeof value === "object") {
        if (hasPrototypePollution(value, visited)) {
          return true;
        }
      }
    }
  }

  return false;
}

type CollectionType = "content" | "data";

export interface CollectionDefinition<TData = unknown> {
  type: CollectionType;
  schema: z.ZodType<TData>;
  live?: boolean; // NEW: Enable runtime loading
  loader?: () => Promise<TData[]>; // NEW: Runtime data loader
  cacheTtl?: number; // NEW: Cache TTL for live collections (ms)
  /**
   * Sanitize rendered HTML through a real parser-based sanitizer. Enable for
   * collections whose content is NOT fully trusted (CMS, user submissions,
   * remote sources). Local authored files are trusted by default. Requires the
   * optional `sanitize-html` dependency.
   */
  sanitize?: boolean;
}

export interface DefineCollectionOptions<TData = unknown> {
  type?: CollectionType;
  schema: z.ZodType<TData>;
  live?: boolean; // NEW: Enable runtime loading
  loader?: () => Promise<TData[]>; // NEW: Runtime data loader
  cacheTtl?: number; // NEW: Cache TTL in milliseconds (default: 60000)
  /** Sanitize rendered HTML for untrusted content. See CollectionDefinition. */
  sanitize?: boolean;
}

export interface ContentCollectionMap {}

export interface CollectionEntry<TData = unknown> {
  id: string;
  slug: string;
  collection: string;
  body: string;
  html: string;
  data: TData;
  filePath: string;
  sourceType: "markdown" | "mdx" | "html" | "data";
  sanitize?: boolean;
  render: () => Promise<{ Content: preact.FunctionComponent<any> }>;
}

export interface PrepareContentCollectionsOptions {
  rootDir?: string;
  writeManifest?: boolean;
  writeTypes?: boolean;
  manifestPath?: string;
}

type CollectionConfigMap = Record<string, CollectionDefinition<unknown>>;

interface CollectionStore {
  collections: Record<string, Array<CollectionEntry<unknown>>>;
  generatedTypes: string;
}

interface SerializedCollectionEntry {
  id: string;
  slug: string;
  collection: string;
  body: string;
  html: string;
  data: unknown;
  filePath: string;
  sourceType?: "markdown" | "mdx" | "html" | "data";
  sanitize?: boolean;
}

interface CacheRecord {
  fingerprint: string;
  store: CollectionStore;
}

const CONTENT_CONFIG_CANDIDATES = [
  "src/content/config.ts",
  "src/content/config.js",
  "src/content/config.mjs",
  "src/content/config.cjs",
];

const CONTENT_MANIFEST_DIST_NAME = ".neutron-content.json";
const COLLECTION_FILE_EXTENSIONS = new Set([".md", ".mdx", ".html", ".htm", ".json", ".yaml", ".yml"]);
const cacheByRoot = new Map<string, CacheRecord>();

// In-memory, content-addressed render cache. Body rendering (KaTeX/Shiki/MDX)
// is the expensive part of serving a markdown/MDX entry; caching it lets a
// long-running SSR server reuse the result for repeated content instead of
// recompiling per request, and bounds memory to a working set rather than the
// whole collection (an unbounded cache is what caused the original build OOM).
//
// The key is content-addressed — hash(sourceType + body + renderer version) —
// so identical bodies across files share one entry, and changed bodies miss.
// The markdown config (themes, plugins, extensions) is deliberately NOT part of
// the key: a config change produces a new object reference, and
// setActiveMarkdownConfig clears this cache alongside the store cache, so the
// config is invariant for any single cache lifetime. RENDER_CACHE_VERSION
// guards against rendering-logic changes for the future on-disk cache layer
// (an in-memory cache already dies with the process); bump it whenever
// renderMarkup/compileMdx/syntax-highlight output changes.
const RENDER_CACHE_VERSION = "1";
const DEFAULT_RENDER_CACHE_SIZE = 256;

interface RenderedMarkup {
  html: string;
  renderFactory?: () => Promise<{ Content: preact.FunctionComponent<any> }>;
}

// Insertion-ordered Map used as an LRU: a cache hit re-inserts the key to move
// it to the most-recently-used end; inserts past the cap evict the oldest key.
const renderCache = new Map<string, RenderedMarkup>();
let renderCacheLimit = DEFAULT_RENDER_CACHE_SIZE;
// Count of actual body renders (cache misses). Exposed only for tests, to prove
// repeated rendering of identical content compiles once.
let renderCacheMisses = 0;

/** Test-only view of the render cache. Not part of the public API. */
export function __renderCacheStatsForTest(): {
  size: number;
  limit: number;
  misses: number;
} {
  return { size: renderCache.size, limit: renderCacheLimit, misses: renderCacheMisses };
}

/** Test-only reset of the render cache and its counters. */
export function __resetRenderCacheForTest(): void {
  clearRenderCache();
  renderCacheMisses = 0;
}

function setRenderCacheLimit(size: number | undefined): void {
  const next = typeof size === "number" && Number.isFinite(size) && size >= 0
    ? Math.floor(size)
    : DEFAULT_RENDER_CACHE_SIZE;
  if (next === renderCacheLimit) return;
  renderCacheLimit = next;
  if (renderCache.size > renderCacheLimit) clearRenderCache();
}

function clearRenderCache(): void {
  renderCache.clear();
}

function renderCacheKey(sourceType: "markdown" | "mdx", body: string): string {
  return createHash("sha256")
    .update(RENDER_CACHE_VERSION)
    .update("\0")
    .update(sourceType)
    .update("\0")
    .update(body)
    .digest("hex");
}

function getCachedMarkup(key: string): RenderedMarkup | undefined {
  if (renderCacheLimit === 0) return undefined;
  const hit = renderCache.get(key);
  if (hit === undefined) return undefined;
  // Move to MRU position.
  renderCache.delete(key);
  renderCache.set(key, hit);
  return hit;
}

function setCachedMarkup(key: string, value: RenderedMarkup): void {
  if (renderCacheLimit === 0) return;
  if (renderCache.has(key)) {
    renderCache.delete(key);
  } else if (renderCache.size >= renderCacheLimit) {
    const oldest = renderCache.keys().next().value;
    if (oldest !== undefined) renderCache.delete(oldest);
  }
  renderCache.set(key, value);
}

export function defineCollection<TData>(
  options: DefineCollectionOptions<TData>
): CollectionDefinition<TData> {
  return {
    type: options.type ?? "content",
    schema: options.schema,
    live: options.live,
    loader: options.loader,
    cacheTtl: options.cacheTtl,
    sanitize: options.sanitize,
  };
}

export async function getCollection<TName extends keyof ContentCollectionMap & string>(
  name: TName,
  filter?: (
    entry: CollectionEntry<ContentCollectionMap[TName]>
  ) => boolean
): Promise<Array<CollectionEntry<ContentCollectionMap[TName]>>>;
export async function getCollection(
  name: string,
  filter?: (entry: CollectionEntry<unknown>) => boolean
): Promise<Array<CollectionEntry<unknown>>>;
export async function getCollection(
  name: string,
  filter?: (entry: CollectionEntry<unknown>) => boolean
): Promise<Array<CollectionEntry<unknown>>> {
  const store = await loadCollectionStore(process.cwd());
  const entries = store.collections[name];
  if (!entries) {
    throw new Error(`Unknown content collection "${name}".`);
  }
  return filter ? entries.filter(filter) : entries;
}

export async function getEntry<TName extends keyof ContentCollectionMap & string>(
  name: TName,
  slug: string
): Promise<CollectionEntry<ContentCollectionMap[TName]> | undefined>;
export async function getEntry(
  name: string,
  slug: string
): Promise<CollectionEntry<unknown> | undefined>;
export async function getEntry(
  name: string,
  slug: string
): Promise<CollectionEntry<unknown> | undefined> {
  const entries = await getCollection(name);
  return entries.find((entry) => entry.slug === slug);
}

export async function prepareContentCollections(
  options: PrepareContentCollectionsOptions = {}
): Promise<void> {
  const rootDir = path.resolve(options.rootDir || process.cwd());
  const writeManifest = options.writeManifest !== false;
  const writeTypes = options.writeTypes !== false;
  const manifestPath =
    options.manifestPath || path.join(rootDir, "dist", CONTENT_MANIFEST_DIST_NAME);

  const store = await loadCollectionStore(rootDir, {
    force: true,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    markdownConfig: (options as any).markdownConfig ?? activeMarkdownConfig,
  });

  if (writeManifest) {
    const serializableCollections = await toSerializableCollections(store.collections);
    await fsp.mkdir(path.dirname(manifestPath), { recursive: true });
    await fsp.writeFile(
      manifestPath,
      JSON.stringify({
        collections: serializableCollections,
        generatedAt: new Date().toISOString(),
      }),
      "utf-8"
    );
  }

  if (writeTypes) {
    const typesPath = path.join(rootDir, "src", "content", ".neutron-content.d.ts");
    await fsp.mkdir(path.dirname(typesPath), { recursive: true });
    await fsp.writeFile(typesPath, store.generatedTypes, "utf-8");
  }
}

// Module-level fallback for the markdown config. Set by the CLI during
// build/dev startup so callers of getCollection (which don't receive a
// markdownConfig as an argument) still pick up user-supplied marked
// extensions, remark/rehype plugins, etc.
let activeMarkdownConfig: NeutronMarkdownConfig | undefined;

export function setActiveMarkdownConfig(config: NeutronMarkdownConfig | undefined) {
  // No-op on identical references. Layout and SSR-bootstrap files commonly
  // call this at module-load time, so every HMR pass would otherwise wipe
  // the content cache and force every collection file to be re-read,
  // re-parsed, and re-rendered — turning a layout edit into a multi-GB
  // memory churn cycle. Genuine config swaps still produce a fresh
  // reference and correctly invalidate.
  if (activeMarkdownConfig === config) return;
  activeMarkdownConfig = config;
  cacheByRoot.clear();
  // A config change can alter rendered output (themes, plugins, extensions), so
  // the content-addressed render cache — whose key intentionally omits the
  // config — must be dropped in lockstep with the store cache.
  clearRenderCache();
  setRenderCacheLimit(config?.renderCacheSize);
}

export function getActiveMarkdownConfig(): NeutronMarkdownConfig | undefined {
  return activeMarkdownConfig;
}

async function loadCollectionStore(
  rootDir: string,
  options: { force?: boolean; markdownConfig?: NeutronMarkdownConfig } = {}
): Promise<CollectionStore> {
  const fingerprint = await computeContentFingerprint(rootDir);
  const cached = cacheByRoot.get(rootDir);
  if (!options.force && cached && cached.fingerprint === fingerprint) {
    return cached.store;
  }
  const effectiveConfig = options.markdownConfig ?? activeMarkdownConfig;

  const config = await loadContentConfig(rootDir);
  if (!config) {
    const manifestStore = await loadManifestStore(rootDir);
    if (manifestStore) {
      cacheByRoot.set(rootDir, { fingerprint, store: manifestStore });
      return manifestStore;
    }
    const emptyStore: CollectionStore = {
      collections: {},
      generatedTypes: [
        "// Auto-generated by Neutron. Do not edit.",
        'declare module "@neutron-build/core/content" {',
        "  interface ContentCollectionMap {}",
        "}",
        "",
        "export {};",
        "",
      ].join("\n"),
    };
    cacheByRoot.set(rootDir, { fingerprint, store: emptyStore });
    return emptyStore;
  }

  const collections: Record<string, Array<CollectionEntry<unknown>>> = {};
  for (const [collectionName, definition] of Object.entries(config)) {
    collections[collectionName] = await readCollectionEntries(
      rootDir,
      collectionName,
      definition,
      effectiveConfig
    );
  }

  const generatedTypes = generateCollectionTypes(config);
  const store: CollectionStore = {
    collections,
    generatedTypes,
  };

  cacheByRoot.set(rootDir, { fingerprint, store });
  return store;
}

async function loadContentConfig(rootDir: string): Promise<CollectionConfigMap | null> {
  const configPath = await resolveContentConfigPath(rootDir);
  if (!configPath) {
    return null;
  }

  const module = await importContentConfigModule(configPath);
  const raw = module.collections as Record<string, unknown> | undefined;
  if (!raw || typeof raw !== "object") {
    throw new Error(
      `Content config "${configPath}" must export a "collections" object.`
    );
  }

  const config: CollectionConfigMap = {};
  for (const [name, entry] of Object.entries(raw)) {
    if (!entry || typeof entry !== "object") {
      throw new Error(`Invalid collection definition for "${name}".`);
    }
    const candidate = entry as Partial<CollectionDefinition<unknown>>;
    if (!candidate.schema) {
      throw new Error(`Collection "${name}" is missing a Zod schema.`);
    }
    config[name] = {
      type: candidate.type ?? "content",
      schema: candidate.schema as z.ZodType<unknown>,
    };
  }

  return config;
}

async function importContentConfigModule(
  configPath: string
): Promise<Record<string, unknown>> {
  try {
    return await importModuleByPath(configPath);
  } catch (error) {
    if (!shouldTranspileTsContentConfig(configPath, error)) {
      throw error;
    }

    const transpiledPath = await transpileTsContentConfig(configPath);
    try {
      return await importModuleByPath(transpiledPath);
    } finally {
      await fsp.rm(transpiledPath, { force: true });
    }
  }
}

async function importModuleByPath(filePath: string): Promise<Record<string, unknown>> {
  const moduleUrl = `${pathToFileURL(filePath).href}?t=${Date.now()}`;
  return (await import(/* @vite-ignore */ moduleUrl)) as Record<string, unknown>;
}

function shouldTranspileTsContentConfig(configPath: string, error: unknown): boolean {
  const ext = path.extname(configPath);
  if (ext !== ".ts" && ext !== ".tsx") {
    return false;
  }

  const code =
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    typeof (error as { code?: unknown }).code === "string"
      ? (error as { code: string }).code
      : "";
  return code === "ERR_UNKNOWN_FILE_EXTENSION";
}

async function transpileTsContentConfig(configPath: string): Promise<string> {
  let typescript: typeof import("typescript");
  try {
    typescript = (await import("typescript")) as typeof import("typescript");
  } catch {
    throw new Error(
      `Failed to load "${configPath}". TypeScript content configs require the "typescript" package to be installed.`
    );
  }

  const source = await fsp.readFile(configPath, "utf-8");
  const transpiled = typescript.transpileModule(source, {
    fileName: configPath,
    compilerOptions: {
      target: typescript.ScriptTarget.ES2020,
      module: typescript.ModuleKind.ESNext,
      moduleResolution: typescript.ModuleResolutionKind.Bundler,
      jsx: typescript.JsxEmit.Preserve,
      esModuleInterop: true,
    },
  });

  const fileName = `.neutron-content-config-${process.pid}-${Date.now()}-${Math.random()
    .toString(36)
    .slice(2)}.mjs`;
  const transpiledPath = path.join(path.dirname(configPath), fileName);
  await fsp.writeFile(transpiledPath, transpiled.outputText, "utf-8");
  return transpiledPath;
}

async function resolveContentConfigPath(rootDir: string): Promise<string | null> {
  for (const candidate of CONTENT_CONFIG_CANDIDATES) {
    const fullPath = path.join(rootDir, candidate);
    if (fs.existsSync(fullPath)) {
      return fullPath;
    }
  }
  return null;
}

async function loadManifestStore(rootDir: string): Promise<CollectionStore | null> {
  const manifestPath = path.join(rootDir, "dist", CONTENT_MANIFEST_DIST_NAME);
  if (!fs.existsSync(manifestPath)) {
    return null;
  }

  const raw = await fsp.readFile(manifestPath, "utf-8");
  const parsed = JSON.parse(raw) as {
    collections?: Record<string, Array<SerializedCollectionEntry>>;
  };
  if (!parsed.collections || typeof parsed.collections !== "object") {
    return null;
  }

  const collections: Record<string, Array<CollectionEntry<unknown>>> = {};
  for (const [collectionName, entries] of Object.entries(parsed.collections)) {
    collections[collectionName] = (entries || []).map((entry) =>
      createEntry({
        id: entry.id,
        slug: entry.slug,
        collection: collectionName,
        filePath: entry.filePath,
        body: entry.body,
        html: entry.html,
        data: entry.data,
        sourceType: entry.sourceType ?? "data",
        sanitize: entry.sanitize,
      })
    );
  }

  return {
    collections,
    generatedTypes: "/* generated from manifest */\n",
  };
}

async function readCollectionEntries(
  rootDir: string,
  collectionName: string,
  definition: CollectionDefinition<unknown>,
  markdownConfig?: NeutronMarkdownConfig
): Promise<Array<CollectionEntry<unknown>>> {
  const collectionDir = path.join(rootDir, "src", "content", collectionName);
  if (!fs.existsSync(collectionDir)) {
    return [];
  }

  const files = await collectCollectionFiles(collectionDir);
  const entries: Array<CollectionEntry<unknown>> = [];

  for (const relativeFilePath of files) {
    const ext = path.extname(relativeFilePath).toLowerCase();
    const filePath = path.join(collectionDir, relativeFilePath);
    const raw = await fsp.readFile(filePath, "utf-8");
    const slug = relativeFilePath
      .slice(0, -ext.length)
      .split(path.sep)
      .join("/");
    const id = `${collectionName}/${slug}`;

    if (definition.type === "data") {
      try {
        const parsedData = parseDataFile(raw, ext);
        const data = definition.schema.parse(parsedData);
        entries.push(createEntry({
          id,
          slug,
          collection: collectionName,
          filePath,
          body: raw,
          html: "",
          data,
          sourceType: "data",
        }));
      } catch (error) {
        throw withCollectionContext(
          collectionName,
          relativeFilePath,
          `Failed to parse or validate data entry`,
          error
        );
      }
      continue;
    }

    // HTML passthrough — no compilation, just frontmatter extraction
    if (ext === ".html" || ext === ".htm") {
      try {
        const parsed = matter(raw);
        const data = definition.schema.parse(parsed.data);
        entries.push(createEntry({
          id,
          slug,
          collection: collectionName,
          filePath,
          body: parsed.content,
          html: parsed.content,
          data,
          sourceType: "html",
          sanitize: definition.sanitize,
        }));
      } catch (error) {
        throw withCollectionContext(
          collectionName,
          relativeFilePath,
          `Failed to parse or validate HTML content entry`,
          error
        );
      }
      continue;
    }

    try {
      const parsed = matter(raw);
      const data = definition.schema.parse(parsed.data);
      const sourceType = ext === ".mdx" ? "mdx" : "markdown";

      // Lazy rendering. Rendering every entry's body through KaTeX/Shiki/MDX at
      // load time and pinning the resulting HTML in the content-store cache for
      // the whole build is what pushed large content sets (thousands of entries)
      // past the V8 heap limit — the rendered HTML for the entire collection sat
      // in memory even though listing pages only read frontmatter and each page
      // renders its own body exactly once. Defer rendering into render() so the
      // HTML is produced on demand and collected by GC after each page is
      // written. The closure captures the already-retained body string, so it
      // adds no per-entry memory.
      //
      // Frontmatter parsing and schema validation stay eager (above), so malformed
      // frontmatter still fails fast at load. Only body *rendering* defers — its
      // errors (e.g. MDX compile failures) now surface when render() is called,
      // wrapped in the same collection context as before so messages are identical.
      const cacheKey = renderCacheKey(sourceType, parsed.content);
      const lazyMarkup = async (): Promise<RenderedMarkup> => {
        const cached = getCachedMarkup(cacheKey);
        if (cached) return cached;
        try {
          renderCacheMisses++;
          const rendered = await renderMarkup(
            parsed.content,
            sourceType,
            relativeFilePath,
            markdownConfig
          );
          setCachedMarkup(cacheKey, rendered);
          return rendered;
        } catch (error) {
          throw withCollectionContext(
            collectionName,
            relativeFilePath,
            `Failed to render content entry`,
            error
          );
        }
      };

      entries.push(createEntry({
        id,
        slug,
        collection: collectionName,
        filePath,
        body: parsed.content,
        html: "",
        data,
        sourceType,
        sanitize: definition.sanitize,
        lazyMarkup,
      }));
    } catch (error) {
      throw withCollectionContext(
        collectionName,
        relativeFilePath,
        `Failed to parse, validate, or render content entry`,
        error
      );
    }
  }

  return entries;
}

async function collectCollectionFiles(collectionDir: string): Promise<string[]> {
  const files: string[] = [];
  const stack = [collectionDir];

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) continue;
    const dirEntries = await fsp.readdir(current, { withFileTypes: true });
    for (const entry of dirEntries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      const ext = path.extname(entry.name).toLowerCase();
      if (!COLLECTION_FILE_EXTENSIONS.has(ext)) {
        continue;
      }
      files.push(path.relative(collectionDir, fullPath));
    }
  }

  files.sort((a, b) => a.localeCompare(b));
  return files;
}

function createEntry(input: {
  id: string;
  slug: string;
  collection: string;
  filePath: string;
  body: string;
  html: string;
  data: unknown;
  sourceType: "markdown" | "mdx" | "html" | "data";
  sanitize?: boolean;
  renderFactory?: () => Promise<{ Content: preact.FunctionComponent<any> }>;
  // Defers body rendering (markdown/MDX) until render() is first called, so the
  // rendered HTML is not held in the content-store cache for every entry. See
  // readCollectionEntries.
  lazyMarkup?: () => Promise<{
    html: string;
    renderFactory?: () => Promise<{ Content: preact.FunctionComponent<any> }>;
  }>;
}): CollectionEntry<unknown> {
  const fallbackRender = async () => {
    // Local content files are trusted authored content and rendered faithfully.
    // When the collection is marked `sanitize: true` (untrusted source), run the
    // real parser-based sanitizer first. There is no regex fallback — a regex
    // cannot safely sanitize HTML.
    const html = input.sanitize ? await sanitizeHtml(input.html) : input.html;
    return {
      Content: () => h("div", { dangerouslySetInnerHTML: { __html: html } }),
    };
  };

  // Render the body on demand. The result is intentionally NOT memoized onto the
  // (long-lived, cached) entry: a static build renders each page once, so caching
  // would simply re-accumulate the whole collection's HTML in memory — the exact
  // problem lazy rendering exists to avoid.
  const lazyRender = input.lazyMarkup
    ? async () => {
        const rendered = await input.lazyMarkup!();
        if (rendered.renderFactory) {
          return rendered.renderFactory();
        }
        const html = input.sanitize ? await sanitizeHtml(rendered.html) : rendered.html;
        return {
          Content: () => h("div", { dangerouslySetInnerHTML: { __html: html } }),
        };
      }
    : undefined;

  const { renderFactory, lazyMarkup, ...rest } = input;
  const entry = rest as CollectionEntry<unknown>;
  // Expose the lazy renderer non-enumerably so serialization (manifest writes)
  // can materialize the HTML for entries that were never rendered during build.
  if (lazyMarkup) {
    Object.defineProperty(entry, '__lazyMarkup', {
      value: lazyMarkup,
      writable: false,
      enumerable: false,
      configurable: false,
    });
  }
  Object.defineProperty(entry, 'render', {
    value: renderFactory || lazyRender || fallbackRender,
    writable: false,
    enumerable: false,
    configurable: false,
  });
  return entry;
}

function parseDataFile(raw: string, ext: string): unknown {
  let parsed: unknown;

  if (ext === ".json") {
    parsed = JSON.parse(raw);
  } else if (ext === ".yaml" || ext === ".yml") {
    // SECURITY: Use safe YAML parsing options to prevent attacks
    // - merge: false - Prevents YAML merge key attacks (<<: *anchor)
    // - schema: 'core' - Restricts to JSON-compatible types, blocks custom types
    parsed = YAML.parse(raw, { merge: false, schema: 'core' });
  } else {
    throw new Error(
      `Unsupported data file extension "${ext}". Use .json, .yaml, or .yml for data collections.`
    );
  }

  // SECURITY: Validate against prototype pollution
  if (hasPrototypePollution(parsed)) {
    throw new Error(
      `Data file contains potentially malicious prototype pollution properties (__proto__, constructor, prototype)`
    );
  }

  return parsed;
}

async function renderMarkup(
  source: string,
  sourceType: "markdown" | "mdx",
  filePathForErrors?: string,
  markdownConfig?: NeutronMarkdownConfig
): Promise<{
  html: string;
  renderFactory?: () => Promise<{ Content: preact.FunctionComponent<any> }>;
}> {
  if (sourceType === "mdx") {
    const compiled = await compileMdx(source, filePathForErrors, markdownConfig);
    return {
      html: compiled.html,
      renderFactory: compiled.renderFactory,
    };
  }

  const markedInstance = new Marked();
  // Syntax highlighting via Shiki. Highlighting runs in the extension's async
  // walkTokens hook (Marked v15 renderers are sync-only), and degrades to plain
  // escaped <pre><code> when the optional `shiki` peer dependency is absent.
  // Enabled by default; opt out with `syntaxHighlight: false`.
  const syntaxHighlight =
    markdownConfig?.syntaxHighlight ?? activeMarkdownConfig?.syntaxHighlight;
  if (syntaxHighlight !== false) {
    markedInstance.use(markedShikiExtension(syntaxHighlight?.theme ?? "github-dark"));
  }
  // Apply user-supplied marked extensions (KaTeX, directive, custom tokens).
  // Each entry is forwarded directly to Marked.use(). Wired here so plain
  // `.md` content gets the same plugin opportunity that MDX has via
  // remark/rehype.
  const extList = markdownConfig?.markedExtensions ?? activeMarkdownConfig?.markedExtensions;
  if (extList?.length) {
    for (const ext of extList) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      markedInstance.use(ext as any);
    }
  }

  const html = await markedInstance.parse(source);
  return { html: typeof html === "string" ? html : String(html) };
}

async function compileMdx(
  source: string,
  filePathForErrors?: string,
  markdownConfig?: NeutronMarkdownConfig
): Promise<{
  html: string;
  renderFactory: () => Promise<{ Content: preact.FunctionComponent<any> }>;
}> {
  let evaluated: { default?: preact.FunctionComponent<any> };
  try {
    const mdxOptions: Record<string, unknown> = {
      ...preactJsxRuntime,
      format: "mdx",
      development: false,
    };
    if (markdownConfig?.remarkPlugins?.length) {
      mdxOptions.remarkPlugins = markdownConfig.remarkPlugins;
    }
    if (markdownConfig?.rehypePlugins?.length) {
      mdxOptions.rehypePlugins = markdownConfig.rehypePlugins;
    }
    evaluated = (await evaluate(source, mdxOptions as any)) as { default?: preact.FunctionComponent<any> };
  } catch (error) {
    const location =
      typeof filePathForErrors === "string" && filePathForErrors.length > 0
        ? ` in "${filePathForErrors}"`
        : "";
    throw new Error(
      `MDX compilation failed${location}: ${toErrorMessage(error)}`
    );
  }

  const Content = evaluated.default || (() => h("div", null, ""));
  let html = "";
  try {
    html = renderToString(h(Content, {}));
  } catch (error) {
    const location =
      typeof filePathForErrors === "string" && filePathForErrors.length > 0
        ? ` in "${filePathForErrors}"`
        : "";
    throw new Error(`MDX render failed${location}: ${toErrorMessage(error)}`);
  }

  return {
    html,
    renderFactory: async () => ({ Content }),
  };
}

function withCollectionContext(
  collectionName: string,
  relativeFilePath: string,
  summary: string,
  error: unknown
): Error {
  return new Error(
    `[content:${collectionName}] ${summary} for "${relativeFilePath}": ${toErrorMessage(error)}`
  );
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

async function computeContentFingerprint(rootDir: string): Promise<string> {
  const parts: string[] = [];
  const configPath = await resolveContentConfigPath(rootDir);
  if (configPath && fs.existsSync(configPath)) {
    const stat = await fsp.stat(configPath);
    parts.push(`${configPath}:${stat.mtimeMs}:${stat.size}`);
  }

  const contentDir = path.join(rootDir, "src", "content");
  if (!fs.existsSync(contentDir)) {
    return parts.join("|");
  }

  const stack = [contentDir];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) continue;
    const entries = await fsp.readdir(current, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      const ext = path.extname(entry.name).toLowerCase();
      if (!COLLECTION_FILE_EXTENSIONS.has(ext)) {
        continue;
      }
      const stat = await fsp.stat(fullPath);
      parts.push(`${fullPath}:${stat.mtimeMs}:${stat.size}`);
    }
  }

  return parts.sort().join("|");
}

function generateCollectionTypes(config: CollectionConfigMap): string {
  const lines: string[] = [];
  lines.push("// Auto-generated by Neutron. Do not edit.");
  lines.push('declare module "@neutron-build/core/content" {');
  lines.push("  interface ContentCollectionMap {");
  for (const [name, definition] of Object.entries(config)) {
    lines.push(`    "${name}": ${schemaToTs(definition.schema)};`);
  }
  lines.push("  }");
  lines.push("}");
  lines.push("");
  // `export {}` makes this file a module so the `declare module` block augments
  // (merges into) the real ContentCollectionMap instead of shadowing it.
  lines.push("export {};");
  lines.push("");
  return lines.join("\n");
}

async function toSerializableCollections(
  collections: Record<string, Array<CollectionEntry<unknown>>>
): Promise<Record<string, Array<SerializedCollectionEntry>>> {
  const result: Record<string, Array<SerializedCollectionEntry>> = {};
  for (const [name, entries] of Object.entries(collections)) {
    const serialized: SerializedCollectionEntry[] = [];
    for (const entry of entries) {
      // Lazily-rendered entries carry an empty html field until first render.
      // Materialize it here so a written manifest is self-contained.
      let html = entry.html;
      const lazyMarkup = (entry as { __lazyMarkup?: () => Promise<{ html: string }> }).__lazyMarkup;
      if (!html && typeof lazyMarkup === "function") {
        html = (await lazyMarkup()).html;
      }
      serialized.push({
        id: entry.id,
        slug: entry.slug,
        collection: entry.collection,
        body: entry.body,
        html,
        data: entry.data,
        filePath: entry.filePath,
        sourceType: entry.sourceType,
        sanitize: entry.sanitize,
      });
    }
    result[name] = serialized;
  }
  return result;
}

function schemaToTs(schema: ZodTypeAny): string {
  if (schema instanceof ZodString) {
    return "string";
  }
  if (schema instanceof ZodNumber) {
    return "number";
  }
  if (schema instanceof ZodBoolean) {
    return "boolean";
  }
  if (schema instanceof ZodDate) {
    return "Date";
  }
  if (schema instanceof ZodArray) {
    return `${schemaToTs(schema.element)}[]`;
  }
  if (schema instanceof ZodOptional) {
    return `${schemaToTs(schema.unwrap())} | undefined`;
  }
  if (schema instanceof ZodNullable) {
    return `${schemaToTs(schema.unwrap())} | null`;
  }
  if (schema instanceof ZodDefault) {
    return schemaToTs(schema.removeDefault());
  }
  if (schema instanceof ZodEffects) {
    return schemaToTs(schema.innerType());
  }
  if (schema instanceof ZodEnum) {
    return (schema.options as string[]).map((v: string) => JSON.stringify(v)).join(" | ");
  }
  if (schema instanceof ZodLiteral) {
    const val = schema.value;
    return typeof val === "string" ? JSON.stringify(val) : String(val);
  }
  if (schema instanceof ZodUnion) {
    return (schema.options as ZodTypeAny[]).map((o: ZodTypeAny) => schemaToTs(o)).join(" | ");
  }
  if (schema instanceof ZodObject) {
    const shape = schema.shape;
    const props = Object.entries(shape).map(([key, value]) => {
      const field = value as ZodTypeAny;
      if (field instanceof ZodOptional) {
        return `${JSON.stringify(key)}?: ${schemaToTs(field.unwrap())};`;
      }
      return `${JSON.stringify(key)}: ${schemaToTs(field)};`;
    });
    return `{ ${props.join(" ")} }`;
  }
  return "unknown";
}
