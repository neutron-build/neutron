import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { evaluate } from "@mdx-js/mdx";
import matter from "gray-matter";
import { marked } from "marked";
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
}

export interface DefineCollectionOptions<TData = unknown> {
  type?: CollectionType;
  schema: z.ZodType<TData>;
  live?: boolean; // NEW: Enable runtime loading
  loader?: () => Promise<TData[]>; // NEW: Runtime data loader
  cacheTtl?: number; // NEW: Cache TTL in milliseconds (default: 60000)
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
  sourceType: "markdown" | "mdx" | "data";
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
  sourceType?: "markdown" | "mdx" | "data";
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
const COLLECTION_FILE_EXTENSIONS = new Set([".md", ".mdx", ".json", ".yaml", ".yml"]);
const cacheByRoot = new Map<string, CacheRecord>();

export function defineCollection<TData>(
  options: DefineCollectionOptions<TData>
): CollectionDefinition<TData> {
  return {
    type: options.type ?? "content",
    schema: options.schema,
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

  const store = await loadCollectionStore(rootDir, { force: true });

  if (writeManifest) {
    const serializableCollections = toSerializableCollections(store.collections);
    await fsp.mkdir(path.dirname(manifestPath), { recursive: true });
    await fsp.writeFile(
      manifestPath,
      JSON.stringify(
        {
          collections: serializableCollections,
          generatedAt: new Date().toISOString(),
        },
        null,
        2
      ),
      "utf-8"
    );
  }

  if (writeTypes) {
    const typesPath = path.join(rootDir, "src", "content", ".neutron-content.d.ts");
    await fsp.mkdir(path.dirname(typesPath), { recursive: true });
    await fsp.writeFile(typesPath, store.generatedTypes, "utf-8");
  }
}

async function loadCollectionStore(
  rootDir: string,
  options: { force?: boolean } = {}
): Promise<CollectionStore> {
  const fingerprint = await computeContentFingerprint(rootDir);
  const cached = cacheByRoot.get(rootDir);
  if (!options.force && cached && cached.fingerprint === fingerprint) {
    return cached.store;
  }

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
        'declare module "neutron/content" {',
        "  interface ContentCollectionMap {}",
        "}",
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
      definition
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
  definition: CollectionDefinition<unknown>
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

    try {
      const parsed = matter(raw);
      const data = definition.schema.parse(parsed.data);
      const sourceType = ext === ".mdx" ? "mdx" : "markdown";
      const rendered = await renderMarkup(parsed.content, sourceType, relativeFilePath);

      entries.push(createEntry({
        id,
        slug,
        collection: collectionName,
        filePath,
        body: parsed.content,
        html: rendered.html,
        data,
        sourceType,
        renderFactory: rendered.renderFactory,
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

function sanitizeHtml(html: string): string {
  // Strip script tags and their contents
  html = html.replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '');
  // Strip event handler attributes
  html = html.replace(/\s+on\w+\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]*)/gi, '');
  // Strip javascript: URLs
  html = html.replace(/(?:href|src|action)\s*=\s*(?:"javascript:[^"]*"|'javascript:[^']*')/gi, '');
  return html;
}

function createEntry(input: {
  id: string;
  slug: string;
  collection: string;
  filePath: string;
  body: string;
  html: string;
  data: unknown;
  sourceType: "markdown" | "mdx" | "data";
  renderFactory?: () => Promise<{ Content: preact.FunctionComponent<any> }>;
}): CollectionEntry<unknown> {
  const fallbackRender = async () => {
    const html = input.html;
    return {
      Content: () => h("div", { dangerouslySetInnerHTML: { __html: sanitizeHtml(html) } }),
    };
  };

  const { renderFactory, ...rest } = input;
  const entry = rest as CollectionEntry<unknown>;
  Object.defineProperty(entry, 'render', {
    value: renderFactory || fallbackRender,
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
  filePathForErrors?: string
): Promise<{
  html: string;
  renderFactory?: () => Promise<{ Content: preact.FunctionComponent<any> }>;
}> {
  if (sourceType === "mdx") {
    const compiled = await compileMdx(source, filePathForErrors);
    return {
      html: compiled.html,
      renderFactory: compiled.renderFactory,
    };
  }

  const html = await marked.parse(source);
  return { html: typeof html === "string" ? html : String(html) };
}

async function compileMdx(
  source: string,
  filePathForErrors?: string
): Promise<{
  html: string;
  renderFactory: () => Promise<{ Content: preact.FunctionComponent<any> }>;
}> {
  let evaluated: { default?: preact.FunctionComponent<any> };
  try {
    evaluated = (await evaluate(source, {
      ...preactJsxRuntime,
      format: "mdx",
      development: false,
    })) as { default?: preact.FunctionComponent<any> };
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
  lines.push('declare module "neutron/content" {');
  lines.push("  interface ContentCollectionMap {");
  for (const [name, definition] of Object.entries(config)) {
    lines.push(`    "${name}": ${schemaToTs(definition.schema)};`);
  }
  lines.push("  }");
  lines.push("}");
  lines.push("");
  return lines.join("\n");
}

function toSerializableCollections(
  collections: Record<string, Array<CollectionEntry<unknown>>>
): Record<string, Array<SerializedCollectionEntry>> {
  const result: Record<string, Array<SerializedCollectionEntry>> = {};
  for (const [name, entries] of Object.entries(collections)) {
    result[name] = entries.map((entry) => ({
      id: entry.id,
      slug: entry.slug,
      collection: entry.collection,
      body: entry.body,
      html: entry.html,
      data: entry.data,
      filePath: entry.filePath,
      sourceType: entry.sourceType,
    }));
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
