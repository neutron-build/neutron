import assert from "node:assert/strict";
import test from "node:test";
import { exportSchema, pgTable, serial, integer, text, varchar, boolean, timestamp, index, relations, vector } from "./index.js";

const users = pgTable(
  "users",
  {
    id: serial("id").primaryKey(),
    email: varchar("email", 255).notNull().unique(),
    active: boolean("active").notNull().default(true),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => [index("users_email_idx").on(t.email)],
);

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
  title: text("title").notNull(),
});

test("exportSchema: deterministic, sorted, complete", () => {
  const exported = exportSchema({ posts, users });
  assert.equal(exported.version, 1);
  assert.deepEqual(exported.tables.map((t) => t.name), ["posts", "users"]);

  const usersTable = exported.tables[1];
  assert.deepEqual(
    usersTable.columns.map((c) => c.name),
    ["id", "email", "active", "created_at"],
  );
  assert.equal(usersTable.columns[1].varcharLength, 255);
  assert.equal(usersTable.columns[1].notNull, true);
  assert.equal(usersTable.columns[1].unique, true);
  assert.equal(usersTable.columns[2].default, "true");
  assert.equal(usersTable.columns[3].defaultNow, true);
  assert.deepEqual(usersTable.indexes, [{ name: "users_email_idx", unique: false, columns: ["email"] }]);

  const postsTable = exported.tables[0];
  assert.deepEqual(postsTable.columns[1].foreignKey, { table: "users", column: "id", onDelete: "cascade" });
});

test("exportSchema: JSON round-trip is stable", () => {
  const a = JSON.stringify(exportSchema({ posts, users }));
  const b = JSON.stringify(exportSchema({ users, posts }));
  assert.equal(a, b);
  const parsed = JSON.parse(a);
  assert.equal(parsed.tables.length, 2);
});

test("exportSchema: relations entries are ignored (tables only)", () => {
  const usersRelations = relations(users, ({ many }) => ({ posts: many(posts) }));
  const exported = exportSchema({ users, posts, usersRelations } as never);
  assert.equal(exported.tables.length, 2);
});

test("exportSchema: vector columns marked nucleusOnly with dimensions", () => {
  const docs = pgTable("docs", {
    id: serial("id").primaryKey(),
    embedding: vector("embedding", 1536),
  });
  const exported = exportSchema({ docs });
  const emb = exported.tables[0].columns[1];
  assert.equal(emb.type, "vector");
  assert.equal(emb.nucleusOnly, true);
  assert.equal(emb.vectorDimensions, 1536);
});

// ---------------------------------------------------------------------------
// F04: schema document v2 — deterministic export, canonical bytes that the Go
// CLI and the contracts/data reference consumer agree on, and the explicit
// v1 compatibility reader.
// ---------------------------------------------------------------------------

import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import {
  boolean as boolCol,
  canonicalSchemaJson,
  exportSchemaV2,
  numeric,
  readSchemaDocumentV1,
  text as textCol,
  timestamptz,
  type SchemaDocumentV2,
} from "./index.js";

const require = createRequire(import.meta.url);
/** Resolve a path under contracts/data relative to this test file (dist/). */
const contractPath = (rel: string): string => require.resolve(`../../../../contracts/data/${rel}`);

// The schema whose export is pinned as a cross-language golden fixture
// (contracts/data/golden/valid/exported-v2.json): serial PK + sequence
// default, varchar length, unique, FK with onDelete, defaultNow, tagged
// literal defaults across type kinds, an index, and a vector column pinning
// the nucleus capability.
const fixtureUsers = pgTable(
  "users",
  {
    id: serial("id").primaryKey(),
    email: varchar("email", 255).notNull().unique(),
    name: textCol("name"),
    createdAt: timestamptz("created_at").notNull().defaultNow(),
    active: boolCol("active").notNull().default(true),
    balance: numeric("balance").default("0"),
  },
  (t) => [index("users_name_idx").on(t.name)],
);

const fixturePosts = pgTable(
  "posts",
  {
    id: serial("id").primaryKey(),
    authorId: integer("author_id").notNull().references(() => fixtureUsers.id, { onDelete: "cascade" }),
    title: textCol("title").notNull(),
    views: integer("views").default(0),
    note: textCol("note").default("hello world"),
  },
  (t) => [index("posts_title_idx").on(t.title)],
);

const fixtureDocs = pgTable("docs", {
  id: serial("id").primaryKey(),
  embedding: vector("embedding", 1536),
});

const fixtureSchema = { users: fixtureUsers, posts: fixturePosts, docs: fixtureDocs };

const sha256 = (text: string): string => createHash("sha256").update(text, "utf8").digest("hex");

test("exportSchemaV2: deterministic across runs and input key order", () => {
  const a = canonicalSchemaJson(exportSchemaV2(fixtureSchema));
  const b = canonicalSchemaJson(exportSchemaV2({ docs: fixtureDocs, posts: fixturePosts, users: fixtureUsers }));
  assert.equal(a, b);
  assert.equal(sha256(a), sha256(b));
  // Tables sorted; columns keep declaration order (attnum semantics).
  const doc = exportSchemaV2(fixtureSchema);
  assert.deepEqual(doc.tables.map((t) => t.identity.name), ["docs", "posts", "users"]);
  assert.deepEqual(
    doc.tables[2].columns.map((c) => c.name),
    ["id", "email", "name", "created_at", "active", "balance"],
  );
  assert.deepEqual(doc.capabilities, ["nucleus"]);
});

test("exportSchemaV2: golden fixture agreement (canonical bytes + sha, Go + TS CI-pinned)", () => {
  const canonical = canonicalSchemaJson(exportSchemaV2(fixtureSchema));
  const fixturePath = contractPath("golden/valid/exported-v2.json");
  const fixtureBytes = readFileSync(fixturePath, "utf8");
  assert.equal(canonical, fixtureBytes, "exporter canonical bytes must equal the golden fixture");
});

test("exportSchemaV2: identical exports across fresh processes", async () => {
  const child = (extraEnv: Record<string, string>): Promise<string> =>
    new Promise((resolve, reject) => {
      const proc = require("node:child_process").spawn(
        process.execPath,
        ["--input-type=module", "-e", CHILD_SNIPPET],
        { cwd: require.resolve("./index.js").replace(/\/dist\/index\.js$/, ""), env: { ...process.env, ...extraEnv } },
      );
      let out = "";
      let err = "";
      proc.stdout.on("data", (d: Buffer) => (out += d));
      proc.stderr.on("data", (d: Buffer) => (err += d));
      proc.on("close", (code: number) => (code === 0 ? resolve(out.trim()) : reject(new Error(`exit ${code}: ${err}`))));
    });
  const hashes: string[] = [];
  const envs: Array<Record<string, string>> = [{}, { TZ: "Asia/Tokyo" }, { TZ: "America/New_York" }];
  for (const env of envs) {
    hashes.push(await child(env));
  }
  assert.equal(hashes[0], hashes[1]);
  assert.equal(hashes[0], hashes[2]);
  assert.equal(hashes[0], sha256(canonicalSchemaJson(exportSchemaV2(fixtureSchema))));
});

const CHILD_SNIPPET = `
import { createHash } from "node:crypto";
import { pgTable, serial, integer, text, varchar, boolean, timestamptz, numeric, index, vector, exportSchemaV2, canonicalSchemaJson } from "./dist/index.js";
const users = pgTable("users", { id: serial("id").primaryKey(), email: varchar("email", 255).notNull().unique(), name: text("name"), createdAt: timestamptz("created_at").notNull().defaultNow(), active: boolean("active").notNull().default(true), balance: numeric("balance").default("0") }, (t) => [index("users_name_idx").on(t.name)]);
const posts = pgTable("posts", { id: serial("id").primaryKey(), authorId: integer("author_id").notNull().references(() => users.id, { onDelete: "cascade" }), title: text("title").notNull(), views: integer("views").default(0), note: text("note").default("hello world") }, (t) => [index("posts_title_idx").on(t.title)]);
const docs = pgTable("docs", { id: serial("id").primaryKey(), embedding: vector("embedding", 1536) });
console.log(createHash("sha256").update(canonicalSchemaJson(exportSchemaV2({ users, posts, docs })), "utf8").digest("hex"));
`;

test("exportSchemaV2: v2 mapping rules (types, codecs, defaults, constraints)", () => {
  const doc = exportSchemaV2(fixtureSchema);
  const users = doc.tables[2];
  assert.equal(users.identity.schema, "public");
  assert.equal(users.columns[0].type.name, "int4");
  assert.equal(users.columns[0].type.codec, "number");
  assert.deepEqual(users.columns[0].default, { kind: "sequence", sequence: { schema: "public", name: "users_id_seq" } });
  assert.deepEqual(users.columns[1].type.params, { length: 255 });
  assert.equal(users.columns[3].default!.kind, "expression");
  assert.equal((users.columns[3].default as { sql: string }).sql, "now()");
  assert.deepEqual(users.columns[4].default, { kind: "literal", sql: "true" });
  assert.deepEqual(users.columns[5].default, { kind: "literal", sql: "0" });
  const posts = doc.tables[1];
  assert.deepEqual(posts.columns[4].default, { kind: "literal", sql: "'hello world'" });
  const fk = posts.constraints.find((c) => c.type === "foreign-key")!;
  assert.equal(fk.name, "posts_author_id_fkey");
  assert.deepEqual(fk.references!.table, { schema: "public", name: "users" });
  assert.equal(fk.references!.onDelete, "cascade");
  assert.ok(users.constraints.some((c) => c.type === "primary-key" && c.name === "users_pkey"));
  assert.ok(users.constraints.some((c) => c.type === "unique" && c.name === "users_email_key"));
  const docs = doc.tables[0];
  assert.deepEqual(docs.columns[1].type.params, { dimensions: 1536 });
});

test("exportSchemaV2: unrepresentable schemas fail clearly", () => {
  const serialDefault = pgTable("bad", { id: serial("id").default(5) });
  assert.throws(() => exportSchemaV2({ bad: serialDefault }), /\[invalid-default\].*serial/);
  const danglingFk = pgTable("child", { id: serial("id").primaryKey(), otherId: integer("other_id").notNull() });
  (danglingFk.otherId as { foreignKey?: unknown }).foreignKey = () => null as never;
  assert.throws(() => exportSchemaV2({ child: danglingFk }), /\[fk-target\]/);
  const missingTarget = pgTable("child2", { id: serial("id").primaryKey(), userId: integer("user_id").notNull().references(() => fixtureUsers.id) });
  assert.throws(() => exportSchemaV2({ child2: missingTarget }), /\[fk-target\].*not part of the exported schema/);
  const fkToNonUnique = pgTable("child4", {
    id: serial("id").primaryKey(),
    buddyId: integer("buddy_id").references(() => fixturePosts.views),
  });
  assert.throws(() => exportSchemaV2({ users: fixtureUsers, posts: fixturePosts, child4: fkToNonUnique }), /\[fk-not-unique\]/);
});

type AnyCol = ReturnType<typeof integer>;

// ---------------------------------------------------------------------------
// v1 compatibility reader
// ---------------------------------------------------------------------------

const goldenDir = (rel: string) => contractPath(`golden/v1/${rel}`);

test("readSchemaDocumentV1: upgrades legacy documents to the pinned golden bytes", () => {
  const doc = readSchemaDocumentV1(JSON.parse(readFileSync(goldenDir("basic-upgrade.json"), "utf8")));
  const canonical = canonicalSchemaJson(doc);
  const expected = readFileSync(goldenDir("basic-upgrade.canonical.json"), "utf8");
  assert.equal(canonical, expected, "upgraded canonical bytes must equal the Go-recorded golden output");
  const manifest = JSON.parse(readFileSync(contractPath("golden/manifest.json"), "utf8"));
  const entry = manifest.v1.find((e: { name: string }) => e.name === "basic-upgrade");
  assert.equal(sha256(canonical), entry.sha256, "hash agreement with the Go-recorded manifest value");

  const vectorDoc: SchemaDocumentV2 = readSchemaDocumentV1(JSON.parse(readFileSync(goldenDir("vector-upgrade.json"), "utf8")));
  assert.deepEqual(vectorDoc.capabilities, ["nucleus"]);
  const vectorCanonical = canonicalSchemaJson(vectorDoc);
  assert.equal(vectorCanonical, readFileSync(goldenDir("vector-upgrade.canonical.json"), "utf8"));
});

test("readSchemaDocumentV1: ambiguities fail with the exact contract code", () => {
  for (const name of ["ambiguous-default.json", "hasdefault-no-value.json"]) {
    const doc = JSON.parse(readFileSync(goldenDir(name), "utf8"));
    assert.throws(() => readSchemaDocumentV1(doc), /\[ambiguous-default\]/, name);
  }
  assert.throws(() => readSchemaDocumentV1({ version: 2 }), /\[unknown-version\].*already version 2/);
  assert.throws(() => readSchemaDocumentV1({ version: 3, tables: [] }), /\[unknown-version\]/);
  assert.throws(() => readSchemaDocumentV1({ tables: [] }), /\[unknown-version\]/);
  assert.throws(() => readSchemaDocumentV1('{"version": 1, "tables": ['.repeat(1)), /\[invalid-json\]/);
  assert.throws(
    () => readSchemaDocumentV1({ version: 1, tables: [{ name: "t", columns: [{ name: "a", type: "bogus" }] }] }),
    /\[unknown-type\].*bogus/,
  );
  assert.throws(
    () => readSchemaDocumentV1({ version: 1, tables: [{ name: "t", columns: [] }] }),
    /\[invalid-value\].*at least one column/,
  );
  assert.throws(
    () =>
      readSchemaDocumentV1({
        version: 1,
        tables: [
          { name: "t", columns: [{ name: "a", type: "text" }] },
          { name: "t", columns: [{ name: "a", type: "text" }] },
        ],
      }),
    /\[duplicate-table\]/,
  );
  assert.throws(
    () => readSchemaDocumentV1({ version: 1, tables: [{ name: "t", columns: [{ name: "a", type: "text" }, { name: "a", type: "text" }] }] }),
    /\[duplicate-column\]/,
  );
});

test("readSchemaDocumentV1: v1 exportSchema output upgrades to the same v2 bytes when unambiguous", () => {
  // v1 can express everything in this schema unambiguously (no text/quoted
  // defaults): the v1 export → v1 reader path and the direct v2 export must
  // produce identical canonical bytes.
  const roundUsers = pgTable(
    "users",
    {
      id: serial("id").primaryKey(),
      email: varchar("email", 255).notNull().unique(),
      createdAt: timestamptz("created_at").notNull().defaultNow(),
      active: boolCol("active").notNull().default(true),
      balance: numeric("balance").default("0"),
    },
    (t) => [index("users_email_idx").on(t.email)],
  );
  const roundPosts = pgTable(
    "posts",
    {
      id: serial("id").primaryKey(),
      authorId: integer("author_id").notNull().references(() => roundUsers.id, { onDelete: "cascade" }),
      title: textCol("title").notNull(),
      views: integer("views").default(0),
    },
    (t) => [index("posts_title_idx").on(t.title)],
  );
  const tables = { users: roundUsers, posts: roundPosts };
  const upgraded = readSchemaDocumentV1(exportSchema(tables));
  assert.equal(canonicalSchemaJson(upgraded), canonicalSchemaJson(exportSchemaV2(tables)));

  // A text default IS ambiguous in v1: the reader must reject the v1 export
  // of such a schema instead of guessing literal vs expression.
  const withTextDefault = pgTable("t", { id: serial("id").primaryKey(), note: textCol("note").default("hello world") });
  assert.throws(() => readSchemaDocumentV1(exportSchema({ t: withTextDefault })), /\[ambiguous-default\].*note/);
});
