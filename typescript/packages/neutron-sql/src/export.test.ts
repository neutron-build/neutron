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

test("exportSchema: vector columns fail closed in the v1 shape (X01)", () => {
  const docs = pgTable("docs", {
    id: serial("id").primaryKey(),
    embedding: vector("embedding", 1536),
  });
  assert.throws(() => exportSchema({ docs }), /vector column.*legacy v1 export shape cannot represent it.*exportSchemaV2/s);
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
  assert.deepEqual(doc.capabilities, ["pgvector"]);
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
  assert.deepEqual(vectorDoc.capabilities, ["pgvector"]);
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

// ---------------------------------------------------------------------------
// Q07: rich PostgreSQL schema surface — namespaces, enums, arrays, composite
// constraints, richer indexes, identity/generated columns, views. The kitchen
// sink below is pinned as a cross-language golden fixture
// (contracts/data/golden/valid/q07-exported-v2.json): its canonical bytes are
// validated and re-canonicalized independently by the Go validator and the
// contracts/data reference consumer.
// ---------------------------------------------------------------------------

import {
  ascNullsFirst,
  bigint,
  check,
  desc,
  descNullsLast,
  foreignKey,
  ident,
  pgEnum,
  pgSchema,
  pgView,
  primaryKey,
  sql,
  unique,
} from "./index.js";

const app = pgSchema("app");
const mood = app.enum("mood", ["sad", "ok", "glad"]);
const color = pgEnum("color", ["red", "green", "blue"]);

const tenants = app.table(
  "tenants",
  {
    id: integer("id").generatedByDefaultAsIdentity(),
    name: varchar("name", 120).notNull(),
    tone: mood("tone").default("ok"),
    palette: color("palette").array(),
    scores: bigint("scores").array().notNull(),
    aliases: varchar("aliases", 40).array(),
  },
  (t) => [
    primaryKey({ columns: [t.id] }),
    check("tenants_name_check", sql`${t.name} <> ''`),
    index("tenants_scores_idx").on(desc(t.scores)).where(sql`${t.scores} is not null`),
    // gin over an array column (default array_ops): `gin (lower(name))` has no
    // default text opclass and cannot apply on a real server — the pinned
    // fixture documents appliable DDL only.
    index("tenants_aliases_gin").using("gin").on(t.aliases),
    unique({ name: "tenants_name_aliases_unq", columns: [t.name, t.aliases] }),
  ],
);

const notices = pgTable(
  "notices",
  {
    id: serial("id"),
    tenant: integer("tenant").notNull(),
    rank: integer("rank"),
    memo: textCol("memo"),
    net: numeric("net").notNull(),
    gross: numeric("gross").generatedAlwaysAs(sql`${ident("net")} * 2`),
  },
  (t) => [
    foreignKey({ columns: [t.tenant] }).references(tenants, [tenants.id], { onDelete: "cascade", match: "full" }),
    primaryKey({ name: "notices_alt_pkey", columns: [t.id, t.tenant] }),
    index("notices_rank_idx").on(descNullsLast(t.rank), ascNullsFirst(t.memo)).include(t.memo),
  ],
);

const writers = app.view(
  "writers",
  { id: tenants.id, name: tenants.name },
  { definition: sql`select ${tenants.id}, ${tenants.name} from ${tenants} where ${tenants.tone} = 'glad'`, checkOption: "cascaded", securityInvoker: true },
);

test("exportSchemaV2 Q07: namespaces, enums, arrays, constraints, indexes, identity, generated, views", () => {
  const doc = exportSchemaV2({ tenants, notices, writers, color });
  assert.deepEqual(doc.schemas.map((s) => s.name), ["app", "public"]);
  assert.deepEqual(
    doc.enums.map((e) => [e.identity.schema, e.identity.name, e.values]),
    [
      ["app", "mood", ["sad", "ok", "glad"]],
      ["public", "color", ["red", "green", "blue"]],
    ],
  );
  const t = doc.tables.find((x) => x.identity.name === "tenants")!;
  assert.deepEqual(t.identity, { schema: "app", name: "tenants" });
  assert.deepEqual(t.columns[0].default, { kind: "identity", generated: "by default" });
  assert.equal(t.columns[0].notNull, true);
  assert.deepEqual(t.columns[2].type, { name: "enum", codec: "enum", enum: { schema: "app", name: "mood" } });
  assert.deepEqual(t.columns[2].default, { kind: "literal", sql: "'ok'" });
  assert.deepEqual(t.columns[3].type, { array: true, codec: "array", name: "enum", enum: { schema: "public", name: "color" } });
  assert.equal(t.columns[3].default, undefined);
  assert.deepEqual(t.columns[4].type, { array: true, codec: "array", name: "int8" });
  assert.deepEqual(t.columns[5].type, { array: true, codec: "array", name: "varchar", params: { length: 40 } });
  assert.deepEqual(t.constraints.find((c) => c.type === "primary-key")!.columns, ["id"]);
  const checkC = t.constraints.find((c) => c.type === "check")!;
  assert.equal(checkC.name, "tenants_name_check");
  assert.equal(checkC.expression, `"name" <> ''`);
  const scoresIdx = t.indexes.find((i) => i.identity.name === "tenants_scores_idx")!;
  assert.deepEqual(scoresIdx, {
    identity: { schema: "app", name: "tenants_scores_idx" },
    unique: false,
    method: "btree",
    key: [{ column: "scores", order: "desc" }],
    where: `"scores" is not null`,
  });
  const ginIdx = t.indexes.find((i) => i.identity.name === "tenants_aliases_gin")!;
  assert.equal(ginIdx.method, "gin");
  assert.deepEqual(ginIdx.key, [{ column: "aliases" }]);
  assert.deepEqual(t.constraints.find((c) => c.name === "tenants_name_aliases_unq")!.columns, ["name", "aliases"]);

  const n = doc.tables.find((x) => x.identity.name === "notices")!;
  const fk = n.constraints.find((c) => c.type === "foreign-key")!;
  assert.equal(fk.name, "notices_tenant_fkey");
  assert.deepEqual(fk.references!.table, { schema: "app", name: "tenants" });
  assert.equal(fk.references!.match, "full");
  assert.equal(fk.references!.onDelete, "cascade");
  assert.deepEqual(n.constraints.find((c) => c.name === "notices_alt_pkey")!.columns, ["id", "tenant"]);
  assert.equal(n.columns[5].name, "gross");
  assert.deepEqual(n.columns[5].generated, { expression: `"net" * 2` });
  const rankIdx = n.indexes.find((i) => i.identity.name === "notices_rank_idx")!;
  assert.deepEqual(rankIdx.key, [
    { column: "rank", order: "desc", nulls: "last" },
    { column: "memo", nulls: "first" },
  ]);
  assert.deepEqual(rankIdx.include, ["memo"]);

  const v = doc.views[0];
  assert.deepEqual(v.identity, { schema: "app", name: "writers" });
  assert.equal(v.checkOption, "cascaded");
  assert.equal(v.securityInvoker, true);
  assert.equal(v.definition, `select "app"."tenants"."id", "app"."tenants"."name" from "app"."tenants" where "app"."tenants"."tone" = 'glad'`);

  // Deterministic across input key order.
  assert.equal(canonicalSchemaJson(exportSchemaV2({ writers, color, notices, tenants })), canonicalSchemaJson(doc));
});

test("exportSchemaV2 Q07: golden fixture agreement (canonical bytes, Go + consumer CI-pinned)", () => {
  const canonical = canonicalSchemaJson(exportSchemaV2({ tenants, notices, writers, color }));
  const fixturePath = contractPath("golden/valid/q07-exported-v2.json");
  const fixtureBytes = readFileSync(fixturePath, "utf8");
  assert.equal(canonical, fixtureBytes, "exporter canonical bytes must equal the golden fixture");
});

test("exportSchemaV2 Q07: precise rejections", () => {
  // Column-level + table-level primary key.
  assert.throws(
    () =>
      exportSchemaV2({
        t: pgTable(
          "t",
          { id: serial("id").primaryKey(), a: integer("a").notNull() },
          (t2) => [primaryKey({ columns: [t2.id, t2.a] })],
        ),
      }),
    /multiple-primary-key/,
  );
  // FK target tuple not covered by a pk/unique constraint on the target.
  const target = pgTable("target", { id: serial("id").primaryKey(), junk: integer("junk") });
  assert.throws(
    () => exportSchemaV2({ target, src: pgTable("src", { id: serial("id").primaryKey(), ref: integer("ref").notNull().references(() => target.junk) }) }),
    /\[fk-not-unique\]/,
  );
  // Table-level FK to a table outside the exported set.
  const outside = pgTable("outside", { id: serial("id").primaryKey() });
  assert.throws(
    () =>
      exportSchemaV2({
        src: pgTable("src", { id: serial("id").primaryKey() }, (t2) => [foreignKey({ columns: [t2.id] }).references(outside, [outside.id])]),
      }),
    /\[fk-target\].*not part of the exported schema/,
  );
  // Check expression referencing another table's column is rejected by the
  // table-scoped renderer.
  const other = pgTable("other", { x: integer("x") });
  assert.throws(
    () => exportSchemaV2({ t: pgTable("t", { a: integer("a") }, (t2) => [check("c", sql`${t2.a} > ${other.x}`)]) }),
    /does not belong to table/,
  );
  // Enum array defaults are not spellable (quoted cast).
  const m2 = pgEnum("m2", ["a", "b"]);
  assert.throws(
    () => exportSchemaV2({ t: pgTable("t", { id: serial("id").primaryKey(), m: m2("m").array().default(["a"]) }) }),
    /enum array column defaults are explicitly unsupported/,
  );
  // Array defaults on plain element types spell as cast literals.
  const okArrays = exportSchemaV2({ t: pgTable("t2", { id: serial("id").primaryKey(), xs: integer("xs").array().default([1, 2]) }) });
  assert.deepEqual(okArrays.tables[0].columns[1].default, { kind: "literal", sql: "'{1,2}'::int4[]" });
});

// ---------------------------------------------------------------------------
// X02 (X01 review M2): the TS v1 upgrade reader agrees with Go's
// ValidateSchemaV1ForUpgrade on the legacy vector shape. Same bytes, same
// verdict, same message core ("legacy vector columns must be nucleusOnly
// (pre-X01 shape); a non-nucleusOnly vector column never had meaning in v1").
// ---------------------------------------------------------------------------

test("X02: readSchemaDocumentV1 rejects a non-nucleusOnly v1 vector column (Go parity)", () => {
  const doc = {
    version: 1,
    tables: [
      {
        name: "users",
        columns: [
          { name: "id", type: "serial", notNull: true, primaryKey: true },
          { name: "embedding", type: "vector", notNull: true, vectorDimensions: 3 },
        ],
        indexes: [],
      },
    ],
  };
  assert.throws(
    () => readSchemaDocumentV1(doc),
    /\[invalid-legacy-vector\] tables\[public\.users\]\.columns\[embedding\]: legacy vector columns must be nucleusOnly \(pre-X01 shape\); a non-nucleusOnly vector column never had meaning in v1/,
  );
});

test("X02: readSchemaDocumentV1 keeps accepting the legacy nucleusOnly+dims shape", () => {
  const doc = readSchemaDocumentV1(
    JSON.parse(readFileSync(goldenDir("vector-upgrade.json"), "utf8")),
  );
  assert.deepEqual(doc.capabilities, ["pgvector"]);
});

test("X02: readSchemaDocumentV1 rejects misplaced legacy vector fields like Go", () => {
  const base: {
    version: number;
    tables: Array<{
      name: string;
      columns: Array<Record<string, unknown>>;
      indexes: unknown[];
    }>;
  } = {
    version: 1,
    tables: [
      {
        name: "t",
        columns: [
          { name: "id", type: "serial", notNull: true, primaryKey: true },
          { name: "label", type: "text" },
        ],
        indexes: [],
      },
    ],
  };
  const nucleusOnlyOnText = structuredClone(base);
  nucleusOnlyOnText.tables[0].columns[1].nucleusOnly = true;
  assert.throws(
    () => readSchemaDocumentV1(nucleusOnlyOnText),
    /nucleusOnly is only valid for vector columns/,
  );
  const dimsOnText = structuredClone(base);
  dimsOnText.tables[0].columns[1].vectorDimensions = 3;
  assert.throws(
    () => readSchemaDocumentV1(dimsOnText),
    /vectorDimensions is only valid for vector columns/,
  );
});

test("X02: legacy vector dimensions must be a positive integer (Go parity)", () => {
  for (const dims of [0, -3, 2.5]) {
    const doc = {
      version: 1,
      tables: [
        {
          name: "t",
          columns: [
            { name: "id", type: "serial", notNull: true, primaryKey: true },
            { name: "embedding", type: "vector", notNull: true, nucleusOnly: true, vectorDimensions: dims },
          ],
          indexes: [],
        },
      ],
    };
    assert.throws(
      () => readSchemaDocumentV1(doc),
      /vector type requires vectorDimensions > 0/,
      `dims=${dims}`,
    );
  }
});
