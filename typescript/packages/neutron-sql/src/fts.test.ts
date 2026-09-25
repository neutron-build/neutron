import assert from "node:assert/strict";
import test from "node:test";
import {
  pgTable,
  serial,
  text,
  index,
  sql,
  compileStatement,
  selectStatement,
  projection,
  ident,
  exportSchemaV2,
  type Condition,
} from "./index.js";
import { tsvector, toTsvector, toTsquery, plaintoTsquery, phrasetoTsquery, websearchToTsquery, matches, tsRank, tsRankCd } from "./fts.js";

// ---------------------------------------------------------------------------
// X01 unit coverage for the FTS surface: expression compilation, capability
// requirements, tsvector column export. Live ranking/ordering exactness vs
// raw-SQL twins lives in live.x01.fts.postgres.test.ts.
// ---------------------------------------------------------------------------

const posts = pgTable("x01_posts", {
  id: serial("id").primaryKey(),
  body: text("body").notNull(),
  tsv: tsvector("tsv"),
});

const compileExpr = (e: unknown): { sql: string; params: readonly unknown[] } => {
  const stmt = selectStatement({ projections: [projection(e as never, "v")], from: ident("x01_posts") });
  return compileStatement(stmt);
};

test("fts: to_tsvector/websearch compile with regconfig-bound parameters", () => {
  const cases: Array<[string, unknown, string, unknown[]]> = [
    ["toTsvector col", toTsvector("english", posts.body), `to_tsvector($1::text::regconfig, "body")`, ["english"]],
    ["toTsvector str", toTsvector("simple", "hello world"), `to_tsvector($1::text::regconfig, $2::text::text)`, ["simple", "hello world"]],
    ["websearch", websearchToTsquery("english", 'neutron "exact phrase"'), `websearch_to_tsquery($1::text::regconfig, $2::text::text)`, ["english", 'neutron "exact phrase"']],
    ["plainto", plaintoTsquery("english", "the cat"), `plainto_tsquery($1::text::regconfig, $2::text::text)`, ["english", "the cat"]],
    ["phraseto", phrasetoTsquery("english", "the cat sat"), `phraseto_tsquery($1::text::regconfig, $2::text::text)`, ["english", "the cat sat"]],
    ["toTsquery", toTsquery("english", "cat & dog"), `to_tsquery($1::text::regconfig, $2::text::text)`, ["english", "cat & dog"]],
  ];
  for (const [name, e, wantSql, wantParams] of cases) {
    const c = compileExpr(e);
    assert.equal(c.sql, `select ${wantSql} as "v" from "x01_posts"`, name);
    assert.deepEqual(c.params, wantParams, name);
  }
});

test("fts: config names validate as identifiers (never spliced into SQL text)", () => {
  assert.throws(() => toTsvector("english'; drop table x; --", posts.body), /plain lowercase identifiers/);
  assert.throws(() => websearchToTsquery("ENGLISH", "x"), /plain lowercase identifiers/);
});

test("fts: match predicate compiles to the @@ operator", () => {
  const c = compileExpr(matches(toTsvector("english", posts.body), websearchToTsquery("english", "cat")));
  assert.equal(c.sql, `select (to_tsvector($1::text::regconfig, "body") @@ websearch_to_tsquery($2::text::regconfig, $3::text::text)) as "v" from "x01_posts"`);
  assert.deepEqual(c.params, ["english", "english", "cat"]);
});

test("fts: ts_rank/ts_rank_cd compile with optional normalization", () => {
  const tsv = toTsvector("english", posts.tsv as never);
  const q = websearchToTsquery("english", "cat");
  const rank = compileExpr(tsRank(tsv, q));
  assert.equal(rank.sql, `select ts_rank(to_tsvector($1::text::regconfig, "tsv"), websearch_to_tsquery($2::text::regconfig, $3::text::text)) as "v" from "x01_posts"`);
  const rankN = compileExpr(tsRankCd(tsv, q, 8));
  assert.match(rankN.sql, /ts_rank_cd\(.*,\s*\$4::text::int4\)/);
  assert.deepEqual(rankN.params, ["english", "english", "cat", "8"]);
  assert.throws(() => tsRank(tsv, q, 33 as never), /bitmask 0..32/);
  assert.throws(() => tsRankCd(tsv, q, -1 as never), /bitmask 0..32/);
});

test("fts: expressions carry capability requirements", () => {
  const caps = (e: unknown): string[] => [...((e as { requires?: string[] }).requires ?? [])].sort();
  assert.deepEqual(caps(toTsvector("english", posts.body)), ["fts-functions"]);
  assert.deepEqual(caps(websearchToTsquery("english", "x")), ["fts-functions", "fts-websearch-tsquery"]);
  assert.deepEqual(caps(matches(toTsvector("english", posts.body), toTsquery("english", "x"))), ["fts-functions"]);
  assert.deepEqual(caps(tsRank(toTsvector("english", posts.body), toTsquery("english", "x"))), ["fts-functions"]);
});

test("fts: plans collect FTS requirements (fail-closed substrate)", async () => {
  const { createDatabase } = await import("./index.js");
  const db = await createDatabase({
    url: "postgres://snapshot:nouser@127.0.0.1:1/none",
    driverOptions: { driver: "postgres" },
    tables: { posts },
  });
  const q = db
    .select({ id: posts.id, rank: tsRank(toTsvector("english", posts.body), websearchToTsquery("english", "cat")) })
    .from(posts)
    .where(matches(toTsvector("english", posts.body), websearchToTsquery("english", "cat")) as unknown as Condition)
    .limit(5);
  const compiled = q.toCompiled();
  assert.deepEqual([...compiled.capabilities].sort(), ["fts-functions", "fts-websearch-tsquery"]);
});

test("fts: tsvector columns export through schema document v2 (no capability required — core PG)", () => {
  const t = pgTable("x01_fts", { id: serial("id").primaryKey(), tsv: tsvector("tsv") }, (tb) => [
    index("x01_fts_tsv_gin").using("gin").on(tb.tsv),
  ]);
  const doc = exportSchemaV2({ t });
  assert.deepEqual(doc.capabilities, []);
  const col = doc.tables[0].columns.find((c) => c.name === "tsv")!;
  assert.equal(col.type.name, "tsvector");
  assert.equal(col.type.codec, "tsvector");
  const idx = doc.tables[0].indexes[0];
  assert.equal(idx.method, "gin");
  assert.deepEqual(idx.key.map((k) => k.column), ["tsv"]);
});

test("fts: gin expression keys stay outside the contract (default opclass unverifiable); tsvector columns are the representable pattern", () => {
  // The sql template renders the expression fine...
  const doc1 = exportSchemaV2({
    t: pgTable("x01_fts2", { id: serial("id").primaryKey(), body: text("body").notNull() }, (tb) => [
      index("x01_fts2_body_gin").using("btree").on(sql`to_tsvector('english', ${tb.body})`),
    ]),
  });
  assert.deepEqual(doc1.tables[0].indexes[0].key.map((k) => k.expression), [`to_tsvector('english', "body")`]);
  // ...but a GIN expression index is refused at export: the default
  // operator class of an expression result cannot be verified at definition
  // time (Q07 discipline). A stored/generated tsvector column + gin column
  // index is the representable FTS-index shape.
  assert.throws(
    () =>
      exportSchemaV2({
        t: pgTable("x01_fts3", { id: serial("id").primaryKey(), body: text("body").notNull() }, (tb) => [
          index("x01_fts3_body_gin").using("gin").on(sql`to_tsvector('english', ${tb.body})`),
        ]),
      }),
    /expression keys are only definable with method "btree"/,
  );
});

test("fts: tsvector columns fail closed in the legacy DDL emitter", async () => {
  const { schemaToDDL } = await import("./index.js");
  assert.throws(() => schemaToDDL([posts]), /legacy DDL emitter cannot emit it/s);
});
