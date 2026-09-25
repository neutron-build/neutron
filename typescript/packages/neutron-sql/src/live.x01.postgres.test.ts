import assert from "node:assert/strict";
import test from "node:test";
import { createDatabase, pgTable, serial, text, sql, asc, desc, type Condition, type NeutronDatabase } from "./index.js";
import { pgVector, l2Distance, innerProduct, cosineDistance, l1Distance, pgvectorExtension } from "./pgvector.js";
import { tsvector, toTsvector, websearchToTsquery, plaintoTsquery, matches, tsRank } from "./fts.js";
import { CapabilityRequirementError } from "./engine.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// ---------------------------------------------------------------------------
// X01 live battery: vectors and search through the ORM's own adapters.
//
// Three legs per driver, each in its own throwaway x01_-prefixed database:
//   1. FTS on a plain database (no extension — full-text search is core
//      PostgreSQL): write -> query -> inspect round-trips, ranking/ordering
//      exact against raw-SQL twins, tsvector column round-trip, reconnect
//      invariance.
//   2. pgvector on a database WITH `create extension vector` (the real
//      extension boundary — never mocked): typed writes/reads, dimension
//      failures before any SQL (statement counter 0), distance ordering
//      exact against raw twins for all four operators, EXPLAIN-verified
//      HNSW/IVFFlat index plans, hybrid vector+FTS journey, reconnect
//      invariance.
//   3. pgvector on a database WITHOUT the extension: the extension gate
//      reports available-not-installed, statements carrying vector
//      requirements fail closed BEFORE any SQL (statement counter 0, no
//      partial work), with the precise capability evidence.
// ---------------------------------------------------------------------------

type DriverKind = "postgres" | "pg";

interface Fixture {
  db: NeutronDatabase<Record<string, never>>;
  statements: { count: number };
  url: string;
  close: () => Promise<void>;
}

async function withDatabase(
  driverKind: DriverKind,
  dbName: string,
  createExtension: boolean,
  fn: (fx: Fixture) => Promise<void>,
): Promise<void> {
  if (!(await ensureLive(`live x01 (${driverKind})`))) return;
  const adminUrl = new URL(TEST_URL);
  const { Pool } = (await import("pg")) as unknown as {
    Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
  };
  const admin = new Pool({ connectionString: adminUrl.toString(), max: 1 });
  await admin.query(`drop database if exists "${dbName}"`);
  await admin.query(`create database "${dbName}"`);
  await admin.end();

  const url = new URL(TEST_URL);
  url.pathname = `/${dbName}`;
  const statements = { count: 0 };
  const db = await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
    logger: (e) => {
      if (e.kind === "query-end") statements.count += 1;
    },
  });
  try {
    if (createExtension) {
      await db.driver.execute("create extension vector");
    }
    await fn({ db, statements, url: url.toString(), close: async () => void (await db.driver.close()) });
  } finally {
    await db.driver.close().catch(() => undefined);
    const admin2 = new Pool({ connectionString: adminUrl.toString(), max: 1 });
    await admin2.query(`drop database if exists "${dbName}"`);
    await admin2.end();
  }
}


/** EXPLAIN with seq scans disabled on one pinned connection (five-row
 * tables never attract indexes by cost; this proves the index is usable for
 * the operator — the plan the planner picks at scale). Session state is
 * reset before the connection returns to the pool. */
async function explainWithSeqscanOff(db: NeutronDatabase<Record<string, never>>, querySql: string, params: unknown[]): Promise<string> {
  const pin = db.driver.pin;
  if (pin === undefined) throw new Error("driver does not support pinning");
  const pinned = await pin();
  try {
    await pinned.execute("set enable_seqscan = off");
    try {
      const rows = await pinned.query<Record<string, unknown>>(`explain (costs off) ${querySql}`, params);
      return rows.map((r) => String(Object.values(r)[0])).join("\n");
    } finally {
      await pinned.execute("reset enable_seqscan").catch(() => undefined);
    }
  } finally {
    pinned.release();
  }
}

// ---------------------------------------------------------------------------
// Leg 3 first: extension ABSENT — fail closed, no partial work.
// ---------------------------------------------------------------------------

const absentDocs = pgTable("x01_absent_docs", {
  id: serial("id").primaryKey(),
  embedding: pgVector("embedding", 3).notNull(),
});

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live x01 (${driverKind}): extension gate reports available-not-installed; vector statements fail closed pre-SQL`, async () => {
    await withDatabase(driverKind, uniqueDbName("x01_absent"), false, async (fx) => {
      const info = await pgvectorExtension(fx.db.driver);
      assert.equal(info.status, "available", `expected the brew pgvector to be server-available, got ${JSON.stringify(info)}`);
      assert.match(info.availableVersion!, /^0\./);
      assert.match(info.detail, /create extension vector/);

      // A statement carrying vector requirements is rejected before any SQL
      // runs — statement counter stays at the extension probes' level.
      const before = fx.statements.count;
      const rejected = fx.db
        .select({ id: absentDocs.id, d: l2Distance(absentDocs.embedding, [1, 2, 3]) })
        .from(absentDocs)
        .orderBy(asc(l2Distance(absentDocs.embedding, [1, 2, 3])))
        .limit(3)
        .execute();
      await assert.rejects(
        () => rejected,
        (err: unknown) => {
          assert.ok(err instanceof CapabilityRequirementError, `expected CapabilityRequirementError, got ${err}`);
          const failing = err.results.filter((r) => r.status !== "supported");
          assert.ok(failing.some((r) => r.capability === "vector-type" && r.status === "unsupported"), "vector-type must resolve unsupported with probe evidence");
          assert.match(failing[0].evidence, /sqlstate 42704|does not exist|unknown/i);
          return true;
        },
      );
      // The capability probes run on the raw driver (unlogged by design);
      // the statement logger counts relational statements only. ZERO logged
      // statements around the rejection — no partial work, and the probe
      // evidence in the error is the live server's own refusal.
      assert.equal(fx.statements.count, before, `no logged relational statement may run around a fail-closed rejection (before=${before}, after=${fx.statements.count})`);

      // No table was created: the failed statement executed nothing.
      const tables = await fx.db.driver.query<{ n: string }>(
        `select table_name as n from information_schema.tables where table_name = 'x01_absent_docs'`,
      );
      assert.equal(tables.length, 0);
    });
  });
}

// ---------------------------------------------------------------------------
// Leg 2: extension PRESENT — the real pgvector boundary.
// ---------------------------------------------------------------------------

const docs = pgTable("x01_docs", {
  id: serial("id").primaryKey(),
  body: text("body").notNull(),
  embedding: pgVector("embedding", 3).notNull(),
});

const V = [
  [1, 0, 0],
  [0, 1, 0],
  [0.9, 0.1, 0],
  [0, 0, 1],
  [0.7, 0.7, 0],
];

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live x01 (${driverKind}): pgvector write->query->inspect, dimension failures, index plans, hybrid, reconnect`, async () => {
    await withDatabase(driverKind, uniqueDbName("x01_vec"), true, async (fx) => {
      const { db, statements } = fx;

      // Extension gate: installed, with the exact version.
      const info = await pgvectorExtension(db.driver);
      assert.equal(info.status, "installed", JSON.stringify(info));
      assert.match(info.installedVersion!, /^0\.\d+/);

      // Create the table (typed vector column, dimension in DDL).
      await db.driver.execute(`create table "x01_docs" ("id" serial primary key, "body" text not null, "embedding" vector(3) not null)`);

      // Inspect: the column is vector(3) in the catalog.
      const col = await db.driver.query<{ atttypmod: number; typname: string }>(
        `select format_type(a.atttypid, a.atttypmod) as typname, a.atttypmod from pg_attribute a join pg_class c on c.oid = a.attrelid where c.relname = 'x01_docs' and a.attname = 'embedding'`,
      );
      assert.equal(col[0].typname, "vector(3)");

      // Dimension failure at WRITE time: precise client error, zero SQL.
      const before = statements.count;
      await assert.rejects(
        () => db.insert(docs).values({ body: "bad", embedding: [1, 2] }).execute(),
        /vector column expects 3 dimensions, got 2/s,
      );
      assert.equal(statements.count, before, "a wrong-dimension write must not execute any SQL");

      // Dimension failure at QUERY time (distance bind): the bind is
      // validated when the expression is built — before any SQL exists.
      assert.throws(
        () => sql`${cosineDistance(docs.embedding, [1, 2])} < ${0.5}`,
        /expects 3 dimensions, got 2/s,
      );
      assert.equal(statements.count, before);

      // Write -> read round-trip.
      await db.insert(docs).values(V.map((embedding, i) => ({ body: `doc ${i} about cats and databases`, embedding }))).execute();
      const rows = await db.select().from(docs).orderBy(asc(docs.id)).execute();
      assert.equal(rows.length, 5);
      for (let i = 0; i < V.length; i++) {
        assert.ok(Array.isArray(rows[i].embedding), "decoded embedding is number[]");
        assert.equal(rows[i].embedding.length, 3);
        for (let d = 0; d < 3; d++) {
          assert.ok(Math.abs(rows[i].embedding[d] - V[i][d]) < 1e-6, `row ${i} dim ${d}: ${rows[i].embedding[d]} vs ${V[i][d]}`);
        }
      }

      // Distance ordering exact vs the raw-SQL twin, all four operators.
      for (const [name, makeExpr, rawOp] of [
        ["l2", l2Distance, "<->"],
        ["innerProduct", innerProduct, "<#>"],
        ["cosine", cosineDistance, "<=>"],
        ["l1", l1Distance, "<+>"],
      ] as const) {
        const q: number[] = [0.95, 0.05, 0];
        const viaOrm = await db
          .select({ id: docs.id, d: makeExpr(docs.embedding, q) })
          .from(docs)
          .orderBy(asc(makeExpr(docs.embedding, q)))
          .execute();
        const viaRaw = await db.driver.query<{ id: number; d: number }>(
          `select id, embedding ${rawOp} $1::vector as d from x01_docs order by embedding ${rawOp} $1::vector`,
          [`[${q.join(",")}]`],
        );
        assert.deepEqual(viaOrm.map((r) => r.id), viaRaw.map((r) => r.id), `${name} ordering must equal the raw twin`);
        for (let i = 0; i < viaOrm.length; i++) {
          // Same server computation -> exactly equal doubles.
          assert.equal(viaOrm[i].d, viaRaw[i].d, `${name} distance row ${i}`);
        }
      }

      // Index plans: build HNSW (cosine opclass) + IVFFlat, then EXPLAIN the
      // ORM-generated statement text — the planner must use the index.
      await db.driver.execute(`create index x01_docs_emb_hnsw on x01_docs using hnsw (embedding vector_cosine_ops) with (m = 16, ef_construction = 64)`);
      await db.driver.execute(`create index x01_docs_emb_ivfflat on x01_docs using ivfflat (embedding vector_ip_ops) with (lists = 10)`);
      await db.driver.execute("analyze x01_docs");

      const cosineQ = db
        .select({ id: docs.id })
        .from(docs)
        .orderBy(asc(cosineDistance(docs.embedding, [0.95, 0.05, 0])))
        .limit(3);
      const { sql: cosineSql, params: cosineParams } = cosineQ.toSQL();
      // Five rows: the planner prefers a seq scan by cost. Disabling seq
      // scans inside one transaction (pinned connection) proves the HNSW
      // index is usable for the <=> ordering — the EXPLAIN-verified plan.
      const planText = await explainWithSeqscanOff(db, cosineSql, cosineParams as unknown[]);
      assert.match(planText, /Index Scan/i);
      assert.match(planText, /x01_docs_emb_hnsw/);
      assert.match(planText, /embedding <=>/);
      // The ORM query with the index returns the same rows as the raw twin.
      const viaOrm = await cosineQ.execute();
      const viaRaw = await db.driver.query<{ id: number }>(
        `select id from x01_docs order by embedding <=> $1::vector limit 3`,
        ["[0.95,0.05,0]"],
      );
      assert.deepEqual(viaOrm.map((r) => r.id), viaRaw.map((r) => r.id));

      // Index metadata introspects back (inspect leg): both indexes carry
      // their opclass + reloptions in the catalog.
      const idx = await db.driver.query<{ name: string; def: string }>(
        `select ic.relname as name, pg_get_indexdef(i.indexrelid) as def from pg_index i join pg_class ic on ic.oid = i.indexrelid where ic.relname in ('x01_docs_emb_hnsw', 'x01_docs_emb_ivfflat') order by name`,
      );
      assert.equal(idx.length, 2);
      assert.match(idx[0].def, /using hnsw \(embedding vector_cosine_ops\).*m=.?16.?,.*ef_construction=.?64.?/i);
      assert.match(idx[1].def, /using ivfflat \(embedding vector_ip_ops\).*lists=.?10.?/i);

      // Hybrid journey: FTS rank first, vector distance as tie-breaker,
      // documented scoring: ORDER BY ts_rank DESC, embedding <=> q LIMIT n.
      const hybridQ = db
        .select({ id: docs.id, rank: tsRank(toTsvector("english", docs.body), websearchToTsquery("english", "databases")) })
        .from(docs)
        .where(matches(toTsvector("english", docs.body), websearchToTsquery("english", "databases or cats")) as unknown as Condition)
        .orderBy(sql`${tsRank(toTsvector("english", docs.body), websearchToTsquery("english", "databases"))} desc, ${cosineDistance(docs.embedding, [0.95, 0.05, 0])}`)
        .limit(4);
      const hybrid = await hybridQ.execute();
      const hybridRaw = await db.driver.query<{ id: number; rank: number }>(
        `select id, ts_rank(to_tsvector('english', body), websearch_to_tsquery('english', 'databases')) as rank
         from x01_docs
         where to_tsvector('english', body) @@ websearch_to_tsquery('english', 'databases or cats')
         order by ts_rank(to_tsvector('english', body), websearch_to_tsquery('english', 'databases')) desc, embedding <=> $1::vector asc
         limit 4`,
        ["[0.95,0.05,0]"],
      );
      assert.deepEqual(hybrid.map((r) => r.id), hybridRaw.map((r) => r.id));
      for (let i = 0; i < hybrid.length; i++) assert.equal(hybrid[i].rank, hybridRaw[i].rank);
      // Scoring semantics documented: rank is a float4 the server computed;
      // ordering is total only with the vector tie-break (equal ranks share).
      assert.ok(hybrid.length >= 1 && hybrid.length <= 4);

      // Reconnect invariance (V18 inspect after reconnect): close, reopen on
      // the same database, extension gate re-probes, results identical.
      await fx.close();
      const db2 = await createDatabase({ url: fx.url, driverOptions: { driver: driverKind } });
      try {
        const info2 = await pgvectorExtension(db2.driver);
        assert.equal(info2.status, "installed");
        assert.equal(info2.installedVersion, info.installedVersion);
        const reread = await db2.select().from(docs).orderBy(asc(docs.id)).execute();
        assert.deepEqual(reread.map((r) => r.embedding), rows.map((r) => r.embedding));
      } finally {
        await db2.driver.close();
      }
    });
  });
}

// ---------------------------------------------------------------------------
// Leg 1: FTS on a plain database — always available, both drivers.
// ---------------------------------------------------------------------------

const ftsPosts = pgTable("x01_fts_posts", {
  id: serial("id").primaryKey(),
  title: text("title").notNull(),
  body: text("body").notNull(),
  keywords: tsvector("keywords"),
});

const CORPUS = [
  { title: "cats", body: "The cat sat on the mat and purred loudly.", keywords: "animal pet" },
  { title: "dogs", body: "A dog barked at the cat next door.", keywords: "animal guard" },
  { title: "databases", body: "Postgres full text search indexes documents with tsvector.", keywords: "software search" },
  { title: "hybrid", body: "Vector search and text search combine into hybrid ranking.", keywords: "search ai" },
  { title: "noise", body: "Nothing relevant lives here.", keywords: "none" },
];

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live x01 (${driverKind}): FTS journeys on a plain database (no extension required)`, async () => {
    await withDatabase(driverKind, uniqueDbName("x01_fts"), false, async (fx) => {
      const { db } = fx;
      await db.driver.execute(`create table "x01_fts_posts" ("id" serial primary key, "title" text not null, "body" text not null, "keywords" tsvector)`);

      // tsvector column round-trip: write text, read the server-normalized
      // lexeme form ('animal':1 'pet':2).
      await db.insert(ftsPosts).values(CORPUS).execute();
      const rows = await db.select().from(ftsPosts).orderBy(asc(ftsPosts.id)).execute();
      assert.equal(rows[0].keywords, "'animal' 'pet'");
      assert.equal(rows[4].keywords, "'none'");

      // Match + ranking: exact agreement with the raw twin (order + values).
      const rankedQ = db
        .select({ id: ftsPosts.id, rank: tsRank(toTsvector("english", ftsPosts.body), websearchToTsquery("english", "search")) })
        .from(ftsPosts)
        .where(matches(toTsvector("english", ftsPosts.body), websearchToTsquery("english", "search")) as unknown as Condition)
        .orderBy(desc(sql`${tsRank(toTsvector("english", ftsPosts.body), websearchToTsquery("english", "search"))}`))
        .execute();
      const ranked = await rankedQ;
      const raw = await db.driver.query<{ id: number; rank: number }>(
        `select id, ts_rank(to_tsvector('english', body), websearch_to_tsquery('english', 'search')) as rank
         from x01_fts_posts
         where to_tsvector('english', body) @@ websearch_to_tsquery('english', 'search')
         order by ts_rank(to_tsvector('english', body), websearch_to_tsquery('english', 'search')) desc`,
      );
      assert.deepEqual(ranked.map((r) => r.id), raw.map((r) => r.id));
      for (let i = 0; i < ranked.length; i++) assert.equal(ranked[i].rank, raw[i].rank);

      // websearch semantics: quoted phrase + OR + exclusion, exact rows.
      const phraseQ = await db
        .select({ id: ftsPosts.id })
        .from(ftsPosts)
        .where(matches(toTsvector("english", ftsPosts.body), websearchToTsquery("english", '"text search" or vector')) as unknown as Condition)
        .execute();
      const phraseRaw = await db.driver.query<{ id: number }>(
        `select id from x01_fts_posts where to_tsvector('english', body) @@ websearch_to_tsquery('english', '"text search" or vector') order by id`,
      );
      assert.deepEqual(phraseQ.map((r) => r.id).sort((a, b) => a - b), phraseRaw.map((r) => r.id));

      // plainto_tsquery over the tsvector COLUMN directly.
      const kwQ = await db
        .select({ id: ftsPosts.id })
        .from(ftsPosts)
        .where(matches(ftsPosts.keywords, plaintoTsquery("english", "guard")) as unknown as Condition)
        .execute();
      assert.deepEqual(kwQ.map((r) => r.id), [2]);

      // Ranking normalization flag round-trips (rank exactness with 2|4).
      const rankN = await db
        .select({ id: ftsPosts.id, rank: tsRank(toTsvector("english", ftsPosts.body), websearchToTsquery("english", "search"), 6 as 2 | 4) })
        .from(ftsPosts)
        .where(matches(toTsvector("english", ftsPosts.body), websearchToTsquery("english", "search")) as unknown as Condition)
        .execute();
      const rankNRaw = await db.driver.query<{ rank: number }>(
        `select ts_rank(to_tsvector('english', body), websearch_to_tsquery('english', 'search'), 2|4) as rank
         from x01_fts_posts where to_tsvector('english', body) @@ websearch_to_tsquery('english', 'search') order by id`,
      );
      assert.deepEqual(rankN.map((r) => r.rank), rankNRaw.map((r) => r.rank));

      // GIN index over the tsvector column + EXPLAIN uses it.
      await db.driver.execute(`create index x01_fts_posts_kw_gin on x01_fts_posts using gin (keywords)`);
      await db.driver.execute("analyze x01_fts_posts");
      const ginText = await explainWithSeqscanOff(db, "select id from x01_fts_posts where keywords @@ plainto_tsquery('english', 'guard')", []);
      assert.match(ginText, /x01_fts_posts_kw_gin/);

      // Reconnect invariance.
      await fx.close();
      const db2 = await createDatabase({ url: fx.url, driverOptions: { driver: driverKind } });
      try {
        const reread = await db2.select().from(ftsPosts).orderBy(asc(ftsPosts.id)).execute();
        assert.deepEqual(reread.map((r) => r.keywords), rows.map((r) => r.keywords));
        const rereadRanked = await db2
          .select({ id: ftsPosts.id, rank: tsRank(toTsvector("english", ftsPosts.body), websearchToTsquery("english", "search")) })
          .from(ftsPosts)
          .where(matches(toTsvector("english", ftsPosts.body), websearchToTsquery("english", "search")) as unknown as Condition)
          .orderBy(desc(sql`${tsRank(toTsvector("english", ftsPosts.body), websearchToTsquery("english", "search"))}`))
          .execute();
        assert.deepEqual(rereadRanked.map((r) => r.id), ranked.map((r) => r.id));
      } finally {
        await db2.driver.close();
      }
    });
  });
}
