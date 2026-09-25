import assert from "node:assert/strict";
import test from "node:test";
import {
  pgTable,
  serial,
  integer,
  text,
  index,
  compileStatement,
  selectStatement,
  projection,
  ident,
  createDatabase,
  exportSchemaV2,
  asc,
  desc,
  sql,
  type Condition,
} from "./index.js";
import {
  pgVector,
  l2Distance,
  innerProduct,
  cosineDistance,
  l1Distance,
  MAX_VECTOR_DIMENSIONS,
  withVectorOpclass,
  type VectorDistanceExpr,
} from "./pgvector.js";

// ---------------------------------------------------------------------------
// X01 unit coverage: definition validation, typed distance expressions,
// index metadata, export v2 shapes. Live round-trips (write -> query ->
// inspect, EXPLAIN index plans, dimension failures at the server, extension
// gating) live in live.x01.*.test.ts.
// ---------------------------------------------------------------------------

const docs = pgTable("x01_docs", {
  id: serial("id").primaryKey(),
  title: text("title").notNull(),
  embedding: pgVector("embedding", 3).notNull(),
});

test("pgVector: definition-time dimension validation", () => {
  assert.equal(MAX_VECTOR_DIMENSIONS, 16000);
  for (const bad of [0, -1, 1.5, NaN, 16001, Infinity]) {
    assert.throws(() => pgVector("emb", bad as number), /dimensions must be an integer between 1 and 16000/, `dims=${bad}`);
  }
  for (const good of [1, 2, 2000, 16000]) {
    const c = pgVector("emb", good);
    assert.equal(c.vectorDimensions, good);
  }
});

test("pgVector: distance expressions compile to the pgvector operators with literal-text binds", () => {
  const cases: Array<[string, VectorDistanceExpr, string, string]> = [
    ["l2", l2Distance(docs.embedding, [1, 2, 3]), "<->", "[1,2,3]"],
    ["innerProduct", innerProduct(docs.embedding, [0.5, -1, 2]), "<#>", "[0.5,-1,2]"],
    ["cosine", cosineDistance(docs.embedding, [0.1, 0.2, 0.3]), "<=>", "[0.1,0.2,0.3]"],
    ["l1", l1Distance(docs.embedding, [4, 5, 6]), "<+>", "[4,5,6]"],
  ];
  for (const [name, expr, op, bind] of cases) {
    const stmt = selectStatement({ projections: [projection(expr, "d")], from: ident("x01_docs") });
    const compiled = compileStatement(stmt);
    assert.equal(compiled.sql, `select ("embedding" ${op} $1::text::vector) as "d" from "x01_docs"`, name);
    assert.deepEqual(compiled.params, [bind], name);
  }
});

test("pgVector: distance expressions carry their capability requirements", () => {
  const capsOf = (e: VectorDistanceExpr): Set<string> => {
    const out = new Set<string>();
    for (const c of (e as unknown as { requires?: string[] }).requires ?? []) out.add(c);
    return out;
  };
  assert.deepEqual(capsOf(l2Distance(docs.embedding, [1, 2, 3])), new Set(["vector-type", "vector-operator-l2"]));
  assert.deepEqual(capsOf(innerProduct(docs.embedding, [1, 2, 3])), new Set(["vector-type", "vector-operator-inner-product"]));
  assert.deepEqual(capsOf(cosineDistance(docs.embedding, [1, 2, 3])), new Set(["vector-type", "vector-operator-cosine"]));
  assert.deepEqual(capsOf(l1Distance(docs.embedding, [1, 2, 3])), new Set(["vector-type", "vector-operator-l1"]));
});

test("pgVector: wrong-dimension query vectors fail before any SQL", () => {
  assert.throws(() => l2Distance(docs.embedding, [1, 2]), /expects 3 dimensions, got 2/s);
  assert.throws(() => cosineDistance(docs.embedding, []), /expects 3 dimensions, got 0/s);
  assert.throws(() => l2Distance(docs.embedding, [1, 2, Number.NaN]), /arrays of finite numbers/);
});

test("pgVector: distance applies to vector columns only", () => {
  assert.throws(() => l2Distance(docs.title as never, [1, 2, 3]), /distance expressions apply to vector columns/);
});

test("pgVector: plans collect vector requirements from where/order/projections (fail-closed substrate)", async () => {
  const db = await createDatabase({
    url: "postgres://snapshot:nouser@127.0.0.1:1/none",
    driverOptions: { driver: "postgres" },
    tables: { docs },
  });
  // Distance filters compose through the sql template (structural splice of
  // the typed expression); ordering takes the expression directly.
  const q = db
    .select({ id: docs.id, d: l2Distance(docs.embedding, [1, 2, 3]) })
    .from(docs)
    .where(sql`${l2Distance(docs.embedding, [1, 2, 3])} < ${5}` as unknown as Condition)
    .orderBy(asc(l2Distance(docs.embedding, [1, 2, 3])))
    .limit(3);
  const compiled = q.toCompiled();
  assert.deepEqual([...compiled.capabilities].sort(), ["vector-operator-l2", "vector-type"]);
  // No execution happens (no connection): toSQL is pure.
  const { sql: text, params } = q.toSQL();
  assert.match(text, /"embedding" <-> \$1::text::vector/);
  assert.match(text, /order by \("embedding" <-> \$4::text::vector\) asc/);
  assert.deepEqual(params, ["[1,2,3]", "[1,2,3]", 5, "[1,2,3]"]);
});

test("pgVector: hnsw/ivfflat index metadata exports through schema document v2", () => {
  const table = pgTable(
    "x01_vec",
    {
      id: serial("id").primaryKey(),
      embedding: pgVector("embedding", 3).notNull(),
    },
    (t) => [
      index("x01_vec_embedding_hnsw")
        .using("hnsw")
        .on(t.embedding)
        .opclass(t.embedding, "vector_cosine_ops")
        .with({ m: 16, ef_construction: 64 }),
      index("x01_vec_embedding_ivfflat")
        .using("ivfflat")
        .on(t.embedding)
        .opclass(t.embedding, "vector_ip_ops")
        .with({ lists: 100 }),
    ],
  );
  const doc = exportSchemaV2({ table });
  assert.deepEqual(doc.capabilities, ["pgvector"]);
  const idx = doc.tables[0].indexes;
  assert.equal(idx.length, 2);
  const hnsw = idx.find((i) => i.identity.name === "x01_vec_embedding_hnsw")!;
  assert.equal(hnsw.method, "hnsw");
  assert.deepEqual(hnsw.key.map((k) => ({ column: k.column, opclass: k.opclass })), [{ column: "embedding", opclass: "vector_cosine_ops" }]);
  assert.deepEqual(hnsw.with, { m: 16, ef_construction: 64 });
  const ivf = idx.find((i) => i.identity.name === "x01_vec_embedding_ivfflat")!;
  assert.equal(ivf.method, "ivfflat");
  assert.deepEqual(ivf.with, { lists: 100 });
});

test("pgVector: index API validates opclass/with inputs", () => {
  const t = pgTable("x01_vec2", { id: serial("id").primaryKey(), embedding: pgVector("embedding", 3) });
  const idx = index("i").using("hnsw").on(t.embedding);
  assert.throws(() => idx.opclass(t.embedding, "Not Lower"), /plain lowercase identifiers/);
  assert.throws(() => idx.with({ "bad-name": 1 }), /plain lowercase identifiers/);
  assert.throws(() => idx.with({ m: 1 }).with({ m: 2 }), /called twice/);
  assert.throws(() => idx.opclass(t.id as never, "vector_ops"), /not a key part/);
  // Non-integer numbers pass the builder (a number), but the contract
  // carries integer access-method parameters only.
  const frac = pgTable("x01_vec_frac", { id: serial("id").primaryKey(), embedding: pgVector("embedding", 3) }, (tb) => [
    index("frac_hnsw").using("hnsw").on(tb.embedding).with({ m: 1.5 }),
  ]);
  assert.throws(() => exportSchemaV2({ frac }), /access-method parameters are integers/);
  assert.equal(typeof withVectorOpclass(index("j").using("hnsw").on(t.embedding), t.embedding, "vector_ip_ops"), "object");
});

test("pgVector: integer with-parameters round-trip on any method (introspected btree fillfactor reality)", () => {
  // X01 review-corrected: introspection captures btree reloptions like
  // fillfactor, so the contract carries integer with-parameters on ANY
  // method; the server validates the keys at DDL time.
  const good = pgTable("x01_vec4", { id: serial("id").primaryKey(), name: text("name") }, (tb) => [
    index("b").using("btree").on(tb.name).with({ fillfactor: 90 }),
  ]);
  const doc = exportSchemaV2({ good });
  assert.deepEqual(doc.tables[0].indexes[0].with, { fillfactor: 90 });
});

test("pgVector: vector columns fail closed in the legacy DDL emitter and v1 export", async () => {
  const t = pgTable("x01_vec5", { id: serial("id").primaryKey(), embedding: pgVector("embedding", 3) });
  const { schemaToDDL, exportSchema } = await import("./index.js");
  assert.throws(() => schemaToDDL([t]), /legacy DDL emitter cannot emit it/s);
  assert.throws(() => exportSchema({ t }), /legacy v1 export shape cannot represent it/s);
});

test("pgVector: raw sql fragments stay composable with the distance expressions", () => {
  const frag = sql`(${l2Distance(docs.embedding, [1, 2, 3])}) + (${cosineDistance(docs.embedding, [1, 2, 3])})`;
  const stmt = selectStatement({ projections: [projection(frag, "mixed")], from: ident("x01_docs") });
  const compiled = compileStatement(stmt);
  assert.equal(
    compiled.sql,
    `select (("embedding" <-> $1::text::vector)) + (("embedding" <=> $2::text::vector)) as "mixed" from "x01_docs"`,
  );
  assert.deepEqual(compiled.params, ["[1,2,3]", "[1,2,3]"]);
});

void desc;
void integer;
