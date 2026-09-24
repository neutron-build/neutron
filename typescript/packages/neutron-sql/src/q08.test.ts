import assert from "node:assert/strict";
import test from "node:test";
import {
  alias,
  count,
  createDatabase,
  desc,
  eq,
  integer,
  pgTable,
  serial,
  sql,
  text,
  cteTable,
  derivedTable,
  lockingClause,
  compileStatement,
  selectStatement,
  MAX_STREAM_BATCH_SIZE,
  DEFAULT_STREAM_BATCH_SIZE,
  type LockStrength,
} from "./index.js";

// ---------------------------------------------------------------------------
// Q08 — locking clauses, batch plans and stream plans (unit leg).
// Deterministic SQL text, fail-closed rejections and pure plan inspection.
// Live V13/V16 oracles (two-connection locking semantics, atomicity, early
// exit, pool reuse) live in live.q08.postgres.test.ts.
// ---------------------------------------------------------------------------

const jobs = pgTable("q08_jobs", {
  id: serial("id").primaryKey(),
  queue: text("queue").notNull(),
  state: text("state").notNull(),
  attempts: integer("attempts").notNull(),
});

const owners = pgTable("q08_owners", {
  id: serial("id").primaryKey(),
  jobId: integer("job_id").notNull(),
});

const db = await createDatabase({
  url: "postgres://snapshot:nouser@127.0.0.1:1/none",
  driverOptions: { driver: "postgres" },
  tables: { q08_jobs: jobs, q08_owners: owners },
});

// ---------------------------------------------------------------------------
// Locking: rendering
// ---------------------------------------------------------------------------

test("locking: for update renders after limit/offset with every strength and wait policy", () => {
  assert.equal(
    db.select().from(jobs).where(eq(jobs.queue, "a")).limit(10).for("update").toSQL().sql,
    'select "q08_jobs"."id", "q08_jobs"."queue", "q08_jobs"."state", "q08_jobs"."attempts" from "q08_jobs" where ("q08_jobs"."queue" = $1) limit 10 for update',
  );
  assert.equal(db.select().from(jobs).for("no key update").toSQL().sql.includes("for no key update"), true);
  assert.equal(db.select().from(jobs).for("share").toSQL().sql.includes("for share"), true);
  assert.equal(db.select().from(jobs).for("key share").toSQL().sql.includes("for key share"), true);
  assert.equal(db.select().from(jobs).for("update", { noWait: true }).toSQL().sql.includes("for update nowait"), true);
  assert.equal(db.select().from(jobs).for("update", { skipLocked: true }).toSQL().sql.includes("for update skip locked"), true);
});

test("locking: of names the from table and joined alias handles", () => {
  const o = alias(owners, "o");
  const q = db
    .select({ id: jobs.id })
    .from(jobs)
    .innerJoin(o, sql`${o.jobId} = ${jobs.id}`)
    .for("update", { of: [jobs, o] })
    .toSQL();
  assert.equal(q.sql.endsWith('for update of "q08_jobs", "o"'), true);
  const q2 = db.select({ id: jobs.id }).from(jobs).innerJoin(o, sql`${o.jobId} = ${jobs.id}`).for("share", { of: o }).toSQL();
  assert.equal(q2.sql.endsWith('for share of "o"'), true);
});

test("locking: multiple clauses accumulate in call order", () => {
  const q = db.select().from(jobs).for("update", { of: jobs }).for("share", { of: jobs }).toSQL();
  assert.equal(q.sql.endsWith('for update of "q08_jobs" for share of "q08_jobs"'), true);
});

test("locking: statements without locks keep their exact pre-Q08 shape", () => {
  const stmt = selectStatement({
    from: sql`${jobs}` as never,
    projections: [],
  });
  assert.equal("locking" in stmt, false);
  assert.equal(db.select().from(jobs).toSQL().sql.includes("for "), false);
});

// ---------------------------------------------------------------------------
// Locking: builder validation
// ---------------------------------------------------------------------------

test("locking: for() validates strength, options and OF targets", () => {
  assert.throws(() => db.select().from(jobs).for("exclusive" as LockStrength), /unknown lock strength/);
  assert.throws(() => db.select().from(jobs).for("update", { noWait: true, skipLocked: true }), /mutually exclusive/);
  assert.throws(() => db.select().from(jobs).for("update", { wat: 1 } as never), /unknown option/);
  assert.throws(() => db.select().from(jobs).for("update", { of: [] }), /at least one from item/);
  assert.throws(() => db.select().from(jobs).for("update", { of: 42 as never }).toSQL(), /of entries must be/);
  assert.throws(
    () => db.select().from(jobs).for("update", { of: owners }).toSQL(),
    /not this statement's from table.*alias\(\) handle/,
  );
});

test("locking: of rejects unknown aliases and CTE handles honestly", () => {
  const o = alias(owners, "o");
  const notJoined = alias(owners, "elsewhere");
  assert.throws(
    () => db.select({ id: jobs.id }).from(jobs).innerJoin(o, sql`${o.jobId} = ${jobs.id}`).for("update", { of: notJoined }).toSQL(),
    /which is not joined in this statement/,
  );
  const cte = cteTable("cte_src", db.select({ id: jobs.id, queue: jobs.queue }).from(jobs));
  assert.throws(
    () => db.select().from(cte).for("update", { of: cte as never }).toSQL(),
    /of cannot name the CTE "cte_src".*SQLSTATE 0A000/,
  );
  const unrelated = derivedTable("d_unrelated", db.select({ id: jobs.id }).from(jobs));
  assert.throws(
    () => db.select().from(jobs).for("update", { of: unrelated }).toSQL(),
    /neither this statement's from item nor joined here/,
  );
});

test("locking: derived FROM items lock through to their base rows (PG-17 verified matrix)", () => {
  // Two-connection live oracle (live.q08): PostgreSQL 17 propagates a row
  // lock through plain/limited/nested/joined derived tables and through OF
  // on a derived alias (a competing base-row lock hits 55P03). The builder
  // must ALLOW these and render the subquery forms.
  const plain = derivedTable("d", db.select({ id: jobs.id }).from(jobs));
  assert.equal(
    db.select().from(plain).for("update").toSQL().sql,
    'select "d"."id" from (select "q08_jobs"."id" from "q08_jobs") as "d" for update',
  );
  // A limited derivation still locks the base rows it emits.
  const limited = derivedTable("d_lim", db.select({ id: jobs.id }).from(jobs).limit(3));
  assert.doesNotThrow(() => db.select().from(limited).for("update").toSQL());
  // Derived over derived: the lock still reaches the base table.
  const inner = derivedTable("d_in", db.select({ id: jobs.id }).from(jobs));
  const outer = derivedTable("d_out", db.select({ id: inner.id }).from(inner));
  assert.doesNotThrow(() => db.select().from(outer).for("update").toSQL());
  // A derived table JOINed to a base table: both lock.
  const joined = derivedTable("d_join", db.select({ id: jobs.id }).from(jobs));
  assert.doesNotThrow(() =>
    db.select({ id: jobs.id }).from(jobs).innerJoin(joined, sql`${joined.id} = ${jobs.id}`).for("update").toSQL(),
  );
  // OF on the derived FROM handle renders the alias (PG locks the base rows
  // through it) and OF on a joined derived handle renders its join alias.
  assert.equal(db.select().from(plain).for("update", { of: plain }).toSQL().sql.endsWith('for update of "d"'), true);
  assert.equal(
    db.select({ id: jobs.id }).from(jobs).innerJoin(joined, sql`${joined.id} = ${jobs.id}`).for("update", { of: joined }).toSQL().sql.endsWith('for update of "d_join"'),
    true,
  );
});

test("locking: unqualified locks reject outer-join nullable sides and CTE-reaching FROM items", () => {
  const o = alias(owners, "o");
  assert.throws(
    () => db.select({ id: jobs.id }).from(jobs).leftJoin(o, sql`${o.jobId} = ${jobs.id}`).for("update").toSQL(),
    /nullable side of an outer join/,
  );
  // The real PG-17 hazard (two-connection oracle): a lock over a WITH query
  // locks NOTHING — silently, in every form (inline/materialized/multi-ref).
  const cte = cteTable("cte_src", db.select({ id: jobs.id }).from(jobs));
  assert.throws(
    () => db.select().from(cte).for("update").toSQL(),
    /an unqualified lock over "cte_src" would silently lock nothing.*WITH query/,
  );
  // Same hazard one level down: a derived table whose only FROM item is a
  // CTE — PG silently locks nothing through the CTE scan (fully silent).
  const cteInner = cteTable("c_in", db.select({ id: jobs.id }).from(jobs));
  const derivedOverCte = derivedTable("d_over", db.select({ id: cteInner.id }).from(cteInner));
  assert.throws(
    () => db.select().from(derivedOverCte).for("update").toSQL(),
    /an unqualified lock over "d_over" would silently lock nothing.*WITH query/,
  );
  assert.throws(
    () => db.select().from(derivedOverCte).for("update", { of: derivedOverCte }).toSQL(),
    /of "d_over" would silently lock nothing/,
  );
  // MIXED derivation (base table + CTE on the join side): PG locks the base
  // rows through it and silently skips only the WITH-sourced rows — partial
  // silence (two-connection verified: base row 55P03, CTE-source row free,
  // inline and materialized, unqualified and OF forms). Still rejected: part
  // of what the lock promises is silently dropped.
  const cteJoinSide = cteTable("c_side", db.select({ jobId: owners.jobId }).from(owners));
  const derivedJoinCte = derivedTable(
    "d_jc",
    db.select({ id: jobs.id }).from(jobs).innerJoin(cteJoinSide, sql`${cteJoinSide.jobId} = ${jobs.id}`),
  );
  assert.throws(
    () => db.select().from(derivedJoinCte).for("update").toSQL(),
    /an unqualified lock over "d_jc" would be partially silent.*base tables.*WITH query.*"d_jc"/,
  );
  assert.throws(
    () => db.select().from(derivedJoinCte).for("update", { of: derivedJoinCte }).toSQL(),
    /of "d_jc" would be partially silent.*base tables.*WITH query/,
  );
  // The same mix at the outer level (a base table with a CTE joined to it):
  // base rows lock, the WITH-sourced rows are silently skipped.
  assert.throws(
    () => db.select({ id: jobs.id }).from(jobs).innerJoin(cteJoinSide, sql`${cteJoinSide.jobId} = ${jobs.id}`).for("update").toSQL(),
    /an unqualified lock over "q08_jobs", "c_side" would be partially silent.*"c_side"/,
  );
  // A derivation over a set operation is NOT silent-classified: PostgreSQL
  // errors on the lock itself (0A000), so the statement goes through and
  // the server rejects it honestly (covered live).
  const unionSrc = db.select({ id: jobs.id }).from(jobs).union(db.select({ id: owners.jobId }).from(owners));
  assert.doesNotThrow(() => db.select().from(derivedTable("d_u", unionSrc)).for("update").toSQL());
  // Qualifying with of keeps the outer-join case valid (non-nullable side).
  assert.doesNotThrow(() =>
    db.select({ id: jobs.id }).from(jobs).leftJoin(o, sql`${o.jobId} = ${jobs.id}`).for("update", { of: jobs }).toSQL(),
  );
});

test("locking: capability requirements attach per strength and policy", () => {
  assert.deepEqual(db.select().from(jobs).for("update").toCompiled().capabilities, ["row-locking"]);
  assert.deepEqual(db.select().from(jobs).for("key share").toCompiled().capabilities, ["row-locking", "row-locking-key-strength"]);
  assert.deepEqual(db.select().from(jobs).for("no key update").toCompiled().capabilities, ["row-locking", "row-locking-key-strength"]);
  assert.deepEqual(db.select().from(jobs).for("update", { skipLocked: true }).toCompiled().capabilities, ["row-locking", "row-locking-skip-locked"]);
});

// ---------------------------------------------------------------------------
// Locking: compile choke point (PostgreSQL rejects these shapes itself)
// ---------------------------------------------------------------------------

test("locking: compile rejects locks on distinct/grouped/aggregated/windowed statements", () => {
  assert.throws(() => db.select({ q: jobs.queue }).from(jobs).distinct().for("update").toSQL(), /not allowed with DISTINCT/);
  assert.throws(() => db.select({ q: jobs.queue }).from(jobs).groupBy(jobs.queue).for("update").toSQL(), /not allowed with GROUP BY/);
  assert.throws(() => db.select({ n: count() }).from(jobs).having(sql`count(*) > ${1}`).for("update").toSQL(), /not allowed with HAVING/);
  assert.throws(() => db.select({ n: count() }).from(jobs).for("update").toSQL(), /not allowed with aggregate functions/);
});

test("locking: set operations refuse locking branches before any rebuild", () => {
  const a = () => db.select({ id: jobs.id }).from(jobs);
  const locked = () => db.select({ id: jobs.id }).from(jobs).for("update");
  assert.throws(() => locked().union(a()), /first branch carries a locking clause/);
  assert.throws(() => a().union(locked()).toSQL(), /locking clause/);
});

test("locking: lockingClause() validates hand-built nodes (forged ASTs fail closed)", () => {
  assert.throws(() => lockingClause({ strength: "super" as LockStrength }), /unknown lock strength/);
  assert.throws(() => lockingClause({ strength: "update", wait: "patiently" as never }), /unknown lock wait policy/);
  assert.throws(() => lockingClause({ strength: "update", of: ["a.b"] }), /schema-qualified/);
  assert.throws(() => lockingClause({ strength: "update", of: ["a", "a"] }), /named twice/);
  assert.throws(() => lockingClause({ strength: "update", of: [""] }), /non-empty/);
  // A forged node bypassing the constructor is stopped at the compile choke
  // point too.
  // A forged node bypassing the constructor is stopped at BOTH gates:
  // selectStatement validates its inputs, and compileStatement re-checks.
  const forged = { kind: "locking", strength: "update", of: ["jobs"], wait: "wait" } as const;
  const stmt = selectStatement({
    from: sql`${jobs}` as never,
    projections: [],
    locking: [forged],
  });
  assert.doesNotThrow(() => compileStatement(stmt));
  assert.throws(
    () =>
      compileStatement(
        selectStatement({ from: sql`${jobs}` as never, projections: [], locking: [{ ...forged, wait: "forever" } as never] }),
      ),
    /unknown lock wait policy/,
  );
  assert.throws(
    () => selectStatement({ from: sql`${jobs}` as never, projections: [], locking: [{ ...forged, wait: "forever" } as never] }),
    /unknown lock wait policy/,
  );
});

// ---------------------------------------------------------------------------
// Batch: pure plan surface
// ---------------------------------------------------------------------------

test("batch: explain lists every statement with the transaction envelope", () => {
  const b = db.batch([
    db.select({ n: count() }).from(jobs),
    db.update(jobs).set({ state: "ready" }).where(sql`${jobs.id} < ${5}`),
    db.insert(jobs).values({ queue: "q", state: "new", attempts: 0 }),
  ]);
  const plan = b.explain();
  assert.equal(plan.strategy, "sequential-one-connection");
  assert.equal(plan.transaction.ownership, "own");
  assert.equal(plan.transaction.retry, false);
  assert.equal(plan.transaction.begin, "begin isolation level repeatable read");
  assert.equal(plan.statementCount, 3);
  assert.deepEqual(
    plan.statements.map((s) => s.result),
    ["rows", "count", "count"],
  );
  assert.match(plan.statements[1].sql, /^update "q08_jobs" set "state" = \$1 where \("q08_jobs"\."id" < \$2\)$/);
  assert.deepEqual(plan.statements[1].params, ["ready", 5]);
  assert.equal(new Set(plan.capabilities).size, plan.capabilities.length);
});

test("batch: explicit isolation replaces the repeatable-read default", () => {
  const plan = db.batch([db.select({ n: count() }).from(jobs)], { isolation: "read-committed" }).explain();
  assert.equal(plan.transaction.begin, "begin isolation level read committed");
});

test("batch: rejects empty input, non-batchable items and unknown options", () => {
  assert.throws(() => db.batch([]), /non-empty array/);
  assert.throws(() => db.batch([Promise.resolve(1) as never]), /not a batchable query builder/);
  assert.throws(() => db.batch([db.select().from(jobs)], { wat: 1 } as never), /unknown option/);
});

test("batch: set operations participate as row queries", () => {
  const a = db.select({ id: jobs.id }).from(jobs);
  const plan = db.batch([a.union(db.select({ id: jobs.id }).from(jobs))]).explain();
  assert.equal(plan.statements[0].result, "rows");
});

// ---------------------------------------------------------------------------
// Stream: pure plan surface
// ---------------------------------------------------------------------------

test("stream: explain describes the cursor lifecycle exactly", () => {
  const s = db.select().from(jobs).stream({ batchSize: 25 });
  const plan = s.explain();
  assert.equal(plan.strategy, "server-cursor");
  assert.equal(plan.transaction, "own");
  assert.equal(plan.batchSize, 25);
  assert.equal(plan.earlyExit, "rollback");
  assert.deepEqual(plan.statements.map((x) => x.role), ["begin", "declare", "fetch", "close", "commit"]);
  assert.equal(plan.statements[0].sql, "begin");
  assert.match(plan.statements[1].sql, /^declare "neutron_cursor_\d+" no scroll cursor for select /);
  assert.match(plan.statements[2].sql, /^fetch forward 25 from "neutron_cursor_\d+"$/);
  assert.equal(plan.statements[2].repeats, true);
  assert.deepEqual(plan.capabilities, ["server-cursors"]);
});

test("stream: option validation is constructor-level (no connection touched)", () => {
  assert.throws(() => db.select().from(jobs).stream({ batchSize: 0 }), /batchSize must be an integer/);
  assert.throws(() => db.select().from(jobs).stream({ batchSize: MAX_STREAM_BATCH_SIZE + 1 }), /batchSize must be an integer/);
  assert.throws(() => db.select().from(jobs).stream({ batchSize: 1.5 }), /batchSize must be an integer/);
  assert.throws(() => db.select().from(jobs).stream({ wat: 1 } as never), /unknown option/);
  assert.throws(() => db.select().from(jobs).stream({ deadlineMs: -1 }), /deadlineMs must be a positive/);
  assert.equal(DEFAULT_STREAM_BATCH_SIZE, 100);
});

test("stream: capabilities include the query's own requirements", () => {
  const s = db.select({ id: jobs.id }).from(jobs).stream();
  assert.deepEqual(s.explain().capabilities, ["server-cursors"]);
});
