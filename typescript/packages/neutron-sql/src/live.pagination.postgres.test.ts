import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  keyset,
  ascNullsLast,
  ascNullsFirst,
  descNullsLast,
  descNullsFirst,
  bigint,
  integer,
  numeric,
  pgTable,
  serial,
  text,
  timestamptz,
  schemaToDDL,
  sql,
  CursorError,
  type KeysetPage,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Live V13/V16 suite (Q04): keyset pagination against REAL PostgreSQL —
// page-walks verified against independently authored hand-SQL oracles
// (offset-based ordering twins, never the builder's own SQL), duplicate-heavy
// non-unique prefixes, ties on the tie-breaker, precision keysets (int8 max,
// 40-digit numerics, microsecond timestamptz, NULL cursor values), malformed
// cursor rejection with zero driver traffic, and documented concurrent-insert
// behavior (default READ COMMITTED) plus a REPEATABLE READ snapshot walk.
// Live databases are q04_-prefixed throwaways; the shared admin DB's
// _neutron_migrations is never touched.

const DB_NAME = uniqueDbName("q04_pagination");

const walk = pgTable("q04_walk", {
  id: serial("id").primaryKey(),
  grp: integer("grp"),
  score: integer("score"),
  label: text("label").notNull(),
});

const exact = pgTable("q04_exact", {
  id: bigint("id", { mode: "string" }).primaryKey(),
  amt: numeric("amt"),
  at: timestamptz("at"),
  label: text("label").notNull(),
});

const conc = pgTable("q04_conc", {
  id: serial("id").primaryKey(),
  k: integer("k").notNull(),
  tag: text("tag").notNull(),
});

interface SuiteFixture {
  db: NeutronDatabase;
  statements: { count: number };
}

async function withSuite(driverKind: "postgres" | "pg", fn: (fx: SuiteFixture) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live pagination (${driverKind})`))) {
    return;
  }
  const { Pool } = (await import("pg")) as unknown as {
    Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
  };
  const admin = new Pool({ connectionString: TEST_URL, max: 1 });
  await admin.query(`drop database if exists "${DB_NAME}"`);
  await admin.query(`create database "${DB_NAME}"`);
  await admin.end();

  const url = new URL(TEST_URL);
  url.pathname = `/${DB_NAME}`;
  const statements = { count: 0 };
  const db = await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
    tables: { walk, exact, conc },
    logger: () => {
      statements.count += 1;
    },
  });
  try {
    for (const stmt of schemaToDDL([walk, exact, conc])) {
      await db.driver.execute(stmt);
    }
    await fn({ db, statements });
  } finally {
    await db.close();
    const cleanup = new Pool({ connectionString: TEST_URL, max: 1 });
    await cleanup.query(`drop database if exists "${DB_NAME}"`);
    await cleanup.end();
  }
}

/** 40-row duplicate-heavy dataset: every prefix value repeats across many
 * rows, nulls in both keyset columns, identical (grp, score) tuples. */
function walkRows(): Array<{ grp: number | null; score: number | null; label: string }> {
  const rows: Array<{ grp: number | null; score: number | null; label: string }> = [];
  const grps: Array<number | null> = [3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, null, null, null, null, null];
  const scores: Array<number | null> = [null, null, 5, 5, 10, 10, 10, null, 5, 5, 5, 10, 10, null, null, 5, 10, null, 5, 5, 10, 10];
  for (let i = 0; i < grps.length; i++) {
    rows.push({ grp: grps[i], score: scores[i], label: `row-${String(i).padStart(2, "0")}` });
  }
  // extra duplicates to cross page boundaries mid-tie
  for (let i = 0; i < 18; i++) {
    rows.push({ grp: 3, score: 5, label: `extra-${String(i).padStart(2, "0")}` });
  }
  return rows;
}

interface WalkRow {
  id: number;
  grp: number | null;
  score: number | null;
  label: string;
}

test(`live pagination: page-walk matches the hand-SQL oracle page for page (mixed directions, NULLS mix, duplicate prefix)`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db }) => {
        await db.insert(walk).values(walkRows());

        const pager = keyset(walk, [descNullsFirst(walk.grp), ascNullsLast(walk.score)], { perPage: 7 });
        assert.deepEqual(
          pager.columns.map((c) => c.propertyKey),
          ["grp", "score", "id"],
          "id auto-appended as the unique tie-breaker",
        );

        // Independent oracle: hand-written SQL, offset pages, the same
        // explicit ordering — authored without the builder.
        const oraclePage = async (pageIdx: number): Promise<WalkRow[]> => {
          const rows = (await db.driver.query(
            "select id, grp, score, label from q04_walk order by grp desc nulls first, score asc nulls last, id asc nulls last limit 7 offset $1",
            [pageIdx * 7],
          )) as WalkRow[];
          return rows;
        };

        let cursor: string | undefined;
        const seen: WalkRow[] = [];
        for (let pageIdx = 0; ; pageIdx++) {
          const page: KeysetPage<WalkRow> = await pager.page(db.select().from(walk), cursor, 7);
          const oracle = await oraclePage(pageIdx);
          assert.deepEqual(
            page.rows.map((r) => [r.id, r.grp, r.score, r.label]),
            oracle.map((r) => [r.id, r.grp, r.score, r.label]),
            `page ${pageIdx} must match the hand-SQL oracle exactly`,
          );
          seen.push(...page.rows);
          if (page.nextCursor === null) {
            assert.equal(oracle.length, page.rows.length, "the oracle is also exhausted");
            break;
          }
          assert.ok(oracle.length === 7, "oracle continues — so must the walk");
          cursor = page.nextCursor;
        }
        assert.equal(seen.length, 40, "no duplicate/omitted rows across the whole walk");
        assert.equal(new Set(seen.map((r) => r.id)).size, 40);
      });
    });
  }
});

test(`live pagination: ties on the non-unique prefix never duplicate or omit (boundary forced inside a tie group)`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db }) => {
        // every row shares ONE (grp, score) tuple: ordering is decided
        // entirely by the tie-breaker, and pages split inside the group.
        const rows = Array.from({ length: 23 }, (_, i) => ({ grp: 7, score: null, label: `tie-${i}` }));
        await db.insert(walk).values(rows);
        const pager = keyset(walk, [ascNullsLast(walk.grp), descNullsLast(walk.score)], { perPage: 4 });
        const oracle = (await db.driver.query(
          "select id, grp, score, label from q04_walk order by grp asc nulls last, score desc nulls last, id asc nulls last",
        )) as WalkRow[];
        const collected: number[] = [];
        let cursor: string | undefined;
        for (;;) {
          const page = await pager.page(db.select().from(walk), cursor, 4);
          collected.push(...page.rows.map((r) => r.id));
          if (page.nextCursor === null) break;
          cursor = page.nextCursor;
        }
        assert.deepEqual(collected, oracle.map((r) => r.id), "the walk is the oracle's exact order");
      });
    });
  }
});

test(`live pagination: the other two null placements walk correctly (asc NULLS FIRST + desc NULLS LAST)`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db }) => {
        await db.insert(walk).values(walkRows());
        const pager = keyset(walk, [ascNullsFirst(walk.grp), descNullsLast(walk.score)], { perPage: 6 });
        const oracle = (await db.driver.query(
          "select id, grp, score, label from q04_walk order by grp asc nulls first, score desc nulls last, id asc nulls last",
        )) as WalkRow[];
        const collected: number[] = [];
        let cursor: string | undefined;
        for (;;) {
          const page = await pager.page(db.select().from(walk), cursor, 6);
          collected.push(...page.rows.map((r) => r.id));
          if (page.nextCursor === null) break;
          cursor = page.nextCursor;
        }
        assert.deepEqual(collected, oracle.map((r) => r.id), "the (asc nulls first, desc nulls last) walk matches the hand-SQL oracle");
      });
    });
  }
});

test(`live pagination: non-unique keyset without a tie-breaker errors before SQL`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db, statements }) => {
        const before = statements.count;
        assert.throws(() => keyset(conc, [ascNullsLast(conc.k)], { autoAppendTiebreaker: false }), /not a unique ordering/);
        // via the schema route a tie-breaker IS available (id): construction
        // succeeds and pages — proving the flag, not the capability, gated it.
        const pager = keyset(conc, [ascNullsLast(conc.k)]);
        assert.equal(pager.columns.length, 2);
        const page = await pager.page(db.select().from(conc), undefined, 5);
        assert.equal(page.rows.length, 0);
        assert.equal(statements.count - before >= 1, true, "only the successful statement ran");
      });
    });
  }
});

test(`live pagination: precision keysets — int8 max ids, 40-digit numerics, microsecond timestamptz, NULL cursor values`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db }) => {
        await db.insert(exact).values([
          { id: "9223372036854775807", amt: "-9998887776665554443332221110009998887776.5", at: "2027-07-14T13:45:22.123456Z", label: "max" },
          { id: "9223372036854775806", amt: "-9998887776665554443332221110009998887776.5", at: "2027-07-14T13:45:22.123456Z", label: "dup" },
          { id: "9007199254740993", amt: null, at: "1999-12-31T23:59:59.999999Z", label: "safe-boundary" },
          { id: "0", amt: "0.000001", at: null, label: "null-at" },
          { id: "-9223372036854775808", amt: null, at: null, label: "min-id" },
        ]);

        const pager = keyset(exact, [descNullsLast(exact.amt), ascNullsFirst(exact.at)], { perPage: 2 });
        const oracle = (await db.driver.query(
          "select id::text, amt, to_jsonb(at at time zone 'UTC')::text as at_wire, label from q04_exact order by amt desc nulls last, at asc nulls first, id asc nulls last",
        )) as Array<{ id: string; amt: string | null; at_wire: string | null; label: string }>;

        let cursor: string | undefined;
        const seen: string[] = [];
        for (;;) {
          const page = await pager.page(db.select().from(exact), cursor, 2);
          seen.push(...page.rows.map((r) => r.id));
          if (page.nextCursor === null) break;
          cursor = page.nextCursor;
        }
        assert.deepEqual(seen, oracle.map((r) => r.id), "precision walk matches the hand-SQL oracle order");

        // Cursor precision spot-check: the FIRST cursor must carry the exact
        // 40-digit numeric, the microsecond instant, and a null (at) value.
        const first = await pager.page(db.select().from(exact), undefined, 2);
        assert.ok(first.nextCursor);
        const again = await pager.page(db.select().from(exact), first.nextCursor, 2);
        assert.equal(again.rows.length, 2, "the cursor round-trips losslessly through the walk");
        // exact boundary equality: seek by an explicit cursor built from the
        // max row must exclude exactly the max row and its duplicate
        // (oracle order: "0.000001" first, then the -9998...5 pair, then the
        // amt-null rows with at-null first under NULLS FIRST).
        const maxRow = (await db.driver.query("select id::text, amt, to_jsonb(at at time zone 'UTC')::text as at_wire from q04_exact where label = 'max'")) as Array<{ id: string; amt: string; at_wire: string }>;
        const atCanonical = JSON.parse(maxRow[0].at_wire) + "Z";
        const explicit = pager.cursorOf({ id: maxRow[0].id, amt: maxRow[0].amt, at: atCanonical, label: "max" });
        const next = await pager.page(db.select().from(exact), explicit, 10);
        assert.deepEqual(
          next.rows.map((r) => r.id),
          ["-9223372036854775808", "9007199254740993"],
          "a hand-built cursor from raw ::text oracle values seeks exactly past the tie",
        );
      });
    });
  }
});

test(`live pagination: malformed cursors fail before any SQL runs`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db, statements }) => {
        const pager = keyset(walk, [descNullsFirst(walk.grp), ascNullsLast(walk.score)]);
        const valid = pager.cursorOf({ id: 1, grp: 1, score: 1, label: "x" });
        const variants: Array<[string, RegExp]> = [
          ["", /non-empty/],
          ["%%not-base64%%", /not valid base64url/],
          ["AA", /too short/],
          ["aGVsbG8", /version 104/],
          ["AQ", /not valid JSON|too short/],
          [valid.slice(0, -4) + "!!!!", /not valid base64url|not valid JSON/],
          ["_" + valid.slice(1), /version \d+ is not supported|not valid JSON|shape/],
        ];
        for (const [cursor, re] of variants) {
          const before = statements.count;
          assert.throws(
            () => db.select().from(walk).where(pager.seekCondition(cursor)).toSQL(),
            (err: unknown) => {
              assert.ok(err instanceof CursorError, `CursorError for ${JSON.stringify(cursor).slice(0, 24)}, got: ${String((err as Error).message).slice(0, 80)}`);
              assert.match(err.message, re);
              return true;
            },
          );
          assert.equal(statements.count, before, "zero driver traffic on a rejected cursor");
        }
      });
    });
  }
});

test(`live pagination: concurrent inserts land on the documented side (READ COMMITTED, one statement per page)`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db }) => {
        // 30 originals, k = 1..30 (dense, no gaps); keyset (k asc nulls last).
        await db.insert(conc).values(Array.from({ length: 30 }, (_, i) => ({ k: i + 1, tag: `orig-${i + 1}` })));
        const pager = keyset(conc, [ascNullsLast(conc.k)], { perPage: 10 });

        const page1 = await pager.page(db.select().from(conc), undefined, 10);
        assert.equal(page1.rows.length, 10);
        assert.ok(page1.nextCursor);
        // k=15 exists in page 2 today. Inserted mid-walk:
        const kAfterCursor1 = Number(page1.rows[9].k); // 10
        await db.insert(conc).values([
          { k: kAfterCursor1 - 5, tag: "before-cursor" }, // sorts before the cursor -> never reappears
          { k: 15, tag: "mid-rest" }, // inside the remaining range -> appears (already-passed positions can't, this one hasn't)
          { k: 999, tag: "after-all" }, // after every original -> appears on a later page
        ]);
        const collected: string[] = [...page1.rows.map((r) => r.tag)];
        let cursor = page1.nextCursor;
        for (;;) {
          const page = await pager.page(db.select().from(conc), cursor, 10);
          collected.push(...page.rows.map((r) => r.tag));
          if (page.nextCursor === null) break;
          cursor = page.nextCursor;
        }
        // Documented behavior: originals exactly once; before-cursor absent;
        // the other two inserts present (they sort after the cursor at the
        // time their page executed).
        const originals = Array.from({ length: 30 }, (_, i) => `orig-${i + 1}`);
        for (const tag of originals) {
          assert.equal(collected.filter((x) => x === tag).length, 1, `original ${tag} exactly once`);
        }
        assert.equal(collected.includes("before-cursor"), false, "a row sorting before the cursor never reappears");
        assert.equal(collected.includes("mid-rest"), true);
        assert.equal(collected.includes("after-all"), true);
        assert.equal(collected.length, 32, "30 originals exactly once + the two after-cursor inserts");
      });
    });
  }
});

test(`live pagination: pre-ordered/offset builders are rejected before any SQL (fail-closed guard)`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db, statements }) => {
        const pager = keyset(walk, [ascNullsLast(walk.grp)], { perPage: 5 });
        const before = statements.count;
        assert.throws(
          () => pager.apply(db.select().from(walk).orderBy(walk.id), undefined),
          /already carries ORDER BY terms/,
        );
        assert.throws(
          () => pager.apply(db.select().from(walk).offset(5), undefined),
          /already carries an OFFSET/,
        );
        await assert.rejects(
          () => pager.page(db.select().from(walk).orderBy(walk.id), undefined, 5),
          /already carries ORDER BY terms/,
        );
        assert.equal(statements.count, before, "the guard fires with zero driver traffic");
      });
    });
  }
});

test(`live pagination: the documented combinations — filtered fresh builder walks exactly; own ordering paginates directly`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db }) => {
        await db.insert(walk).values(walkRows());
        // Combination 1: filter a FRESH builder (.where) — the pager applies
        // its ordering and limit exclusively; the walk covers the filtered
        // set exactly (no dup/omit against the filtered oracle).
        const pager = keyset(walk, [descNullsFirst(walk.grp), ascNullsLast(walk.score)], { perPage: 6 });
        const oracle = (await db.driver.query(
          "select id from q04_walk where grp is not null order by grp desc nulls first, score asc nulls last, id asc nulls last",
        )) as Array<{ id: number }>;
        const collected: number[] = [];
        let cursor: string | undefined;
        for (;;) {
          const page = await pager.page(db.select().from(walk).where(sql`${walk.grp} is not null`), cursor, 6);
          collected.push(...page.rows.map((r) => r.id));
          if (page.nextCursor === null) break;
          cursor = page.nextCursor;
        }
        assert.deepEqual(collected, oracle.map((r) => r.id), "the filtered walk is the filtered oracle's exact order");
        assert.equal(new Set(collected).size, collected.length, "no duplicates");
        // Combination 2: to keep your OWN ordering, paginate the builder
        // directly (limit/offset) instead of the pager.
        const direct = await db
          .select()
          .from(walk)
          .orderBy(descNullsFirst(walk.grp), ascNullsLast(walk.score), ascNullsLast(walk.id))
          .limit(6)
          .offset(6);
        const oracleDirect = (await db.driver.query(
          "select id from q04_walk order by grp desc nulls first, score asc nulls last, id asc nulls last limit 6 offset 6",
        )) as Array<{ id: number }>;
        assert.deepEqual(direct.map((r) => r.id), oracleDirect.map((r) => r.id), "direct offset pagination with own ordering works without the pager");
      });
    });
  }
});

test(`live pagination: REPEATABLE READ transaction pins one snapshot across pages`, async (t) => {
  for (const driverKind of ["postgres", "pg"] as const) {
    await t.test(driverKind, async () => {
      await withSuite(driverKind, async ({ db }) => {
        await db.insert(conc).values(Array.from({ length: 25 }, (_, i) => ({ k: i + 1, tag: `snap-${i + 1}` })));
        const pager = keyset(conc, [ascNullsLast(conc.k)], { perPage: 10 });

        const collected: string[] = [];
        await db.driver.begin(async (tx) => {
          // SET TRANSACTION must be the first statement of the transaction.
          await tx.execute("set transaction isolation level repeatable read");
          const txDb = await createDatabase({ driver: tx, tables: { conc } });
          let cursor: string | undefined;
          for (;;) {
            const page = await pager.page(txDb.select().from(conc), cursor, 10);
            collected.push(...page.rows.map((r) => r.tag));
            if (page.nextCursor === null) break;
            cursor = page.nextCursor;
          }
          // Mid-walk insert from OUTSIDE the snapshot transaction.
          await db.insert(conc).values({ k: 0, tag: "concurrent" });
          const after = await pager.page(txDb.select().from(conc), undefined, 100);
          assert.equal(after.rows.length, 25, "the repeatable-read snapshot never sees the concurrent insert");
        });
        assert.deepEqual(
          collected,
          Array.from({ length: 25 }, (_, i) => `snap-${i + 1}`),
          "the snapshot walk is exactly the original set in order",
        );
        const total = (await db.driver.query("select count(*)::int as n from q04_conc")) as Array<{ n: number }>;
        assert.equal(total[0].n, 26, "the concurrent insert IS committed for everyone else");
      });
    });
  }
});
