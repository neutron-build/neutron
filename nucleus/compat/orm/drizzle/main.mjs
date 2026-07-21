// Drizzle-on-Nucleus canonical flow: migrate (drizzle-kit push) → CRUD →
// transaction commit + rollback → prepared statement → join.
// Exits non-zero on the first failed step; prints one line per step.
import { execFileSync } from "node:child_process";
import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";
import { eq, and, gt, sql as dsql } from "drizzle-orm";
import { users, posts, tags, postTags } from "./schema.mjs";

const url = process.env.DATABASE_URL;
if (!url) {
  console.error("DATABASE_URL not set");
  process.exit(2);
}

function step(name) {
  console.log(`[drizzle] ${name}`);
}

function assert(cond, msg) {
  if (!cond) {
    console.error(`[drizzle] ASSERT FAILED: ${msg}`);
    process.exit(1);
  }
}

// 1. Migration — drizzle-kit push generates + applies CREATE TABLE/index DDL.
step("migrate (drizzle-kit push)");
execFileSync(
  "npx",
  [
    "drizzle-kit",
    "push",
    "--dialect=postgresql",
    "--schema=./schema.mjs",
    `--url=${url}`,
    "--force",
  ],
  { stdio: "inherit" }
);

const client = postgres(url, { max: 1, onnotice: () => {} });
const db = drizzle(client);

try {
  // 2. CRUD
  step("insert returning");
  const inserted = await db
    .insert(users)
    .values([
      { id: 1, email: "ada@example.com", name: "Ada", age: 36 },
      { id: 2, email: "grace@example.com", name: "Grace", age: 45 },
    ])
    .returning({ id: users.id });
  assert(inserted.length === 2, `insert returned ${inserted.length} rows`);

  step("select where");
  const found = await db.select().from(users).where(eq(users.email, "ada@example.com"));
  assert(found.length === 1 && found[0].name === "Ada", "select-by-email");

  step("update");
  await db.update(users).set({ age: 37 }).where(eq(users.id, 1));
  const aged = await db.select({ age: users.age }).from(users).where(eq(users.id, 1));
  assert(aged[0].age === 37, `update visible (got ${aged[0]?.age})`);

  step("delete");
  await db.insert(users).values({ id: 3, email: "tmp@example.com", name: "Tmp", age: 1 });
  await db.delete(users).where(eq(users.id, 3));
  const gone = await db.select().from(users).where(eq(users.id, 3));
  assert(gone.length === 0, "delete visible");

  // 3. Transactions
  step("transaction commit");
  await db.transaction(async (tx) => {
    await tx.insert(posts).values({ id: 1, userId: 1, title: "Hello", content: "world" });
    await tx.insert(posts).values({ id: 2, userId: 2, title: "Compat", content: "notes", published: true });
  });
  const postCount = await db.select({ n: dsql`count(*)` }).from(posts);
  assert(Number(postCount[0].n) === 2, `committed txn rows (got ${postCount[0].n})`);

  step("transaction rollback");
  try {
    await db.transaction(async (tx) => {
      await tx.insert(posts).values({ id: 99, userId: 1, title: "doomed", content: "x" });
      throw new Error("force rollback");
    });
  } catch (e) {
    if (!/force rollback/.test(String(e))) throw e;
  }
  const doomed = await db.select().from(posts).where(eq(posts.id, 99));
  assert(doomed.length === 0, "rolled-back row absent");

  // 4. Prepared statement (extended protocol under the hood)
  step("prepared statement");
  const prepared = db
    .select()
    .from(users)
    .where(and(gt(users.age, dsql.placeholder("minAge"))))
    .prepare("users_older_than");
  const older = await prepared.execute({ minAge: 40 });
  assert(older.length === 1 && older[0].name === "Grace", "prepared exec");

  // 5. Join across FK
  step("join");
  await db.insert(tags).values({ id: 1, name: "db" });
  await db.insert(postTags).values({ postId: 2, tagId: 1 });
  const joined = await db
    .select({ title: posts.title, tag: tags.name, author: users.name })
    .from(postTags)
    .innerJoin(posts, eq(postTags.postId, posts.id))
    .innerJoin(tags, eq(postTags.tagId, tags.id))
    .innerJoin(users, eq(posts.userId, users.id));
  assert(
    joined.length === 1 && joined[0].tag === "db" && joined[0].author === "Grace",
    "three-way join"
  );

  console.log("[drizzle] ALL STEPS PASSED");
} finally {
  await client.end({ timeout: 2 });
}
