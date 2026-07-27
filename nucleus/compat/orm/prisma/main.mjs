// Prisma-on-Nucleus canonical flow: client connect → CRUD → nested write →
// interactive transaction commit + rollback → relation include → aggregate.
// `prisma db push` (schema DDL) runs before this script, from run.sh.
import { PrismaClient } from "./generated/index.js";

const prisma = new PrismaClient();

function step(name) {
  console.log(`[prisma] ${name}`);
}

function assert(cond, msg) {
  if (!cond) {
    console.error(`[prisma] ASSERT FAILED: ${msg}`);
    process.exit(1);
  }
}

try {
  step("create");
  await prisma.user.create({
    data: { id: 1, email: "ada@example.com", name: "Ada", age: 36 },
  });
  await prisma.user.create({
    data: { id: 2, email: "grace@example.com", name: "Grace", age: 45 },
  });

  step("findUnique");
  const ada = await prisma.user.findUnique({ where: { email: "ada@example.com" } });
  assert(ada && ada.name === "Ada", "findUnique by unique email");

  step("update");
  await prisma.user.update({ where: { id: 1 }, data: { age: 37 } });
  const aged = await prisma.user.findUnique({ where: { id: 1 } });
  assert(aged.age === 37, `update visible (got ${aged?.age})`);

  step("nested write (create with relation)");
  await prisma.user.create({
    data: {
      id: 3,
      email: "linus@example.com",
      name: "Linus",
      age: 55,
      posts: { create: [{ id: 1, title: "Hello", content: "world" }] },
    },
  });
  const withPosts = await prisma.user.findUnique({
    where: { id: 3 },
    include: { posts: true },
  });
  assert(withPosts.posts.length === 1, "nested create + include");

  step("interactive transaction commit");
  await prisma.$transaction(async (tx) => {
    await tx.post.create({ data: { id: 2, userId: 1, title: "Txn", content: "committed" } });
    await tx.post.create({ data: { id: 3, userId: 2, title: "Txn2", content: "committed", published: true } });
  });
  const postCount = await prisma.post.count();
  assert(postCount === 3, `committed txn rows (got ${postCount})`);

  step("interactive transaction rollback");
  try {
    await prisma.$transaction(async (tx) => {
      await tx.post.create({ data: { id: 99, userId: 1, title: "doomed", content: "x" } });
      throw new Error("force rollback");
    });
  } catch (e) {
    if (!/force rollback/.test(String(e))) throw e;
  }
  const doomed = await prisma.post.findUnique({ where: { id: 99 } });
  assert(doomed === null, "rolled-back row absent");

  step("filtered findMany + orderBy");
  const olderThan40 = await prisma.user.findMany({
    where: { age: { gt: 40 } },
    orderBy: { age: "desc" },
  });
  assert(
    olderThan40.length === 2 && olderThan40[0].name === "Linus",
    "findMany filter+order"
  );

  step("aggregate");
  const agg = await prisma.user.aggregate({ _avg: { age: true }, _count: true });
  assert(agg._count === 3, `aggregate count (got ${agg._count})`);

  step("delete");
  await prisma.post.deleteMany({ where: { userId: 3 } });
  await prisma.user.delete({ where: { id: 3 } });
  const gone = await prisma.user.findUnique({ where: { id: 3 } });
  assert(gone === null, "delete visible");

  console.log("[prisma] ALL STEPS PASSED");
} finally {
  await prisma.$disconnect();
}
