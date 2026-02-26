import { createDrizzleDatabase } from "neutron-data";

const seedTodos = [
  { id: "seed-1", text: "Seeded todo: validate migrations", done: 1, createdAt: Date.now() - 3000 },
  { id: "seed-2", text: "Seeded todo: run neutron worker", done: 0, createdAt: Date.now() - 2000 },
  { id: "seed-3", text: "Seeded todo: benchmark after feature work", done: 0, createdAt: Date.now() - 1000 },
];

async function main() {
  const database = await createDrizzleDatabase();
  const query = createQueryAdapter(database);

  await query.run(
    `CREATE TABLE IF NOT EXISTS neutron_todos (
      id TEXT PRIMARY KEY,
      text TEXT NOT NULL,
      done INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL
    )`
  );

  for (const todo of seedTodos) {
    await query.run(
      `INSERT INTO neutron_todos (id, text, done, created_at)
       VALUES (?, ?, ?, ?)
       ON CONFLICT (id) DO UPDATE SET
         text = EXCLUDED.text,
         done = EXCLUDED.done,
         created_at = EXCLUDED.created_at`,
      [todo.id, todo.text, todo.done, todo.createdAt]
    );
  }

  await database.close();
  console.log(`Seed complete (${seedTodos.length} rows).`);
}

function createQueryAdapter(database) {
  if (database.profile.provider === "postgres") {
    const client = database.client;
    if (typeof client?.unsafe !== "function") {
      throw new Error("Postgres client missing unsafe(sql, params) method.");
    }

    return {
      run: async (sql, params = []) => {
        await client.unsafe(toPostgresSql(sql), params);
      },
    };
  }

  const client = database.client;
  if (typeof client?.execute !== "function") {
    throw new Error("SQLite client missing execute(...) method.");
  }

  return {
    run: async (sql, params = []) => {
      await client.execute({ sql, args: params });
    },
  };
}

function toPostgresSql(sql) {
  let index = 0;
  return sql.replace(/\?/g, () => {
    index += 1;
    return `$${index}`;
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
