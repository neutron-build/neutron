import type { DrizzleDatabase } from "neutron-data";

export interface Todo {
  id: string;
  text: string;
  done: boolean;
  createdAt: number;
}

export interface TodoStore {
  list(): Promise<Todo[]>;
  add(text: string): Promise<Todo>;
  toggle(id: string): Promise<Todo | null>;
  remove(id: string): Promise<boolean>;
}

export function createMemoryTodoStore(initial: Todo[] = []): TodoStore {
  const items = [...initial];

  return {
    async list(): Promise<Todo[]> {
      return [...items].sort((a, b) => a.createdAt - b.createdAt);
    },
    async add(text: string): Promise<Todo> {
      const todo: Todo = {
        id: createId(),
        text,
        done: false,
        createdAt: Date.now(),
      };
      items.push(todo);
      return todo;
    },
    async toggle(id: string): Promise<Todo | null> {
      const todo = items.find((item) => item.id === id);
      if (!todo) {
        return null;
      }
      todo.done = !todo.done;
      return { ...todo };
    },
    async remove(id: string): Promise<boolean> {
      const index = items.findIndex((item) => item.id === id);
      if (index < 0) {
        return false;
      }
      items.splice(index, 1);
      return true;
    },
  };
}

export async function createSqlTodoStore(database: DrizzleDatabase): Promise<TodoStore> {
  const query = buildQueryAdapter(database);

  await query.run(
    `CREATE TABLE IF NOT EXISTS neutron_todos (
      id TEXT PRIMARY KEY,
      text TEXT NOT NULL,
      done INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL
    )`
  );

  return {
    async list(): Promise<Todo[]> {
      const rows = await query.all<{
        id: unknown;
        text: unknown;
        done: unknown;
        created_at: unknown;
      }>("SELECT id, text, done, created_at FROM neutron_todos ORDER BY created_at ASC");

      return rows.map((row) => ({
        id: String(row.id),
        text: String(row.text),
        done: toBoolean(row.done),
        createdAt: toNumber(row.created_at, Date.now()),
      }));
    },
    async add(text: string): Promise<Todo> {
      const todo: Todo = {
        id: createId(),
        text,
        done: false,
        createdAt: Date.now(),
      };

      await query.run(
        "INSERT INTO neutron_todos (id, text, done, created_at) VALUES (?, ?, ?, ?)",
        [todo.id, todo.text, 0, todo.createdAt]
      );

      return todo;
    },
    async toggle(id: string): Promise<Todo | null> {
      const existing = await query.first<{
        id: unknown;
        text: unknown;
        done: unknown;
        created_at: unknown;
      }>("SELECT id, text, done, created_at FROM neutron_todos WHERE id = ?", [id]);

      if (!existing) {
        return null;
      }

      const nextDone = toBoolean(existing.done) ? 0 : 1;
      await query.run("UPDATE neutron_todos SET done = ? WHERE id = ?", [nextDone, id]);

      return {
        id: String(existing.id),
        text: String(existing.text),
        done: Boolean(nextDone),
        createdAt: toNumber(existing.created_at, Date.now()),
      };
    },
    async remove(id: string): Promise<boolean> {
      const before = await query.first<{ count: unknown }>(
        "SELECT COUNT(*) AS count FROM neutron_todos WHERE id = ?",
        [id]
      );
      if (!before || toNumber(before.count, 0) < 1) {
        return false;
      }

      await query.run("DELETE FROM neutron_todos WHERE id = ?", [id]);
      return true;
    },
  };
}

interface QueryAdapter {
  run(sql: string, params?: unknown[]): Promise<void>;
  all<T extends Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]>;
  first<T extends Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T | null>;
}

function buildQueryAdapter(database: DrizzleDatabase): QueryAdapter {
  if (database.profile.provider === "postgres") {
    const client = database.client as {
      unsafe?: (sql: string, params?: unknown[]) => Promise<unknown[]>;
    };
    if (typeof client.unsafe !== "function") {
      throw new Error("Postgres client missing unsafe(sql, params) method.");
    }

    return {
      run: async (sql, params = []) => {
        await client.unsafe!(toPostgresSql(sql), params);
      },
      all: async <T extends Record<string, unknown>>(sql: string, params: unknown[] = []) => {
        const rows = await client.unsafe!(toPostgresSql(sql), params);
        return rows as T[];
      },
      first: async <T extends Record<string, unknown>>(sql: string, params: unknown[] = []) => {
        const rows = await client.unsafe!(toPostgresSql(sql), params);
        return (rows[0] as T | undefined) ?? null;
      },
    };
  }

  const client = database.client as {
    execute?: (input: string | { sql: string; args?: unknown[] }) => Promise<{ rows?: unknown[] }>;
  };

  if (typeof client.execute !== "function") {
    throw new Error("SQLite client missing execute(...) method.");
  }

  return {
    run: async (sql, params = []) => {
      await client.execute!({ sql, args: params });
    },
    all: async <T extends Record<string, unknown>>(sql: string, params: unknown[] = []) => {
      const result = await client.execute!({ sql, args: params });
      return ((result.rows || []) as T[]).map((row) => normalizeRow(row));
    },
    first: async <T extends Record<string, unknown>>(sql: string, params: unknown[] = []) => {
      const result = await client.execute!({ sql, args: params });
      const row = (result.rows || [])[0] as T | undefined;
      return row ? normalizeRow(row) : null;
    },
  };
}

function normalizeRow<T extends Record<string, unknown>>(row: T): T {
  return row;
}

function toPostgresSql(sql: string): string {
  let index = 0;
  return sql.replace(/\?/g, () => {
    index += 1;
    return `$${index}`;
  });
}

function toBoolean(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    return value === "1" || value.toLowerCase() === "true";
  }
  return false;
}

function toNumber(value: unknown, fallback: number): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return fallback;
}

function createId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}
