import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

export const todos = sqliteTable("neutron_todos", {
  id: text("id").primaryKey(),
  text: text("text").notNull(),
  done: integer("done").notNull().default(0),
  createdAt: integer("created_at").notNull(),
});
