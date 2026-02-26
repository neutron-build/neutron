import { integer, pgTable, text } from "drizzle-orm/pg-core";

export const todos = pgTable("neutron_todos", {
  id: text("id").primaryKey(),
  text: text("text").notNull(),
  done: integer("done").notNull().default(0),
  createdAt: integer("created_at").notNull(),
});
