import type { LoaderArgs, ActionArgs } from "neutron";
import { getDataRuntime } from "../data/runtime.js";

export const config = { mode: "app" };

export async function loader({ params }: LoaderArgs) {
  const runtime = await getDataRuntime();
  const todos = await runtime.todos.list();

  return {
    todos,
    dataProfile: runtime.profile,
    dbProvider: runtime.database?.profile.provider || "none",
    queueDriver: runtime.drivers.queue,
  };
}

export async function action({ request }: ActionArgs) {
  const runtime = await getDataRuntime();
  const formData = await request.formData();
  const intent = formData.get("_intent") as string;
  let changedTodoId: string | null = null;

  if (intent === "add") {
    const text = formData.get("text") as string;
    if (text?.trim()) {
      const added = await runtime.todos.add(text.trim());
      changedTodoId = added.id;
    }
  } else if (intent === "toggle") {
    const id = String(formData.get("id") || "");
    const updated = await runtime.todos.toggle(id);
    if (updated) {
      changedTodoId = updated.id;
    }
  } else if (intent === "delete") {
    const id = String(formData.get("id") || "");
    const removed = await runtime.todos.remove(id);
    if (removed) {
      changedTodoId = id;
    }
  }

  let queued = false;
  if (changedTodoId) {
    try {
      await runtime.queue.add("todo.changed", {
        id: changedTodoId,
        intent,
        at: Date.now(),
      });
      queued = true;
    } catch {
      queued = false;
    }
  }

  return { success: true, queued };
}

interface LoaderData {
  todos: Array<{ id: string; text: string; done: boolean }>;
  dataProfile: string;
  dbProvider: string;
  queueDriver: string;
}

export default function Todos({
  data,
  actionData,
}: {
  data: LoaderData;
  actionData?: { success: boolean; queued?: boolean };
}) {
  return (
    <div>
      <h1>Todos</h1>

      {actionData?.success && (
        <p style={{ color: "#00E5A0" }}>
          Action completed{actionData.queued ? " and queued for worker." : "."}
        </p>
      )}

      <p style={{ color: "#888" }}>
        Data profile: <code>{data?.dataProfile}</code> | DB: <code>{data?.dbProvider}</code> | Queue:{" "}
        <code>{data?.queueDriver}</code>
      </p>

      <form method="post" style={{ marginBottom: "1rem" }}>
        <input type="hidden" name="_intent" value="add" />
        <input
          type="text"
          name="text"
          placeholder="Add a todo..."
          required
          style={{ padding: "0.5rem", marginRight: "0.5rem" }}
        />
        <button type="submit">Add</button>
      </form>

      <ul style={{ listStyle: "none", padding: 0 }}>
        {data?.todos.map((todo) => (
          <li
            key={todo.id}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "0.5rem",
              padding: "0.5rem 0",
              borderBottom: "1px solid #333",
            }}
          >
            <form method="post" style={{ margin: 0 }}>
              <input type="hidden" name="_intent" value="toggle" />
              <input type="hidden" name="id" value={todo.id} />
              <input
                type="checkbox"
                checked={todo.done}
                onChange={(e) => (e.target as HTMLInputElement).form?.submit()}
              />
            </form>
            <span
              style={{
                flex: 1,
              textDecoration: todo.done ? "line-through" : "none",
                color: todo.done ? "#888" : "inherit",
              }}
            >
              {todo.text}
            </span>
            <form method="post" style={{ margin: 0 }}>
              <input type="hidden" name="_intent" value="delete" />
              <input type="hidden" name="id" value={todo.id} />
              <button type="submit" style={{ color: "#FF4444" }}>Delete</button>
            </form>
          </li>
        ))}
      </ul>

      <p style={{ marginTop: "1rem", color: "#888" }}>
        {data?.todos.filter((t) => t.done).length} of {data?.todos.length} completed
      </p>
    </div>
  );
}
