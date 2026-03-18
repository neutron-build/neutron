import { Form } from "neutron/client";
import { addTicket, listTickets, toggleTicket } from "../lib/store.js";

export const config = { mode: "app" };

export async function loader() {
  return {
    tickets: listTickets(),
  };
}

export async function action({ request }: { request: Request }) {
  const form = await request.formData();
  const intent = String(form.get("_intent") || "");

  if (intent === "create") {
    const title = String(form.get("title") || "").trim();
    if (title) {
      addTicket(title);
    }
  }

  if (intent === "toggle") {
    const id = String(form.get("id") || "");
    if (id) {
      toggleTicket(id);
    }
  }

  return { ok: true };
}

export default function Tickets({
  data,
}: {
  data: {
    tickets: Array<{ id: string; title: string; status: "open" | "closed" }>;
  };
}) {
  return (
    <section>
      <h1>Tickets</h1>
      <Form method="post" style={{ marginBottom: "1rem" }}>
        <input type="hidden" name="_intent" value="create" />
        <input name="title" placeholder="New ticket title" />
        <button type="submit">Create</button>
      </Form>

      <ul style={{ listStyle: "none", padding: 0 }}>
        {data.tickets.map((ticket) => (
          <li key={ticket.id} style={{ marginBottom: "0.5rem" }}>
            <Form method="post" style={{ display: "inline-flex", gap: "0.5rem" }}>
              <input type="hidden" name="_intent" value="toggle" />
              <input type="hidden" name="id" value={ticket.id} />
              <button type="submit">
                {ticket.status === "open" ? "Close" : "Reopen"}
              </button>
              <span>
                {ticket.title} ({ticket.status})
              </span>
            </Form>
          </li>
        ))}
      </ul>
    </section>
  );
}
