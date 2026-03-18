export interface Ticket {
  id: string;
  title: string;
  status: "open" | "closed";
}

const tickets: Ticket[] = [
  { id: "t1", title: "Provision team workspace", status: "open" },
  { id: "t2", title: "Configure billing profile", status: "closed" },
];

export function listTickets(): Ticket[] {
  return [...tickets];
}

export function addTicket(title: string): Ticket {
  const ticket: Ticket = {
    id: `t${Date.now()}`,
    title,
    status: "open",
  };
  tickets.push(ticket);
  return ticket;
}

export function toggleTicket(id: string): Ticket | null {
  const target = tickets.find((ticket) => ticket.id === id);
  if (!target) {
    return null;
  }
  target.status = target.status === "open" ? "closed" : "open";
  return { ...target };
}
