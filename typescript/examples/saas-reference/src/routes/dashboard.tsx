export const config = { mode: "app", cache: { loaderMaxAge: 20 } };

export async function loader() {
  return {
    activeUsers: 128,
    openTickets: 7,
    loadedAt: new Date().toISOString(),
  };
}

export default function Dashboard({
  data,
}: {
  data: {
    activeUsers: number;
    openTickets: number;
    loadedAt: string;
  };
}) {
  return (
    <section>
      <h1>Dashboard</h1>
      <p>Active users: {data.activeUsers}</p>
      <p>Open tickets: {data.openTickets}</p>
      <p style={{ color: "#666" }}>Loaded at: {data.loadedAt}</p>
    </section>
  );
}
