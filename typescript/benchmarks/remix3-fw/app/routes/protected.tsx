import { useLoaderData } from "react-router";

export async function loader({ request }: { request: Request }) {
  const authorized = request.headers.get("authorization") === "Bearer valid-token";
  return new Response(JSON.stringify({ authorized }), {
    status: authorized ? 200 : 401,
    headers: { "Content-Type": "application/json" },
  });
}

export default function ProtectedRoute() {
  const data = useLoaderData() as { authorized: boolean };
  return (
    <main>
      <h1>bench-protected</h1>
      <p>{data.authorized ? "authorized" : "unauthorized"}</p>
    </main>
  );
}
