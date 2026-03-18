import { json } from "@remix-run/node";
import { useLoaderData } from "@remix-run/react";

export async function loader({ request }) {
  const authorized = request.headers.get("authorization") === "Bearer valid-token";
  return json({ authorized }, { status: authorized ? 200 : 401 });
}

export default function ProtectedRoute() {
  const data = useLoaderData();
  return (
    <main>
      <h1>bench-protected</h1>
      <p>{data.authorized ? "authorized" : "unauthorized"}</p>
    </main>
  );
}
