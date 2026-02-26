export const config = { mode: "app" };

export async function loader() {
  throw new Response(JSON.stringify({ ok: false, error: "Method Not Allowed" }), {
    status: 405,
    headers: {
      "Content-Type": "application/json",
      Allow: "POST",
    },
  });
}

export async function action({ request }: { request: Request }) {
  const authorized = request.headers.get("authorization") === "Bearer valid-token";
  if (!authorized) {
    return new Response(JSON.stringify({ ok: false, error: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  return new Response(
    JSON.stringify({
      ok: true,
      refreshed: true,
      token: "valid-token",
    }),
    {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }
  );
}

export default function ApiSessionRefreshRoute() {
  return <main>POST /api/session/refresh</main>;
}
