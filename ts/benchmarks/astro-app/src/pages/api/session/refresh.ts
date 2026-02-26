import type { APIRoute } from "astro";

export const GET: APIRoute = async () => {
  return new Response(JSON.stringify({ ok: false, error: "Method Not Allowed" }), {
    status: 405,
    headers: {
      "Content-Type": "application/json",
      Allow: "POST",
    },
  });
};

export const POST: APIRoute = async ({ request }) => {
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
};
