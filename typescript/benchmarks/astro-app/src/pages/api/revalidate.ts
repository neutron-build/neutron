import type { APIRoute } from "astro";
import { revalidateBenchCache } from "../../lib/bench-cache";

export const GET: APIRoute = async () => {
  return new Response(JSON.stringify({ ok: false, error: "Method Not Allowed" }), {
    status: 405,
    headers: {
      "Content-Type": "application/json",
      Allow: "POST",
    },
  });
};

export const POST: APIRoute = async () => {
  const version = revalidateBenchCache();
  return new Response(JSON.stringify({ ok: true, version }), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
};
