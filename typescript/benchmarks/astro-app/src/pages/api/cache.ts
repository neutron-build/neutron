import type { APIRoute } from "astro";
import { readBenchCache } from "../../lib/bench-cache";

export const GET: APIRoute = async () => {
  const result = readBenchCache();
  return new Response(JSON.stringify(result.payload), {
    status: 200,
    headers: {
      "Content-Type": "application/json",
      "x-bench-cache": result.hit ? "HIT" : "MISS",
    },
  });
};

export const POST: APIRoute = async () => {
  return new Response(JSON.stringify({ ok: false, error: "Method Not Allowed" }), {
    status: 405,
    headers: {
      "Content-Type": "application/json",
      Allow: "GET",
    },
  });
};
