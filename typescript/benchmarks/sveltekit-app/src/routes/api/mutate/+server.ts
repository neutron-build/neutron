import { json } from "@sveltejs/kit";
import { lcgWork } from "$lib/server/data";
import type { RequestHandler } from "./$types";

export const POST: RequestHandler = async ({ request }) => {
  const body = await request.json();
  const seed = body.seed ?? 42;
  const repeat = body.repeat ?? 140000;
  const value = lcgWork(seed, repeat);
  return json({ ok: true, value });
};

export const GET: RequestHandler = async () => {
  return new Response(JSON.stringify({ error: "Method not allowed" }), {
    status: 405,
    headers: { "Content-Type": "application/json" },
  });
};
