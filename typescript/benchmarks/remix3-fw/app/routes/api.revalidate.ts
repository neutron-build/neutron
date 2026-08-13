import { revalidateBenchCache } from "../lib/bench-cache";

export async function loader() {
  return new Response(JSON.stringify({ ok: false, error: "Method Not Allowed" }), {
    status: 405,
    headers: {
      "Content-Type": "application/json",
      Allow: "POST",
    },
  });
}

export async function action() {
  const version = revalidateBenchCache();
  return new Response(JSON.stringify({ ok: true, version }), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}
