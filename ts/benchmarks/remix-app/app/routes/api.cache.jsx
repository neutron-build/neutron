import { json } from "@remix-run/node";
import { readBenchCache } from "../lib/bench-cache";

export async function loader() {
  const result = readBenchCache();
  return json(result.payload, {
    headers: {
      "x-bench-cache": result.hit ? "HIT" : "MISS",
    },
  });
}

export async function action() {
  return json({ ok: false, error: "Method Not Allowed" }, { status: 405 });
}
