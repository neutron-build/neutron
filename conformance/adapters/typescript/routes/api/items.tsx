// The one substantive route the TypeScript conformance app serves.
//
// GET  — the compressible JSON body (~50 repetitive records) that the
//        `mw.compression` probe acts on; also the preflight target for
//        `mw.cors` and the x-request-id carrier for `mw.requestid`.
// POST — the canonical validation endpoint (FRAMEWORK_CONTRACT.md §2):
//        an invalid body must answer 422 `application/problem+json` with a
//        populated `errors[]`. The validation itself is the SDK's, not
//        hand-rolled here — `validateJsonBody` + a zod schema — so the
//        dimension exercises the framework feature (S81) rather than an
//        adapter-shaped imitation of it.
//
// Same path and shape the Go, Rust, Python and Elixir conformance apps serve
// at /api/items, so the five are probed identically.
import {
  json,
  validateJsonBody,
  z,
  type ActionArgs,
} from "@neutron-build/core";

export const config = { mode: "app" };

const NewItem = z.object({
  name: z.string().min(1),
  price: z.number().gte(0),
});

export async function loader() {
  const items = Array.from({ length: 50 }, (_, i) => ({
    id: i + 1,
    name: `conformance-item-${i + 1}`,
    price: i + 1,
  }));

  throw new Response(JSON.stringify(items), {
    status: 200,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

export async function action({ request }: ActionArgs) {
  const item = await validateJsonBody(request, NewItem);
  return json({ id: 1, name: item.name, price: item.price }, 201);
}
