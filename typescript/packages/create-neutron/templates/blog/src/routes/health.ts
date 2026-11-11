import cms from "../cms";

export const config = { mode: "app" };

// Fails when the database is missing, so a deploy that lost its volume stops
// instead of serving an empty site.
export function GET() {
  const ready = cms.ready();
  return Response.json(
    { status: ready ? "ok" : "unavailable" },
    { status: ready ? 200 : 503, headers: { "Cache-Control": "no-store" } },
  );
}
