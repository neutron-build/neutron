// Standalone islands entry.
//
// This is the framework-provided client entry for prerendered/static pages that
// contain interactive <Island>s. It imports ONLY the island runtime plus the
// virtual islands manifest — never hydrate.ts / the router / the fetcher — so a
// static page with an island ships a tiny runtime + that island's own
// code-split chunk instead of the full 34KB SPA runtime.

import { initIslands } from "./island-runtime.js";
// @ts-expect-error virtual module resolved by the neutron Vite plugin
import { islands } from "virtual:neutron-islands";

initIslands(islands as Record<string, () => Promise<unknown>>);
