import { createContext } from "preact";

// True below an <Island>. The outer island's render (SSR and client) owns its
// whole subtree, so a nested <Island> must not become a second hydration root.
// The islands runtime hydrates each island inside this provider too, so the
// client render matches the server's.
export const InsideIsland = createContext(false);
