import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";
import * as path from "node:path";
import { describe, it, expect } from "vitest";

/**
 * Type-contract test: the export-sweep — every component export of the
 * public surface must be usable from React-typed JSX.
 *
 * react-compat apps typecheck against @types/react (`jsxImportSource:
 * "react"`) while the runtime stays preact/compat. The dedicated fixture
 * families cover Island, Form/Link/NavLink/ViewTransitions/ScrollReveal,
 * Show/For/Switch/Match/Index and ServerIsland; this test compiles
 * type-fixtures/react-export-sweep/ — the remaining component exports
 * (Await from the root entry, Image and Picture from the client entry,
 * whose JSX-agnostic returns predate the campaign) — the same way, against
 * the package's built dist declarations, with @ts-expect-error pins that
 * fail the compile if prop checking regresses under the react import
 * source. Together the families are the proof that no exported component
 * fails TS2786 from React-typed JSX.
 *
 * Requires `pnpm build` first, like the other dist-resolving tests.
 */
describe("export sweep from React-typed JSX (react-compat type contract)", () => {
  it("compiles a jsxImportSource:react module using Await, Image and Picture", () => {
    const require = createRequire(import.meta.url);
    const tsc = require.resolve("typescript/lib/tsc.js");
    const project = path.join(__dirname, "type-fixtures", "react-export-sweep", "tsconfig.json");

    let compilerOutput = "";
    try {
      execFileSync(process.execPath, [tsc, "--project", project, "--pretty", "false"], {
        cwd: __dirname,
        stdio: "pipe",
        encoding: "utf8",
      });
    } catch (error) {
      const err = error as { stdout?: string; stderr?: string; message?: string };
      compilerOutput = `${err.stdout ?? ""}${err.stderr ?? ""}${err.message ?? ""}`;
      // The exact TS2786 text lives in the diff of the commit that
      // introduced this test; reprinting it here would just go stale.
      expect.fail(`react-typed fixture failed to compile:\n${compilerOutput}`);
    }
  }, 60_000);
});
