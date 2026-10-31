import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";
import * as path from "node:path";
import { describe, it, expect } from "vitest";

/**
 * Type-contract test: `<Island>` must be usable from React-typed JSX.
 *
 * react-compat apps typecheck against @types/react (`jsxImportSource:
 * "react"`) while the runtime stays preact/compat. Island's element is a
 * preact VNode, which React's ReactNode rejects (TS2786) — the return type
 * is therefore deliberately JSX-agnostic (see island.tsx). This test
 * compiles type-fixtures/react-island/ with tsc exactly the way such an app
 * would, against the package's built dist declarations.
 *
 * Requires `pnpm build` first, like the other dist-resolving tests.
 */
describe("<Island> from React-typed JSX (react-compat type contract)", () => {
  it("compiles a jsxImportSource:react module using <Island> with children", () => {
    const require = createRequire(import.meta.url);
    const tsc = require.resolve("typescript/lib/tsc.js");
    const project = path.join(__dirname, "type-fixtures", "react-island", "tsconfig.json");

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
      // The exact TS2786 text lives in the diff of the commit that introduced
      // this test; reprinting it here would just go stale.
      expect.fail(`react-typed fixture failed to compile:\n${compilerOutput}`);
    }
  }, 60_000);
});
