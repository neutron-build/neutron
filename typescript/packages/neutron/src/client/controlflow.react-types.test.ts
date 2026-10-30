import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";
import * as path from "node:path";
import { describe, it, expect } from "vitest";

/**
 * Type-contract test: the control-flow components must be usable from
 * React-typed JSX.
 *
 * react-compat apps typecheck against @types/react (`jsxImportSource:
 * "react"`) while the runtime stays preact/compat. Show/For/Switch/Match/
 * Index returned preact VNodes, which React's ReactNode rejects (TS2786);
 * their element returns are therefore deliberately JSX-agnostic now (see
 * control-flow.tsx), props untouched. This test compiles
 * type-fixtures/react-control-flow/ with tsc exactly the way such an app
 * would, against the package's built dist declarations — including
 * @ts-expect-error pins that fail the compile if a mistyped prop stops
 * being rejected under the react import source.
 *
 * Requires `pnpm build` first, like the other dist-resolving tests.
 */
describe("control-flow components from React-typed JSX (react-compat type contract)", () => {
  it("compiles a jsxImportSource:react module using Show, For, Switch, Match and Index", () => {
    const require = createRequire(import.meta.url);
    const tsc = require.resolve("typescript/lib/tsc.js");
    const project = path.join(__dirname, "type-fixtures", "react-control-flow", "tsconfig.json");

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
