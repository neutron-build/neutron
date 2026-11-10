// Runs every scenario in Chrome, then in WebKit: one headless browser at a time, a fresh site each.
//   pnpm run test:browser                  both browsers
//   pnpm run test:browser -- webkit        one browser
//   pnpm run test:browser -- --grep media  scenarios whose name matches
//   SHOTS=/some/folder                     where screenshots go (default: a temp folder, removed)
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { chromium, webkit } from "playwright-core";
import { startFixture } from "./fixture.mjs";
import { allScenarios, Failure, Run } from "./harness.mjs";
import "./scenarios/auth.mjs";
import "./scenarios/editor.mjs";
import "./scenarios/posts.mjs";
import "./scenarios/embeds.mjs";
import "./scenarios/lists.mjs";
import "./scenarios/edit-site.mjs";
import "./scenarios/history.mjs";
import "./scenarios/media.mjs";
import "./scenarios/screens.mjs";
import "./scenarios/people.mjs";

const args = process.argv.slice(2).filter((arg) => arg !== "--");
const grepAt = args.indexOf("--grep");
const grep = grepAt >= 0 ? new RegExp(args.splice(grepAt, 2)[1], "i") : null;
const browsers = [
  { name: "chrome", launch: () => chromium.launch({ channel: "chrome", headless: true }) },
  { name: "webkit", launch: () => webkit.launch({ headless: true }) },
].filter((browser) => !args.length || args.includes(browser.name));

const temporaryShots = !process.env.SHOTS;
const shots = process.env.SHOTS ?? mkdtempSync(join(tmpdir(), "cms-shots-"));
const summary = [];
let failedAny = false;

for (const { name, launch } of browsers) {
  const fixture = await startFixture();
  const browser = await launch();
  const run = new Run({ browser, browserName: name, fixture, shots });
  const started = Date.now();
  let passed = 0;
  const failed = [];
  for (const { name: title, run: body } of allScenarios()) {
    if (grep && !grep.test(title)) continue;
    const before = run.problems.length;
    try {
      await body(run);
      if (run.problems.length > before) throw new Failure(run.problems.slice(before).join("\n    "));
      passed += 1;
      console.log(`  ok   ${name}  ${title}`);
    } catch (error) {
      failed.push(title);
      console.log(`  FAIL ${name}  ${title}\n    ${error instanceof Failure ? error.message : error.stack}`);
    } finally {
      await run.closeAll();
    }
  }
  await browser.close();
  await fixture.stop();
  const seconds = Math.round((Date.now() - started) / 1000);
  const line = `${name}: ${passed + failed.length} scenarios, ${run.assertions} assertions, ${failed.length} failed, ${seconds}s`;
  summary.push(line + (failed.length ? `\n  failed: ${failed.join("; ")}` : ""));
  failedAny ||= failed.length > 0;
}

console.log(`\n${summary.join("\n")}`);
if (temporaryShots) rmSync(shots, { recursive: true, force: true });
process.exit(failedAny ? 1 : 0);
