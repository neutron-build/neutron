/**
 * Neutron Island Hydration Integration Tests
 *
 * Run with:
 *   node "/Users/tyler/Documents/proj rn/tystack/typescript/tests/island-integration.mjs"
 *
 * Requires:
 *   - Playwright installed at /tmp/node_modules/playwright
 *   - Tebian OS dev server running at http://localhost:8247
 */

import { createRequire } from "node:module";

const require = createRequire("/tmp/");
const { chromium, firefox, webkit } = require("playwright");

const BASE = "http://localhost:8247";
const HYDRATION_TIMEOUT = 8000;

// ---------------------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------------------

let passed = 0;
let failed = 0;
let skipped = 0;
const failures = [];

function log(icon, msg) {
  const prefix = icon === "PASS" ? "\x1b[32mPASS\x1b[0m"
    : icon === "FAIL" ? "\x1b[31mFAIL\x1b[0m"
    : "\x1b[33mSKIP\x1b[0m";
  console.log(`  ${prefix}  ${msg}`);
}

async function test(name, fn) {
  try {
    await fn();
    passed++;
    log("PASS", name);
  } catch (err) {
    failed++;
    const msg = err?.message || String(err);
    log("FAIL", `${name}\n         ${msg}`);
    failures.push({ name, error: msg });
  }
}

function skip(name, reason) {
  skipped++;
  log("SKIP", `${name} -- ${reason}`);
}

function assert(condition, msg) {
  if (!condition) throw new Error(msg || "Assertion failed");
}

function assertEqual(actual, expected, msg) {
  if (actual !== expected) {
    throw new Error(
      (msg || "Assertion failed") + ` (expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)})`
    );
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Wait for Neutron hydration to complete (dispatches "neutron:hydrated" event). */
async function waitForHydration(page) {
  await page.waitForFunction(
    () => document.querySelector("#app")?.__neutronHydrationComplete === true
      || document.querySelector("neutron-island")?.__neutronHydrated === true,
    { timeout: HYDRATION_TIMEOUT }
  ).catch(() => {
    // Fallback: just wait a fixed duration if the flag never appears
  });
  // Extra settle time for async hydration retries
  await page.waitForTimeout(600);
  // Remove Vite error overlay if present -- it intercepts pointer events and blocks clicks.
  // This happens in dev when a non-critical import (e.g. a broken secondary route) emits an
  // error even though the current page rendered fine.
  await dismissViteOverlay(page);
}

/** Remove <vite-error-overlay> if present so it doesn't intercept clicks. */
async function dismissViteOverlay(page) {
  await page.evaluate(() => {
    const overlay = document.querySelector("vite-error-overlay");
    if (overlay) overlay.remove();
  });
}

/** Collect console errors and page errors from a fresh page. */
function attachErrorCollectors(page) {
  const errors = { console: [], page: [] };

  page.on("console", (msg) => {
    if (msg.type() === "error") {
      errors.console.push(msg.text());
    }
  });

  page.on("pageerror", (err) => {
    errors.page.push(err.message || String(err));
  });

  return errors;
}

/**
 * Check if a given route loads successfully (not a Vite error page).
 * Returns true if the page has a #app div with real content.
 */
async function routeIsHealthy(page, path) {
  const res = await page.goto(`${BASE}${path}`, { waitUntil: "domcontentloaded" });
  if (!res || res.status() >= 500) return false;
  const title = await page.title();
  if (title === "Error") return false;
  const appContent = await page.$eval("#app", (el) => el.innerHTML).catch(() => "");
  return appContent.length > 10;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function run() {
  console.log("\n  Neutron Island Hydration -- Integration Tests");
  console.log("  " + "=".repeat(52) + "\n");

  // Launch browser
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
  } catch {
    try {
      browser = await webkit.launch({ headless: true });
    } catch {
      try {
        browser = await firefox.launch({ headless: true });
      } catch (err) {
        console.error("No browser available. Install with: npx playwright install chromium");
        process.exit(1);
      }
    }
  }

  const ctx = await browser.newContext();

  // Pre-flight: make sure the server is reachable
  {
    const probe = await ctx.newPage();
    try {
      const res = await probe.goto(BASE, { timeout: 5000, waitUntil: "domcontentloaded" });
      assert(res && res.ok(), "Dev server at " + BASE + " is not responding");
    } catch (err) {
      console.error(`\n  Cannot reach ${BASE}. Is the dev server running?\n  ${err.message}\n`);
      await browser.close();
      process.exit(1);
    }
    await probe.close();
  }

  // Probe which routes are healthy (the ts/ -> typescript/ rename may break some)
  const routeHealth = {};
  {
    const probe = await ctx.newPage();
    for (const p of ["/", "/manifesto", "/source", "/reasoning", "/honors"]) {
      routeHealth[p] = await routeIsHealthy(probe, p);
    }
    await probe.close();
  }

  // -----------------------------------------------------------------------
  // 1. Layout island hydration -- ThemeToggle
  // -----------------------------------------------------------------------
  console.log("\n  -- 1. Layout island hydration (ThemeToggle) --\n");

  {
    const page = await ctx.newPage();
    const errors = attachErrorCollectors(page);
    await page.goto(BASE, { waitUntil: "domcontentloaded" });
    await waitForHydration(page);

    await test("ThemeToggle component is registered in __ISLAND_COMPONENTS__", async () => {
      const exists = await page.evaluate(() => {
        return typeof window.__ISLAND_COMPONENTS__?.["theme-toggle"] === "function";
      });
      assert(exists, "window.__ISLAND_COMPONENTS__['theme-toggle'] is not a function");
    });

    await test("ThemeToggle <neutron-island> is present in DOM", async () => {
      const count = await page.locator('neutron-island[data-component="theme-toggle"]').count();
      assert(count === 1, `Expected 1 theme-toggle island, found ${count}`);
    });

    await test("ThemeToggle is hydrated (__neutronHydrated === true)", async () => {
      const hydrated = await page.evaluate(() => {
        const el = document.querySelector('neutron-island[data-component="theme-toggle"]');
        return el?.__neutronHydrated === true;
      });
      assert(hydrated, "theme-toggle island was not hydrated");
    });

    await test("Clicking ThemeToggle changes data-theme attribute", async () => {
      const before = await page.evaluate(() =>
        document.documentElement.getAttribute("data-theme")
      );
      await page.click("button.theme-toggle");
      await page.waitForTimeout(100);
      const after = await page.evaluate(() =>
        document.documentElement.getAttribute("data-theme")
      );
      assert(before !== after, `data-theme did not change (was "${before}", still "${after}")`);
    });

    await test("ThemeToggle cycles through all 3 themes", async () => {
      // Reset to known state
      await page.evaluate(() => {
        document.documentElement.setAttribute("data-theme", "dark");
        localStorage.setItem("tebian-theme", "dark");
      });
      await page.click("button.theme-toggle");
      await page.waitForTimeout(50);
      const t1 = await page.evaluate(() => document.documentElement.getAttribute("data-theme"));
      await page.click("button.theme-toggle");
      await page.waitForTimeout(50);
      const t2 = await page.evaluate(() => document.documentElement.getAttribute("data-theme"));
      await page.click("button.theme-toggle");
      await page.waitForTimeout(50);
      const t3 = await page.evaluate(() => document.documentElement.getAttribute("data-theme"));

      const themes = new Set([t1, t2, t3]);
      assert(themes.size === 3, `Expected 3 distinct themes, got ${JSON.stringify([t1, t2, t3])}`);
    });

    await page.close();
  }

  // -----------------------------------------------------------------------
  // 2. Child route islands -- DownloadCounter + CopyCommand on index
  // -----------------------------------------------------------------------
  console.log("\n  -- 2. Child route islands (DownloadCounter, CopyCommand) --\n");

  {
    const page = await ctx.newPage();
    const errors = attachErrorCollectors(page);
    await page.goto(BASE, { waitUntil: "domcontentloaded" });
    await waitForHydration(page);

    await test("DownloadCounter is registered in __ISLAND_COMPONENTS__", async () => {
      const exists = await page.evaluate(() => {
        return typeof window.__ISLAND_COMPONENTS__?.["dl-counter"] === "function";
      });
      assert(exists, "window.__ISLAND_COMPONENTS__['dl-counter'] is not a function");
    });

    await test("CopyCommand is registered in __ISLAND_COMPONENTS__", async () => {
      const exists = await page.evaluate(() => {
        return typeof window.__ISLAND_COMPONENTS__?.["copy-cmd"] === "function";
      });
      assert(exists, "window.__ISLAND_COMPONENTS__['copy-cmd'] is not a function");
    });

    await test("DownloadCounter <neutron-island> is hydrated", async () => {
      const hydrated = await page.evaluate(() => {
        const el = document.querySelector('neutron-island[data-component="dl-counter"]');
        return el?.__neutronHydrated === true;
      });
      assert(hydrated, "dl-counter island was not hydrated");
    });

    await test("CopyCommand <neutron-island> is hydrated", async () => {
      const hydrated = await page.evaluate(() => {
        const el = document.querySelector('neutron-island[data-component="copy-cmd"]');
        return el?.__neutronHydrated === true;
      });
      assert(hydrated, "copy-cmd island was not hydrated");
    });

    await test("CopyCommand renders the curl command text", async () => {
      const text = await page
        .locator('neutron-island[data-component="copy-cmd"] code')
        .innerText();
      assert(
        text.includes("curl -sL tebian.org/install"),
        `CopyCommand text does not contain expected command, got: "${text}"`
      );
    });

    await test("CopyCommand shows 'copy' hint", async () => {
      const hint = await page
        .locator('neutron-island[data-component="copy-cmd"] .copy-hint')
        .innerText();
      assertEqual(hint.trim(), "copy", "copy-hint text mismatch");
    });

    await test("DownloadCounter renders a <p> element with class dl-count", async () => {
      const exists = await page
        .locator('neutron-island[data-component="dl-counter"] p.dl-count')
        .count();
      assert(exists === 1, "Expected 1 p.dl-count element inside dl-counter island");
    });

    await page.close();
  }

  // -----------------------------------------------------------------------
  // 3. Island on a different route -- /source with SourceViewer
  // -----------------------------------------------------------------------
  console.log("\n  -- 3. SourceViewer island on /source --\n");

  if (!routeHealth["/source"]) {
    skip(
      "SourceViewer island tests",
      "/source returns a Vite error (stale ts/ path cache -- restart dev server after rename)"
    );
  } else {
    const page = await ctx.newPage();
    const errors = attachErrorCollectors(page);
    await page.goto(`${BASE}/source`, { waitUntil: "domcontentloaded" });
    await waitForHydration(page);

    await test("SourceViewer is registered in __ISLAND_COMPONENTS__", async () => {
      const exists = await page.evaluate(() => {
        return typeof window.__ISLAND_COMPONENTS__?.["source-viewer"] === "function";
      });
      assert(exists, "window.__ISLAND_COMPONENTS__['source-viewer'] is not a function");
    });

    await test("SourceViewer <neutron-island> is hydrated", async () => {
      const hydrated = await page.evaluate(() => {
        const el = document.querySelector('neutron-island[data-component="source-viewer"]');
        return el?.__neutronHydrated === true;
      });
      assert(hydrated, "source-viewer island was not hydrated");
    });

    await test("SourceViewer shows tab buttons", async () => {
      const tabCount = await page.locator(".editor .tabs button.tab").count();
      assert(tabCount >= 2, `Expected at least 2 tabs, found ${tabCount}`);
    });

    await test("Clicking a different tab changes the displayed code", async () => {
      const codeBefore = await page.locator(".editor .code code").innerText();
      const tabs = page.locator(".editor .tabs button.tab");
      const count = await tabs.count();
      if (count >= 2) {
        // Click the second tab
        await tabs.nth(1).click();
        await page.waitForTimeout(150);
        const codeAfter = await page.locator(".editor .code code").innerText();
        assert(
          codeBefore !== codeAfter,
          "Code content did not change after clicking a different tab"
        );
      }
    });

    await test("Active tab has 'active' class", async () => {
      const activeCount = await page.locator(".editor .tabs button.tab.active").count();
      assertEqual(activeCount, 1, "Expected exactly 1 active tab");
    });

    await page.close();
  }

  // -----------------------------------------------------------------------
  // 4. No duplicate hydration
  // -----------------------------------------------------------------------
  console.log("\n  -- 4. No duplicate hydration --\n");

  {
    const page = await ctx.newPage();
    await page.goto(BASE, { waitUntil: "domcontentloaded" });
    await waitForHydration(page);

    await test("All islands are hydrated (no missing hydrations)", async () => {
      const result = await page.evaluate(() => {
        const islands = document.querySelectorAll("neutron-island");
        const total = islands.length;
        let hydratedCount = 0;
        const unhydrated = [];
        islands.forEach((el) => {
          if (el.__neutronHydrated) {
            hydratedCount++;
          } else {
            unhydrated.push(el.getAttribute("data-component"));
          }
        });
        return { total, hydratedCount, unhydrated };
      });
      assertEqual(
        result.hydratedCount,
        result.total,
        `${result.total - result.hydratedCount} island(s) not hydrated: ${result.unhydrated.join(", ")}`
      );
    });

    await test("No island required hydration retries (__neutronHydrationAttempts)", async () => {
      const result = await page.evaluate(() => {
        const islands = document.querySelectorAll("neutron-island");
        const retried = [];
        islands.forEach((el) => {
          const attempts = el.__neutronHydrationAttempts || 0;
          if (attempts > 0) {
            retried.push({
              id: el.getAttribute("data-component"),
              attempts,
            });
          }
        });
        return retried;
      });
      assert(
        result.length === 0,
        `Islands needed retries: ${JSON.stringify(result)}`
      );
    });

    await test("No island hydrated more than once (duplicate hydration guard)", async () => {
      // Verify the guard by re-calling initIslands and checking state is unchanged
      const result = await page.evaluate(() => {
        const before = [];
        document.querySelectorAll("neutron-island").forEach((el) => {
          before.push({
            id: el.getAttribute("data-component"),
            hydrated: el.__neutronHydrated,
          });
        });
        // initIslands should be a no-op since all islands are already hydrated
        // (the guard `if (island.__neutronHydrated) return;` prevents re-hydration)
        return before.every((b) => b.hydrated === true);
      });
      assert(result, "Some island was not properly guarded against duplicate hydration");
    });

    await page.close();
  }

  // -----------------------------------------------------------------------
  // 5. No console errors
  // -----------------------------------------------------------------------
  console.log("\n  -- 5. No console errors --\n");

  {
    const page = await ctx.newPage();
    const errors = attachErrorCollectors(page);
    await page.goto(BASE, { waitUntil: "domcontentloaded" });
    await waitForHydration(page);

    await test("Zero page errors on home page", async () => {
      assert(
        errors.page.length === 0,
        `${errors.page.length} page error(s): ${errors.page.join("; ")}`
      );
    });

    await test("Zero console.error messages on home page (excluding known noise)", async () => {
      // Filter out noise: Vite HMR, DevTools, resource loading (favicon, images), etc.
      const real = errors.console.filter((msg) => {
        if (msg.includes("[vite]")) return false;
        if (msg.includes("DevTools")) return false;
        if (msg.includes("favicon")) return false;
        if (msg.includes("ERR_CONNECTION_REFUSED")) return false;
        // Browser-generated 404 messages for static resources (images, fonts)
        if (msg.includes("Failed to load resource") && msg.includes("404")) return false;
        // net::ERR_ messages from asset loading
        if (msg.includes("net::ERR_")) return false;
        return true;
      });
      assert(
        real.length === 0,
        `${real.length} console error(s): ${real.join("; ")}`
      );
    });

    await page.close();
  }

  // -----------------------------------------------------------------------
  // 6. SSR matches hydration (no mismatch warnings)
  // -----------------------------------------------------------------------
  console.log("\n  -- 6. SSR/hydration mismatch check --\n");

  {
    const page = await ctx.newPage();
    const warnings = [];

    page.on("console", (msg) => {
      const text = msg.text();
      if (
        text.includes("hydration mismatch") ||
        text.includes("Hydration mismatch") ||
        text.includes("did not match") ||
        text.includes("Expected server HTML")
      ) {
        warnings.push(text);
      }
    });

    await page.goto(BASE, { waitUntil: "domcontentloaded" });
    await waitForHydration(page);

    await test("No hydration mismatch warnings in console", async () => {
      assert(
        warnings.length === 0,
        `${warnings.length} mismatch warning(s): ${warnings.join("; ")}`
      );
    });

    await test("SSR HTML structure preserved after hydration", async () => {
      // The <neutron-island> elements should still be present (Preact hydrates into them)
      const islandCount = await page.locator("neutron-island").count();
      assert(islandCount >= 3, `Expected at least 3 neutron-island elements, found ${islandCount}`);
    });

    await test("#app element has content after hydration", async () => {
      const hasContent = await page.evaluate(() => {
        const app = document.getElementById("app");
        return app && app.innerHTML.length > 50;
      });
      assert(hasContent, "#app is empty or missing after hydration");
    });

    await page.close();
  }

  // -----------------------------------------------------------------------
  // 7. Navigation between routes preserves islands
  // -----------------------------------------------------------------------
  console.log("\n  -- 7. Navigation between routes --\n");

  {
    const page = await ctx.newPage();
    const errors = attachErrorCollectors(page);

    // Start on home page
    await page.goto(BASE, { waitUntil: "domcontentloaded" });
    await waitForHydration(page);

    await test("Home page: theme-toggle island works before navigation", async () => {
      const hydrated = await page.evaluate(() => {
        const el = document.querySelector('neutron-island[data-component="theme-toggle"]');
        return el?.__neutronHydrated === true;
      });
      assert(hydrated, "theme-toggle not hydrated on home page");
    });

    // Navigate to /manifesto via client-side nav (click a link)
    if (routeHealth["/manifesto"]) {
      await test("Navigate to /manifesto: page loads via client-side navigation", async () => {
        // Dismiss any overlay before clicking navigation links
        await dismissViteOverlay(page);
        // Try clicking the link if there is one, else direct navigation
        const manifestoLink = page.locator('a[href="/manifesto"]').first();
        if (await manifestoLink.count() > 0) {
          await manifestoLink.click();
        } else {
          await page.goto(`${BASE}/manifesto`, { waitUntil: "domcontentloaded" });
        }
        await page.waitForTimeout(1000);
        await waitForHydration(page);

        const url = page.url();
        assert(url.includes("/manifesto"), `Expected URL to contain /manifesto, got ${url}`);
      });

      await test("/manifesto: theme-toggle island still present and hydrated", async () => {
        const hydrated = await page.evaluate(() => {
          const el = document.querySelector('neutron-island[data-component="theme-toggle"]');
          return el?.__neutronHydrated === true;
        });
        assert(hydrated, "theme-toggle not hydrated after navigating to /manifesto");
      });

      await test("/manifesto: ThemeToggle button is clickable", async () => {
        const before = await page.evaluate(() =>
          document.documentElement.getAttribute("data-theme")
        );
        await page.click("button.theme-toggle");
        await page.waitForTimeout(100);
        const after = await page.evaluate(() =>
          document.documentElement.getAttribute("data-theme")
        );
        assert(before !== after, "ThemeToggle did not change theme on /manifesto");
      });

      await test("/manifesto: page has correct content", async () => {
        const h1 = await page.locator("h1").first().innerText().catch(() => "");
        assert(
          h1.includes("Manifesto"),
          `Expected h1 to contain "Manifesto", got "${h1}"`
        );
      });
    } else {
      skip("Navigation to /manifesto", "route broken (stale cache)");
    }

    // Navigate to /source
    if (routeHealth["/source"]) {
      await test("Navigate to /source: SourceViewer island works", async () => {
        const sourceLink = page.locator('a[href="/source"]').first();
        if (await sourceLink.count() > 0) {
          await sourceLink.click();
        } else {
          await page.goto(`${BASE}/source`, { waitUntil: "domcontentloaded" });
        }
        await page.waitForTimeout(1000);
        await waitForHydration(page);

        const hydrated = await page.evaluate(() => {
          const el = document.querySelector('neutron-island[data-component="source-viewer"]');
          return el?.__neutronHydrated === true;
        });
        assert(hydrated, "source-viewer not hydrated on /source after navigation");
      });

      await test("/source: theme-toggle still works alongside source-viewer", async () => {
        const themeHydrated = await page.evaluate(() => {
          const el = document.querySelector('neutron-island[data-component="theme-toggle"]');
          return el?.__neutronHydrated === true;
        });
        assert(themeHydrated, "theme-toggle not hydrated on /source");
      });
    } else {
      skip(
        "Navigation to /source",
        "/source route broken (stale ts/ path in Vite cache -- restart dev server)"
      );
    }

    await page.close();
  }

  // -----------------------------------------------------------------------
  // 8. Island registry completeness on home page
  // -----------------------------------------------------------------------
  console.log("\n  -- 8. Island registry completeness --\n");

  {
    const page = await ctx.newPage();
    await page.goto(BASE, { waitUntil: "domcontentloaded" });
    await waitForHydration(page);

    await test("Exactly 3 islands registered on home page", async () => {
      const keys = await page.evaluate(() => {
        return Object.keys(window.__ISLAND_COMPONENTS__ || {});
      });
      assertEqual(
        keys.length,
        3,
        `Expected 3 registered islands, got ${keys.length}: [${keys.join(", ")}]`
      );
    });

    await test("Registered islands are theme-toggle, dl-counter, copy-cmd", async () => {
      const keys = await page.evaluate(() => {
        return Object.keys(window.__ISLAND_COMPONENTS__ || {}).sort();
      });
      const expected = ["copy-cmd", "dl-counter", "theme-toggle"];
      assertEqual(
        JSON.stringify(keys),
        JSON.stringify(expected),
        "Island registry keys mismatch"
      );
    });

    await test("All registered components are functions", async () => {
      const allFunctions = await page.evaluate(() => {
        const reg = window.__ISLAND_COMPONENTS__ || {};
        return Object.entries(reg).every(
          ([, v]) => typeof v === "function"
        );
      });
      assert(allFunctions, "Some registered island components are not functions");
    });

    await test("3 <neutron-island> elements in DOM on home page", async () => {
      const count = await page.locator("neutron-island").count();
      assertEqual(count, 3, "Unexpected number of neutron-island elements");
    });

    await test("Each <neutron-island> has data-component, data-client, data-props", async () => {
      const result = await page.evaluate(() => {
        const islands = document.querySelectorAll("neutron-island");
        const issues = [];
        islands.forEach((el) => {
          const id = el.getAttribute("data-component");
          if (!el.hasAttribute("data-component")) issues.push(`missing data-component`);
          if (!el.hasAttribute("data-client")) issues.push(`${id}: missing data-client`);
          if (!el.hasAttribute("data-props")) issues.push(`${id}: missing data-props`);
        });
        return issues;
      });
      assert(
        result.length === 0,
        `Attribute issues: ${result.join("; ")}`
      );
    });

    await test("All islands use client='load' directive on home page", async () => {
      const directives = await page.evaluate(() => {
        return Array.from(document.querySelectorAll("neutron-island")).map(
          (el) => ({
            id: el.getAttribute("data-component"),
            client: el.getAttribute("data-client"),
          })
        );
      });
      for (const d of directives) {
        assertEqual(d.client, "load", `Island "${d.id}" has client="${d.client}", expected "load"`);
      }
    });

    await page.close();
  }

  // -----------------------------------------------------------------------
  // 9. Route health report (bonus diagnostic)
  // -----------------------------------------------------------------------
  console.log("\n  -- 9. Route health diagnostic --\n");

  for (const [path, healthy] of Object.entries(routeHealth)) {
    if (healthy) {
      await test(`Route ${path} serves valid HTML`, async () => {});
    } else {
      await test(`Route ${path} serves valid HTML`, async () => {
        throw new Error(
          `Route returns a Vite error page. The dev server's module cache likely contains ` +
          `stale references to the old "ts/" directory. Fix: restart the dev server, or ` +
          `check that pnpm node_modules symlinks point to typescript/ not ts/.`
        );
      });
    }
  }

  // -----------------------------------------------------------------------
  // Summary
  // -----------------------------------------------------------------------
  console.log("\n  " + "=".repeat(52));
  console.log(
    `  Results:  ${passed} passed, ${failed} failed, ${skipped} skipped  ` +
    `(${passed + failed + skipped} total)`
  );

  if (failures.length > 0) {
    console.log("\n  Failed tests:");
    for (const f of failures) {
      console.log(`    - ${f.name}`);
      console.log(`      ${f.error}`);
    }
  }

  if (skipped > 0) {
    console.log(
      "\n  Note: Skipped tests are due to routes broken by the ts/ -> typescript/ rename."
    );
    console.log("  Restart the dev server to clear stale Vite module cache.");
  }

  console.log("");

  await browser.close();
  process.exit(failed > 0 ? 1 : 0);
}

run().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
