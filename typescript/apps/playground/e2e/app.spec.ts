import { test, expect, type Page } from "@playwright/test";

/**
 * The three behaviours that make this a framework rather than a static
 * renderer: client-side routing, a form round-trip through an action, and
 * island hydration that actually attaches event handlers.
 *
 * Each test asserts something that is observably different when the feature
 * is broken — not merely that the server-rendered markup is present.
 */

declare global {
  interface Window {
    __NEUTRON_ROUTER_ACTIVE__?: boolean;
    __e2eNavMarker?: string;
  }
}

async function waitForRouter(page: Page): Promise<void> {
  await expect
    .poll(() => page.evaluate(() => window.__NEUTRON_ROUTER_ACTIVE__ === true))
    .toBe(true);
}

test("navigate: a nav link changes the view client-side, without a page load", async ({
  page,
}) => {
  await page.goto("/todos");
  await expect(page.getByRole("heading", { name: "Todos" })).toBeVisible();
  await waitForRouter(page);

  // A marker on the window object survives only if the document is NOT
  // replaced. A full page load (the regression this guards against: the
  // click interceptor failing over to browser navigation) wipes it.
  await page.evaluate(() => {
    window.__e2eNavMarker = "alive";
  });

  await page.click('nav a[href="/dashboard"]');

  await expect(page).toHaveURL(/\/dashboard$/);
  await expect(
    page.getByRole("heading", { name: "Dashboard" })
  ).toBeVisible();
  await expect(page.getByText("Hello from the loader!")).toBeVisible();

  const marker = await page.evaluate(() => window.__e2eNavMarker);
  expect(marker).toBe("alive");
});

test("form: submitting the add-todo form round-trips through the action", async ({
  page,
}) => {
  await page.goto("/todos");
  await expect(page.getByRole("heading", { name: "Todos" })).toBeVisible();

  const addForm = page.locator(
    'form:has(input[name="_intent"][value="add"])'
  );
  const todoText = `e2e form round-trip ${Date.now()}`;

  // Happy path: the action's response is reflected in the DOM.
  await page.fill('input[name="text"]', todoText);
  await addForm.getByRole("button", { name: "Add" }).click();
  await expect(page.getByText("Action completed")).toBeVisible();
  await expect(page.getByText(todoText)).toBeVisible();

  // Failure path: whitespace-only text passes the browser's `required`
  // check, so the action receives it — and must refuse to add a row.
  const rowsBefore = await page.locator("ul li").count();
  await page.fill('input[name="text"]', "   ");
  await addForm.getByRole("button", { name: "Add" }).click();
  await expect(page.getByText("Action completed")).toBeVisible();
  await expect(page.locator("ul li")).toHaveCount(rowsBefore);
});

test("island: the Toggle island is interactive after hydration", async ({
  page,
}) => {
  await page.goto("/islands");
  await expect(
    page.getByRole("heading", { name: "Islands Demo" })
  ).toBeVisible();

  const toggle = page.locator('neutron-island[data-component="Toggle"] button');

  // Hydration flag on the marker element (set by the client runtime).
  await expect
    .poll(() =>
      page.evaluate(() => {
        const el = document.querySelector(
          'neutron-island[data-component="Toggle"]'
        ) as (HTMLElement & { __neutronHydrated?: boolean }) | null;
        return el?.__neutronHydrated === true;
      })
    )
    .toBe(true);

  // The assertion that matters: clicking flips the label. SSR alone leaves
  // "OFF" in the markup with no handler; only a hydrated island can change it.
  await expect(toggle).toHaveText("OFF");
  await toggle.scrollIntoViewIfNeeded();
  await toggle.click();
  await expect(toggle).toHaveText("ON");
});
