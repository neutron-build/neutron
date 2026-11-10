import { allTexts, listItems } from "../../../dist/store.js";
import { scenario } from "../harness.mjs";

const entries = (page) => page.getByRole("navigation", { name: "Saves" }).getByRole("link");

scenario("history: each kind of save, filters, restore and undo", async (t) => {
  const page = await t.page();

  // A text save.
  await t.go(page, "/n-admin/lists/text");
  const before = allTexts().tagline;
  await page.getByLabel("Tagline").fill("History tagline");
  await page.getByRole("button", { name: "Save" }).click();
  await page.waitForURL(/saved=1/);
  await page.getByRole("link", { name: "History" }).click();
  await page.waitForURL(/history\?only=text/);
  await t.fits(page, "history of texts");
  await t.seeText(page.locator(".history-filter"), "Showing saves of Text.");
  await t.seeText(entries(page).first(), "Tagline", "the newest save names the field");
  t.equal(await entries(page).first().getAttribute("aria-current"), "true", "and is shown");
  await t.seeText(page.locator(".history-detail"), before, "the value before the save");
  await t.seeText(page.locator(".history-now"), "History tagline", "and the value now");

  await page.getByRole("button", { name: "Restore" }).click();
  await page.waitForURL(/restored=\d+/);
  await t.seeText(page.getByRole("status"), "Restored.");
  t.equal(allTexts().tagline, before, "Restore puts the text back");
  await t.fits(page, "history after a restore");
  await page.getByRole("button", { name: "Undo" }).click();
  await t.eventually(() => allTexts().tagline === "History tagline", "Undo puts the restored text back");
  await t.seeText(page.getByRole("status"), "Restored.", "an undo can itself be undone");
  t.equal(allTexts().tagline, "History tagline", "Undo puts the restored text back");
  await t.seeText(entries(page).first(), "Tagline", "the restore and its undo are saves too");

  // A list save, filtered.
  await t.go(page, "/n-admin/lists/links");
  await page.getByRole("button", { name: "Add", exact: true }).click();
  await page.locator(".rows > .item").last().getByLabel("Label").fill("History link");
  await page.locator(".rows > .item").last().getByLabel("URL").fill("/history-link");
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.waitForURL(/saved=1/);
  await page.getByRole("link", { name: "History" }).click();
  await page.waitForURL(/only=links/);
  await t.seeText(entries(page).first(), "Links: 1 item added");
  const shown = await entries(page).allTextContents();
  t.check(shown.every((text) => text.includes("Links")), "the filter shows only saves of Links");
  await t.seeText(page.locator(".history-detail"), "Added since", "what the save added is listed");
  await t.seeText(page.locator(".history-detail"), "History link");
  await page.getByRole("button", { name: "Restore" }).click();
  await page.waitForURL(/restored=\d+/);
  t.check(!listItems("links").some((item) => item.label === "History link"), "Restore takes the added link out");
  await page.getByRole("button", { name: "Undo" }).click();
  await page.waitForLoadState("load");
  await t.eventually(() => listItems("links").some((item) => item.label === "History link"), "Undo brings the link back");
  t.check(listItems("links").some((item) => item.label === "History link"), "Undo brings it back");

  await page.getByRole("link", { name: "Show everything" }).click();
  await page.waitForURL(/\/history$/);
  const all = await entries(page).allTextContents();
  t.check(all.some((text) => text.includes("Team")), "Edit site saves are listed");
  t.check(all.some((text) => text.includes("Tagline")) && all.some((text) => text.includes("Links")), "with texts and lists together");

  // An older entry, chosen.
  await entries(page).nth(2).click();
  await page.waitForURL(/s=\d+/);
  t.equal(await entries(page).nth(2).getAttribute("aria-current"), "true", "a chosen save is shown");
  await t.fits(page, "a chosen save");
});
