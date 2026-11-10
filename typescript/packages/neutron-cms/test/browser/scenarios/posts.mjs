import { getPost, inTrash } from "../../../dist/store.js";
import { caretAtEnd, markdown, newPost, openSettings, postId, VISUAL } from "./editor.mjs";
import { scenario } from "../harness.mjs";

scenario("posts: search and paging", async (t) => {
  const page = await t.page();
  await t.go(page, "/n-admin");
  await t.seeText(page.locator(".pager"), "Page 1 of 2", "57 posts show 50 at a time");
  t.equal(await page.locator("table.posts tr").count(), 50, "fifty on the first page");
  await page.getByRole("link", { name: "Older" }).click();
  await page.waitForURL(/page=2/);
  await t.fits(page, "posts page 2");
  t.check(await page.locator("table.posts tr").count() >= 7, "the rest on the second");
  await page.getByRole("link", { name: "Newer" }).click();
  await page.waitForURL((url) => !url.search.includes("page="));

  const search = page.getByRole("searchbox", { name: "Find a post" });
  await search.fill("needle");
  await search.press("Enter");
  await page.waitForURL(/q=needle/);
  t.equal(await page.locator("table.posts tr").count(), 1, "searching the text finds one post");
  await t.seeText(page.locator("table.posts"), "Seed post 7");
  await t.fits(page, "posts search");
  await search.fill("50%_");
  await search.press("Enter");
  await t.seeText(page.locator("main"), 'Nothing matches "50%_".', "no match says so");
  await page.getByRole("link", { name: "Show all" }).click();
  await page.waitForURL((url) => !url.search.includes("q="));
  t.equal(await search.inputValue(), "", "Show all clears the search");
});

scenario("posts: versions and restore", async (t) => {
  const page = await t.page();
  await newPost(t, page);
  await page.getByLabel("Title").fill("Versioned");
  await page.locator(VISUAL).click();
  await page.keyboard.type("First wording.");
  await page.getByRole("button", { name: "Publish" }).click();
  await page.waitForURL(/\/posts\/\d+$/);
  const id = postId(page);
  t.fixture.ids.versioned = id;
  for (const words of [" Second wording.", " Third wording."]) {
    await caretAtEnd(page);
    await page.keyboard.type(words);
    await page.getByRole("button", { name: "Update" }).click();
    await page.waitForURL(/\/posts\/\d+$/);
    await t.seeText(page.locator("[data-state]"), "Saved", "Update says it saved");
  }
  await openSettings(page);
  await page.getByRole("link", { name: /^Versions \(\d+\)$/ }).click();
  await page.waitForURL(/\/versions$/);
  await t.fits(page, "versions");
  const versions = page.getByRole("navigation", { name: "Versions" }).getByRole("link");
  t.check(await versions.count() >= 2, "earlier versions are listed");
  await versions.last().click();
  await page.waitForURL(/v=\d+/);
  await t.seeText(page.frameLocator(".version-preview").locator(".prose"), "First wording.", "the chosen version shows");
  t.equal(await versions.last().getAttribute("aria-current"), "true", "and is marked");
  await page.getByRole("button", { name: "Restore this version" }).click();
  await page.waitForURL(/\/posts\/\d+$/);
  await t.eventually(async () => (await markdown(page)) === "First wording.", "the old text is back in the editor");
  t.equal(getPost(id).body, "First wording.", "and stored");
  await t.go(page, `/n-admin/posts/${id}/versions`);
  t.check((await page.locator(".version-list small").first().textContent()).includes("words"), "the text it replaced is a version too");
});

scenario("posts: trash, undo, restore and delete forever", async (t) => {
  const page = await t.page();
  await newPost(t, page);
  await page.getByLabel("Title").fill("Doomed post");
  await page.locator(VISUAL).click();
  await page.keyboard.type("Words.");
  await t.seeText(page.locator("[data-state]"), "Saved");
  const id = postId(page);
  await openSettings(page);
  await page.getByRole("button", { name: "Move to Trash" }).click();
  await page.waitForURL(/\?deleted=\d+/);
  await t.seeText(page.getByRole("status"), '"Doomed post" moved to Trash.');
  t.equal(getPost(id), undefined, "the post left the list");
  await page.getByRole("button", { name: "Undo" }).click();
  await page.waitForURL(/\/posts\/\d+$/);
  t.equal(getPost(postId(page))?.title, "Doomed post", "Undo brings it back");

  await openSettings(page);
  await page.getByRole("button", { name: "Move to Trash" }).click();
  await page.waitForURL(/\?deleted=\d+/);
  const trashId = Number(new URL(page.url()).searchParams.get("deleted"));
  await page.getByRole("link", { name: /^Trash \(\d+\)$/ }).click();
  await page.waitForURL(/\/trash$/);
  await t.fits(page, "trash");
  const row = page.locator("tr", { hasText: "Doomed post" });
  await t.visible(row, "the post is in the Trash");
  page.answers.push(false);
  await row.getByRole("button", { name: "Delete forever" }).click();
  await page.waitForTimeout(200);
  t.check(Boolean(inTrash(trashId)), "cancelling keeps it");
  page.answers.push(true);
  await row.getByRole("button", { name: "Delete forever" }).click();
  await t.hidden(page.locator("tr", { hasText: "Doomed post" }), "Delete forever removes it");
  t.equal(inTrash(trashId), undefined, "for good");
});

scenario("pages: a new page, published at its own address", async (t) => {
  const page = await t.page();
  await t.go(page, "/n-admin/pages");
  await t.visible(page.locator("table.posts", { hasText: "Portfolio" }), "the designed page is listed");
  await newPost(t, page, "page");
  t.check(await page.getByRole("link", { name: "Back to pages" }).isVisible(), "a page goes back to Pages");
  await page.getByLabel("Title").fill("About us");
  await page.locator(VISUAL).click();
  await page.keyboard.type("We make things.");
  await openSettings(page);
  await page.getByLabel("URL").fill("team-page");
  await page.getByRole("button", { name: "Publish" }).click();
  await page.waitForURL(/\/posts\/\d+$/);
  await t.seeText(page.locator(".editor-top .status"), "Live");
  const site = await page.context().newPage();
  await site.goto(`${t.origin}/team-page`);
  await t.seeText(site.locator("article"), "We make things.", "the page is on the site");
  await t.go(page, "/n-admin/pages");
  await t.visible(page.locator("table.posts", { hasText: "About us" }), "and in Pages");
});
