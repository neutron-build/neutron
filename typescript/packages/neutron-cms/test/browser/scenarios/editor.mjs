import { readFileSync } from "node:fs";
import { basename } from "node:path";
import { getPost } from "../../../dist/store.js";
import { imageFile, SAMPLES } from "../fixture.mjs";
import { PHONE, pickFiles, scenario, sendFiles } from "../harness.mjs";

export const VISUAL = ".visual .ProseMirror";
export const markdown = (page) => page.locator("textarea[name=body]").inputValue();
export const postId = (page) => Number(new URL(page.url()).pathname.split("/").at(-1));

export async function newPost(t, page, kind = "post") {
  await t.go(page, kind === "page" ? "/n-admin/pages" : "/n-admin");
  await page.getByRole("button", { name: kind === "page" ? "New page" : "New post" }).click();
  await page.waitForURL(/\/n-admin\/posts\/\d+$/);
  await t.visible(page.locator(VISUAL), "the visual editor shows");
  await t.fits(page, "a new post");
}

/** Selects words in the visual editor, as a drag over them would. */
export async function selectText(page, text) {
  await page.evaluate(({ selector, text }) => {
    const root = document.querySelector(selector);
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    for (let node = walker.nextNode(); node; node = walker.nextNode()) {
      const at = node.data.indexOf(text);
      if (at < 0) continue;
      root.focus();
      const range = document.createRange();
      range.setStart(node, at);
      range.setEnd(node, at + text.length);
      getSelection().removeAllRanges();
      getSelection().addRange(range);
      return;
    }
    throw new Error(`No "${text}" in the editor`);
  }, { selector: VISUAL, text });
  // The editor reads the selection on the next frames.
  await page.evaluate(() => new Promise((done) => requestAnimationFrame(() => requestAnimationFrame(done))));
  await page.locator(".float-panel:not([hidden])").waitFor({ timeout: 3000 }).catch(() => {});
}

export async function caretAtEnd(page) {
  await page.locator(VISUAL).click();
  await page.keyboard.press("ControlOrMeta+ArrowDown").catch(() => {});
  await page.evaluate((selector) => {
    const root = document.querySelector(selector);
    root.focus();
    const range = document.createRange();
    range.selectNodeContents(root.lastElementChild ?? root);
    range.collapse(false);
    getSelection().removeAllRanges();
    getSelection().addRange(range);
  }, VISUAL);
}

/** "Add image": straight to the file picker when there are no images yet, else through the chooser. */
export async function addImageByButton(t, page, path, button = page.locator("[data-add-image]")) {
  const picker = page.waitForEvent("filechooser", { timeout: 5000 }).then((chooser) => ({ chooser }));
  const dialog = page.locator("dialog.chooser[open]").waitFor({ timeout: 5000 }).then(() => ({ dialog: true }));
  await button.click();
  const first = await Promise.race([picker, dialog]);
  if (first.chooser) await first.chooser.setFiles(path);
  else await pickFiles(page, () => page.locator("dialog.chooser").getByRole("button", { name: "Upload" }).click(), path);
}

const settings = (page) => page.locator("[data-toggle-meta]");
export async function openSettings(page) {
  if ((await settings(page).getAttribute("aria-expanded")) !== "true") await settings(page).click();
}

const waitSaved = (t, page) => t.seeText(page.locator("[data-state]"), "Saved", "the draft autosaves");

scenario("editor: write a draft with formatting from the panel and by shortcut", async (t) => {
  const page = await t.page();
  await newPost(t, page);
  await t.eventually(() => page.evaluate(() => document.activeElement?.getAttribute("name") === "title"), "the title has focus in a new post");
  await page.keyboard.type("Formatting test");
  await page.keyboard.press("Enter");
  t.check(await page.evaluate((s) => document.querySelector(s).contains(document.activeElement), VISUAL), "Enter in the title moves to the text");
  await page.keyboard.type("Plain words to make bold and italic and linked.");
  await page.keyboard.press("Enter");
  await page.keyboard.type("A heading line");
  await page.keyboard.press("Enter");
  await page.keyboard.type("First item");
  await page.keyboard.press("Enter");
  await page.keyboard.type("Numbered item");

  await selectText(page, "bold");
  await page.getByRole("button", { name: "Bold", exact: true }).click();
  await t.eventually(async () => (await markdown(page)).includes("**bold**"), "Bold from the panel");
  t.equal(await page.getByRole("button", { name: "Bold", exact: true }).getAttribute("aria-pressed"), "true", "Bold shows as on");
  await t.fits(page, "editor with the panel open");

  await selectText(page, "italic");
  await page.keyboard.press("ControlOrMeta+i");
  await t.eventually(async () => (await markdown(page)).includes("*italic*"), "Italic by shortcut");

  await selectText(page, "make");
  await page.keyboard.press("ControlOrMeta+b");
  await t.eventually(async () => (await markdown(page)).includes("**make**"), "Bold by shortcut");

  await selectText(page, "linked");
  await page.getByRole("button", { name: "Link", exact: true }).click();
  const address = page.getByLabel("Link address");
  t.check(await address.evaluate((el) => el === document.activeElement), "the link field takes focus");
  await address.fill("javascript:alert(1)");
  await address.press("Enter");
  t.check(!(await markdown(page)).includes("javascript:"), "a script link is refused");
  await address.fill("example.com/page");
  await address.press("Enter");
  await t.eventually(async () => (await markdown(page)).includes("[linked](https://example.com/page)"), "a typed address becomes a link");

  await selectText(page, "A heading line");
  await page.getByRole("button", { name: "Heading", exact: true }).click();
  await t.eventually(async () => (await markdown(page)).includes("## A heading line"), "Heading from the panel");
  await selectText(page, "First item");
  await page.getByRole("button", { name: "Bulleted list" }).click();
  await t.eventually(async () => /^[-*] First item/m.test(await markdown(page)), "Bulleted list from the panel");
  await selectText(page, "Numbered item");
  await page.getByRole("button", { name: "Numbered list" }).click();
  await t.eventually(async () => /^1\. Numbered item/m.test(await markdown(page)), "Numbered list from the panel");

  // Typed Markdown turns into formatting.
  await caretAtEnd(page);
  await page.keyboard.press("Enter");
  await page.keyboard.press("Enter");
  await page.keyboard.press("Enter");
  await page.keyboard.type("### Typed heading");
  await page.keyboard.press("Enter");
  await page.keyboard.type("Some **typed bold** here");
  await t.eventually(async () => {
    const text = await markdown(page);
    return text.includes("### Typed heading") && text.includes("**typed bold**");
  }, "typed Markdown becomes formatting");
  t.equal(await page.locator(`${VISUAL} h3`).textContent(), "Typed heading", "the heading shows as a heading");

  await page.keyboard.press("Escape");
  await waitSaved(t, page);
  const saved = getPost(postId(page));
  t.check(saved.body.includes("**bold**") && saved.body.includes("## A heading line") && saved.title === "Formatting test", "the autosave stored the Markdown");
  t.equal(saved.published_at, null, "it is still a draft");
});

scenario("editor: images by button, paste and drop, with descriptions", async (t) => {
  const page = await t.page();
  await newPost(t, page);
  await page.getByLabel("Title").fill("Images");
  await page.locator(VISUAL).click();
  await page.keyboard.type("Text before images.");

  await addImageByButton(t, page, imageFile(t.fixture.files, "button.png", 1200, 900));
  await t.eventually(async () => /!\[\]\(\/n-admin\/media\/[0-9a-f]{32}-1200x900\.webp\)|!\[\]\(\/n-admin\/media\/[0-9a-f]{32}-1200x900\.(png|jpg)\)/.test(await markdown(page)), "the uploaded image is in the text, at its full size");
  await t.eventually(async () => (await page.locator(`${VISUAL} .image img`).first().getAttribute("src"))?.includes("-t."), "the editor shows the small copy");

  // A click on the image asks for its description.
  await page.locator(`${VISUAL} .image img`).first().click();
  const description = page.getByLabel("Image description");
  await t.eventually(() => description.evaluate((el) => el === document.activeElement), "the description field takes the typing");
  await page.keyboard.type("A coloured square");
  await page.keyboard.press("Enter");
  await t.eventually(async () => (await markdown(page)).includes("![A coloured square](/n-admin/media/"), "the description is saved as alt text");

  const pasted = imageFile(t.fixture.files, "pasted.png");
  await caretAtEnd(page);
  await sendFiles(page, VISUAL, "paste", [{ name: "pasted.png", data: readFileSync(pasted) }]);
  await t.eventually(async () => ((await markdown(page)).match(/\/n-admin\/media\//g) ?? []).length === 2, "a pasted image uploads into the text");

  const dropped = imageFile(t.fixture.files, "dropped.png");
  await sendFiles(page, VISUAL, "drop", [{ name: "dropped.png", data: readFileSync(dropped) }]);
  await t.eventually(async () => ((await markdown(page)).match(/\/n-admin\/media\//g) ?? []).length === 3, "a dropped image uploads into the text");
  t.equal(await page.locator(`${VISUAL} .image.uploading`).count(), 0, "no image is left uploading");
  t.check(!(await markdown(page)).includes("blob:"), "no temporary address is saved");

  await waitSaved(t, page);
  t.equal((getPost(postId(page)).body.match(/\/n-admin\/media\//g) ?? []).length, 3, "all three images are saved");
  t.check(!basename(pasted).includes("/"), "files made");
});

scenario("editor: restore unsaved text after leaving, and discard it", async (t) => {
  const page = await t.page();
  await t.go(page, `/n-admin/posts/${t.fixture.ids.untouched}`);
  await caretAtEnd(page);
  await page.keyboard.type(" Unsaved addition.");
  await t.seeText(page.locator("[data-state]"), "Unsaved changes");
  await page.reload();
  await t.visible(page.locator("[data-restore]"), "offers the unsaved text");
  t.check(!(await markdown(page)).includes("Unsaved addition."), "the saved text shows first");
  await page.getByRole("button", { name: "Restore" }).click();
  await t.eventually(async () => (await markdown(page)).includes("Unsaved addition."), "Restore brings the text back");
  await t.seeText(page.locator(VISUAL), "Unsaved addition.", "and the visual editor shows it");
  await page.reload();
  await page.getByRole("button", { name: "Discard" }).click();
  await t.hidden(page.locator("[data-restore]"), "Discard hides the offer");
  await page.reload();
  t.check(await page.locator("[data-restore]").isHidden(), "and the copy is gone");
  t.equal(getPost(t.fixture.ids.untouched).body, SAMPLES.untouched, "nothing was saved");
});

scenario("editor: an untouched post opens and saves with its Markdown unchanged", async (t) => {
  const page = await t.page();
  for (const id of [t.fixture.ids.untouched, t.fixture.ids.iframe]) {
    const before = getPost(id);
    await t.go(page, `/n-admin/posts/${id}`);
    t.equal(await markdown(page), before.body, "the field holds the stored Markdown");
    await page.waitForTimeout(300);
    t.check(!(await page.locator("[data-state]").textContent()).includes("Unsaved"), "opening it changes nothing");
    await page.getByRole("button", { name: "Update" }).click();
    await page.waitForURL(/\/posts\/\d+$/);
    await t.seeText(page.locator("[data-state]"), "Saved");
    t.equal(getPost(id).body, before.body, "saving it leaves the Markdown byte for byte");
  }
});

scenario("editor: publish, update, unpublish, schedule, and URL rules", async (t) => {
  const page = await t.page();
  await newPost(t, page);
  await page.locator(VISUAL).click();
  await page.keyboard.type("A post with no title yet.");
  await page.getByRole("button", { name: "Publish" }).click();
  await t.seeText(page.getByRole("alert"), "Add a title before publishing.");
  t.equal(await settings(page).getAttribute("aria-expanded"), "true", "the settings open to show the error");
  await t.fits(page, "editor with settings open");

  await page.getByLabel("Title").fill("Rules post");
  await page.getByLabel("URL").fill("about");
  await page.getByRole("button", { name: "Publish" }).click();
  await t.seeText(page.getByRole("alert"), "That URL belongs to another page on the site.");
  await page.getByLabel("URL").fill("seed-1");
  await page.getByRole("button", { name: "Publish" }).click();
  await t.seeText(page.getByRole("alert"), "Another post already uses that URL.");
  await page.getByLabel("URL").fill("Rules Post Here!");
  await page.getByRole("button", { name: "Publish" }).click();
  await page.waitForURL(/\/posts\/\d+$/);
  await t.seeText(page.locator(".editor-top .status"), "Live");
  const id = postId(page);
  t.equal(getPost(id).slug, "rules-post-here", "the URL is cleaned up");
  await openSettings(page);
  t.check(await page.getByLabel("URL").isDisabled(), "the URL is fixed while live");
  const site = await page.context().newPage();
  await site.goto(`${t.origin}/rules-post-here`);
  await t.seeText(site.locator("article"), "A post with no title yet.", "the post is on the site");
  await site.close();

  await page.keyboard.press("Escape");
  await caretAtEnd(page);
  await page.keyboard.type(" Updated.");
  await page.getByRole("button", { name: "Update" }).click();
  await page.waitForURL(/\/posts\/\d+$/);
  t.check(getPost(id).body.endsWith("Updated."), "Update saves");

  page.answers.push(false);
  await page.getByRole("button", { name: "Unpublish" }).click();
  await page.waitForTimeout(200);
  t.check(getPost(id).published_at !== null, "cancelling the confirm keeps it live");
  page.answers.push(true);
  await page.getByRole("button", { name: "Unpublish" }).click();
  await t.seeText(page.locator(".editor-top .status"), "Draft");
  t.equal(getPost(id).published_at, null, "Unpublish takes it offline");

  await openSettings(page);
  await page.getByLabel("Publish on").fill("2031-05-06T07:08");
  await t.seeText(page.locator("[data-publish]"), "Schedule", "a future date makes Publish into Schedule");
  await page.locator("[data-publish]").click();
  await t.seeText(page.locator(".editor-top .status"), "Scheduled");
  t.check(getPost(id).published_at > "2031-05-05", "scheduled for the date given");
  await t.visible(page.getByRole("button", { name: "Unschedule" }), "Unschedule is offered");
});

scenario("editor: Markdown mode and back, and preview", async (t) => {
  const page = await t.page();
  await newPost(t, page);
  await page.getByLabel("Title").fill("Modes");
  await page.locator(VISUAL).click();
  await page.keyboard.type("Visual words.");
  await openSettings(page);
  await page.getByRole("button", { name: "Edit as Markdown" }).click();
  const field = page.locator("textarea[name=body]");
  await t.visible(field, "the Markdown field shows");
  t.check(await page.locator(VISUAL).isHidden(), "the visual editor hides");
  await field.click();
  await field.press("ControlOrMeta+End");
  await page.keyboard.type("\n\n## From Markdown\n\n- a\n- b");
  await openSettings(page);
  await page.getByRole("button", { name: "Edit visually" }).click();
  await t.visible(page.locator(`${VISUAL} h2`), "the heading typed as Markdown shows");
  t.equal(await page.locator(`${VISUAL} li`).count(), 2, "and the list");

  // Markdown it can't show stays as Markdown, with a note.
  await openSettings(page);
  await page.getByRole("button", { name: "Edit as Markdown" }).click();
  await field.click();
  await field.press("ControlOrMeta+End");
  await page.keyboard.type("\n\n| a | b |\n|---|---|\n| 1 | 2 |");
  await openSettings(page);
  await page.getByRole("button", { name: "Edit visually" }).click();
  const shownVisual = await page.locator(VISUAL).isVisible();
  const raw = shownVisual ? await page.locator(`${VISUAL} .raw`).count() : 0;
  t.check(shownVisual ? raw > 0 : await page.locator("[data-markdown-note]").isVisible(), "a table is kept as source, or the text stays Markdown with a note");

  await page.getByRole("button", { name: "Preview" }).click();
  const preview = page.frameLocator("[data-preview]");
  await t.seeText(preview.locator("h1"), "Modes", "the preview shows the title");
  await t.seeText(preview.locator("h2"), "From Markdown", "and the text, rendered");
  t.equal(await page.locator("[data-toggle-preview]").getAttribute("aria-pressed"), "true", "Preview is pressed");
  await t.fits(page, "preview");
  await page.getByRole("button", { name: "Edit", exact: true }).click();
  await t.visible(page.locator(".writing"), "Edit goes back to writing");
});

scenario("editor: phone bar fits and Add offers image and embed", async (t) => {
  const page = await t.page({ size: PHONE });
  await newPost(t, page);
  const bar = await page.locator(".editor-top").boundingBox();
  t.check(bar.height <= 60, `the editor bar stays one line on a phone (${bar.height}px)`);
  await page.getByRole("button", { name: "Add", exact: true }).click();
  const menu = page.locator("dialog[open]");
  await t.visible(menu.getByRole("button", { name: "Image" }), "Add offers Image");
  await t.visible(menu.getByRole("button", { name: "File" }), "File");
  await t.visible(menu.getByRole("button", { name: "Embed" }), "and Embed");
  await t.fits(page, "phone Add menu");
  await page.keyboard.press("Escape");
  await t.hidden(menu, "Escape closes the menu");
  await page.getByRole("button", { name: "Add", exact: true }).tap();
  await menu.getByRole("button", { name: "Embed" }).tap();
  await t.visible(page.locator("dialog[open]").getByLabel("Link or embed code"), "Embed asks for a link");
  await page.locator("dialog[open]").getByLabel("Link or embed code").fill("https://youtu.be/dQw4w9WgXcQ");
  await page.locator("dialog[open]").getByRole("button", { name: "Embed" }).tap();
  await t.eventually(async () => (await markdown(page)).includes("youtube-nocookie.com/embed/dQw4w9WgXcQ"), "the embed is added");
  await t.fits(page, "phone editor with an embed");
  await page.getByRole("button", { name: "Add", exact: true }).tap();
  await pickFiles(page, () => page.locator("dialog[open]").getByRole("button", { name: "Image" }).tap().then(async () => {
    const chooser = page.locator("dialog.chooser[open]").getByRole("button", { name: "Upload" });
    if (await chooser.isVisible().catch(() => false)) await chooser.tap();
  }), imageFile(t.fixture.files, "phone.png"));
  await t.eventually(async () => (await markdown(page)).includes("/n-admin/media/"), "Add, Image uploads an image");
});
