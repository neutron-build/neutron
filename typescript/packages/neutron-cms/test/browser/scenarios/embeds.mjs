import { getPost } from "../../../dist/store.js";
import { SAMPLES } from "../fixture.mjs";
import { pasteText, scenario } from "../harness.mjs";
import { caretAtEnd, markdown, newPost, postId, VISUAL } from "./editor.mjs";

scenario("embeds: the Embed button, a bad link, focus and Escape", async (t) => {
  const page = await t.page();
  await newPost(t, page);
  await page.getByLabel("Title").fill("Embeds");
  await page.locator(VISUAL).click();
  await page.keyboard.type("Intro.");
  const button = page.getByRole("button", { name: "Embed", exact: true });
  // From the keyboard, so the button has focus to go back to (Safari doesn't focus a clicked button).
  await button.focus();
  await page.keyboard.press("Enter");
  const dialog = page.locator("dialog[open]");
  const field = dialog.getByLabel("Link or embed code");
  await t.eventually(() => field.evaluate((el) => el === document.activeElement), "the dialog's field takes focus");
  await t.fits(page, "embed dialog");
  await page.keyboard.press("Escape");
  await t.hidden(dialog, "Escape closes it");
  t.check(await button.evaluate((el) => el === document.activeElement), "focus goes back to Embed");

  await button.click();
  await field.fill("https://example.com/not-a-player");
  await field.press("Enter");
  await t.seeText(dialog.getByRole("alert"), "Use a YouTube, Vimeo or Spotify link");
  // Focus stays in the dialog.
  t.check(await t.trapped(page, dialog, 8), "Tab never reaches the page behind the dialog");
  await field.fill("https://vimeo.com/76979871");
  await dialog.getByRole("button", { name: "Embed" }).click();
  await t.hidden(dialog, "a good link closes the dialog");
  await t.visible(page.locator(`${VISUAL} .embed iframe`), "the player shows live in the editor");
  await t.seeText(page.locator(`${VISUAL} .embed-bar`), "vimeo.com");
  await t.eventually(async () => (await markdown(page)).includes('<iframe src="https://player.vimeo.com/video/76979871"'), "the embed is stored as an iframe");
  await t.fits(page, "editor with an embed");
  await t.seeText(page.locator("[data-state]"), "Saved");
  t.check(getPost(postId(page)).body.includes("player.vimeo.com"), "and saved");
});

scenario("embeds: a pasted player link, and Keep as link", async (t) => {
  const page = await t.page();
  await newPost(t, page);
  await page.getByLabel("Title").fill("Pasted");
  await page.locator(VISUAL).click();
  await page.keyboard.type("Intro.");
  await page.keyboard.press("Enter");
  await pasteText(page, VISUAL, "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=90");
  await t.eventually(async () => (await markdown(page)).includes("youtube-nocookie.com/embed/dQw4w9WgXcQ?start=90"), "a link pasted on an empty line becomes the player");
  const keep = page.getByRole("button", { name: "Keep as link" });
  await t.visible(keep, "Keep as link is offered");
  await keep.click();
  await t.eventually(async () => {
    const text = await markdown(page);
    return !text.includes("<iframe") && text.includes("https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=90");
  }, "Keep as link leaves the link as text");

  // A link pasted into a line with words stays a link.
  await caretAtEnd(page);
  await page.keyboard.press("Enter");
  await page.keyboard.type("Words then ");
  await pasteText(page, VISUAL, "https://youtu.be/dQw4w9WgXcQ");
  t.check(!(await markdown(page)).includes("<iframe"), "inside text, a player link stays text");
});

scenario("embeds: an iframe block survives editing around it byte for byte", async (t) => {
  const page = await t.page();
  const id = t.fixture.ids.iframe;
  await t.go(page, `/n-admin/posts/${id}`);
  await t.visible(page.locator(`${VISUAL} .embed iframe`), "the stored iframe shows as a player");
  await page.locator(`${VISUAL} p`).first().click();
  await page.keyboard.press("End");
  await page.keyboard.type(" Edited");
  await page.getByRole("button", { name: "Update" }).click();
  await page.waitForURL(/\/posts\/\d+$/);
  const iframe = SAMPLES.iframe.split("\n\n")[1];
  const body = getPost(id).body;
  t.check(body.includes(`\n\n${iframe}\n\n`), "the iframe is saved exactly as it was");
  t.check(body.includes("Edited"), "with the edit around it");
});
