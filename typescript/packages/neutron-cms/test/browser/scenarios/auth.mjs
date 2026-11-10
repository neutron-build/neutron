import { scenario } from "../harness.mjs";

scenario("log in: wrong password, then right, then log out", async (t) => {
  const page = await t.page({ owner: false });
  await t.go(page, "/n-admin/posts/1");
  t.check(page.url().includes("/n-admin/login?next="), "a signed-out visit goes to the login with where it was");
  await t.visible(page.getByText("Your session ended"), "says the session ended");
  await page.getByLabel("Password").fill("not the password");
  await page.getByRole("button", { name: "Log in" }).click();
  await t.seeText(page.getByRole("alert"), "Wrong password.");
  await t.fits(page, "login with an error");
  await page.getByLabel("Password").fill(t.ownerPassword);
  await page.getByRole("button", { name: "Log in" }).click();
  await page.waitForURL(/\/n-admin\/posts\/1$/);
  t.check(true, "back where it was after logging in");
  await t.go(page, "/n-admin");
  await page.getByRole("button", { name: "Log out" }).click();
  await page.waitForURL(/\/n-admin\/login$/);
  await t.go(page, "/n-admin/media");
  t.check(page.url().includes("/n-admin/login"), "logged out for real");
});

scenario("log in: password field has focus and Enter submits", async (t) => {
  const page = await t.page({ owner: false });
  await t.go(page, "/n-admin/login");
  await t.eventually(() => page.evaluate(() => document.activeElement?.getAttribute("name") === "password"), "password field is focused");
  await page.keyboard.type(t.ownerPassword);
  await page.keyboard.press("Enter");
  await page.waitForURL(`${t.origin}/n-admin`);
  t.check(true, "Enter logs in");
});
