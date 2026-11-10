// Runs last: the owner's password changes at the end.
import { DESKTOP, PHONE, scenario } from "../harness.mjs";
import { everyWay } from "./screens.mjs";

async function logIn(t, page, name, password) {
  await page.goto(`${t.origin}/n-admin/login`);
  await page.getByLabel("Name").fill(name);
  await page.getByLabel("Password").fill(password);
  await page.getByRole("button", { name: "Log in" }).click();
}

async function changePassword(page, current, next, repeat = next) {
  await page.getByLabel("Current password").fill(current);
  await page.locator("input[name=password]").fill(next);
  await page.getByLabel("New password again").fill(repeat);
  await page.getByRole("button", { name: "Change password" }).click();
}

scenario("people: add, log in as them, account, reset, remove", async (t) => {
  const owner = await t.page();
  await t.go(owner, "/n-admin/people");
  await t.seeText(owner.locator("main"), "Only you can log in now.");
  const name = owner.getByLabel("Name");
  await name.fill("a b");
  await owner.getByRole("button", { name: "Add person" }).click();
  await t.seeText(owner.getByRole("alert"), "Use letters, numbers, dots, dashes or underscores");
  await owner.getByLabel("Name").fill("Sam");
  await owner.getByRole("button", { name: "Add person" }).click();
  const given = owner.locator(".given");
  await t.seeText(given, "sam is added.", "the name is kept in lower case");
  const password = (await given.locator("dd code").nth(1).textContent()).trim();
  t.check(password.length >= 24, "a strong password is shown once");
  await t.fits(owner, "people with a new password");
  await owner.reload();
  t.equal(await owner.locator(".given").count(), 0, "the password is not shown again");
  await owner.getByLabel("Name").fill("SAM");
  await owner.getByRole("button", { name: "Add person" }).click();
  await t.seeText(owner.getByRole("alert"), "Someone already has that name.");
  await everyWay(t, "/n-admin/people", "people");
  await everyWay(t, "/n-admin/login", "login-with-name", undefined, { state: { cookies: [], origins: [] } });

  // Sam logs in from another browser.
  const samContext = await t.context({ owner: false });
  const sam = await samContext.newPage();
  await logIn(t, sam, "sam", "wrong password here");
  await t.seeText(sam.getByRole("alert"), "Wrong name or password.");
  t.equal(await sam.getByLabel("Name").inputValue(), "sam", "the name is kept after a wrong password");
  await logIn(t, sam, "Sam", password);
  await sam.waitForURL(`${t.origin}/n-admin`);
  await sam.locator("details.menu summary").click();
  t.check(await sam.getByRole("link", { name: "Account" }).isVisible(), "a person's menu has Account");
  t.equal(await sam.locator("details.menu a", { hasText: "People" }).count(), 0, "a person has no People link");
  t.equal((await sam.locator("details.menu summary").textContent()).trim(), "sam", "the menu carries their name");
  const blocked = await sam.goto(`${t.origin}/n-admin/people`);
  t.equal(blocked.status(), 404, "and can't open People");
  await t.go(sam, "/n-admin/account");
  await t.seeText(sam.locator(".account-who"), "You are logged in as sam");

  // Sam changes their own password.
  await changePassword(sam, "not it at all", "a brand new password");
  await t.seeText(sam.getByRole("alert"), "Your current password is wrong.");
  await changePassword(sam, password, "a brand new password", "a different password");
  await t.seeText(sam.getByRole("alert"), "The two new passwords don't match.");
  await sam.getByLabel("Current password").fill(password);
  await sam.locator("input[name=password]").fill("short");
  t.check(!(await sam.locator("input[name=password]").evaluate((el) => el.checkValidity())), "a short password is refused in the form");
  await changePassword(sam, password, "a brand new password");
  await t.seeText(sam.getByRole("status"), "Password changed.");
  await t.fits(sam, "account after a change");
  const samTwo = await (await t.context({ owner: false })).newPage();
  await logIn(t, samTwo, "sam", "a brand new password");
  await samTwo.waitForURL(`${t.origin}/n-admin`);
  t.check(true, "the new password works");

  // The owner gives Sam a new password: Sam is logged out everywhere.
  await t.go(owner, "/n-admin/people");
  owner.answers.push(true);
  await owner.locator("tr", { hasText: "sam" }).getByRole("button", { name: "New password" }).click();
  await t.seeText(owner.locator(".given"), "sam has a new password and is logged out.");
  const reset = (await owner.locator(".given dd code").nth(1).textContent()).trim();
  await sam.reload();
  t.check(sam.url().includes("/n-admin/login"), "a reset logs the person out");
  await logIn(t, sam, "sam", reset);
  await sam.waitForURL(`${t.origin}/n-admin`);

  // Removing Sam ends the session at once.
  await t.go(owner, "/n-admin/people");
  owner.answers.push(false);
  await owner.locator("tr", { hasText: "sam" }).getByRole("button", { name: "Remove" }).click();
  await owner.waitForTimeout(200);
  t.equal(await owner.locator("tr", { hasText: "sam" }).count(), 1, "cancelling keeps them");
  owner.answers.push(true);
  await owner.locator("tr", { hasText: "sam" }).getByRole("button", { name: "Remove" }).click();
  await t.seeText(owner.getByRole("status"), "Removed, and logged out.");
  await sam.reload();
  t.check(sam.url().includes("/n-admin/login"), "removing logs them out");
  t.equal(await sam.getByLabel("Name").count(), 0, "with nobody else, the login asks only for a password");
});

scenario("account: the owner changes their password", async (t) => {
  // Logged in on its own, not sharing this run's saved session.
  const other = await t.page({ owner: false });
  await other.goto(`${t.origin}/n-admin/login`);
  await other.getByLabel("Password").fill(t.ownerPassword);
  await other.getByRole("button", { name: "Log in" }).click();
  await other.waitForURL(`${t.origin}/n-admin`);
  const page = await t.page({ size: PHONE });
  await t.go(page, "/n-admin/account");
  await t.seeText(page.locator(".account-who"), "You are the owner.");
  await changePassword(page, t.ownerPassword, "the owner's new password");
  await t.seeText(page.getByRole("status"), "Password changed.");
  await t.fits(page, "owner account on a phone");
  await t.go(page, "/n-admin");
  t.check(!page.url().includes("login"), "this browser stays logged in");
  await other.goto(`${t.origin}/n-admin`);
  t.check(other.url().includes("/n-admin/login"), "other browsers are logged out");
  t.ownerPassword = "the owner's new password";
  t.ownerState = null;
  const again = await t.page({ size: DESKTOP });
  await t.go(again, "/n-admin");
  t.check(!again.url().includes("login"), "the new password logs in");
});
