#!/usr/bin/env -S node --no-warnings
import { randomBytes } from "node:crypto";
import { hashPassword } from "./auth.js";
import { backupTo, db, dbPath } from "./store.js";

const [command, argument] = process.argv.slice(2);

const commands: Record<string, () => void | Promise<void>> = {
  async password() {
    const password = randomBytes(24).toString("base64url");
    console.log(`Password (shown once, keep it somewhere safe):\n\n  ${password}\n`);
    console.log(`Set this in the environment. Doing so signs out every session:\n\n  CMS_PASSWORD_HASH=${await hashPassword(password)}\n`);
  },
  init() {
    db({ create: true });
    console.log(`Database ready at ${dbPath()}`);
  },
  backup() {
    if (!argument) throw new Error("Usage: neutron-cms backup <file>");
    backupTo(argument);
    console.log(`Backed up to ${argument}`);
  },
};

const run = command && Object.hasOwn(commands, command) ? commands[command] : undefined;
if (!run) {
  console.error("Usage: neutron-cms password | init | backup <file>");
  process.exit(1);
}
await run();
