import { build } from "./build.js";
import { deployCheck } from "./deploy-check.js";

export async function releaseCheck(): Promise<void> {
  console.log("Running release-grade checks...");
  await build();
  await deployCheck();
  console.log("Release-grade checks passed.");
}
