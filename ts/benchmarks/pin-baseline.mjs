import { copyFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const cwd = process.cwd();
const sourcePath = path.resolve(cwd, process.env.BENCH_BASELINE_SOURCE || "results/latest.json");
const targetPath = path.resolve(
  cwd,
  process.env.BENCH_BASELINE_TARGET || "results/baseline-full.json",
);

try {
  await copyFile(sourcePath, targetPath);
  console.log(`Pinned baseline: ${targetPath}`);
  console.log(`Source: ${sourcePath}`);
} catch (error) {
  console.error("Failed to pin baseline.");
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
