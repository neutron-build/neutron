/**
 * The public/ file list the SSR server serves root URLs from. Split out of
 * build.ts (which imports vite at load time) so it can be tested from source,
 * like static-paths.ts.
 */
import * as fs from "node:fs";
import * as path from "node:path";

/**
 * Record the root URL of every file Vite copied from public/ into dist, so the
 * production server serves those and nothing else from the dist root (not the
 * Vite manifest, build metadata or a preset server bundle). Written even when
 * empty: its presence tells the server to use the list.
 */
export function writePublicFilesMetadata(outputDir: string, publicDir: string | null): void {
  const files: string[] = [];
  const walk = (dir: string, prefix: string): void => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const urlPath = `${prefix}/${entry.name}`;
      if (entry.isDirectory()) {
        walk(path.join(dir, entry.name), urlPath);
      } else if (entry.isFile() && fs.existsSync(path.join(outputDir, urlPath))) {
        files.push(urlPath);
      }
    }
  };
  if (publicDir && fs.existsSync(publicDir)) {
    walk(publicDir, "");
  }
  files.sort();
  fs.writeFileSync(
    path.join(outputDir, ".neutron-public-files.json"),
    JSON.stringify({ files }, null, 2)
  );
}
